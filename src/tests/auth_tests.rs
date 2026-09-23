//! Authentication / authorization tests: loopback owner perms, identity gates
//! (control surface / test runner / credential read), and JWT name-trust.

use crate::auth::{AuthStore, generate_auth_token};
use crate::{
    is_always_trusted, is_control_surface, is_loopback_addr, is_test_runner,
    may_read_other_credentials, userdb_actor_perm,
};

#[test]
fn loopback_grants_owner_perm() {
    // A loopback TUI connection may act with owner perms even when the
    // payload supplies no actor.
    assert_eq!(userdb_actor_perm(true, "user"), "owner");
    assert_eq!(userdb_actor_perm(true, ""), "owner");
    // Non-loopback connections keep the supplied actor (remote TUI-login
    // is deferred); the engine/user DB still enforce it.
    assert_eq!(userdb_actor_perm(false, "user"), "user");
    assert_eq!(userdb_actor_perm(false, "admin"), "admin");
    assert_eq!(userdb_actor_perm(false, ""), "");
}

#[test]
fn loopback_addr_detection() {
    assert!(is_loopback_addr(&"127.0.0.1:1111".parse::<std::net::SocketAddr>().unwrap()));
    assert!(is_loopback_addr(&"[::1]:1111".parse::<std::net::SocketAddr>().unwrap()));
    assert!(!is_loopback_addr(&"192.168.1.10:1111".parse::<std::net::SocketAddr>().unwrap()));
    assert!(!is_loopback_addr(&"10.0.0.5:1111".parse::<std::net::SocketAddr>().unwrap()));
}

/// A2 — identity gates: only the TUI / test-runner / audit-viewer reach the
/// privileged roles; ordinary modules cannot.
#[test]
fn control_surface_and_trusted_roles() {
    assert!(is_control_surface("cockatiel-tui"));
    assert!(is_control_surface("cockatiel-tui-child"));
    assert!(!is_control_surface("twitch-adapter"));
    assert!(!is_control_surface("reprimand"));
    assert!(!is_control_surface(""));

    assert!(is_always_trusted("cockatiel-tui"));
    assert!(is_always_trusted("cockatiel-test-runner"));
    assert!(is_always_trusted("cockatiel-audit-viewer"));
    assert!(!is_always_trusted("twitch-adapter"));

    assert!(is_test_runner("cockatiel-test-runner"));
    assert!(!is_test_runner("cockatiel-tui"));
}

/// A2 — credential reads: only the TUI and term-chat's OAuth login may read
/// other modules' secrets.
#[test]
fn credential_read_gate() {
    assert!(may_read_other_credentials("cockatiel-tui"));
    assert!(may_read_other_credentials("term-chat"));
    assert!(!may_read_other_credentials("twitch-adapter"));
    assert!(!may_read_other_credentials("reprimand"));
}

/// A3 — JWT name-trust: a valid token bound to its module name passes; the
/// same token under a different name, a different uuid, or a forged token is
/// rejected.
#[test]
fn jwt_name_trust_binding() {
    let store = AuthStore::new("test-secret".to_string());
    let token = store.generate_token("uuid-1", "module-a");

    assert!(store.verify_token("uuid-1", &token, "module-a"));

    // Replay the token under a trusted name -> the name claim mismatches.
    assert!(!store.verify_token("uuid-1", &token, "cockatiel-tui"));
    // Replay under a different uuid.
    assert!(!store.verify_token("uuid-2", &token, "module-a"));
    // Wrong secret -> forged token.
    let forged = generate_auth_token("other-secret", "uuid-1", "module-a");
    assert!(!store.verify_token("uuid-1", &forged, "module-a"));
    // Empty/garbage token.
    assert!(!store.verify_token("uuid-1", "not.a.jwt", "module-a"));
}