//! The failed-PIN throttle's keying.
//!
//! The engine binds loopback by default, so every local module AND the TUI
//! share one peer IP. A per-IP lockout therefore let one stale/bad module lock
//! the operator's own TUI out of the engine — the outage that motivated keying
//! the gate by `(IP, module name)` instead.

use crate::PinGate;
use std::net::IpAddr;

fn loopback() -> IpAddr {
    "127.0.0.1".parse().unwrap()
}

#[test]
fn five_failures_lock_that_identity_only() {
    let gate = PinGate::default();
    for _ in 0..5 {
        gate.record_failure(loopback(), "stale-module");
    }
    // The offender is locked...
    assert!(!gate.check(loopback(), "stale-module"));
    // ...but a DIFFERENT module on the same IP is untouched.
    assert!(gate.check(loopback(), "some-other-module"));
    // ...and so is the TUI.
    assert!(gate.check(loopback(), "cockatiel-tui"));
}

#[test]
fn a_correct_pin_clears_only_that_identity() {
    let gate = PinGate::default();
    for _ in 0..3 {
        gate.record_failure(loopback(), "a");
    }
    for _ in 0..3 {
        gate.record_failure(loopback(), "b");
    }
    gate.record_success(loopback(), "a");
    // "a" starts fresh: it would need 5 more failures to lock.
    for _ in 0..4 {
        gate.record_failure(loopback(), "a");
    }
    assert!(gate.check(loopback(), "a"), "a success reset a's counter");
    // "b" still has its 3 failures; 2 more lock it.
    for _ in 0..2 {
        gate.record_failure(loopback(), "b");
    }
    assert!(!gate.check(loopback(), "b"));
}

#[test]
fn four_failures_do_not_lock() {
    let gate = PinGate::default();
    for _ in 0..4 {
        gate.record_failure(loopback(), "m");
    }
    assert!(gate.check(loopback(), "m"));
}
