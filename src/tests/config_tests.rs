//! Config tests: the startup backfill writes newly-added keys into config.json
//! with their code defaults (house convention) without disturbing existing keys.

use crate::config::{backfill_config_defaults, Config, ConfigState};
use std::sync::{Arc, Mutex};

/// A ConfigState pointed at a throwaway temp config.json, parsed from the given
/// content (missing keys fall back to serde defaults — same as engine startup).
fn temp_state(dir: &std::path::Path, content: &str) -> (Arc<Mutex<ConfigState>>, std::path::PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("config.json");
    std::fs::write(&path, content).unwrap();
    let config: Config = serde_json::from_str(content).unwrap();
    let state = Arc::new(Mutex::new(ConfigState {
        path: path.clone(),
        last_size: content.len() as u64,
        config,
        pin: 0,
        jwt_secret: String::new(),
    }));
    (state, path)
}

#[test]
fn backfill_writes_missing_keys_and_preserves_existing() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backfill-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "module_approval_policy": "auto-allow"
    }"#;
    let (state, path) = temp_state(&dir, original);

    backfill_config_defaults(&state);

    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    // Existing keys are preserved untouched.
    assert_eq!(v["timeline_database_location"], "./test.db");
    assert_eq!(v["timeline_database_backup_location"], "./test-backup.db");
    assert_eq!(v["port"], 9734);
    assert_eq!(v["module_approval_policy"], "auto-allow");

    // The five new keys were written at the TOP level with their defaults.
    assert_eq!(v["max_message_bytes"], 16 * 1024 * 1024);
    assert_eq!(v["max_connections"], 128);
    assert_eq!(v["handshake_timeout_secs"], 10);
    assert_eq!(v["send_timeout_secs"], 5);
    assert_eq!(v["recovery_grace_secs"], 10);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn backfill_is_a_noop_when_all_keys_present() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backfill-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "max_message_bytes": 1,
        "max_connections": 2,
        "handshake_timeout_secs": 3,
        "send_timeout_secs": 4,
        "recovery_grace_secs": 5,
        "module_approval_policy": "auto-allow"
    }"#;
    let (state, path) = temp_state(&dir, original);

    backfill_config_defaults(&state);

    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    // Nothing rewritten — the operator's values survive verbatim.
    assert_eq!(v["max_message_bytes"], 1);
    assert_eq!(v["max_connections"], 2);
    assert_eq!(v["handshake_timeout_secs"], 3);
    assert_eq!(v["send_timeout_secs"], 4);
    assert_eq!(v["recovery_grace_secs"], 5);

    std::fs::remove_dir_all(&dir).ok();
}