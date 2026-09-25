//! Database semantics tests for crash recovery: marking mid-flight rows failed,
//! listing queued uuids, and loading a queued row's content. Uses an in-memory
//! timeline DB (`:memory:`).

use crate::database::{DatabaseConfig, DatabaseManager};
use std::path::PathBuf;

async fn test_db() -> DatabaseManager {
    let db = DatabaseManager::new(DatabaseConfig {
        local_path: PathBuf::from(":memory:"),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();
    db
}

#[tokio::test]
async fn mark_processing_failed_and_get_queued_uuids_semantics() {
    let db = test_db().await;
    let u1 = b"u1-uuid-bytes-0000000001".to_vec();
    let u2 = b"u2-uuid-bytes-0000000002".to_vec();
    let u3 = b"u3-uuid-bytes-0000000003".to_vec();
    db.insert_event(&u1, 1, "twitch", &[], "hello one", "", "{}").await.unwrap();
    db.insert_event(&u2, 1, "kick", &[], "hello two", "", "{}").await.unwrap();
    db.insert_event(&u3, 1, "youtube", &[], "hello three", "", "{}").await.unwrap();

    // u1 + u3 are mid-flight; u2 never started.
    db.set_pipeline_status(&u1, "processing").await.unwrap();
    db.set_pipeline_status(&u3, "processing").await.unwrap();

    // Only u2 is still queued.
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![String::from_utf8(u2.clone()).unwrap()]);

    // Marking the mid-flight rows failed leaves the queued set untouched.
    // (The exact count turso reports is a scan counter, not strictly the number
    // of modified rows — the real contract is the resulting row state below.)
    let n = db.mark_all_processing_as_failed("interrupted by engine restart").await.unwrap();
    assert!(n >= 2);
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![String::from_utf8(u2.clone()).unwrap()]);

    // The failed rows carry the reason; the queued row still loads normally.
    let json = db.get_event_as_json(&u1).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "failed");
    assert_eq!(v["error_message"], "interrupted by engine restart");
    assert!(db.load_queued_message(&u1).await.unwrap().is_none());
    assert!(db.load_queued_message(&u3).await.unwrap().is_none());

    let q = db.load_queued_message(&u2).await.unwrap().unwrap();
    assert_eq!(q.event_type, 1);
    assert_eq!(q.platform, "kick");
    assert_eq!(q.raw_message, "hello two");
    assert_eq!(q.command, "");
    assert_eq!(q.flags, "{}");
    assert_eq!(q.user_uuid7, "");
}

#[tokio::test]
async fn get_queued_uuids_is_ordered_ascending() {
    let db = test_db().await;
    // "a-..." < "b-..." byte-wise → the earlier-uuid row sorts first.
    let a = b"a-queued-row".to_vec();
    let b = b"b-queued-row".to_vec();
    db.insert_event(&b, 1, "twitch", &[], "bee", "", "{}").await.unwrap();
    db.insert_event(&a, 1, "twitch", &[], "aye", "", "{}").await.unwrap();
    assert_eq!(
        db.get_queued_uuids().await.unwrap(),
        vec![String::from_utf8(a).unwrap(), String::from_utf8(b).unwrap()]
    );
}

/// sync_to_remote must produce a portable backup file that survives repeated
/// calls, and the backup must contain the inserted event when re-opened as a
/// fresh DatabaseManager. Also guards the destroy-then-rename regression: a
/// second sync over an existing backup must not clobber it.
#[tokio::test]
async fn sync_to_remote_writes_a_consistent_reusable_backup() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let local_path = dir.join("cockatiel_data_test.db");
    let backup_path = dir.join("cockatiel_backup_test.db");

    let db = DatabaseManager::new(DatabaseConfig {
        local_path: local_path.clone(),
        remote_url: Some(backup_path.to_string_lossy().into_owned()),
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();

    let uuid = b"sync-backup-uuid-000001".to_vec();
    db.insert_event(&uuid, 1, "twitch", &[], "backup me", "!test", "{}").await.unwrap();

    // First sync creates the backup; a second sync over it must succeed and
    // leave the file present (no destroy-then-rename window).
    assert_eq!(db.sync_to_remote().await.unwrap(), 1);
    assert!(backup_path.exists(), "backup file must exist after first sync");
    assert_eq!(db.sync_to_remote().await.unwrap(), 1);
    assert!(backup_path.exists(), "backup file must survive a second sync");

    // The backup is a real DB: re-open it as a fresh manager and find the event.
    let restored = DatabaseManager::new(DatabaseConfig {
        local_path: backup_path.clone(),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    restored.initialize().await.unwrap();
    let json = restored.get_event_as_json(&uuid).await.unwrap().expect("backup must contain the synced event");
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["raw_message"], "backup me");
    assert_eq!(v["command"], "!test");

    std::fs::remove_dir_all(&dir).ok();
}
