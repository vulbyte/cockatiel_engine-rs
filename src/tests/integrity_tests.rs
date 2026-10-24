//! Timeline DB corruption defenses.
//!
//! The real corruption is a torn btree write that leaves the DB OPENABLE but
//! unscannable. These tests pin the three behaviors:
//!   1. the integrity oracle detects a corrupt file and accepts a clean one;
//!   2. the startup gate restores a verified-clean backup (or moves the corrupt
//!      file aside when there is no clean backup);
//!   3. a corrupt live DB never overwrites the last-known-good backup.
//!
//! The small synthetic corruption (an impossible cell count on the table root)
//! is deterministic and fast. The `#[ignore]`d test at the bottom runs against
//! the real 1.8 GB copies and documents what turso 0.1.5 actually does.

use crate::database::{DatabaseConfig, DatabaseManager, IntegrityVerdict, check_file_integrity};
use std::path::{Path, PathBuf};

fn sqlite3_available() -> bool {
    std::process::Command::new("sqlite3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("cockatiel-integrity-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn test_config(
    local: PathBuf,
    backup: Option<PathBuf>,
    on_open: bool,
    on_backup: bool,
) -> DatabaseConfig {
    DatabaseConfig {
        local_path: local,
        remote_url: backup.map(|p| p.to_string_lossy().into_owned()),
        sync_interval_secs: 15,
        local_target_mb: 50,
        log_min_severity: 0.3,
        integrity_check_on_open: on_open,
        integrity_check_on_backup: on_backup,
        integrity_check_min_mb: 0,
    }
}

fn remove_sidecars(path: &Path) {
    let p = path.to_string_lossy();
    let _ = std::fs::remove_file(format!("{p}-wal"));
    let _ = std::fs::remove_file(format!("{p}-shm"));
}

/// Create a real timeline DB (all schema + indexes) with one marker row, merge
/// its WAL, then drop the manager so the main file is the whole database.
async fn make_db(path: &Path, marker: &[u8], message: &str) -> DatabaseManager {
    let db = DatabaseManager::new(test_config(path.to_path_buf(), None, false, false));
    db.initialize().await.unwrap();
    db.insert_event(marker, 1, "twitch", &[], message, "", "{}")
        .await
        .unwrap();
    db.checkpoint_wal().await;
    db
}

async fn timeline_root_page(db: &DatabaseManager) -> u64 {
    let json = db
        .execute_query("SELECT rootpage FROM sqlite_schema WHERE name = 'timeline_events'")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    v.as_array().unwrap()[0]["rootpage"].as_u64().unwrap()
}

/// Make the table root's cell count impossible (200 when the page only has a
/// handful of slots). `PRAGMA quick_check` reports out-of-range cell offsets;
/// the file still opens.
fn corrupt_root_cell_count(path: &Path, root_page: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut header = [0u8; 100];
    f.read_exact(&mut header).unwrap();
    let mut page_size = u16::from_be_bytes([header[16], header[17]]) as u64;
    if page_size == 1 {
        page_size = 65536;
    }
    let offset = (root_page - 1) * page_size + 3; // page-type + 2 freeblock bytes
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&200u16.to_be_bytes()).unwrap();
    f.sync_all().unwrap();
}

fn has_corrupt_aside(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains(".corrupt."))
}

#[tokio::test]
async fn integrity_check_detects_a_corrupt_file_and_accepts_a_clean_one() {
    if !sqlite3_available() {
        eprintln!("skipping: sqlite3 is not installed");
        return;
    }
    let dir = temp_dir();
    let path = dir.join("data.db");
    let db = make_db(&path, b"clean-marker-0001", "clean").await;
    let root = timeline_root_page(&db).await;
    drop(db);
    remove_sidecars(&path);

    assert_eq!(check_file_integrity(&path).await, IntegrityVerdict::Clean);

    corrupt_root_cell_count(&path, root);
    match check_file_integrity(&path).await {
        IntegrityVerdict::Corrupt(detail) => {
            assert!(!detail.is_empty(), "the corruption detail must be reported");
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }

    cleanup(dir);
}

#[tokio::test]
async fn startup_gate_restores_a_verified_clean_backup() {
    if !sqlite3_available() {
        eprintln!("skipping: sqlite3 is not installed");
        return;
    }
    let dir = temp_dir();
    let local = dir.join("data.db");
    let backup = dir.join("backup.db");
    let marker = b"backup-marker-0001";

    // A known-good backup with a row only it has.
    let backup_db = make_db(&backup, marker, "from the backup").await;
    drop(backup_db);
    remove_sidecars(&backup);
    assert_eq!(check_file_integrity(&backup).await, IntegrityVerdict::Clean);

    // A corrupt live DB.
    let local_db = make_db(&local, b"local-marker-0001", "corrupt me").await;
    let root = timeline_root_page(&local_db).await;
    drop(local_db);
    remove_sidecars(&local);
    corrupt_root_cell_count(&local, root);
    assert!(matches!(
        check_file_integrity(&local).await,
        IntegrityVerdict::Corrupt(_)
    ));

    let db = DatabaseManager::new(test_config(local.clone(), Some(backup), true, false));
    db.initialize().await.unwrap();

    // The marker row proves the backup (not a fresh empty DB) was restored.
    let json = db
        .get_event_as_json(marker)
        .await
        .unwrap()
        .expect("the backup's row must survive the restore");
    assert!(json.contains("from the backup"));
    drop(db);

    assert_eq!(check_file_integrity(&local).await, IntegrityVerdict::Clean);
    assert!(
        !has_corrupt_aside(&dir),
        "a clean backup was used, so nothing should have been moved aside"
    );

    cleanup(dir);
}

#[tokio::test]
async fn startup_gate_moves_a_corrupt_db_aside_when_no_clean_backup_exists() {
    if !sqlite3_available() {
        eprintln!("skipping: sqlite3 is not installed");
        return;
    }
    let dir = temp_dir();
    let local = dir.join("data.db");

    let local_db = make_db(&local, b"local-marker-0001", "corrupt me").await;
    let root = timeline_root_page(&local_db).await;
    drop(local_db);
    remove_sidecars(&local);
    corrupt_root_cell_count(&local, root);

    let db = DatabaseManager::new(test_config(local.clone(), None, true, false));
    db.initialize().await.unwrap(); // fresh empty DB
    drop(db);

    assert!(
        has_corrupt_aside(&dir),
        "the corrupt DB must be preserved as <db>.corrupt.<timestamp>"
    );
    assert_eq!(check_file_integrity(&local).await, IntegrityVerdict::Clean);

    cleanup(dir);
}

#[tokio::test]
async fn a_corrupt_source_never_overwrites_a_clean_backup() {
    if !sqlite3_available() {
        eprintln!("skipping: sqlite3 is not installed");
        return;
    }
    let dir = temp_dir();
    let local = dir.join("data.db");
    let backup = dir.join("backup.db");

    // The last-known-good backup.
    let backup_db = make_db(&backup, b"good-marker-0001", "good backup").await;
    drop(backup_db);
    remove_sidecars(&backup);
    assert_eq!(check_file_integrity(&backup).await, IntegrityVerdict::Clean);
    let before = std::fs::read(&backup).unwrap();

    // A corrupt live DB. The startup gate is off so we can exercise the
    // periodic backup against it directly.
    let local_db = make_db(&local, b"bad-marker-0001", "corrupt source").await;
    let root = timeline_root_page(&local_db).await;
    drop(local_db);
    remove_sidecars(&local);
    corrupt_root_cell_count(&local, root);

    let db = DatabaseManager::new(test_config(local, Some(backup.clone()), false, true));
    db.initialize().await.unwrap();
    let result = db.sync_to_remote().await;
    assert!(
        result.is_err(),
        "a corrupt source must not produce a promoted backup"
    );
    drop(db);

    let after = std::fs::read(&backup).unwrap();
    assert_eq!(
        before, after,
        "the last-known-good backup must be byte-for-byte unchanged"
    );
    assert_eq!(check_file_integrity(&backup).await, IntegrityVerdict::Clean);

    cleanup(dir);
}

/// Empirical record of what turso 0.1.5 actually does with the real corrupt
/// copy. Run with:
///   COCKATIEL_CORRUPT_DB=/tmp/corrupt.db COCKATIEL_CLEAN_DB=/tmp/clean.db \
///     cargo test --features probe --all-targets real_corrupt_copy -- --ignored --nocapture
///
/// Observed 2026-10-23:
///   * `PRAGMA quick_check` → `Parse error: Not a valid pragma name`
///     (turso has no quick_check; only integrity_check exists).
///   * `PRAGMA integrity_check` → `ok` on this corrupt file (it misses the
///     torn-write duplicate page), so turso is NOT a trustworthy oracle here.
///   * `sqlite3 'PRAGMA quick_check'` → `2nd reference to page ...`.
#[tokio::test]
#[ignore = "requires the real /tmp/corrupt.db and /tmp/clean.db copies"]
async fn real_corrupt_copy_turso_quick_check_behavior() {
    let corrupt = PathBuf::from(
        std::env::var("COCKATIEL_CORRUPT_DB").unwrap_or_else(|_| "/tmp/corrupt.db".to_string()),
    );
    let clean = PathBuf::from(
        std::env::var("COCKATIEL_CLEAN_DB").unwrap_or_else(|_| "/tmp/clean.db".to_string()),
    );
    if !corrupt.exists() || !clean.exists() {
        eprintln!("skipping: {corrupt:?} and/or {clean:?} are missing");
        return;
    }

    // The oracle the engine actually uses (sqlite3 quick_check).
    match check_file_integrity(&corrupt).await {
        IntegrityVerdict::Corrupt(detail) => assert!(
            detail.contains("2nd reference to page"),
            "expected sqlite3 to name the duplicate page, got: {detail}"
        ),
        other => panic!("sqlite3 must detect the real corruption, got {other:?}"),
    }
    assert_eq!(check_file_integrity(&clean).await, IntegrityVerdict::Clean);

    // Document the in-process turso behavior on the same file.
    let db = turso::Builder::new_local(corrupt.to_string_lossy().as_ref())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();

    let quick = conn.query("PRAGMA quick_check", ()).await;
    match quick {
        Err(e) => assert!(
            e.to_string().contains("Not a valid pragma name"),
            "unexpected turso quick_check error: {e}"
        ),
        Ok(_) => panic!("turso unexpectedly accepted PRAGMA quick_check"),
    }

    let mut stmt = conn.query("PRAGMA integrity_check", ()).await.unwrap();
    let mut lines = Vec::new();
    while let Some(row) = stmt.next().await.unwrap() {
        if let turso::Value::Text(t) = row.get_value(0).unwrap() {
            lines.push(t);
        }
    }
    assert_eq!(
        lines,
        vec!["ok".to_string()],
        "turso 0.1.5 is expected to MISS this corruption; if it starts detecting \
         it, the fallback oracle can be trusted again"
    );
}
