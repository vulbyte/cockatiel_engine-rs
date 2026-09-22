use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use turso::Value;
use uuid::Uuid;

pub const SCHEMA_VERSION: i32 = 1;

const CREATE_TIMELINE_EVENTS: &str = "
CREATE TABLE IF NOT EXISTS timeline_events (
    uuid7 BLOB PRIMARY KEY,
    schema_version INTEGER NOT NULL DEFAULT 1,
    event_type INTEGER NOT NULL DEFAULT 1,
    platform TEXT,
    raw_data BLOB,
    raw_message TEXT NOT NULL,
    command TEXT,
    flags TEXT,
    data_blob BLOB,
    user_uuid7 TEXT,
    processed_message TEXT,
    error_message TEXT,
    pre_process_completed_at INTEGER,
    in_process_completed_at INTEGER,
    post_process_completed_at INTEGER,
    persisted_at INTEGER,
    pipeline_status TEXT NOT NULL DEFAULT 'queued',
    synced_at INTEGER
)";

const CREATE_SYNCED_INDEX: &str = "
CREATE INDEX IF NOT EXISTS idx_timeline_synced ON timeline_events(synced_at)
";

const CREATE_STATUS_INDEX: &str = "
CREATE INDEX IF NOT EXISTS idx_timeline_status ON timeline_events(pipeline_status)
";

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub local_path: PathBuf,
    pub remote_url: Option<String>,
    pub sync_interval_secs: u32,
    pub local_target_mb: u32,
}

#[derive(Debug, Clone)]
pub struct DatabaseManager {
    config: DatabaseConfig,
    local: Arc<Mutex<Option<turso::Connection>>>,
}

impl DatabaseManager {
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            local: Arc::new(Mutex::new(None)),
        }
    }

    /// True when a backup DB path is configured (and thus a backup is usable).
    pub fn backup_configured(&self) -> bool {
        self.config
            .remote_url
            .as_deref()
            .map(|p| !p.trim().is_empty())
            .unwrap_or(false)
    }

    pub async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Open the local DB (the write buffer / queue). If it is corrupt or
        // otherwise can't be opened and a backup exists, restore from the
        // backup before giving up.
        let local_result = Self::open_local(&self.config.local_path).await;
        let conn = match local_result {
            Ok(c) => c,
            Err(_) => {
                if let Some(bp) = self.config.remote_url.as_deref() {
                    let backup_file = std::path::Path::new(bp);
                    if backup_file.exists() {
                        eprintln!(
                            "[Database] local DB unavailable — restoring from backup {}",
                            bp
                        );
                        if std::fs::copy(backup_file, &self.config.local_path).is_ok() {
                            Self::open_local(&self.config.local_path).await?
                        } else {
                            return Err("local DB corrupt and backup restore failed".into());
                        }
                    } else {
                        return Err("local DB corrupt and no backup exists".into());
                    }
                } else {
                    return Err("local DB corrupt and no backup configured".into());
                }
            }
        };

        {
            let mut local = self.local.lock().await;
            *local = Some(conn);
        }

        println!("[Database] Initialized at {:?}", self.config.local_path);

        if self.backup_configured() {
            println!(
                "[Database] Backup enabled at {} (periodic snapshot)",
                self.config.remote_url.as_deref().unwrap_or("")
            );
        } else {
            println!("[Database] NO BACKUP SET — a corruption could mean TOTAL DATA LOSS");
        }

        Ok(())
    }

    async fn open_local(path: &std::path::Path) -> Result<turso::Connection, Box<dyn std::error::Error>> {
        let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await?;
        let conn = db.connect()?;
        conn.execute(CREATE_TIMELINE_EVENTS, ()).await?;
        conn.execute(CREATE_SYNCED_INDEX, ()).await?;
        conn.execute(CREATE_STATUS_INDEX, ()).await?;
        Ok(conn)
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    pub async fn insert_event(
        &self,
        uuid7: &[u8],
        event_type: i32,
        platform: &str,
        raw_data: &[u8],
        raw_message: &str,
        command: &str,
        flags: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "INSERT INTO timeline_events (uuid7, schema_version, event_type, platform, raw_data, raw_message, command, flags, pipeline_status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued')",
            turso::params![
                uuid7,
                SCHEMA_VERSION,
                event_type,
                platform,
                raw_data,
                raw_message,
                command,
                flags,
            ],
        ).await?;

        Ok(())
    }

    /// Insert a timeline archival event that is already 'complete' and marked
    /// as synced, so the message-pipeline queue and the remote sync never touch
    /// it. Used for module lifecycle logging and outbound platform messages
    /// (SendToPlatforms) so the actor who sent them is recorded in the timeline.
    /// `command` distinguishes the event's semantic kind (e.g.
    /// "module_lifecycle" for connect/disconnect/log, "send_to_platforms" for
    /// outbound sends, "test_archive" for compliance results).
    pub async fn insert_archival_event(
        &self,
        platform: &str,
        command: &str,
        raw_message: &str,
        flags: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let uuid7 = Uuid::now_v7().as_bytes().to_vec();
        let now = Self::now_ms();

        conn.execute(
            "INSERT INTO timeline_events (uuid7, schema_version, event_type, platform, raw_message, command, flags, pipeline_status, synced_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'complete', ?8)",
            turso::params![
                uuid7.as_slice(),
                SCHEMA_VERSION,
                5, // event_type = 5 (user message)
                platform,
                raw_message,
                command,
                flags,
                now,
            ],
        ).await?;

        Ok(())
    }

    pub async fn update_stage_completed(
        &self,
        uuid7: &[u8],
        stage: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let field = match stage {
            "pre_process" => "pre_process_completed_at",
            "in_process" => "in_process_completed_at",
            "post_process" => "post_process_completed_at",
            "persisted" => "persisted_at",
            _ => return Err(format!("Unknown stage: {}", stage).into()),
        };

        let now = Self::now_ms();
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let sql = format!("UPDATE timeline_events SET {} = ?1 WHERE uuid7 = ?2", field);
        conn.execute(&sql, turso::params![now, uuid7]).await?;

        Ok(())
    }

    pub async fn set_pipeline_status(
        &self,
        uuid7: &[u8],
        status: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET pipeline_status = ?1 WHERE uuid7 = ?2",
            turso::params![status, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn set_error(
        &self,
        uuid7: &[u8],
        error: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET error_message = ?1, pipeline_status = 'failed' WHERE uuid7 = ?2",
            turso::params![error, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn set_processed_message(
        &self,
        uuid7: &[u8],
        processed: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET processed_message = ?1 WHERE uuid7 = ?2",
            turso::params![processed, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn set_user_uuid(
        &self,
        uuid7: &[u8],
        user_uuid: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET user_uuid7 = ?1 WHERE uuid7 = ?2 AND user_uuid7 IS NULL",
            turso::params![user_uuid, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn set_data_blob(
        &self,
        uuid7: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET data_blob = ?1 WHERE uuid7 = ?2",
            turso::params![data, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn set_flags(
        &self,
        uuid7: &[u8],
        flags: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET flags = ?1 WHERE uuid7 = ?2",
            turso::params![flags, uuid7],
        ).await?;

        Ok(())
    }

    /// Store rendered audio on a message: the bytes go in `data_blob` and the
/// content type rides in the `flags` column as `audio_type=<mime>`, so the
/// web UI / displays can retrieve it later.
    pub async fn set_audio(
        &self,
        uuid7: &[u8],
        audio_type: &str,
        audio: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        // Append the content type to any existing flags.
        let mut existing_flags = conn.query(
            "SELECT flags FROM timeline_events WHERE uuid7 = ?1",
            turso::params![uuid7],
        ).await?;
        let mut flags = String::new();
        if let Some(row) = existing_flags.next().await? {
            if let Ok(f) = row.get::<String>(0) {
                flags = f;
            }
        }
        drop(existing_flags);
        let audio_marker = format!("audio_type={}", audio_type);
        if !flags.split(',').any(|p| p == audio_marker) {
            if !flags.is_empty() {
                flags.push(',');
            }
            flags.push_str(&audio_marker);
        }

        conn.execute(
            "UPDATE timeline_events SET data_blob = ?1, flags = ?2 WHERE uuid7 = ?3",
            turso::params![audio, flags, uuid7],
        ).await?;

        Ok(())
    }

    /// Fetch a message's rendered audio (content type, bytes) if it has any.
    pub async fn get_audio(
        &self,
        uuid7: &[u8],
    ) -> Result<Option<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT data_blob, flags FROM timeline_events WHERE uuid7 = ?1",
            turso::params![uuid7],
        ).await?;
        if let Some(row) = rows.next().await? {
            let blob: Option<Vec<u8>> = row.get(0).unwrap_or(None);
            let flags: String = row.get(1).unwrap_or_default();
            if let Some(bytes) = blob {
                if bytes.is_empty() {
                    return Ok(None);
                }
                let audio_type = flags
                    .split(',')
                    .find_map(|p| p.strip_prefix("audio_type="))
                    .unwrap_or("audio/mpeg")
                    .to_string();
                return Ok(Some((audio_type, bytes)));
            }
        }
        Ok(None)
    }

    pub async fn set_command(
        &self,
        uuid7: &[u8],
        command: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        conn.execute(
            "UPDATE timeline_events SET command = ?1 WHERE uuid7 = ?2",
            turso::params![command, uuid7],
        ).await?;

        Ok(())
    }

    pub async fn mark_synced(
        &self,
        uuid7s: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        let now = Self::now_ms();

        for uuid in uuid7s {
            conn.execute(
                "UPDATE timeline_events SET synced_at = ?1 WHERE uuid7 = ?2",
                turso::params![now, uuid.as_slice()],
            ).await?;
        }

        Ok(())
    }

    pub async fn get_next_queued(
        &self,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT uuid7 FROM timeline_events WHERE pipeline_status = 'queued' ORDER BY uuid7 ASC LIMIT 1",
            (),
        ).await?;

        if let Some(row) = rows.next().await? {
            let uuid7: Vec<u8> = row.get(0)?;
            Ok(Some(uuid7))
        } else {
            Ok(None)
        }
    }

    pub async fn get_incomplete(
        &self,
    ) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT uuid7 FROM timeline_events WHERE pipeline_status IN ('queued', 'processing') AND synced_at IS NULL ORDER BY uuid7 ASC",
            (),
        ).await?;

        let mut results = Vec::new();
        while let Some(row) = rows.next().await? {
            let uuid7: Vec<u8> = row.get(0)?;
            results.push(uuid7);
        }

        Ok(results)
    }

    pub async fn get_unsynced_count(
        &self,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT COUNT(*) FROM timeline_events WHERE synced_at IS NULL AND pipeline_status = 'complete'",
            (),
        ).await?;

        if let Some(row) = rows.next().await? {
            let count: i64 = row.get(0)?;
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub async fn get_event_as_json(
        &self,
        uuid7: &[u8],
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT schema_version, event_type, platform, raw_message, command, flags, user_uuid7, processed_message, error_message, pre_process_completed_at, in_process_completed_at, post_process_completed_at, persisted_at, pipeline_status, synced_at FROM timeline_events WHERE uuid7 = ?1",
            turso::params![uuid7],
        ).await?;

        if let Some(row) = rows.next().await? {
            let schema_version: i32 = row.get(0)?;
            let event_type: i32 = row.get(1)?;
            let platform: Option<String> = row.get(2)?;
            let raw_message: String = row.get(3)?;
            let command: Option<String> = row.get(4)?;
            let flags: Option<String> = row.get(5)?;
            let user_uuid7: Option<String> = row.get(6)?;
            let processed_message: Option<String> = row.get(7)?;
            let error_message: Option<String> = row.get(8)?;
            let pre: Option<i64> = row.get(9)?;
            let in_p: Option<i64> = row.get(10)?;
            let post: Option<i64> = row.get(11)?;
            let persisted: Option<i64> = row.get(12)?;
            let status: String = row.get(13)?;
            let synced: Option<i64> = row.get(14)?;

            let json = serde_json::json!({
                "schema_version": schema_version,
                "event_type": event_type,
                "platform": platform,
                "raw_message": raw_message,
                "command": command,
                "flags": flags,
                "user_uuid7": user_uuid7,
                "processed_message": processed_message,
                "error_message": error_message,
                "pre_process_completed_at": pre,
                "in_process_completed_at": in_p,
                "post_process_completed_at": post,
                "persisted_at": persisted,
                "pipeline_status": status,
                "synced_at": synced,
            });

            Ok(Some(serde_json::to_string(&json)?))
        } else {
            Ok(None)
        }
    }

    /// Execute an arbitrary read-only SQL query and return results as JSON.
    /// Returns an array of objects where keys are column names and values are the cell values.
    pub async fn execute_query(
        &self,
        sql: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut stmt = conn.prepare(sql).await?;
        let columns: Vec<String> = stmt.columns().iter().map(|c| c.name().to_string()).collect();
        let mut rows = stmt.query(()).await?;
        let mut results = Vec::new();

        while let Some(row) = rows.next().await? {
            let mut map = serde_json::Map::new();
            let col_count = row.column_count();
            for i in 0..col_count {
                let name = columns.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                let val = row.get_value(i)?;
                match val {
                    Value::Integer(n) => { map.insert(name, serde_json::Value::Number(n.into())); }
                    Value::Real(f) => {
                        if let Some(num) = serde_json::Number::from_f64(f) {
                            map.insert(name, serde_json::Value::Number(num));
                        } else {
                            map.insert(name, serde_json::Value::Null);
                        }
                    }
                    Value::Text(s) => { map.insert(name, serde_json::Value::String(s)); }
                    Value::Blob(b) => {
                        map.insert(name, serde_json::Value::String(
                            String::from_utf8_lossy(&b).to_string(),
                        ));
                    }
                    Value::Null => { map.insert(name, serde_json::Value::Null); }
                }
            }
            results.push(serde_json::Value::Object(map));
        }

        Ok(serde_json::to_string(&results)?)
    }

    pub async fn mark_all_processing_as_queued(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let result = conn.execute(
            "UPDATE timeline_events SET pipeline_status = 'queued' WHERE pipeline_status = 'processing'",
            (),
        ).await?;

        Ok(result as u64)
    }

    // ── Audit (held-for-review messages) ───────────────────────────────

    /// Hold a message for human review: status 'audit', reason stored in flags.
    pub async fn mark_audit(
        &self,
        uuid7: &[u8],
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        conn.execute(
            "UPDATE timeline_events SET pipeline_status = 'audit', flags = ?1 WHERE uuid7 = ?2",
            turso::params![reason, uuid7],
        )
        .await?;
        Ok(())
    }

    /// Whether a message is currently held for audit.
    pub async fn is_audited(&self, uuid7: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        let mut rows = conn
            .query(
                "SELECT 1 FROM timeline_events WHERE uuid7 = ?1 AND pipeline_status = 'audit'",
                turso::params![uuid7],
            )
            .await?;
        Ok(rows.next().await?.is_some())
    }

    /// Fetch the content of a held message for the audit prompt.
    pub async fn get_audit_entry(
        &self,
        uuid7: &[u8],
    ) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        let mut rows = conn
            .query(
                "SELECT platform, raw_message, user_uuid7, flags FROM timeline_events WHERE uuid7 = ?1",
                turso::params![uuid7],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            let platform: Option<String> = row.get(0)?;
            let raw_message: String = row.get(1)?;
            let user_uuid7: Option<String> = row.get(2)?;
            let flags: Option<String> = row.get(3)?;
            Ok(Some(serde_json::json!({
                "platform": platform,
                "raw_message": raw_message,
                "user_uuid7": user_uuid7,
                "reason": flags,
            })))
        } else {
            Ok(None)
        }
    }

    /// List held-for-audit messages as JSON rows (uuid, platform, message,
    /// user, reason, persisted_at).
    pub async fn list_audit(
        &self,
        limit: i32,
        offset: i32,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        let limit = if limit <= 0 { 100 } else { limit };
        let offset = if offset < 0 { 0 } else { offset };

        let mut rows = conn
            .query(
                "SELECT uuid7, platform, raw_message, user_uuid7, flags, persisted_at
                 FROM timeline_events WHERE pipeline_status = 'audit'
                 ORDER BY persisted_at ASC LIMIT ?1 OFFSET ?2",
                turso::params![limit, offset],
            )
            .await?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let uuid7: Vec<u8> = row.get(0)?;
            let platform: Option<String> = row.get(1)?;
            let raw_message: String = row.get(2)?;
            let user_uuid7: Option<String> = row.get(3)?;
            let flags: Option<String> = row.get(4)?;
            let persisted_at: Option<i64> = row.get(5)?;
            out.push(serde_json::json!({
                "uuid7": String::from_utf8_lossy(&uuid7).to_string(),
                "platform": platform,
                "raw_message": raw_message,
                "user_uuid7": user_uuid7,
                "reason": flags,
                "persisted_at": persisted_at,
            }));
        }
        Ok(out)
    }

    /// Release an audited message. approve=true resubmits it into the timeline
    /// as a normal ('complete') message; approve=false marks it 'failed'.
    pub async fn release_audit(
        &self,
        uuid7: &[u8],
        approve: bool,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        if approve {
            conn.execute(
                "UPDATE timeline_events SET pipeline_status = 'complete', persisted_at = ?2 WHERE uuid7 = ?1 AND pipeline_status = 'audit'",
                turso::params![uuid7, Self::now_ms()],
            )
            .await?;
        } else {
            conn.execute(
                "UPDATE timeline_events SET pipeline_status = 'failed' WHERE uuid7 = ?1 AND pipeline_status = 'audit'",
                turso::params![uuid7],
            )
            .await?;
        }
        Ok(true)
    }

    /// Write a consistent snapshot of the local DB to the configured backup path.
    /// Merges the WAL into the main file, then copies it while the DB lock is
    /// held (no concurrent writes) so the snapshot is authoritative and the
    /// backup file stays portable/inspectable. Returns 1 on success, 0 when no
    /// backup is configured.
    pub async fn sync_to_remote(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let Some(bp) = self.config.remote_url.as_deref() else {
            return Ok(0);
        };
        if bp.trim().is_empty() {
            return Ok(0);
        }

        let conn = self.local.lock().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        // Merge the WAL so the copy is authoritative.
        // (PRAGMA returns a result row — drain it so the driver doesn't error.)
        if let Ok(mut stmt) = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await {
            while let Ok(Some(_)) = stmt.next().await {}
        }

        // Copy while the lock is held → no concurrent writes → consistent.
        let tmp = format!("{}.tmp", bp);
        let _ = std::fs::remove_file(&tmp);
        std::fs::copy(&self.config.local_path, &tmp)
            .map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))?;
        drop(conn);

        let _ = std::fs::remove_file(bp);
        std::fs::rename(&tmp, bp).map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))?;
        Ok(1)
    }

    /// Force the local SQLite write-ahead log to merge into the main file and
    /// truncate. Called periodically and when the WAL exceeds ~5 MB.
    pub async fn checkpoint_wal(&self) {
        if let Some(conn) = self.local.lock().await.as_ref().map(|c| c.clone()) {
            if let Ok(mut stmt) = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await {
                while let Ok(Some(_)) = stmt.next().await {}
            }
        }
    }

    /// Approximate size of the local DB's write-ahead log in bytes.
    pub fn wal_size(&self) -> u64 {
        let mut path = self.config.local_path.as_os_str().to_owned();
        path.push("-wal");
        std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
    }
}
