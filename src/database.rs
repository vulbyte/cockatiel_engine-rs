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

/// The content of a queued (never-started) timeline row, enough to rebuild a
/// pipeline message after a crash. NOTE: `channel_id`, `user_data` and the
/// parsed `Command` are NOT persisted on the timeline row — a recovered message
/// runs without them.
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    pub event_type: i32,
    pub platform: String,
    pub raw_message: String,
    pub command: String,
    pub flags: String,
    pub user_uuid7: String,
}

/// The terminal state a timeline row ends in.
///
/// A message's `pipeline_status` is written exactly ONCE, at the end of its
/// chain, by [`DatabaseManager::write_terminal_outcome`]. Between the ingest
/// INSERT ('queued') and that write the row is never touched again: while a
/// message is in flight the in-memory pipeline state and [`crate::pipeline::AckTracker`]
/// are the source of truth, and the database learns the outcome in one
/// round-trip instead of a dozen per-stage UPDATEs against the same row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineOutcome {
    /// The whole chain ran: `pipeline_status = 'complete'`.
    Complete,
    /// The message failed: `pipeline_status = 'failed'` + the reason in
    /// `error_message`. (A module erroring is not this — that is logged and
    /// recorded against the module, and the message is left to finish.)
    Failed(String),
    /// The chain was abandoned before it finished: `pipeline_status = 'dropped'`
    /// + the reason in `error_message`.
    Dropped(String),
    /// Take the hold: `pipeline_status = 'audit'` with the reason in `flags`
    /// (the column the audit prompt and `list_audit` read the reason back from).
    Audit(String),
    /// The row is ALREADY held: land the run's accumulated result on it and
    /// leave the hold alone — `pipeline_status` is not written at all and the
    /// reason already in `flags` is untouched. The pipeline reaches this when a
    /// message an operator is holding finishes its chain: the data the
    /// per-stage writes used to leave behind still lands, the hold does not get
    /// released.
    AuditHeld,
}

impl PipelineOutcome {
    /// The `pipeline_status` this outcome lands the row on, or `None` when the
    /// outcome deliberately leaves the status column alone.
    pub fn status(&self) -> Option<&'static str> {
        match self {
            Self::Complete => Some("complete"),
            Self::Failed(_) => Some("failed"),
            Self::Dropped(_) => Some("dropped"),
            Self::Audit(_) => Some("audit"),
            Self::AuditHeld => None,
        }
    }

    /// The `error_message` this outcome carries, or `None` to leave the column
    /// exactly as it is — a completed or held message has no error.
    fn error(&self) -> Option<&str> {
        match self {
            Self::Failed(e) | Self::Dropped(e) => Some(e),
            Self::Complete | Self::Audit(_) | Self::AuditHeld => None,
        }
    }
}

/// Everything a message's run accumulated, gathered in memory while the message
/// is in flight and handed to [`DatabaseManager::write_terminal_outcome`] once at
/// the end. This is what the per-stage writes used to mirror into the row one
/// column at a time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineResult {
    /// The final processed text. `None` leaves `processed_message` alone.
    pub processed_message: Option<String>,
    /// When `pre_process` finished (its last module acked), ms since the epoch.
    pub pre_process_completed_at: Option<i64>,
    /// When `in_process` finished, ms since the epoch.
    pub in_process_completed_at: Option<i64>,
    /// Rendered audio: `(content type, bytes)`. `None` — or empty bytes —
    /// means the message carries no audio, and the row's existing `data_blob`
    /// and audio marker in `flags` are left untouched rather than clobbered.
    pub audio: Option<(String, Vec<u8>)>,
    /// Every `audio_type=<mime>` marker the run produced, in the order the
    /// stage writes used to append them. A message that rendered audio at two
    /// stages carries both markers, exactly as the per-stage `set_audio` calls
    /// left them in `flags`.
    pub audio_type_markers: Vec<String>,
}

/// Append `clause = ?N` to a SET list, binding `value` as the Nth parameter.
fn push_set(sets: &mut Vec<String>, args: &mut Vec<Value>, clause: &str, value: Value) {
    sets.push(format!("{} = ?{}", clause, args.len() + 1));
    args.push(value);
}

#[derive(Debug, Clone)]
pub struct DatabaseManager {
    config: DatabaseConfig,
    local: Arc<Mutex<Option<turso::Connection>>>,
    /// Counts the calls this manager makes into the database layer. Test-only:
    /// the whole point of the consolidated write is how much a message costs, and
    /// the turso driver exposes no statement hook to count them from the outside
    /// — so every method takes its connection through [`Self::locked`] and the
    /// tests read the delta. (One call == one `locked()`; `set_audio` is two SQL
    /// statements under one call.) Compiled out of non-test builds entirely.
    #[cfg(test)]
    round_trips: Arc<std::sync::atomic::AtomicUsize>,
}

impl DatabaseManager {
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            local: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            round_trips: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// The one door every call into the database goes through, so the tests can
    /// count them.
    #[cfg(test)]
    async fn locked(&self) -> tokio::sync::MutexGuard<'_, Option<turso::Connection>> {
        self.round_trips
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.local.lock().await
    }

    #[cfg(not(test))]
    async fn locked(&self) -> tokio::sync::MutexGuard<'_, Option<turso::Connection>> {
        self.local.lock().await
    }

    /// Test-only: read the call count and reset it to zero, so a test can measure
    /// exactly what happened between two points.
    #[cfg(test)]
    pub fn take_round_trips(&self) -> usize {
        self.round_trips.swap(0, std::sync::atomic::Ordering::SeqCst)
    }

    /// True when a backup DB path is configured (and thus a backup is usable).
    pub fn backup_configured(&self) -> bool {
        self.config
            .remote_url
            .as_deref()
            .map(|p| !p.trim().is_empty())
            .unwrap_or(false)
    }

    /// Size of the local timeline DB main file in bytes (the warning logic
    /// compares this against the configured target).
    pub fn db_size_bytes(&self) -> u64 {
        std::fs::metadata(&self.config.local_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// The configured target size (MB) for the local DB.
    pub fn target_mb(&self) -> u32 {
        self.config.local_target_mb
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
                        // A stale -wal/-shm for the corrupt local DB would be
                        // re-applied over the restored file — drop them
                        // (best-effort) before copying so the restored snapshot
                        // is clean.
                        let local_path = self.config.local_path.to_string_lossy();
                        let _ = std::fs::remove_file(format!("{}-wal", local_path));
                        let _ = std::fs::remove_file(format!("{}-shm", local_path));
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
            let mut local = self.locked().await;
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

    /// Replace the local connection with a fresh one. Used after a query
    /// panics: a turso/Limbo "not yet implemented" panic poisons the shared
    /// connection (even clones), so we discard it and reopen the DB file.
    pub async fn reopen_local(&self) -> Result<(), Box<dyn std::error::Error>> {
        let fresh = Self::open_local(&self.config.local_path).await?;
        let mut local = self.locked().await;
        *local = Some(fresh);
        Ok(())
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
        let conn = self.locked().await;
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

    /// The one write that ENDS a message: the final processed text, the audio,
    /// the stage-completion timestamps and the terminal status all land in a
    /// single round-trip. This replaces the twelve writes (fourteen calls into
    /// this layer — `set_audio` is two statements) the pipeline used to issue
    /// against the same row for one message through all three stages:
    /// `set_pipeline_status` at the start, `set_processed_message` / `set_audio` /
    /// `update_stage_completed` per stage, then the completion tail. The row ends
    /// up with exactly the same columns and values, it just stops being rewritten
    /// while the message is in flight.
    ///
    /// Two rules keep that equivalence:
    ///
    /// * A column the run never produced is left OUT of the SET list rather than
    ///   bound as NULL — so a message with no audio keeps whatever `data_blob` /
    ///   `flags` the row already had instead of being clobbered with empties.
    /// * `post_process_completed_at` / `persisted_at` are stamped by this write
    ///   and only for [`PipelineOutcome::Complete`]: they mean "the whole
    ///   pipeline ran", so a failed, dropped or held message must not get them —
    ///   exactly the contract the two `update_stage_completed` calls at the end
    ///   of the old completion path had.
    ///
    /// A row held for audit is never moved to 'complete'. The hold is the
    /// operator's queue item and only `release_audit` may release it, so a
    /// pipeline that finishes a held message must not publish it behind the
    /// operator's back; every other outcome lands exactly as it always did.
    pub async fn write_terminal_outcome(
        &self,
        uuid7: &[u8],
        outcome: &PipelineOutcome,
        result: &PipelineResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Self::now_ms();
        let mut sets: Vec<String> = Vec::new();
        let mut args: Vec<Value> = Vec::new();

        // The terminal status. `AuditHeld` deliberately writes no status at all:
        // the row is already held and stays exactly as it is.
        if let Some(status) = outcome.status() {
            push_set(
                &mut sets,
                &mut args,
                "pipeline_status",
                Value::Text(status.to_string()),
            );
        }
        if let Some(error) = outcome.error() {
            push_set(&mut sets, &mut args, "error_message", Value::Text(error.to_string()));
        }

        // What the run produced. `None` means "the run never got here".
        if let Some(processed) = &result.processed_message {
            push_set(
                &mut sets,
                &mut args,
                "processed_message",
                Value::Text(processed.clone()),
            );
        }
        if let Some(ts) = result.pre_process_completed_at {
            push_set(&mut sets, &mut args, "pre_process_completed_at", Value::Integer(ts));
        }
        if let Some(ts) = result.in_process_completed_at {
            push_set(&mut sets, &mut args, "in_process_completed_at", Value::Integer(ts));
        }
        if matches!(outcome, PipelineOutcome::Complete) {
            push_set(&mut sets, &mut args, "post_process_completed_at", Value::Integer(now));
            push_set(&mut sets, &mut args, "persisted_at", Value::Integer(now));
        }

        // Audio: the bytes go in `data_blob` and the content type rides in
        // `flags` as `audio_type=<mime>`. Appending to the row's own flags —
        // rather than re-deriving the whole string in memory — is what lets one
        // statement serve a normal row and a HELD one, whose `flags` column is
        // the operator's hold reason and must survive. The join reproduces
        // `set_audio`'s output exactly, empty-flags case included.
        let audio = result
            .audio
            .as_ref()
            .filter(|(_, bytes)| !bytes.is_empty() && !result.audio_type_markers.is_empty());
        match audio {
            Some((_, bytes)) => {
                push_set(&mut sets, &mut args, "data_blob", Value::Blob(bytes.clone()));
                let joined = args.len() + 1;
                sets.push(format!(
                    "flags = CASE WHEN flags IS NULL OR flags = '' THEN ?{joined} ELSE flags || ',' || ?{joined} END"
                ));
                args.push(Value::Text(result.audio_type_markers.join(",")));
            }
            None => {
                if let PipelineOutcome::Audit(reason) = outcome {
                    push_set(&mut sets, &mut args, "flags", Value::Text(reason.clone()));
                }
            }
        }

        // Nothing to say — an `AuditHeld` write for a row whose in-memory state
        // is already gone. A bare `SET` is not a statement, and the row needs no
        // change, so don't spend a round-trip on it.
        if sets.is_empty() {
            return Ok(());
        }

        let uuid_arg = args.len() + 1;
        args.push(Value::Blob(uuid7.to_vec()));
        let mut sql = format!(
            "UPDATE timeline_events SET {} WHERE uuid7 = ?{}",
            sets.join(", "),
            uuid_arg
        );
        if matches!(outcome, PipelineOutcome::Complete) {
            sql.push_str(" AND pipeline_status <> 'audit'");
        }

        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        conn.execute(&sql, args).await?;

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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
    ) -> Result<i32, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT COUNT(*) FROM timeline_events WHERE synced_at IS NULL AND pipeline_status = 'complete'",
            (),
        ).await?;

        if let Some(row) = rows.next().await? {
            let count: i64 = row.get(0)?;
            Ok(count as i32)
        } else {
            Ok(0)
        }
    }

    pub async fn get_event_as_json(
        &self,
        uuid7: &[u8],
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
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

    /// Query timeline events by filter. `None`/empty filters mean "any". Only
    /// read-only SQL is built, from a fixed set of columns, with bound
    /// parameters — never string-concatenated input. Returns JSON rows.
    pub async fn query_timeline(
        &self,
        timeline_id_uuid7: Option<&[u8]>,
        event_type: Option<i32>,
        platform: Option<&str>,
        user_uuid7: Option<&str>,
        kind: Option<&str>,
        raw_prefix: Option<&str>,
        since_ms: Option<i64>,
        pipeline_status: Option<&str>,
        limit: i32,
        offset: i32,
    ) -> Result<String, String> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut limit = if limit <= 0 { 100 } else { limit.min(500) };
        let mut offset = offset.max(0);

        // Base select over the fixed timeline columns.
        let mut sql = String::from(
            "SELECT uuid7, event_type, platform, raw_message, command, flags, data_blob, \
             user_uuid7, processed_message, error_message, persisted_at, pipeline_status \
             FROM timeline_events WHERE 1=1",
        );
        let mut args: Vec<Value> = Vec::new();

        if let Some(id) = timeline_id_uuid7 {
            sql.push_str(" AND uuid7 = ?");
            args.push(Value::Blob(id.to_vec()));
            // A single-event fetch ignores pagination.
            limit = 1;
            offset = 0;
        }
        if let Some(et) = event_type {
            sql.push_str(" AND event_type = ?");
            args.push(Value::Integer(et as i64));
        }
        if let Some(p) = platform {
            sql.push_str(" AND platform = ?");
            args.push(Value::Text(p.to_string()));
        }
        if let Some(u) = user_uuid7 {
            sql.push_str(" AND user_uuid7 = ?");
            args.push(Value::Text(u.to_string()));
        }
        if let Some(k) = kind {
            // `kind` lives inside the flags blob; match it with a LIKE on the
            // JSON-ish text, safe because the pattern is bound, not injected.
            sql.push_str(" AND flags LIKE ?");
            args.push(Value::Text(format!("%{}%", k)));
        }
        if let Some(prefix) = raw_prefix {
            sql.push_str(" AND raw_message LIKE ?");
            args.push(Value::Text(format!("{}%", prefix)));
        }
        if let Some(ts) = since_ms {
            sql.push_str(" AND persisted_at >= ?");
            args.push(Value::Integer(ts));
        }
        if let Some(ps) = pipeline_status {
            sql.push_str(" AND pipeline_status = ?");
            args.push(Value::Text(ps.to_string()));
        }
        // LIMIT/OFFSET are inlined as literals rather than bound: turso 0.1.5
        // fails to step a query whose LIMIT/OFFSET are parameters
        // (`Parse error: MustBeInt`). Both are i32 already clamped above, so
        // formatting them cannot inject.
        sql.push_str(&format!(
            " ORDER BY persisted_at DESC LIMIT {} OFFSET {}",
            limit, offset
        ));

        let mut rows = conn
            .query(sql.as_str(), turso::params_from_iter(args))
            .await
            .map_err(|e| format!("timeline query failed: {}", e))?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| format!("row read failed: {}", e))? {
            let uuid7: Option<Vec<u8>> = row.get(0).ok();
            let event_type: i32 = row.get(1).unwrap_or(0);
            let platform: Option<String> = row.get(2).ok();
            let raw_message: String = row.get(3).unwrap_or_default();
            let command: Option<String> = row.get(4).ok();
            let flags: Option<String> = row.get(5).ok();
            let data_blob: Option<Vec<u8>> = row.get(6).ok();
            let user_uuid7: Option<String> = row.get(7).ok();
            let processed_message: Option<String> = row.get(8).ok();
            let error_message: Option<String> = row.get(9).ok();
            let persisted_at: Option<i64> = row.get(10).ok();
            let pipeline_status: Option<String> = row.get(11).ok();

            let id_hex = uuid7
                .as_deref()
                .and_then(|b| uuid::Uuid::from_slice(b).ok())
                .map(|u| u.to_string())
                .unwrap_or_default();
            out.push(serde_json::json!({
                "timeline_id_uuid7": id_hex,
                "event_type": event_type,
                "platform": platform,
                "raw_message": raw_message,
                "command": command,
                "flags": flags,
                "data_blob": data_blob,
                "user_uuid7": user_uuid7,
                "processed_message": processed_message,
                "error_message": error_message,
                "persisted_at": persisted_at,
                "pipeline_status": pipeline_status,
            }));
        }

        Ok(serde_json::to_string(&out).map_err(|e| format!("json encode failed: {}", e))?)
    }

    /// Execute an arbitrary read-only SQL query and return results as JSON.
    /// Returns an array of objects where keys are column names and values are the cell values.
    pub async fn execute_query(
        &self,
        sql: &str,
    ) -> Result<String, String> {
        // Return a `String` error (Send) so callers can run this in a contained
        // task that catches turso/Limbo panics on unsupported SQL.
        let run = async {
        // Clone the connection and drop the guard BEFORE running the query:
        // a turso/Limbo "not yet implemented" panic on the SQL would otherwise
        // poison the shared mutex and brick the timeline DB. The clone is a
        // cheap shared handle (turso::Connection is Clone).
        let conn = {
            let guard = self.locked().await;
            guard.as_ref().ok_or("Local database not initialized")?.clone()
        };
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
        };
        run.await.map_err(|e: Box<dyn std::error::Error>| e.to_string())
    }

    /// Mark every 'processing' row as failed with the given reason. Returns the
    /// number of rows updated.
    ///
    /// NOTHING THIS ENGINE WRITES IS EVER 'processing' ANY MORE, so this sweep
    /// has no rows of its own to act on: a message is inserted 'queued' and
    /// stays 'queued' until the single terminal write at the end of its chain
    /// (see [`DatabaseManager::write_terminal_outcome`]), because the in-memory
    /// pipeline state — not `pipeline_status` — is what marks a message as in
    /// flight. It is kept, deliberately and harmlessly, for the one case it can
    /// still serve: rows stranded in 'processing' by an engine build that
    /// predates the consolidation. Those can never be resumed (the recovery
    /// drain only re-drives 'queued' rows), so failing them is the right
    /// outcome — and matching on 'processing' means it can never touch a row
    /// the current engine owns.
    ///
    /// It is NOT the crash-recovery path any more. A message that was mid-flight
    /// when the engine died is still 'queued' and is replayed to completion by
    /// `get_queued_uuids` + `recover_one`, which now claims rows in memory.
    pub async fn mark_all_processing_as_failed(&self, reason: &str) -> Result<u32, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let result = conn.execute(
            "UPDATE timeline_events SET pipeline_status = 'failed', error_message = ?1 WHERE pipeline_status = 'processing'",
            turso::params![reason],
        ).await?;

        Ok(result as u32)
    }

    /// Load a queued row's content by uuid7, if it is still `pipeline_status =
    /// 'queued'`. Returns None when the row is missing or has already been
    /// driven past 'queued' by a terminal write.
    ///
    /// NOTE: a message that is IN FLIGHT is 'queued' too now (the pipeline
    /// writes no intermediate status), so this is no longer the "is a live
    /// pipeline running this?" check it used to be — callers must pair it with
    /// the in-memory claim (`recover_one` does, via `pipeline_states`).
    pub async fn load_queued_message(&self, uuid7: &[u8]) -> Result<Option<QueuedMessage>, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT event_type, platform, raw_message, command, flags, user_uuid7 FROM timeline_events WHERE uuid7 = ?1 AND pipeline_status = 'queued'",
            turso::params![uuid7],
        ).await?;

        if let Some(row) = rows.next().await? {
            let event_type: i32 = row.get(0)?;
            let platform: Option<String> = row.get(1)?;
            let raw_message: String = row.get(2)?;
            let command: Option<String> = row.get(3)?;
            let flags: Option<String> = row.get(4)?;
            let user_uuid7: Option<String> = row.get(5)?;
            Ok(Some(QueuedMessage {
                event_type,
                platform: platform.unwrap_or_default(),
                raw_message,
                command: command.unwrap_or_default(),
                flags: flags.unwrap_or_default(),
                user_uuid7: user_uuid7.unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Every uuid7 currently `pipeline_status = 'queued'`, in ascending uuid7
    /// order — the crash-recovery drain set.
    /// Uuids of every stranded 'queued' row, oldest first. The uuid7 column may be
    /// stored as BLOB or TEXT depending on how the row was inserted (turso's
    /// `FromValue for Vec<u8>` only accepts BLOB and `String` only TEXT, so
    /// neither alone is safe) — use `get_value` and decode both.
    pub async fn get_queued_uuids(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT uuid7 FROM timeline_events WHERE pipeline_status = 'queued' ORDER BY uuid7 ASC",
            (),
        ).await?;

        let mut results = Vec::new();
        while let Some(row) = rows.next().await? {
            let uuid7: turso::Value = row.get_value(0)?;
            let uuid = match uuid7 {
                turso::Value::Text(t) => t,
                turso::Value::Blob(b) => String::from_utf8_lossy(&b).to_string(),
                _ => continue,
            };
            results.push(uuid);
        }

        Ok(results)
    }

    /// Normalize uuid7 storage to BLOB. Historical rows (e.g. direct SQL
    /// inserts with a string, or older writers) can store the uuid as TEXT;
    /// every keyed read/write in the pipeline binds the uuid as bytes (BLOB),
    /// and SQLite's type rules mean a BLOB param never equals a TEXT column.
    /// Re-write those rows as BLOB so all byte-keyed queries (status updates,
    /// stage completion, recovery) work on them. Returns rows normalized.
    pub async fn normalize_uuid_storage(&self) -> Result<u32, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;

        let mut rows = conn.query(
            "SELECT uuid7 FROM timeline_events WHERE typeof(uuid7) = 'text'",
            (),
        ).await?;
        let mut text_uuids: Vec<String> = Vec::new();
        while let Some(row) = rows.next().await? {
            let v: turso::Value = row.get_value(0)?;
            if let turso::Value::Text(t) = v {
                text_uuids.push(t);
            }
        }

        let mut normalized = 0u32;
        for u in text_uuids {
            // Match the TEXT row by string; set the same bytes as a BLOB.
            conn.execute(
                "UPDATE timeline_events SET uuid7 = ?1 WHERE uuid7 = ?2 AND typeof(uuid7) = 'text'",
                turso::params![u.as_bytes(), u.as_str()],
            ).await?;
            normalized += 1;
        }
        Ok(normalized)
    }

    // ── Audit (held-for-review messages) ───────────────────────────────

    /// Hold a message for human review: status 'audit', reason stored in flags.
    ///
    /// A hold is a terminal outcome like any other, so it goes through the same
    /// single write — with an EMPTY result, because taking a hold must not touch
    /// any of the columns the pipeline accumulates. The statement is identical
    /// to the one this used to issue (`pipeline_status = 'audit'`, `flags` =
    /// reason) and is still awaited inline by the caller: it has to land before
    /// the flagging module's ack is processed.
    pub async fn mark_audit(
        &self,
        uuid7: &[u8],
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.write_terminal_outcome(
            uuid7,
            &PipelineOutcome::Audit(reason.to_string()),
            &PipelineResult::default(),
        )
        .await
    }

    /// Whether a message is currently held for audit.
    pub async fn is_audited(&self, uuid7: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
        let conn = self.locked().await;
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
    pub async fn sync_to_remote(&self) -> Result<u32, Box<dyn std::error::Error>> {
        let Some(bp) = self.config.remote_url.as_deref() else {
            return Ok(0);
        };
        if bp.trim().is_empty() {
            return Ok(0);
        }

        let conn = self.locked().await;
        let conn = conn.as_ref().ok_or("Local database not initialized")?;
        // Merge the WAL so the copy is authoritative — but only when a -wal
        // file actually exists for the local DB (checkpointing a missing WAL
        // should not fail the backup). Best-effort either way: a failed
        // checkpoint must never fail the backup.
        // (PRAGMA returns a result row — drain it so the driver doesn't error.)
        let wal_path = format!("{}-wal", self.config.local_path.to_string_lossy());
        if std::path::Path::new(&wal_path).exists()
            && let Ok(mut stmt) = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await
        {
            while let Ok(Some(_)) = stmt.next().await {}
        }

        // Copy while the lock is held → no concurrent writes → consistent.
        let tmp = format!("{}.tmp", bp);
        let _ = std::fs::remove_file(&tmp);
        std::fs::copy(&self.config.local_path, &tmp)
            .map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))?;
        // Drop the borrowed connection reference (the MutexGuard itself was
        // already released when `conn` was shadowed) before the atomic rename.
        let _ = conn;

        // POSIX rename() atomically replaces an existing target — never delete
        // the last good backup before the new snapshot lands.
        std::fs::rename(&tmp, bp).map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))?;
        Ok(1)
    }

    /// Force the local SQLite write-ahead log to merge into the main file and
    /// truncate. Called periodically and when the WAL exceeds ~5 MB.
    pub async fn checkpoint_wal(&self) {
        if let Some(conn) = self.locked().await.as_ref().map(|c| c.clone()) {
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
