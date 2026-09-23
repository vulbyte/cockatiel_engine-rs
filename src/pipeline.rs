use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

use crate::cockatiel_protobuf;
use crate::cockatiel_protobuf::{Container, container::Payload, ChatMessage, Command};
use crate::command_registry::CommandRegistry;
use crate::database::DatabaseManager;

const DEFAULT_ACK_TIMEOUT_MS: u64 = 3000;

fn uuid7_string_to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn uuid7_string() -> Option<String> {
    Some(uuid::Uuid::now_v7().to_string())
}

#[derive(Debug, Clone)]
pub struct PipelineMessage {
    pub uuid7: String,
    pub raw_message: String,
    pub platform: String,
    pub event_type: i32,
    pub command: Option<String>,
    pub flags: Option<String>,
    pub parsed_command: Option<crate::cockatiel_protobuf::Command>,
    pub channel_id: String,
    pub user_uuid7: String,
    pub user_data: Option<crate::cockatiel_protobuf::UserData>,
}

#[derive(Debug, Clone)]
pub struct PendingAck {
    pub uuid7: String,
    pub stage: String,
    pub module_name: String,
    pub sent_at: Instant,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct AckTracker {
    pending: HashMap<String, Vec<PendingAck>>,
}

impl AckTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    pub fn track(
        &mut self,
        uuid7: String,
        stage: String,
        module_name: String,
        timeout_ms: u64,
    ) {
        let entry = PendingAck {
            uuid7: uuid7.clone(),
            stage,
            module_name,
            sent_at: Instant::now(),
            timeout: Duration::from_millis(timeout_ms),
        };

        self.pending.entry(uuid7).or_default().push(entry);
    }

    pub fn ack(&mut self, uuid7: &str) -> Vec<PendingAck> {
        self.pending.remove(uuid7).unwrap_or_default()
    }

    pub fn check_timeouts(&mut self) -> Vec<PendingAck> {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        let mut to_remove = Vec::new();

        for (uuid7, entries) in &mut self.pending {
            let mut still_pending = Vec::new();
            for entry in entries.drain(..) {
                if now.duration_since(entry.sent_at) > entry.timeout {
                    timed_out.push(entry);
                } else {
                    still_pending.push(entry);
                }
            }
            if still_pending.is_empty() {
                to_remove.push(uuid7.clone());
            } else {
                *entries = still_pending;
            }
        }

        for uuid in to_remove {
            self.pending.remove(&uuid);
        }

        timed_out
    }

    pub fn has_pending(&self, uuid7: &str) -> bool {
        self.pending
            .get(uuid7)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub pre_process_modules: Vec<String>,
    pub in_process_modules: Vec<String>,
    pub post_process_modules: Vec<String>,
    pub ack_timeout_ms: u64,
    pub critical_modules: Vec<String>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            pre_process_modules: Vec::new(),
            in_process_modules: Vec::new(),
            post_process_modules: Vec::new(),
            ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
            critical_modules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    Inserted,
    PreProcessing,
    InProcessing,
    PostProcessing,
    Complete,
    Failed,
}

impl PipelineStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inserted => "queued",
            Self::PreProcessing => "processing",
            Self::InProcessing => "processing",
            Self::PostProcessing => "processing",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineState {
    pub uuid7: String,
    pub stage: PipelineStage,
    pub current_in_process_index: usize,
    pub raw_message: String,
    pub processed_message: String,
    pub platform: String,
    pub event_type: i32,
    pub command: Option<String>,
    pub flags: Option<String>,
    /// The engine-parsed command (with flag values) when this message is a
    /// registered chat command; drives targeted routing + downstream modules.
    pub parsed_command: Option<Command>,
    pub channel_id: String,
    pub user_uuid7: String,
    pub user_data: Option<crate::cockatiel_protobuf::UserData>,
    /// Rendered audio carried with the message (created by a pre/in-process
    /// module), so it flows forward to the post-process stage / displays.
    pub audio: Vec<u8>,
    /// Content type of `audio` (e.g. "audio/mpeg").
    pub audio_type: String,
    /// Which stage produced the audio: "pre" / "in" / "post".
    pub audio_stage: String,
}

#[derive(Clone)]
pub struct PipelineOrchestrator {
    pub db: DatabaseManager,
    pub ack_tracker: Arc<Mutex<AckTracker>>,
    pub pipeline_states: Arc<Mutex<HashMap<String, PipelineState>>>,
    pub config: Arc<Mutex<PipelineConfig>>,
    pub module_senders: Arc<Mutex<HashMap<String, mpsc::Sender<Container>>>>,
    pub command_registry: Arc<std::sync::Mutex<CommandRegistry>>,
}

impl PipelineOrchestrator {
    pub fn new(
        db: DatabaseManager,
        config: PipelineConfig,
        module_senders: Arc<Mutex<HashMap<String, mpsc::Sender<Container>>>>,
        command_registry: Arc<std::sync::Mutex<CommandRegistry>>,
    ) -> Self {
        Self {
            db,
            ack_tracker: Arc::new(Mutex::new(AckTracker::new())),
            pipeline_states: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(config)),
            module_senders,
            command_registry,
        }
    }

    /// Snapshot of the current pipeline config (runtime-updatable via the config poll task).
    pub async fn config_snapshot(&self) -> PipelineConfig {
        self.config.lock().await.clone()
    }

    pub async fn set_config(&self, config: PipelineConfig) {
        *self.config.lock().await = config;
    }

    pub async fn insert_and_start(
        &self,
        msg: PipelineMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(&msg.uuid7);

        self.db.insert_event(
            &uuid7_bytes,
            msg.event_type,
            &msg.platform,
            &[],
            &msg.raw_message,
            msg.command.as_deref().unwrap_or(""),
            msg.flags.as_deref().unwrap_or("{}"),
        ).await?;

        if let Some(ref cmd) = msg.command {
            self.db.set_command(&uuid7_bytes, cmd).await?;
        }

        let state = PipelineState {
            uuid7: msg.uuid7.clone(),
            stage: PipelineStage::PreProcessing,
            current_in_process_index: 0,
            raw_message: msg.raw_message.clone(),
            processed_message: msg.raw_message.clone(),
            platform: msg.platform.clone(),
            event_type: msg.event_type,
            command: msg.command.clone(),
            flags: msg.flags.clone(),
            parsed_command: msg.parsed_command.clone(),
            channel_id: msg.channel_id.clone(),
            user_uuid7: msg.user_uuid7.clone(),
            user_data: msg.user_data.clone(),
            audio: Vec::new(),
            audio_type: String::new(),
            audio_stage: String::new(),
        };

        if !msg.user_uuid7.is_empty() {
            self.db.set_user_uuid(&uuid7_bytes, &msg.user_uuid7).await?;
        }

        {
            let mut states = self.pipeline_states.lock().await;
            states.insert(msg.uuid7.clone(), state);
        }

        self.db.set_pipeline_status(&uuid7_bytes, "processing").await?;

        self.broadcast_pre_process(&msg.uuid7).await?;

        Ok(())
    }

    async fn broadcast_pre_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let state = {
            let states = self.pipeline_states.lock().await;
            states.get(uuid7).cloned()
        };

        let state = match state {
            Some(s) => s,
            None => return Ok(()),
        };

        let container = Container {
            version: 1,
            auth_token: String::new(),
            module_name: "cockatiel".into(),
            module_instance_uuid7: String::new(),
            payload: Some(Payload::MessagePreProcess(
                cockatiel_protobuf::MessagePreProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: vec![],
                        raw_message: state.raw_message.clone(),
                        user_uuid7: state.user_uuid7.clone(),
                        command: state.parsed_command.clone(),
                        channel_id: state.channel_id.clone(),
                        user_data: state.user_data.clone(),
                    }),
                    audio: Vec::new(),
                    audio_type: String::new(),
                },
            )),
        };

        // Targeted command routing: a registered command goes ONLY to the
        // owning module + any catch-all modules (empty Commands = receives
        // everything). Everything else fans out to all pre-process modules.
        let targets: Option<Vec<String>> = {
            let registry = self.command_registry.lock().unwrap();
            match &state.parsed_command {
                Some(pc) => {
                    let mut t: Vec<String> = Vec::new();
                    if let Some(owner) = registry.owner(&pc.command_flag, &pc.command_name) {
                        t.push(owner);
                    }
                    t.extend(registry.catch_alls());
                    Some(t)
                }
                None => None,
            }
        };

        let senders = self.module_senders.lock().await;
        let mut ack_guard = self.ack_tracker.lock().await;
        let cfg = self.config_snapshot().await;

        let recipients: Vec<&String> = match &targets {
            Some(list) => list.iter().collect(),
            None => cfg.pre_process_modules.iter().collect(),
        };

        for module_name in recipients {
            if let Some(sender) = senders.get(module_name) {
                let _ = sender.send(container.clone()).await;
                ack_guard.track(
                    uuid7.to_string(),
                    "pre_process".into(),
                    module_name.clone(),
                    cfg.ack_timeout_ms,
                );
            }
        }

        Ok(())
    }

    async fn start_in_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Held-for-audit messages are not advanced or shown.
        if self.db.is_audited(&uuid7_string_to_bytes(uuid7)).await.unwrap_or(false) {
            return Ok(());
        }
        let state = {
            let mut states = self.pipeline_states.lock().await;
            if let Some(s) = states.get_mut(uuid7) {
                s.stage = PipelineStage::InProcessing;
                s.clone()
            } else {
                return Ok(());
            }
        };

        let cfg = self.config_snapshot().await;

        if state.current_in_process_index >= cfg.in_process_modules.len() {
            self.start_post_process(uuid7).await?;
            return Ok(());
        }

        let module_name = &cfg.in_process_modules[state.current_in_process_index];

        let container = Container {
            version: 1,
            auth_token: String::new(),
            module_name: "cockatiel".into(),
            module_instance_uuid7: String::new(),
            payload: Some(Payload::MessageInProcess(
                cockatiel_protobuf::MessageInProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: vec![],
                        raw_message: state.raw_message.clone(),
                        user_uuid7: state.user_uuid7.clone(),
                        command: None,
                        channel_id: state.channel_id.clone(),
                        user_data: state.user_data.clone(),
                    }),
                    processed_message: state.processed_message.clone(),
                    abandon_message: false,
                    audio: Vec::new(),
                    audio_type: String::new(),
                },
            )),
        };

        let senders = self.module_senders.lock().await;
        let mut ack_guard = self.ack_tracker.lock().await;

        if let Some(sender) = senders.get(module_name) {
            let _ = sender.send(container).await;
            ack_guard.track(
                uuid7.to_string(),
                "in_process".into(),
                module_name.clone(),
                cfg.ack_timeout_ms,
            );
        }

        Ok(())
    }

    async fn start_post_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Held-for-audit messages are not advanced or shown.
        if self.db.is_audited(&uuid7_string_to_bytes(uuid7)).await.unwrap_or(false) {
            return Ok(());
        }
        let state = {
            let mut states = self.pipeline_states.lock().await;
            if let Some(s) = states.get_mut(uuid7) {
                s.stage = PipelineStage::PostProcessing;
                s.clone()
            } else {
                return Ok(());
            }
        };

        // Audio created by a pre/in-process module flows forward to the
        // post-process modules (displays). Audio created AT the post-process
        // stage is only saved (persisted above) — never re-sent to modules.
        let (audio, audio_type) = if state.audio_stage == "post" || state.audio.is_empty() {
            (Vec::new(), String::new())
        } else {
            (state.audio.clone(), state.audio_type.clone())
        };

        let container = Container {
            version: 1,
            auth_token: String::new(),
            module_name: "cockatiel".into(),
            module_instance_uuid7: String::new(),
            payload: Some(Payload::MessagePostProcess(
                cockatiel_protobuf::MessagePostProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: vec![],
                        raw_message: state.raw_message.clone(),
                        user_uuid7: state.user_uuid7.clone(),
                        command: None,
                        channel_id: state.channel_id.clone(),
                        user_data: state.user_data.clone(),
                    }),
                    processed_message: state.processed_message.clone(),
                    audio,
                    audio_type,
                },
            )),
        };

        let senders = self.module_senders.lock().await;
        let mut ack_guard = self.ack_tracker.lock().await;
        let cfg = self.config_snapshot().await;

        let mut sent_any = false;
        for module_name in &cfg.post_process_modules {
            if let Some(sender) = senders.get(module_name) {
                let _ = sender.send(container.clone()).await;
                ack_guard.track(
                    uuid7.to_string(),
                    "post_process".into(),
                    module_name.clone(),
                    cfg.ack_timeout_ms,
                );
                sent_any = true;
            }
        }

        // If no post-process module is actually connected, there is nobody to
        // ack — complete the message now instead of leaving it stuck forever.
        if cfg.post_process_modules.is_empty() || !sent_any {
            drop(ack_guard);
            drop(senders);
            self.mark_complete(uuid7).await?;
        }

        Ok(())
    }

    async fn mark_complete(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        // Held-for-audit messages stay held, not completed.
        if self.db.is_audited(&uuid7_bytes).await.unwrap_or(false) {
            return Ok(());
        }

        // Persist the final processed text (what the pipeline produced) before
        // dropping the in-memory state. Without this, a message that no module
        // explicitly modified completes with a NULL processed_message even
        // though it went through the whole pipeline.
        {
            let states = self.pipeline_states.lock().await;
            if let Some(state) = states.get(uuid7) {
                self.db
                    .set_processed_message(&uuid7_bytes, &state.processed_message)
                    .await?;
            }
        }

        {
            let mut states = self.pipeline_states.lock().await;
            states.remove(uuid7);
        }

        self.db.update_stage_completed(&uuid7_bytes, "post_process").await?;
        // A message is only truly persisted when the whole pipeline has run;
        // record when that happened (the `persisted_at` column is otherwise
        // never populated for normally-completed messages).
        self.db.update_stage_completed(&uuid7_bytes, "persisted").await?;
        self.db.set_pipeline_status(&uuid7_bytes, "complete").await?;

        let mut ack_guard = self.ack_tracker.lock().await;
        ack_guard.ack(uuid7);

        Ok(())
    }

    async fn mark_failed(
        &self,
        uuid7: &str,
        error: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        {
            let mut states = self.pipeline_states.lock().await;
            states.remove(uuid7);
        }

        self.db.set_error(&uuid7_bytes, error).await?;

        let mut ack_guard = self.ack_tracker.lock().await;
        ack_guard.ack(uuid7);

        Ok(())
    }

    pub async fn handle_ack(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config_snapshot().await;
        let completed_stage = {
            let mut ack_guard = self.ack_tracker.lock().await;
            let acks = ack_guard.ack(uuid7);
            acks.first().map(|a| a.stage.clone())
        };

        let stage = match completed_stage {
            Some(s) => s,
            None => return Ok(()),
        };

        let still_pending = {
            let ack_guard = self.ack_tracker.lock().await;
            ack_guard.has_pending(uuid7)
        };

        match stage.as_str() {
            "pre_process" => {
                if !still_pending {
                    let uuid7_bytes = uuid7_string_to_bytes(uuid7);
                    self.db.update_stage_completed(&uuid7_bytes, "pre_process").await?;
                    self.start_in_process(uuid7).await?;
                } else {
                }
            }
            "in_process" => {
                let uuid7_bytes = uuid7_string_to_bytes(uuid7);
                self.db.update_stage_completed(&uuid7_bytes, "in_process").await?;

                let next_index = {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(uuid7) {
                        state.current_in_process_index += 1;
                        state.current_in_process_index
                    } else {
                        return Ok(());
                    }
                };

                if next_index >= cfg.in_process_modules.len() {
                    self.start_post_process(uuid7).await?;
                } else {
                    self.start_in_process(uuid7).await?;
                }
            }
            "post_process" => {
                if !still_pending {
                    self.mark_complete(uuid7).await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn handle_timeout(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config_snapshot().await;
        let timed_out = {
            let mut ack_guard = self.ack_tracker.lock().await;
            ack_guard.check_timeouts()
        };

        for entry in timed_out {
            let is_critical = cfg.critical_modules.contains(&entry.module_name);

            if is_critical {
                self.mark_failed(
                    &entry.uuid7,
                    &format!("Critical module '{}' timed out at stage '{}'", entry.module_name, entry.stage),
                ).await?;
            } else {
                let still_pending = {
                    let ack_guard = self.ack_tracker.lock().await;
                    ack_guard.has_pending(&entry.uuid7)
                };

                if !still_pending {
                    match entry.stage.as_str() {
                        "pre_process" => {
                            let uuid7_bytes = uuid7_string_to_bytes(&entry.uuid7);
                            self.db.update_stage_completed(&uuid7_bytes, "pre_process").await?;
                            self.start_in_process(&entry.uuid7).await?;
                        }
                        "in_process" => {
                            let uuid7_bytes = uuid7_string_to_bytes(&entry.uuid7);
                            self.db.update_stage_completed(&uuid7_bytes, "in_process").await?;

                            let next_index = {
                                let mut states = self.pipeline_states.lock().await;
                                if let Some(state) = states.get_mut(&entry.uuid7) {
                                    state.current_in_process_index += 1;
                                    state.current_in_process_index
                                } else {
                                    continue;
                                }
                            };

                            if next_index >= cfg.in_process_modules.len() {
                                self.start_post_process(&entry.uuid7).await?;
                            } else {
                                self.start_in_process(&entry.uuid7).await?;
                            }
                        }
                        "post_process" => {
                            self.mark_complete(&entry.uuid7).await?;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    /// Record rendered audio for a message: persisted to the timeline (so the
    /// web UI can retrieve it) and carried on the pipeline state so audio from
    /// an earlier stage flows forward to post-process modules / displays.
    async fn store_audio(
        &self,
        uuid7: &str,
        stage: &str,
        audio_type: &str,
        audio: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if audio.is_empty() {
            return Ok(());
        }
        let mime = if audio_type.is_empty() { "audio/mpeg" } else { audio_type };
        {
            let mut states = self.pipeline_states.lock().await;
            if let Some(state) = states.get_mut(uuid7) {
                state.audio = audio.to_vec();
                state.audio_type = mime.to_string();
                state.audio_stage = stage.to_string();
            }
        }
        self.db.set_audio(&uuid7_string_to_bytes(uuid7), mime, audio).await?;
        Ok(())
    }

    pub async fn handle_message_from_module(
        &self,
        container: &Container,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match &container.payload {
            Some(Payload::MessagePreProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    // New message from an adapter (input) — ingest it into the
                    // pipeline with a fresh uuid7. This is the DB-as-queue entry.
                    if let Some(chat) = &msg.raw_message {
                        let msg_in = PipelineMessage {
                            uuid7: uuid7_string().unwrap_or_default(),
                            platform: chat.platform.clone(),
                            event_type: 1,
                            raw_message: chat.raw_message.clone(),
                            command: None,
                            flags: None,
                            parsed_command: chat.command.clone(),
                            channel_id: chat.channel_id.clone(),
                            user_uuid7: chat.user_uuid7.clone(),
                            user_data: chat.user_data.clone(),
                        };
                        self.insert_and_start(msg_in).await?;
                        return Ok(true);
                    }
                    return Ok(true);
                }

                let processed = msg.raw_message.as_ref()
                    .map(|cm| cm.raw_message.clone())
                    .unwrap_or_default();

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = processed.clone();
                    }
                }

                let uuid7_bytes = uuid7_string_to_bytes(&msg.message_uuid7);
                self.db.set_processed_message(&uuid7_bytes, &processed).await?;
                self.store_audio(&msg.message_uuid7, "pre", &msg.audio_type, &msg.audio).await?;
                self.handle_ack(&msg.message_uuid7).await?;
                Ok(true)
            }
            Some(Payload::MessageInProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    return Ok(true);
                }

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = msg.processed_message.clone();
                    }
                }

                let uuid7_bytes = uuid7_string_to_bytes(&msg.message_uuid7);
                self.db.set_processed_message(&uuid7_bytes, &msg.processed_message).await?;
                self.store_audio(&msg.message_uuid7, "in", &msg.audio_type, &msg.audio).await?;
                self.handle_ack(&msg.message_uuid7).await?;
                Ok(true)
            }
            Some(Payload::MessagePostProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    return Ok(true);
                }

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = msg.processed_message.clone();
                    }
                }

                let uuid7_bytes = uuid7_string_to_bytes(&msg.message_uuid7);
                self.db.set_processed_message(&uuid7_bytes, &msg.processed_message).await?;
                self.store_audio(&msg.message_uuid7, "post", &msg.audio_type, &msg.audio).await?;
                self.handle_ack(&msg.message_uuid7).await?;
                Ok(true)
            }
            Some(Payload::MessageAck(ack)) => {
                if ack.message_uuid7.is_empty() {
                    return Ok(true);
                }
                self.handle_ack(&ack.message_uuid7).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
