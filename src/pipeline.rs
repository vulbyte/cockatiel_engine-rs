use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};

use crate::cockatiel_protobuf;
use crate::cockatiel_protobuf::{Container, container::Payload, ChatMessage, Command};
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseManager, PipelineOutcome, PipelineResult};

const DEFAULT_ACK_TIMEOUT_MS: u64 = 3000;

/// How often the ack-timeout sweep runs. This bounds how long a stage waits once
/// its ack budget has expired, so it must stay well below [`DEFAULT_ACK_TIMEOUT_MS`]
/// rather than riding on a multi-second housekeeping interval. It used to run on
/// the 15s DB-sync loop, which quantised every timed-out stage to a 15s grid
/// (measured p50 15s / p90 45s per message, on 15s and 30s boundaries).
pub const TIMEOUT_SWEEP_INTERVAL: Duration = Duration::from_millis(250);

fn uuid7_string_to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// The outcome of a bounded, non-wedging send to a module. Distinguishes a
/// module that simply isn't connected (no sender slot) from a connected module
/// whose outbound queue is full (the container was dropped) — so callers can
/// tell a genuine drop apart from a missing peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    /// A sender existed and the container was queued (try_send or timed send).
    Sent,
    /// No sender exists for the module (it is not connected).
    NotConnected,
    /// A sender existed but its channel was full and the timed send fell
    /// through — the container was dropped.
    Dropped,
}

/// Bounded, non-wedging send to a connected module. Clones the sender out of
/// the shared `module_senders` lock (so the lock is never held across the
/// send), `try_send`s first, and only falls back to a short timeout send when
/// the channel is full — a module that isn't draining its socket must not
/// stall the liveness watchdog or the routing lock for every other module.
/// A missing sender slot yields `NotConnected`; a closed channel is treated as
/// not connected too (the module is gone even if its slot lingers); a full
/// channel that never frees up yields `Dropped`.
pub(crate) async fn send_to_module(
    senders: &Arc<Mutex<HashMap<String, mpsc::Sender<Container>>>>,
    module_name: &str,
    container: Container,
    label: &str,
) -> SendOutcome {
    let sender = {
        let senders = senders.lock().await;
        senders.get(module_name).cloned()
    };
    let Some(sender) = sender else {
        return SendOutcome::NotConnected;
    };
    // A closed channel means the module's socket is gone even though its sender
    // slot still lingers in the map — report NotConnected, not Dropped.
    if sender.is_closed() {
        return SendOutcome::NotConnected;
    }
    if sender.try_send(container.clone()).is_ok() {
        return SendOutcome::Sent;
    }
    if tokio::time::timeout(Duration::from_millis(1000), sender.send(container)).await.is_ok() {
        return SendOutcome::Sent;
    }
    eprintln!(
        "[engine] dropped send to '{}' ({}) — channel full >1s",
        module_name, label
    );
    SendOutcome::Dropped
}

fn uuid7_string() -> Option<String> {
    Some(uuid::Uuid::now_v7().to_string())
}

/// Milliseconds since the Unix epoch — the unit the timeline row's stage
/// timestamps use. Captured in memory as each stage finishes, because the row is
/// written once at the end of the chain rather than once per stage.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The dispatch a message was about to make when a pause landed, and did not.
///
/// A held message is NOT lost and NOT failed: its timeline row is written, its
/// in-memory state is live, and the only thing missing is the send. Resume
/// replays the held dispatch from the point recorded here, so the chain picks
/// up exactly where it stopped — a module is never sent the same message twice
/// by the replay, because the ack that produced the hold already consumed the
/// tracker's entry for that stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldPoint {
    /// Before the pre-process fanout: the entry gate. The row is written and
    /// the state is live, but nothing has reached any module yet.
    Entry,
    /// The next in-process module has not been sent to (pre-process is done).
    InProcess,
    /// The post-process fanout has not happened (in-process is done).
    PostProcess,
}

/// The engine's pause state, and the messages a pause is holding.
///
/// `held` is keyed by uuid7 in a `BTreeMap` so the resume order is uuid7 order,
/// which is time order for uuid7s — the same ordering the recovery drain gets
/// from `get_queued_uuids`, and the same ordering the messages arrived in.
#[derive(Debug, Default)]
struct PauseState {
    paused: bool,
    held: BTreeMap<String, HoldPoint>,
}

/// What a resume did, for the control surface's response and the log line.
#[derive(Debug, Default)]
pub struct ResumeOutcome {
    /// Was the engine actually paused? A resume while it was already running is
    /// a no-op and says so, rather than claiming to have drained anything.
    pub was_paused: bool,
    /// How many held messages are being released (see [`PipelineOrchestrator::resume`]).
    pub resumed_messages: usize,
    /// The post-resume timeout sweep failing. Empty on a clean resume; a
    /// message whose replay fails later is logged, not returned.
    pub errors: Vec<String>,
}

/// What one pass of the crash-recovery drain did with the stranded rows it
/// found. Reported so the drain can be logged without re-querying the database.
#[derive(Debug, Default)]
pub struct RecoveryDrain {
    /// How many 'queued' rows the pass looked at.
    pub considered: usize,
    /// How many of them this pass actually drove into the pipeline. A row the
    /// live pipeline already owns is NOT counted here — it was correctly
    /// skipped, and counting it would report a drain that did not happen.
    pub claimed: usize,
    /// `(uuid7, error)` for every row the pass could not drive.
    pub failures: Vec<(String, String)>,
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

/// A module's rolling per-message processing latency.
///
/// The last `WINDOW` samples are kept so the reported average reflects the
/// module's CURRENT speed (an operator watching the modules window sees a
/// slowdown within a few messages and can kill the offender) rather than a
/// since-boot mean. `Window` is 8: the engine's ack timeout is 3s, so 8
/// messages is a small enough window to stay responsive to a regression while
/// still smoothing single-message jitter.
#[derive(Debug, Clone, Default)]
pub struct ModuleTiming {
    /// The most recent samples, oldest first, capped at [`WINDOW`].
    samples: Vec<f64>,
    /// The average of the current samples (ms). Kept so a zero-sample module
    /// still reports a definite 0 rather than a blank.
    pub avg_ms: f64,
}

impl ModuleTiming {
    /// How many recent messages make up the rolling average.
    pub const WINDOW: usize = 8;

    /// Record one message's processing time (ms). Keeps only the last
    /// [`WINDOW`] samples and re-derives the average.
    pub fn record(&mut self, elapsed_ms: f64) {
        self.samples.push(elapsed_ms);
        if self.samples.len() > Self::WINDOW {
            let overflow = self.samples.len() - Self::WINDOW;
            self.samples.drain(0..overflow);
        }
        let sum: f64 = self.samples.iter().sum();
        self.avg_ms = sum / self.samples.len() as f64;
    }
}

/// Per-module rolling latencies, keyed by module name.
#[derive(Debug, Clone, Default)]
pub struct ModuleTimings {
    by_module: HashMap<String, ModuleTiming>,
}

impl ModuleTimings {
    /// Record a processing time for `module_name`, creating its window on first
    /// use.
    pub fn record(&mut self, module_name: &str, elapsed_ms: f64) {
        self.by_module
            .entry(module_name.to_string())
            .or_default()
            .record(elapsed_ms);
    }

    /// The current rolling average (ms) for `module_name`, or `None` if the
    /// module has not completed a message yet.
    pub fn avg_ms(&self, module_name: &str) -> Option<f64> {
        self.by_module.get(module_name).map(|t| t.avg_ms)
    }

    /// Snapshot of every recorded module's current average (ms).
    pub fn all_avgs(&self) -> HashMap<String, f64> {
        self.by_module
            .iter()
            .map(|(n, t)| (n.clone(), t.avg_ms))
            .collect()
    }
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

    /// Remove ONLY the acking module's pending entry for `uuid7` (a stage with
    /// several modules must wait for ALL of them to ack, not advance on the
    /// first). Returns the stage that was acked AND the processing duration
    /// (ms since the send) — the duration is what feeds the module's rolling
    /// latency average. `None` if this module had no pending ack for the message.
    pub fn ack_module(&mut self, uuid7: &str, module_name: &str) -> Option<(String, f64)> {
        let now = Instant::now();
        let (stage, elapsed_ms) = {
            let entries = self.pending.get(uuid7)?;
            let entry = entries.iter().find(|e| e.module_name == module_name)?;
            let elapsed_ms = now.duration_since(entry.sent_at).as_secs_f64() * 1000.0;
            (entry.stage.clone(), elapsed_ms)
        };
        if let Some(entries) = self.pending.get_mut(uuid7) {
            entries.retain(|e| e.module_name != module_name);
            if entries.is_empty() {
                self.pending.remove(uuid7);
            }
        }
        Some((stage, elapsed_ms))
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

    /// Restart every pending ack's clock to now.
    ///
    /// Called on the resume from a pause, and it is the whole reason a pause
    /// cannot lose a message. `sent_at` is wall-clock, so a message that was
    /// mid-chain when the pause landed has already "spent" its ack budget by
    /// the time the engine resumes — ten minutes paused would time out and
    /// FAIL every message that was in flight, which is the exact opposite of
    /// what pausing is for. Restarting the clock hands each in-flight message
    /// its full budget from the moment processing resumes, which is what
    /// "paused time does not count against a message" has to mean in practice.
    ///
    /// Paired with the gate in [`PipelineOrchestrator::handle_timeout`] that
    /// stops the sweep entirely while paused: the entries are not advanced,
    /// just re-based, so nothing is failed and nothing is spuriously kept
    /// alive either.
    pub fn restart_clocks(&mut self) {
        let now = Instant::now();
        for entries in self.pending.values_mut() {
            for entry in entries.iter_mut() {
                entry.sent_at = now;
            }
        }
    }

    /// Test-only: inject pending entries with arbitrary `sent_at`/`timeout` so
    /// timeout semantics can be exercised deterministically without sleeping.
    #[cfg(test)]
    pub(crate) fn inject(&mut self, uuid7: String, entries: Vec<PendingAck>) {
        self.pending.insert(uuid7, entries);
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
    /// When `pre_process` finished (its last module acked), ms since the epoch.
    /// Captured here instead of written per stage: the row gets it with the
    /// single terminal write, and the value is the same either way.
    pub pre_process_completed_at: Option<i64>,
    /// When `in_process` finished, ms since the epoch. Captured, not written.
    pub in_process_completed_at: Option<i64>,
    /// Every `audio_type=<mime>` marker the run produced, in order. The row
    /// stores the content type in `flags` and the per-stage `set_audio` calls
    /// appended a marker each time, so the terminal write has to append exactly
    /// the same set.
    pub audio_type_markers: Vec<String>,
}

#[derive(Clone)]
pub struct PipelineOrchestrator {
    pub db: DatabaseManager,
    pub ack_tracker: Arc<Mutex<AckTracker>>,
    pub pipeline_states: Arc<Mutex<HashMap<String, PipelineState>>>,
    pub config: Arc<Mutex<PipelineConfig>>,
    pub module_senders: Arc<Mutex<HashMap<String, mpsc::Sender<Container>>>>,
    pub command_registry: Arc<std::sync::Mutex<CommandRegistry>>,
    /// The operator pause and the messages it is holding.
    ///
    /// Deliberately NOT seeded from config here: the engine picks its boot
    /// state in `main` (boots PAUSED unless the headless escape hatch says
    /// otherwise) and sets it explicitly, so the pipeline primitive itself
    /// starts neutral and gating is a property of a running engine, not of
    /// constructing an orchestrator.
    pause_state: Arc<Mutex<PauseState>>,
    /// Rolling per-module processing latencies, fed by `handle_ack` and read by
    /// `module_list` (the TUI's 2s poll) to show each module's current average
    /// time and the per-stage sums.
    pub module_timings: Arc<Mutex<ModuleTimings>>,
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
            pause_state: Arc::new(Mutex::new(PauseState::default())),
            module_timings: Arc::new(Mutex::new(ModuleTimings::default())),
        }
    }

    // ── The pause ──────────────────────────────────────────────────────
    //
    // Pausing holds DISPATCHES, not messages. Ingest keeps running, the
    // timeline row is still written, commands are still parsed: an incoming
    // message while paused is persisted as 'queued' and joins the held set, so
    // nothing is lost and resume is just "work through a bigger queue".

    /// Is the pipeline holding every dispatch to every module?
    pub async fn is_paused(&self) -> bool {
        self.pause_state.lock().await.paused
    }

    /// How many messages are waiting on a resume, for the control surface.
    pub async fn held_count(&self) -> usize {
        self.pause_state.lock().await.held.len()
    }

    /// Hold every dispatch from now on. Returns false if already paused.
    ///
    /// In-flight messages are NOT touched: whatever stage a message has reached
    /// keeps its state and its pending acks, and the ack-timeout sweep stops
    /// (see [`Self::handle_timeout`]), so a pause can never fail a message that
    /// was already moving. Only the NEXT dispatch of each chain is held.
    pub async fn pause(&self) -> bool {
        let mut state = self.pause_state.lock().await;
        if state.paused {
            return false;
        }
        state.paused = true;
        true
    }

    /// The one gate every dispatch point asks before it touches a module's
    /// socket. `true` means HELD — the caller must return without dispatching.
    ///
    /// `uuid7`/`point` are recorded so [`Self::resume`] can replay exactly the
    /// dispatch that was skipped. One message is held at one point: the ack that
    /// advanced it to that point consumed the tracker's entry, so no second ack
    /// can advance the same stage and re-hold it.
    async fn hold_if_paused(&self, uuid7: &str, point: HoldPoint) -> bool {
        let mut state = self.pause_state.lock().await;
        if !state.paused {
            return false;
        }
        state.held.insert(uuid7.to_string(), point);
        true
    }

    /// Release the pause and resume completely normal operation, working
    /// through whatever accumulated while it was held. Three steps, and the
    /// order is the point:
    ///
    /// 1. Every pending ack's clock restarts. A message that was mid-chain when
    ///    the pause landed was waiting on a module that had no chance to be told
    ///    to answer, so the wall-clock spent paused must NOT count against its
    ///    ack budget. Without this, a ten-minute pause would time out and FAIL
    ///    every message that was in flight — data loss caused by pausing.
    /// 2. The timeout sweep runs, now that the engine is running again: a stage
    ///    whose module really is gone still times out, but from a rebased clock.
    /// 3. The held backlog is released — in a background task, in uuid7 order.
    ///
    /// Step 3 is detached deliberately. A ten-minute pause can accumulate a
    /// backlog of thousands of messages, and each replay costs a database read
    /// plus a bounded send (a full module channel costs up to a second, by
    /// design). Doing that inline on the control surface's read loop would stall
    /// it for long enough for the liveness probe to declare the OPERATOR'S OWN
    /// TUI unresponsive and kill it — the resume would break the thing that
    /// asked for it. The engine is running again before this returns; the
    /// backlog is released behind it, and each replay re-checks the gate, so a
    /// pause that lands again mid-release simply re-holds what it had not
    /// reached yet.
    ///
    /// The caller owns the log line and the query response, so the backlog
    /// replay logs its own failures here rather than returning them.
    pub async fn resume(&self) -> ResumeOutcome {
        let (was_paused, held) = {
            let mut state = self.pause_state.lock().await;
            let was_paused = state.paused;
            let held: Vec<(String, HoldPoint)> = state
                .held
                .iter()
                .map(|(uuid7, point)| (uuid7.clone(), *point))
                .collect();
            state.held.clear();
            state.paused = false;
            (was_paused, held)
        };

        // (1) Paused time is nobody's fault but the clock's.
        self.ack_tracker.lock().await.restart_clocks();

        // (2) The sweep, now that there is a live budget left to expire. It is
        // in here, before the backlog, so a module that really has died still
        // times out against a message the backlog is about to re-drive.
        let mut errors = Vec::new();
        if let Err(e) = self.handle_timeout().await {
            errors.push(format!("timeout sweep failed after resume: {}", e));
        }

        // (3) Release the backlog behind the caller's back. Ordered, and one
        // task, so the order the messages accumulated in is the order they go
        // back out in.
        let released = held.len();
        if !held.is_empty() {
            let orchestrator = self.clone();
            tokio::spawn(async move {
                for (uuid7, point) in &held {
                    if let Err(e) = orchestrator.replay_hold(uuid7, *point).await {
                        eprintln!("[pipeline] resume: could not replay {}: {}", uuid7, e);
                    }
                }
            });
        }

        ResumeOutcome {
            was_paused,
            resumed_messages: released,
            errors,
        }
    }

    /// Re-issue the dispatch a pause held, from the point it was held at.
    async fn replay_hold(
        &self,
        uuid7: &str,
        point: HoldPoint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match point {
            HoldPoint::Entry => self.broadcast_pre_process(uuid7).await,
            HoldPoint::InProcess => self.start_in_process(uuid7).await,
            HoldPoint::PostProcess => self.start_post_process(uuid7).await,
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
            pre_process_completed_at: None,
            in_process_completed_at: None,
            audio_type_markers: Vec::new(),
        };

        if !msg.user_uuid7.is_empty() {
            self.db.set_user_uuid(&uuid7_bytes, &msg.user_uuid7).await?;
        }

        {
            let mut states = self.pipeline_states.lock().await;
            states.insert(msg.uuid7.clone(), state);
        }

        // The row is claimed by the in-memory state, not by a status write: it
        // stays 'queued' until the single terminal write at the end of the
        // chain, and `recover_one` checks the state map to avoid re-broadcasting
        // a message that is already in flight.
        self.broadcast_pre_process(&msg.uuid7).await?;

        Ok(())
    }

    /// Re-process a message that was stranded in the queue: a crash between the
    /// timeline insert and the first broadcast left the row 'queued' with no
    /// live task ever going to drain it. Reloads the row, rebuilds a fresh
    /// pipeline state and runs it through pre-process.
    /// NOTE: a recovered message loses `channel_id`, `user_data` and the
    /// engine-parsed `command` — those are never persisted on the timeline row,
    /// so recovery cannot reconstruct them. Messages that pass through a
    /// command flag still classify on ingest (if an adapter re-sends them); a
    /// recovered row only has the stored raw message.
    ///
    /// Returns whether THIS call claimed the message. `false` means it left an
    /// existing one alone (already in flight, or the row is no longer 'queued')
    /// — the caller needs the distinction to report a drain honestly.
    ///
    /// While the pipeline is paused this claims the row and then holds at the
    /// entry gate: recovery is not skipped, it is deferred to the resume like
    /// any other dispatch.
    pub async fn recover_one(&self, uuid7: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // The claim check the database used to do. A row in flight is 'queued'
        // now (the pipeline writes no intermediate status), so `load_queued_message`
        // can no longer tell a live message from a stranded one — the state map
        // is the claim registry, and a message already in it must not be
        // re-broadcast or have its state overwritten.
        {
            let states = self.pipeline_states.lock().await;
            if states.contains_key(uuid7) {
                // Already claimed by a live pipeline — nothing to do.
                return Ok(false);
            }
        }

        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        let Some(msg) = self.db.load_queued_message(&uuid7_bytes).await? else {
            // Row missing or already driven past 'queued' by a terminal write.
            return Ok(false);
        };

        let state = PipelineState {
            uuid7: uuid7.to_string(),
            stage: PipelineStage::PreProcessing,
            current_in_process_index: 0,
            raw_message: msg.raw_message.clone(),
            processed_message: msg.raw_message.clone(),
            platform: msg.platform.clone(),
            event_type: msg.event_type,
            command: if msg.command.is_empty() { None } else { Some(msg.command.clone()) },
            flags: if msg.flags.is_empty() { None } else { Some(msg.flags.clone()) },
            parsed_command: None,
            channel_id: String::new(),
            user_uuid7: msg.user_uuid7.clone(),
            user_data: None,
            audio: Vec::new(),
            audio_type: String::new(),
            audio_stage: String::new(),
            pre_process_completed_at: None,
            in_process_completed_at: None,
            audio_type_markers: Vec::new(),
        };

        {
            let mut states = self.pipeline_states.lock().await;
            states.insert(uuid7.to_string(), state);
        }

        self.broadcast_pre_process(uuid7).await?;

        Ok(true)
    }

    /// ONE terminating pass of the crash-recovery drain: read the stranded
    /// 'queued' uuids, then re-drive each of them.
    ///
    /// This is deliberately single-pass, and that is a correctness fix, not a
    /// simplification. It used to loop until `get_queued_uuids` came back
    /// empty, which was only safe while a started message moved its row to
    /// 'processing'. It no longer does anything of the sort: a message the
    /// pipeline owns stays 'queued' for its whole flight, because the only
    /// status write is the terminal one
    /// ([`DatabaseManager::write_terminal_outcome`]). So the next iteration
    /// re-selected the row the previous one had just re-driven, `recover_one`
    /// returned early because the message was already in `pipeline_states`, the
    /// row was still 'queued' — and the loop spun on SELECTs against the
    /// timeline DB forever.
    ///
    /// One pass is also COMPLETE, not just terminating: every stranded row is
    /// in the snapshot this reads (rows are only ever inserted 'queued', so a
    /// row inserted after the snapshot is a message the live pipeline owns and
    /// drives itself), and a row this pass could not drive is handed back to
    /// the caller to log rather than retried in a tight loop.
    pub async fn drain_queued_once(&self) -> Result<RecoveryDrain, Box<dyn std::error::Error>> {
        let uuids = self.db.get_queued_uuids().await?;
        let mut drain = RecoveryDrain {
            considered: uuids.len(),
            ..Default::default()
        };
        for uuid7 in uuids {
            match self.recover_one(&uuid7).await {
                Ok(true) => drain.claimed += 1,
                Ok(false) => {}
                Err(e) => drain.failures.push((uuid7, e.to_string())),
            }
        }
        Ok(drain)
    }

    async fn broadcast_pre_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 1 of 3 — the entry gate. By the time a message reaches
        // here its timeline row is written and its state is live, so the pause
        // holds the CHAIN, not the message: nothing is lost, and resume replays
        // this same fanout from `HoldPoint::Entry`. Both entries into the chain
        // funnel through here (live ingest and crash recovery), so one gate
        // covers both.
        if self.hold_if_paused(uuid7, HoldPoint::Entry).await {
            return Ok(());
        }

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

        let mut ack_guard = self.ack_tracker.lock().await;
        let cfg = self.config_snapshot().await;

        let recipients: Vec<&String> = match &targets {
            Some(list) => list.iter().collect(),
            None => cfg.pre_process_modules.iter().collect(),
        };
        let recipients_empty = recipients.is_empty();

        let mut sent_any = false;
        for module_name in recipients {
            match send_to_module(&self.module_senders, module_name, container.clone(), "pre_process").await {
                SendOutcome::Sent => {
                    sent_any = true;
                    ack_guard.track(
                        uuid7.to_string(),
                        "pre_process".into(),
                        module_name.clone(),
                        cfg.ack_timeout_ms,
                    );
                }
                SendOutcome::Dropped => {
                    eprintln!(
                        "[engine] dropped send to '{}' (pre_process) — channel full >1s",
                        module_name
                    );
                }
                SendOutcome::NotConnected => {}
            }
        }
        drop(ack_guard);

        // No pre-process module is connected — there is nobody to ack, so
        // advance immediately instead of leaving the message stuck in
        // `pipeline_states` forever (a leak + a perpetually 'processing' row).
        if recipients_empty || !sent_any {
            self.start_in_process(uuid7).await?;
        }

        Ok(())
    }

    async fn start_in_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 2 of 3. Hold rather than dispatch: the message keeps its
        // state and its `current_in_process_index`, so the replay sends it to the
        // module it was about to reach — and the audit check, the stage stamp
        // and the send all wait for the resume rather than half-happening.
        if self.hold_if_paused(uuid7, HoldPoint::InProcess).await {
            return Ok(());
        }

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

        let sent = send_to_module(&self.module_senders, module_name, container, "in_process").await;
        match sent {
            SendOutcome::Sent => {
                self.ack_tracker.lock().await.track(
                    uuid7.to_string(),
                    "in_process".into(),
                    module_name.clone(),
                    cfg.ack_timeout_ms,
                );
            }
            _ => {
                // The configured in-process module isn't connected (or its
                // channel is full — nothing will ever ack this stage). Skip to
                // the next stage so the message doesn't hang in
                // `pipeline_states` forever.
                if sent == SendOutcome::Dropped {
                    eprintln!(
                        "[engine] dropped send to '{}' (in_process) — channel full >1s",
                        module_name
                    );
                }
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
                    Box::pin(self.start_in_process(uuid7)).await?;
                }
            }
        }

        Ok(())
    }

    async fn start_post_process(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 3 of 3 — same rule as the in-process gate: hold, keep the
        // state, let the replay finish it. Note this also covers the
        // no-module-connected completion below, so a message cannot reach a
        // terminal 'complete' write while the pipeline is held.
        if self.hold_if_paused(uuid7, HoldPoint::PostProcess).await {
            return Ok(());
        }

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

        let cfg = self.config_snapshot().await;

        let mut sent_any = false;
        for module_name in &cfg.post_process_modules {
            match send_to_module(&self.module_senders, module_name, container.clone(), "post_process").await {
                SendOutcome::Sent => {
                    self.ack_tracker.lock().await.track(
                        uuid7.to_string(),
                        "post_process".into(),
                        module_name.clone(),
                        cfg.ack_timeout_ms,
                    );
                    sent_any = true;
                }
                SendOutcome::Dropped => {
                    eprintln!(
                        "[engine] dropped send to '{}' (post_process) — channel full >1s",
                        module_name
                    );
                }
                SendOutcome::NotConnected => {}
            }
        }

        // If no post-process module is actually connected, there is nobody to
        // ack — complete the message now instead of leaving it stuck forever.
        if cfg.post_process_modules.is_empty() || !sent_any {
            self.mark_complete(uuid7).await?;
        }

        Ok(())
    }

    /// The accumulated result of a message's run, in the shape the single
    /// terminal write takes.
    ///
    /// This IS the record while the message is in flight: the row has not been
    /// written since the ingest INSERT, so everything the per-stage writes used
    /// to mirror column-by-column is read straight off the state here and lands
    /// in one statement. `post_process_completed_at` / `persisted_at` are
    /// deliberately absent — the terminal write stamps those itself, and only
    /// for a message that completed.
    fn result_of(state: &PipelineState) -> PipelineResult {
        PipelineResult {
            // Always present: the state seeds it with the raw message, so a
            // message no module touched still persists its text.
            processed_message: Some(state.processed_message.clone()),
            pre_process_completed_at: state.pre_process_completed_at,
            in_process_completed_at: state.in_process_completed_at,
            audio: if state.audio.is_empty() {
                None
            } else {
                Some((state.audio_type.clone(), state.audio.clone()))
            },
            audio_type_markers: state.audio_type_markers.clone(),
        }
    }

    /// The accumulated result for `uuid7`, leaving the state in place.
    async fn result_snapshot(&self, uuid7: &str) -> PipelineResult {
        let states = self.pipeline_states.lock().await;
        states.get(uuid7).map(Self::result_of).unwrap_or_default()
    }

    /// The accumulated result for `uuid7`, dropping the in-memory state: a
    /// terminal message is finished, so nothing more should be able to change
    /// it. `None` when there is no state (a late duplicate from a module that
    /// replied after the message was already terminal).
    async fn take_result(&self, uuid7: &str) -> Option<PipelineResult> {
        let mut states = self.pipeline_states.lock().await;
        states.remove(uuid7).map(|s| Self::result_of(&s))
    }

    /// Stamp `pre_process_completed_at` IN MEMORY. The timestamp is carried out
    /// by the message's single terminal write, so the database sees no
    /// per-stage UPDATE while the message is in flight — and the value is the
    /// same one the per-stage write used to record.
    async fn note_pre_process_completed(&self, uuid7: &str) {
        let now = now_ms();
        let mut states = self.pipeline_states.lock().await;
        if let Some(state) = states.get_mut(uuid7) {
            state.pre_process_completed_at = Some(now);
        }
    }

    /// [`Self::note_pre_process_completed`] for the in-process stage.
    async fn note_in_process_completed(&self, uuid7: &str) {
        let now = now_ms();
        let mut states = self.pipeline_states.lock().await;
        if let Some(state) = states.get_mut(uuid7) {
            state.in_process_completed_at = Some(now);
        }
    }

    async fn mark_complete(
        &self,
        uuid7: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        // Held-for-audit messages stay held, not completed. The run's
        // accumulated result still lands on the row (the per-stage writes used
        // to leave it there), but the hold itself is untouched: only
        // `release_audit` may release it. The state and the pending acks are
        // left in place, exactly as this path always did.
        if self.db.is_audited(&uuid7_bytes).await.unwrap_or(false) {
            let held = self.result_snapshot(uuid7).await;
            self.db
                .write_terminal_outcome(&uuid7_bytes, &PipelineOutcome::AuditHeld, &held)
                .await?;
            return Ok(());
        }

        // Take the final processed text, audio and stage timestamps out of
        // memory (what the pipeline produced) and land them with the terminal
        // status in ONE write. Without the processed text a message that no
        // module explicitly modified would complete with a NULL
        // processed_message even though it went through the whole pipeline.
        let result = self.take_result(uuid7).await.unwrap_or_default();
        self.db
            .write_terminal_outcome(&uuid7_bytes, &PipelineOutcome::Complete, &result)
            .await?;

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

        // Everything the run got as far as producing lands with the failure —
        // the per-stage writes had already mirrored it onto the row by the time
        // the failure was recorded.
        let result = self.take_result(uuid7).await.unwrap_or_default();
        self.db
            .write_terminal_outcome(&uuid7_bytes, &PipelineOutcome::Failed(error.to_string()), &result)
            .await?;

        let mut ack_guard = self.ack_tracker.lock().await;
        ack_guard.ack(uuid7);

        Ok(())
    }

    /// A module handed the message back marked for abandonment
    /// (`MessageInProcess.abandon_message`). The flag has been on the wire — and
    /// plumbed through by modules — since v1, but the engine never read it, so
    /// the message carried on down the chain as if nothing had happened. It is a
    /// terminal outcome now: the chain stops here, the message's pending acks are
    /// cleared so a sibling module's late ack cannot restart it, and the row
    /// lands a 'dropped' outcome with everything the run had produced.
    ///
    /// A late/duplicate abandon for a message that already reached a terminal
    /// state is ignored: the row is the record, and only a message that is
    /// still in flight can be abandoned.
    async fn mark_dropped(
        &self,
        uuid7: &str,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(result) = self.take_result(uuid7).await else {
            return Ok(());
        };
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        self.db
            .write_terminal_outcome(&uuid7_bytes, &PipelineOutcome::Dropped(reason.to_string()), &result)
            .await?;

        let mut ack_guard = self.ack_tracker.lock().await;
        ack_guard.ack(uuid7);

        Ok(())
    }

    pub async fn handle_ack(
        &self,
        uuid7: &str,
        module_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config_snapshot().await;
        let completed_stage = {
            let mut ack_guard = self.ack_tracker.lock().await;
            ack_guard.ack_module(uuid7, module_name)
        };

        let (stage, elapsed_ms) = match completed_stage {
            Some(v) => v,
            None => return Ok(()),
        };

        // Feed the module's rolling latency average — this is the data the TUI
        // shows as each module's current ms. Only count real completed
        // messages (an ack is the module saying "done"), so the average is
        // purely how long a module takes on the messages it actually handled.
        self.module_timings.lock().await.record(module_name, elapsed_ms);

        let still_pending = {
            let ack_guard = self.ack_tracker.lock().await;
            ack_guard.has_pending(uuid7)
        };

        match stage.as_str() {
            "pre_process" => {
                if !still_pending {
                    self.note_pre_process_completed(uuid7).await;
                    self.start_in_process(uuid7).await?;
                }
            }
            "in_process" => {
                self.note_in_process_completed(uuid7).await;

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
        // PAUSE GATE (the sweep) — and the one that decides whether pausing can
        // ever LOSE a message. `sent_at` is wall-clock: a sweep that ran while
        // the engine was paused would find every in-flight stage long expired
        // and mark those messages FAILED, so a ten-minute pause would destroy
        // exactly the messages it was meant to be holding. So the sweep does not
        // advance at all while paused, and `resume` restarts every pending
        // ack's clock before running it — each in-flight message then gets its
        // FULL budget from the moment processing resumes, and one that really
        // has a dead module still times out.
        if self.is_paused().await {
            return Ok(());
        }

        let cfg = self.config_snapshot().await;
        let timed_out = {
            let mut ack_guard = self.ack_tracker.lock().await;
            ack_guard.check_timeouts()
        };

        for entry in timed_out {
            let is_critical = cfg.critical_modules.contains(&entry.module_name);
            // Name the stalling module: a stage waits for EVERY one of its
            // modules, so one silent module turns every message into a timeout
            // wait. Without this line the only symptom is "chat feels slow".
            eprintln!(
                "[pipeline] stage '{}' timed out waiting for '{}' after {}ms",
                entry.stage,
                entry.module_name,
                entry.timeout.as_millis()
            );

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
                            self.note_pre_process_completed(&entry.uuid7).await;
                            self.start_in_process(&entry.uuid7).await?;
                        }
                        "in_process" => {
                            self.note_in_process_completed(&entry.uuid7).await;

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

    /// Record rendered audio on the message: carried on the pipeline state so
    /// audio from an earlier stage flows forward to post-process modules /
    /// displays, and persisted to the timeline by the message's single terminal
    /// write (so the web UI can still retrieve it). The per-stage `set_audio` —
    /// a SELECT to read the flags back plus an UPDATE, for every stage that
    /// produced audio — is gone; the state carries the bytes and the content
    /// type marker until the one write at the end.
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
        let mut states = self.pipeline_states.lock().await;
        if let Some(state) = states.get_mut(uuid7) {
            state.audio = audio.to_vec();
            state.audio_type = mime.to_string();
            state.audio_stage = stage.to_string();
            // `set_audio` appended `audio_type=<mime>` to the row's flags on
            // every call and skipped a marker already there; record the same
            // markers so the terminal write appends the same flags string.
            let marker = format!("audio_type={}", mime);
            if !state.audio_type_markers.contains(&marker) {
                state.audio_type_markers.push(marker);
            }
        }
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
                        state.processed_message = processed;
                    }
                }

                self.store_audio(&msg.message_uuid7, "pre", &msg.audio_type, &msg.audio).await?;
                self.handle_ack(&msg.message_uuid7, &container.module_name).await?;
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

                self.store_audio(&msg.message_uuid7, "in", &msg.audio_type, &msg.audio).await?;

                // The module handed the message back marked for abandonment: the
                // chain ends here instead of continuing to the next stage.
                if msg.abandon_message {
                    self.mark_dropped(
                        &msg.message_uuid7,
                        &format!("abandoned by module '{}'", container.module_name),
                    ).await?;
                    return Ok(true);
                }

                self.handle_ack(&msg.message_uuid7, &container.module_name).await?;
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

                self.store_audio(&msg.message_uuid7, "post", &msg.audio_type, &msg.audio).await?;
                self.handle_ack(&msg.message_uuid7, &container.module_name).await?;
                Ok(true)
            }
            Some(Payload::MessageAck(ack)) => {
                if ack.message_uuid7.is_empty() {
                    return Ok(true);
                }
                self.handle_ack(&ack.message_uuid7, &container.module_name).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod timeout_sweep_tests {
    use super::*;

    /// The sweep bounds how long a stage waits AFTER its ack budget expires.
    /// It used to ride on the 15s DB-sync interval, so a stage that fell back to
    /// the timeout path was quantised to a 15s grid — measured on the real
    /// timeline DB as p50 15s / p90 45s per message, with the deltas landing on
    /// exactly 15s and 30s.
    #[test]
    fn the_sweep_interval_is_far_below_the_ack_timeout() {
        let sweep = super::TIMEOUT_SWEEP_INTERVAL;
        let ack = Duration::from_millis(DEFAULT_ACK_TIMEOUT_MS);
        assert!(
            sweep < ack,
            "sweep {:?} must be well under the {:?} ack budget",
            sweep,
            ack
        );
        // And an order of magnitude under the 15s housekeeping loop it replaced.
        assert!(
            sweep * 10 < Duration::from_secs(15),
            "sweep {:?} is not meaningfully tighter than the old 15s loop",
            sweep
        );
    }

    /// A stage waits for EVERY module it was sent to, so a timed-out entry must
    /// only advance the message when nothing else is still pending.
    #[test]
    fn a_timed_out_stage_does_not_advance_while_others_are_pending() {
        let mut tracker = AckTracker::new();
        // One entry already expired, one still live.
        tracker.inject(
            "uuid-1".to_string(),
            vec![
                PendingAck {
                    uuid7: "uuid-1".into(),
                    stage: "pre_process".into(),
                    module_name: "slow".into(),
                    sent_at: Instant::now() - Duration::from_secs(10),
                    timeout: Duration::from_millis(1),
                },
                PendingAck {
                    uuid7: "uuid-1".into(),
                    stage: "pre_process".into(),
                    module_name: "live".into(),
                    sent_at: Instant::now(),
                    timeout: Duration::from_secs(30),
                },
            ],
        );
        let timed_out = tracker.check_timeouts();
        assert_eq!(timed_out.len(), 1, "only the expired entry times out");
        assert_eq!(timed_out[0].module_name, "slow");
        assert!(
            tracker.has_pending("uuid-1"),
            "the message must NOT be considered acked while 'live' is still pending"
        );

        // Once the survivor acks, the message is free to advance.
        assert_eq!(tracker.ack_module("uuid-1", "live").map(|(s, _)| s), Some("pre_process".into()));
        assert!(!tracker.has_pending("uuid-1"));
    }

    /// The diagnostic must name the module and stage, otherwise a stall is
    /// indistinguishable from "chat is just slow".
    #[test]
    fn a_timeout_names_the_stalling_module() {
        let mut tracker = AckTracker::new();
        tracker.track(
            "uuid-2".to_string(),
            "post_process".into(),
            "tts-service".into(),
            0,
        );
        let timed_out = tracker.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        let e = &timed_out[0];
        assert_eq!(e.module_name, "tts-service");
        assert_eq!(e.stage, "post_process");
    }

    /// A pending ack ages on WALL-CLOCK, so an entry that predates a long pause
    /// is "expired" the instant the engine resumes — which would fail every
    /// message that was in flight, i.e. turn a pause into data loss. The
    /// rebase is what makes paused time not count, and it must not become a
    /// blanket amnesty: a budget that really does expire afterwards still times
    /// out.
    #[test]
    fn restarting_the_clocks_forgives_the_paused_interval_and_nothing_more() {
        let aged = || PendingAck {
            uuid7: "uuid-3".into(),
            stage: "in_process".into(),
            module_name: "mid".into(),
            // Ten minutes ago — a pause, not a slow module.
            sent_at: Instant::now() - Duration::from_secs(600),
            timeout: Duration::from_secs(3),
        };

        // Without the rebase, the ten minutes are charged to the module.
        let mut unrebased = AckTracker::new();
        unrebased.inject("uuid-3".to_string(), vec![aged()]);
        assert_eq!(
            unrebased.check_timeouts().len(),
            1,
            "an un-rebased entry is expired by the wall-clock it sat through"
        );

        // With it, the message is back in credit for a full budget.
        let mut rebased = AckTracker::new();
        rebased.inject("uuid-3".to_string(), vec![aged()]);
        rebased.restart_clocks();
        assert!(
            rebased.check_timeouts().is_empty(),
            "the paused interval must not count against the message"
        );
        assert!(rebased.has_pending("uuid-3"), "the stage is still waiting, not reaped");

        // ...and a budget that really does expire from the rebase point still
        // does, so the rebase is a hand-back of credit, not an amnesty.
        rebased.track(
            "uuid-3".to_string(),
            "post_process".into(),
            "tts".into(),
            0,
        );
        let timed_out = rebased.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].stage, "post_process");
    }
}

#[cfg(test)]
mod timeout_wiring_tests {
    /// The sweep constant and the diagnostic are both correct in isolation, but
    /// the latency only improves if the engine actually USES them. Assert the
    /// wiring structurally — the alternative is a green suite while a 15s
    /// quantisation is still in production.
    #[test]
    fn the_engine_runs_the_sweep_on_the_tight_interval() {
        let src = include_str!("main.rs");
        assert!(
            src.contains("crate::pipeline::TIMEOUT_SWEEP_INTERVAL"),
            "the sweep task must use the tight interval, not a literal"
        );
        // And the 15s DB-sync loop must NOT be doing the sweeping any more.
        let sync_start = src.find("let mut interval = tokio::time::interval(sync_interval);")
            .expect("sync loop not found");
        let sweep_start = src.find("crate::pipeline::TIMEOUT_SWEEP_INTERVAL")
            .expect("sweep task not found");
        assert!(
            sweep_start < sync_start,
            "the dedicated sweep task must be spawned before the sync loop"
        );
        // Exactly one handle_timeout call site, and it is the tight one.
        assert_eq!(
            src.matches("orchestrator.handle_timeout()").count(),
            1,
            "handle_timeout must have exactly one call site (the tight sweep)"
        );
    }

    #[test]
    fn a_timeout_is_logged_with_its_module_and_stage() {
        let src = include_str!("pipeline.rs");
        let start = src.find("pub async fn handle_timeout(")
            .expect("handle_timeout not found");
        let end = start + src[start..].find("\n    pub async fn ")
            .unwrap_or(src[start..].len());
        let body = &src[start..end];
        assert!(
            body.contains("entry.module_name") && body.contains("entry.stage"),
            "the timeout log must name the stalling module and the stage"
        );
        assert!(
            body.contains("timed out waiting for"),
            "the timeout log should say what it is reporting"
        );
    }
}

#[cfg(test)]
mod timing_tests {
    use super::*;

    #[test]
    fn module_timing_rolls_over_to_the_last_8_samples() {
        let mut t = ModuleTiming::default();
        // 12 messages: 1..=12. The window is 8, so the average must be of
        // 5..=12 (sum 68 / 8 = 8.5), not the since-start mean.
        for i in 1..=12 {
            t.record(i as f64);
        }
        assert_eq!(t.samples.len(), ModuleTiming::WINDOW);
        assert!((t.avg_ms - 8.5).abs() < 1e-9, "avg was {}", t.avg_ms);
    }

    #[test]
    fn module_timings_track_per_module() {
        let mut ts = ModuleTimings::default();
        assert_eq!(ts.avg_ms("alpha"), None, "no sample yet");
        ts.record("alpha", 10.0);
        ts.record("alpha", 20.0);
        ts.record("bravo", 100.0);
        assert!((ts.avg_ms("alpha").unwrap() - 15.0).abs() < 1e-9);
        assert!((ts.avg_ms("bravo").unwrap() - 100.0).abs() < 1e-9);
        let all = ts.all_avgs();
        assert_eq!(all.len(), 2);
        assert!((all["alpha"] - 15.0).abs() < 1e-9);
    }
}
