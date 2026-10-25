use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

use crate::cockatiel_protobuf;
use crate::cockatiel_protobuf::{
    ChatMessage, Command, ContainerForEngine, ContainerForModule,
    container_for_engine::Payload as EnginePayload, container_for_module::Payload as ModulePayload,
};
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseManager, PipelineOutcome, PipelineResult};
use crate::stream_rewards::{SharedStreamTracker, StreamTracker};

const DEFAULT_ACK_TIMEOUT_MS: u32 = 3000;

/// How many times the engine resends a stage message whose RECEIPT ping never
/// arrived. Past this the module is treated as dead (normal timeout path).
const MAX_ACK_RESENDS: u32 = 3;

/// Decide which modules receive a message, based on command routing.
///
/// A message with a parsed command goes ONLY to the owning module + any
/// catch-all modules (modules that registered an empty command set). A message
/// with NO command goes to every module in the stage EXCEPT command-scoped ones
/// (modules that registered specific commands): those only handle their own
/// commands, so sending them plain chat would waste work they ignore AND charge
/// their price gate for a message they do not process. Catch-alls and
/// never-registered displays/scorers/censors still receive every plain message.
///
/// `stage_modules` is the stage's configured module list, so the result is
/// always a subset of it. Returns `None` only for the (currently unused)
/// escape hatch meaning "every module in the stage".
pub(crate) fn command_recipients(
    parsed_command: Option<&Command>,
    registry: &CommandRegistry,
    stage_modules: &[String],
) -> Option<Vec<String>> {
    let Some(cmd) = parsed_command else {
        // No command: every stage module that is NOT command-scoped, PLUS any
        // command-scoped module that opted to keep seeing chat
        // (`alert_on_unknown_command` — e.g. a module that owns a command but
        // also parses its own subcommands and reacts to plain messages). A pure
        // command owner is skipped so its price gate is not charged.
        return Some(
            stage_modules
                .iter()
                .filter(|m| !registry.is_command_scoped(m) || registry.wants_plain_messages(m))
                .cloned()
                .collect(),
        );
    };
    let mut targets: Vec<String> = Vec::new();
    if let Some(owner) = registry.owner(&cmd.command_flag, &cmd.command_name) {
        targets.push(owner);
    }
    targets.extend(registry.catch_alls());
    // Keep only targets actually in this stage's configured list.
    targets.retain(|name| stage_modules.iter().any(|m| m == name));
    Some(targets)
}

/// Whether a same-name connection should be accepted as a silent "shadow"
/// (keeping the incumbent's routing slot) rather than taking the slot itself.
/// Only a LIVE incumbent from a DIFFERENT instance warrants a shadow: a
/// reconnect of the SAME instance must take the slot so its new socket becomes
/// the routing target, even if the old socket is still closing.
pub(crate) fn should_shadow(live_incumbent: bool, same_instance: bool) -> bool {
    live_incumbent && !same_instance
}

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

/// The `query_id` the engine sends a chat module (as a legacy
/// `DatabaseQueryResult` push) to ask for the bans it currently holds. The
/// module answers with a `module_bans_report` `DatabaseQuery`. These two strings
/// are the whole engine↔module bans protocol, so no proto change / crate
/// republish is needed.
pub const MODULE_BANS_QUERY: &str = "module_bans_query";

/// Bounded, non-wedging send to a connected module. Clones the sender out of
/// the shared `module_senders` lock (so the lock is never held across the
/// send), `try_send`s first, and only falls back to a short timeout send when
/// the channel is full — a module that isn't draining its socket must not
/// stall the liveness watchdog or the routing lock for every other module.
/// A missing sender slot yields `NotConnected`; a closed channel is treated as
/// not connected too (the module is gone even if its slot lingers); a full
/// channel that never frees up yields `Dropped`.
pub(crate) async fn send_to_module(
    senders: &Arc<Mutex<HashMap<String, mpsc::Sender<ContainerForModule>>>>,
    module_name: &str,
    container: ContainerForModule,
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
    if tokio::time::timeout(Duration::from_millis(1000), sender.send(container))
        .await
        .is_ok()
    {
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

/// A required dependency the engine cannot run without. While one is missing the
/// engine is BLOCKED: it accepts connections and ingests, but dispatches
/// nothing (exactly like a pause) until the dependency appears.
///
/// Reported to the UIs through `db_status` so the operator sees WHY nothing is
/// moving, instead of a misleading PAUSED badge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedReason {
    /// The user database service is not reachable. The TUI owns its lifecycle
    /// (builds/launches it); the engine only observes reachability.
    UserDb,
    /// The configured timeline database location is unusable (empty path, or
    /// its parent directory is missing and cannot be created).
    TimelineDb,
}

impl BlockedReason {
    /// The wire string reported in `db_status`.
    pub fn as_str(self) -> &'static str {
        match self {
            BlockedReason::UserDb => "user_db",
            BlockedReason::TimelineDb => "timeline_db",
        }
    }

    /// Higher outranks lower when both are missing: a broken timeline DB is the
    /// more fundamental fault, so it is the reason reported while both are down.
    fn priority(self) -> u8 {
        match self {
            BlockedReason::TimelineDb => 2,
            BlockedReason::UserDb => 1,
        }
    }
}

/// The engine's pause state, the messages a pause is holding, and whether a
/// required dependency is currently blocking dispatch.
///
/// `held` is keyed by uuid7 in a `BTreeMap` so the resume order is uuid7 order,
/// which is time order for uuid7s — the same ordering the recovery drain gets
/// from `get_queued_uuids`, and the same ordering the messages arrived in.
#[derive(Debug, Default)]
struct PauseState {
    paused: bool,
    held: BTreeMap<String, HoldPoint>,
    /// The missing dependency gating dispatch, if any. `None` = not blocked.
    blocked: Option<BlockedReason>,
    /// When a block clears, should the engine resume automatically? Set when the
    /// operator (or auto-start) asks to resume WHILE blocked; cleared when the
    /// operator asks to pause while blocked. Drives the UI's
    /// "will resume on clear" / "will stay paused after clear" wording.
    resume_when_unblocked: bool,
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
    /// Adapter-supplied opaque bytes (`ChatMessage.raw_data`, UTF-8 JSON). Read
    /// at ingest to detect a donation and forwarded to every stage so a module
    /// sees the same payload the engine did. Deliberately NOT persisted to the
    /// timeline (it can be kilobytes per message; the donation facts ride in
    /// `flags` and the enriched `user_data` instead).
    pub raw_data: Vec<u8>,
}

/// A donation the adapter attached to a chat message via `raw_data`.
///
/// The adapter encodes `ChatMessage.raw_data` as UTF-8 JSON:
///
/// `{"donation":{"kind":"superchat","amount_micros":5000000,
/// "currency":"USD","display":"$5.00","message":"nice!"}}`
///
/// `amount_micros` stays `i64`: it is an external-protocol value with no
/// 32-bit alternative at the source (a large tip in micros overflows `i32`),
/// matching the AGENTS.md exception. Everything else is a string.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DonationInfo {
    /// `superchat` / `supersticker` / `membership` / `gift`.
    pub kind: String,
    /// Amount in millionths of `currency`. `i64` (external protocol exception).
    pub amount_micros: i64,
    /// ISO currency code, e.g. `USD`.
    pub currency: String,
    /// Human-facing amount, e.g. `$5.00`.
    pub display: String,
    /// The donor's comment / the membership line.
    pub message: String,
}

impl DonationInfo {
    /// Parse `raw_data` as the adapter's donation JSON. `None` for empty bytes,
    /// non-JSON, or JSON without a `donation.kind` — a plain message is simply
    /// not a donation, never an error.
    pub fn parse(raw_data: &[u8]) -> Option<Self> {
        if raw_data.is_empty() {
            return None;
        }
        let value: serde_json::Value = serde_json::from_slice(raw_data).ok()?;
        let donation = value.get("donation")?;
        let kind = donation.get("kind")?.as_str()?.to_string();
        if kind.is_empty() {
            return None;
        }
        let text = |key: &str| {
            donation
                .get(key)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        Some(Self {
            kind,
            amount_micros: donation
                .get("amount_micros")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            currency: text("currency"),
            display: text("display"),
            message: text("message"),
        })
    }

    /// The engine's internal `flags` string for a donation row:
    /// `donation=<kind>;amount=<display>;currency=<currency>`.
    pub fn flags_string(&self) -> String {
        format!(
            "donation={};amount={};currency={}",
            self.kind, self.display, self.currency
        )
    }

    /// The four `user_data` css_properties keys a donation advertises alongside
    /// the score/rank keys. Owned `(key, value)` pairs so callers can insert
    /// them into any map.
    pub fn css_properties(&self) -> [(&'static str, String); 4] {
        [
            ("is_donation", "1".to_string()),
            ("donation_kind", self.kind.clone()),
            ("donation_amount", self.display.clone()),
            ("donation_currency", self.currency.clone()),
        ]
    }
}

/// Insert a donation's css keys into an enriched `user_data` map. A `None`
/// donation is a no-op (a plain message carries no donation keys).
pub fn add_donation_css(
    css: &mut std::collections::HashMap<String, String>,
    donation: Option<&DonationInfo>,
) {
    if let Some(d) = donation {
        for (key, value) in d.css_properties() {
            css.insert(key.to_string(), value);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingAck {
    pub uuid7: String,
    pub stage: String,
    pub module_name: String,
    pub sent_at: Instant,
    pub timeout: Duration,
    /// Whether the module sent its receipt ping (`MessageAck`) for this
    /// message. A receipt confirms delivery; the RESULT (the stage-echo) is
    /// what clears the pending entry. When the receipt never arrives within
    /// the window, the engine RESENDS the stage message.
    pub receipt_received: bool,
    /// How many times this stage message has been resent because the receipt
    /// never arrived. Capped so a genuinely dead module cannot be resent
    /// forever — past the cap it takes the normal timeout path.
    pub resend_count: u32,
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
    samples: Vec<f32>,
    /// The average of the current samples (ms). Kept so a zero-sample module
    /// still reports a definite 0 rather than a blank.
    pub avg_ms: f32,
}

impl ModuleTiming {
    /// How many recent messages make up the rolling average.
    pub const WINDOW: usize = 8;

    /// Record one message's processing time (ms). Keeps only the last
    /// [`WINDOW`] samples and re-derives the average.
    pub fn record(&mut self, elapsed_ms: f32) {
        self.samples.push(elapsed_ms);
        if self.samples.len() > Self::WINDOW {
            let overflow = self.samples.len() - Self::WINDOW;
            self.samples.drain(0..overflow);
        }
        let sum: f32 = self.samples.iter().sum();
        self.avg_ms = sum / self.samples.len() as f32;
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
    pub fn record(&mut self, module_name: &str, elapsed_ms: f32) {
        self.by_module
            .entry(module_name.to_string())
            .or_default()
            .record(elapsed_ms);
    }

    /// The current rolling average (ms) for `module_name`, or `None` if the
    /// module has not completed a message yet.
    pub fn avg_ms(&self, module_name: &str) -> Option<f32> {
        self.by_module.get(module_name).map(|t| t.avg_ms)
    }

    /// Snapshot of every recorded module's current average (ms).
    pub fn all_avgs(&self) -> HashMap<String, f32> {
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

    pub fn track(&mut self, uuid7: String, stage: String, module_name: String, timeout_ms: u32) {
        let entry = PendingAck {
            uuid7: uuid7.clone(),
            stage,
            module_name,
            sent_at: Instant::now(),
            timeout: Duration::from_millis(timeout_ms as u64),
            receipt_received: false,
            resend_count: 0,
        };

        self.pending.entry(uuid7).or_default().push(entry);
    }

    /// Mark that `module_name` sent its receipt ping for `uuid7`. A receipt
    /// confirms DELIVERY only — the pending entry stays until the result
    /// (stage-echo) arrives. Idempotent: a second receipt is a no-op.
    pub fn mark_receipt(&mut self, uuid7: &str, module_name: &str) {
        if let Some(entries) = self.pending.get_mut(uuid7) {
            if let Some(entry) = entries.iter_mut().find(|e| e.module_name == module_name) {
                entry.receipt_received = true;
            }
        }
    }

    /// Re-track a stage message after the engine resent it because the receipt
    /// never arrived. `check_timeouts` DRAINS the expired entry out of `pending`,
    /// so this re-adds a fresh one carrying the incremented resend count (the
    /// resend keeps the message in flight with a full budget).
    pub fn re_track(&mut self, entry: &PendingAck) {
        let fresh = PendingAck {
            uuid7: entry.uuid7.clone(),
            stage: entry.stage.clone(),
            module_name: entry.module_name.clone(),
            sent_at: Instant::now(),
            timeout: entry.timeout,
            receipt_received: false,
            resend_count: entry.resend_count + 1,
        };
        self.pending
            .entry(entry.uuid7.clone())
            .or_default()
            .push(fresh);
    }

    pub fn ack(&mut self, uuid7: &str) -> Vec<PendingAck> {
        self.pending.remove(uuid7).unwrap_or_default()
    }

    /// Remove ONLY the acking module's pending entry for `uuid7` (a stage with
    /// several modules must wait for ALL of them to ack, not advance on the
    /// first). Returns the stage that was acked, the processing duration
    /// (ms since the send) — the duration is what feeds the module's rolling
    /// latency average — AND whether the message still has a pending entry
    /// afterwards. `None` if this module had no pending ack for the message.
    ///
    /// The removal and the "anything still pending?" answer are computed in the
    /// SAME call, under one lock acquisition by the caller. Splitting them (as
    /// the old `ack_module` + `has_pending` pair did) let two recipients ack
    /// near-simultaneously and BOTH observe "nothing pending", each advancing
    /// the stage — double-dispatching the next stage and racing the terminal
    /// write.
    pub fn ack_module(&mut self, uuid7: &str, module_name: &str) -> Option<(String, f32, bool)> {
        let now = Instant::now();
        let (stage, elapsed_ms) = {
            let entries = self.pending.get(uuid7)?;
            let entry = entries.iter().find(|e| e.module_name == module_name)?;
            let elapsed_ms = now.duration_since(entry.sent_at).as_secs_f32() * 1000.0;
            (entry.stage.clone(), elapsed_ms)
        };
        let still_pending = if let Some(entries) = self.pending.get_mut(uuid7) {
            entries.retain(|e| e.module_name != module_name);
            if entries.is_empty() {
                self.pending.remove(uuid7);
                false
            } else {
                true
            }
        } else {
            false
        };
        Some((stage, elapsed_ms, still_pending))
    }

    /// Drain every expired entry, reporting with each one whether the message
    /// still has a pending ack AFTER the drain. That per-entry pending state is
    /// captured under the same lock as the drain, so a concurrent `ack_module`
    /// cannot slip between "remove the expired entry" and "is anything still
    /// pending?" and let both paths advance the stage. Several entries for one
    /// uuid can expire in a single pass (a stage with more than one silent
    /// module); each carries the same post-drain snapshot.
    pub fn check_timeouts(&mut self) -> Vec<(PendingAck, bool)> {
        let now = Instant::now();
        let mut timed_out: Vec<(PendingAck, bool)> = Vec::new();

        for entries in self.pending.values_mut() {
            let mut still_pending = Vec::new();
            let mut expired = Vec::new();
            for entry in entries.drain(..) {
                if now.duration_since(entry.sent_at) > entry.timeout {
                    expired.push(entry);
                } else {
                    still_pending.push(entry);
                }
            }
            let has_pending_after = !still_pending.is_empty();
            *entries = still_pending;
            for entry in expired {
                timed_out.push((entry, has_pending_after));
            }
        }
        // Drop the uuids whose entries were all expired (the drained `Vec` is
        // left empty above rather than removed mid-iteration).
        self.pending.retain(|_, entries| !entries.is_empty());

        timed_out
    }

    pub fn has_pending(&self, uuid7: &str) -> bool {
        self.pending
            .get(uuid7)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }

    /// Whether `module_name` holds a pending ack for `uuid7` at `stage`. This is
    /// the ownership check a stage reply must pass before it may mutate the
    /// message's in-memory state: a module with no pending entry for that
    /// uuid+stage is answering something that is not its to answer, and its
    /// processed text / audio must be ignored.
    pub fn is_pending_for(&self, uuid7: &str, module_name: &str, stage: &str) -> bool {
        self.pending
            .get(uuid7)
            .map(|entries| {
                entries
                    .iter()
                    .any(|e| e.module_name == module_name && e.stage == stage)
            })
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
    pub ack_timeout_ms: u32,
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
    /// Adapter-supplied opaque bytes, forwarded to each stage's `ChatMessage`.
    pub raw_data: Vec<u8>,
    /// Parsed donation for this message, when `raw_data` carried one. Drives
    /// the donation css keys on the enriched `user_data` map.
    pub donation: Option<DonationInfo>,
}

/// The cost/authority gate for one module, read from its manifest:
///
///  - `authority`: minimum role. 0=user (no gate), 1=mod (mod|admin|owner),
///    2=admin (admin|owner), 3=owner. Cascades up.
///  - `min_rank`: the user's rank (0-1 float) must be >= this. 0 = no gate.
///  - `price`: score deducted from the user's CURRENT score when the module
///    receives their message. 0 = free. Lifetime `total_score` is untouched.
///
/// (0, 0, 0) = free + unrestricted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModuleGate {
    pub authority: u32,
    pub min_rank: f32,
    pub price: u32,
}

impl ModuleGate {
    /// Whether this gate lets everyone through (no authority, rank or price bar).
    pub fn is_open(&self) -> bool {
        self.authority == 0 && self.min_rank <= 0.0 && self.price == 0
    }
}

/// The per-module authority tiers, aligned with the `User` role flags.
pub const AUTHORITY_USER: u32 = 0;
pub const AUTHORITY_MOD: u32 = 1;
pub const AUTHORITY_ADMIN: u32 = 2;
pub const AUTHORITY_OWNER: u32 = 3;

#[derive(Clone)]
pub struct PipelineOrchestrator {
    pub db: DatabaseManager,
    pub ack_tracker: Arc<Mutex<AckTracker>>,
    pub pipeline_states: Arc<Mutex<HashMap<String, PipelineState>>>,
    pub config: Arc<Mutex<PipelineConfig>>,
    pub module_senders: Arc<Mutex<HashMap<String, mpsc::Sender<ContainerForModule>>>>,
    /// Records which instance owns the name-keyed routing slot (`module_senders`)
    /// so a duplicate/shadow disconnect can't evict the live module's sender.
    /// Invariant: an entry is present only while `module_senders` holds a slot
    /// for the same name, and its value is the instance that installed it.
    pub slot_owner: Arc<Mutex<HashMap<String, String>>>,
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
    /// Per-module gates: module name -> authority/rank/price. Populated from
    /// the discovered module manifests.
    pub module_gates: Arc<std::sync::Mutex<HashMap<String, ModuleGate>>>,
    /// The user database client, used by the price/rank gate to charge score
    /// and check rank. `None` in unit tests that don't gate.
    pub user_db: Option<crate::user_db_client::SharedUserDbClient>,
    /// Per-user stream streak + within-stream activity multipliers. Shared with
    /// the connection handler (stream open/close + score scaling) and enriched
    /// onto outgoing `css_properties`.
    pub stream_tracker: SharedStreamTracker,
    /// Cached identities of globally-blocked users: each blocked user's uuid7
    /// plus every `platform\0handle` / `platform\0channel_id` alias from their
    /// channels. This is the cheap, unconditional gate for the new-message arm:
    /// `UserData.bans` is only populated when a message is enriched (a display
    /// or catch-all is connected, or it carries a command), so without this a
    /// blocked user's plain message slips through when nothing is enriched.
    /// Loaded once at startup from the user-db and kept in sync by the
    /// `userdb_block` / `userdb_unblock` handlers.
    pub blocked_users: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Per-module ban caches: module name -> the set of identities that module
    /// currently holds a ban for. A module REPLACES its own set on every report
    /// (a `module_bans_report` query), so a ban added or removed by hand in the
    /// module's `bans.json` is picked up on the next refresh. Kept SEPARATE from
    /// `blocked_users` so a global `userdb_unblock` cannot clear a module ban
    /// and a startup reload of the global blocks cannot wipe the module bans.
    /// The effective gate is global OR any module (see
    /// [`is_identifier_blocked`](Self::is_identifier_blocked)).
    pub module_bans: Arc<std::sync::Mutex<HashMap<String, HashSet<String>>>>,
}

impl PipelineOrchestrator {
    pub fn new(
        db: DatabaseManager,
        config: PipelineConfig,
        module_senders: Arc<Mutex<HashMap<String, mpsc::Sender<ContainerForModule>>>>,
        command_registry: Arc<std::sync::Mutex<CommandRegistry>>,
    ) -> Self {
        Self {
            db,
            ack_tracker: Arc::new(Mutex::new(AckTracker::new())),
            pipeline_states: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(config)),
            module_senders,
            slot_owner: Arc::new(Mutex::new(HashMap::new())),
            command_registry,
            pause_state: Arc::new(Mutex::new(PauseState::default())),
            module_timings: Arc::new(Mutex::new(ModuleTimings::default())),
            module_gates: Arc::new(std::sync::Mutex::new(HashMap::new())),
            user_db: None,
            stream_tracker: Arc::new(std::sync::Mutex::new(StreamTracker::new())),
            blocked_users: Arc::new(std::sync::Mutex::new(HashSet::new())),
            module_bans: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set the per-module gates (authority / rank / price) from the discovered
    /// manifests.
    pub fn set_module_gates(&self, gates: HashMap<String, ModuleGate>) {
        *self.module_gates.lock().unwrap() = gates;
    }

    /// The alias key under which a blocked user's platform channel is indexed,
    /// so an un-enriched adapter message — which carries a platform login in
    /// `user_uuid7`, not the canonical uuid7 — can still be matched against the
    /// block cache. The NUL separator cannot appear in a platform name or a
    /// handle/channel id, so the key is unambiguous.
    pub fn blocked_alias_key(platform: &str, identifier: &str) -> String {
        format!("{}\u{0}{}", platform, identifier)
    }

    /// Whether the sender is globally blocked, before any enrichment.
    ///
    /// `identifier` is whatever the adapter put in `user_uuid7`: the canonical
    /// uuid7 once the message has been enriched, or the raw platform
    /// login/channel id before it has. Both the uuid7 and the
    /// `platform`+identifier alias are checked. An empty identifier (a
    /// system/adapter message with no user identity) is never treated as
    /// blocked.
    pub fn is_identifier_blocked(&self, platform: &str, identifier: &str) -> bool {
        let id = identifier.trim();
        if id.is_empty() {
            return false;
        }
        let cache = self.blocked_users.lock().unwrap();
        if cache.contains(id) || cache.contains(&Self::blocked_alias_key(platform, id)) {
            return true;
        }
        drop(cache);
        self.is_module_banned(platform, id)
    }

    /// Add a user's uuid7 and every one of their platform channels to the block
    /// cache (called on `userdb_block`).
    pub fn mark_user_blocked(&self, uuid7: &str, channels: &[cockatiel_protobuf::ChannelRef]) {
        if uuid7.trim().is_empty() {
            return;
        }
        let mut cache = self.blocked_users.lock().unwrap();
        cache.insert(uuid7.to_string());
        for ch in channels {
            if !ch.channel_id.trim().is_empty() {
                cache.insert(Self::blocked_alias_key(&ch.platform, &ch.channel_id));
            }
            if !ch.handle.trim().is_empty() {
                cache.insert(Self::blocked_alias_key(&ch.platform, &ch.handle));
            }
        }
    }

    /// Remove a user's uuid7 and platform channels from the block cache (called
    /// on `userdb_unblock`).
    pub fn mark_user_unblocked(&self, uuid7: &str, channels: &[cockatiel_protobuf::ChannelRef]) {
        let mut cache = self.blocked_users.lock().unwrap();
        cache.remove(uuid7);
        for ch in channels {
            cache.remove(&Self::blocked_alias_key(&ch.platform, &ch.channel_id));
            cache.remove(&Self::blocked_alias_key(&ch.platform, &ch.handle));
        }
    }

    /// Replace the whole block cache (used by the one-time startup load).
    pub fn replace_blocked_users(&self, entries: HashSet<String>) {
        *self.blocked_users.lock().unwrap() = entries;
    }

    /// Number of entries (uuid7s + aliases) currently in the block cache.
    pub fn blocked_cache_len(&self) -> usize {
        self.blocked_users.lock().unwrap().len()
    }

    /// Replace one module's ban set with the identities from its latest report.
    /// An empty set removes the module's entry entirely, so a module that has
    /// cleared its `bans.json` stops contributing to the aggregate. Called by
    /// the `module_bans_report` query handler on every report.
    pub fn replace_module_bans(&self, module: &str, keys: HashSet<String>) {
        let mut map = self.module_bans.lock().unwrap();
        if keys.is_empty() {
            map.remove(module);
        } else {
            map.insert(module.to_string(), keys);
        }
    }

    /// Whether any module currently holds a ban matching `identifier` on
    /// `platform`. Both the bare identity and the `platform`+identifier alias
    /// are checked, matching how `blocked_users` is keyed — a module may report
    /// a raw handle (matched directly) or a platform-scoped one (matched by
    /// alias).
    pub fn is_module_banned(&self, platform: &str, identifier: &str) -> bool {
        let id = identifier.trim();
        if id.is_empty() {
            return false;
        }
        let alias = Self::blocked_alias_key(platform, id);
        self.module_bans
            .lock()
            .unwrap()
            .values()
            .any(|set| set.contains(id) || set.contains(&alias))
    }

    /// Whether a user (by canonical uuid7) or any of their known platform
    /// channels is module-banned. Used by the enrich path to append a synthetic
    /// `Ban` to `UserData.bans`, so an ENRICHED message from a module-banned
    /// user is still dropped even when the module reported a different identity
    /// form (e.g. a platform handle rather than the canonical uuid7).
    pub fn has_module_ban_for(
        &self,
        uuid7: &str,
        channels: &[cockatiel_protobuf::ChannelRef],
    ) -> bool {
        if self.is_module_banned("", uuid7) {
            return true;
        }
        channels.iter().any(|ch| {
            self.is_module_banned(&ch.platform, &ch.channel_id)
                || self.is_module_banned(&ch.platform, &ch.handle)
        })
    }

    /// Number of modules currently contributing a ban set (for logs).
    pub fn module_bans_len(&self) -> usize {
        self.module_bans.lock().unwrap().len()
    }

    /// Whether `module_name` is permitted to receive a message for `user_uuid7`
    /// (the user who sent it), given the module's authority / rank / price gates.
    ///
    /// An open gate (authority 0 AND min_rank 0 AND price 0) is always allowed.
    /// An empty `user_uuid7` (a system/adapter-originated message, or one with
    /// no user identity) is allowed — there is nobody to check or charge.
    /// Otherwise the user is fetched ONCE from the user database (roles, rank
    /// and score arrive together, a single trip): their authority tier must
    /// meet the module's `authority` (cascades up), their rank must be >=
    /// `min_rank`, and the module price is deducted from their current score if
    /// they can afford it. A failure (insufficient authority/rank, or
    /// insufficient score) returns `false` so the caller skips THIS module; the
    /// rest of the pipeline (and the message) is unaffected.
    pub async fn permit_module(&self, user_uuid7: &str, module_name: &str) -> bool {
        let Some(&gate) = self.module_gates.lock().unwrap().get(module_name) else {
            return true;
        };
        if gate.is_open() {
            return true;
        }
        if user_uuid7.trim().is_empty() {
            return true;
        }
        let Some(user_db) = &self.user_db else {
            // No user-db wired (unit test / degraded): don't block modules.
            return true;
        };
        // Fetch the user once — roles, rank and score travel on the same response.
        let resp = match user_db.get_user(user_uuid7, "", "", "").await {
            Ok(r) => r,
            Err(_) => return true, // DB hiccup: don't stall the pipeline on a gate.
        };
        let Some(user) = resp.user else {
            // Unknown user: treat as failing authority/rank and lacking funds
            // unless the gate is truly open (already handled above).
            return false;
        };

        // Authority gate (cascades up): mod=1 admits mod|admin|owner, admin=2
        // admits admin|owner, owner=3 admits owner. A user's tier is their
        // HIGHEST held role.
        if gate.authority > 0 {
            let tier = if user.is_owner {
                AUTHORITY_OWNER
            } else if user.is_admin {
                AUTHORITY_ADMIN
            } else if user.is_moderator {
                AUTHORITY_MOD
            } else {
                AUTHORITY_USER
            };
            if tier < gate.authority {
                return false;
            }
        }

        // Rank gate (cheaper to reason about, no mutation). Rank is a 0-1 float.
        if gate.min_rank > 0.0 && user.rank < gate.min_rank {
            return false;
        }

        // Price gate: deduct from the current score, guarded by the user-db.
        if gate.price > 0 {
            match user_db.deduct_score(user_uuid7, gate.price as i32).await {
                Ok(outcome) if outcome.applied => {}
                _ => return false, // insufficient funds or user missing.
            }
        }
        true
    }

    // ── The pause ──────────────────────────────────────────────────────
    //
    // Pausing holds DISPATCHES, not messages. Ingest keeps running, the
    // timeline row is still written, commands are still parsed: an incoming
    // message while paused is persisted as 'queued' and joins the held set, so
    // nothing is lost and resume is just "work through a bigger queue".

    /// Is the pipeline holding every dispatch to every module? This is the
    /// OPERATOR pause flag specifically; `is_gated` also folds in a missing
    /// dependency block.
    pub async fn is_paused(&self) -> bool {
        self.pause_state.lock().await.paused
    }

    /// The missing dependency gating dispatch, if any (for `db_status`).
    pub async fn blocked_reason(&self) -> Option<BlockedReason> {
        self.pause_state.lock().await.blocked
    }

    /// Whether the engine will resume automatically when the block clears.
    pub async fn will_resume_on_unblock(&self) -> bool {
        self.pause_state.lock().await.resume_when_unblocked
    }

    /// Is dispatch held, either by an operator pause or a dependency block?
    pub async fn is_gated(&self) -> bool {
        let state = self.pause_state.lock().await;
        state.paused || state.blocked.is_some()
    }

    /// Block dispatch on a missing dependency. A higher-priority reason (a
    /// broken timeline DB) is not overwritten by a lower one (a missing user
    /// DB), so the reported cause is the more fundamental fault.
    pub async fn set_block(&self, reason: BlockedReason) {
        let mut state = self.pause_state.lock().await;
        let replace = match state.blocked {
            None => true,
            Some(current) => reason.priority() > current.priority(),
        };
        if replace {
            state.blocked = Some(reason);
        }
    }

    /// Clear the block if it matches `reason`. Returns `true` when the caller
    /// should now run [`Self::resume`] — i.e. the block cleared and the operator
    /// had asked to resume (or auto-start had). A block that stays paused after
    /// clearing returns `false`, and the operator resumes normally.
    pub async fn clear_block(&self, reason: BlockedReason) -> bool {
        let mut state = self.pause_state.lock().await;
        if state.blocked != Some(reason) {
            return false;
        }
        state.blocked = None;
        let resume = state.resume_when_unblocked;
        state.resume_when_unblocked = false;
        resume
    }

    /// The operator (or auto-start) asked to resume while blocked: remember the
    /// intent so the engine opens the gate the moment the block clears.
    pub async fn defer_resume(&self) {
        self.pause_state.lock().await.resume_when_unblocked = true;
    }

    /// The operator asked to pause (or stay paused) while blocked: record that
    /// the engine must NOT auto-resume when the block clears, and hold now.
    pub async fn defer_pause(&self) {
        let mut state = self.pause_state.lock().await;
        state.resume_when_unblocked = false;
        state.paused = true;
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
    /// Holds on either an operator pause OR a missing-dependency block; the two
    /// are indistinguishable to a message, and both release through `resume`.
    ///
    /// `uuid7`/`point` are recorded so [`Self::resume`] can replay exactly the
    /// dispatch that was skipped. One message is held at one point: the ack that
    /// advanced it to that point consumed the tracker's entry, so no second ack
    /// can advance the same stage and re-hold it.
    async fn hold_if_gated(&self, uuid7: &str, point: HoldPoint) -> bool {
        let mut state = self.pause_state.lock().await;
        if !state.paused && state.blocked.is_none() {
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

        self.db
            .insert_event(
                &uuid7_bytes,
                msg.event_type,
                &msg.platform,
                &[],
                &msg.raw_message,
                msg.command.as_deref().unwrap_or(""),
                msg.flags.as_deref().unwrap_or("{}"),
            )
            .await?;

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
            raw_data: msg.raw_data.clone(),
            donation: DonationInfo::parse(&msg.raw_data),
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
            command: if msg.command.is_empty() {
                None
            } else {
                Some(msg.command.clone())
            },
            flags: if msg.flags.is_empty() {
                None
            } else {
                Some(msg.flags.clone())
            },
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
            // `raw_data` is never persisted, so a recovered row cannot
            // reconstruct it or its donation; only the stored `flags` survive.
            raw_data: Vec::new(),
            donation: None,
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

    async fn broadcast_pre_process(&self, uuid7: &str) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 1 of 3 — the entry gate. By the time a message reaches
        // here its timeline row is written and its state is live, so the pause
        // holds the CHAIN, not the message: nothing is lost, and resume replays
        // this same fanout from `HoldPoint::Entry`. Both entries into the chain
        // funnel through here (live ingest and crash recovery), so one gate
        // covers both.
        if self.hold_if_gated(uuid7, HoldPoint::Entry).await {
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

        let container = ContainerForModule {
            version: 2,
            auth_token: String::new(),
            module_instance_uuid7: String::new(),
            payload: Some(ModulePayload::MessagePreProcess(
                cockatiel_protobuf::MessagePreProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: state.raw_data.clone(),
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

        // Command routing: a message with a registered command goes ONLY to the
        // owning module + any catch-all modules; a message with NO command goes
        // to EVERY module in the stage (scorers, censors and displays all need
        // plain chat). A module that registered specific commands it didn't get
        // is skipped — it is never burdened with commands it doesn't handle.
        let cfg = self.config_snapshot().await;
        let targets = {
            let registry = self.command_registry.lock().unwrap();
            command_recipients(
                state.parsed_command.as_ref(),
                &registry,
                &cfg.pre_process_modules,
            )
        };

        let recipients: Vec<String> = match &targets {
            Some(list) => list.clone(),
            None => cfg.pre_process_modules.clone(),
        };
        let recipients_empty = recipients.is_empty();

        let mut sent_any = false;
        for module_name in &recipients {
            // Cost gate: skip THIS module if the user can't afford its price or
            // doesn't meet its min_rank. The rest of the fanout + message are
            // unaffected (skip-that-module semantics).
            if !self.permit_module(&state.user_uuid7, module_name).await {
                eprintln!(
                    "[engine] skipped '{}' (pre_process) — user lacks the score/rank the module requires",
                    module_name
                );
                continue;
            }
            match send_to_module(
                &self.module_senders,
                module_name,
                container.clone(),
                "pre_process",
            )
            .await
            {
                SendOutcome::Sent => {
                    sent_any = true;
                    // The tracker lock is taken ONLY for this mutation — never
                    // held across the user-db round-trip (`permit_module`) or the
                    // channel send above. A slow user-db must not stall every
                    // ack and the timeout sweep behind the tracker mutex.
                    self.ack_tracker.lock().await.track(
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

        // No pre-process module is connected — there is nobody to ack, so
        // advance immediately instead of leaving the message stuck in
        // `pipeline_states` forever (a leak + a perpetually 'processing' row).
        if recipients_empty || !sent_any {
            self.start_in_process(uuid7).await?;
        }

        Ok(())
    }

    async fn start_in_process(&self, uuid7: &str) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 2 of 3. Hold rather than dispatch: the message keeps its
        // state and its `current_in_process_index`, so the replay sends it to the
        // module it was about to reach — and the audit check, the stage stamp
        // and the send all wait for the resume rather than half-happening.
        if self.hold_if_gated(uuid7, HoldPoint::InProcess).await {
            return Ok(());
        }

        // Held-for-audit messages are not advanced or shown. A read error fails
        // CLOSED — `?` propagates instead of treating "could not read the hold"
        // as "not held", which would let an audited message advance.
        if self.db.is_audited(&uuid7_string_to_bytes(uuid7)).await? {
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

        let container = ContainerForModule {
            version: 2,
            auth_token: String::new(),
            module_instance_uuid7: String::new(),
            payload: Some(ModulePayload::MessageInProcess(
                cockatiel_protobuf::MessageInProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: state.raw_data.clone(),
                        raw_message: state.raw_message.clone(),
                        user_uuid7: state.user_uuid7.clone(),
                        command: state.parsed_command.clone(),
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

        // Command routing: a COMMAND message is only processed by the module
        // that registered it (plus catch-alls); a message with NO command is
        // processed by every in-process module, in order. If THIS module isn't a
        // recipient, skip it without sending.
        let is_recipient = {
            let registry = self.command_registry.lock().unwrap();
            let targets = command_recipients(
                state.parsed_command.as_ref(),
                &registry,
                &cfg.in_process_modules,
            );
            targets
                .as_ref()
                .map(|t| t.iter().any(|m| m == module_name))
                .unwrap_or(true)
        };
        if !is_recipient {
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
            return Ok(());
        }

        // Cost gate: a user who can't afford the module's price / meet its min_rank
        // skips to the NEXT in-process module (same as an unconnected module),
        // so the message continues through the chain.
        let sent = if self.permit_module(&state.user_uuid7, module_name).await {
            send_to_module(&self.module_senders, module_name, container, "in_process").await
        } else {
            eprintln!(
                "[engine] skipped '{}' (in_process) — user lacks the score/rank the module requires",
                module_name
            );
            SendOutcome::NotConnected
        };
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

    async fn start_post_process(&self, uuid7: &str) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE 3 of 3 — same rule as the in-process gate: hold, keep the
        // state, let the replay finish it. Note this also covers the
        // no-module-connected completion below, so a message cannot reach a
        // terminal 'complete' write while the pipeline is held.
        if self.hold_if_gated(uuid7, HoldPoint::PostProcess).await {
            return Ok(());
        }

        // Held-for-audit messages are not advanced or shown. A read error fails
        // CLOSED — `?` propagates instead of treating "could not read the hold"
        // as "not held", which would let an audited message advance.
        if self.db.is_audited(&uuid7_string_to_bytes(uuid7)).await? {
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

        let container = ContainerForModule {
            version: 2,
            auth_token: String::new(),
            module_instance_uuid7: String::new(),
            payload: Some(ModulePayload::MessagePostProcess(
                cockatiel_protobuf::MessagePostProcess {
                    message_uuid7: uuid7.to_string(),
                    raw_message: Some(ChatMessage {
                        platform: state.platform.clone(),
                        raw_data: state.raw_data.clone(),
                        raw_message: state.raw_message.clone(),
                        user_uuid7: state.user_uuid7.clone(),
                        command: state.parsed_command.clone(),
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

        // Post-process modules are display/archival CONSUMERS: they see EVERY
        // message, commands included — that is the whole point of a display
        // (term-chat, audit-viewer never register commands, yet must render
        // `!tts`, `!ban`, …). The only exception is a command-scoped module,
        // which handles only the command it owns and must never be sent a
        // message it ignores (that would charge its price gate for nothing).
        // So: every non-command-scoped post-process module + the command owner.
        let recipients: Vec<String> = {
            let registry = self.command_registry.lock().unwrap();
            let owner = state
                .parsed_command
                .as_ref()
                .and_then(|c| registry.owner(&c.command_flag, &c.command_name));
            cfg.post_process_modules
                .iter()
                .filter(|m| {
                    !registry.is_command_scoped(m)
                        || owner.as_deref() == Some(m.as_str())
                        || registry.wants_plain_messages(m)
                })
                .cloned()
                .collect()
        };

        let mut sent_any = false;
        for module_name in &recipients {
            // Cost gate: skip THIS post-process module if the user can't afford
            // its price / meet its min_rank (skip-that-module semantics).
            if !self.permit_module(&state.user_uuid7, module_name).await {
                eprintln!(
                    "[engine] skipped '{}' (post_process) — user lacks the score/rank the module requires",
                    module_name
                );
                continue;
            }
            match send_to_module(
                &self.module_senders,
                module_name,
                container.clone(),
                "post_process",
            )
            .await
            {
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

    /// If `uuid7` is held for audit, land the run's accumulated result on the
    /// row WITHOUT touching the hold, and report that the terminal path must
    /// stop. The state and the pending acks are left in place, exactly as the
    /// completion path always did. A read error propagates (fails closed): an
    /// unknown hold state must never let a held message be published.
    ///
    /// Every terminal path (complete / failed / dropped) funnels through this,
    /// so a critical-module timeout or a module abandon preserves an operator's
    /// hold instead of clobbering it with `failed`/`dropped`.
    async fn preserve_audit_hold(&self, uuid7: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);
        if !self.db.is_audited(&uuid7_bytes).await? {
            return Ok(false);
        }
        let held = self.result_snapshot(uuid7).await;
        self.db
            .write_terminal_outcome(&uuid7_bytes, &PipelineOutcome::AuditHeld, &held)
            .await?;
        Ok(true)
    }

    async fn mark_complete(&self, uuid7: &str) -> Result<(), Box<dyn std::error::Error>> {
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        // Held-for-audit messages stay held, not completed. The run's
        // accumulated result still lands on the row, but the hold itself is
        // untouched: only `release_audit` may release it.
        if self.preserve_audit_hold(uuid7).await? {
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

        // A critical timeout must not clobber an operator's hold: preserve it
        // and stop, exactly like a normal completion on a held message.
        if self.preserve_audit_hold(uuid7).await? {
            return Ok(());
        }

        // Everything the run got as far as producing lands with the failure —
        // the per-stage writes had already mirrored it onto the row by the time
        // the failure was recorded.
        let result = self.take_result(uuid7).await.unwrap_or_default();
        self.db
            .write_terminal_outcome(
                &uuid7_bytes,
                &PipelineOutcome::Failed(error.to_string()),
                &result,
            )
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
        // A module abandon must not clobber an operator's hold either.
        if self.preserve_audit_hold(uuid7).await? {
            return Ok(());
        }
        let Some(result) = self.take_result(uuid7).await else {
            return Ok(());
        };
        let uuid7_bytes = uuid7_string_to_bytes(uuid7);

        self.db
            .write_terminal_outcome(
                &uuid7_bytes,
                &PipelineOutcome::Dropped(reason.to_string()),
                &result,
            )
            .await?;

        let mut ack_guard = self.ack_tracker.lock().await;
        ack_guard.ack(uuid7);

        Ok(())
    }

    /// Re-read the message owner's enriched `user_data` after the pre-process
    /// stage. The ingest-time enrichment captures the user's score BEFORE any
    /// pre-process scorer module (score-messages) adjusted it, so without this
    /// the downstream stages — and displays like term-chat — would show a score
    /// one message behind. Best-effort: a failure leaves the ingest value.
    async fn refresh_user_data(&self, uuid7: &str) {
        let Some(client) = self.user_db.as_ref() else {
            return;
        };
        let (platform, user_uuid7, raw_data) = {
            let states = self.pipeline_states.lock().await;
            match states.get(uuid7) {
                Some(s) if !s.user_uuid7.is_empty() => {
                    (s.platform.clone(), s.user_uuid7.clone(), s.raw_data.clone())
                }
                _ => return,
            }
        };
        let mut chat = cockatiel_protobuf::ChatMessage {
            platform,
            user_uuid7,
            // Carry the adapter bytes so the post-process enrich re-applies the
            // donation css keys (it sees the same payload ingest did).
            raw_data,
            ..Default::default()
        };
        crate::enrich_chat_user(client, &self.stream_tracker, self, &mut chat).await;
        if chat.user_data.is_some() {
            let mut states = self.pipeline_states.lock().await;
            if let Some(state) = states.get_mut(uuid7) {
                state.user_data = chat.user_data;
            }
        }
    }

    pub async fn handle_ack(
        &self,
        uuid7: &str,
        module_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = self.config_snapshot().await;
        // Remove this module's entry AND learn whether anything is still pending
        // in ONE lock acquisition. The two facts have to come from the same
        // snapshot: two recipients acking near-simultaneously must not both see
        // "nothing pending" and both advance the stage.
        let completed_stage = {
            let mut ack_guard = self.ack_tracker.lock().await;
            ack_guard.ack_module(uuid7, module_name)
        };

        let (stage, elapsed_ms, still_pending) = match completed_stage {
            Some(v) => v,
            None => return Ok(()),
        };

        // Feed the module's rolling latency average — this is the data the TUI
        // shows as each module's current ms. Only count real completed
        // messages (an ack is the module saying "done"), so the average is
        // purely how long a module takes on the messages it actually handled.
        self.module_timings
            .lock()
            .await
            .record(module_name, elapsed_ms);

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
                    self.refresh_user_data(uuid7).await;
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

    /// Resend a stage message whose receipt ping never arrived. Returns:
    /// `Ok(true)` = resent + clock restarted, `Ok(false)` = the module isn't
    /// connected (nothing to resend to), `Err` = rebuild/send failure.
    async fn resend_stage(&self, entry: &PendingAck) -> Result<bool, Box<dyn std::error::Error>> {
        let state = {
            let states = self.pipeline_states.lock().await;
            states.get(&entry.uuid7).cloned()
        };
        let Some(state) = state else {
            // The message is gone (already completed/failed) — nothing to resend.
            return Ok(false);
        };

        let container = match entry.stage.as_str() {
            "pre_process" => ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::MessagePreProcess(
                    cockatiel_protobuf::MessagePreProcess {
                        message_uuid7: entry.uuid7.clone(),
                        raw_message: Some(ChatMessage {
                            platform: state.platform.clone(),
                            raw_data: state.raw_data.clone(),
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
            },
            "in_process" => ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::MessageInProcess(
                    cockatiel_protobuf::MessageInProcess {
                        message_uuid7: entry.uuid7.clone(),
                        raw_message: Some(ChatMessage {
                            platform: state.platform.clone(),
                            raw_data: state.raw_data.clone(),
                            raw_message: state.raw_message.clone(),
                            user_uuid7: state.user_uuid7.clone(),
                            command: state.parsed_command.clone(),
                            channel_id: state.channel_id.clone(),
                            user_data: state.user_data.clone(),
                        }),
                        processed_message: state.processed_message.clone(),
                        abandon_message: false,
                        audio: Vec::new(),
                        audio_type: String::new(),
                    },
                )),
            },
            "post_process" => ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::MessagePostProcess(
                    cockatiel_protobuf::MessagePostProcess {
                        message_uuid7: entry.uuid7.clone(),
                        raw_message: Some(ChatMessage {
                            platform: state.platform.clone(),
                            raw_data: state.raw_data.clone(),
                            raw_message: state.raw_message.clone(),
                            user_uuid7: state.user_uuid7.clone(),
                            command: state.parsed_command.clone(),
                            channel_id: state.channel_id.clone(),
                            user_data: state.user_data.clone(),
                        }),
                        processed_message: state.processed_message.clone(),
                        audio: Vec::new(),
                        audio_type: String::new(),
                    },
                )),
            },
            _ => return Ok(false),
        };

        let outcome = send_to_module(
            &self.module_senders,
            &entry.module_name,
            container,
            entry.stage.as_str(),
        )
        .await;
        match outcome {
            SendOutcome::Sent => {
                self.ack_tracker.lock().await.re_track(entry);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub async fn handle_timeout(&self) -> Result<(), Box<dyn std::error::Error>> {
        // PAUSE GATE (the sweep) — and the one that decides whether pausing can
        // ever LOSE a message. `sent_at` is wall-clock: a sweep that ran while
        // the engine was paused would find every in-flight stage long expired
        // and mark those messages FAILED, so a ten-minute pause would destroy
        // exactly the messages it was meant to be holding. So the sweep does not
        // advance at all while paused, and `resume` restarts every pending
        // ack's clock before running it — each in-flight message then gets its
        // FULL budget from the moment processing resumes, and one that really
        // has a dead module still times out. A dependency block gates the sweep
        // the same way (and `clear_block` + `resume` restart the clocks).
        if self.is_gated().await {
            return Ok(());
        }

        let cfg = self.config_snapshot().await;
        let timed_out = {
            let mut ack_guard = self.ack_tracker.lock().await;
            ack_guard.check_timeouts()
        };

        // Several entries for one uuid can expire together (a stage with more
        // than one silent module), and each carries the same post-drain "nothing
        // pending" snapshot. Advance the message at most ONCE per sweep, never
        // after a critical timeout failed it, and never while a resend left a
        // fresh entry pending.
        let critical_uuids: HashSet<String> = timed_out
            .iter()
            .filter(|(e, _)| cfg.critical_modules.contains(&e.module_name))
            .map(|(e, _)| e.uuid7.clone())
            .collect();
        let mut advanced: HashSet<String> = HashSet::new();
        let mut failed: HashSet<String> = HashSet::new();
        let mut resent: HashSet<String> = HashSet::new();

        for (entry, still_pending) in timed_out {
            // RECEIPT RESEND: the module never confirmed it received the stage
            // message. Resend it (capped) rather than assuming the module is
            // dead — a lost frame on a busy adapter is common, and the receipt
            // ping exists precisely so the engine can distinguish "didn't get
            // it" from "got it but hasn't finished". The result path (stage-echo)
            // is untouched: a module that DID send its receipt but is slow on
            // the result still takes the normal timeout below.
            if !entry.receipt_received && entry.resend_count < MAX_ACK_RESENDS {
                match self.resend_stage(&entry).await {
                    Ok(true) => {
                        // A resend re-tracks the entry, so the message is still
                        // pending even though this sweep drained it.
                        resent.insert(entry.uuid7.clone());
                        continue;
                    }
                    Ok(false) => {
                        // Resend could not be queued (module disconnected) —
                        // fall through to the normal timeout path.
                    }
                    Err(e) => {
                        eprintln!(
                            "[pipeline] resend to '{}' for stage '{}' failed: {}",
                            entry.module_name, entry.stage, e
                        );
                        continue;
                    }
                }
            }

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
                // Fail the message once, however many critical entries expired
                // together.
                if failed.insert(entry.uuid7.clone()) {
                    self.mark_failed(
                        &entry.uuid7,
                        &format!(
                            "Critical module '{}' timed out at stage '{}'",
                            entry.module_name, entry.stage
                        ),
                    )
                    .await?;
                }
            } else if !still_pending
                && !resent.contains(&entry.uuid7)
                && !critical_uuids.contains(&entry.uuid7)
                && advanced.insert(entry.uuid7.clone())
            {
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
                            self.refresh_user_data(&entry.uuid7).await;
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
        let mime = if audio_type.is_empty() {
            "audio/mpeg"
        } else {
            audio_type
        };
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

    /// Whether `module_name` owns a pending ack for `uuid7` at `stage`. A stage
    /// reply that fails this check is not the sender's to make: it must not be
    /// allowed to overwrite another in-flight message's processed text/audio.
    async fn owns_pending_stage(&self, uuid7: &str, module_name: &str, stage: &str) -> bool {
        self.ack_tracker
            .lock()
            .await
            .is_pending_for(uuid7, module_name, stage)
    }

    pub async fn handle_message_from_module(
        &self,
        container: &ContainerForEngine,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match &container.payload {
            Some(EnginePayload::MessagePreProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    // New message from an adapter (input) — ingest it into the
                    // pipeline with a fresh uuid7. This is the DB-as-queue entry.
                    if let Some(chat) = &msg.raw_message {
                        // Record presence for the streak/activity multiplier.
                        // Keyed on the user uuid the adapter (or the ingest-time
                        // enrich) supplied; a message with no user is ignored.
                        self.stream_tracker
                            .lock()
                            .unwrap()
                            .note_attendance(&chat.user_uuid7, now_ms());
                        // Detect an adapter donation once, here at ingest: the
                        // donation's facts land on the row's `flags` column and
                        // the raw bytes are carried forward to every stage.
                        let donation = DonationInfo::parse(&chat.raw_data);
                        // Global block enforcement: a user with an active ban
                        // (any `bans` entry not yet unbanned) is dropped before
                        // the pipeline ever starts — UNLESS the message invokes
                        // `appeal`, which must reach the block module so a
                        // blocked user can contest the block. The row has not
                        // been inserted yet (that is `insert_and_start`), so
                        // there is nothing to `mark_dropped`; we return without
                        // creating it.
                        //
                        // The `bans` check only fires for an ENRICHED message.
                        // The block cache is the unconditional gate: it is keyed
                        // by uuid7 AND by `platform`+the raw adapter identity,
                        // so a blocked user's plain message is dropped even when
                        // nothing fetched their user data (no display/catch-all
                        // connected). Both checks are kept.
                        let has_active_ban = chat
                            .user_data
                            .as_ref()
                            .map(|ud| ud.bans.iter().any(|b| !b.unbanned))
                            .unwrap_or(false);
                        let is_appeal = chat
                            .command
                            .as_ref()
                            .map(|c| c.command_name == "appeal")
                            .unwrap_or(false);
                        let cached_block =
                            self.is_identifier_blocked(&chat.platform, &chat.user_uuid7);
                        if (has_active_ban || cached_block) && !is_appeal {
                            return Ok(true);
                        }
                        let msg_in = PipelineMessage {
                            uuid7: uuid7_string().unwrap_or_default(),
                            platform: chat.platform.clone(),
                            event_type: 1,
                            raw_message: chat.raw_message.clone(),
                            command: None,
                            flags: donation.as_ref().map(DonationInfo::flags_string),
                            parsed_command: chat.command.clone(),
                            channel_id: chat.channel_id.clone(),
                            user_uuid7: chat.user_uuid7.clone(),
                            user_data: chat.user_data.clone(),
                            raw_data: chat.raw_data.clone(),
                        };
                        self.insert_and_start(msg_in).await?;
                        return Ok(true);
                    }
                    return Ok(true);
                }

                // Ownership gate: only a module with a tracked pending ack for
                // this uuid+stage may write the message's state. Without it any
                // authenticated module could overwrite another message's
                // processed text/audio before `handle_ack` noticed it had no
                // entry to clear.
                if !self
                    .owns_pending_stage(&msg.message_uuid7, &container.module_name, "pre_process")
                    .await
                {
                    return Ok(true);
                }

                let processed = msg
                    .raw_message
                    .as_ref()
                    .map(|cm| cm.raw_message.clone())
                    .unwrap_or_default();

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = processed;
                    }
                }

                self.store_audio(&msg.message_uuid7, "pre", &msg.audio_type, &msg.audio)
                    .await?;
                self.handle_ack(&msg.message_uuid7, &container.module_name)
                    .await?;
                Ok(true)
            }
            Some(EnginePayload::MessageInProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    return Ok(true);
                }

                if !self
                    .owns_pending_stage(&msg.message_uuid7, &container.module_name, "in_process")
                    .await
                {
                    return Ok(true);
                }

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = msg.processed_message.clone();
                    }
                }

                self.store_audio(&msg.message_uuid7, "in", &msg.audio_type, &msg.audio)
                    .await?;

                // The module handed the message back marked for abandonment: the
                // chain ends here instead of continuing to the next stage.
                if msg.abandon_message {
                    self.mark_dropped(
                        &msg.message_uuid7,
                        &format!("abandoned by module '{}'", container.module_name),
                    )
                    .await?;
                    return Ok(true);
                }

                self.handle_ack(&msg.message_uuid7, &container.module_name)
                    .await?;
                Ok(true)
            }
            Some(EnginePayload::MessagePostProcess(msg)) => {
                if msg.message_uuid7.is_empty() {
                    return Ok(true);
                }

                if !self
                    .owns_pending_stage(&msg.message_uuid7, &container.module_name, "post_process")
                    .await
                {
                    return Ok(true);
                }

                {
                    let mut states = self.pipeline_states.lock().await;
                    if let Some(state) = states.get_mut(&msg.message_uuid7) {
                        state.processed_message = msg.processed_message.clone();
                    }
                }

                self.store_audio(&msg.message_uuid7, "post", &msg.audio_type, &msg.audio)
                    .await?;
                self.handle_ack(&msg.message_uuid7, &container.module_name)
                    .await?;
                Ok(true)
            }
            Some(EnginePayload::MessageAck(ack)) => {
                if ack.message_uuid7.is_empty() {
                    return Ok(true);
                }
                // Receipt ping: the module confirms it RECEIVED the stage
                // message. This does NOT advance the pipeline — the result
                // (the stage-echo carrying the same message_uuid7) does. A
                // receipt lets the engine stop resending and wait for the
                // result instead.
                self.ack_tracker
                    .lock()
                    .await
                    .mark_receipt(&ack.message_uuid7, &container.module_name);
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
        let ack = Duration::from_millis(DEFAULT_ACK_TIMEOUT_MS as u64);
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
                    receipt_received: false,
                    resend_count: 0,
                },
                PendingAck {
                    uuid7: "uuid-1".into(),
                    stage: "pre_process".into(),
                    module_name: "live".into(),
                    sent_at: Instant::now(),
                    timeout: Duration::from_secs(30),
                    receipt_received: true,
                    resend_count: 0,
                },
            ],
        );
        let timed_out = tracker.check_timeouts();
        assert_eq!(timed_out.len(), 1, "only the expired entry times out");
        assert_eq!(timed_out[0].0.module_name, "slow");
        assert!(
            tracker.has_pending("uuid-1"),
            "the message must NOT be considered acked while 'live' is still pending"
        );

        // Once the survivor acks, the message is free to advance.
        assert_eq!(
            tracker.ack_module("uuid-1", "live").map(|(s, _, _)| s),
            Some("pre_process".into())
        );
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
        let e = &timed_out[0].0;
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
            receipt_received: true,
            resend_count: 0,
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
        assert!(
            rebased.has_pending("uuid-3"),
            "the stage is still waiting, not reaped"
        );

        // ...and a budget that really does expire from the rebase point still
        // does, so the rebase is a hand-back of credit, not an amnesty.
        rebased.track("uuid-3".to_string(), "post_process".into(), "tts".into(), 0);
        let timed_out = rebased.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].0.stage, "post_process");
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
        let sync_start = src
            .find("let mut interval = tokio::time::interval(sync_interval);")
            .expect("sync loop not found");
        let sweep_start = src
            .find("crate::pipeline::TIMEOUT_SWEEP_INTERVAL")
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
        let start = src
            .find("pub async fn handle_timeout(")
            .expect("handle_timeout not found");
        let end = start
            + src[start..]
                .find("\n    pub async fn ")
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
            t.record(i as f32);
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
