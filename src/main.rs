#![allow(clippy::type_complexity)]

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::{
    accept_async_with_config, tungstenite::protocol::{Message as WsMessage, WebSocketConfig},
    WebSocketStream,
};
use uuid::Uuid;

/* MODULES & CONFIG */
#[path = "./module_manager.rs"]
mod module_manager;
use module_manager::ModuleRegistry;

mod tls;

mod config;
use config::{Config, ConfigState, get_config, verify_config};

mod auth;
use auth::{AuthSession, AuthStore, verify_pin};

mod command_registry;
use command_registry::{CommandRegistry, parse_command};

mod module_registry;
use module_registry::ModuleRegistryPersistence;

mod auto_config;

mod prompts;

mod database;
use database::{DatabaseConfig, DatabaseManager};

mod credentials;

mod pipeline;
mod help;
use pipeline::{PipelineConfig, PipelineOrchestrator, SendOutcome};

/* QUERY SURFACE */
mod queries;
use queries::{QueryContext, SharedShutdown, ShutdownStage};

mod user_db_client;
mod rank_chart;
use user_db_client::{SharedUserDbClient, UserDbClient};

/* PROTOBUF STUFF */
pub use cockatiel_proto::proto as cockatiel_protobuf;

use cockatiel_protobuf::{
    container_for_engine::Payload as EnginePayload,
    container_for_module::Payload as ModulePayload,
    ContainerForEngine, ContainerForModule, ProcessPosition, Prompt, PromptType, Log,
    TimelineQueryResult,
};


/// Where a PromptResponse should be routed. The engine's own prompts (module
/// connection approval) wait on a oneshot; prompts originating from a module
/// are forwarded back to that module's connection.
pub(crate) enum PromptSink {
    Engine(oneshot::Sender<bool>),
    /// A module-originated prompt: (origin module name, response channel).
    Module(String, tokio::sync::mpsc::Sender<ContainerForModule>),
}

pub(crate) type SharedPromptRoutes = Arc<Mutex<HashMap<String, PromptSink>>>;

/// One channel's latest viewer/member stats, as reported by its adapter.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChannelStatsEntry {
    pub platform: String,
    pub channel: String,
    pub viewers: i32,
    pub is_live: bool,
    pub title: String,
    pub updated_at: i64,
}

/// The engine's in-memory store of per-channel viewer/member counts. Adapters
/// push their latest `ChannelStats` on each poll; other modules read them on
/// demand via the `channel_viewers` virtual query.
pub(crate) type SharedChannelStats = Arc<Mutex<HashMap<String, ChannelStatsEntry>>>;

/// The platform adapters allowed to publish channel stats. A random module
/// spoofing viewer counts would poison every consumer, so the relay gate keys
/// off these names.
pub(crate) const CHANNEL_STATS_ADAPTERS: [&str; 4] =
    ["twitch-adapter", "kick-adapter", "youtube-adapter", "discord-adapter"];

/// The outcome of asking the user via a broadcast prompt.
enum PromptOutcome {
    /// A UI answered y/n.
    Decided(bool),
    /// No module/UI was connected to ask.
    NoUi,
    /// A UI was connected but never answered within the timeout.
    TimedOut,
}

/// Per-peer-IP failed-PIN tracker: 5 consecutive failures lock that IP out for
/// 60s. A correct PIN resets the counter. Modules share one PIN, so an attacker
/// brute-forcing from a single address gets throttled while a legit module
/// (different IP) is unaffected.
#[derive(Default, Clone)]
struct PinGate {
    inner: Arc<Mutex<HashMap<std::net::IpAddr, PinAttempt>>>,
}

#[derive(Default)]
struct PinAttempt {
    failures: u32,
    locked_until_ms: i64,
}

impl PinGate {
    /// Whether a PIN attempt from `ip` is currently allowed.
    fn check(&self, ip: std::net::IpAddr) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let inner = self.inner.lock().unwrap();
        match inner.get(&ip) {
            Some(attempt) => attempt.locked_until_ms <= now,
            None => true,
        }
    }

    /// Record a wrong PIN. After 5 consecutive failures the IP is locked for
    /// 60s (checked via `locked_until_ms`).
    fn record_failure(&self, ip: std::net::IpAddr) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut inner = self.inner.lock().unwrap();
        let attempt = inner.entry(ip).or_default();
        attempt.failures = attempt.failures.saturating_add(1);
        if attempt.failures >= 5 {
            attempt.locked_until_ms = now + 60_000;
        }
    }

    /// Record a correct PIN: clear the per-IP failure state.
    fn record_success(&self, ip: std::net::IpAddr) {
        self.inner.lock().unwrap().remove(&ip);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ModuleEntry {
    pub name: String,
    pub priority: i32,
}

#[derive(Clone)]
pub struct ModuleInfo {
    pub name: String,
    pub instance_uuid7: String,
    pub priority: i32,
    pub process_position: String,
    pub connected_at: Option<i64>,
    pub shutdown_at: Option<i64>,
    pub sender: Option<tokio::sync::mpsc::Sender<ContainerForModule>>,
}

#[derive(Clone)]
pub struct TimelineDisplayEvent {
    pub id: String,
    pub timestamp: String,
    pub text: String,
}

#[derive(Clone)]
pub struct EngineState {
    pub modules: Vec<ModuleInfo>,
    pub timeline: Vec<TimelineDisplayEvent>,
    /// Best-effort channel for surfacing engine log lines to connected UIs.
    pub log_broadcast_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            modules: Vec::new(),
            timeline: Vec::new(),
            log_broadcast_tx: None,
        }
    }
}

/// Resource bounds for module connections, read from config once at startup:
/// WS message size cap, max concurrent connections, and the handshake / send
/// timeouts that keep a wedged socket from stalling a connection task forever.
#[derive(Clone)]
struct EngineBounds {
    max_message_bytes: usize,
    max_connections: usize,
    handshake_timeout_secs: u32,
    send_timeout_secs: u32,
}

pub fn log_event(state: &Arc<Mutex<EngineState>>, text: impl Into<String>) {
    let text = text.into();
    println!("[Cockatiel] {}", text);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".into());

    let mut state = state.lock().unwrap();

    state.timeline.push(TimelineDisplayEvent {
        id: Uuid::now_v7().to_string(),
        timestamp,
        text,
    });

    if state.timeline.len() > 100 {
        state.timeline.remove(0);
    }
}

/// Log an engine-originated line AND broadcast it to connected modules (UIs)
/// so it shows up live in the TUI log window. Module-originated Log/Err payloads
/// keep using `log_event` (their source is already surfaced by the UI).
pub fn log_event_broadcast(state: &Arc<Mutex<EngineState>>, text: impl Into<String>) {
    let text = text.into();
    log_event(state, text.clone());
    if let Some(tx) = state.lock().unwrap().log_broadcast_tx.clone() {
        let _ = tx.send(text);
    }
}

fn process_position_to_string(pos: ProcessPosition) -> String {
    match pos {
        ProcessPosition::Connection => "input",
        ProcessPosition::Preprocess => "preprocess",
        ProcessPosition::Inprocess => "inprocess",
        ProcessPosition::Postprocess => "postprocess",
        _ => "input",
    }
    .to_string()
}

/// Normalize a module manifest `capabilities` string to a canonical pipeline
/// stage name ("input"/"preprocess"/"inprocess"/"postprocess"). Known aliases
/// map to their stage; anything unrecognized is returned trimmed+lowercased
/// as-is so callers can tell it apart from a real stage.
fn normalize_capability(cap: &str) -> String {
    match cap.trim().to_lowercase().as_str() {
        "input" | "inputs" | "connection" => "input",
        "preprocess" | "pre" => "preprocess",
        "inprocess" | "process" => "inprocess",
        "postprocess" | "output" | "outputs" | "display" | "post" => "postprocess",
        other => other,
    }
    .to_string()
}

/// Best-effort startup warning when a module's manifest `capabilities`
/// (discovered_registry) normalizes to something OTHER than a known pipeline
/// stage. Known-stage capabilities are already honored by the approval-time
/// override in the connection flow (which trusts the manifest over the
/// requested process_position), so only genuinely unrecognized capabilities
/// warn here. Log-only — the module is still approved.
fn warn_stage_capability_mismatch(
    module_name: &str,
    discovered_registry: &Arc<Mutex<ModuleRegistry>>,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    let capability = discovered_registry
        .lock()
        .unwrap()
        .get(module_name)
        .map(|m| m.manifest.capabilities.trim().to_lowercase())
        .unwrap_or_default();
    if capability.is_empty() {
        return;
    }
    let normalized = normalize_capability(&capability);
    if !matches!(
        normalized.as_str(),
        "input" | "preprocess" | "inprocess" | "postprocess"
    ) {
        log_event_broadcast(
            ui_state,
            format!(
                "WARN: module '{}' manifests unrecognized capability '{}' — verify config.json pipeline placement",
                module_name, capability
            ),
        );
    }
}

fn module_search_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Ok(custom) = env::var("COCKATIEL_MODULE_PATHS") {
        paths.extend(env::split_paths(&custom));
    }

    paths.push(PathBuf::from("./modules"));
    paths.push(PathBuf::from("./cockatiel-engine/modules"));

    if let Ok(current) = env::current_dir() {
        paths.push(current.join("modules"));
        paths.push(current.join("cockatiel-engine/modules"));
        // Also check parent directory (for when engine runs from cockpit-engine-rs/)
        if let Some(parent) = current.parent() {
            paths.push(parent.join("modules"));
        }
    }

    paths.sort();
    paths.dedup();

    paths
}

/// Enrich a ChatMessage with user data from the user database. If the message
/// carries a non-empty `user_uuid7`, look the user up (by uuid or channel/handle)
/// and attach a populated `UserData` (username, roles, name color) so downstream
/// modules (e.g. term-chat) can display the user nicely.
/// What to do with a raw chat message after command classification.
#[derive(Debug)]
enum CommandAction {
    /// Attach this parsed command (known command — pipeline routes it).
    Attach(crate::cockatiel_protobuf::Command),
    /// Send the built-in help list back to the chat. `Some(arg)` = the module
    /// name after `!help`, for the per-module detail view.
    Help(Option<String>),
    /// Send the apology reply (unregistered command under an alerting flag).
    Alert,
    /// Nothing (not a flagged command, or a known command already attached).
    None,
}

/// Classify a raw chat message against the command registry. Runs entirely
/// synchronously so the std MutexGuard never crosses an await boundary.
fn classify_command(raw: &str, registry: &CommandRegistry) -> CommandAction {
    let raw = raw.trim();
    if raw.is_empty() {
        return CommandAction::None;
    }
    let lower = raw.to_lowercase();
    if lower == "!help" {
        return CommandAction::Help(None);
    }
    if lower.starts_with("!help ") {
        // `!help <module>` — the module name is everything after the token.
        let arg = raw[6..].trim().to_string();
        return CommandAction::Help(if arg.is_empty() { None } else { Some(arg) });
    }
    let Some(parsed) = parse_command(raw, registry) else {
        return CommandAction::None;
    };
    let flag = parsed.command.command_flag.clone();
    let name = parsed.command.command_name.clone();
    if registry.owner(&flag, &name).is_some() {
        return CommandAction::Attach(parsed.command);
    }
    if registry.alert_owner_for(&flag).is_some() {
        return CommandAction::Alert;
    }
    CommandAction::None
}

/// What the ingest-side command handling did to a message. The caller uses
/// this to decide whether the user-db lookup is warranted at all, so it must
/// distinguish "a registered command was attached to the message" from every
/// other case — see [`should_fetch_user_data`].
///
/// `!help` and the unknown-command apology are reported as
/// [`IngestCommandOutcome::EngineHandled`], NOT `Attached`, even though both
/// consume the message. Both are entirely engine-local replies that are built
/// from the command registry and the raw identifier already on the message
/// (the apology falls back to `chat.user_uuid7` precisely for the case where
/// `user_data` is absent), and neither forwards the message to a module. The
/// fetch condition in the ingest path is specifically about a *registered
/// command being attached*, so neither earns a user-db round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestCommandOutcome {
    /// A registered command was attached to `chat.command`; the pipeline
    /// routes it to its owning module (plus any catch-alls).
    Attached,
    /// Nothing was attached: either no command at all, or the engine handled
    /// the message itself (`!help` / the unknown-command apology).
    EngineHandled,
}

impl IngestCommandOutcome {
    /// Did a *registered* command get attached to the message? The one bit the
    /// fetch decision reads.
    pub fn command_attached(self) -> bool {
        matches!(self, Self::Attached)
    }
}

/// Should the engine spend two WebSocket round-trips to the user database
/// (`get_user` + `read_user_value` for `name_color`) on this message?
///
/// `enrich_chat_user` is the single most expensive thing on the ingest path and
/// the overwhelming majority of chat traffic never looks at the result, so the
/// lookup is gated on something downstream actually consuming `user_data`.
///
/// Fetch if and only if one of the three conditions holds:
///
/// 1. `command_attached` — a *registered* command was attached to the message.
///    Command modules (reprimand, commend, tts, …) need the actor's roles,
///    score and canonical id to authorise the action and attribute the rating,
///    so the lookup is mandatory on this path. This is why the ingest arm
///    classifies the command BEFORE asking this question.
/// 2. a catch-all module is registered — `CommandRegistry::catch_alls()` is
///    exactly the set of modules that registered an EMPTY `Commands` list, and
///    those receive EVERY message regardless of routing, so any of them may
///    read `user_data`.
/// 3. a post-process (output display) module is configured AND connected.
///    This condition is NOT padding. The display modules (`term-chat`,
///    `cockatiel-audit-viewer`) never register a `Commands` payload at all, so
///    they are completely invisible to the command registry and only receive
///    messages through the pipeline's post-process stage fanout driven by the
///    config. Without this condition every ordinary (non-command) chat message
///    would reach each display with `user_data == None` and lose its rank /
///    name-colour / score styling — a real regression.
///
///    Liveness matters for the same reason the lookup does: a display that is
///    configured but not connected reads nothing, so there is nothing to pay
///    for. `connected` is the live-sender view of the configured list (a sender
///    slot whose channel is already closed means the module is gone, which is
///    the same test `send_to_module` applies), so a dead display in the config
///    does not hold the hot path open.
pub fn should_fetch_user_data(
    command_attached: bool,
    registry: &CommandRegistry,
    cfg: &PipelineConfig,
    connected: &[String],
) -> bool {
    command_attached
        || !registry.catch_alls().is_empty()
        || cfg
            .post_process_modules
            .iter()
            .any(|name| connected.iter().any(|live| live == name))
}

/// Snapshot of the module names that are currently connected with a live
/// outbound channel. This is the liveness input to [`should_fetch_user_data`].
async fn connected_module_names(
    senders: &Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>>,
) -> Vec<String> {
    let senders = senders.lock().await;
    senders
        .iter()
        .filter(|(_, tx)| !tx.is_closed())
        .map(|(name, _)| name.clone())
        .collect()
}

/// Parse a raw chat message for registered commands and act:
/// - a known command is attached to `chat.command` (the pipeline routes it);
/// - `!help` lists every registered command back to the chat;
/// - an unregistered command under a flag whose owner set
///   `alert_on_unknown_command` gets the apology reply.
///
/// Returns what it did — the ingest path needs the `Attached` / not-`Attached`
/// distinction to decide whether to fetch user data, so classification has to
/// complete before the fetch decision is made.
pub async fn handle_command_on_ingest(
    chat: &mut cockatiel_protobuf::ChatMessage,
    registry: &Arc<Mutex<CommandRegistry>>,
    orchestrator: &PipelineOrchestrator,
    ui_state: &Arc<Mutex<EngineState>>,
) -> IngestCommandOutcome {
    let action = {
        let reg = registry.lock().unwrap();
        classify_command(&chat.raw_message, &reg)
    }; // guard dropped here — before any await.

    let mut outcome = IngestCommandOutcome::EngineHandled;
    match action {
        CommandAction::Attach(cmd) => {
            chat.command = Some(cmd);
            outcome = IngestCommandOutcome::Attached;
        }
        CommandAction::Help(arg) => {
            let reply = {
                let reg = registry.lock().unwrap();
                match arg {
                    Some(module) => match crate::help::format_module(&reg, &module) {
                        Some(text) => text,
                        None => format!(
                            "no module named '{module}' — try '!help' to see all modules"
                        ),
                    },
                    None => crate::help::format_overview(&reg),
                }
            };
            let platform = chat.platform.clone();
            let channel_id = chat.channel_id.clone();
            engine_reply_to_platform(orchestrator, ui_state, &platform, &channel_id, &reply).await;
        }
        CommandAction::Alert => {
            let who = if chat.user_data.as_ref().map(|u| !u.username.is_empty()).unwrap_or(false) {
                chat.user_data.as_ref().unwrap().username.clone()
            } else if !chat.user_uuid7.is_empty() {
                chat.user_uuid7.clone()
            } else {
                "user".to_string()
            };
            let platform = chat.platform.clone();
            let channel_id = chat.channel_id.clone();
            let reply = format!(
                "sorry {}, that command isn't valid, try '!help' to see all available commands",
                who
            );
            engine_reply_to_platform(orchestrator, ui_state, &platform, &channel_id, &reply).await;
        }
        CommandAction::None => {}
    }
    outcome
}

/// Send an engine-originated reply to a platform's adapters (no actor check —
/// this is the engine itself, not a human). Routes like SendToPlatforms.
pub async fn engine_reply_to_platform(
    orchestrator: &PipelineOrchestrator,
    ui_state: &Arc<Mutex<EngineState>>,
    platform: &str,
    channel_id: &str,
    msg: &str,
) {
    let targets: Vec<&str> = match platform {
        "all" => vec!["twitch-adapter", "kick-adapter", "youtube-adapter", "discord-adapter"],
        "twitch" => vec!["twitch-adapter"],
        "kick" => vec!["kick-adapter"],
        "youtube" => vec!["youtube-adapter"],
        "discord" => vec!["discord-adapter"],
        _ => return,
    };
    let send = cockatiel_protobuf::SendToPlatforms {
        msg: msg.to_string(),
        level: 0,
        module_uuid7: String::new(),
        pid: String::new(),
        platform: platform.to_string(),
        actor_platform: String::new(),
        actor_handle: "cockatiel".to_string(),
        actor_uuid7: String::new(),
        channel_id: channel_id.to_string(),
    };
    let container = ContainerForModule {
        version: 2,
        auth_token: String::new(),
        module_instance_uuid7: String::new(),
        payload: Some(ModulePayload::SendToPlatforms(send)),
    };
    for name in targets {
            let sent = crate::pipeline::send_to_module(&orchestrator.module_senders, name, container.clone(), "commands").await;
            if matches!(sent, SendOutcome::Sent) {
                log_event_broadcast(&ui_state, format!("[Commands] reply '{}' -> {}", name, msg));
            }
        }
}

pub async fn enrich_chat_user(
    client: &SharedUserDbClient,
    chat: &mut cockatiel_protobuf::ChatMessage,
) {
    if chat.user_uuid7.is_empty() {
        return;
    }
    let uid = chat.user_uuid7.clone();
    // If the identifier looks like a UUID, look up by uuid; otherwise treat it
    // as a platform handle/channel and resolve via find-by-channel.
    let looks_like_uuid = uid.len() == 36 && uid.chars().filter(|c| *c == '-').count() == 4;
    let resp = if looks_like_uuid {
        client.get_user(&uid, &chat.platform, "", "").await
    } else {
        client.get_user("", &chat.platform, &uid, &uid).await
    };
    let resp = match resp {
        Ok(r) => r,
        Err(_) => return,
    };
    if !resp.success {
        return;
    }
    let Some(user) = resp.user else {
        return;
    };

    // Name color: check a "name_color" user value (e.g. set by mod tools).
    let mut name_color = String::new();
    if let Ok(vr) = client.read_user_value(&user.uuid7, "name_color").await {
        if vr.success {
            if let Some(v) = vr.value {
                name_color = v.value;
            }
        }
    }

    let mut css = std::collections::HashMap::new();
    if !name_color.is_empty() {
        css.insert("color".to_string(), name_color);
    }
    // The numeric rank is 0-1 (numbers are for logic). The display tier NAME
    // comes from the shared root `rank_chart.json`; platform roles still win
    // over the mineral ladder.
    let rank_name = if user.is_owner {
        "owner".to_string()
    } else if user.is_admin {
        "admin".to_string()
    } else if user.is_moderator {
        "mod".to_string()
    } else if user.is_sponsor {
        "sponsor".to_string()
    } else {
        crate::rank_chart::tier_name(user.rank)
    };
    css.insert("rank".to_string(), rank_name);
    // Expose the numeric 0-1 rank so displays/gates can compare numbers without
    // loading the chart (e.g. term-chat's `image_min_rank` numeric gate).
    css.insert("rank_value".to_string(), format!("{:.6}", user.rank));
    // Expose the raw score too, so displays can apply their own trust level.
    css.insert("score".to_string(), user.score.to_string());
    // Expose the rating counters so displays can show a reprimand indicator.
    css.insert("reprimands".to_string(), user.reprimands.to_string());
    css.insert("commendations".to_string(), user.commendations.to_string());

    chat.user_uuid7 = user.uuid7.clone();
    chat.user_data = Some(cockatiel_protobuf::UserData {
        uuid: user.uuid7,
        username: user.username,
        is_sponsor: user.is_sponsor,
        is_moderator: user.is_moderator,
        is_admin: user.is_admin,
        is_owner: user.is_owner,
        bans: Vec::new(),
        commendations: Vec::new(),
        styling: Some(cockatiel_protobuf::UserStylingTemplate { css_properties: css }),
        platform_ids: std::collections::HashMap::new(),
    });
}

/// The TUI control surface is always trusted — otherwise it would block its
/// own connection waiting on a prompt it is supposed to show. The compliance
/// test runner is also trusted so it can run without an interactive prompt.
fn is_always_trusted(name: &str) -> bool {
    name == "cockatiel-tui"
        || name == "cockatiel-tui-child"
        || name == "cockatiel-test-runner"
    // NOTE: cockatiel-audit-viewer is intentionally NOT always-trusted. It
    // connects via the client SDK, which performs a first-contact handshake
    // with a fresh instance uuid on every connect — and the control-surface
    // auto-approve now requires the pinned uuid. As a read-only viewer it
    // doesn't need that status: it auto-approves by name like any registered
    // module (modules.json auto_auth), so it reconnects without a prompt.
}

/// True when a socket address is on the loopback interface (127.0.0.1/::1).
fn is_loopback_addr(addr: &std::net::SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// Startup remediation: tighten the engine's own secret files (`.env`,
/// `config.json`, `modules.json`, the TLS key) to owner-only 0o600. Best-effort
/// — a file that can't be chmodded (or doesn't exist yet) is logged, never fatal.
///
/// Run on the blocking pool: it touches a `std::sync::Mutex` (config_state) and
/// does file chmods — doing that inline on an async worker can deadlock against
/// a background task holding config_state (blocking std-Mutex locks on a tokio
/// worker must never wait on another task on the same worker).
fn remediate_secret_file_permissions(
    config_state: &Arc<Mutex<ConfigState>>,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    let targets = {
        let state = config_state.lock().unwrap();
        let dir = state
            .path
            .parent()
            .unwrap_or(&PathBuf::from("."))
            .to_path_buf();
        // Capture every target path under ONE brief lock; never hold the lock
        // across the file operations below.
        [
            config::env_path(&dir),
            state.path.clone(),
            dir.join("modules.json"),
            tls::key_path(),
        ]
    };
    for path in targets {
        if !path.exists() {
            continue;
        }
        if let Err(e) = config::chmod_owner_only(&path) {
            log_event_broadcast(
                ui_state,
                format!("WARN: could not tighten permissions on {}: {}", path.display(), e),
            );
        }
    }
}

/// Owner-perm override for privileged userdb mutations (set_roles, delete_user).
/// A TUI control-surface connection from loopback (the operator's own machine)
/// acts with owner perms regardless of any supplied actor; any other
/// connection must supply a valid actor (the remote TUI-login flow is a
/// separate, deferred task). The actor name-trust and control-surface gating
/// are unchanged.
pub(crate) fn userdb_actor_perm(peer_loopback: bool, actor_role: &str) -> String {
    if peer_loopback {
        "owner".to_string()
    } else {
        actor_role.to_string()
    }
}

/// The TUI (or a detached TUI sub-window) is the operator's control surface:
/// it may run tests, inspect/release held-audit messages, and access the user
/// database. The name is bound to the session's JWT, so this check is sound.
pub(crate) fn is_control_surface(name: &str) -> bool {
    name == "cockatiel-tui" || name == "cockatiel-tui-child"
}

/// Who may read OTHER modules' credential values from `module_list`.
/// The TUI control surface (operator) and term-chat (which legitimately reads
/// adapter client ids/secrets to drive its OAuth login flows) may see them;
/// every other module gets the structural list with `credential_values`
/// redacted, so one module cannot dump another module's secrets.
pub(crate) fn may_read_other_credentials(name: &str) -> bool {
    is_control_surface(name) || name == "term-chat"
}

/// The compliance test-runner may archive test results to the timeline.
pub(crate) fn is_test_runner(name: &str) -> bool {
    name == "cockatiel-test-runner"
}

/// Best-effort link to the setup/docs page relevant to the prompt's subject.
fn prompt_link_for(module_name: &str) -> String {
    match module_name {
        "twitch-adapter" => "https://dev.twitch.tv/console/apps".to_string(),
        "kick-adapter" => "https://kick.com/settings/developer".to_string(),
        "youtube-adapter" => "https://console.cloud.google.com/apis/credentials".to_string(),
        _ => "https://github.com/vulbyte/cockatiel".to_string(),
    }
}

/// Persist an engine/module lifecycle or log event into the timeline database
/// as an archival (already-complete) entry, so it is queryable without being
/// touched by the message pipeline.
async fn log_to_timeline(db: &DatabaseManager, kind: &str, source: &str, message: &str) {
    let flags = serde_json::json!({ "source": source, "kind": kind }).to_string();
    // Module lifecycle events are NOT platform sends — label them distinctly so
    // the `command` column stays meaningful (send_to_platforms vs lifecycle).
    if let Err(e) = db.insert_archival_event("engine", "module_lifecycle", message, &flags).await {
        eprintln!("[Timeline] failed to record {}: {}", kind, e);
    }
}

/// Broadcast a Prompt to all connected modules (UIs, e.g. the TUI / term-chat)
/// and wait for the first response. First responder wins.
async fn broadcast_prompt_and_wait(
    prompt: Prompt,
    module_senders: &Arc<
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>,
    >,
    prompt_routes: &SharedPromptRoutes,
    ui_state: &Arc<Mutex<EngineState>>,
    log_label: &str,
) -> PromptOutcome {
    let prompt_id = prompt.prompt_id_uuid7.clone();
    let timeout = if prompt.timeout > 0 { prompt.timeout } else { 30 };

    let senders: Vec<tokio::sync::mpsc::Sender<ContainerForModule>> = {
        let senders = module_senders.lock().await;
        senders.values().cloned().collect()
    };
    if senders.is_empty() {
        return PromptOutcome::NoUi;
    }

    let (tx, rx) = oneshot::channel();
    prompt_routes
        .lock()
        .unwrap()
        .insert(prompt_id.clone(), PromptSink::Engine(tx));

    let container = ContainerForModule {
        version: 2,
        auth_token: String::new(),
        module_instance_uuid7: String::new(),
        payload: Some(ModulePayload::Prompt(prompt)),
    };
    for sender in senders {
        // Best-effort broadcast: a full module queue must never wedge a prompt.
        let _ = sender.try_send(container.clone());
    }

    log_event(ui_state, log_label.to_string());

    let result = tokio::time::timeout(Duration::from_secs(timeout as u64), rx).await;
    prompt_routes.lock().unwrap().remove(&prompt_id);
    match result {
        Ok(Ok(decision)) => PromptOutcome::Decided(decision),
        // Channel dropped (UI gone) or timed out.
        Ok(Err(_)) | Err(_) => PromptOutcome::TimedOut,
    }
}

/// Ask the user (via a Prompt broadcast to connected modules, e.g. the TUI)
/// whether to allow a new module to connect. Returns Some(decision) when a UI
/// responded; None when no module was connected to ask, so the caller can fall
/// back to an interactive terminal prompt.
async fn prompt_user_to_allow(
    module_name: &str,
    uuid7: &str,
    position: &str,
    priority: u32,
    module_senders: &Arc<
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>,
    >,
    prompt_routes: &SharedPromptRoutes,
    ui_state: &Arc<Mutex<EngineState>>,
    discovered_registry: &Arc<Mutex<ModuleRegistry>>,
) -> Option<bool> {
    // Context for the prompt: the module's own description (from its manifest)
    // when available, plus a link to where the user can configure / learn more.
    let description = discovered_registry
        .lock()
        .unwrap()
        .get(module_name)
        .map(|m| m.manifest.description.clone())
        .filter(|d| !d.is_empty());
    let link = prompt_link_for(module_name);

    let mut details = format!(
        "Module '{}' [{}] wants to connect on position '{}' (priority {}).",
        module_name, uuid7, position, priority
    );
    if let Some(desc) = description {
        details.push_str(&format!("\n\nWhat it is: {}", desc));
    }

    let instructions = format!(
        "This is the first time '{}' has connected, or its access was reset.\nAllow it only if you recognize it and want it talking to the engine.\nOnce approved it will reconnect automatically in the future.",
        module_name
    );

    let prompt = Prompt {
        prompt_id_uuid7: Uuid::now_v7().to_string(),
        prompt: "Allow this module to connect?".to_string(),
        details,
        yes_dialog: "Allow".to_string(),
        no_dialog: "Deny".to_string(),
        timeout: 30,
        origin: module_name.to_string(),
        origin_uuid7: uuid7.to_string(),
        instructions,
        link,
        input_label: String::new(),
        prompt_type: PromptType::Boolean as i32,
    };

    match broadcast_prompt_and_wait(
        prompt,
        module_senders,
        prompt_routes,
        ui_state,
        &format!("Prompting user to allow module '{}'", module_name),
    )
    .await
    {
        PromptOutcome::Decided(decision) => Some(decision),
        PromptOutcome::TimedOut => Some(false),
        PromptOutcome::NoUi => None,
    }
}

/// Hold a message for audit and ask the user (via a Prompt to connected UIs)
/// to approve (resubmit as a normal message) or reject it. First response wins.
/// If no UI answers, the message stays held.
/// Human-readable broadcast line for a rejected message (shown in UIs/log).
fn compose_rejection_broadcast(origin: &str, uuid: &str, reason: &str, raw: &str, processed: &str) -> String {
    format!(
        "[{}] rejected {}: {} (\"{}\" -> \"{}\")",
        origin, uuid, reason, raw, processed
    )
}

/// Searchable timeline record for a rejected message (persisted to the DB).
fn compose_rejection_record(origin: &str, reason: &str, raw: &str, processed: &str) -> String {
    format!(
        "[rejected by {}] {} : \"{}\" -> \"{}\"",
        origin, reason, raw, processed
    )
}

/// A module reported it rejected a message. Surface it clearly (structured
/// broadcast, no buried log strings) and persist a searchable timeline record.
/// The message itself still flows as the module chose — this is an audit
/// record, not a pipeline stop.
async fn handle_chat_message_rejected(
    db: &DatabaseManager,
    ui_state: &Arc<Mutex<EngineState>>,
    rej: &cockatiel_protobuf::ChatMessageRejected,
) {
    let origin = &rej.origin;
    let reason = &rej.reason;
    let raw = &rej.message.as_ref().map(|m| m.raw_message.clone()).unwrap_or_default();
    let processed = rej.processed_message.as_deref().unwrap_or("");
    log_event_broadcast(
        ui_state,
        compose_rejection_broadcast(origin, &rej.message_uuid7, reason, raw, processed),
    );
    let platform = rej.message.as_ref().map(|m| m.platform.clone()).unwrap_or_default();
    let record = compose_rejection_record(origin, reason, raw, processed);
    if let Err(e) = db.insert_archival_event(&platform, "chat_rejected", &record, raw).await {
        log_event(ui_state, format!("[chat_rejected] failed to persist record: {}", e));
    }
}

/// Detached continuation of an audit flag: the timeline row was ALREADY marked
/// `audit` synchronously by the caller (so the hold lands before the flagging
/// module's ack can be processed); this only drives the operator prompt and the
/// eventual release.
async fn handle_audit_flag(
    db: &DatabaseManager,
    flag: &cockatiel_protobuf::AuditFlag,
    module_senders: &Arc<
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>,
    >,
    prompt_routes: &SharedPromptRoutes,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    let uuid_bytes = flag.message_uuid7.as_bytes().to_vec();

    let entry = db.get_audit_entry(&uuid_bytes).await.ok().flatten();
    let details = match &entry {
        Some(e) => format!(
            "[{}] {}: \"{}\"\n\nReason: {}",
            e.get("platform").and_then(|v| v.as_str()).unwrap_or("?"),
            e.get("user_uuid7").and_then(|v| v.as_str()).unwrap_or("?"),
            e.get("raw_message").and_then(|v| v.as_str()).unwrap_or(""),
            e.get("reason").and_then(|v| v.as_str()).unwrap_or("flagged"),
        ),
        None => format!(
            "Message [{}] held for audit (reason: {})",
            flag.message_uuid7, flag.reason
        ),
    };

let prompt = Prompt {
        prompt_id_uuid7: Uuid::now_v7().to_string(),
        prompt: "Audit message".to_string(),
        details,
        yes_dialog: "Approve".to_string(),
        no_dialog: "Reject".to_string(),
        timeout: 120,
        origin: if flag.origin.is_empty() {
            "engine".to_string()
        } else {
            flag.origin.clone()
        },
        origin_uuid7: String::new(),
        instructions: String::new(),
        link: format!(
            "http://localhost:3000/message/{}",
            flag.message_uuid7
        ),
        input_label: String::new(),
        prompt_type: PromptType::Boolean as i32,
    };

    let outcome = broadcast_prompt_and_wait(
        prompt,
        module_senders,
        prompt_routes,
        ui_state,
        &format!("[audit] prompt for message {}", flag.message_uuid7),
    )
    .await;

    match outcome {
        PromptOutcome::Decided(true) => {
            if db.release_audit(&uuid_bytes, true).await.is_ok() {
                log_event(ui_state, format!("[audit] approved + released: {}", flag.message_uuid7));
            }
        }
        PromptOutcome::Decided(false) => {
            if db.release_audit(&uuid_bytes, false).await.is_ok() {
                log_event(ui_state, format!("[audit] rejected: {}", flag.message_uuid7));
            }
        }
        _ => {
            log_event(ui_state, format!("[audit] held (no UI response): {}", flag.message_uuid7));
        }
    }
}

pub async fn broadcast_stage(
    modules: &Arc<Mutex<HashMap<String, ModuleInfo>>>,
    position: &str,
    container: &ContainerForModule,
    config_state: &Arc<Mutex<ConfigState>>,
) {
    let config = get_config(config_state);
    let mut entries = match position {
        "input" | "connections" => config.inputs.clone(),
        "preprocess" => config.preprocess_modules.clone(),
        "inprocess" => config.inprocess_modules.clone(),
        "postprocess" => config.postprocess_modules.clone(),
        _ => Vec::new(),
    };
    entries.sort_by_key(|entry| entry.priority);

    for entry in entries {
        let matching_senders: Vec<tokio::sync::mpsc::Sender<ContainerForModule>> = {
            let mods = modules.lock().unwrap();
            mods.values()
                .filter(|m| m.name == entry.name)
                .filter_map(|m| m.sender.clone())
                .collect()
        };
        for sender in matching_senders {
            let _ = sender.try_send(container.clone());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        r#"
                         X
        XXXXXXXXX      XXX
      XXXXXXXXXXXXXXXXXXX 
     XX    XXXXXXXXXXXXX  
  XXXX      XXXXXXXXXXXXXX
 XXXXXX    XXXXXXXXX XX   
   XXXXXXXXXXXXXXXXX      
     XXXXXXXXXXXXXXX      
     XXX XXXXXXX XXX      
     XX   XXXX    XX      
 
cockatiel
   -by vulbyte
"#
    );

    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let modules: Arc<Mutex<HashMap<String, ModuleInfo>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut discovered_registry_inner = ModuleRegistry::new();
    let search_paths = module_search_paths();
    log_event_broadcast(&ui_state, format!("Searching for modules in {} paths...", search_paths.len()));

    match discovered_registry_inner.discover(&search_paths) {
        Ok(()) => {}
        Err(errors) => {
            for error in errors {
                log_event_broadcast(&ui_state, format!("Module discovery: {}", error));
            }
        }
    }

    log_event_broadcast(&ui_state, format!("Discovered {} module(s)", discovered_registry_inner.len()));
    for (name, m) in discovered_registry_inner.iter() {
        let auto = if m.manifest.autostart { " [autostart]" } else { "" };
        let term = if m.manifest.terminal { " [terminal]" } else { "" };
        log_event_broadcast(&ui_state, format!("  - {}{}{}", name, auto, term));
    }

    let (config_string, config_path) = verify_config().await?;
    let config: Config = serde_json::from_str(&config_string)?;
    let config_size = fs::metadata(&config_path)?.len() as u32;
    let config_state = Arc::new(Mutex::new(ConfigState {
        path: config_path,
        last_size: config_size,
        config: config.clone(),
        pin: 0,
        jwt_secret: String::new(),
    }));

    // Secrets (PIN + JWT secret) come from `.env` (migrated out of a legacy
    // config.json that still carried them); config.json holds settings only.
    let jwt_secret = config::ensure_secrets(&config_state);
    // Backfill any newly-added settings keys into config.json (they already
    // resolved to code defaults in memory via #[serde(default)] — this makes
    // them explicit and editable on disk).
    config::backfill_config_defaults(&config_state);
    let auth_store = AuthStore::new(jwt_secret);
    let module_registry = ModuleRegistryPersistence::load(&config_state);

    // Auto-configure discovered modules: register them in modules.json, wire
    // them into the pipeline ordering, and give each a connection config.json
    // pointing at this engine (only filling missing keys — existing configs and
    // operator choices always win). This makes a fresh clone's ./modules/
    // self-sufficient without the TUI having to register/start each module.
    {
        let auto_log = auto_config::run(&config_state, &discovered_registry_inner);
        for line in &auto_log {
            log_event_broadcast(&ui_state, line.clone());
        }
    }

    let discovered_registry = Arc::new(Mutex::new(discovered_registry_inner));

    // Database
    let db_config = DatabaseConfig {
        local_path: PathBuf::from(&config.timeline_database_location),
        remote_url: Some(config.timeline_database_backup_location.clone()),
        sync_interval_secs: 15,
        local_target_mb: config.timeline_database_target_mb,
    };
    let db = DatabaseManager::new(db_config);
    db.initialize().await?;

    // User database client (remote-only service, engine-internal). The token is
    // a real secret — never fall back to a known constant. The TUI supervisor
    // generates one and passes it when it launches the engine.
    let user_db_host = env::var("USER_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let user_db_port: u16 = env::var("USER_DB_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(9736);
    let user_db_token = env::var("USER_DB_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "USER_DB_TOKEN is required: set it in the environment or run the \
                 engine through the TUI supervisor (which generates and passes it). \
                 Refusing to start with a default token.",
            )
        })?;
    let user_db_client: SharedUserDbClient = Arc::new(UserDbClient::new(&user_db_host, user_db_port, &user_db_token));
    log_event_broadcast(&ui_state, format!("UserDB client configured for {}:{}", user_db_host, user_db_port));

    // Pipeline
    let module_senders: Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    // Command registry: which modules subscribe to which chat commands
    // (populated by `commands_payload` registrations). Shared with the
    // pipeline for targeted command routing.
    let cmd_registry: Arc<Mutex<CommandRegistry>> = Arc::new(Mutex::new(CommandRegistry::default()));

    // Per-session kill switch: the probe task signals a hung module's
    // connection task to close, so the normal disconnect cleanup runs.
    let kill_map: Arc<
        tokio::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>,
    > = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    // Liveness probing: opportunistically AuthVerify modules during dead air.
    {
        let auth_store = auth_store.clone();
        let module_senders = Arc::clone(&module_senders);
        let discovered_registry = Arc::clone(&discovered_registry);
        let db = db.clone();
        let ui_state = Arc::clone(&ui_state);
        let kill_map = Arc::clone(&kill_map);
        let default_interval = config.module_probe_interval_secs;
        let default_response = config.module_probe_response_secs;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                // Garbage-collect sessions that were disconnected but whose
                // cleanup never finished (wedged connection task) — they would
                // otherwise linger forever and make dead modules look live.
                auth_store.sweep_shutdown_sessions(now_ms, 60_000);

                for session in auth_store.values() {
                    if session.unresponsive {
                        continue;
                    }
                    // Per-module override (manifest) falls back to config.
                    let (interval, response) = {
                        let reg = discovered_registry.lock().unwrap();
                        match reg.get(&session.module_name) {
                            Some(d) => {
                                let i = if d.manifest.unresponsive_timeout_secs > 0 {
                                    d.manifest.unresponsive_timeout_secs
                                } else {
                                    default_interval
                                };
                                let r = if d.manifest.probe_response_secs > 0 {
                                    d.manifest.probe_response_secs
                                } else {
                                    default_response
                                };
                                (i, r)
                            }
                            None => (default_interval, default_response),
                        }
                    };

                    // A pending probe whose window expired → unresponsive.
                    if session.probe_deadline_ms > 0 && now_ms >= session.probe_deadline_ms {
                        log_event_broadcast(
                            &ui_state,
                            format!(
                                "Module unresponsive: {} [{}] (no reply within {}s)",
                                session.module_name, session.instance_uuid7, response
                            ),
                        );
                        log_to_timeline(
                            &db,
                            "module_unresponsive",
                            &session.module_name,
                            &format!("no response to AuthVerify within {}s", response),
                        )
                        .await;
                        auth_store.mark_unresponsive(&session.instance_uuid7);
                        if let Some(kill) = kill_map
                            .lock()
                            .await
                            .get(&session.instance_uuid7)
                            .cloned()
                        {
                            let _ = kill.send(true);
                        }
                        continue;
                    }

                    // Dead air ≥ interval AND ≥ interval since the last probe →
                    // ask the module to prove it's alive with its auth token.
                    let interval_ms = (interval * 1000) as i64;
                    if now_ms - session.last_activity_ms >= interval_ms
                        && now_ms - session.last_probe_at_ms >= interval_ms
                    {
                        let deadline = now_ms + (response * 1000) as i64;
                        auth_store.mark_probed(&session.instance_uuid7, now_ms, deadline);
                        let probe = ContainerForModule {
                            version: 2,
                            auth_token: String::new(),
                            module_instance_uuid7: String::new(),
                            payload: Some(ModulePayload::AuthVerify(cockatiel_protobuf::AuthVerify {
                                cur_auth: String::new(),
                            })),
                        };
                        let _ = crate::pipeline::send_to_module(&module_senders, &session.module_name, probe, "probe").await;
                    }
                }
            }
        });
    }

    // Engine log broadcast: `log_event_broadcast` lines are forwarded to all
    // connected modules (UIs) as Log payloads so they surface live in the TUI.
    let (log_tx, mut log_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    {
        let mut es = ui_state.lock().unwrap();
        es.log_broadcast_tx = Some(log_tx);
    }
    {
        let module_senders = Arc::clone(&module_senders);
        tokio::spawn(async move {
            while let Some(line) = log_rx.recv().await {
                let container = ContainerForModule {
                    version: 2,
                    auth_token: String::new(),
                    module_instance_uuid7: String::new(),
                    payload: Some(ModulePayload::Log(Log {
                        log: line,
                        blob: vec![],
                    })),
                };
                let senders: Vec<_> = {
                    let senders = module_senders.lock().await;
                    senders.values().cloned().collect()
                };
                for sender in senders {
                    // Best-effort: a full module queue must never wedge the
                    // log broadcast (which would then back up unboundedly).
                    let _ = sender.try_send(container.clone());
                }
            }
        });
    }

    let pipeline_config = PipelineConfig {
        pre_process_modules: config.preprocess_modules.iter().map(|m| m.name.clone()).collect(),
        in_process_modules: config.inprocess_modules.iter().map(|m| m.name.clone()).collect(),
        post_process_modules: config.postprocess_modules.iter().map(|m| m.name.clone()).collect(),
        ack_timeout_ms: 3000,
        critical_modules: Vec::new(),
    };

    let mut orchestrator = PipelineOrchestrator::new(
        db.clone(),
        pipeline_config,
        module_senders.clone(),
        cmd_registry.clone(),
    );

    // Wire the module gates (authority / rank / price from the discovered
    // manifests) and the user database into the orchestrator, so a user must
    // meet the module's authority + rank and pay its price for the module to
    // run on their message.
    {
        let gates: std::collections::HashMap<String, crate::pipeline::ModuleGate> = {
            let reg = discovered_registry.lock().unwrap();
            reg.iter()
                .map(|(name, m)| {
                    (
                        name.clone(),
                        crate::pipeline::ModuleGate {
                            authority: m.manifest.authority,
                            min_rank: m.manifest.min_rank,
                            price: m.manifest.price,
                        },
                    )
                })
                .collect()
        };
        orchestrator.set_module_gates(gates);
        orchestrator.user_db = Some(user_db_client.clone());
    }

    // Boot PAUSED. The engine accepts module connections, ingests messages,
    // parses commands and writes the timeline exactly as it normally would, but
    // dispatches nothing to any module until the operator resumes it — so a
    // restart (or a boot with a backlog) can never let unattended messages
    // flood the modules. Nothing is lost while paused: every message is
    // persisted as 'queued' and replayed on resume.
    //
    // Headless runs — the compliance test runner, CI — must set
    // COCKATIEL_START_PAUSED=0 (or `start_paused: false` in config.json),
    // because nothing would ever press the resume key. The state is read ONCE,
    // here: pausing is an operator action, not a setting, so the config poll
    // task below deliberately does not touch it.
    if config::start_paused(&config) {
        orchestrator.pause().await;
        log_event_broadcast(&ui_state, "Pipeline: paused at startup — messages are queued but nothing is dispatched (resume from the TUI)");
    } else {
        log_event_broadcast(&ui_state, "Pipeline: running (COCKATIEL_START_PAUSED is off)");
    }

    // Normalize any TEXT-stored uuid7 rows to BLOB (all keyed queries bind
    // bytes; a BLOB param never equals a TEXT column in SQLite). Do this
    // before the recovery drain so stranded rows are found and progressed.
    match db.normalize_uuid_storage().await {
        Ok(n) if n > 0 => log_event_broadcast(&ui_state, format!("Recovery: normalized {} uuid columns to BLOB", n)),
        _ => {}
    }

    // Restart recovery: any 'processing' rows were mid-flight when the engine
    // died — they can never complete, so mark them failed. Stranded 'queued'
    // rows (inserted but never started) are drained by the recovery task
    // spawned below.
    match db.mark_all_processing_as_failed("interrupted by engine restart").await {
        Ok(count) if count > 0 => {
            log_event_broadcast(&ui_state, format!("Recovery: marked {} interrupted messages failed", count));
        }
        _ => {}
    }

    // Spawn sync + timeout task
    {
        let db = db.clone();
        let orchestrator = orchestrator.clone();
        let ui_state = ui_state.clone();
        let sync_interval = std::time::Duration::from_secs(15);
        const WAL_CHECKPOINT_MB: u64 = 5 * 1024 * 1024;
        // DB-size warning: fires once when the local file passes the 5 MB
        // floor and reaches 95% of the configured target; clears when it
        // drops back below.
        let warn_floor: u64 = 5 * 1024 * 1024;
        let mut db_warned = false;
        // Pipeline ack-timeout sweep, on its OWN tight interval.
        //
        // This used to ride along on the 15s DB-sync loop, which quantised every
        // stage that had to fall back on the timeout path to a 15s grid: measured
        // on the real timeline DB, a message spent p50 15s / p90 45s waiting for
        // `pre_process_completed_at` -> `persisted_at`, and the deltas landed on
        // exactly 15s and 30s boundaries. A module that misses its ack now waits
        // only the ack timeout plus at most one sweep tick.
        {
            let orchestrator = orchestrator.clone();
            let mut interval = tokio::time::interval(crate::pipeline::TIMEOUT_SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tokio::spawn(async move {
                loop {
                    interval.tick().await;
                    if let Err(e) = orchestrator.handle_timeout().await {
                        eprintln!("[Pipeline] Timeout check error: {}", e);
                    }
                }
            });
        }

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(sync_interval);
            let mut ticks_since_checkpoint = 0u32;
            loop {
                interval.tick().await;
                match db.sync_to_remote().await {
                    Ok(0) => {}
                    Ok(n) => println!("[Sync] Synced {} events to backup", n),
                    Err(e) => eprintln!("[Sync] Error: {}", e),
                }
                // Checkpoint the WAL when it exceeds ~5 MB, and also roughly
                // once a minute, so the log never grows unbounded.
                ticks_since_checkpoint += 1;
                if db.wal_size() > WAL_CHECKPOINT_MB || ticks_since_checkpoint >= 4 {
                    ticks_since_checkpoint = 0;
                    db.checkpoint_wal().await;
                }
                // Local DB size warning (5 MB floor, 95% of target).
                let size = db.db_size_bytes();
                let target = (db.target_mb() as u64) * 1024 * 1024;
                let over = size > warn_floor && size >= (target * 95) / 100;
                if over && !db_warned {
                    db_warned = true;
                    let pct = if target > 0 { (size * 100) / target } else { 0 };
                    log_event_broadcast(
                        &ui_state,
                        format!(
                            "WARNING: timeline DB is {:.1} MB ({pct}% of the {} MB target) — consider purging or raising timeline_database_target_mb",
                            size as f64 / 1048576.0,
                            db.target_mb()
                        ),
                    );
                } else if !over && db_warned {
                    db_warned = false;
                }
            }
        });
    }

    // Crash recovery: drain stranded 'queued' messages (rows the pipeline
    // inserted but never broadcast — e.g. a crash between insert and start).
    // Waits `recovery_grace_secs` so a live pipeline can claim them first; the
    // get_config read happens once, before the task, not inside the loop.
    //
    // ONE PASS, deliberately. This used to loop until `get_queued_uuids` came
    // back empty, which was only safe while a started message moved its row to
    // 'processing'. Nothing writes that status any more — a message the
    // pipeline owns stays 'queued' for its whole flight, since the terminal
    // write is the only one — so the loop re-selected the row it had just
    // re-driven, `recover_one` returned early because the message was already
    // in `pipeline_states`, the row was still 'queued', and the loop spun on
    // SELECTs against the timeline DB forever. One pass drains every stranded
    // row: all of them are in the snapshot, and a row inserted after it is one
    // the live pipeline owns and drives itself.
    {
        let orchestrator = orchestrator.clone();
        let ui_state = ui_state.clone();
        let grace = config::get_config(&config_state).recovery_grace_secs;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(grace as u64)).await;
            match orchestrator.drain_queued_once().await {
                Ok(drain) => {
                    for (uuid, error) in drain.failures {
                        log_event_broadcast(&ui_state, format!("Recovery failed for {}: {}", uuid, error));
                    }
                    log_event_broadcast(&ui_state, format!("Recovery: drained {} stranded 'queued' message(s) of {}", drain.claimed, drain.considered));
                }
                Err(e) => log_event_broadcast(&ui_state, format!("Recovery error: {}", e)),
            }
        });
    }

    // Config poll task: watch config.json ordering lists and push updates to
    // the orchestrator so the message chain order can change at runtime.
    {
        let config_state = Arc::clone(&config_state);
        let orchestrator = orchestrator.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
            loop {
                interval.tick().await;
                // Bypass the size gate: a TUI stage move can rewrite the
                // ordering lists to the SAME byte length, and the gate would
                // silently skip that. Force a re-read every poll so ordering
                // changes apply live without a restart.
                config::refresh_config(&config_state);
                let config = get_config(&config_state);
                let updated = PipelineConfig {
                    pre_process_modules: config.preprocess_modules.iter().map(|m| m.name.clone()).collect(),
                    in_process_modules: config.inprocess_modules.iter().map(|m| m.name.clone()).collect(),
                    post_process_modules: config.postprocess_modules.iter().map(|m| m.name.clone()).collect(),
                    ack_timeout_ms: 3000,
                    critical_modules: Vec::new(),
                };
                orchestrator.set_config(updated).await;
            }
        });
    }

let bind_ip = env::var("COCKATIEL_BIND_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let listener = TcpListener::bind(format!("{}:{}", bind_ip, config.port)).await?;

    // The engine only accepts WSS:// — every connection must complete a TLS
    // handshake against the engine's self-signed cert (generated + persisted
    // under `tls/`). A plain ws:// connection fails the handshake and is
    // dropped before any Cockatiel frame is exchanged.
    let (tls_acceptor, cert_path) = match tls::build_tls_acceptor() {
        Ok(pair) => pair,
        Err(e) => {
            log_event_broadcast(&ui_state, format!("FATAL: could not set up TLS: {}", e));
            return Err(e.into());
        }
    };
    // Do the chmod pass on the blocking pool (std-Mutex locks + file syscalls
    // must not run on an async worker — they can deadlock with background
    // tasks holding the same locks).
    {
        let config_state = Arc::clone(&config_state);
        let ui_state = ui_state.clone();
        tokio::task::spawn_blocking(move || {
            remediate_secret_file_permissions(&config_state, &ui_state);
        })
        .await
        .map_err(|e| format!("remediation task failed: {}", e))?;
    }
    // Never print the actual PIN (it lives in `.env` as COCKATIEL_PIN).
    log_event_broadcast(&ui_state, format!("Listening on {}:{} (WSS) | PIN: see .env (COCKATIEL_PIN)", bind_ip, config.port));
    log_event_broadcast(&ui_state, format!("TLS certificate: {}", cert_path.display()));

    let prompt_routes: SharedPromptRoutes = Arc::new(Mutex::new(HashMap::new()));

    // Per-channel viewer/member counts, fed by the platform adapters and read
    // by other modules via the `channel_viewers` virtual query.
    let channel_stats: SharedChannelStats = Arc::new(Mutex::new(HashMap::new()));

    // Control-surface shutdown signal. Parked at Idle until an accepted
    // `engine_shutdown` request moves it to Requested; the accept loop below
    // watches for that and, once the answer has reached the TUI's socket
    // (Answered), exits cleanly. The two-stage handoff is what guarantees the
    // TUI is told "yes" before the process goes away — see `queries::ShutdownSignal`.
    let shutdown = queries::ShutdownSignal::new();

    // Per-IP failed-PIN throttle (5 strikes -> 60s lockout).
    let pin_gate = PinGate::default();

    // Resource bounds read once from config: WS message size cap, connection
    // ceiling, and the handshake/send timeouts. (recovery_grace_secs is read
    // by the recovery task above.)
    let bounds = EngineBounds {
        max_message_bytes: config.max_message_bytes as usize,
        max_connections: config.max_connections,
        handshake_timeout_secs: config.handshake_timeout_secs,
        send_timeout_secs: config.send_timeout_secs,
    };
    // Connection ceiling: each spawned connection holds one permit for its
    // whole lifetime; when the ceiling is reached, new connections are dropped
    // before the (expensive) TLS handshake instead of piling up.
    let conn_sem = Arc::new(tokio::sync::Semaphore::new(bounds.max_connections));

    // A control-surface shutdown request breaks this loop. `accept()` is
    // cancel-safe and the watch receiver only resolves on a NEW value, so losing
    // the race to an incoming connection costs nothing.
    let mut shutdown_stage = shutdown.subscribe();
    let mut watching_shutdown = true;
    loop {
        let accepted = tokio::select! {
            changed = shutdown_stage.changed(), if watching_shutdown => {
                match changed {
                    // Any stage past Idle releases the exit.
                    Ok(()) if *shutdown_stage.borrow_and_update() != ShutdownStage::Idle => {
                        log_event_broadcast(
                            &ui_state,
                            "[engine] shutdown requested by the control surface — closing the listener",
                        );
                        break;
                    }
                    // The signal's owner is gone, so it can never report
                    // anything again: stop watching rather than spin on the
                    // error this returns immediately and forever.
                    Err(_) => watching_shutdown = false,
                    Ok(()) => {}
                }
                continue;
            }
            accepted = listener.accept() => accepted,
        };
        let (stream, address) = accepted?;
        // Refuse above the connection ceiling — drop the socket outright.
        let permit = match conn_sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                log_event_broadcast(&ui_state, "Connection limit reached");
                drop(stream);
                continue;
            }
        };
        // Capture the peer before the TLS handshake (the TlsStream doesn't
        // expose peer_addr directly).
        let peer_loopback = is_loopback_addr(&address);
        let peer_ip = address.ip();
        let tls_acceptor = tls_acceptor.clone();
        let bounds = bounds.clone();
        // Clone shared state per iteration so the async closure never moves
        // a variable still borrowed by the loop.
        let config_state = Arc::clone(&config_state);
        let modules = Arc::clone(&modules);
        let ui_state = Arc::clone(&ui_state);
        let auth_store = auth_store.clone();
        let module_registry = module_registry.clone();
        let orchestrator = orchestrator.clone();
        let db = db.clone();
        let discovered_registry = Arc::clone(&discovered_registry);
        let user_db_client = Arc::clone(&user_db_client);
        let prompt_routes = Arc::clone(&prompt_routes);
        let channel_stats = Arc::clone(&channel_stats);
        let kill_map = Arc::clone(&kill_map);
        let cmd_registry = Arc::clone(&cmd_registry);
        let pin_gate = pin_gate.clone();
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            // The permit is held for the connection's lifetime (dropping it
            // frees a slot in the connection ceiling).
            let _permit = permit;
            // Bound the TLS handshake: a client that connects but never
            // completes it must not squat a task (and a connection slot)
            // forever.
            match tokio::time::timeout(
Duration::from_secs(bounds.handshake_timeout_secs as u64),
                tls_acceptor.accept(stream),
            )
            .await
            {
                Ok(Ok(tls_stream)) => {
                    log_event_broadcast(&ui_state, format!("TLS connection from {}", address));
                    if let Err(error) = handle_connection(
                        tls_stream,
                        peer_loopback,
                        peer_ip,
                        pin_gate,
                        bounds,
                        config_state,
                        modules,
                        ui_state.clone(),
                        auth_store,
                        module_registry,
                        orchestrator,
                        db,
                        discovered_registry,
                        user_db_client,
                        prompt_routes,
                        channel_stats,
                        kill_map,
                        cmd_registry,
                        shutdown,
                    )
                    .await
                    {
                        log_event_broadcast(&ui_state, format!("Connection error from {}: {}", address, error));
                    }
                }
                Ok(Err(e)) => {
                    // Plain ws:// (or a bogus handshake) lands here.
                    log_event_broadcast(&ui_state, format!("Rejected non-TLS connection from {}: {}", address, e));
                }
                Err(_) => {
                    log_event_broadcast(&ui_state, format!("TLS handshake timed out from {}", address));
                }
            }
        });
    }

    // ── Graceful exit ─────────────────────────────────────────────────────
    // The listener is closed, so nothing new can connect; the connection tasks
    // still hold their sockets. Wait for the answer to the shutdown request to
    // be confirmed on the wire before the process goes away — the 5s bound is a
    // backstop, not the plan: the answer is normally written in the very next
    // loop iteration of the connection that asked.
    if tokio::time::timeout(Duration::from_secs(5), shutdown.wait_answered())
        .await
        .is_err()
    {
        log_event_broadcast(
            &ui_state,
            "WARN: shutdown: the response could not be confirmed on the socket within 5s — exiting anyway",
        );
    }
    // Written as an ordinary shutdown, not a crash: the TUI supervisor sees a
    // clean exit status, and its own restart policy (not this one) decides
    // whether the engine comes back.
    log_event_broadcast(
        &ui_state,
        "[engine] shutdown complete — exiting cleanly (a restart comes back paused; nothing is lost while paused)",
    );
    println!("[Cockatiel] engine shutdown: exiting cleanly");
    // `exit`, not a `return`: unwinding the runtime would tear down the turso
    // connection and every spawned task, and a panic in a Drop on the way out
    // would turn a clean exit into a non-zero one.
    std::process::exit(0);
}

/// Monotonic id identifying the socket a session is currently bound to. A
/// reconnect claims a session under a new id; a stale socket's cleanup can
/// then detect it no longer owns the session and must not evict it.
static NEXT_SOCKET_TOKEN: AtomicU32 = AtomicU32::new(1);

/// Send a WebSocket message with a bounded wait so a wedged socket can't stall
/// the connection task indefinitely. A timeout (or a send error) is treated as
/// a send failure — callers decide whether to `?`/break and let cleanup run.
async fn bounded_ws_send<S>(
    websocket: &mut WebSocketStream<S>,
    msg: WsMessage,
    timeout_secs: u32,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(Duration::from_secs(timeout_secs as u64), websocket.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err("outbound WebSocket send timed out".into()),
    }
}

async fn handle_connection<S>(
    stream: S,
    peer_loopback: bool,
    peer_ip: std::net::IpAddr,
    pin_gate: PinGate,
    bounds: EngineBounds,
    config_state: Arc<Mutex<ConfigState>>,
    modules: Arc<Mutex<HashMap<String, ModuleInfo>>>,
    ui_state: Arc<Mutex<EngineState>>,
    auth_store: AuthStore,
    module_registry: ModuleRegistryPersistence,
    orchestrator: PipelineOrchestrator,
    db: DatabaseManager,
    discovered_registry: Arc<Mutex<ModuleRegistry>>,
    user_db_client: SharedUserDbClient,
    prompt_routes: SharedPromptRoutes,
    channel_stats: SharedChannelStats,
    kill_map: Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
    cmd_registry: Arc<Mutex<CommandRegistry>>,
    shutdown: SharedShutdown,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let socket_token = NEXT_SOCKET_TOKEN.fetch_add(1, Ordering::Relaxed);
    let mut websocket = accept_async_with_config(
        stream,
        Some(WebSocketConfig {
            max_message_size: Some(bounds.max_message_bytes),
            max_frame_size: Some(bounds.max_message_bytes),
            ..Default::default()
        }),
    )
    .await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ContainerForModule>(64);

    // Loopback status is computed from the pre-TLS peer address (passed in) —
    // a TlsStream doesn't expose peer_addr directly.

    // ── Await ConnectionRequest ─────────────────────────────────────────
    // Bound the wait for the first message: a client that connects (TLS done)
    // but never speaks must not squat the connection task (or a slot) forever.
    let first_msg = match tokio::time::timeout(
        Duration::from_secs(bounds.handshake_timeout_secs as u64),
        websocket.next(),
    )
    .await
    {
        Ok(Some(msg)) => msg,
        Ok(None) | Err(_) => return Ok(()),
    };
    let first_msg = first_msg?;
    let WsMessage::Binary(data) = first_msg else {
        log_event_broadcast(&ui_state, "Rejected: first message was not binary");
        return Ok(());
    };

    let container = ContainerForEngine::decode(data.as_ref())?;

    let Some(EnginePayload::ConnectionRequest(request)) = container.payload.as_ref() else {
        log_event(
            &ui_state,
            format!(
                "Rejected: first message from '{}' was not a ConnectionRequest",
                container.module_name
            ),
        );
        log_to_timeline(&db, "module_reject", &container.module_name, "first message was not a ConnectionRequest").await;
        return Ok(());
    };

    // ── Check if this is a reconnection (has auth_token) or new (has PIN) ──

    // These will be set by either branch
    let module_name;
    let assigned_uuid;

    if !container.auth_token.is_empty() {
        // Reconnection: verify auth token AND that the claimed module name
        // matches the name the token was issued to.
        let token_valid = auth_store.verify_token(
            &container.module_instance_uuid7,
            &container.auth_token,
            &container.module_name,
        );
        if !token_valid {
            log_event(
                &ui_state,
                format!("Rejected: invalid auth token from '{}'", container.module_name),
            );
            log_to_timeline(&db, "module_reject", &container.module_name, "invalid auth token").await;
            websocket.close(None).await?;
            return Ok(());
        }
        log_event(
            &ui_state,
            format!("Reconnected: {} [{}]", container.module_name, container.module_instance_uuid7),
        );
        module_name = container.module_name.clone();
        assigned_uuid = container.module_instance_uuid7.clone();
        let reconnect_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        auth_store.update_activity(&assigned_uuid, reconnect_now);
        // A reconnect may arrive from a different interface — refresh the
        // recorded loopback status.
        auth_store.set_peer_loopback(&assigned_uuid, peer_loopback);
        // Bind the session to THIS socket: the old socket's disconnect
        // cleanup will see a mismatched socket_token and must not evict it.
        auth_store.claim_socket(&assigned_uuid, socket_token, reconnect_now);
        // If the old socket's cleanup already removed the session, rebuild it
        // as authenticated — the JWT is valid and bound to this module, and
        // position/priority ride along on the ConnectionRequest.
        if auth_store.get(&assigned_uuid).is_none() {
            let position = process_position_to_string(
                ProcessPosition::try_from(request.process_position).unwrap_or(ProcessPosition::Unspecified),
            );
            auth_store.insert(AuthSession {
                module_name: container.module_name.clone(),
                instance_uuid7: assigned_uuid.clone(),
                auth_token: container.auth_token.clone(),
                position,
                priority: request.priority as i32,
                authenticated: true,
                connected_at: Some(reconnect_now),
                shutdown_at: None,
                last_activity_ms: reconnect_now,
                last_probe_at_ms: 0,
                probe_deadline_ms: 0,
                unresponsive: false,
                peer_loopback,
                socket_token,
            });
        }

        // Ack the handshake exactly like the PIN path does: a reconnecting
        // client (the TUI reconnects with its persisted token + uuid) waits for
        // a ConnectionRequestReturn before entering its read loop. Without it,
        // the TUI's handshake never completes and it reconnects forever.
        let response = ContainerForModule {
            version: 2,
            auth_token: container.auth_token.clone(),
            module_instance_uuid7: assigned_uuid.clone(),
            payload: Some(ModulePayload::ConnectionRequestReturn(
                cockatiel_protobuf::ConnectionRequestReturn {
                    new_port: 0,
                    module_instance_uuid7: assigned_uuid.clone(),
                },
            )),
        };
        let mut bytes = Vec::new();
        response.encode(&mut bytes)?;
        bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
    } else {
        // New connection: validate PIN (throttled per peer IP — 5 consecutive
        // failures lock the address out for 60s to blunt brute-force).
        if !pin_gate.check(peer_ip) {
            log_event(
                &ui_state,
                format!("PIN locked out for {}", peer_ip),
            );
            let response = ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::ConnectionRequestReturn(
                    cockatiel_protobuf::ConnectionRequestReturn {
                        new_port: 0,
                        module_instance_uuid7: String::new(),
                    },
                )),
            };
            let mut bytes = Vec::new();
            response.encode(&mut bytes)?;
            bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
            websocket.close(None).await?;
            return Ok(());
        }
        if !verify_pin(request.pin, config::get_pin(&config_state)) {
            pin_gate.record_failure(peer_ip);
            log_event(
                &ui_state,
                format!("Rejected: invalid PIN from '{}'", container.module_name),
            );
            log_to_timeline(&db, "module_reject", &container.module_name, "invalid PIN").await;
            let response = ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::ConnectionRequestReturn(
                    cockatiel_protobuf::ConnectionRequestReturn {
                        new_port: 0,
                        module_instance_uuid7: String::new(),
                    },
                )),
            };
            let mut bytes = Vec::new();
            response.encode(&mut bytes)?;
            bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
            websocket.close(None).await?;
            return Ok(());
        }
        pin_gate.record_success(peer_ip);

        // Containment: reject the placeholder "unnamed_module" identity. The
        // client defaults to it when no name is configured, and if two modules
        // ever connected under it they would silently share one routing slot
        // and one JWT. The supervisor always passes --name; a module connecting
        // with a blank/placeholder name is a misconfiguration.
        let claimed = container.module_name.trim();
        if claimed.is_empty() || claimed == "unnamed_module" {
            log_event(
                &ui_state,
                format!(
                    "Rejected: module connected without a valid name ('{}'). The supervisor passes --name.",
                    container.module_name
                ),
            );
            log_to_timeline(&db, "module_reject", claimed, "blank/unnamed module identity").await;
            let response = ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::ConnectionRequestReturn(
                    cockatiel_protobuf::ConnectionRequestReturn {
                        new_port: 0,
                        module_instance_uuid7: String::new(),
                    },
                )),
            };
            let mut bytes = Vec::new();
            response.encode(&mut bytes)?;
            bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
            websocket.close(None).await?;
            return Ok(());
        }

        module_name = container.module_name.clone();
        let requested_uuid = request.module_instance_uuid7.clone();
        let mut position = process_position_to_string(ProcessPosition::try_from(request.process_position).unwrap_or(ProcessPosition::Unspecified));
        let priority = request.priority as i32;

        // Trust the NORMALIZED manifest capability over the requested
        // process_position at approval time: adapters (twitch/kick/youtube/
        // discord) declare `capabilities: "input"` in their manifests while
        // their client config requests process_position=Preprocess, which
        // would otherwise land them in preprocessModules (redundantly with
        // inputs) and spam the stage-mismatch warning on every boot. When the
        // manifest declares a known stage, that stage wins for the registration
        // (AuthSession, module_registry, config.json). Unknown/empty
        // capabilities keep the requested position.
        let requested_position = position.clone();
        let manifest_capability = discovered_registry
            .lock()
            .unwrap()
            .get(&module_name)
            .map(|m| m.manifest.capabilities.trim().to_lowercase())
            .unwrap_or_default();
        if !manifest_capability.is_empty() {
            let manifest_position = normalize_capability(&manifest_capability);
            if matches!(
                manifest_position.as_str(),
                "input" | "preprocess" | "inprocess" | "postprocess"
            ) {
                position = manifest_position;
                log_event_broadcast(
                    &ui_state,
                    format!(
                        "[{}] registered as {} (manifest capability) instead of requested {}",
                        module_name, position, requested_position
                    ),
                );
            }
        }

        assigned_uuid = {
            let mods = modules.lock().unwrap();
            if requested_uuid.is_empty() || mods.contains_key(&requested_uuid) {
                loop {
                    let id = Uuid::now_v7().to_string();
                    if !mods.contains_key(&id) {
                        break id;
                    }
                }
            } else {
                requested_uuid.clone()
            }
        };

        // Approve known/auto-authed modules outright. Two different trust
        // models apply:
        //  - Control-surface names (cockatiel-tui, -test-runner,
        //    -audit-viewer): auto-approved ONLY when the connecting module
        //    presents the exact instance uuid the engine pinned for that name
        //    (a PIN-holder must not mint a control-surface identity by name
        //    alone). EXCEPTION: the very first registration of such a name
        //    (fresh-install bootstrap) is auto-approved so the TUI can connect
        //    once and approve everyone else — after that registration the uuid
        //    is pinned.
        //  - Regular registered modules: name-only auto-approval. The client
        //    SDK connects FIRST-CONTACT with a fresh uuid and empty auth_token
        //    on every reconnect (it never replays a stored identity), so
        //    requiring the pinned uuid here would break every module reconnect.
        // Everything else goes through a Prompt routed to connected modules
        // (e.g. the TUI); if no UI is connected, fall back to an interactive
        // terminal prompt.
        // Refresh the registry first: the TUI may have registered this module
        // at runtime (e.g. a duplicated module) since the engine booted.
        module_registry.refresh();
        let registry_entry = module_registry.find(&module_name);
        let auto_approve = if is_always_trusted(&module_name) {
            // Control-surface names: only the pinned instance uuid may auto-approve
            // (a PIN-holder must not mint a control-surface identity by name alone),
            // EXCEPT on the very first registration of such a name (fresh-install
            // bootstrap so the TUI can connect once and approve everyone else).
            let uuid_matches = registry_entry
                .as_ref()
                .map(|e| e.instance_uuid7 == assigned_uuid)
                .unwrap_or(false);
            uuid_matches || registry_entry.is_none()
        } else {
            // Regular registered module: name-only auto-approval. The client SDK
            // connects first-contact with a fresh uuid on EVERY reconnect (it never
            // replays a stored identity), so requiring the pinned uuid here would
            // break every module reconnect.
            module_registry.is_known_and_auto_auth(&module_name)
        };
        let approved = if auto_approve {
            log_event_broadcast(&ui_state, format!("Auto-approving {}", module_name));
            true
        } else {
            match prompt_user_to_allow(
                &module_name,
                &assigned_uuid,
                &position,
                priority as u32,
                &orchestrator.module_senders,
                &prompt_routes,
                &ui_state,
                &discovered_registry,
            )
            .await
            {
                Some(true) => true,
                Some(false) => false,
                None => {
                    let auth_info = prompts::ModuleAuthInfo {
                        name: module_name.clone(),
                        instance_uuid7: assigned_uuid.clone(),
                        position: position.clone(),
                        priority: priority as u32,
                    };
                    prompts::prompt_user_to_auth_module(&auth_info).await
                }
            }
        };

        if !approved {
            log_event_broadcast(&ui_state, format!("Rejected: user denied module '{}'", module_name));
            log_to_timeline(&db, "module_reject", &module_name, "user denied").await;
            let response = ContainerForModule {
                version: 2,
                auth_token: String::new(),
                module_instance_uuid7: String::new(),
                payload: Some(ModulePayload::ConnectionRequestReturn(
                    cockatiel_protobuf::ConnectionRequestReturn {
                        new_port: 0,
                        module_instance_uuid7: String::new(),
                    },
                )),
            };
            let mut bytes = Vec::new();
            response.encode(&mut bytes)?;
            bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
            websocket.close(None).await?;
            return Ok(());
        }

        let auth_token = auth_store.generate_token(&assigned_uuid, &module_name);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        auth_store.insert(AuthSession {
            module_name: module_name.clone(),
            instance_uuid7: assigned_uuid.clone(),
            auth_token: auth_token.clone(),
            position: position.clone(),
            priority,
            authenticated: true,
            connected_at: Some(now_ms),
            shutdown_at: None,
            last_activity_ms: now_ms,
            last_probe_at_ms: 0,
            probe_deadline_ms: 0,
            unresponsive: false,
            peer_loopback,
            socket_token,
        });

        module_registry.register(module_registry::RegisteredModule {
            name: module_name.clone(),
            instance_uuid7: assigned_uuid.clone(),
            position: position.clone(),
            priority,
            auto_auth: true,
            auth_token: auth_token.clone(),
        });

        config::add_module_to_config(&config_state, &module_name, &position, priority);
        warn_stage_capability_mismatch(&module_name, &discovered_registry, &ui_state);

        log_event_broadcast(&ui_state, format!("Approved: {} on {}", module_name, position));
        log_to_timeline(
            &db,
            "module_connect",
            &module_name,
            &format!("approved on '{}' [{}]", position, assigned_uuid),
        )
        .await;

        let response = ContainerForModule {
            version: 2,
            auth_token: auth_token.clone(),
            module_instance_uuid7: assigned_uuid.clone(),
            payload: Some(ModulePayload::ConnectionRequestReturn(
                cockatiel_protobuf::ConnectionRequestReturn {
                    new_port: 0,
                    module_instance_uuid7: assigned_uuid.clone(),
                },
            )),
        };
let mut bytes = Vec::new();
        response.encode(&mut bytes)?;
        bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await?;
    }

    let instance_uuid7 = assigned_uuid;

    // Register module sender for pipeline routing (keyed by module name,
    // matching the pipeline's lookups).
    {
        let mut senders = orchestrator.module_senders.lock().await;
        senders.insert(module_name.clone(), tx.clone());
    }

    // Register a kill switch so the probe task can close this connection
    // (running the normal cleanup) when the module goes unresponsive.
    let (kill_tx, mut kill_rx) = tokio::sync::watch::channel(false);
    kill_map.lock().await.insert(instance_uuid7.clone(), kill_tx);

    // Reject anything the module pipelined BEFORE it was authorized: the only
    // valid first payload after a fresh connection is nothing — the module must
    // wait for its ConnectionRequestReturn (token) before sending anything. Any
    // message that arrived while the approval was pending is discarded so a
    // not-yet-authorized module can never inject a payload.
    for _ in 0..32 {
        match tokio::time::timeout(
            std::time::Duration::from_millis(40),
            websocket.next(),
        )
        .await
        {
            Ok(Some(Ok(WsMessage::Binary(_)))) => {
                log_event_broadcast(
                    &ui_state,
                    format!("Discarded payload from '{}' sent before authorization", module_name),
                );
                // keep draining — the module may have pipelined several.
            }
            _ => break,
        }
    }

    // Set once this connection has queued the answer to an accepted
    // `engine_shutdown` request. The answer is, by construction, the next
    // container this socket writes, so the outbound arm can hand the exit back
    // to the main loop at exactly the moment the TUI has the response in hand.
    let mut shutdown_flush = false;

    loop {
        tokio::select! {
            incoming = websocket.next() => {
                let Some(incoming) = incoming else {
                    break;
                };

                let message = match incoming {
                    Ok(m) => m,
                    Err(e) => {
                        log_event_broadcast(&ui_state, format!("Connection error: {}", e));
                        break;
                    }
                };
                let WsMessage::Binary(data) = message else {
                    continue;
                };

                let container = match ContainerForEngine::decode(data.as_ref()) {
                    Ok(c) => c,
                    Err(e) => {
                        log_event_broadcast(&ui_state, format!("Decode error: {}", e));
                        break;
                    }
                };

                // Auth check on every message: the token must be valid AND
                // bound to the name the container claims (name-trust: a valid
                // token can't be replayed under a trusted module name), AND the
                // session must actually be authenticated/authorized. Anything
                // else is rejected outright — no payload is processed without
                // valid auth.
                if !auth_store.verify_token(
                    &container.module_instance_uuid7,
                    &container.auth_token,
                    &container.module_name,
                ) || !auth_store.is_authenticated(&container.module_instance_uuid7)
                {
                    log_event(
                        &ui_state,
                        format!(
                            "Severed: invalid auth from '{}' ({})",
                            container.module_name, container.module_instance_uuid7
                        ),
                    );
                    websocket.close(None).await?;
                    return Ok(());
                }

                // Any valid container proves the module is alive (this also
                // clears a pending probe window).
                let activity_now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                auth_store.update_activity(&container.module_instance_uuid7, activity_now);

                match container.payload {
                    Some(EnginePayload::ConnectionRequest(_)) => {
                        log_event_broadcast(&ui_state, "Ignoring ConnectionRequest from authenticated module");
                    }
                    Some(EnginePayload::AuthVerify(_)) => {
                        // Liveness probe response. The generic per-message auth
                        // check above already verified the token; nothing more
                        // to do — last_activity was refreshed.
                    }
                    Some(EnginePayload::MessagePreProcess(ref msg)) => {
                        let mut enriched = msg.clone();
                        if let Some(chat) = enriched.raw_message.as_mut() {
                            // Command parsing FIRST: if the raw message starts
                            // with a registered flag, attach the parsed Command
                            // (with flag values) so the pipeline routes it to the
                            // owning module (+ catch-alls). Unknown commands on
                            // an alerting flag get the apology reply; `!help`
                            // lists every registered command. This has to
                            // complete before the user-data fetch below, because
                            // whether a command was attached is an input to the
                            // decision of whether to fetch at all.
                            let command_outcome = handle_command_on_ingest(
                                chat,
                                &cmd_registry,
                                &orchestrator,
                                &ui_state,
                            )
                            .await;
                            // Then enrich the chat message with user data from
                            // the user DB (if the adapter supplied a user
                            // identifier) — but only when something downstream
                            // will actually consume it, since the lookup costs
                            // two WebSocket round-trips. A plain message that
                            // no catch-all, command module or connected display
                            // cares about is left with `user_data == None`.
                            // Every guard is dropped before the enrich await:
                            // a std MutexGuard must never be held across one.
                            let connected = connected_module_names(&orchestrator.module_senders).await;
                            let fetch = {
                                let cfg = orchestrator.config.lock().await;
                                let reg = cmd_registry.lock().unwrap();
                                should_fetch_user_data(
                                    command_outcome.command_attached(),
                                    &reg,
                                    &cfg,
                                    &connected,
                                )
                            };
                            if fetch {
                                enrich_chat_user(&user_db_client, chat).await;
                            }
                            // A real user message from an adapter: count it toward
                            // the user's `messages_sent` (a rank factor). Best-effort
                            // and non-blocking — a failed count never stalls ingest.
                            if !chat.user_uuid7.is_empty() {
                                let _ = user_db_client.increment_messages_sent(&chat.user_uuid7).await;
                            }
                        }
                        let enriched_container = ContainerForEngine {
                            version: container.version,
                            auth_token: container.auth_token.clone(),
                            module_name: container.module_name.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(EnginePayload::MessagePreProcess(enriched)),
                        };
                        if let Err(e) = orchestrator.handle_message_from_module(&enriched_container).await {
                            log_event_broadcast(&ui_state, format!("Pipeline error: {}", e));
                        }
                    }
                    Some(EnginePayload::MessageInProcess(_))
                    | Some(EnginePayload::MessagePostProcess(_))
                    | Some(EnginePayload::MessageAck(_)) => {
                        if let Err(e) = orchestrator.handle_message_from_module(&container).await {
                            log_event_broadcast(&ui_state, format!("Pipeline error: {}", e));
                        }
                    }
                    Some(EnginePayload::TimelineQuery(query)) => {
                        // A module requests timeline events. Build the response
                        // and ship it back on the same outbound channel.
                        let id = if query.timeline_id_uuid7.is_empty() {
                            None
                        } else {
                            uuid::Uuid::parse_str(&query.timeline_id_uuid7)
                                .ok()
                                .map(|u| u.as_bytes().to_vec())
                        };
                        let event_type = if query.event_type == 0 {
                            None
                        } else {
                            Some(query.event_type as i32)
                        };
                        let platform = if query.platform.is_empty() { None } else { Some(query.platform.as_str()) };
                        let user = if query.user_uuid7.is_empty() { None } else { Some(query.user_uuid7.as_str()) };
                        let kind = if query.kind.is_empty() { None } else { Some(query.kind.as_str()) };
                        let raw_prefix = if query.raw_prefix.is_empty() { None } else { Some(query.raw_prefix.as_str()) };
                        let since = if query.since_ms == 0 { None } else { Some(query.since_ms) };
                        let pipeline_status = if query.pipeline_status.is_empty() {
                            None
                        } else {
                            Some(query.pipeline_status.as_str())
                        };
                        let rows = db
                            .query_timeline(
                                id.as_deref(),
                                event_type,
                                platform,
                                user,
                                kind,
                                raw_prefix,
                                since,
                                pipeline_status,
                                query.limit,
                                query.offset,
                            )
                            .await;
                        match rows {
                            Ok(json) => {
                                let events = serde_json::from_str::<Vec<serde_json::Value>>(&json)
                                    .unwrap_or_default()
                                    .into_iter()
                                    .filter_map(|row| {
                                        Some(cockatiel_protobuf::TimelineEvent {
                                            timeline_id_uuid7: row.get("timeline_id_uuid7")?.as_str()?.to_string(),
                                            event_type: row
                                                .get("event_type")
                                                .and_then(|v| v.as_i64())
                                                .unwrap_or(0) as i32,
                                            command_flag: row.get("command").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            data_blob: Vec::new(),
                                            error_message: row.get("error_message").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            raw_flags: row.get("flags").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            message_origin: String::new(),
                                            stream_origin: row.get("platform").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            raw_message: row.get("raw_message").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            processed_message: row.get("processed_message").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            user_uuid7: row.get("user_uuid7").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            version: 0,
                                            pipeline_status: row.get("pipeline_status").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                        })
                                    })
                                    .collect();
                                let result = TimelineQueryResult {
                                    events,
                                    truncated: false,
                                    request_id: query.request_id.clone(),
                                };
                                let reply = ContainerForModule {
                                    version: 2,
                                    auth_token: container.auth_token.clone(),
                                    module_instance_uuid7: container.module_instance_uuid7.clone(),
                                    payload: Some(ModulePayload::TimelineQueryResult(result)),
                                };
                                let mut buf = Vec::new();
                                if reply.encode(&mut buf).is_ok() {
                                    if let Err(e) = bounded_ws_send(&mut websocket, WsMessage::Binary(buf), bounds.send_timeout_secs).await {
                                        log_event_broadcast(&ui_state, format!("TimelineQuery reply send failed: {}", e));
                                    }
                                }
                            }
                            Err(e) => {
                                log_event_broadcast(&ui_state, format!("TimelineQuery failed: {}", e));
                            }
                        }
                    }
                    Some(EnginePayload::Log(ref lg)) => {
                        log_event(&ui_state, format!("[{}] {}", module_name, lg.log));
                        log_to_timeline(&db, "module_log", &module_name, &lg.log).await;
                    }
                    Some(EnginePayload::Err(ref err)) => {
                        log_event(&ui_state, format!("[{}] Error: {}", module_name, err.log));
                        log_to_timeline(&db, "module_error", &module_name, &err.log).await;
                    }
                    Some(EnginePayload::DatabaseQuery(ref query)) => {
                        // The query surface, its gates and its response
                        // envelope all live in `queries`; this arm only lends
                        // it the connection's ambient state and ships the answer
                        // back on the same 1s bound it always used.
                        let was_requested = shutdown.stage() == ShutdownStage::Requested;
                        let ctx = QueryContext {
                            db: &db,
                            auth_store: &auth_store,
                            orchestrator: &orchestrator,
                            ui_state: &ui_state,
                            user_db_client: &user_db_client,
                            config_state: &config_state,
                            module_registry: &module_registry,
                            discovered_registry: &discovered_registry,
                            prompt_routes: &prompt_routes,
                            channel_stats: &channel_stats,
                            module_name: &module_name,
                            instance_uuid7: &instance_uuid7,
                            tx: &tx,
                            shutdown: &shutdown,
                            peer_loopback,
                        };
                        let outcome =
                            queries::handle_query(&ctx, &query.query_id, &query.sql).await;
                        // Did THIS query raise a shutdown request? Reading the
                        // signal either side of the call is what makes the
                        // answer below unambiguous: a concurrent request on
                        // another socket cannot claim this connection's write.
                        let answered_with_shutdown =
                            !was_requested && shutdown.stage() == ShutdownStage::Requested;
                        let queued = queries::send_query_response(
                            &ctx,
                            &container.auth_token,
                            &query.query_id,
                            outcome,
                        )
                        .await;
                        // Armed only now — AFTER the answer is on the channel.
                        // The channel is FIFO and the answer was queued before
                        // this point, so the very next frame this socket writes
                        // IS the answer, whatever the log-broadcast task has
                        // enqueued around it. The outbound arm below completes
                        // the handoff once that write lands; the main loop is
                        // holding the exit until it does.
                        shutdown_flush = answered_with_shutdown && queued;
                        if answered_with_shutdown && !queued {
                            // The answer could not even be queued (the outbound
                            // channel is wedged — `send_query_response` has
                            // already logged the drop). There is nothing to
                            // write and nothing to wait for, so release the
                            // exit rather than leaving the engine running after
                            // it accepted the request.
                            shutdown.mark_answered();
                        }
                    }
Some(EnginePayload::QueryRequest(req)) => {
                        // The typed Phase-2 query surface: dispatch by QueryOp
                        // and reply with a QueryResponse (not the legacy
                        // DatabaseQueryResult envelope).
                        let ctx = QueryContext {
                            db: &db,
                            auth_store: &auth_store,
                            orchestrator: &orchestrator,
                            ui_state: &ui_state,
                            user_db_client: &user_db_client,
                            config_state: &config_state,
                            module_registry: &module_registry,
                            discovered_registry: &discovered_registry,
                            prompt_routes: &prompt_routes,
                            channel_stats: &channel_stats,
                            module_name: &module_name,
                            instance_uuid7: &instance_uuid7,
                            tx: &tx,
                            shutdown: &shutdown,
                            peer_loopback,
                        };
                        let outcome =
                            queries::handle_query_request(&ctx, &req).await;
                        let reply = ContainerForModule {
                            version: 2,
                            auth_token: container.auth_token.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(ModulePayload::QueryResponse(
                                cockatiel_protobuf::QueryResponse {
                                    request_id: req.request_id.clone(),
                                    operation: req.operation,
                                    success: outcome.success,
                                    error: outcome.error,
                                    result: Some(cockatiel_protobuf::QueryResult {
                                        result_blob: outcome.result_blob,
                                    }),
                                },
                            )),
                        };
                        let mut buf = Vec::new();
                        if reply.encode(&mut buf).is_ok() {
                            if let Err(e) = bounded_ws_send(&mut websocket, WsMessage::Binary(buf), bounds.send_timeout_secs).await {
                                log_event_broadcast(&ui_state, format!("QueryResponse send failed: {}", e));
                            }
                        }
                    }
                    Some(EnginePayload::SendToPlatforms(send)) => {
                        // The actor (the human who triggered the send) must be
                        // verified against the user DB before anything routes.
                        let actor_lookup = if !send.actor_uuid7.is_empty() {
                            user_db_client.get_user(&send.actor_uuid7, "", "", "").await
                        } else {
                            user_db_client.get_user("", &send.actor_platform, &send.actor_handle, &send.actor_handle).await
                        };
                        let actor_ok = match actor_lookup {
                            Ok(resp) => match resp.user {
                                Some(user) => user.is_moderator || user.is_admin || user.is_owner,
                                None => false,
                            },
                            Err(e) => {
                                log_event_broadcast(&ui_state, format!("[SendToPlatforms] actor lookup failed: {}", e));
                                false
                            }
                        };
                        if !actor_ok {
                            let actor_where = if send.actor_platform.is_empty() { String::new() } else { format!(" on {}", send.actor_platform) };
                            log_event(
                                &ui_state,
                                format!("[SendToPlatforms] denied: actor '{}{}' is not a verified moderator/admin/owner", send.actor_handle, actor_where),
                            );
                            continue;
                        }

                        // Route an outbound message to the target adapter(s).
                        // platform: "twitch"|"kick"|"youtube"|"all"
let targets: Vec<&str> = match send.platform.as_str() {
                            "all" => vec!["twitch-adapter", "kick-adapter", "youtube-adapter", "discord-adapter"],
                            "twitch" => vec!["twitch-adapter"],
                            "kick" => vec!["kick-adapter"],
                            "youtube" => vec!["youtube-adapter"],
                            "discord" => vec!["discord-adapter"],
                            other => {
                                log_event(&ui_state, format!("[SendToPlatforms] unknown platform '{}'", other));
                                Vec::new()
                            }
                        };
                        let forward = ContainerForModule {
                            version: 2,
                            auth_token: container.auth_token.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(ModulePayload::SendToPlatforms(send.clone())),
                        };
                        for name in targets {
                            let sent = crate::pipeline::send_to_module(&orchestrator.module_senders, name, forward.clone(), "SendToPlatforms").await;
                            if matches!(sent, SendOutcome::Sent) {
                                log_event_broadcast(&ui_state, format!("[SendToPlatforms] '{}' -> {}", container.module_name, name));
                            } else if matches!(sent, SendOutcome::Dropped) {
                                log_event_broadcast(&ui_state, format!("[SendToPlatforms] adapter '{}' dropped (channel full)", name));
                            } else {
                                log_event_broadcast(&ui_state, format!("[SendToPlatforms] adapter '{}' not connected", name));
                            }
                        }

                        // Timeline archival: record who sent the outbound message.
                        let flags = serde_json::json!({
                            "actor_platform": send.actor_platform,
                            "actor_handle": send.actor_handle,
                            "target": send.platform,
                        }).to_string();
                        if let Err(e) = db.insert_archival_event("engine", "send_to_platforms", &send.msg, &flags).await {
                            log_event_broadcast(&ui_state, format!("[SendToPlatforms] archival insert failed: {}", e));
                        } else {
                            log_event_broadcast(&ui_state, format!("[SendToPlatforms] '{}' -> {} (by {})", send.msg, send.platform, send.actor_handle));
                        }
                    }
Some(EnginePayload::ModuleControl(_)) => {
                        // Process lifecycle is owned by the TUI supervisor.
                        // The engine no longer starts/stops modules.
                        log_event_broadcast(&ui_state, "[{}] ModuleControl ignored (processes owned by TUI)".to_string());
                    }
                    Some(EnginePayload::Commands(commands)) => {
                        // Command registration: the module subscribes to these
                        // (flag, command) pairs. An EMPTY list = catch-all
                        // (receives every message). `alert_on_unknown_command`
                        // opts into the apology reply for unregistered commands
                        // under this module's flags.
                        let n = commands.commands.len();
                        cmd_registry.lock().unwrap().register(&module_name, commands.clone());
                        log_event_broadcast(
                            &ui_state,
                            format!(
                                "[Commands] {} registered ({} command(s), catch_all={})",
                                module_name,
                                n,
                                n == 0
                            ),
                        );
                    }
                    Some(EnginePayload::Command(command)) => {
                        // Standalone command invocation: a module/UI sends a
                        // `Command` outside of any chat message. The engine
                        // routes it to the owning module (delivered as a
                        // MessagePreProcess carrying the command, origin =
                        // the sender's module identity). Unregistered commands
                        // are logged + ignored.
                        let owner = {
                        let registry = cmd_registry.lock().unwrap();
                        registry.owner(&command.command_flag, &command.command_name)
                    }; // guard dropped before any await
                    if let Some(owner) = owner {
                            let forward = ContainerForModule {
                                version: 2,
                                auth_token: String::new(),
                                module_instance_uuid7: String::new(),
                                payload: Some(ModulePayload::MessagePreProcess(
                                    cockatiel_protobuf::MessagePreProcess {
                                        message_uuid7: String::new(),
                                        raw_message: Some(cockatiel_protobuf::ChatMessage {
                                            platform: format!("command:{}", module_name),
                                            raw_data: vec![],
                                            raw_message: String::new(),
                                            user_uuid7: String::new(),
                                            command: Some(command.clone()),
                                            channel_id: String::new(),
                                            user_data: None,
                                        }),
                                        audio: Vec::new(),
                                        audio_type: String::new(),
                                    },
                                )),
                            };
                            let sent = crate::pipeline::send_to_module(&orchestrator.module_senders, &owner, forward, "command").await;
                            if matches!(sent, SendOutcome::Sent) {
                                log_event_broadcast(
                                    &ui_state,
                                    format!(
                                        "[Commands] {} invoked '{}' -> {}",
                                        module_name,
                                        command.command_name,
                                        owner
                                    ),
                                );
                            } else if matches!(sent, SendOutcome::Dropped) {
                                log_event_broadcast(
                                    &ui_state,
                                    format!(
                                        "[Commands] owner '{}' of '{}' dropped (channel full)",
                                        owner, command.command_name
                                    ),
                                );
                            } else {
                                log_event_broadcast(
                                    &ui_state,
                                    format!("[Commands] owner '{}' of '{}' not connected", owner, command.command_name),
                                );
                            }
                        } else {
                            log_event_broadcast(
                                &ui_state,
                                format!(
                                    "[Commands] unregistered command '{}' invoked by {}",
                                    command.command_name, module_name
                                ),
                            );
                        }
                    }
                    Some(EnginePayload::Prompt(ref prompt)) => {
                        // A module is asking the user something (e.g. "allow this
                        // action?"). Forward it to every OTHER connected module so
                        // they can display it, and remember the origin so a
                        // PromptResponse can be routed back to it.
                        let prompt = prompt.clone();
                        prompt_routes.lock().unwrap().insert(
                            prompt.prompt_id_uuid7.clone(),
                            PromptSink::Module(module_name.clone(), tx.clone()),
                        );
                        let forward = ContainerForModule {
                            version: 2,
                            auth_token: container.auth_token.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(ModulePayload::Prompt(prompt)),
                        };
                        let senders: Vec<tokio::sync::mpsc::Sender<ContainerForModule>> = {
                            let senders = orchestrator.module_senders.lock().await;
                            senders
                                .iter()
                                .filter(|(n, _)| n.as_str() != module_name.as_str())
                                .map(|(_, s)| s.clone())
                                .collect()
                        };
                        for sender in senders {
                            let _ = sender.try_send(forward.clone());
                        }
                    }
                    Some(EnginePayload::PredictionUpdate(ref update)) => {
                        // The predictions module (the brain) broadcasts a
                        // prediction bar to every connected module so they can
                        // display it. Only the brain — or the TUI control
                        // surface — may do this: a random module spoofing a bar
                        // would be indistinguishable from a real one, so the
                        // origin is gated before the forward.
                        if module_name != "events" && !is_control_surface(&module_name) {
                            log_event_broadcast(
                                &ui_state,
                                format!("[PredictionUpdate] ignored from '{}': only the events module or TUI may broadcast", module_name),
                            );
                        } else {
                            let update = update.clone();
                            let forward = ContainerForModule {
                                version: 2,
                                auth_token: container.auth_token.clone(),
                                module_instance_uuid7: container.module_instance_uuid7.clone(),
                                payload: Some(ModulePayload::PredictionUpdate(update)),
                            };
                            let senders: Vec<tokio::sync::mpsc::Sender<ContainerForModule>> = {
                                let senders = orchestrator.module_senders.lock().await;
                                senders
                                    .iter()
                                    .filter(|(n, _)| n.as_str() != module_name.as_str())
                                    .map(|(_, s)| s.clone())
                                    .collect()
                            };
                            for sender in senders {
                                let _ = sender.try_send(forward.clone());
                            }
                        }
                    }
                    Some(EnginePayload::PollUpdate(ref update)) => {
                        // Same relay contract as PredictionUpdate: the
                        // predictions module broadcasts free-vote poll state to
                        // every connected module. Origin-gated identically — a
                        // random module spoofing a poll would be
                        // indistinguishable from a real one.
                        if module_name != "events" && !is_control_surface(&module_name) {
                            log_event_broadcast(
                                &ui_state,
                                format!("[PollUpdate] ignored from '{}': only the events module or TUI may broadcast", module_name),
                            );
                        } else {
                            let update = update.clone();
                            let forward = ContainerForModule {
                                version: 2,
                                auth_token: container.auth_token.clone(),
                                module_instance_uuid7: container.module_instance_uuid7.clone(),
                                payload: Some(ModulePayload::PollUpdate(update)),
                            };
                            let senders: Vec<tokio::sync::mpsc::Sender<ContainerForModule>> = {
                                let senders = orchestrator.module_senders.lock().await;
                                senders
                                    .iter()
                                    .filter(|(n, _)| n.as_str() != module_name.as_str())
                                    .map(|(_, s)| s.clone())
                                    .collect()
                            };
                            for sender in senders {
                                let _ = sender.try_send(forward.clone());
                            }
                        }
                    }
                    Some(EnginePayload::ChannelStats(ref stats)) => {
                        // Adapters push their channel's current viewer/member
                        // count on each poll; the engine stores it so other
                        // modules can read it on demand via the
                        // `channel_viewers` virtual query. Origin-gated to the
                        // platform adapters — a random module spoofing counts
                        // would poison every consumer.
                        if !CHANNEL_STATS_ADAPTERS.contains(&module_name.as_str()) {
                            log_event_broadcast(
                                &ui_state,
                                format!("[ChannelStats] ignored from '{}': only the platform adapters may publish viewer counts", module_name),
                            );
                        } else {
                            let stats = stats.clone();
                            let mut map = channel_stats.lock().unwrap();
                            map.insert(
                                format!("{}:{}", stats.platform, stats.channel),
                                ChannelStatsEntry {
                                    platform: stats.platform,
                                    channel: stats.channel,
                                    viewers: stats.viewers,
                                    is_live: stats.is_live,
                                    title: stats.title,
                                    updated_at: stats.updated_at,
                                },
                            );
                        }
                    }
                    Some(EnginePayload::PromptResponse(ref resp)) => {
                        // Route the user's answer back to whoever is waiting on
                        // this prompt_id (the engine's own connection prompt, or
                        // the module that originally raised the prompt).
                        let resp = resp.clone();
                        let sink = {
                            let mut routes = prompt_routes.lock().unwrap();
                            routes.remove(&resp.prompt_id_uuid7)
                        };
                        if let Some(sink) = sink {
                            match sink {
                                PromptSink::Engine(tx) => {
                                    // oneshot send — non-blocking by construction.
                                    let _ = tx.send(resp.accepted);
                                }
                                PromptSink::Module(_origin, sender) => {
                                    let forward = ContainerForModule {
                                        version: 2,
                                        auth_token: container.auth_token.clone(),
                                        module_instance_uuid7: container.module_instance_uuid7.clone(),
                                        payload: Some(ModulePayload::PromptResponse(resp)),
                                    };
                                    let _ = tokio::time::timeout(Duration::from_millis(1000), sender.send(forward)).await;
                                }
                            }
                        }
                    }
                    Some(EnginePayload::AuditFlag(ref flag)) => {
                        // A module flagged a message for human review (e.g. a
                        // different language). Hold it and ask a connected UI.
                        //
                        // The DB hold MUST land BEFORE the flagging module's
                        // pre-process ack is processed: start_in_process()
                        // consults is_audited() before advancing, so if the
                        // timeline row is still marked 'queued' the flagged
                        // message slips past the hold. The row update is fast
                        // and is AWAITED INLINE here. Only the prompt wait (up
                        // to the 120s prompt timeout) runs DETACHED — it must
                        // never block this module's read loop (it couldn't
                        // answer its own liveness probe and would be killed).
                        let uuid_bytes = flag.message_uuid7.as_bytes().to_vec();
                        // Already held (a prior flag is unresolved): do not
                        // re-hold or spawn a second prompt — the operator is
                        // already looking at it.
                        let newly_held = if db.is_audited(&uuid_bytes).await.unwrap_or(false) {
                            false
                        } else {
                            match db.mark_audit(&uuid_bytes, &flag.reason).await {
                                Ok(()) => true,
                                Err(_) => {
                                    log_event(
                                        &ui_state,
                                        format!("[audit] failed to hold message {}", flag.message_uuid7),
                                    );
                                    false
                                }
                            }
                        };
                        if newly_held {
                            let flag = flag.clone();
                            let audit_db = db.clone();
                            let audit_senders = orchestrator.module_senders.clone();
                            let audit_routes = prompt_routes.clone();
                            let audit_ui = ui_state.clone();
                            tokio::spawn(async move {
                                handle_audit_flag(
                                    &audit_db,
                                    &flag,
                                    &audit_senders,
                                    &audit_routes,
                                    &audit_ui,
                                )
                                .await;
                            });
                        }
                    }
                    Some(EnginePayload::ChatMessageRejected(ref rej)) => {
                        // A module rejected a message: surface it clearly (no
                        // buried log strings) and persist it as a searchable
                        // timeline record. The message itself still flows as the
                        // module chose — this is an audit record, not a stop.
                        handle_chat_message_rejected(&db, &ui_state, rej).await;
                    }
                    _ => {}
                }
            }

            outbound = rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };

                let mut bytes = Vec::new();
                outbound.encode(&mut bytes)?;

                if bounded_ws_send(&mut websocket, WsMessage::Binary(bytes), bounds.send_timeout_secs).await.is_err() {
                    log_event_broadcast(&ui_state, format!(
                        "Connection to '{}' dropped: outbound send failed/timed out ({}s)",
                        module_name, bounds.send_timeout_secs
                    ));
                    break;
                }

                // The write landed. If this frame was the answer to an accepted
                // shutdown request, the caller now knows — release the process
                // exit. (FIFO ordering guarantees this is that answer: it was
                // queued before the flush was armed.)
                if shutdown_flush {
                    shutdown_flush = false;
                    shutdown.mark_answered();
                }
            }
            killed = kill_rx.changed() => {
                // The probe task flagged this session as unresponsive —
                // close it so the disconnect cleanup below runs.
                if killed.is_ok() && *kill_rx.borrow() {
                    log_event_broadcast(&ui_state, format!(
                        "Connection to '{}' dropped: probe flagged unresponsive",
                        module_name
                    ));
                    break;
                }
            }
        }
    }

    // Unregister the kill switch.
    kill_map.lock().await.remove(&instance_uuid7);

    // The socket is gone with a shutdown answer still unflushed (send failure,
    // peer hang-up, …). No answer can ever reach that caller now, so the main
    // loop must not keep waiting for one: release the exit here instead.
    if shutdown_flush {
        shutdown.mark_answered();
    }

    // Drop any prompt routes whose origin module just disconnected (their
    // outbound channel is now closed). Without this, unanswered module prompts
    // leak entries forever, retaining a dead sender.
    {
        let mut routes = prompt_routes.lock().unwrap();
        routes.retain(|_, sink| match sink {
            PromptSink::Module(_origin, tx) => !tx.is_closed(),
            PromptSink::Engine(_) => true,
        });
    }

    // Unregister the module sender (keyed by module name) — but ONLY if no OTHER
    // live session with the same name remains. A relaunch can briefly coexist
    // with the old instance; removing the name slot unconditionally would
    // evict the still-connected sibling's sender (killing its routing + probe
    // delivery → it gets flagged unresponsive).
    {
        let sibling_alive = auth_store
            .values()
            .iter()
            .any(|s| s.module_name == module_name && s.instance_uuid7 != instance_uuid7);
        if !sibling_alive {
            let mut senders = orchestrator.module_senders.lock().await;
            senders.remove(&module_name);
        }
    }

    log_event_broadcast(&ui_state, format!("Disconnected: {} [{}]", module_name, instance_uuid7));
    log_to_timeline(&db, "module_disconnect", &module_name, "disconnected").await;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // Only tear down the session if this socket still owns it — a reconnect
    // may have already claimed it under a new socket.
    auth_store.set_shutdown_if_socket(&instance_uuid7, now_ms, socket_token);
    auth_store.remove_if_socket(&instance_uuid7, socket_token);

    Ok(())
}

#[cfg(test)]
mod tests;
