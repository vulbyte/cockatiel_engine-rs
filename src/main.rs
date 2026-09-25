#![allow(clippy::type_complexity)]

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
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

mod prompts;

mod database;
use database::{DatabaseConfig, DatabaseManager};

mod credentials;
use credentials::{
    credential_values_map, is_config_complete, save_module_credentials,
    validate_credential_fields,
};

mod pipeline;
use pipeline::{PipelineConfig, PipelineOrchestrator, SendOutcome};

mod user_db_client;
use user_db_client::{SharedUserDbClient, UserDbClient, userdb_response_to_json};

/* PROTOBUF STUFF */
pub use cockatiel_proto::proto as cockatiel_protobuf;

use cockatiel_protobuf::{
    Container, container::Payload, ProcessPosition, DatabaseQueryResult, Prompt, PromptType, Log,
    ChatMessage, Command, DatabaseQuery, MessageAck, MessagePreProcess, Shutdown, AuthVerify,
};

/// Where a PromptResponse should be routed. The engine's own prompts (module
/// connection approval) wait on a oneshot; prompts originating from a module
/// are forwarded back to that module's connection.
enum PromptSink {
    Engine(oneshot::Sender<bool>),
    /// A module-originated prompt: (origin module name, response channel).
    Module(String, tokio::sync::mpsc::Sender<Container>),
}

type SharedPromptRoutes = Arc<Mutex<HashMap<String, PromptSink>>>;

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
    pub sender: Option<tokio::sync::mpsc::Sender<Container>>,
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
    handshake_timeout_secs: u64,
    send_timeout_secs: u64,
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
enum CommandAction {
    /// Attach this parsed command (known command — pipeline routes it).
    Attach(crate::cockatiel_protobuf::Command),
    /// Send the built-in help list back to the chat.
    Help,
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
    if lower == "!help" || lower.starts_with("!help ") {
        return CommandAction::Help;
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

/// Parse a raw chat message for registered commands and act:
/// - a known command is attached to `chat.command` (the pipeline routes it);
/// - `!help` lists every registered command back to the chat;
/// - an unregistered command under a flag whose owner set
///   `alert_on_unknown_command` gets the apology reply.
pub async fn handle_command_on_ingest(
    chat: &mut cockatiel_protobuf::ChatMessage,
    registry: &Arc<Mutex<CommandRegistry>>,
    orchestrator: &PipelineOrchestrator,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    let action = {
        let reg = registry.lock().unwrap();
        classify_command(&chat.raw_message, &reg)
    }; // guard dropped here — before any await.

    match action {
        CommandAction::Attach(cmd) => {
            chat.command = Some(cmd);
        }
        CommandAction::Help => {
            let reply = {
                let reg = registry.lock().unwrap();
                let mut lines = vec!["Available commands:".to_string()];
                for c in reg.all_commands() {
                    lines.push(format!("  {}{} — {}", c.command_flag, c.command_name, c.command_description));
                }
                if lines.len() == 1 {
                    lines.push("  (none registered yet)".to_string());
                }
                lines.join("\n")
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
    let container = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "cockatiel".into(),
        module_instance_uuid7: String::new(),
        payload: Some(Payload::SendToPlatforms(send)),
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
    // Rank tiers derived from the user's score (negative → lower, positive → higher).
    let rank = if user.is_owner {
        "owner"
    } else if user.is_admin {
        "admin"
    } else if user.is_moderator {
        "mod"
    } else if user.is_sponsor {
        "sponsor"
    } else if user.score >= 50 {
        "opal"
    } else if user.score >= 20 {
        "gold"
    } else if user.score >= 5 {
        "silver"
    } else if user.score <= -20 {
        "trash"
    } else if user.score <= -5 {
        "coal"
    } else {
        "regular"
    };
    css.insert("rank".to_string(), rank.to_string());
    // Expose the raw score too, so displays can apply their own trust level
    // (e.g. term-chat's `image_min_rank` can be a numeric score threshold).
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

/// Handle a `test_run` virtual query from the TUI. The SQL field carries a JSON
/// payload: { "suite": "chain"|"modules"|"all", "module": "name?", "iterations": n }.
/// Spawns the compliance test runner, captures its output, and returns it.
pub async fn run_test_suite(
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    engine_pin: u32,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid test_run payload: {}", e)),
    };

    let suite = payload.get("suite").and_then(|v| v.as_str()).unwrap_or("all");
    let module = payload.get("module").and_then(|v| v.as_str()).unwrap_or("");
    let iterations = payload.get("iterations").and_then(|v| v.as_i64()).unwrap_or(100);

    let mut args = vec!["--suite".to_string(), suite.to_string()];
    if !module.is_empty() {
        args.push("--module".to_string());
        args.push(module.to_string());
    }
    args.push("--iterations".to_string());
    args.push(iterations.to_string());
    args.push("--json".to_string());

    // Locate the runner binary relative to the engine's working directory.
    let engine_dir = env::current_dir().unwrap_or_default();
    let runner_dir = engine_dir.parent().unwrap_or(&engine_dir).join("cockatiel_test_runner-rs");
    let runner_bin = runner_dir.join("target").join("release").join("cockatiel-test-runner");
    let runner_bin = if runner_bin.exists() {
        runner_bin
    } else {
        runner_dir.join("target").join("debug").join("cockatiel-test-runner")
    };

    if !runner_bin.exists() {
        return (
            false,
            Vec::new(),
            format!("test-runner binary not found at {}", runner_bin.display()),
        );
    }

    log_event(ui_state, format!("[test] running suite '{}' (module: '{}', n={})", suite, module, iterations));

    let output = match tokio::time::timeout(
        Duration::from_secs(300),
        tokio::process::Command::new(&runner_bin)
            .args(&args)
            .current_dir(runner_dir)
            .env("COCKATIEL_PIN", engine_pin.to_string())
            .output(),
    )
    .await
    {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return (false, Vec::new(), format!("failed to spawn test runner: {}", e)),
        Err(_) => return (false, Vec::new(), "test runner timed out after 300s".to_string()),
    };

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Surface progress lines as engine logs so the TUI shows them live.
    for line in stdout.lines() {
        log_event(ui_state, format!("[test] {}", line));
    }
    if !stderr.trim().is_empty() {
        log_event(ui_state, format!("[test] stderr: {}", stderr.trim()));
    }

    let exit_ok = output.status.success();
    (exit_ok, stdout.into_bytes(), if exit_ok { String::new() } else { stderr })
}

/// Handle a `userdb_*` virtual query from the TUI. The SQL field carries a JSON
/// payload specific to each operation. Only the engine's control surface (TUI)
/// may reach the user database.
pub async fn userdb_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
    peer_loopback: bool,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid userdb payload: {}", e)),
    };

    let outcome = match query_id {
        "userdb_add_user" => {
            let username = payload.get("username").and_then(|v| v.as_str()).unwrap_or("");
            let channel = parse_channel_ref(payload.get("channel"));
            client.add_user(username, channel.as_ref()).await
        }
        "userdb_delete_user" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor = payload.get("actor_uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let role = userdb_actor_perm(peer_loopback, payload.get("actor_role").and_then(|v| v.as_str()).unwrap_or("user"));
            client.delete_user(uuid7, actor, &role).await
        }
        "userdb_add_score" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.add_score(uuid7, delta, reason).await
        }
        "userdb_remove_score" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_score(uuid7, delta, reason).await
        }
        "userdb_add_channel" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let channel = parse_channel_ref(payload.get("channel"));
            client.add_channel(uuid7, channel.as_ref()).await
        }
        "userdb_remove_channel" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let channel_id = payload.get("channel_id").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_channel(uuid7, platform, channel_id).await
        }
        "userdb_get_user" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let channel_id = payload.get("channel_id").and_then(|v| v.as_str()).unwrap_or("");
            let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
            client.get_user(uuid7, platform, channel_id, handle).await
        }
        "userdb_list_users" => {
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;
            let offset = payload.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            client.list_users(platform, limit, offset).await
        }
        "userdb_update_flags" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let flags = payload.get("flags").and_then(|v| v.as_str()).unwrap_or("{}");
            client.update_flags(uuid7, flags).await
        }
        "userdb_set_roles" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor = payload.get("actor_uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let role = userdb_actor_perm(peer_loopback, payload.get("actor_role").and_then(|v| v.as_str()).unwrap_or("user"));
            let sponsor = payload.get("is_sponsor").and_then(|v| v.as_bool()).unwrap_or(false);
            let mod_ = payload.get("is_moderator").and_then(|v| v.as_bool()).unwrap_or(false);
            let admin = payload.get("is_admin").and_then(|v| v.as_bool()).unwrap_or(false);
            let owner = payload.get("is_owner").and_then(|v| v.as_bool()).unwrap_or(false);
            client.set_roles(uuid7, actor, &role, sponsor, mod_, admin, owner).await
        }
        "userdb_read_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            client.read_user_value(uuid7, key).await
        }
        "userdb_write_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let value = payload.get("value").and_then(|v| v.as_str()).unwrap_or("");
            client.write_user_value(uuid7, key, value).await
        }
        "userdb_delete_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            client.delete_user_value(uuid7, key).await
        }
        "userdb_list_user_values" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            client.list_user_values(uuid7).await
        }
        // Mod commands: map to user DB operations.
        "userdb_commendation" => {
            // { uuid7, reason? } → +1 score (commendation)
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.add_score(uuid7, 1, reason).await
        }
        "userdb_reprimand" => {
            // { uuid7, reason? } → -1 score (reprimand)
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_score(uuid7, 1, reason).await
        }
        "userdb_ban" => {
            // { uuid7, reason? } → set banned flag value
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let flag = serde_json::json!({ "banned": true, "reason": reason, "at": now }).to_string();
            client.write_user_value(uuid7, "ban", &flag).await
        }
        "userdb_timeout" => {
            // { uuid7, duration_secs, reason? } → set timeout value with expiry
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let duration_secs = payload.get("duration_secs").and_then(|v| v.as_i64()).unwrap_or(300);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default();
            let expires = now_ms + (duration_secs as u128 * 1000);
            let flag = serde_json::json!({
                "timed_out": true,
                "reason": reason,
                "expires_at_ms": expires,
            }).to_string();
            client.write_user_value(uuid7, "timeout", &flag).await
        }
        _ => return (false, Vec::new(), format!("Unknown userdb query: {}", query_id)),
    };

    match outcome {
        Ok(resp) => {
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] UserDB error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle the `userdb_adjust_score` virtual query: apply a REAL arbitrary delta
/// to a user's `score` (no ±1 clamp, no user-db rating cooldown, no counter
/// inflation). Unlike `mod_commend`/`mod_reprimand` it accepts any signed delta
/// and uses the user-db's dedicated score-only op.
///
/// Gate: only the score-messages module or the TUI control surface may call it.
pub async fn userdb_adjust_score_virtual_query(
    client: &SharedUserDbClient,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid userdb_adjust_score payload: {}", e)),
    };

    let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("userdb_adjust_score");

    // Resolve the target user: prefer an explicit uuid7, else by (platform, handle).
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");

    let uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        let resolved = match client.get_user("", platform, handle, handle).await {
            Ok(r) => r,
            Err(e) => return (false, Vec::new(), e),
        };
        if !resolved.success {
            log_event(ui_state, format!("[{}] userdb_adjust_score: target '{}' not found on '{}'", requester, handle, platform));
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        }
        let Some(user) = resolved.user else {
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        };
        user.uuid7
    } else {
        return (false, Vec::new(), "userdb_adjust_score requires uuid7 or platform + handle".to_string());
    };

    if delta == 0 {
        let json = serde_json::json!({ "success": true, "uuid7": uuid7, "delta": 0 }).to_string();
        return (true, json.into_bytes(), String::new());
    }

    let outcome = client.adjust_score_only(&uuid7, delta, reason).await;

    match outcome {
        Ok(resp) => {
            log_event(ui_state, format!("[{}] userdb_adjust_score {} by {} -> {}", requester, delta, requester, uuid7));
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] userdb_adjust_score error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle a `mod_*` virtual query from an adapter (e.g. a mod typing a command
/// in chat). The adapter identifies the target by platform + handle; the engine
/// resolves the user DB record and applies the action.
/// Query IDs: mod_commend, mod_reprimand, mod_ban, mod_timeout.
///
/// The actor (the human triggering the action) MUST be verified against the
/// user database before any privileged action runs; a missing or unauthorized
/// actor is rejected. The only exception is the SYSTEM path: the automated
/// scorer module ("score-messages") applies ±1 score deltas (mod_commend /
/// mod_reprimand) without a human actor.
pub async fn mod_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid mod payload: {}", e)),
    };

    // ── Actor verification ──────────────────────────────────────────────
    // The optional `actor` object is { "platform", "handle", "uuid7" }.
    let actor = payload.get("actor").filter(|a| !a.is_null());
    let (actor_uuid7, actor_handle) = match actor {
        Some(actor) => {
            let actor_uuid = actor.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor_platform = actor.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let actor_handle = actor.get("handle").and_then(|v| v.as_str()).unwrap_or("");
            // Prefer a uuid7; otherwise resolve by (platform, handle).
            let resolved = if !actor_uuid.is_empty() {
                client.get_user(actor_uuid, "", "", "").await
            } else {
                client.get_user("", actor_platform, actor_handle, actor_handle).await
            };
            let resolved = match resolved {
                Ok(r) => r,
                Err(e) => return (false, Vec::new(), format!("Actor lookup failed: {}", e)),
            };
            let Some(user) = resolved.user else {
                let actor_where = if actor_platform.is_empty() { String::new() } else { format!(" on {}", actor_platform) };
                log_event(
                    ui_state,
                    format!("[{}] {} denied: actor '{}{}' not found in user DB", requester, query_id, actor_handle, actor_where),
                );
                return (false, Vec::new(), "permission denied: actor is not a moderator/admin/owner".to_string());
            };
            if !(user.is_moderator || user.is_admin || user.is_owner) {
                log_event(
                    ui_state,
                    format!("[{}] {} denied: actor '{}' lacks a mod/admin/owner role", requester, query_id, user.username),
                );
                return (false, Vec::new(), "permission denied: actor is not a moderator/admin/owner".to_string());
            }
            (user.uuid7, user.username)
        }
        None => {
            // SYSTEM path: the automated scorer applies ±1 score deltas for
            // every message. No other mod_* action may run without an actor.
            if !matches!(query_id, "mod_commend" | "mod_reprimand") || requester != "score-messages" {
                log_event(
                    ui_state,
                    format!("[{}] {} denied: missing actor (module '{}')", requester, query_id, requester),
                );
                return (false, Vec::new(), "permission denied: missing actor".to_string());
            }
            (String::new(), format!("system:{}", requester))
        }
    };

    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
    let duration_secs = payload.get("duration_secs").and_then(|v| v.as_i64()).unwrap_or(300);

    // Resolve the target user: prefer an explicit uuid7, else by (platform, handle).
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        let resolved = match client.get_user("", platform, handle, handle).await {
            Ok(r) => r,
            Err(e) => return (false, Vec::new(), e),
        };
        if !resolved.success {
            log_event(ui_state, format!("[{}] mod: target '{}' not found on '{}'", requester, handle, platform));
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        }
        let Some(user) = resolved.user else {
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        };
        user.uuid7
    } else {
        return (false, Vec::new(), "mod query requires uuid7 or platform + handle".to_string());
    };

    let outcome = match query_id {
        "mod_commend" => client.add_score(&uuid7, 1, reason).await,
        "mod_reprimand" => client.remove_score(&uuid7, 1, reason).await,
        "mod_ban" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let flag = serde_json::json!({ "banned": true, "reason": reason, "at": now }).to_string();
            client.write_user_value(&uuid7, "ban", &flag).await
        }
        "mod_timeout" => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default();
            let expires = now_ms + (duration_secs as u128 * 1000);
            let flag = serde_json::json!({
                "timed_out": true,
                "reason": reason,
                "expires_at_ms": expires,
            }).to_string();
            client.write_user_value(&uuid7, "timeout", &flag).await
        }
        _ => return (false, Vec::new(), format!("Unknown mod query: {}", query_id)),
    };

    match outcome {
        Ok(resp) => {
            log_event(
                ui_state,
                format!("[{}] {} by {} -> {} (actor={})", requester, query_id, actor_handle, handle, actor_uuid7),
            );
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] Mod command error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle a `chat_commend` / `chat_reprimand` virtual query from the commend /
/// reprimand command modules. Unlike `mod_*`, the actor does NOT need mod
/// status — any verified user may rate another user. The user-db enforces the
/// 24h reprimand cooldown atomically (rating_history); a denial returns
/// success=false with the cooldown reason.
pub async fn chat_rating_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    // Gate to the two dedicated modules so no random module can fake ratings.
    let expected = if query_id == "chat_commend" { "commend" } else { "reprimand" };
    if requester != expected {
        log_event(
            ui_state,
            format!("[{}] {} denied: requester '{}' is not the '{}' module", requester, query_id, requester, expected),
        );
        return (false, Vec::new(), format!("{} denied: not the '{}' module", query_id, expected));
    }

    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid rating payload: {}", e)),
    };

    // Actor (the giver) — resolve + require existence (ANY role is fine).
    let actor = payload.get("actor").filter(|a| !a.is_null());
    let Some(actor) = actor else {
        return (false, Vec::new(), "rating requires an actor".to_string());
    };
    let actor_uuid = actor.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let actor_platform = actor.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let actor_handle = actor.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let giver_uuid7 = if !actor_uuid.is_empty() {
        actor_uuid.to_string()
    } else if !actor_platform.is_empty() && !actor_handle.is_empty() {
        match client.get_user("", actor_platform, actor_handle, actor_handle).await {
            Ok(resp) if resp.success && resp.user.is_some() => resp.user.unwrap().uuid7,
            Ok(_) => {
                log_event(ui_state, format!("[{}] rating denied: giver '{}' not found", requester, actor_handle));
                return (false, Vec::new(), format!("giver '{}' not found on {}", actor_handle, actor_platform));
            }
            Err(e) => return (false, Vec::new(), format!("giver lookup failed: {}", e)),
        }
    } else {
        return (false, Vec::new(), "rating actor requires uuid7 or platform+handle".to_string());
    };

    // Target (the recipient) — resolve by uuid7 or platform + handle.
    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let recipient_uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        match client.get_user("", platform, handle, handle).await {
            Ok(resp) if resp.success && resp.user.is_some() => resp.user.unwrap().uuid7,
            Ok(_) => {
                log_event(ui_state, format!("[{}] rating denied: target '{}' not found", requester, handle));
                return (false, Vec::new(), format!("target '{}' not found on {}", handle, platform));
            }
            Err(e) => return (false, Vec::new(), format!("target lookup failed: {}", e)),
        }
    } else {
        return (false, Vec::new(), "rating requires uuid7 or platform+handle".to_string());
    };

    if recipient_uuid7 == giver_uuid7 {
        return (false, Vec::new(), "you cannot rate yourself".to_string());
    }

    let is_commendation = query_id == "chat_commend";
    match client
        .rate_user(&giver_uuid7, &recipient_uuid7, is_commendation, platform, handle, reason)
        .await
    {
        Ok(resp) => {
            if resp.success {
                log_event(ui_state, format!("[{}] {} applied to '{}'", requester, query_id, handle));
                (true, resp.message.into_bytes(), String::new())
            } else {
                // Cooldown denial — surface the reason to the module (it logs it).
                let err = if resp.error.is_empty() { resp.message } else { resp.error };
                log_event(ui_state, format!("[{}] {} denied: {}", requester, query_id, err));
                (false, Vec::new(), err)
            }
        }
        Err(e) => (false, Vec::new(), format!("rating failed: {}", e)),
    }
}

/// Handle a `chat_verify_identity` virtual query from the terminal chat module.
/// This is the identity bootstrap / write-through path: term-chat sends the
/// platform identity it just authenticated and the roles the platform vouches
/// for. The engine creates-or-finds the user and elevates their roles to the
/// union of existing + verified roles (ELEVATE, never revoke).
pub async fn chat_verify_identity_virtual_query(
    client: &SharedUserDbClient,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    module_name: &str,
) -> (bool, Vec<u8>, String) {
    if module_name != "term-chat" {
        log_event(ui_state, format!("[{}] chat_verify_identity denied: not term-chat", module_name));
        return (false, Vec::new(), "access denied: not term-chat".to_string());
    }

    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid chat_verify_identity payload: {}", e)),
    };

    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");

    // Create-or-find the user keyed by channel (the service already does
    // find-by-channel and returns the existing record).
    let channel = parse_channel_ref(Some(&serde_json::json!({
        "platform": platform,
        "channel_id": "",
        "handle": handle,
    })));
    let add_resp = match client.add_user(handle, channel.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            log_event(ui_state, format!("[{}] chat_verify_identity userdb error: {}", module_name, e));
            return (false, Vec::new(), e);
        }
    };
    if !add_resp.success {
        return (false, Vec::new(), add_resp.error.clone());
    }
    let Some(user) = add_resp.user else {
        return (false, Vec::new(), "chat_verify_identity: no user returned".to_string());
    };

    // term-chat asserts its platform-verified roles as booleans under
    // `verified_roles`. The engine must NOT union those onto the stored record
    // (that can never revoke — and a self-asserted login_kick handle would
    // stick elevated roles onto the matching stored user forever). Set each
    // role EXACTLY from the claim so a role that is no longer asserted is
    // revoked (straight assignment, never AND/OR with the stored value).
    // When the payload carries no verified_roles at all, preserve the stored
    // roles (previous behavior) but surface a warning — the claim is untrusted.
    let verified_roles = payload.get("verified_roles");
    let (is_sponsor, is_moderator, is_admin, is_owner) = match verified_roles {
        Some(verified) if verified.is_object() => (
            verified.get("is_sponsor").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_moderator").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_admin").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_owner").and_then(|v| v.as_bool()).unwrap_or(false),
        ),
        _ => {
            log_event(
                ui_state,
                format!(
                    "[{}] chat_verify_identity: payload for '{}' on '{}' carries no verified_roles — preserving stored roles (unverified claim)",
                    module_name, handle, platform
                ),
            );
            (user.is_sponsor, user.is_moderator, user.is_admin, user.is_owner)
        }
    };

    let set_resp = match client
        .set_roles(&user.uuid7, "", "owner", is_sponsor, is_moderator, is_admin, is_owner)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            log_event(ui_state, format!("[{}] chat_verify_identity set_roles error: {}", module_name, e));
            return (false, Vec::new(), e);
        }
    };
    if !set_resp.success {
        return (false, Vec::new(), set_resp.error.clone());
    }

    log_event(
        ui_state,
        format!("[{}] verified identity: {} on {} (mod={})", module_name, handle, platform, is_moderator),
    );

    let json = userdb_response_to_json(&set_resp);
    (true, json.into_bytes(), String::new())
}

fn parse_channel_ref(value: Option<&serde_json::Value>) -> Option<user_db_client::proto::ChannelRef> {
    let Some(value) = value else {
        return None;
    };
    if value.is_null() {
        return None;
    }
    Some(user_db_client::proto::ChannelRef {
        platform: value.get("platform").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        channel_id: value.get("channel_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        handle: value.get("handle").and_then(|v| v.as_str()).unwrap_or("").to_string(),
    })
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
fn userdb_actor_perm(peer_loopback: bool, actor_role: &str) -> String {
    if peer_loopback {
        "owner".to_string()
    } else {
        actor_role.to_string()
    }
}

/// The TUI (or a detached TUI sub-window) is the operator's control surface:
/// it may run tests, inspect/release held-audit messages, and access the user
/// database. The name is bound to the session's JWT, so this check is sound.
fn is_control_surface(name: &str) -> bool {
    name == "cockatiel-tui" || name == "cockatiel-tui-child"
}

/// Who may read OTHER modules' credential values from `module_list`.
/// The TUI control surface (operator) and term-chat (which legitimately reads
/// adapter client ids/secrets to drive its OAuth login flows) may see them;
/// every other module gets the structural list with `credential_values`
/// redacted, so one module cannot dump another module's secrets.
fn may_read_other_credentials(name: &str) -> bool {
    is_control_surface(name) || name == "term-chat"
}

/// The compliance test-runner may archive test results to the timeline.
fn is_test_runner(name: &str) -> bool {
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
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<Container>>>,
    >,
    prompt_routes: &SharedPromptRoutes,
    ui_state: &Arc<Mutex<EngineState>>,
    log_label: &str,
) -> PromptOutcome {
    let prompt_id = prompt.prompt_id_uuid7.clone();
    let timeout = if prompt.timeout > 0 { prompt.timeout } else { 30 };

    let senders: Vec<tokio::sync::mpsc::Sender<Container>> = {
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

    let container = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "engine".to_string(),
        module_instance_uuid7: String::new(),
        payload: Some(Payload::Prompt(prompt)),
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
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<Container>>>,
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
    let processed = &rej.processed_message;
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
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<Container>>>,
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
    container: &Container,
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
        let matching_senders: Vec<tokio::sync::mpsc::Sender<Container>> = {
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
    let config_size = fs::metadata(&config_path)?.len();
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
    let module_senders: Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<Container>>>> =
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
                        let probe = Container {
                            version: 1,
                            auth_token: String::new(),
                            module_name: "engine".into(),
                            module_instance_uuid7: String::new(),
                            payload: Some(Payload::AuthVerify(cockatiel_protobuf::AuthVerify {
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
                let container = Container {
                    version: 1,
                    auth_token: String::new(),
                    module_name: "engine".into(),
                    module_instance_uuid7: String::new(),
                    payload: Some(Payload::Log(Log {
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

    let orchestrator = PipelineOrchestrator::new(
        db.clone(),
        pipeline_config,
        module_senders.clone(),
        cmd_registry.clone(),
    );

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
                if let Err(e) = orchestrator.handle_timeout().await {
                    eprintln!("[Pipeline] Timeout check error: {}", e);
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
    {
        let db = db.clone();
        let orchestrator = orchestrator.clone();
        let ui_state = ui_state.clone();
        let grace = config::get_config(&config_state).recovery_grace_secs;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(grace)).await;
            loop {
                let uuids = match db.get_queued_uuids().await {
                    Ok(u) => u,
                    Err(e) => {
                        log_event_broadcast(&ui_state, format!("Recovery error: {}", e));
                        break;
                    }
                };
                if uuids.is_empty() {
                    break;
                }
                for s in uuids {
                    if let Err(e) = orchestrator.recover_one(&s).await {
                        log_event_broadcast(&ui_state, format!("Recovery failed for {}: {}", s, e));
                    }
                }
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

    loop {
        let (stream, address) = listener.accept().await?;
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
        let kill_map = Arc::clone(&kill_map);
        let cmd_registry = Arc::clone(&cmd_registry);
        let pin_gate = pin_gate.clone();
        tokio::spawn(async move {
            // The permit is held for the connection's lifetime (dropping it
            // frees a slot in the connection ceiling).
            let _permit = permit;
            // Bound the TLS handshake: a client that connects but never
            // completes it must not squat a task (and a connection slot)
            // forever.
            match tokio::time::timeout(
                Duration::from_secs(bounds.handshake_timeout_secs),
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
                        kill_map,
                        cmd_registry,
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
}

/// Read-only SQL boundary for the `DatabaseQuery` fallback. Strips leading
/// whitespace and `--`/`/* */` comment lines, then requires the statement to
/// begin with SELECT / EXPLAIN. Any other statement (INSERT, UPDATE, DELETE,
/// DROP, ALTER, CREATE, PRAGMA, WITH, ...) is denied — modules can only read
/// the timeline, never write to it through arbitrary SQL. A `WITH` block is not
/// allowed up front because a `WITH x AS (...) INSERT/DELETE/...` CTE can smuggle
/// a write through; as defense-in-depth the whole statement is also tokenized
/// and rejected if any write keyword appears anywhere in it.
fn is_read_only_sql(sql: &str) -> bool {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            // Skip the whole comment line (not just the `--`).
            s = match rest.find('\n') {
                Some(end) => rest[end + 1..].trim_start(),
                None => return false,
            };
            continue;
        }
        if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(end) => s = rest[end + 2..].trim_start(),
                None => return false,
            }
            continue;
        }
        break;
    }
    // Leading keyword.
    let mut kw = String::new();
    for c in s.chars() {
        if c.is_ascii_alphabetic() {
            kw.push(c);
        } else {
            break;
        }
    }
    // Reject multi-statement injection: a `;` outside string literals means a
    // second statement (e.g. `SELECT 1; DROP TABLE …`) — never let it through.
    let mut in_string = false;
    let mut quote = ' ';
    for c in s.chars() {
        if in_string {
            if c == quote {
                in_string = false;
            }
            continue;
        }
        match c {
            '\'' | '"' => {
                in_string = true;
                quote = c;
            }
            ';' => return false,
            _ => {}
        }
    }
    if !matches!(kw.to_ascii_uppercase().as_str(), "SELECT" | "EXPLAIN") {
        return false;
    }
    // Defense-in-depth: a write keyword ANYWHERE in the statement is fatal
    // (catches CTE-smuggled writes and anything the leading-keyword gate
    // missed). Tokenize on any non-alphanumeric char and compare lowercased.
    // Tokens inside a string literal are SKIPPED so a query like
    // `WHERE name = 'delete'` isn't a false positive — the literal is data,
    // not a statement.
    const WRITE_TOKENS: &[&str] = &[
        "insert",
        "update",
        "delete",
        "replace",
        "alter",
        "drop",
        "attach",
        "vacuum",
        "pragma",
        "create",
    ];
    let mut in_string = false;
    let mut quote = ' ';
    let mut token = String::new();
    for c in s.chars() {
        if in_string {
            if c == quote {
                in_string = false;
            }
            continue;
        }
        match c {
            '\'' | '"' => {
                in_string = true;
                quote = c;
            }
            c if c.is_ascii_alphanumeric() => token.push(c),
            _ => {
                if WRITE_TOKENS.contains(&token.to_ascii_lowercase().as_str()) {
                    return false;
                }
                token.clear();
            }
        }
    }
    if WRITE_TOKENS.contains(&token.to_ascii_lowercase().as_str()) {
        return false;
    }
    true
}

/// Run a module-supplied SQL query in a contained task so a turso/Limbo
/// "not yet implemented" panic (e.g. `EXISTS`/subqueries the translator can't
/// build) surfaces as a query error instead of killing the connection task.
/// The engine's timeline DB uses the turso/Limbo driver, which panics on
/// some SQL constructs — the read-only boundary protects writes, this protects
/// the process from a panic.
pub async fn guarded_execute_query(
    db: &DatabaseManager,
    sql: &str,
) -> Result<String, String> {
    let sql = sql.to_string();
    let db = db.clone();
    let spawn_db = db.clone();
    let query_fut = async move { spawn_db.execute_query(&sql).await };
    // Bound the query: a hung (not panicking) turso call must not block the
    // calling module's read loop forever (a wedged read loop can't answer the
    // engine's own AuthVerify probe and gets killed).
    match tokio::time::timeout(Duration::from_secs(10), tokio::spawn(query_fut)).await {
        Ok(Ok(Ok(json))) => Ok(json),
        Ok(Ok(Err(e))) => Err(format!("{}", e)),
        Ok(Err(join)) => {
            // A panicking query poisons the turso connection — reopen a fresh
            // one so the timeline DB stays usable.
            let reopened = db.reopen_local().await;
            match reopened {
                Ok(()) => Err(format!("query panicked (unsupported SQL?); database reopened: {}", join)),
                Err(re) => Err(format!("query panicked ({}); reopen failed: {}", join, re)),
            }
        }
        Err(_) => Err("query timed out after 10s".to_string()),
    }
}

/// Build a Container payload of the requested probe type. `payload_json`
/// carries optional fields (e.g. a log line, a SQL string). Unknown types
/// yield None (the probe is skipped).
fn build_probe_payload(ptype: &str, json: &serde_json::Value) -> Option<Payload> {
    match ptype {
        "auth_verify" => Some(Payload::AuthVerify(AuthVerify { cur_auth: String::new() })),
        "log" => Some(Payload::Log(Log {
            log: json.get("log").and_then(|v| v.as_str()).unwrap_or("test probe").to_string(),
            blob: vec![],
        })),
        "database_query" => Some(Payload::DatabaseQuery(DatabaseQuery {
            query_id: json.get("query_id").and_then(|v| v.as_str()).unwrap_or("probe").to_string(),
            sql: json.get("sql").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            params: vec![],
        })),
        "message_pre_process" => Some(Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(ChatMessage {
                platform: json.get("platform").and_then(|v| v.as_str()).unwrap_or("test").to_string(),
                raw_data: vec![],
                raw_message: json.get("raw_message").and_then(|v| v.as_str()).unwrap_or("probe").to_string(),
                user_uuid7: String::new(),
                command: None,
                user_data: None,
                channel_id: String::new(),
            }),
            audio: Vec::new(),
            audio_type: String::new(),
        })),
        "prompt" => Some(Payload::Prompt(Prompt {
            prompt_id_uuid7: Uuid::now_v7().to_string(),
            prompt: json.get("prompt").and_then(|v| v.as_str()).unwrap_or("test probe").to_string(),
            details: json.get("details").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            yes_dialog: String::new(),
            no_dialog: String::new(),
            timeout: 30,
            origin: "cockatiel".to_string(),
            origin_uuid7: String::new(),
            instructions: String::new(),
            link: String::new(),
            input_label: String::new(),
            prompt_type: 0,
        })),
        "shutdown" => Some(Payload::Shutdown(Shutdown { reason: "test probe".to_string() })),
        "message_ack" => Some(Payload::MessageAck(MessageAck {
            message_uuid7: json.get("message_uuid7").and_then(|v| v.as_str()).unwrap_or("probe").to_string(),
        })),
        "command_payload" => Some(Payload::CommandPayload(Command {
            command_name: json.get("command").and_then(|v| v.as_str()).unwrap_or("probe").to_string(),
            command_flag: json.get("flag").and_then(|v| v.as_str()).unwrap_or("!").to_string(),
            command_description: String::new(),
            command_flags: vec![],
        })),
        _ => None,
    }
}

/// `test_probe` virtual query: send a payload of the requested type to ONE
/// module's connection and measure the round-trip (the module's session
/// `last_activity` advancing proves a response). Returns
/// `{ module, type, responded, latency_ms }`.
pub async fn handle_test_probe(
    sql: &str,
    orchestrator: &PipelineOrchestrator,
    auth_store: &AuthStore,
    ui_state: &Arc<Mutex<EngineState>>,
) -> Result<String, String> {
    let payload: serde_json::Value =
        serde_json::from_str(sql).map_err(|e| format!("Invalid test_probe payload: {}", e))?;
    let module = payload.get("module").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ptype = payload.get("type").and_then(|v| v.as_str()).unwrap_or("log").to_string();
    let pjson = payload.get("payload_json").cloned().unwrap_or_else(|| serde_json::json!({}));
    if module.is_empty() {
        return Err("test_probe requires a module".to_string());
    }

    let (instance_uuid, last_activity) = auth_store
        .find_by_module(&module)
        .ok_or_else(|| format!("module '{}' not connected", module))?;

    let probe_payload = build_probe_payload(&ptype, &pjson);
    let container = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "cockatiel".into(),
        module_instance_uuid7: instance_uuid.clone(),
        payload: probe_payload,
    };
    let mut buf = Vec::new();
    container
        .encode(&mut buf)
        .map_err(|e| format!("probe encode failed: {}", e))?;

    let sent = crate::pipeline::send_to_module(&orchestrator.module_senders, &module, container, "test_probe").await;
    if !matches!(sent, SendOutcome::Sent) {
        return Err(format!("module '{}' has no sender", module));
    }

    let now_ms = || -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    };
    let sent_at = now_ms();
    let mut responded = false;
    let mut latency = 0i64;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if let Some((_, la)) = auth_store.find_by_module(&module)
            && la > last_activity
        {
            responded = true;
            latency = now_ms().saturating_sub(sent_at);
            break;
        }
    }
    log_event_broadcast(
        ui_state,
        format!("[test_probe] '{}' <- {} responded={} latency={}ms", module, ptype, responded, latency),
    );
    Ok(serde_json::json!({
        "module": module,
        "type": ptype,
        "responded": responded,
        "latency_ms": latency,
    })
    .to_string())
}

/// Monotonic id identifying the socket a session is currently bound to. A
/// reconnect claims a session under a new id; a stale socket's cleanup can
/// then detect it no longer owns the session and must not evict it.
static NEXT_SOCKET_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Send a WebSocket message with a bounded wait so a wedged socket can't stall
/// the connection task indefinitely. A timeout (or a send error) is treated as
/// a send failure — callers decide whether to `?`/break and let cleanup run.
async fn bounded_ws_send<S>(
    websocket: &mut WebSocketStream<S>,
    msg: WsMessage,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(Duration::from_secs(timeout_secs), websocket.send(msg)).await {
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
    kill_map: Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>,
    cmd_registry: Arc<Mutex<CommandRegistry>>,
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
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Container>(64);

    // Loopback status is computed from the pre-TLS peer address (passed in) —
    // a TlsStream doesn't expose peer_addr directly.

    // ── Await ConnectionRequest ─────────────────────────────────────────
    // Bound the wait for the first message: a client that connects (TLS done)
    // but never speaks must not squat the connection task (or a slot) forever.
    let first_msg = match tokio::time::timeout(
        Duration::from_secs(bounds.handshake_timeout_secs),
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

    let container = Container::decode(data.as_ref())?;

    let Payload::ConnectionRequest(request) = container.payload.as_ref().unwrap() else {
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
    } else {
        // New connection: validate PIN (throttled per peer IP — 5 consecutive
        // failures lock the address out for 60s to blunt brute-force).
        if !pin_gate.check(peer_ip) {
            log_event(
                &ui_state,
                format!("PIN locked out for {}", peer_ip),
            );
            let response = Container {
                version: 1,
                auth_token: String::new(),
                module_name: "cockatiel".into(),
                module_instance_uuid7: String::new(),
                payload: Some(Payload::ConnectionRequestReturn(
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
            let response = Container {
                version: 1,
                auth_token: String::new(),
                module_name: "cockatiel".into(),
                module_instance_uuid7: String::new(),
                payload: Some(Payload::ConnectionRequestReturn(
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
            let response = Container {
                version: 1,
                auth_token: String::new(),
                module_name: "cockatiel".into(),
                module_instance_uuid7: String::new(),
                payload: Some(Payload::ConnectionRequestReturn(
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
            let response = Container {
                version: 1,
                auth_token: String::new(),
                module_name: "cockatiel".into(),
                module_instance_uuid7: String::new(),
                payload: Some(Payload::ConnectionRequestReturn(
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

        let response = Container {
            version: 1,
            auth_token: auth_token.clone(),
            module_name: "cockatiel".into(),
            module_instance_uuid7: assigned_uuid.clone(),
            payload: Some(Payload::ConnectionRequestReturn(
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

                let container = match Container::decode(data.as_ref()) {
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
                    Some(Payload::ConnectionRequest(_)) => {
                        log_event_broadcast(&ui_state, "Ignoring ConnectionRequest from authenticated module");
                    }
                    Some(Payload::AuthVerify(_)) => {
                        // Liveness probe response. The generic per-message auth
                        // check above already verified the token; nothing more
                        // to do — last_activity was refreshed.
                    }
                    Some(Payload::MessagePreProcess(ref msg)) => {
                        // Enrich the chat message with user data from the user DB
                        // (if the adapter supplied a user identifier).
                        let mut enriched = msg.clone();
                        if let Some(chat) = enriched.raw_message.as_mut() {
                            enrich_chat_user(&user_db_client, chat).await;
                            // Command parsing: if the raw message starts with a
                            // registered flag, attach the parsed Command (with
                            // flag values) so the pipeline routes it to the
                            // owning module (+ catch-alls). Unknown commands on
                            // an alerting flag get the apology reply; `!help`
                            // lists every registered command.
                            handle_command_on_ingest(
                                chat,
                                &cmd_registry,
                                &orchestrator,
                                &ui_state,
                            )
                            .await;
                        }
                        let enriched_container = Container {
                            version: container.version,
                            auth_token: container.auth_token.clone(),
                            module_name: container.module_name.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(Payload::MessagePreProcess(enriched)),
                        };
                        if let Err(e) = orchestrator.handle_message_from_module(&enriched_container).await {
                            log_event_broadcast(&ui_state, format!("Pipeline error: {}", e));
                        }
                    }
                    Some(Payload::MessageInProcess(_))
                    | Some(Payload::MessagePostProcess(_))
                    | Some(Payload::MessageAck(_)) => {
                        if let Err(e) = orchestrator.handle_message_from_module(&container).await {
                            log_event_broadcast(&ui_state, format!("Pipeline error: {}", e));
                        }
                    }
                    Some(Payload::TimelineEvent(ref ev)) => {
                        log_event_broadcast(&ui_state, format!("Timeline event: {:?}", ev));
                    }
                    Some(Payload::Log(ref lg)) => {
                        log_event(&ui_state, format!("[{}] {}", module_name, lg.log));
                        log_to_timeline(&db, "module_log", &module_name, &lg.log).await;
                    }
                    Some(Payload::Err(ref err)) => {
                        log_event(&ui_state, format!("[{}] Error: {}", module_name, err.log));
                        log_to_timeline(&db, "module_error", &module_name, &err.log).await;
                    }
                    Some(Payload::DatabaseQuery(ref query)) => {
                        // Virtual queries — engine metadata, not database
                        let (success, result_blob, error_msg) = if query.query_id == "db_status" {
                            // Backup status for the timeline + user database, so
                            // the UI can warn the operator about data-loss risk.
                            let userdb_backup = std::env::var("USER_DB_BACKUP_PATH")
                                .map(|p| !p.trim().is_empty())
                                .unwrap_or(false);
                            let status = serde_json::json!({
                                "timeline_backup": db.backup_configured(),
                                "userdb_backup": userdb_backup,
                                "timeline_db_size_bytes": db.db_size_bytes(),
                                "timeline_db_target_mb": db.target_mb(),
                            });
                            (true, status.to_string().into_bytes(), String::new())
                        } else if query.query_id == "module_list" {
                            let sessions = auth_store.values();
                            // Modules with an unanswered prompt are waiting on
                            // the operator (e.g. a setup/credential question) —
                            // expose it so UIs and the test harness can tell a
                            // connected-but-stuck module apart from an idle one.
                            let pending_prompts: std::collections::HashSet<String> = prompt_routes
                                .lock()
                                .unwrap()
                                .values()
                                .filter_map(|sink| match sink {
                                    PromptSink::Module(origin, _) => Some(origin.clone()),
                                    PromptSink::Engine(_) => None,
                                })
                                .collect();
                            let registered = module_registry.values();

                            // Merge by module name: discovered (manifests on disk) +
                            // registered (persisted modules.json) + live sessions.
                            let mut entries: HashMap<String, serde_json::Value> = HashMap::new();

                            // 1. Discovered modules — what the engine can find on disk
                            // `credential_values` contains `.env` secrets; only the
                            // TUI control surface and term-chat (OAuth login) may see
                            // them. Other modules get the structural list redacted.
                            let expose_creds = may_read_other_credentials(&module_name);
                            for (name, discovered) in discovered_registry.lock().unwrap().iter() {
                                let cred_values = credential_values_map(discovered);
                                let config_complete =
                                    is_config_complete(&discovered.manifest.credentials, &cred_values);
                                let exposed_values = if expose_creds {
                                    cred_values
                                } else {
                                    std::collections::HashMap::new()
                                };
                                entries.insert(
                                    name.clone(),
                                    serde_json::json!({
                                        "name": name,
                                        "description": discovered.manifest.description,
                                        "uuid7": null,
                                        "position": "unknown",
                                        "priority": null,
                                        "autostart": discovered.manifest.autostart,
                                        "connected_at": null,
                                        "shutdown_at": null,
                                        "credentials": discovered.manifest.credentials,
                                        "directory": discovered.directory.to_string_lossy().to_string(),
                                        "credential_values": exposed_values,
                                        "config_complete": config_complete,
                                    }),
                                );
                            }

                            // 2. Registered modules — persisted knowledge from modules.json
                            for m in &registered {
                                let entry = entries
                                    .entry(m.name.clone())
                                    .or_insert_with(|| serde_json::json!({
                                        "name": m.name,
                                        "uuid7": null,
                                        "position": "unknown",
                                        "priority": null,
                                        "autostart": null,
                                        "connected_at": null,
                                        "shutdown_at": null,
                                    }));
                                entry["uuid7"] = serde_json::json!(m.instance_uuid7);
                                entry["position"] = serde_json::json!(m.position);
                                entry["priority"] = serde_json::json!(m.priority);
                            }

                            // 3. Live sessions — overlay current state on top.
                            // Shutdown sessions (disconnected but cleanup not
                            // yet finished) are NOT live: they must not appear
                            // as connected (that made UIs probe a dead module
                            // and restart it).
                            for s in sessions.iter().filter(|s| s.shutdown_at.is_none()) {
                                let entry = entries
                                    .entry(s.module_name.clone())
                                    .or_insert_with(|| serde_json::json!({
                                        "name": s.module_name,
                                        "uuid7": null,
                                        "position": "unknown",
                                        "priority": null,
                                        "autostart": null,
                                        "connected_at": null,
                                        "shutdown_at": null,
                                    }));
                                entry["uuid7"] = serde_json::json!(s.instance_uuid7);
                                entry["position"] = serde_json::json!(s.position);
                                entry["priority"] = serde_json::json!(s.priority);
                                entry["connected_at"] = serde_json::json!(s.connected_at);
                                entry["shutdown_at"] = serde_json::json!(s.shutdown_at);
                                entry["alive"] = serde_json::json!(!s.unresponsive);
                                entry["last_seen"] = serde_json::json!(s.last_activity_ms);
                                entry["pending_prompt"] =
                                    serde_json::json!(pending_prompts.contains(&s.module_name));
                            }

                            let mut list: Vec<serde_json::Value> = entries.into_values().collect();
                            list.sort_by(|a, b| {
                                a.get("name")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .cmp(b.get("name").and_then(|v| v.as_str()).unwrap_or(""))
                            });
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string());
                            (true, json.into_bytes(), String::new())
                        } else if query.query_id == "engine_info" {
                            let config = get_config(&config_state);
                            // The PIN is the master key for first connections — only
                            // the TUI control surface may read it. Other callers get
                            // connection metadata only.
                            let pin = if is_control_surface(&module_name) {
                                serde_json::json!(config::get_pin(&config_state))
                            } else {
                                serde_json::Value::Null
                            };
                            let json = serde_json::json!({
                                "port": config.port,
                                "pin": pin,
                                "timeline_database_location": config.timeline_database_location,
                            }).to_string();
                            (true, json.into_bytes(), String::new())
                        } else if query.query_id == "test_run" {
                            // Compliance test runner — TUI-only gate, mirrors userdb.
                            if !is_control_surface(&module_name) {
                                (false, Vec::new(), "Test runner access denied: not the TUI".to_string())
                            } else {
                                let engine_pin = config::get_pin(&config_state);
                                run_test_suite(&query.sql, &ui_state, engine_pin).await
                            }
                        } else if query.query_id == "test_probe" {
                            // Per-module probe: the engine sends a payload of the
                            // requested type to one module's connection and
                            // measures the round-trip. TUI/test-runner gated.
                            if !is_control_surface(&module_name) && !is_test_runner(&module_name) {
                                (false, Vec::new(), "test_probe denied: not the TUI/test-runner".to_string())
                            } else {
                                let probe = handle_test_probe(
                                    &query.sql,
                                    &orchestrator,
                                    &auth_store,
                                    &ui_state,
                                ).await;
                                match probe {
                                    Ok(json) => (true, json.into_bytes(), String::new()),
                                    Err(e) => (false, Vec::new(), e),
                                }
                            }
                        } else if query.query_id == "audit_list" {
                            // List held-for-audit messages. TUI-only.
                            if !is_control_surface(&module_name) {
                                (false, Vec::new(), "Audit access denied: not the TUI".to_string())
                            } else {
                                let payload: serde_json::Value = serde_json::from_str(&query.sql).unwrap_or(serde_json::json!({}));
                                let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;
                                let offset = payload.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                                match db.list_audit(limit, offset).await {
                                    Ok(items) => {
                                        let json = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
                                        (true, json.into_bytes(), String::new())
                                    }
                                    Err(e) => (false, Vec::new(), format!("audit_list failed: {}", e)),
                                }
                            }
                        } else if query.query_id == "audit_approve" || query.query_id == "audit_reject" {
                            // Approve (resubmit as normal) or reject an audited message. TUI-only.
                            if !is_control_surface(&module_name) {
                                (false, Vec::new(), "Audit access denied: not the TUI".to_string())
                            } else {
                                let payload: serde_json::Value = serde_json::from_str(&query.sql).unwrap_or(serde_json::json!({}));
                                let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
                                if uuid7.is_empty() {
                                    (false, Vec::new(), "audit action requires uuid7".to_string())
                                } else {
                                    let approve = query.query_id == "audit_approve";
                                    match db.release_audit(uuid7.as_bytes(), approve).await {
                                        Ok(_) => {
                                            let json = serde_json::json!({ "uuid7": uuid7, "released": approve }).to_string();
                                            (true, json.into_bytes(), String::new())
                                        }
                                        Err(e) => (false, Vec::new(), format!("audit action failed: {}", e)),
                                    }
                                }
                            }
                        } else if query.query_id.starts_with("mod_") {
                            // Mod commands from adapters: resolve the target user
                            // by platform + handle, then apply the action. The
                            // actor (human trigger) is verified first.
                            mod_virtual_query(&user_db_client, &query.query_id, &query.sql, &ui_state, &module_name).await
                        } else if query.query_id == "chat_commend" || query.query_id == "chat_reprimand" {
                            // Chat-command ratings (commend/reprimand modules):
                            // ANY verified user may rate another user (no mod
                            // status required). The 24h reprimand cooldown is
                            // enforced in the user-db rating_history.
                            chat_rating_virtual_query(&user_db_client, &query.query_id, &query.sql, &ui_state, &module_name).await
                        } else if query.query_id == "chat_verify_identity" {
                            // Identity bootstrap / write-through — term-chat only.
                            chat_verify_identity_virtual_query(&user_db_client, &query.sql, &ui_state, &module_name).await
                        } else if query.query_id == "userdb_adjust_score" {
                            // Real arbitrary score delta for the score-messages
                            // module (no ±1 clamp, no user-db rating cooldown).
                            // The TUI control surface may also call it.
                            if !is_control_surface(&module_name) && module_name != "score-messages" {
                                (false, Vec::new(), "userdb_adjust_score denied: not the TUI or score-messages".to_string())
                            } else {
                                userdb_adjust_score_virtual_query(&user_db_client, &query.sql, &ui_state, &module_name).await
                            }
                        } else if query.query_id.starts_with("userdb_") {
                            // User database is engine-internal — only the TUI
                            // control surface may access it. Modules are denied.
                            if !is_control_surface(&module_name) {
                                (false, Vec::new(), "User database access denied: not the TUI".to_string())
                            } else {
                                userdb_virtual_query(&user_db_client, &query.query_id, &query.sql, &ui_state, &module_name, peer_loopback).await
                            }
                        } else if query.query_id == "set_credentials" {
                            // Writing credentials to another module's `.env` /
                            // `config.json` is a control-surface operation — only
                            // the TUI may do it. Otherwise any authenticated module
                            // could rewrite any other module's secrets.
                            if !is_control_surface(&module_name) {
                                (false, Vec::new(), "set_credentials denied: not the TUI".to_string())
                            } else {
                            // Expects JSON: { "module_name": "...", "values": { "key": "value", ... } }
                            let mut result = (false, Vec::new(), "Failed to parse set_credentials payload".to_string());
                            if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&query.sql) {
                                let module_name = payload.get("module_name").and_then(|v| v.as_str()).unwrap_or("");
                                let values: HashMap<String, String> = payload
                                    .get("values")
                                    .and_then(|v| v.as_object())
                                    .map(|obj| {
                                        obj.iter()
                                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                            .collect()
                                    })
                                    .unwrap_or_default();

                                let found = discovered_registry.lock().unwrap().get(module_name).cloned();
                                match found {
                                    Some(module) => {
                                        let fields = module.manifest.credentials.clone();
                                        match validate_credential_fields(&fields, &values) {
                                            Ok(()) => {
                                                match save_module_credentials(&module, &fields, &values) {
                                                    Ok(()) => {
                                                        log_event_broadcast(&ui_state, format!("[{}] Credentials updated for '{}'", module_name, module_name));
                                                        // The TUI supervisor owns process lifecycle — it restarts the
                                                        // module after this query so the new config is picked up.
                                                        result = (true, serde_json::json!({"success": true, "message": format!("Credentials saved for '{}'", module_name)}).to_string().into_bytes(), String::new());
                                                    }
                                                    Err(e) => result = (false, Vec::new(), e),
                                                }
                                            }
                                            Err(e) => result = (false, Vec::new(), e),
                                        }
                                    }
                                    None => result = (false, Vec::new(), format!("Unknown module: {}", module_name)),
                                }
                            }
                            result
                            }
                        } else if query.query_id == "audio_for_message" {
                            // Fetch a message's rendered audio (raw bytes) so
                            // displays can play it. Any module may read it.
                            let payload: serde_json::Value =
                                serde_json::from_str(&query.sql).unwrap_or(serde_json::json!({}));
                            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
                            if uuid7.is_empty() {
                                (false, Vec::new(), "audio_for_message requires uuid7".to_string())
                            } else {
                                match db.get_audio(uuid7.as_bytes()).await {
                                    Ok(Some((_audio_type, bytes))) => {
                                        (true, bytes, String::new())
                                    }
                                    Ok(None) => (true, Vec::new(), String::new()),
                                    Err(e) => (false, Vec::new(), format!("audio_for_message failed: {}", e)),
                                }
                            }
                        } else if query.query_id == "test_archive" {
                            // Compliance-test archival — test-runner only. The
                            // engine inserts archival rows via its own method
                            // (insert_archival_event) rather than allowing raw
                            // INSERTs through the SQL fallback.
                            if !is_test_runner(&module_name) {
                                (false, Vec::new(), "test_archive access denied: not the test runner".to_string())
                            } else {
                                let payload: serde_json::Value =
                                    serde_json::from_str(&query.sql).unwrap_or(serde_json::json!({}));
                                let batch_uuid =
                                    payload.get("batch_uuid").and_then(|v| v.as_str()).unwrap_or("test");
                                let mut inserted = 0u64;
                                let mut error = String::new();
                                if let Some(entries) = payload.get("entries").and_then(|v| v.as_array()) {
                                    for e in entries {
                                        let json = e.get("json").and_then(|v| v.as_str()).unwrap_or("");
                                        if json.is_empty() {
                                            continue;
                                        }
                                        match db
                                            .insert_archival_event("test", "test_archive", json, &format!("test-runner|{}", batch_uuid))
                                            .await
                                        {
                                            Ok(()) => inserted += 1,
                                            Err(e) => {
                                                error = format!("{}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                if error.is_empty() {
                                    let out = serde_json::json!({ "inserted": inserted }).to_string();
                                    (true, out.into_bytes(), String::new())
                                } else {
                                    (false, Vec::new(), format!("test_archive failed after {} inserts: {}", inserted, error))
                                }
                            }
                        } else {
                            // Regular database query — hard read-only boundary.
                            // Only SELECT/EXPLAIN may run; any write must
                            // go through the engine's own methods or a
                            // dedicated virtual query.
                            if is_read_only_sql(&query.sql) {
                                match guarded_execute_query(&db, &query.sql).await {
                                    Ok(json) => (true, json.into_bytes(), String::new()),
                                    Err(e) => {
                                        let msg = format!("{}", e);
                                        log_event_broadcast(&ui_state, format!("[{}] Query error: {}", module_name, msg));
                                        (false, Vec::new(), msg)
                                    }
                                }
                            } else {
                                (false, Vec::new(), "Denied: only read-only SQL (SELECT/EXPLAIN) is allowed via DatabaseQuery — use a dedicated virtual query for writes".to_string())
                            }
                        };
                        let response = Container {
                            version: 1,
                            auth_token: container.auth_token.clone(),
                            module_name: "cockatiel".into(),
                            module_instance_uuid7: instance_uuid7.clone(),
                            payload: Some(Payload::DatabaseQueryResult(DatabaseQueryResult {
                                query_id: query.query_id.clone(),
                                success,
                                error: error_msg,
                                result_blob,
                            })),
                        };
                        if tokio::time::timeout(Duration::from_millis(1000), tx.send(response)).await.is_err() {
                            log_event_broadcast(&ui_state, format!("[{}] dropped query response (outbound full)", module_name));
                        }
                    }
Some(Payload::SendToPlatforms(send)) => {
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
                        let forward = Container {
                            version: 1,
                            auth_token: container.auth_token.clone(),
                            module_name: container.module_name.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(Payload::SendToPlatforms(send.clone())),
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
Some(Payload::ModuleControl(_)) => {
                        // Process lifecycle is owned by the TUI supervisor.
                        // The engine no longer starts/stops modules.
                        log_event_broadcast(&ui_state, "[{}] ModuleControl ignored (processes owned by TUI)".to_string());
                    }
                    Some(Payload::CommandsPayload(commands)) => {
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
                    Some(Payload::CommandPayload(command)) => {
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
                            let forward = Container {
                                version: 1,
                                auth_token: String::new(),
                                module_name: "cockatiel".into(),
                                module_instance_uuid7: String::new(),
                                payload: Some(Payload::MessagePreProcess(
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
                    Some(Payload::Prompt(ref prompt)) => {
                        // A module is asking the user something (e.g. "allow this
                        // action?"). Forward it to every OTHER connected module so
                        // they can display it, and remember the origin so a
                        // PromptResponse can be routed back to it.
                        let prompt = prompt.clone();
                        prompt_routes.lock().unwrap().insert(
                            prompt.prompt_id_uuid7.clone(),
                            PromptSink::Module(module_name.clone(), tx.clone()),
                        );
                        let forward = Container {
                            version: 1,
                            auth_token: container.auth_token.clone(),
                            module_name: container.module_name.clone(),
                            module_instance_uuid7: container.module_instance_uuid7.clone(),
                            payload: Some(Payload::Prompt(prompt)),
                        };
                        let senders: Vec<tokio::sync::mpsc::Sender<Container>> = {
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
                    Some(Payload::PromptResponse(ref resp)) => {
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
                                    let forward = Container {
                                        version: 1,
                                        auth_token: container.auth_token.clone(),
                                        module_name: container.module_name.clone(),
                                        module_instance_uuid7: container.module_instance_uuid7.clone(),
                                        payload: Some(Payload::PromptResponse(resp)),
                                    };
                                    let _ = tokio::time::timeout(Duration::from_millis(1000), sender.send(forward)).await;
                                }
                            }
                        }
                    }
                    Some(Payload::AuditFlag(ref flag)) => {
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
                    Some(Payload::ChatMessageRejected(ref rej)) => {
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
                    break;
                }
            }
            killed = kill_rx.changed() => {
                // The probe task flagged this session as unresponsive —
                // close it so the disconnect cleanup below runs.
                if killed.is_ok() && *kill_rx.borrow() {
                    break;
                }
            }
        }
    }

    // Unregister the kill switch.
    kill_map.lock().await.remove(&instance_uuid7);

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
