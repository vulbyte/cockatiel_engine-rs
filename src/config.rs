use crate::ModuleEntry;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub timeline_database_location: String,
    pub timeline_database_backup_location: String,
    /// Local timeline DB target size in MB — the DB-size warning fires when
    /// the file passes the 5 MB floor and reaches 95% of this target.
    #[serde(default = "default_timeline_target_mb")]
    pub timeline_database_target_mb: u32,
    pub port: u16,

    #[serde(default)]
    pub inputs: Vec<ModuleEntry>,

    #[serde(default, rename = "preprocessModules")]
    pub preprocess_modules: Vec<ModuleEntry>,

    #[serde(default, rename = "inprocessModules")]
    pub inprocess_modules: Vec<ModuleEntry>,

    #[serde(default, rename = "postprocessModules")]
    pub postprocess_modules: Vec<ModuleEntry>,

    /// Dead-air threshold (seconds) before the engine probes a module with an
    /// AuthVerify. Defaults to 30.
    #[serde(default = "default_probe_interval_secs")]
    pub module_probe_interval_secs: u64,

    /// How long (seconds) a module has to answer a probe before it is
    /// declared unresponsive. Defaults to 15.
    #[serde(default = "default_probe_response_secs")]
    pub module_probe_response_secs: u64,

    /// Headless module-approval policy: "auto-deny" (default) or "auto-allow".
    #[serde(default = "default_approval_policy")]
    pub module_approval_policy: String,

    /// Maximum size (bytes) of a single WebSocket message/frame the engine
    /// will accept from a module connection. Defaults to 16 MB.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u64,

    /// Maximum number of concurrent module/TUI connections. Defaults to 128.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Seconds a connection has to complete its TLS handshake and send its
    /// first (ConnectionRequest) message before it is dropped. Defaults to 10.
    #[serde(default = "default_handshake_timeout_secs")]
    pub handshake_timeout_secs: u64,

    /// Seconds a WebSocket outbound send may block before it is treated as a
    /// send failure. Defaults to 5.
    #[serde(default = "default_send_timeout_secs")]
    pub send_timeout_secs: u64,

    /// Seconds the engine waits after startup before draining stranded
    /// 'queued' messages from the timeline DB. Defaults to 10.
    #[serde(default = "default_recovery_grace_secs")]
    pub recovery_grace_secs: u64,

    /// Whether the engine starts with the message pipeline PAUSED. When true
    /// (the default) the engine ingests, persists and parses commands normally
    /// but dispatches nothing to any module until the control surface resumes
    /// it; queued messages accumulate and are replayed on resume, so nothing is
    /// lost. Overridden by `COCKATIEL_START_PAUSED` — see [`start_paused`].
    #[serde(default = "default_start_paused")]
    pub start_paused: bool,

    /// Whether the engine honours an `engine_shutdown` request from the control
    /// surface (the TUI). Defaults to false.
    ///
    /// The engine decides for itself whether to go down: shutting the engine
    /// stops ingest, the pipeline and every connected module, so it must never
    /// happen because something that merely holds a socket asked nicely — or
    /// asked maliciously. An engine that has never heard of this feature has to
    /// refuse, so false is the default and the operator opts in deliberately.
    #[serde(default = "default_shutdown_on_request")]
    pub shutdown_on_request: bool,

    /// The minimum role required to CREATE/RESOLVE a prediction
    /// ("owner" | "admin" | "mod" | "user"). Defaults to "mod". The predictions
    /// module reads ITS OWN config.json for the enforcement decision; this
    /// field is for the display / other surfaces.
    #[serde(default = "default_creator_role")]
    pub prediction_creator_role: String,
}

fn default_probe_interval_secs() -> u64 {
    30
}

fn default_timeline_target_mb() -> u32 {
    50
}

fn default_probe_response_secs() -> u64 {
    15
}

fn default_approval_policy() -> String {
    "auto-deny".to_string()
}

fn default_max_message_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_max_connections() -> usize {
    128
}

fn default_handshake_timeout_secs() -> u64 {
    10
}

fn default_send_timeout_secs() -> u64 {
    5
}

fn default_recovery_grace_secs() -> u64 {
    10
}

/// The engine boots PAUSED. See [`start_paused`].
fn default_start_paused() -> bool {
    true
}

/// Engine shutdown is DISABLED until an operator opts in. See
/// [`shutdown_on_request`](Config::shutdown_on_request).
fn default_shutdown_on_request() -> bool {
    false
}

/// The minimum role a user needs to create/resolve a prediction. Defaults to
/// "mod". See [`prediction_creator_role`](Config::prediction_creator_role).
fn default_creator_role() -> String {
    "mod".to_string()
}

/// Should the engine boot with the message pipeline held?
///
/// Paused is the default, and it is the safe direction to be wrong in: a
/// backlog of unattended messages must never be dispatched to live modules
/// without an operator asking for it. Nothing is lost while paused — every
/// message is written to the timeline as 'queued' and replayed on resume.
///
/// `COCKATIEL_START_PAUSED` overrides the config value, following the same
/// precedence as every other environment override in the engine (a real
/// environment variable wins over the file, and `load_env_file` has already
/// pulled any `.env` key into the environment by the time this is read). It is
/// the headless / CI escape hatch: set it to `0` (or `false`) to boot running,
/// because nothing would ever press the resume key. An unparseable value falls
/// back to the config value rather than guessing in either direction.
pub fn start_paused(config: &Config) -> bool {
    resolve_start_paused(config, env::var("COCKATIEL_START_PAUSED").ok().as_deref())
}

/// [`start_paused`] with the environment value passed in, so the precedence and
/// the parsing are testable without mutating the process environment.
pub fn resolve_start_paused(config: &Config, env_value: Option<&str>) -> bool {
    // Trimmed and lowercased, so `0`, `FALSE` and ` off ` all mean the same
    // thing — a value that is merely mis-cased must not fall through to the
    // default and boot the engine paused for a headless run that asked to run.
    match env_value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("0") | Some("false") | Some("no") | Some("off") => false,
        Some("1") | Some("true") | Some("yes") | Some("on") => true,
        _ => config.start_paused,
    }
}

pub struct ConfigState {
    pub path: PathBuf,
    pub last_size: u64,
    pub config: Config,
    /// Connection PIN (secret) — lives in `.env`, not config.json.
    pub pin: u32,
    /// JWT signing secret — lives in `.env`, not config.json.
    pub jwt_secret: String,
}

/// The engine's secrets file (next to config.json).
pub fn env_path(dir: &PathBuf) -> PathBuf {
    dir.join(".env")
}

/// Load a KEY=VALUE `.env` file into the process environment. Real environment
/// variables win — this only fills in what wasn't already set.
pub fn load_env_file(dir: &PathBuf) {
    let Ok(content) = fs::read_to_string(env_path(dir)) else { return };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let value = value.trim().trim_matches('"').to_string();
            if key.is_empty() {
                continue;
            }
            if env::var(&key).is_err() {
                // Safe: startup-only, values come from a file we own, and the
                // (single-threaded) engine reads them immediately after.
                unsafe {
                    env::set_var(key, value);
                }
            }
        }
    }
}

/// Merge key=value pairs into a `.env` file (creating it if missing), and
/// tighten the file permissions to owner-only since it holds secrets.
fn write_env_file(dir: &PathBuf, pairs: &[(&str, &str)]) {
    let path = env_path(dir);
    let mut lines: Vec<String> = fs::read_to_string(&path)
        .map(|c| c.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    for (key, value) in pairs {
        let entry = format!("{}={}", key, value);
        let prefix = format!("{}=", key);
        if let Some(idx) = lines.iter().position(|l| l.trim().starts_with(&prefix)) {
            lines[idx] = entry;
        } else {
            lines.push(entry);
        }
    }
    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    // Atomic owner-only write (unique temp + 0o600 before rename) so the
    // secrets never sit in a world-readable window.
    let _ = write_atomic(&path, &content);
}

/// Resolve the engine's secrets (PIN + JWT secret) and persist them in `.env`.
/// Precedence: real env vars → `.env` file → legacy config.json (migration) →
/// freshly generated. Also strips legacy `paring_pin`/`jwt_secret` out of
/// config.json so secrets no longer live in the settings file, and stores the
/// resolved values on the ConfigState. Returns the JWT secret.
pub fn ensure_secrets(state: &Arc<Mutex<ConfigState>>) -> String {
    let dir = state
        .lock()
        .unwrap()
        .path
        .parent()
        .unwrap_or(&PathBuf::from("."))
        .to_path_buf();
    load_env_file(&dir);

    // 1. Real env vars take precedence.
    let mut pin: Option<u32> = env::var("COCKATIEL_PIN").ok().and_then(|v| v.parse().ok());
    let mut secret: Option<String> = env::var("COCKATIEL_JWT_SECRET")
        .ok()
        .filter(|s| !s.trim().is_empty());

    // 2. Migrate from a legacy config.json that still carried the secrets.
    {
        let cfg_path = state.lock().unwrap().path.clone();
        if let Ok(data) = fs::read_to_string(&cfg_path) {
            if let Ok(root) = serde_json::from_str::<serde_json::Value>(&data) {
                if let Some(p) = root.get("paring_pin").and_then(|v| v.as_u64()) {
                    pin.get_or_insert(p as u32);
                }
                if let Some(s) = root.get("jwt_secret").and_then(|v| v.as_str()) {
                    if !s.is_empty() {
                        secret.get_or_insert_with(|| s.to_string());
                    }
                }
            }
        }
    }

    // 3. Generate anything still missing.
    let pin = pin.unwrap_or_else(|| (100000 + (Uuid::new_v4().as_u128() % 900000) as u32));
    let secret = secret.unwrap_or_else(|| Uuid::new_v4().to_string());

    // 4. Persist to `.env` (owner-only). Secrets are generated when missing;
    //    settings are written with their code-default so they are present and
    //    editable in place (a real environment variable always wins at load).
    write_env_file(
        &dir,
        &[
            ("COCKATIEL_PIN", &pin.to_string()),
            ("COCKATIEL_JWT_SECRET", &secret),
            ("COCKATIEL_BIND_IP", "127.0.0.1"),
            ("USER_DB_HOST", "127.0.0.1"),
            ("USER_DB_PORT", "9736"),
        ],
    );

    // 5. Strip the legacy secrets out of config.json so settings stay
    // non-sensitive, and apply the resolved values to the in-memory config.
    {
        let state = state.lock().unwrap();
        let cfg_path = &state.path;
        if let Ok(data) = fs::read_to_string(cfg_path) {
            if let Ok(mut root) = serde_json::from_str::<serde_json::Value>(&data) {
                let mut changed = false;
                if root.as_object().is_some() && root.get("paring_pin").is_some() {
                    root.as_object_mut().unwrap().remove("paring_pin");
                    changed = true;
                }
                if root.as_object().is_some() && root.get("jwt_secret").is_some() {
                    root.as_object_mut().unwrap().remove("jwt_secret");
                    changed = true;
                }
                if changed {
                    if let Ok(pretty) = serde_json::to_string_pretty(&root) {
                        let _ = write_atomic(&cfg_path, &pretty);
                    }
                }
            }
        }
    }

    let mut state = state.lock().unwrap();
    state.pin = pin;
    state.jwt_secret = secret.clone();
    secret
}

/// The engine's connection PIN (from `.env`).
pub fn get_pin(state: &Arc<Mutex<ConfigState>>) -> u32 {
    state.lock().unwrap().pin
}

/// Startup backfill (house convention): write any NEW config keys into
/// config.json with their code defaults so they are explicit and editable on
/// disk. Missing keys already resolve to the same defaults in memory via
/// `#[serde(default)]`; this only persists them. All other keys are preserved
/// untouched, and the file is only rewritten (atomically) when something was
/// actually added.
pub fn backfill_config_defaults(state: &Arc<Mutex<ConfigState>>) {
    let cfg_path = state.lock().unwrap().path.clone();
    let Ok(data) = fs::read_to_string(&cfg_path) else {
        return;
    };
    let Ok(mut root) = serde_json::from_str::<serde_json::Value>(&data) else {
        return;
    };
    let obj = match root.as_object_mut() {
        Some(obj) => obj,
        None => return,
    };

    let mut changed = false;
    let defaults: [(&str, serde_json::Value); 8] = [
        ("max_message_bytes", serde_json::json!(default_max_message_bytes())),
        ("max_connections", serde_json::json!(default_max_connections())),
        ("handshake_timeout_secs", serde_json::json!(default_handshake_timeout_secs())),
        ("send_timeout_secs", serde_json::json!(default_send_timeout_secs())),
        ("recovery_grace_secs", serde_json::json!(default_recovery_grace_secs())),
        ("start_paused", serde_json::json!(default_start_paused())),
        ("shutdown_on_request", serde_json::json!(default_shutdown_on_request())),
        ("prediction_creator_role", serde_json::json!(default_creator_role())),
    ];
    for (key, value) in defaults {
        if obj.get(key).is_none() {
            obj.insert(key.to_string(), value);
            changed = true;
        }
    }

    if changed {
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = write_atomic(&cfg_path, &pretty);
        }
    }
}

pub fn create_config(directory: PathBuf) -> Result<String, String> {
    let target = directory.join("config.json");

    // Never overwrite an existing config — regenerating would rotate the
    // engine's settings and invalidate module ordering.
    if let Ok(content) = fs::read_to_string(&target) {
        return Ok(content);
    }

    // NOTE: secrets (PIN, JWT secret) intentionally live in `.env`, created by
    // `ensure_secrets` — config.json holds settings only.
    let config = r#"{
    "timeline_database_location": "./cockatiel_data.db",
    "timeline_database_backup_location": "./cockatiel_backup.db",
    "port": 9734,
    "inputs": [],
    "preprocessModules": [],
    "inprocessModules": [],
    "postprocessModules": [],
    "module_probe_interval_secs": 30,
    "module_probe_response_secs": 15,
    "module_approval_policy": "auto-deny",
    "max_message_bytes": 16777216,
    "max_connections": 128,
    "handshake_timeout_secs": 10,
    "send_timeout_secs": 5,
    "recovery_grace_secs": 10,
    "start_paused": true,
    "shutdown_on_request": false,
    "prediction_creator_role": "mod"
}"#
    .to_string();

    fs::write(&target, &config).map_err(|e| e.to_string())?;

    Ok(config)
}

pub fn get_file(path: impl Into<PathBuf>) -> Result<String, std::io::Error> {
    fs::read_to_string(path.into())
}

/// Write a file atomically: write to a unique-name temp sibling (owner-only),
/// fsync, then rename over the target. `modules.json`/`config.json` are
/// written by BOTH the engine and the TUI supervisor — a torn/interleaved
/// write must never leave a half-written JSON the other side (or the engine's
/// size-based reload) can pick up. The temp name is a fresh UUIDv7 so an
/// attacker can't pre-create a symlink at a predictable `.name.tmp<pid>` path.
pub fn write_atomic(path: &PathBuf, content: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        Uuid::now_v7()
    ));
    std::fs::write(&tmp, content)?;
    chmod_owner_only(&tmp)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// Restrict a file to owner-only access (0o600 on unix). No-op on platforms
/// without unix permission bits. Used on secret stores and the TLS key.
pub fn chmod_owner_only(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

pub fn modules_path(config_state: &Arc<Mutex<ConfigState>>) -> PathBuf {
    let state = config_state.lock().unwrap();
    state.path.parent().unwrap_or(&PathBuf::from(".")).join("modules.json")
}

pub fn get_config(state: &Arc<Mutex<ConfigState>>) -> Config {
    let mut state = state.lock().unwrap();

    if let Ok(metadata) = fs::metadata(&state.path) {
        let size = metadata.len();

        if size != state.last_size {
            if let Ok(content) = fs::read_to_string(&state.path) {
                if let Ok(config) = serde_json::from_str(&content) {
                    state.config = config;
                    state.last_size = size;
                }
            }
        }
    }

    state.config.clone()
}

/// Force `get_config` to re-read the file on its next call, bypassing the
/// size gate.
///
/// The size gate exists so the message hot path never pays a file read when
/// nothing changed — but a rewrite that happens to land on the SAME byte
/// length is invisible to it. The config-poll task rewrites ordering lists at
/// runtime (the TUI's Shift+up/down stage moves), and a same-size reorder must
/// not be silently ignored, so the poll calls this before reading.
pub fn refresh_config(state: &Arc<Mutex<ConfigState>>) {
    let mut state = state.lock().unwrap();
    state.last_size = u64::MAX;
}

pub async fn verify_config() -> Result<(String, PathBuf), Box<dyn std::error::Error>> {
    for path in ["../config.json"] {
        if let Ok(content) = get_file(path) {
            return Ok((content, PathBuf::from(path)));
        }
    }

    let current = env::current_dir()?;

    let target = if current.ends_with("cockatiel-engine") {
        current.parent().unwrap_or(&current).to_path_buf()
    } else {
        current
    };

    let content = create_config(target.clone())?;

    Ok((content, target.join("config.json")))
}

pub fn update_config<F>(state: &Arc<Mutex<ConfigState>>, update: F)
where
    F: FnOnce(&mut Config),
{
    let mut state = state.lock().unwrap();

    update(&mut state.config);

    if let Ok(serialized) = serde_json::to_string_pretty(&state.config) {
        if write_atomic(&state.path, &serialized).is_ok() {
            if let Ok(metadata) = fs::metadata(&state.path) {
                state.last_size = metadata.len();
            }
        }
    }
}

pub fn add_module_to_config(
    config_state: &Arc<Mutex<ConfigState>>,
    name: &str,
    position: &str,
    priority: i32,
) {
    update_config(config_state, |config| {
        // The operator's stage moves (the TUI's Shift+up/down) are the
        // AUTHORITATIVE placement once a module is in config.json. This
        // function runs on every module connect, and forcing a module back to
        // the position it happened to CONNECT with would silently revert a move
        // the operator just made (the classic "moves snap back" bug). So a
        // module already sitting in SOME ordering list keeps its operator-set
        // placement — only its priority is refreshed.
        let already_placed = ["input", "preprocess", "inprocess", "postprocess"].iter().any(|label| {
            let list = match *label {
                "input" => &config.inputs,
                "preprocess" => &config.preprocess_modules,
                "inprocess" => &config.inprocess_modules,
                "postprocess" => &config.postprocess_modules,
                _ => unreachable!(),
            };
            list.iter().any(|entry| entry.name == name)
        });

        if already_placed {
            // Refresh the entry's priority wherever the operator placed it; do
            // not move it back to `position`.
            for list in [
                &mut config.inputs,
                &mut config.preprocess_modules,
                &mut config.inprocess_modules,
                &mut config.postprocess_modules,
            ] {
                if let Some(existing) = list.iter_mut().find(|entry| entry.name == name) {
                    existing.priority = priority;
                    return;
                }
            }
            return;
        }

        // Not placed yet (first registration): remove from the other stage
        // lists defensively, then add to `position` exactly once.
        let list = match position {
            "input" => &mut config.inputs,
            "preprocess" => &mut config.preprocess_modules,
            "inprocess" => &mut config.inprocess_modules,
            "postprocess" => &mut config.postprocess_modules,
            _ => return,
        };

        if let Some(existing) = list.iter_mut().find(|entry| entry.name == name) {
            existing.priority = priority;
            return;
        }

        list.push(ModuleEntry {
            name: name.into(),
            priority,
        });

        list.sort_by_key(|entry| entry.priority);
    });
}
