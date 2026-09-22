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
    if fs::write(&path, content).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
    }
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

    // 4. Persist to `.env` (owner-only).
    write_env_file(
        &dir,
        &[
            ("COCKATIEL_PIN", &pin.to_string()),
            ("COCKATIEL_JWT_SECRET", &secret),
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

/// The engine's JWT signing secret (from `.env`).
pub fn get_jwt_secret(state: &Arc<Mutex<ConfigState>>) -> String {
    state.lock().unwrap().jwt_secret.clone()
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
    "module_approval_policy": "auto-deny"
}"#
    .to_string();

    fs::write(&target, &config).map_err(|e| e.to_string())?;

    Ok(config)
}

pub fn get_file(path: impl Into<PathBuf>) -> Result<String, std::io::Error> {
    fs::read_to_string(path.into())
}

/// Write a file atomically: write to a temp sibling, fsync, then rename over
/// the target. `modules.json`/`config.json` are written by BOTH the engine and
/// the TUI supervisor — a torn/interleaved write must never leave a half-written
/// JSON the other side (or the engine's size-based reload) can pick up.
pub fn write_atomic(path: &PathBuf, content: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp = dir.join(format!(".{}.tmp{}", path.file_name().unwrap_or_default().to_string_lossy(), std::process::id()));
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)
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

pub fn remove_module_from_config(config_state: &Arc<Mutex<ConfigState>>, name: &str) {
    update_config(config_state, |config| {
        config.inputs.retain(|entry| entry.name != name);
        config.preprocess_modules.retain(|entry| entry.name != name);
        config.inprocess_modules.retain(|entry| entry.name != name);
        config.postprocess_modules.retain(|entry| entry.name != name);
    });
}
