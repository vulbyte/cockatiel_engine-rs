//! Startup auto-configuration: for every module discovered in `./modules`, make
//! sure it is registered, wired into the pipeline ordering, and has a connection
//! config that points at THIS engine.
//!
//! The engine discovers modules on disk but (until now) left registration and
//! ordering entirely to the TUI, and connection configs to each module's first
//! launch. A fresh install therefore required manually starting / approving each
//! module one by one. This pass runs once at startup and makes the modules
//! directory self-sufficient:
//!
//! 1. **Registration** — each discovered module gets an entry in `modules.json`
//!    (`auto_auth: false` for a brand-new module so its first connect still asks
//!    the operator; an existing entry is preserved untouched, approval included).
//! 2. **Pipeline ordering** — each discovered module is added to the engine's
//!    `config.json` ordering list that matches its manifest capability (input →
//!    `inputs`, preprocess → `preprocessModules`, …). A module already placed by
//!    the operator in a prior session keeps its operator-set placement; only a
//!    never-before-placed module is added.
//! 3. **Connection config** — each discovered module gets a `config.json` in its
//!    own directory with `ip`/`port`/`pin`/`module_name`/`position`/`priority`
//!    pointing at this engine, but ONLY if the file does not already exist or is
//!    missing those keys. Existing configs (including `module_specific`
//!    credentials) are never overwritten.
//!
//! All three are idempotent: running the engine again is a no-op once the
//! configs exist, and the operator's prior choices (approvals, stage moves,
//! credentials) always win.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::config::{self, ConfigState};
use crate::module_manager::ModuleRegistry;

/// The engine's own address, used to point module connection configs at it.
const DEFAULT_BIND_IP: &str = "127.0.0.1";

/// Map a module manifest `capabilities` string to a `config.json` ordering-list
/// key. Mirrors the TUI supervisor's `config_list_key` so engine-side
/// auto-configuration and TUI-side registration agree on placement.
fn config_list_key(capabilities: &str) -> &'static str {
    match capabilities {
        "input" | "inputs" | "connection" => "inputs",
        "preprocess" => "preprocessModules",
        "inprocess" => "inprocessModules",
        "postprocess" | "output" | "outputs" | "display" => "postprocessModules",
        _ => "preprocessModules",
    }
}

/// The ordering-list keys, in pipeline order, used to look for an existing
/// placement (a module may have been moved by the operator).
const ORDERING_KEYS: [&str; 4] = [
    "inputs",
    "preprocessModules",
    "inprocessModules",
    "postprocessModules",
];

/// Run the full auto-configuration pass over the discovered modules.
///
/// `config_state` carries the engine's config path, port and PIN; `registry` is
/// the set of modules discovered on disk. Returns a human-readable log of what
/// was done (for the startup log).
pub fn run(config_state: &Arc<Mutex<ConfigState>>, registry: &ModuleRegistry) -> Vec<String> {
    let mut log = Vec::new();
    let engine_port = {
        let state = config_state.lock().unwrap();
        state.config.port
    };
    let engine_pin = config::get_pin(config_state);

    let mut sorted: Vec<_> = registry.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    for (name, module) in sorted {
        if let Some(msg) = register_module(config_state, name, &module.manifest.capabilities) {
            log.push(format!("[auto-config] {}: {}", name, msg));
        }
        if let Some(msg) = add_to_ordering(config_state, name, &module.manifest.capabilities) {
            log.push(format!("[auto-config] {}: {}", name, msg));
        }
        if let Some(msg) = write_connection_config(
            &module.directory,
            name,
            &module.manifest.capabilities,
            engine_port,
            engine_pin,
        ) {
            log.push(format!("[auto-config] {}: {}", name, msg));
        }
    }

    log
}

/// Ensure `name` is registered in `modules.json`. A new module is registered
/// with `auto_auth: false` (its first connect still prompts the operator, as
/// before); an existing entry is preserved untouched — approvals granted in a
/// prior session survive.
fn register_module(
    config_state: &Arc<Mutex<ConfigState>>,
    name: &str,
    capabilities: &str,
) -> Option<String> {
    let path = config::modules_path(config_state);
    let mut registry: Vec<serde_json::Value> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_default();

    if registry
        .iter()
        .any(|e| e.get("name").and_then(|v| v.as_str()) == Some(name))
    {
        return None; // already registered — leave it alone
    }

    registry.push(serde_json::json!({
        "name": name,
        "instance_uuid7": Uuid::now_v7().to_string(),
        "position": capabilities,
        "priority": 100,
        "auto_auth": false,
        "auth_token": "",
    }));

    match serde_json::to_string_pretty(&registry) {
        Ok(pretty) => {
            let _ = config::write_atomic(&path, &pretty);
            Some("registered in modules.json (auto_auth: false — first connect will prompt)".to_string())
        }
        Err(e) => Some(format!("failed to register in modules.json: {}", e)),
    }
}

/// Ensure `name` is present in one of the engine `config.json` ordering lists.
/// A module the operator already placed (in any list) keeps its placement — only
/// priority is refreshed; a never-placed module is added to its manifest stage.
fn add_to_ordering(
    config_state: &Arc<Mutex<ConfigState>>,
    name: &str,
    capabilities: &str,
) -> Option<String> {
    let path = {
        let state = config_state.lock().unwrap();
        state.path.clone()
    };
    let mut root: serde_json::Value = std::fs::read_to_string(&path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_else(|| serde_json::json!({}));

    let already_placed = ORDERING_KEYS.iter().any(|key| {
        root.get(*key)
            .and_then(|v| v.as_array())
            .map(|list| list.iter().any(|e| e.get("name").and_then(|v| v.as_str()) == Some(name)))
            .unwrap_or(false)
    });

    if already_placed {
        // Refresh priority where the operator placed it; never move it back.
        for key in ORDERING_KEYS {
            if let Some(existing) = root
                .get_mut(key)
                .and_then(|v| v.as_array_mut())
                .and_then(|list| {
                    list.iter_mut()
                        .find(|e| e.get("name").and_then(|v| v.as_str()) == Some(name))
                })
            {
                existing["priority"] = serde_json::json!(100);
            }
        }
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = config::write_atomic(&path, &pretty);
        }
        return Some("already placed in pipeline ordering — priority refreshed".to_string());
    }

    let key = config_list_key(capabilities);
    if root.get(key).is_none() {
        root[key] = serde_json::json!([]);
    }
    if let Some(list) = root[key].as_array_mut() {
        list.push(serde_json::json!({ "name": name, "priority": 100 }));
    }

    match serde_json::to_string_pretty(&root) {
        Ok(pretty) => {
            let _ = config::write_atomic(&path, &pretty);
            Some(format!("added to {} ordering", key))
        }
        Err(e) => Some(format!("failed to add to {} ordering: {}", key, e)),
    }
}

/// Ensure the module's own `config.json` has connection fields pointing at this
/// engine. Only missing keys are filled in — an existing config (with
/// `module_specific` credentials, prior values, etc.) is preserved wholesale.
fn write_connection_config(
    module_dir: &std::path::Path,
    name: &str,
    capabilities: &str,
    engine_port: u16,
    engine_pin: u32,
) -> Option<String> {
    let path = module_dir.join("config.json");

    let mut root: serde_json::Value = match std::fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };

    let mut changed = false;
    let defaults: HashMap<&str, serde_json::Value> = HashMap::from([
        ("ip", serde_json::json!(DEFAULT_BIND_IP)),
        ("port", serde_json::json!(engine_port)),
        ("pin", serde_json::json!(engine_pin)),
        ("module_name", serde_json::json!(name)),
        ("position", serde_json::json!(capabilities)),
        ("priority", serde_json::json!(100)),
    ]);

    if let Some(obj) = root.as_object_mut() {
        for (key, value) in &defaults {
            if !obj.contains_key(*key) {
                obj.insert((*key).to_string(), value.clone());
                changed = true;
            }
        }
    }

    if !changed {
        return None; // already configured — nothing to do
    }

    match serde_json::to_string_pretty(&root) {
        Ok(pretty) => {
            let _ = config::write_atomic(&path, &pretty);
            Some("wrote connection config.json (missing fields only)".to_string())
        }
        Err(e) => Some(format!("failed to write connection config.json: {}", e)),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_map_to_ordering_keys() {
        assert_eq!(config_list_key("input"), "inputs");
        assert_eq!(config_list_key("inputs"), "inputs");
        assert_eq!(config_list_key("connection"), "inputs");
        assert_eq!(config_list_key("preprocess"), "preprocessModules");
        assert_eq!(config_list_key("inprocess"), "inprocessModules");
        assert_eq!(config_list_key("postprocess"), "postprocessModules");
        assert_eq!(config_list_key("output"), "postprocessModules");
        assert_eq!(config_list_key("display"), "postprocessModules");
        assert_eq!(config_list_key("something-else"), "preprocessModules");
    }

    #[test]
    fn write_connection_config_fills_missing_fields_only() {
        let dir = std::env::temp_dir().join(format!("ckt-acfg-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        // A pre-existing config with a custom port and module_specific block.
        std::fs::write(
            dir.join("config.json"),
            r#"{"ip":"10.0.0.5","port":9999,"module_name":"m","module_specific":{"k":"v"}}"#,
        )
        .unwrap();

        let msg = write_connection_config(&dir, "m", "input", 9734, 987400);
        assert!(msg.is_some(), "missing pin/position/priority should be filled");

        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("config.json")).unwrap()).unwrap();
        // Existing values are preserved.
        assert_eq!(root["ip"], "10.0.0.5");
        assert_eq!(root["port"], 9999);
        assert_eq!(root["module_specific"]["k"], "v");
        // Missing values are filled in.
        assert_eq!(root["pin"], 987400);
        assert_eq!(root["position"], "input");
        assert_eq!(root["priority"], 100);
        assert_eq!(root["module_name"], "m");

        // A second run is a no-op (nothing left to fill).
        let msg2 = write_connection_config(&dir, "m", "input", 9734, 987400);
        assert!(msg2.is_none(), "second run must not rewrite");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_connection_config_creates_missing_file() {
        let dir = std::env::temp_dir().join(format!("ckt-acfg-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();

        let msg = write_connection_config(&dir, "brand-new", "postprocess", 9734, 1234);
        assert!(msg.is_some(), "a missing config.json should be created");

        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("config.json")).unwrap()).unwrap();
        assert_eq!(root["ip"], DEFAULT_BIND_IP);
        assert_eq!(root["port"], 9734);
        assert_eq!(root["pin"], 1234);
        assert_eq!(root["module_name"], "brand-new");
        assert_eq!(root["position"], "postprocess");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
