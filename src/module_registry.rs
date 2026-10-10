use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::config::ConfigState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModule {
    pub name: String,
    // Every field below is `default`: a hand-written or CI-seeded `modules.json`
    // entry may carry only `name` + `auto_auth` (name-only auto-approval), and a
    // missing field must not make the WHOLE registry fail to parse (which would
    // silently drop every approval). `instance_uuid7`/`auth_token` default empty;
    // the pinned-identity reconnect path already treats empty as "no pin".
    #[serde(default)]
    pub instance_uuid7: String,
    #[serde(default)]
    pub position: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub auto_auth: bool,
    #[serde(default)]
    pub auth_token: String,
}

#[derive(Clone)]
pub struct ModuleRegistryPersistence {
    path: PathBuf,
    modules: Arc<Mutex<Vec<RegisteredModule>>>,
}

impl ModuleRegistryPersistence {
    pub fn load(config_state: &Arc<Mutex<ConfigState>>) -> Self {
        let path = super::config::modules_path(config_state);
        let modules = if let Ok(content) = fs::read_to_string(&path) {
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            Vec::new()
        };

        Self {
            path,
            modules: Arc::new(Mutex::new(modules)),
        }
    }

    pub fn save(&self) -> Result<(), String> {
        let modules = self.modules.lock().unwrap();
        let content = serde_json::to_string_pretty(&*modules).map_err(|e| e.to_string())?;
        crate::config::write_atomic(&self.path, &content).map_err(|e| e.to_string())
    }

    pub fn register(&self, module: RegisteredModule) {
        let mut modules = self.modules.lock().unwrap();
        if let Some(existing) = modules.iter_mut().find(|m| m.name == module.name) {
            *existing = module;
        } else {
            modules.push(module);
        }
        drop(modules);
        let _ = self.save();
    }

    /// Re-read modules.json from disk, replacing the in-memory list. The TUI
    /// registers modules at runtime (e.g. a duplicated module), so a connection
    /// from a name the engine hasn't seen since startup must pick it up.
    pub fn refresh(&self) {
        if let Ok(content) = fs::read_to_string(&self.path) {
            if let Ok(modules) = serde_json::from_str::<Vec<RegisteredModule>>(&content) {
                let mut guard = self.modules.lock().unwrap();
                *guard = modules;
            }
        }
    }

    /// Look up a registered module by name. Returns None when the name has no
    /// registry entry (e.g. a fresh install bootstrap).
    pub fn find(&self, name: &str) -> Option<RegisteredModule> {
        let modules = self.modules.lock().unwrap();
        modules.iter().find(|m| m.name == name).cloned()
    }

    /// True only when an entry for `name` EXISTS and is marked auto_auth.
    /// This is name-only: the client SDK connects first-contact with a fresh
    /// `Uuid::now_v7()` on EVERY reconnect and never replays a stored identity,
    /// so requiring the pinned uuid here would break every module reconnect.
    /// Control-surface names are pinned by the caller instead.
    pub fn is_known_and_auto_auth(&self, name: &str) -> bool {
        let modules = self.modules.lock().unwrap();
        modules
            .iter()
            .find(|m| m.name == name)
            .map(|m| m.auto_auth)
            .unwrap_or(false)
    }

    pub fn values(&self) -> Vec<RegisteredModule> {
        let modules = self.modules.lock().unwrap();
        modules.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partial_registry_entry_still_parses_and_auto_auths() {
        // CI/hand-written seeds carry only name + auto_auth; a missing
        // instance_uuid7/auth_token must not drop the whole registry.
        let entries: Vec<RegisteredModule> =
            serde_json::from_str(r#"[{"name":"banned-words","auto_auth":true}]"#)
                .expect("partial entry must parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "banned-words");
        assert!(entries[0].auto_auth);
        assert!(entries[0].instance_uuid7.is_empty());
        assert_eq!(entries[0].priority, 0);
    }

    #[test]
    fn registered_module_round_trips_through_save_and_load() {
        let dir = std::env::temp_dir().join(format!("ckt-reg-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("modules.json");

        let persistence = ModuleRegistryPersistence {
            path: path.clone(),
            modules: Arc::new(Mutex::new(vec![RegisteredModule {
                name: "x".to_string(),
                instance_uuid7: "u".to_string(),
                position: "inprocess".to_string(),
                priority: 7,
                auto_auth: true,
                auth_token: "tok".to_string(),
            }])),
        };
        persistence.save().expect("save succeeds");

        let reloaded: Vec<RegisteredModule> =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].name, "x");
        assert_eq!(reloaded[0].position, "inprocess");
        assert_eq!(reloaded[0].priority, 7);
        assert!(reloaded[0].auto_auth);
        assert_eq!(reloaded[0].auth_token, "tok");

        let _ = fs::remove_dir_all(&dir);
    }
}
