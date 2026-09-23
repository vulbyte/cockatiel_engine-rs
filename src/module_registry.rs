use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::config::ConfigState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModule {
    pub name: String,
    pub instance_uuid7: String,
    pub position: String,
    pub priority: i32,
    pub auto_auth: bool,
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
