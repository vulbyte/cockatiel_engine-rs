use crate::ModuleEntry;
use crate::log_event;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::PathBuf,
    sync::{Arc, Mutex},
}; // wherever that struct actually lives

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub database_location: String,
    pub backup_database_location: String,
    pub paring_pin: u32,
    pub port: u16,

    #[serde(default)]
    pub inputs: Vec<ModuleEntry>,

    #[serde(default, rename = "preprocessModules")]
    pub preprocess_modules: Vec<ModuleEntry>,

    #[serde(default, rename = "inprocessModules")]
    pub inprocess_modules: Vec<ModuleEntry>,

    #[serde(default, rename = "postprocessModules")]
    pub postprocess_modules: Vec<ModuleEntry>,
}

pub struct ConfigState {
    pub path: PathBuf,
    pub last_size: u64,
    pub config: Config,
}

fn create(directory: PathBuf) -> Result<String, String> {
    let target = directory.join("config.json");

    let pin: u32 = 100000 + (Uuid::new_v4().as_u128() % 900000) as u32;

    let config = format!(
        r#"{{
    "database_location": "./cockatiel_data.db",
    "backup_database_location": "./cockatiel_backup.db",
    "paring_pin": {},
    "port": 9734,
    "inputs": [],
    "preprocessModules": [],
    "inprocessModules": [],
    "postprocessModules": []
}}"#,
        pin
    );

    fs::write(&target, &config).map_err(|e| e.to_string())?;

    Ok(config)
}

fn get(state: &Arc<Mutex<ConfigState>>) -> Config {
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

async fn verify() -> Result<(String, PathBuf), Box<dyn std::error::Error>> {
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

fn update<F>(state: &Arc<Mutex<ConfigState>>, update: F)
where
    F: FnOnce(&mut Config),
{
    let mut state = state.lock().unwrap();

    update(&mut state.config);

    if let Ok(serialized) = serde_json::to_string_pretty(&state.config) {
        if fs::write(&state.path, serialized).is_ok() {
            if let Ok(metadata) = fs::metadata(&state.path) {
                state.last_size = metadata.len();
            }
        }
    }
}

fn add_module_to_config(
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
