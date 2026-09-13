#![allow(clippy::type_complexity)]

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message as WsMessage};
use uuid::Uuid;

/* MODULES & CONFIG */
#[path = "./module_manager.rs"]
mod module_manager;
use module_manager::ModuleRegistry;

#[path = "./config.rs"]
mod config_manager;
use config_manager::{Config, ConfigState, get_config, verify_config};

/* PROTOBUF STUFF */
pub mod cockatiel_protobuf {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_protobuf.v1.rs"));
}

use cockatiel_protobuf::{Container, container::Payload};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ModuleEntry {
    pub name: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModuleState {
    Running,
    Paused,
    Stopped,
    Crashed,
}

#[derive(Clone)]
pub struct ModuleInfo {
    pub name: String,
    pub instance_uuid7: String,
    pub priority: i32,
    pub process_position: String,
    pub state: ModuleState,
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
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            modules: Vec::new(),
            timeline: Vec::new(),
        }
    }
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
    }

    paths.sort();
    paths.dedup();

    paths
}

pub async fn broadcast_stage(
    modules: &Arc<Mutex<HashMap<String, ModuleInfo>>>,
    position: &str,
    container: &Container,
    config_state: &Arc<Mutex<ConfigState>>,
) {
    let config = get_config(config_state);
    let mut entries = match position {
        "connections" => config.inputs.clone(),
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
            let _ = sender.send(container.clone()).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        r#"
                    X
                XXXXXXXXXXXX  XXX
             XXXXXXXXXXXXXXXX
            XXX    XXXXXXXXXXX
         XXXXX     XXXXXXXXXXXXXX
        XXXXXXX    XXXXXXXXXXX
         XXXXXXXXXXXXXXXXXX
            XXXXXXXXXXXXXXX
            XXX XXXXXXX XXX
            XX    XXXX    XX
            cockatiel
               -by vulbyte
"#
    );

    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let modules: Arc<Mutex<HashMap<String, ModuleInfo>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut module_registry = ModuleRegistry::new();
    let search_paths = module_search_paths();
    log_event(&ui_state, "Searching for Cockatiel modules...");

    match module_registry.discover(&search_paths) {
        Ok(()) => {}
        Err(errors) => {
            for error in errors {
                log_event(&ui_state, format!("Module discovery: {}", error));
            }
        }
    }

    let (config_string, config_path) = verify_config().await?;
    let config: Config = serde_json::from_str(&config_string)?;
    let config_size = fs::metadata(&config_path)?.len();
    let config_state = Arc::new(Mutex::new(ConfigState {
        path: config_path,
        last_size: config_size,
        config: config.clone(),
    }));

    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;
    log_event(&ui_state, format!("Listening on port {}", config.port));

    loop {
        let (stream, address) = listener.accept().await?;
        log_event(&ui_state, format!("Connection from {}", address));

        let config_state = Arc::clone(&config_state);
        let modules = Arc::clone(&modules);
        let ui_state = Arc::clone(&ui_state);

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, config_state, modules, ui_state).await {
                eprintln!("Connection error: {}", error);
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    config_state: Arc<Mutex<ConfigState>>,
    modules: Arc<Mutex<HashMap<String, ModuleInfo>>>,
    ui_state: Arc<Mutex<EngineState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut websocket = accept_async(stream).await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Container>(64);
    let mut _authenticated = false;
    let mut module_name = String::new();
    let mut instance_uuid7 = String::new();

    loop {
        tokio::select! {
            incoming = websocket.next() => {
                let Some(incoming) = incoming else {
                    break;
                };

                let message = incoming?;
                let WsMessage::Binary(data) = message else {
                    continue;
                };

                let container = Container::decode(data.as_ref())?;

                log_event(&ui_state, format!("Received container from module: {}", container.module_name));

                match container.payload {
                    Some(Payload::ConnectionRequest(_)) => {
                        _authenticated = true;
                        module_name = container.module_name.clone();
                        instance_uuid7 = container.module_instance_uuid7.clone();
                        log_event(&ui_state, format!("Module connected: {} ({})", module_name, instance_uuid7));
                    }
                    Some(Payload::MessagePreProcess(_)) => {
                        broadcast_stage(&modules, "preprocess", &container, &config_state).await;
                    }
                    Some(Payload::MessageInProcess(_)) => {
                        broadcast_stage(&modules, "inprocess", &container, &config_state).await;
                    }
                    Some(Payload::MessagePostProcess(_)) => {
                        broadcast_stage(&modules, "postprocess", &container, &config_state).await;
                    }
                    Some(Payload::TimelineEvent(ref ev)) => {
                        log_event(&ui_state, format!("Timeline event: {:?}", ev));
                    }
                    Some(Payload::Log(ref lg)) => {
                        log_event(&ui_state, format!("[{}] {}", module_name, lg.log));
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

                if websocket.send(WsMessage::Binary(bytes.into())).await.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}
