#![allow(clippy::type_complexity)]

use crate::Payload;
use cockatiel_protobuf::{Container, container::Payload};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    process,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

use uuid::Uuid;

#[path = "./module_manager.rs"]
use module_registry;

/*keymap*/
#[path = "./keymap.rs"]
mod keymap;
#[path = "./module_manager.rs"]
mod module_manager;
#[path = "../modules/tui/src/main.rs"]
mod tui;
/* CONFIG */
#[path = "./config.rs"]
mod config_manager;
/* PROTOBUF STUFF*/
#[path = "./proto/proto-auth_new.rs"]
mod auth_new;
#[path = "./proto/proto-auth_verify.rs"]
mod auth_verify;
#[path = "./proto/proto-ban.rs"]
mod ban;
#[path = "./proto/proto-command.rs"]
mod command;
#[path = "./proto/proto-commands.rs"]
mod commands;
#[path = "./proto/proto-commendment.rs"]
mod commendment;
#[path = "./proto/proto-connection_request.rs"]
mod connection_request;
#[path = "./proto/proto-connection_request_return.rs"]
mod connection_request_return;
#[path = "./proto/proto-err.rs"]
mod err;
#[path = "./proto/proto-flag.rs"]
mod flag;
#[path = "./proto/proto-log.rs"]
mod log;
#[path = "./proto/proto-message_inprocess.rs"]
mod message_inprocess;
#[path = "./proto/proto-message_postprocess.rs"]
mod message_postprocess;
#[path = "./proto/proto-message_preprocess.rs"]
mod message_preprocess;
#[path = "./proto/proto-send_to_platforms.rs"]
mod send_to_platfroms;
#[path = "./proto/proto-shutdown.rs"]
mod shutdown;
#[path = "./proto/proto-timeline_event.rs"]
mod timeline_event;
#[path = "./proto/proto-user_data.rs"]
mod user_data;
#[path = "./proto/proto-user_styling_data.rs"]
mod user_styling_template;

pub mod cockatiel_protobuf {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_protobuf.rs"));
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

    pub sender: Option<mpsc::Sender<Container>>,
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
#[derive(Debug)]
pub enum EngineCommand {
    OpenModuleActions(String),
    TogglePause(String),
    Restart(String),
    Shutdown(String),
    InspectTimeline(String),
    Quit,
}

fn log_event(state: &Arc<Mutex<EngineState>>, text: impl Into<String>) {
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

fn get_file(path: &str) -> Result<String, String> {
    let path = PathBuf::from(path);

    if !path.exists() {
        return Err("file does not exist".into());
    }

    if path.is_dir() {
        return Err("path is a directory".into());
    }

    fs::read_to_string(path).map_err(|e| e.to_string())
}

fn pipeline_list(config: &Config, position: &str) -> Vec<ModuleEntry> {
    let mut list = match position {
        "connections" => config.inputs.clone(),
        "preprocess" => config.preprocess_modules.clone(),
        "inprocess" => config.inprocess_modules.clone(),
        "postprocess" => config.postprocess_modules.clone(),
        _ => Vec::new(),
    };

    list.sort_by_key(|entry| entry.priority);

    list
}

async fn send_to_instance(
    modules: &Arc<Mutex<HashMap<String, ModuleInfo>>>,
    instance: &str,
    container: Container,
) {
    let sender = {
        modules
            .lock()
            .unwrap()
            .get(instance)
            .and_then(|m| m.sender.clone())
    };

    if let Some(sender) = sender {
        let _ = sender.send(container).await;
    }
}

fn refresh_ui_modules(
    ui_state: &Arc<Mutex<EngineState>>,
    modules: &Arc<Mutex<HashMap<String, ModuleInfo>>>,
) {
    let mut list = modules
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    list.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.name.cmp(&b.name)));

    ui_state.lock().unwrap().modules = list;
}

fn module_action(
    command: EngineCommand,
    modules: &Arc<Mutex<HashMap<String, ModuleInfo>>>,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    match command {
        EngineCommand::TogglePause(instance) => {
            let mut modules = modules.lock().unwrap();

            if let Some(module) = modules.get_mut(&instance) {
                module.state = if module.state == ModuleState::Paused {
                    ModuleState::Running
                } else {
                    ModuleState::Paused
                };

                log_event(
                    ui_state,
                    format!(
                        "{} [{}] is now {:?}",
                        module.name, module.instance_uuid7, module.state
                    ),
                );
            }
        }

        EngineCommand::Shutdown(instance) => {
            let module = { modules.lock().unwrap().get(&instance).cloned() };

            if let Some(module) = module {
                if let Some(sender) = module.sender {
                    let container = Container {
                        version: 1,
                        auth_token: String::new(),
                        module_name: "cockatiel".into(),
                        module_instance_uuid7: instance.clone(),
                        payload: Some(Payload::Shutdown(cockatiel_protobuf::Shutdown {
                            reason: "Shutdown requested by operator".into(),
                        })),
                    };

                    let _ = sender.try_send(container);
                }
            }
        }

        EngineCommand::Restart(instance) => {
            log_event(ui_state, format!("Restart requested for {}", instance));
        }

        EngineCommand::InspectTimeline(id) => {
            log_event(ui_state, format!("Timeline inspection requested: {}", id));
        }

        EngineCommand::OpenModuleActions(instance) => {
            log_event(ui_state, format!("Selected module {}", instance));
        }

        EngineCommand::Quit => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        r#"
                         X
              XXXXXXXXXXXX  XXX
            XXXXXXXXXXXXXXXXX
           XXX    XXXXXXXXXXX
        XXXXX      XXXXXXXXXXXXXX
       XXXXXXX    XXXXXXXXXXX
        XXXXXXXXXXXXXXXXXX
           XXXXXXXXXXXXXXX
           XXX XXXXXXX XXX
           XX    XXXX    XX
           cockatiel
              -by vulbyte
"#
    );

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

    for module in module_registry.values() {
        log_event(
            &ui_state,
            format!(
                "Found module {} v{} at {}",
                module.manifest.name,
                module.manifest.version,
                module.directory.display()
            ),
        );
    }

    let (config_string, config_path) = verify_config().await?;
    let config: Config = serde_json::from_str(&config_string)?;
    let config_size = fs::metadata(&config_path)?.len();
    let config_state = Arc::new(Mutex::new(ConfigState {
        path: config_path,
        last_size: config_size,
        config: config.clone(),
    }));

    let modules: Arc<Mutex<HashMap<String, ModuleInfo>>> = Arc::new(Mutex::new(HashMap::new()));
    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let (command_tx, command_rx) = std::sync::mpsc::channel::<EngineCommand>();
    let tui_state = Arc::clone(&ui_state);
    let tui_command_tx = command_tx.clone();
    thread::spawn(move || match tui::Tui::new() {
        Ok(mut tui) => {
            if let Err(error) = tui.run(tui_state, tui_command_tx) {
                eprintln!("TUI error: {}", error);
            }
        }
        Err(error) => {
            eprintln!("Could not start TUI: {}", error);
        }
    });

    let command_modules = Arc::clone(&modules);
    let command_ui_state = Arc::clone(&ui_state);
    thread::spawn(move || {
        while let Ok(command) = command_rx.recv() {
            if matches!(command, EngineCommand::Quit) {
                break;
            }

            module_action(command, &command_modules, &command_ui_state);
        }
    });

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
    let (tx, mut rx) = mpsc::channel::<Container>(64);
    let mut authenticated = false;
    let mut module_name = String::new();
    let mut instance_uuid7 = String::new();

    loop {
        tokio::select! {
            incoming = websocket.next() => {
                let Some(incoming) = incoming else {
                    break;
                };

                let message = incoming?;
                let Message::Binary(data) =
                    message
                else {
                    continue;
                };

                let container =
                    Container::decode(
                        data.as_ref()
                    )?;

                match container.payload { // due to logic being deep for some of these actions, they
                                          // have been moved to a seperte file (saved like 300 lines)
                    Some(Payload::AuthNew(auth_msg)) => {auth_new::handle(auth_msg);}
                    Some(Payload::AuthVerify(auth_verf)) => {auth_verify::handle(auth_verf);}
                    Some(Payload::Ban(ban_data)) => {ban::handle(ban_data);}
                    Some(Payload::Commendment(comm_data)) => {commendment::handle(comm_data);}
                    Some(Payload::Command(command)) => {command::handle(request);}
                    Some(Payload::Commands(commands)) => {commnads::handle(request);}
                    Some(Payload::ConnectionRequest(request)) => {connection_request::handle(request);}
                    Some(Payload::ConnectionRequestReturn(ret)) => {connection_request_return::handle(ret);}
                    Some(Payload::Err(data)) => {err::handle(data);}
                    Some(Payload::Flag(flag_data)) => {flag::handle(flag_data);}
                    Some(Payload::Log(log)) => {log::handle(request);}
                    Some(Payload::MessagePreProcess(ref message)) => {message_preprocess::handle(request);}
                    Some(Payload::MessageInProcess(ref message)) => {message_inprocess::handle(request);}
                    Some(Payload::MessagePostProcess(ref message)) => {message_inprocess::handle(request);}
                    Some(Payload::Send(ref send_req)) => {send_to_platforms::handle(send_req);}
                    Some(Payload::Shutdown(ref shutdown)) => {shutdown::handle(request);}
                    Some(Payload::TimelineEvent(ref event)) => {timeline_event::handle(request);}
                    Some(Payload::UserData(ref user)) => {user_data::handle(request);}
                    Some(Payload::UserStylingTemplate(css_data)) => {user_style_template::handle(css_data);}
                    None(ref data) => {println!("unknown error data {}", data);}
                }
            }

            outbound = rx.recv() => {
                let Some(outbound) =
                    outbound
                else {
                    break;
                };

                let mut bytes =
                    Vec::new();

                outbound.encode(
                    &mut bytes
                )?;

                if websocket
                    .send(
                        Message::Binary(
                            bytes.into()
                        )
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    auth.authenicated();

    Ok(());
}
