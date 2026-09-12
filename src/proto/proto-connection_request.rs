use crate::cockatiel_protobuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

// Adjust these imports to match your project's module paths
use crate::{
    Container, ModuleInfo, ModuleState, Payload, add_module_to_config, get_config, log_event,
    refresh_ui_modules,
};

pub async fn handle<W>(
    request: cockatiel_protobuf::ConnectionRequest, // Adjust type if needed
    container: &Container,
    config_state: &impl std::any::Any, // Replace with your actual ConfigState type
    ui_state: &impl std::any::Any,     // Replace with your actual UiState type
    modules: &Arc<Mutex<std::collections::HashMap<String, ModuleInfo>>>,
    websocket: &mut W,
    tx: &Sender<()>, // Adjust sender type if needed
    module_name_out: &mut String,
    instance_uuid7_out: &mut String,
    authenticated_out: &mut bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: futures_util::SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let config = get_config(config_state);

    if request.pin != config.paring_pin as i32 {
        log_event(
            ui_state,
            format!("Rejected connection from {}", container.module_name),
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
        websocket.send(Message::Binary(bytes.into())).await?;
        return Ok(()); // Replaces the `break` or handles early exit
    }

    *module_name_out = container.module_name.clone();

    let requested_id = request.module_instance_uuid7.clone();

    let assigned_id = {
        let mods = modules.lock().unwrap();

        if requested_id.is_empty() || mods.contains_key(&requested_id) {
            loop {
                let id = Uuid::now_v7().to_string();
                if !mods.contains_key(&id) {
                    break id;
                }
            }
        } else {
            requested_id.clone()
        }
    };

    *instance_uuid7_out = assigned_id.clone();
    *authenticated_out = true;

    let position = match request.process_position.to_lowercase().as_str() {
        "input" | "inputs" => "input",
        "preprocess" => "preprocess",
        "inprocess" => "inprocess",
        "postprocess" | "output" | "outputs" | "display" | "post" => "postprocess",
        _ => "input",
    }
    .to_string();

    add_module_to_config(config_state, module_name_out, &position, request.priority);

    {
        let mut mods = modules.lock().unwrap();
        mods.insert(
            assigned_id.clone(),
            ModuleInfo {
                name: module_name_out.clone(),
                instance_uuid7: assigned_id.clone(),
                priority: request.priority,
                process_position: position.clone(),
                state: ModuleState::Running,
                sender: Some(tx.clone()),
            },
        );
    }

    refresh_ui_modules(ui_state, modules);

    log_event(
        ui_state,
        format!("{} [{}] connected", module_name_out, assigned_id),
    );

    let response = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "cockatiel".into(),
        module_instance_uuid7: assigned_id.clone(),
        payload: Some(Payload::ConnectionRequestReturn(
            cockatiel_protobuf::ConnectionRequestReturn {
                new_port: 0,
                module_instance_uuid7: if assigned_id != requested_id {
                    assigned_id.clone()
                } else {
                    String::new()
                },
            },
        )),
    };

    let mut bytes = Vec::new();
    response.encode(&mut bytes)?;

    websocket.send(Message::Binary(bytes.into())).await?;

    Ok(())
}
