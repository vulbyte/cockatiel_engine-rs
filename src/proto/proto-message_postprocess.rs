use crate::cockatiel_protobuf;
use crate::{Container, ModuleInfo, broadcast_stage, log_event};
use std::sync::{Arc, Mutex};

pub async fn handle(
    authenticated: bool,
    message: &cockatiel_protobuf::MessagePostProcess,
    container: &Container,
    modules: &Arc<Mutex<std::collections::HashMap<String, ModuleInfo>>>,
    config_state: &impl std::any::Any,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(core) = &message.core_message {
        let platform = &core.platform;
        // do something with platform
    }

    broadcast_stage(modules, config_state, "postprocess", container).await;

    log_event(
        ui_state,
        format!("Message finalized from {}", message.user_uuid7),
    );

    Ok(())
}
