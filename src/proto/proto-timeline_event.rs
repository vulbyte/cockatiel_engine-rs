use crate::cockatiel_protobuf;
use crate::{Container, ModuleInfo, broadcast_stage, log_event};
use std::sync::{Arc, Mutex};

pub async fn handle(
    authenticated: bool,
    event: &cockatiel_protobuf::TimelineEvent,
    container: &Container,
    modules: &Arc<Mutex<std::collections::HashMap<String, ModuleInfo>>>,
    config_state: &impl std::any::Any,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if !authenticated {
        return Ok(());
    }

    log_event(ui_state, format!("Timeline: {}", event.i));

    // Timeline database is a normal module.
    broadcast_stage(modules, config_state, "postprocess", container).await;

    Ok(())
}
