use crate::EngineState;
use crate::cockatiel_protobuf;
use crate::log_event;
use std::sync::{Arc, Mutex};

pub fn handle(
    payload: cockatiel_protobuf::AuthNew,
    authenticated: bool,
    module_name: &str,
    log: &cockatiel_protobuf::Log,
    ui_state: &Arc<Mutex<EngineState>>,
) {
    if authenticated {
        log_event(ui_state, format!("[{}] {}", module_name, log.log));
    }

    Ok(())
}
