use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    module_name: &str,
    error: &cockatiel_protobuf::Err,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if authenticated {
        log_event(ui_state, format!("[{}] ERROR: {}", module_name, error.log));
    }

    Ok(())
}
