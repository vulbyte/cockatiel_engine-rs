use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    module_name: &str,
    log: &cockatiel_protobuf::Log,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if authenticated {
        log_event(ui_state, format!("[{}] {}", module_name, log.log));
    }

    Ok(())
}
