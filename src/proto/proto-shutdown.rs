use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    module_name: &str,
    shutdown: &cockatiel_protobuf::Shutdown,
    ui_state: &impl std::any::Any,
) -> Result<bool, Box<dyn std::error::Error>> {
    if authenticated {
        log_event(
            ui_state,
            format!("{} shut down: {}", module_name, shutdown.reason),
        );
    }

    Ok(true) // Returns true to trigger the `break` in your main loop
}
