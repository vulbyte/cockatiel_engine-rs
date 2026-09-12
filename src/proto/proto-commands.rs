use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    module_name: &str,
    commands: &cockatiel_protobuf::Commands,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if authenticated {
        log_event(
            ui_state,
            format!(
                "{} registered {} commands",
                module_name,
                commands.commands.len()
            ),
        );
    }

    Ok(())
}
