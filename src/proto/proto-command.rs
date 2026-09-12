use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    module_name: &str,
    command: &cockatiel_protobuf::Command,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if authenticated {
        log_event(
            ui_state,
            format!(
                "{} registered {} commands",
                module_name,
                command.command_flags.len()
            ),
        );
    }

    Ok(())
}
