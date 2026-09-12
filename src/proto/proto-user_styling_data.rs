use crate::cockatiel_protobuf;
use crate::log_event;

pub fn handle(
    authenticated: bool,
    user: &cockatiel_protobuf::UserData,
    ui_state: &impl std::any::Any,
) -> Result<(), Box<dyn std::error::Error>> {
    if authenticated {
        log_event(
            ui_state,
            format!("User styling data update: {}", user.username),
        );
    }

    Ok(())
}
