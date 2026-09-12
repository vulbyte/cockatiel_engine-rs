use crate::cockatiel_protobuf;
use crate::{Container, Payload, get_config, pipeline_list};

pub fn handle(
    authenticated: bool,
    module_name: &str,
    container: &Container,
    message: &cockatiel_protobuf::MessageInProcess, // Adjust message type if needed
    config_state: &impl std::any::Any,
) -> Result<(usize, Container), &'static str> {
    if !authenticated {
        return Err("Not authenticated");
    }

    let config = get_config(config_state);
    let modules_in_process = pipeline_list(&config, "inprocess");
    let mut next_index = 0;

    for (index, entry) in modules_in_process.iter().enumerate() {
        if entry.name == module_name {
            next_index = index + 1;
            break;
        }
    }

    let mut final_container = container.clone();

    final_container.payload = Some(Payload::MessagePostProcess(
        cockatiel_protobuf::MessagePostProcess {
            platform: message.platform.clone(),
            raw_data: message.raw_data.clone(),
            user_uuid7: message.user_uuid7.clone(),
            raw_message: message.raw_message.clone(),
            processed_message: String::new(),
            command: message.command.clone(),
            user_data: message.user_data.clone(),
        },
    ));

    Ok((next_index, final_container))
}
