use crate::cockatiel_protobuf;
use crate::{
    Container, ModuleInfo, Payload, broadcast_stage, get_config, pipeline_list,
    send_to_instance_by_name,
};
use std::sync::{Arc, Mutex};

pub async fn handle(
    message: &cockatiel_protobuf::MessagePreProcess,
    container: &Container,
    instance_uuid7: &String,
    modules: &Arc<Mutex<std::collections::HashMap<String, ModuleInfo>>>,
    config_state: &impl std::any::Any, // Replace with your actual ConfigState type if needed
) -> Result<(), Box<dyn std::error::Error>> {
    let mut next = container.clone();
    next.module_instance_uuid7 = instance_uuid7.clone();

    broadcast_stage(modules, config_state, "preprocess", &next).await;

    let config = get_config(config_state);

    let next_modules = pipeline_list(&config, "inprocess");

    if let Some(module) = next_modules.first() {
        send_to_instance_by_name(modules, &module.name, next).await;
    } else {
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

        broadcast_stage(modules, config_state, "postprocess", &final_container).await;
    }

    Ok(())
}
