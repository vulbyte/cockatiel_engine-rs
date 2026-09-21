pub struct ModuleAuthInfo {
    pub name: String,
    pub instance_uuid7: String,
    pub position: String,
    pub priority: u32,
}

pub async fn prompt_user_to_auth_module(info: &ModuleAuthInfo) -> bool {
    let name = info.name.clone();
    let uuid = info.instance_uuid7.clone();
    let position = info.position.clone();
    let priority = info.priority;

    let result = tokio::task::spawn_blocking(move || {
        println!(
            "\n[Cockatiel Auth] New module requesting connection:\n  Name:     {}\n  UUID:     {}\n  Position: {}\n  Priority: {}\n\n  Add module? (y/n): ",
            name, uuid, position, priority
        );

        let mut input = String::new();
        match std::io::stdin().read_line(&mut input) {
            Ok(_) => {
                let trimmed = input.trim().to_lowercase();
                trimmed == "y" || trimmed == "yes"
            }
            Err(_) => false,
        }
    })
    .await
    .unwrap_or(false);

    result
}
