use std::collections::HashMap;
use std::path::PathBuf;

use crate::module_manager::{CredentialField, DiscoveredModule};

/// Path to a module's `.env` file (its secret store, next to config.json).
fn module_env_path(module: &DiscoveredModule) -> PathBuf {
    module.directory.join(".env")
}

/// Path to a module's settings file (non-secret config).
fn module_config_path(module: &DiscoveredModule) -> PathBuf {
    module.directory.join("config.json")
}

/// Read a KEY=VALUE `.env` file into a map (values that parse as JSON arrays
/// become arrays, so list credentials round-trip through the TUI).
pub fn read_env_map(path: &PathBuf) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Ok(content) = std::fs::read_to_string(path) else {
        return out;
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_string();
            let value = value.trim().trim_matches('"').to_string();
            if !key.is_empty() {
                out.insert(key, value);
            }
        }
    }
    out
}

/// Write a KEY=VALUE `.env` file (merging into any existing file), owner-only
/// via the engine's atomic 0600 writer (unique temp + fsync + rename).
pub fn write_env_map(path: &PathBuf, entries: &[(String, String)]) -> Result<(), String> {
    let mut lines: Vec<String> = std::fs::read_to_string(path)
        .map(|c| c.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    for (key, value) in entries {
        let entry = format!("{}={}", key, value);
        let prefix = format!("{}=", key);
        if let Some(idx) = lines.iter().position(|l| l.trim().starts_with(&prefix)) {
            lines[idx] = entry;
        } else {
            lines.push(entry);
        }
    }
    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    crate::config::write_atomic(path, &content).map_err(|e| e.to_string())
}

/// Current credential values as a map key -> value. Sensitive fields are read
/// from the module's `.env`; non-sensitive (public) settings are read from
/// `config.json`'s `module_specific`. List fields are joined with "\n".
pub fn credential_values_map(module: &DiscoveredModule) -> HashMap<String, String> {
    let fields = &module.manifest.credentials;
    let env_path = module_env_path(module);
    let env_map = read_env_map(&env_path);
    let config_spec = load_config_module_specific(module);

    let mut out: HashMap<String, String> = HashMap::new();
    for field in fields {
        // Public settings live in config.json; secrets live in .env.
        if let Some(v) = config_spec.get(&field.key) {
            out.insert(field.key.clone(), spec_value_to_string(v));
            continue;
        }
        let env_name = field.env_name();
        if let Some(raw) = env_map.get(env_name).or_else(|| env_map.get(&field.key)) {
            if field.list {
                if let Ok(items) = serde_json::from_str::<Vec<String>>(raw) {
                    out.insert(field.key.clone(), items.join("\n"));
                    continue;
                }
            }
            out.insert(field.key.clone(), raw.clone());
        }
    }

    // Legacy: before the `.env` migration, credentials lived in config.json's
    // `module_specific` section. Only fall back when nothing was found.
    if out.is_empty() && !env_path.exists() {
        load_legacy_credentials(module, fields, &mut out);
    }
    out
}

/// Read the `module_specific` object of a module's config.json (empty if none).
fn load_config_module_specific(module: &DiscoveredModule) -> serde_json::Map<String, serde_json::Value> {
    let path = module_config_path(module);
    let Ok(data) = std::fs::read_to_string(&path) else {
        return serde_json::Map::new();
    };
    let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&data) else {
        return serde_json::Map::new();
    };
    json_val
        .get("module_specific")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default()
}

/// A module_specific value as an editable string (lists joined with "\n").
fn spec_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|i| i.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Read the legacy `module_specific` section of a module's config.json into
/// `out` (list fields joined by "\n").
fn load_legacy_credentials(
    module: &DiscoveredModule,
    fields: &[CredentialField],
    out: &mut HashMap<String, String>,
) {
    let path = module_config_path(module);
    let Ok(data) = std::fs::read_to_string(&path) else { return };
    let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&data) else {
        return;
    };
    let Some(obj) = json_val.get("module_specific").and_then(|v| v.as_object()) else {
        return;
    };
    for (key, value) in obj {
        match value {
            serde_json::Value::Array(items) => {
                let joined = items
                    .iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<&str>>()
                    .join("\n");
                out.insert(key.clone(), joined);
            }
            serde_json::Value::String(s) => {
                out.insert(key.clone(), s.clone());
            }
            other => {
                out.insert(key.clone(), other.to_string());
            }
        }
    }
    let _ = fields;
}

/// Whether every REQUIRED credential in the schema has a non-empty value in
/// the given map. Optional fields (e.g. oauth_token, which adapters acquire
/// automatically) do not block completion.
pub fn is_config_complete(
    fields: &[CredentialField],
    values: &HashMap<String, String>,
) -> bool {
    fields
        .iter()
        .filter(|f| !f.optional)
        .all(|f| {
            values
                .get(&f.key)
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
        })
}

/// Validate the submitted fields against the module's credential schema.
/// Returns an error message listing any unknown keys.
pub fn validate_credential_fields(
    fields: &[CredentialField],
    submitted: &HashMap<String, String>,
) -> Result<(), String> {
    let valid_keys: Vec<&str> = fields.iter().map(|f| f.key.as_str()).collect();
    for key in submitted.keys() {
        if !valid_keys.contains(&key.as_str()) {
            return Err(format!("Unknown credential key: {}", key));
        }
    }
    Ok(())
}

/// Write credentials for a module. **Sensitive** fields go to `.env` (the
/// secret store); **non-sensitive / public** settings (e.g. a Discord guild ID
/// or channel list, which anyone in the server can see) go into `config.json`'s
/// `module_specific` section. List fields are stored as JSON arrays.
pub fn save_module_credentials(
    module: &DiscoveredModule,
    fields: &[CredentialField],
    values: &HashMap<String, String>,
) -> Result<(), String> {
    let mut env_entries: Vec<(String, String)> = Vec::new();
    let mut spec = serde_json::Map::new();
    for field in fields {
        let Some(value) = values.get(&field.key) else { continue };
        if field.sensitive {
            let encoded = if field.list {
                let items: Vec<String> = value
                    .split('\n')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| value.clone())
            } else {
                value.trim().to_string()
            };
            env_entries.push((field.env_name().to_string(), encoded));
        } else if field.list {
            let items: Vec<String> = value
                .split('\n')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            spec.insert(
                field.key.clone(),
                serde_json::Value::Array(items.into_iter().map(serde_json::Value::String).collect()),
            );
        } else {
            spec.insert(field.key.clone(), serde_json::Value::String(value.trim().to_string()));
        }
    }

    if !env_entries.is_empty() {
        write_env_map(&module_env_path(module), &env_entries)?;
    }

    // Merge public settings into config.json's `module_specific`.
    if !spec.is_empty() {
        let path = module_config_path(module);
        let mut root: serde_json::Value = std::fs::read_to_string(&path)
            .ok()
            .and_then(|d| serde_json::from_str(&d).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        if root.as_object().is_none() {
            root = serde_json::json!({});
        }
        // MERGE, never replace. `module_specific` also holds each module's
        // tuning knobs (timeouts, backoffs, queue sizes) which are not
        // credentials, so overwriting the whole object with just the
        // credential fields silently wiped every one of them on each save.
        // Existing keys win only where the credential set doesn't mention them.
        let mut merged: serde_json::Map<String, serde_json::Value> = root
            .get("module_specific")
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        for (key, value) in spec {
            merged.insert(key, value);
        }
        root["module_specific"] = serde_json::Value::Object(merged);
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = crate::config::write_atomic(&path, &pretty);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module_manager::{CredentialField, DiscoveredModule, ModuleManifest};

    /// A scratch module directory that cleans itself up, so these tests never
    /// touch a real module's `.env` / `config.json`.
    struct Scratch(PathBuf);
    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "cockatiel-cred-test-{}-{}",
                tag,
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create scratch dir");
            Scratch(dir)
        }
        fn module(&self) -> DiscoveredModule {
            // Built through serde so this test doesn't need updating whenever a
            // field is added to ModuleManifest.
            let manifest: ModuleManifest =
                serde_json::from_value(serde_json::json!({ "name": "test-adapter" }))
                    .expect("minimal manifest deserializes");
            DiscoveredModule {
                manifest,
                directory: self.0.clone(),
            }
        }
        fn config(&self) -> serde_json::Value {
            serde_json::from_str(
                &std::fs::read_to_string(self.0.join("config.json")).expect("config.json written"),
            )
            .expect("config.json is valid JSON")
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn secret_and_list_fields() -> Vec<CredentialField> {
        vec![
            CredentialField {
                key: "bot_token".to_string(),
                label: "Bot Token".to_string(),
                sensitive: true,
                list: false,
                optional: false,
                env: "DISCORD_BOT_TOKEN".to_string(),
            },
            CredentialField {
                key: "servers".to_string(),
                label: "Servers".to_string(),
                sensitive: false,
                list: true,
                optional: false,
                env: String::new(),
            },
        ]
    }

    /// A module's `module_specific` holds BOTH credentials and tuning knobs.
    /// Saving credentials must only touch the credential keys — the knobs are
    /// not credentials and used to be wiped on every save.
    #[test]
    fn saving_credentials_preserves_unrelated_tuning_knobs() {
        let scratch = Scratch::new("preserve");
        let module = scratch.module();
        std::fs::write(
            module.directory.join("config.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "ip": "127.0.0.1",
                "port": 9734,
                "module_specific": {
                    "servers": { "*": ["*"] },
                    "http_timeout_secs": 42,
                    "send_worker_count": 9,
                    "embed_sends": true,
                    "gateway_reconnect_delay_secs": 7
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let values = HashMap::from([
            ("servers".to_string(), "111=chan-a\n222".to_string()),
        ]);
        save_module_credentials(&module, &secret_and_list_fields(), &values).expect("save succeeds");

        let ms = scratch.config();
        let spec = ms.get("module_specific").expect("module_specific present");
        // The credential was written, in the engine's array form.
        assert_eq!(
            spec.get("servers"),
            Some(&serde_json::json!(["111=chan-a", "222"])),
            "the credential itself must still be written"
        );
        // Every tuning knob survived — this is the regression.
        assert_eq!(spec.get("http_timeout_secs"), Some(&serde_json::json!(42)));
        assert_eq!(spec.get("send_worker_count"), Some(&serde_json::json!(9)));
        assert_eq!(spec.get("embed_sends"), Some(&serde_json::json!(true)));
        assert_eq!(
            spec.get("gateway_reconnect_delay_secs"),
            Some(&serde_json::json!(7))
        );
        // Top-level keys are untouched too.
        assert_eq!(ms.get("port"), Some(&serde_json::json!(9734)));
        assert_eq!(ms.get("ip"), Some(&serde_json::json!("127.0.0.1")));
    }

    #[test]
    fn a_credential_save_can_still_overwrite_its_own_key() {
        // Merging must not make a credential un-editable: re-saving the same
        // key with a new value has to win over the stale value on disk.
        let scratch = Scratch::new("overwrite");
        let module = scratch.module();
        std::fs::write(
            module.directory.join("config.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "module_specific": { "servers": { "*": ["*"] }, "http_timeout_secs": 42 }
            }))
            .unwrap(),
        )
        .unwrap();

        let values = HashMap::from([(
            "servers".to_string(),
            "999=chan-z".to_string(),
        )]);
        save_module_credentials(&module, &secret_and_list_fields(), &values).expect("save succeeds");

        let spec = scratch.config();
        let ms = spec.get("module_specific").expect("module_specific present");
        assert_eq!(ms.get("servers"), Some(&serde_json::json!(["999=chan-z"])));
        assert_eq!(ms.get("http_timeout_secs"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn sensitive_credentials_go_to_env_and_never_to_config() {
        let scratch = Scratch::new("secrets");
        let module = scratch.module();
        let values = HashMap::from([
            ("bot_token".to_string(), "super-secret".to_string()),
            ("servers".to_string(), "111".to_string()),
        ]);
        save_module_credentials(&module, &secret_and_list_fields(), &values).expect("save succeeds");

        let env = std::fs::read_to_string(module.directory.join(".env")).expect(".env written");
        assert!(env.contains("DISCORD_BOT_TOKEN=super-secret"), "got {:?}", env);
        let config = std::fs::read_to_string(module.directory.join("config.json")).unwrap();
        assert!(
            !config.contains("super-secret"),
            "a sensitive credential must never reach config.json"
        );
    }

    #[test]
    fn saving_into_a_missing_or_malformed_config_creates_a_valid_one() {
        // No config.json at all.
        let scratch = Scratch::new("fresh");
        let module = scratch.module();
        let values = HashMap::from([("servers".to_string(), "111".to_string())]);
        save_module_credentials(&module, &secret_and_list_fields(), &values).expect("save succeeds");
        assert_eq!(
            scratch.config().get("module_specific").and_then(|m| m.get("servers")),
            Some(&serde_json::json!(["111"]))
        );

        // Malformed config.json.
        let scratch = Scratch::new("garbage");
        let module = scratch.module();
        std::fs::write(module.directory.join("config.json"), "{ not json").unwrap();
        let values = HashMap::from([("servers".to_string(), "222".to_string())]);
        save_module_credentials(&module, &secret_and_list_fields(), &values).expect("save succeeds");
        assert_eq!(
            scratch.config().get("module_specific").and_then(|m| m.get("servers")),
            Some(&serde_json::json!(["222"]))
        );
    }

    /// The Discord adapter reads `servers` as a JSON OBJECT while the engine
    /// writes it as an ARRAY. Both shapes must survive a save/read round-trip,
    /// or the two writers clobber each other on every cycle.
    #[test]
    fn credential_values_map_reads_back_both_servers_shapes() {
        let scratch = Scratch::new("shapes");
        let module = scratch.module();

        for (label, stored) in [
            ("object", serde_json::json!({ "*": ["*"] })),
            ("array", serde_json::json!(["111=chan-a", "222"])),
        ] {
            std::fs::write(
                module.directory.join("config.json"),
                serde_json::to_string_pretty(&serde_json::json!({
                    "module_specific": { "servers": stored }
                }))
                .unwrap(),
            )
            .unwrap();
            let read = credential_values_map(&module);
            let servers = read.get("servers").expect("servers readable");
            assert!(
                !servers.trim().is_empty(),
                "{} form must read back non-empty, got {:?}",
                label,
                servers
            );
        }
    }
}