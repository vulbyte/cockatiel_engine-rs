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

/// Write a KEY=VALUE `.env` file (merging into any existing file), owner-only.
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
    std::fs::write(path, content).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
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
        root["module_specific"] = serde_json::Value::Object(spec);
        if let Ok(pretty) = serde_json::to_string_pretty(&root) {
            let _ = std::fs::write(&path, pretty);
        }
    }

    Ok(())
}