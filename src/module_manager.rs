use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialField {
    pub key: String,
    pub label: String,

    #[serde(default)]
    pub sensitive: bool,

    #[serde(default)]
    pub list: bool,

    /// Optional fields don't block "config complete" and are not required
    /// (e.g. oauth_token, which is auto-acquired by the adapter).
    #[serde(default)]
    pub optional: bool,

    /// Environment-variable name this credential maps to (the key the module's
    /// own code reads, e.g. "KICK_CLIENT_ID"). Credentials are stored in the
    /// module's `.env` under this name so modules load them via `env::var`.
    /// Empty → falls back to the field `key`.
    #[serde(default)]
    pub env: String,
}

impl CredentialField {
    /// The env name used in the module's `.env` file.
    pub fn env_name(&self) -> &str {
        if self.env.is_empty() {
            &self.key
        } else {
            &self.env
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleManifest {
    pub name: String,

    #[serde(default)]
    pub description: String,

    #[serde(default)]
    pub version: String,

    #[serde(default)]
    pub capabilities: String,

    #[serde(default)]
    pub root_file: String,

    #[serde(default)]
    pub launch_command: String,

    #[serde(default)]
    pub command_flags: Vec<String>,

    #[serde(default)]
    pub autostart: bool,

    #[serde(default)]
    pub terminal: bool,

    #[serde(default)]
    pub credentials: Vec<CredentialField>,

    /// Optional per-module dead-air threshold (seconds) before the engine
    /// probes this module. 0 = use the engine config default (30s).
    #[serde(default)]
    pub unresponsive_timeout_secs: u64,

    /// Optional per-module probe response window (seconds). 0 = use the
    /// engine config default (15s).
    #[serde(default)]
    pub probe_response_secs: u64,
}

#[derive(Debug, Clone)]
pub struct DiscoveredModule {
    pub manifest: ModuleManifest,
    pub directory: PathBuf,
}

#[derive(Debug, Default)]
pub struct ModuleRegistry {
    modules: HashMap<String, DiscoveredModule>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
        }
    }

    pub fn discover(&mut self, roots: &[PathBuf]) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for root in roots {
            if !root.exists() {
                continue;
            }

            if !root.is_dir() {
                errors.push(format!(
                    "Module search path is not a directory: {}",
                    root.display()
                ));
                continue;
            }

            let entries = match fs::read_dir(root) {
                Ok(entries) => entries,
                Err(error) => {
                    errors.push(format!(
                        "Could not read module directory {}: {}",
                        root.display(),
                        error
                    ));
                    continue;
                }
            };

            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        errors.push(format!("Could not read module directory entry: {}", error));
                        continue;
                    }
                };

                let path = entry.path();

                if !path.is_dir() {
                    continue;
                }

                match Self::load_manifest(&path) {
                    Ok(Some(module)) => {
                        let name = module.manifest.name.clone();

                        if self.modules.contains_key(&name) {
                            errors.push(format!(
                                "Duplicate module manifest for '{}': {}",
                                name,
                                path.display()
                            ));
                            continue;
                        }

                        self.modules.insert(name, module);
                    }

                    Ok(None) => {}

                    Err(error) => {
                        errors.push(error);
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn load_manifest(directory: &Path) -> Result<Option<DiscoveredModule>, String> {
        let manifest_path = directory.join("cockatiel_module_info.json");

        if !manifest_path.exists() {
            return Ok(None);
        }

        let contents = fs::read_to_string(&manifest_path)
            .map_err(|error| format!("Could not read {}: {}", manifest_path.display(), error))?;

        let manifest: ModuleManifest = serde_json::from_str(&contents).map_err(|error| {
            format!(
                "Invalid module manifest {}: {}",
                manifest_path.display(),
                error
            )
        })?;

        if manifest.name.trim().is_empty() {
            return Err(format!(
                "Module manifest {} has an empty name",
                manifest_path.display()
            ));
        }

        Ok(Some(DiscoveredModule {
            manifest,
            directory: directory.to_path_buf(),
        }))
    }

    pub fn get(&self, name: &str) -> Option<&DiscoveredModule> {
        self.modules.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &DiscoveredModule)> {
        self.modules.iter()
    }

    pub fn len(&self) -> usize {
        self.modules.len()
    }
}
