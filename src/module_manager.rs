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

    /// Non-empty for core components (engine/tui/user-db/test-runner); a
    /// launchable plugin leaves it empty or sets `"module"`. Discovery skips
    /// anything else — the engine's own `modules/` now holds the user-db
    /// component, which must NOT be launched as a chat module.
    #[serde(default)]
    pub kind: String,

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
    pub unresponsive_timeout_secs: u32,

    /// Optional per-module probe response window (seconds). 0 = use the
    /// engine config default (15s).
    #[serde(default)]
    pub probe_response_secs: u32,

    /// How much score a user must spend for the module to run on their
    /// message. 0 = free. Deducted from the user's CURRENT score when the
    /// module receives their message; the lifetime `total_score` is untouched.
    #[serde(default)]
    pub price: u32,

    /// The minimum rank (0-1 float) a user needs (rank >= this) for the module
    /// to run on their message. 0 = no rank requirement. Numbers are for
    /// logic; tier NAMES are a display concern from the root `rank_chart.json`.
    #[serde(default)]
    pub min_rank: f32,

    /// The minimum authority (role) a user needs for the module to run on
    /// their message. Cascades UP: 0=user (no gate), 1=mod (mod|admin|owner),
    /// 2=admin (admin|owner), 3=owner. A missing field defaults to MOD — new
    /// modules are mod-gated by default; existing modules opt down to `user`
    /// (0) explicitly. This keeps the safe default (a random new module can't
    /// run for every viewer) while letting an operator open a module up.
    #[serde(default = "default_authority")]
    pub authority: u32,
}

/// The default authority for a module with no explicit `authority` field: MOD
/// (1). New modules are mod-gated by default; existing modules explicitly set
/// `authority: 0` (user) to stay open.
pub fn default_authority() -> u32 {
    1
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

            self.discover_in(root, &mut errors);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Recursively walk `dir`, collecting launchable module manifests.
    ///
    /// A directory that contains a `cockatiel_module_info.json` is a LEAF:
    /// whether it loads as a valid module or not (core `kind`, invalid JSON,
    /// empty name), we never descend into its internals (`src/`, `target/`, …).
    /// A directory without a manifest is scanned recursively. Non-directory
    /// entries are ignored.
    fn discover_in(&mut self, dir: &Path, errors: &mut Vec<String>) {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) => {
                errors.push(format!(
                    "Could not read module directory {}: {}",
                    dir.display(),
                    error
                ));
                return;
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

            let manifest_path = path.join("cockatiel_module_info.json");
            if manifest_path.exists() {
                // A directory with a manifest is a leaf — never scan its
                // internals for more modules.
                match Self::load_manifest(&path) {
                    Ok(Some(module)) => {
                        let name = module.manifest.name.clone();

                        if let std::collections::hash_map::Entry::Vacant(slot) =
                            self.modules.entry(name.clone())
                        {
                            slot.insert(module);
                        } else {
                            errors.push(format!(
                                "Duplicate module manifest for '{}': {}",
                                name,
                                path.display()
                            ));
                        }
                    }

                    Ok(None) => {}

                    Err(error) => {
                        errors.push(error);
                    }
                }
            } else {
                self.discover_in(&path, errors);
            }
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

        // Only launchable plugin modules are discovered. A core component
        // manifest (engine/tui/user-db/test-runner) carries a non-empty `kind`.
        if !manifest.kind.is_empty() && manifest.kind != "module" {
            return Ok(None);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ckt-modmgr-{}-{}", tag, uuid::Uuid::now_v7()))
    }

    fn write_manifest(dir: &Path, name: &str, kind: &str) {
        fs::create_dir_all(dir).unwrap();
        fs::write(
            dir.join("cockatiel_module_info.json"),
            format!(r#"{{"name":"{}","kind":"{}"}}"#, name, kind),
        )
        .unwrap();
    }

    #[test]
    fn discovers_a_module_nested_two_levels_deep() {
        let root = temp_root("nested");
        write_manifest(
            &root.join("group").join("deep-module"),
            "deep-module",
            "module",
        );

        let mut registry = ModuleRegistry::new();
        registry
            .discover(std::slice::from_ref(&root))
            .expect("discovery succeeds");

        assert!(registry.get("deep-module").is_some());
        assert_eq!(registry.len(), 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_directory_with_a_manifest_is_a_leaf() {
        let root = temp_root("leaf");
        // The outer plugin is a leaf; its internals (e.g. `target/`) must not
        // be scanned even though they contain another manifest.
        write_manifest(&root.join("plugin-a"), "plugin-a", "module");
        write_manifest(
            &root.join("plugin-a").join("target").join("nested"),
            "should-not-be-found",
            "module",
        );

        let mut registry = ModuleRegistry::new();
        registry
            .discover(std::slice::from_ref(&root))
            .expect("discovery succeeds");

        assert!(registry.get("plugin-a").is_some());
        assert!(
            registry.get("should-not-be-found").is_none(),
            "a plugin directory must be a leaf"
        );
        assert_eq!(registry.len(), 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn core_kind_components_are_skipped_and_do_not_block_walk() {
        let root = temp_root("core");
        write_manifest(&root.join("core-component"), "engine", "engine");
        write_manifest(&root.join("real-module"), "real-module", "module");

        let mut registry = ModuleRegistry::new();
        registry
            .discover(std::slice::from_ref(&root))
            .expect("discovery succeeds");

        assert!(
            registry.get("engine").is_none(),
            "core kind must be skipped"
        );
        assert!(registry.get("real-module").is_some());
        assert_eq!(registry.len(), 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn non_directory_entries_are_ignored() {
        let root = temp_root("files");
        fs::create_dir_all(&root).unwrap();
        // A stray file, even one named like a manifest, is not a directory.
        fs::write(root.join("readme.txt"), "hello").unwrap();
        fs::write(
            root.join("cockatiel_module_info.json"),
            r#"{"name":"not-a-dir"}"#,
        )
        .unwrap();
        write_manifest(&root.join("actual-module"), "actual-module", "module");

        let mut registry = ModuleRegistry::new();
        registry
            .discover(std::slice::from_ref(&root))
            .expect("discovery succeeds");

        assert!(registry.get("not-a-dir").is_none());
        assert!(registry.get("actual-module").is_some());
        assert_eq!(registry.len(), 1);
        let _ = fs::remove_dir_all(&root);
    }
}
