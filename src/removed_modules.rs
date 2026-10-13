//! Transient, file-based suppression of deleted modules.
//!
//! Deleting a module from the TUI kills its process and removes it from
//! `modules.json` and the engine's config ordering — but `module_list` is built
//! from the manifests the engine DISCOVERS on disk, so the deleted module's row
//! reappeared immediately. This records the names the operator deleted in a
//! JSON array next to `config.json` (`removed_modules.json`), and `module_list`
//! excludes them.
//!
//! The suppression is deliberately transient: the engine clears the whole file
//! at startup (a restart re-discovers everything) and clears a single name when
//! that module connects (a reconnect re-initialises it). Nothing is ever
//! permanently hidden — the folder is left in place.

use crate::config::{ConfigState, write_atomic};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// The `removed_modules.json` path in the engine directory (next to
/// `config.json` and `modules.json`).
pub fn removed_modules_path(config_state: &Arc<Mutex<ConfigState>>) -> PathBuf {
    crate::config::modules_path(config_state)
        .parent()
        .unwrap_or(&PathBuf::from("."))
        .join("removed_modules.json")
}

/// Read the suppressed module names. A missing or unparseable file is treated
/// as empty (nothing is suppressed) — a corrupt file must never hide every
/// module from the operator.
pub fn load_removed(path: &Path) -> HashSet<String> {
    fs::read_to_string(path)
        .ok()
        .and_then(|content| serde_json::from_str::<Vec<String>>(&content).ok())
        .map(|names| names.into_iter().collect())
        .unwrap_or_default()
}

/// Persist the set atomically as a sorted JSON array, so concurrent writers
/// converge on the same bytes and the file always keeps the array shape.
fn write_removed(path: &Path, names: &HashSet<String>) -> std::io::Result<()> {
    let mut sorted: Vec<&String> = names.iter().collect();
    sorted.sort();
    let json = serde_json::to_string_pretty(&sorted).unwrap_or_else(|_| "[]".to_string());
    write_atomic(&path.to_path_buf(), &json)
}

/// Add `name` to the suppression file (idempotent), preserving any names
/// another writer already recorded.
///
/// The engine never adds a name itself — writes are owned by the TUI (a
/// separate crate) — but the function is kept here so the file's shape and the
/// read-modify-write rule have one definition and one set of tests.
#[allow(dead_code)]
pub fn add_removed(path: &Path, name: &str) -> std::io::Result<()> {
    let mut names = load_removed(path);
    if !names.insert(name.to_string()) {
        return Ok(());
    }
    write_removed(path, &names)
}

/// Drop `name` from the suppression file (idempotent).
pub fn clear_name(path: &Path, name: &str) -> std::io::Result<()> {
    let mut names = load_removed(path);
    if !names.remove(name) {
        return Ok(());
    }
    write_removed(path, &names)
}

/// Clear every suppressed name — called at startup so a fresh discovery brings
/// every module back. A missing/empty file is left untouched.
pub fn clear_all(path: &Path) -> std::io::Result<()> {
    if load_removed(path).is_empty() {
        return Ok(());
    }
    write_removed(path, &HashSet::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ckt-removed-{}-{}.json", tag, Uuid::now_v7()))
    }

    #[test]
    fn missing_file_loads_empty() {
        let path = temp_path("missing");
        assert!(load_removed(&path).is_empty());
    }

    #[test]
    fn invalid_file_loads_empty() {
        let path = temp_path("invalid");
        fs::write(&path, "not json").unwrap();
        assert!(load_removed(&path).is_empty());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn add_load_round_trip_preserves_array_shape() {
        let path = temp_path("roundtrip");
        add_removed(&path, "clip").unwrap();
        add_removed(&path, "tts-rs").unwrap();

        let loaded = load_removed(&path);
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains("clip") && loaded.contains("tts-rs"));

        // The on-disk shape stays a JSON array (the shape the TUI also writes).
        let raw = fs::read_to_string(&path).unwrap();
        let arr: Vec<String> = serde_json::from_str(&raw).unwrap();
        assert_eq!(arr.len(), 2);

        // Adding again is idempotent — no duplicate entries.
        add_removed(&path, "clip").unwrap();
        assert_eq!(load_removed(&path).len(), 2);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn clear_name_removes_only_that_name() {
        let path = temp_path("clearname");
        add_removed(&path, "clip").unwrap();
        add_removed(&path, "tts-rs").unwrap();

        clear_name(&path, "clip").unwrap();
        let loaded = load_removed(&path);
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains("tts-rs"));

        // Clearing an absent name is a no-op.
        clear_name(&path, "nope").unwrap();
        assert_eq!(load_removed(&path).len(), 1);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn clear_all_empties_the_file() {
        let path = temp_path("clearall");
        add_removed(&path, "clip").unwrap();

        clear_all(&path).unwrap();
        assert!(load_removed(&path).is_empty());

        // The file still exists and holds a valid (empty) array.
        let raw = fs::read_to_string(&path).unwrap();
        let arr: Vec<String> = serde_json::from_str(&raw).unwrap();
        assert!(arr.is_empty());
        let _ = fs::remove_file(&path);
    }
}
