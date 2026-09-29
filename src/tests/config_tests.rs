//! Config tests: the startup backfill writes newly-added keys into config.json
//! with their code defaults (house convention) without disturbing existing keys,
//! and the engine's boot pause state resolves the way the operator asked for.

use crate::config::{
    Config, ConfigState, add_module_to_config, backfill_config_defaults, get_config,
    resolve_start_paused, start_paused, update_config,
};
use std::sync::{Arc, Mutex};

/// A ConfigState pointed at a throwaway temp config.json, parsed from the given
/// content (missing keys fall back to serde defaults — same as engine startup).
fn temp_state(dir: &std::path::Path, content: &str) -> (Arc<Mutex<ConfigState>>, std::path::PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("config.json");
    std::fs::write(&path, content).unwrap();
    let config: Config = serde_json::from_str(content).unwrap();
    let state = Arc::new(Mutex::new(ConfigState {
        path: path.clone(),
        last_size: content.len() as u64,
        config,
        pin: 0,
        jwt_secret: String::new(),
    }));
    (state, path)
}

#[test]
fn backfill_writes_missing_keys_and_preserves_existing() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backfill-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "module_approval_policy": "auto-allow"
    }"#;
    let (state, path) = temp_state(&dir, original);

    backfill_config_defaults(&state);

    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    // Existing keys are preserved untouched.
    assert_eq!(v["timeline_database_location"], "./test.db");
    assert_eq!(v["timeline_database_backup_location"], "./test-backup.db");
    assert_eq!(v["port"], 9734);
    assert_eq!(v["module_approval_policy"], "auto-allow");

    // The eight new keys were written at the TOP level with their defaults.
    assert_eq!(v["max_message_bytes"], 16 * 1024 * 1024);
    assert_eq!(v["max_connections"], 128);
    assert_eq!(v["handshake_timeout_secs"], 10);
    assert_eq!(v["send_timeout_secs"], 5);
    assert_eq!(v["recovery_grace_secs"], 10);
    assert_eq!(v["start_paused"], true, "a fresh config must be explicit about booting paused");
    assert_eq!(
        v["shutdown_on_request"], false,
        "a fresh config must say the engine is NOT shuttable over the wire"
    );
    assert_eq!(
        v["prediction_creator_role"], "mod",
        "a fresh config must say who may create a prediction"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn backfill_is_a_noop_when_all_keys_present() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backfill-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "max_message_bytes": 1,
        "max_connections": 2,
        "handshake_timeout_secs": 3,
        "send_timeout_secs": 4,
        "recovery_grace_secs": 5,
        "start_paused": false,
        "shutdown_on_request": false,
        "prediction_creator_role": "owner",
        "module_approval_policy": "auto-allow"
    }"#;
    let (state, path) = temp_state(&dir, original);

    backfill_config_defaults(&state);

    let content = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();

    // Nothing rewritten — the operator's values survive verbatim.
    assert_eq!(v["max_message_bytes"], 1);
    assert_eq!(v["max_connections"], 2);
    assert_eq!(v["handshake_timeout_secs"], 3);
    assert_eq!(v["send_timeout_secs"], 4);
    assert_eq!(v["recovery_grace_secs"], 5);
    assert_eq!(v["start_paused"], false, "an explicit choice is never overwritten by a default");
    assert_eq!(v["shutdown_on_request"], false);
    assert_eq!(v["prediction_creator_role"], "owner", "an explicit choice is never overwritten by a default");

    std::fs::remove_dir_all(&dir).ok();
}

// ── The boot pause ─────────────────────────────────────────────────────

/// A config with only the required keys — what an old config.json on disk looks
/// like before the backfill has run.
const MINIMAL: &str = r#"{
    "timeline_database_location": "./test.db",
    "timeline_database_backup_location": "./test-backup.db",
    "port": 9734
}"#;

/// The engine boots PAUSED, and the only thing that stops it is an explicit
/// ask. The safe direction to be wrong in is paused: a backlog of unattended
/// messages must never be dispatched to live modules unasked.
#[test]
fn the_engine_boots_paused_by_default() {
    let absent: Config = serde_json::from_str(MINIMAL).unwrap();
    assert!(absent.start_paused, "a missing key must resolve to paused, not to running");
    assert!(resolve_start_paused(&absent, None));

    let explicit: Config = serde_json::from_str(&MINIMAL.replace('}', ",\"start_paused\": true}")).unwrap();
    assert!(resolve_start_paused(&explicit, None));

    // An operator who wants it running on every boot says so in the file.
    let unpaused: Config = serde_json::from_str(&MINIMAL.replace('}', ",\"start_paused\": false}")).unwrap();
    assert!(!unpaused.start_paused);
    assert!(!resolve_start_paused(&unpaused, None));

    // A freshly generated config says so on disk, not just in code.
    let template = include_str!("../config.rs");
    assert!(
        template.contains("\"start_paused\": true"),
        "the generated config.json must boot paused too"
    );
}

/// `COCKATIEL_START_PAUSED` is the headless / CI escape hatch: the compliance
/// test runner and any headless run set it to 0, because nothing would ever
/// press the resume key. A real environment variable wins over the file (the
/// same precedence every other override in the engine uses), and a junk value
/// falls back to the file rather than guessing in either direction.
#[test]
fn the_auto_resume_escape_hatch_flips_the_boot_state() {
    let paused: Config = serde_json::from_str(MINIMAL).unwrap();
    let running: Config = serde_json::from_str(&MINIMAL.replace('}', ",\"start_paused\": false}")).unwrap();

    for off in ["0", "false", "no", "off", "FALSE", " off ", "0\n"] {
        assert!(
            !resolve_start_paused(&paused, Some(off)),
            "COCKATIEL_START_PAUSED={off:?} must boot running"
        );
        assert!(!resolve_start_paused(&running, Some(off)));
    }
    for on in ["1", "true", "yes", "on", "TRUE"] {
        assert!(
            resolve_start_paused(&running, Some(on)),
            "COCKATIEL_START_PAUSED={on:?} must boot paused even against the file"
        );
        assert!(resolve_start_paused(&paused, Some(on)));
    }

    // Nothing set, or set to something meaningless, is the file's answer.
    for junk in [None, Some(""), Some("   "), Some("maybe"), Some("2")] {
        assert!(resolve_start_paused(&paused, junk), "an unusable value must not silently start the engine");
        assert!(!resolve_start_paused(&running, junk));
    }

    // The live entry point reads the real environment and delegates here.
    assert_eq!(start_paused(&paused), resolve_start_paused(&paused, std::env::var("COCKATIEL_START_PAUSED").ok().as_deref()));
}

// ── The shutdown gate ────────────────────────────────────────────────

/// A shutdown request from the wire is refused unless the operator has said yes
/// in config.json. False is the default, and it is the only safe default: a
/// caller that merely holds a socket — or a module that has been compromised —
/// must not be able to take the engine down, and an engine that predates the
/// flag has to refuse.
#[test]
fn engine_shutdown_is_disabled_unless_the_operator_opts_in() {
    // Absent from an existing config.json — the state almost every deployment
    // is in — the key backfills to false.
    let absent: Config = serde_json::from_str(MINIMAL).unwrap();
    assert!(
        !absent.shutdown_on_request,
        "a missing key must resolve to disabled, not to killable"
    );

    // An explicit opt-in is honoured, and survives a serde round trip in both
    // directions (the engine rewrites config.json through `update_config`, so a
    // flag that could not be serialised back would silently reset on the first
    // write).
    let enabled: Config =
        serde_json::from_str(&MINIMAL.replace('}', ",\"shutdown_on_request\": true}")).unwrap();
    assert!(enabled.shutdown_on_request);

    let disabled: Config =
        serde_json::from_str(&MINIMAL.replace('}', ",\"shutdown_on_request\": false}")).unwrap();
    assert!(!disabled.shutdown_on_request);

    for config in [&absent, &enabled, &disabled] {
        let json = serde_json::to_string(config).unwrap();
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.shutdown_on_request, config.shutdown_on_request,
            "the flag must survive a serde round trip"
        );
        assert!(
            json.contains("shutdown_on_request"),
            "the flag must be serialised under its own key, not skipped"
        );
    }

    // A freshly generated config says so on disk, not just in code.
    let template = include_str!("../config.rs");
    assert!(
        template.contains("\"shutdown_on_request\": false"),
        "the generated config.json must refuse shutdown-on-request by default too"
    );
}

// ── The prediction creator role ────────────────────────────────────────

/// The minimum role required to create/resolve a prediction. Defaults to "mod".
#[test]
fn the_prediction_creator_role_defaults_to_mod_and_round_trips() {
    // Absent from an existing config.json the key resolves to the default.
    let absent: Config = serde_json::from_str(MINIMAL).unwrap();
    assert_eq!(
        absent.prediction_creator_role, "mod",
        "a missing key must resolve to the mod default"
    );

    // An explicit role is honoured and survives a serde round trip (the engine
    // rewrites config.json through `update_config`, so a field that could not be
    // serialised back would silently reset on the first write).
    for role in ["owner", "admin", "mod", "user"] {
        let configured: Config =
            serde_json::from_str(&MINIMAL.replace('}', &format!(",\"prediction_creator_role\": \"{role}\"}}")))
                .unwrap();
        assert_eq!(configured.prediction_creator_role, role);

        let json = serde_json::to_string(&configured).unwrap();
        assert!(
            json.contains("prediction_creator_role"),
            "the role must be serialised under its own key, not skipped"
        );
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(back.prediction_creator_role, role, "the role must survive a serde round trip");
    }

    // A freshly generated config says so on disk, not just in code.
    let template = include_str!("../config.rs");
    assert!(
        template.contains("\"prediction_creator_role\": \"mod\""),
        "the generated config.json must default the creator role to mod too"
    );
}
#[test]
fn refresh_config_picks_up_a_same_size_rewrite_the_size_gate_would_miss() {
    // The size gate skips a rewrite that lands on the SAME byte length — which
    // is exactly what a TUI stage move can produce (swap two names of equal
    // length). refresh_config must force the re-read regardless.
    let dir = std::env::temp_dir().join(format!("cockatiel-refresh-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.json");
    // "aaa" vs "bbb": both length-3, so swapping them keeps the file size.
    // The two timeline fields are required (no serde default), so include them.
    let original = r#"{"timeline_database_location":"./t.db","timeline_database_backup_location":"./t-backup.db","port":9734,"preprocessModules":[{"name":"aaa","priority":100}],"inprocessModules":[{"name":"bbb","priority":100}]}"#;
    std::fs::write(&path, original).unwrap();
    // Build the state by hand: `temp_state` also requires `port`; this manual
    // construction only needs what get_config touches (the ordering lists).
    let config: Config = serde_json::from_str(original).unwrap();
    let state = Arc::new(Mutex::new(ConfigState {
        path: path.clone(),
        last_size: original.len() as u64,
        config,
        pin: 0,
        jwt_secret: String::new(),
    }));

    use crate::config::get_config;
    // First read caches size + content.
    let c1 = get_config(&state);
    assert_eq!(c1.preprocess_modules[0].name, "aaa");

    // Rewrite the SAME SIZE file, swapping the two names.
    let swapped = r#"{"timeline_database_location":"./t.db","timeline_database_backup_location":"./t-backup.db","port":9734,"preprocessModules":[{"name":"bbb","priority":100}],"inprocessModules":[{"name":"aaa","priority":100}]}"#;
    assert_eq!(original.len(), swapped.len(), "the swap must not change file size");
    std::fs::write(&path, swapped).unwrap();

    // Without refresh, the size gate hides the change.
    let c2 = get_config(&state);
    assert_eq!(c2.preprocess_modules[0].name, "aaa", "the size gate must skip a same-size rewrite");

    // With refresh, the new order is read.
    crate::config::refresh_config(&state);
    let c3 = get_config(&state);
    assert_eq!(c3.preprocess_modules[0].name, "bbb", "refresh_config must force the re-read");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn add_module_to_config_does_not_revert_an_operator_stage_move() {
    // A stage move (the TUI's Shift+up/down) puts a module in its NEW list.
    // `add_module_to_config` runs on EVERY module connect, and it used to force
    // the module back to the position it happened to CONNECT with — so a module
    // that reconnected (blip, engine restart, TUI relaunch) silently reverted
    // the operator's move. The operator's placement is authoritative: reconnecting
    // must refresh priority only, never move the module back.
    let dir = std::env::temp_dir().join(format!("cockatiel-move-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "preprocessModules": [{"name":"clip","priority":100}],
        "inprocessModules": []
    }"#;
    let (state, path) = temp_state(&dir, original);

    // The operator moves `clip` pre -> in (Shift+down). The TUI's
    // `move_module_by_direction` rewrites config.json on disk, so the engine's
    // next read sees clip in inprocessModules only. Write the moved config to
    // the file exactly as the TUI would.
    let moved = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "preprocessModules": [],
        "inprocessModules": [{"name":"clip","priority":100}],
        "module_probe_interval_secs": 30
    }"#;
    std::fs::write(&path, moved).unwrap();
    // Size changed, so the size gate re-reads it.
    assert_eq!(
        get_config(&state).inprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        vec!["clip"],
        "move must put clip in in-process"
    );

    // The module now reconnects, announcing its manifest/connect position is
    // pre-process. This must NOT move it back — and the engine's own config
    // write (via add_module_to_config -> update_config) must not clobber the
    // on-disk move with a stale in-memory snapshot either.
    add_module_to_config(&state, "clip", "preprocess", 100);
    assert_eq!(
        get_config(&state).inprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        vec!["clip"],
        "a reconnect must not revert the operator's stage move"
    );
    assert_eq!(
        get_config(&state).preprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        Vec::<&str>::new(),
        "a reconnect must not re-add the module to its connect-time stage"
    );
    // And the file on disk must still hold the move after the engine's write.
    let on_disk: Config =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        on_disk.inprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        vec!["clip"],
        "the engine's config write must preserve the on-disk move"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn update_config_re_reads_disk_so_it_never_clobbers_an_external_move() {
    // The engine's in-memory ConfigState snapshot goes stale until the next
    // config-poll re-reads it. `update_config` (used by add_module_to_config on
    // every module connect) used to write that stale snapshot back to disk,
    // wiping an operator stage move the TUI had just written to config.json —
    // the "moved a module and it snapped back within seconds" bug. It must
    // re-read the on-disk state before applying its own change.
    let dir = std::env::temp_dir().join(format!("cockatiel-upd-{}", uuid::Uuid::new_v4()));
    let original = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "preprocessModules": [{"name":"clip","priority":100}],
        "inprocessModules": []
    }"#;
    let (state, path) = temp_state(&dir, original);

    // The operator moves clip pre -> in on disk (the TUI writes config.json).
    let moved = r#"{
        "timeline_database_location": "./test.db",
        "timeline_database_backup_location": "./test-backup.db",
        "port": 9734,
        "preprocessModules": [],
        "inprocessModules": [{"name":"clip","priority":100}],
        "module_probe_interval_secs": 30
    }"#;
    std::fs::write(&path, moved).unwrap();

    // The engine's in-memory snapshot is STILL the original (size-gate). A
    // module connects now; update_config must read the on-disk moved config,
    // apply its change on top, and write it back WITHOUT reverting the move.
    update_config(&state, |config| {
        if let Some(m) = config.inprocess_modules.iter_mut().find(|m| m.name == "clip") {
            m.priority = 50;
        }
    });

    let on_disk: Config = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        on_disk.inprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        vec!["clip"],
        "update_config must preserve the operator's move on disk"
    );
    assert_eq!(
        on_disk.preprocess_modules.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        Vec::<&str>::new(),
        "update_config must not resurrect clip in pre-process"
    );
    assert_eq!(
        on_disk.inprocess_modules[0].priority, 50,
        "update_config's own change must still apply on top of the move"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
