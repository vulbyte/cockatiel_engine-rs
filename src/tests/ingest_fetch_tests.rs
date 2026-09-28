//! Ingest-side user-data fetch gating.
//!
//! The engine used to run `enrich_chat_user` (two WebSocket round-trips to the
//! user database) on EVERY inbound chat message, even though almost no chat
//! traffic reads the result. It is now fetched only when something downstream
//! will consume it.
//!
//! These tests pin the decision matrix against the real
//! [`should_fetch_user_data`], the ordering invariant that the decision depends
//! on the command having been classified first, and the fact that a skipped
//! fetch does not stop the message being routed onward.

use crate::cockatiel_protobuf::{ChatMessage, Command, Commands, Container, MessagePreProcess, container::Payload};
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseConfig, DatabaseManager};
use crate::pipeline::{PipelineConfig, PipelineOrchestrator};
use crate::{
    EngineState, IngestCommandOutcome, handle_command_on_ingest, should_fetch_user_data,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn flag(name: &str) -> crate::cockatiel_protobuf::Flag {
    crate::cockatiel_protobuf::Flag {
        flag_name: name.to_string(),
        flag_description: String::new(),
        limiting_type: 0,
        min_val: 0.0,
        max_val: 0.0,
        options: vec![],
        value: String::new(),
    }
}

/// A registry with one alerting command module (`!tts`) and no catch-all.
fn registry_with_commands() -> CommandRegistry {
    let mut r = CommandRegistry::default();
    r.register("tts-service", Commands {
        commands: vec![Command {
            command_name: "tts".into(),
            command_flag: "!".into(),
            command_description: "read chat out loud".into(),
            command_flags: vec![flag("p")],
        }],
        alert_on_unknown_command: true,
    });
    r
}

/// The same registry plus a catch-all module (registered an EMPTY command list).
fn registry_with_catch_all() -> CommandRegistry {
    let mut r = registry_with_commands();
    r.register("catch-all-mod", Commands { commands: vec![], alert_on_unknown_command: false });
    r
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// A pipeline config with the given post-process (output display) modules.
fn cfg_with_post_process(post: &[&str]) -> PipelineConfig {
    PipelineConfig { post_process_modules: names(post), ..PipelineConfig::default() }
}

fn chat(raw: &str) -> ChatMessage {
    ChatMessage {
        platform: "twitch".to_string(),
        raw_data: vec![],
        raw_message: raw.to_string(),
        // What an adapter actually sends: a platform handle, never a uuid7.
        user_uuid7: "some_chatter".to_string(),
        command: None,
        user_data: None,
        channel_id: "chan-1".to_string(),
    }
}

/// A fresh in-memory timeline DB.
async fn test_db() -> DatabaseManager {
    let db = DatabaseManager::new(DatabaseConfig {
        local_path: PathBuf::from(":memory:"),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();
    db
}

/// The orchestrator shares the command registry with the ingest path, exactly
/// as `main` wires it — targeted command routing reads the same registry the
/// ingest classification wrote to.
async fn test_orchestrator(
    db: DatabaseManager,
    cfg: PipelineConfig,
    registry: &Arc<Mutex<CommandRegistry>>,
) -> PipelineOrchestrator {
    PipelineOrchestrator::new(
        db,
        cfg,
        Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        Arc::clone(registry),
    )
}

// ─────────────────────────── the decision matrix ───────────────────────────

/// All eight combinations of the three conditions, asserted against the real
/// function. Fetch is expected when ANY condition holds and only then.
#[test]
fn fetch_decision_matrix_is_the_full_truth_table() {
    let empty = CommandRegistry::default();
    let with_catch_all = registry_with_catch_all();
    let no_display = cfg_with_post_process(&[]);
    let with_display = cfg_with_post_process(&["term-chat"]);
    let connected = names(&["term-chat"]);

    for command_attached in [false, true] {
        for catch_all in [false, true] {
            for display in [false, true] {
                let registry = if catch_all { &with_catch_all } else { &empty };
                let cfg = if display { &with_display } else { &no_display };
                let expected = command_attached || catch_all || display;

                let got = should_fetch_user_data(command_attached, registry, cfg, &connected);
                assert_eq!(
                    got, expected,
                    "command_attached={} catch_all={} display={} -> expected fetch={}",
                    command_attached, catch_all, display, expected
                );
            }
        }
    }
}

/// The shape the loop above iterates over, spelled out, so a reader can see the
/// whole table rather than trust the nesting.
#[test]
fn fetch_decision_matrix_rows_are_pinned() {
    let empty = CommandRegistry::default();
    let with_catch_all = registry_with_catch_all();
    let no_display = cfg_with_post_process(&[]);
    let with_display = cfg_with_post_process(&["term-chat"]);
    let connected = names(&["term-chat"]);

    // (command_attached, catch_all, display) -> fetch
    let table: [(bool, bool, bool, bool); 8] = [
        (false, false, false, false),
        (false, false, true, true),
        (false, true, false, true),
        (false, true, true, true),
        (true, false, false, true),
        (true, false, true, true),
        (true, true, false, true),
        (true, true, true, true),
    ];
    for (command_attached, catch_all, display, expected) in table {
        let registry = if catch_all { &with_catch_all } else { &empty };
        let cfg = if display { &with_display } else { &no_display };
        assert_eq!(
            should_fetch_user_data(command_attached, registry, cfg, &connected),
            expected,
            "({command_attached}, {catch_all}, {display}) != {expected}"
        );
    }
}

/// Each condition is individually NECESSARY (without all three and nothing
/// else, a plain message is skipped) and individually SUFFICIENT (any one of
/// them alone flips the result to fetch).
#[test]
fn each_condition_is_individually_necessary_and_sufficient() {
    let connected = names(&["term-chat"]);
    let none = CommandRegistry::default();
    let no_display = cfg_with_post_process(&[]);

    // Necessary: all three absent -> no fetch. This is the common plain-chat
    // case and the only reason this whole change exists.
    assert!(!should_fetch_user_data(false, &none, &no_display, &connected));

    // Sufficient, one at a time.
    assert!(should_fetch_user_data(
        true,
        &none,
        &no_display,
        &connected
    ));
    assert!(should_fetch_user_data(
        false,
        &registry_with_catch_all(),
        &no_display,
        &connected
    ));
    assert!(should_fetch_user_data(
        false,
        &none,
        &cfg_with_post_process(&["term-chat"]),
        &connected
    ));
}

/// Condition 3 is the CONFIGURED list intersected with LIVE connections, not
/// the configured list alone: a display in the config that is not connected
/// consumes nothing, so it must not hold the hot path open. A connected module
/// that is not a post-process display does not count either.
#[test]
fn condition_three_uses_liveness_not_just_configuration() {
    let empty = CommandRegistry::default();
    let cfg = cfg_with_post_process(&["term-chat", "cockatiel-audit-viewer"]);

    // Configured but nothing connected -> skip.
    assert!(!should_fetch_user_data(false, &empty, &cfg, &[]));

    // Connected, but not one of the configured post-process modules.
    assert!(!should_fetch_user_data(
        false,
        &empty,
        &cfg,
        &names(&["twitch-adapter", "banned-words"])
    ));

    // One of the configured displays live -> fetch.
    assert!(should_fetch_user_data(
        false,
        &empty,
        &cfg,
        &names(&["twitch-adapter", "cockatiel-audit-viewer"])
    ));
}

/// A registry whose only module is a command module is NOT a catch-all, so
/// condition 2 must not fire for it. This is the subtle one: `catch_alls()`
/// only returns modules that registered an EMPTY `Commands` list.
#[test]
fn a_command_module_alone_is_not_a_catch_all() {
    let registry = registry_with_commands();
    assert!(registry.catch_alls().is_empty());
    assert!(!should_fetch_user_data(
        false,
        &registry,
        &cfg_with_post_process(&[]),
        &[]
    ));
}

// ─────────────────────────── ordering regression ────────────────────────────

/// THE ordering invariant: the command is classified BEFORE the fetch decision
/// is taken, so a message that IS a registered command still gets its user
/// data. If the ingest path decided the fetch first, `chat.command` would
/// still be `None` at decision time and this would skip the lookup for a
/// command whose whole point is the user data.
///
/// This drives the real first step of the ingest sequence
/// (`handle_command_on_ingest` — the step the receive loop runs before
/// `should_fetch_user_data`) and then the real decision function. No user-db
/// WebSocket is involved, and none is needed: the invariant is about the parse
/// result, not about the fetch succeeding.
#[tokio::test]
async fn command_is_parsed_before_the_fetch_decision() {
    let db = test_db().await;
    let registry = Arc::new(Mutex::new(registry_with_commands()));
    let orchestrator = test_orchestrator(db, PipelineConfig::default(), &registry).await;
    let ui_state = Arc::new(Mutex::new(EngineState::new()));

    // Nothing consumes user data except the command path: no catch-all, no
    // display. So the ONLY reason to fetch here is the attached command.
    let cfg = cfg_with_post_process(&[]);

    let mut c = chat("!tts -p 2 hello everyone");
    // The decision made from the message state BEFORE any parsing: no command
    // is attached yet, so the decision would be "skip".
    assert!(!should_fetch_user_data(
        c.command.is_some(),
        &registry.lock().unwrap(),
        &cfg,
        &[]
    ));

    // Step 1 of the ingest path: classify + act.
    let outcome = handle_command_on_ingest(&mut c, &registry, &orchestrator, &ui_state).await;
    assert_eq!(outcome, IngestCommandOutcome::Attached);
    assert!(outcome.command_attached());
    assert_eq!(c.command.as_ref().unwrap().command_name, "tts");

    // Step 2: the same decision, now that the parse has happened -> fetch.
    assert!(should_fetch_user_data(
        c.command.is_some(),
        &registry.lock().unwrap(),
        &cfg,
        &[]
    ));
}

/// The mirror image: a plain message attaches no command, so the same registry
/// and config skip the fetch. This is what makes the command case above a real
/// regression test rather than a tautology.
#[tokio::test]
async fn plain_message_attaches_nothing_and_skips_the_fetch() {
    let db = test_db().await;
    let registry = Arc::new(Mutex::new(registry_with_commands()));
    let orchestrator = test_orchestrator(db, PipelineConfig::default(), &registry).await;
    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let cfg = cfg_with_post_process(&[]);

    let mut c = chat("just chatting");
    let outcome = handle_command_on_ingest(&mut c, &registry, &orchestrator, &ui_state).await;
    assert_eq!(outcome, IngestCommandOutcome::EngineHandled);
    assert!(!outcome.command_attached());
    assert!(c.command.is_none());
    assert!(!should_fetch_user_data(
        outcome.command_attached(),
        &registry.lock().unwrap(),
        &cfg,
        &[]
    ));
}

/// The engine-handled cases are reported as `EngineHandled`, NOT `Attached`,
/// even though both consume the message. Neither forwards the message to a
/// module and neither reads `user_data`, so neither earns a user-db round-trip.
/// This test is the reason the distinction in `IngestCommandOutcome` exists.
#[tokio::test]
async fn help_and_apology_are_engine_handled_not_attached() {
    let db = test_db().await;
    let registry = Arc::new(Mutex::new(registry_with_commands()));
    // No modules connected: the help list and the apology are both routed
    // through the same `send_to_module` the real path uses, and with an empty
    // sender map they are cheap no-ops.
    let orchestrator = test_orchestrator(db, PipelineConfig::default(), &registry).await;
    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let cfg = cfg_with_post_process(&[]);

    // `!help` is handled by the engine.
    let mut help = chat("!help");
    let help_outcome = handle_command_on_ingest(&mut help, &registry, &orchestrator, &ui_state).await;
    assert_eq!(help_outcome, IngestCommandOutcome::EngineHandled);
    assert!(!help_outcome.command_attached());
    assert!(help.command.is_none());
    assert!(!should_fetch_user_data(
        help_outcome.command_attached(),
        &registry.lock().unwrap(),
        &cfg,
        &[]
    ));

    // An unregistered command under the alerting `!` flag gets the apology.
    let mut bogus = chat("!bogus whatever");
    let bogus_outcome = handle_command_on_ingest(&mut bogus, &registry, &orchestrator, &ui_state).await;
    assert_eq!(bogus_outcome, IngestCommandOutcome::EngineHandled);
    assert!(!bogus_outcome.command_attached());
    assert!(bogus.command.is_none());
    assert!(!should_fetch_user_data(
        bogus_outcome.command_attached(),
        &registry.lock().unwrap(),
        &cfg,
        &[]
    ));
}

// ───────────────────── a skipped fetch does not stop routing ─────────────────

/// Receive one broadcast, failing fast rather than hanging forever if the
/// message was never routed.
async fn recv_broadcast(
    rx: &mut tokio::sync::mpsc::Receiver<Container>,
) -> Container {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out: the message was not routed to the pre-process stage")
        .expect("the pre-process channel closed")
}

/// Connect a stand-in pre-process module and return the receiver it is fed by,
/// so a test can observe what the pipeline actually routed onward.
async fn connect_pre_process(
    orchestrator: &PipelineOrchestrator,
    name: &str,
) -> tokio::sync::mpsc::Receiver<Container> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Container>(8);
    orchestrator
        .module_senders
        .lock()
        .await
        .insert(name.to_string(), tx);
    rx
}

/// The adapter's inbound container: an empty `message_uuid7` is a brand new
/// message, which is what an input-stage adapter sends.
fn adapter_container(chat: ChatMessage) -> Container {
    Container {
        version: 1,
        auth_token: String::new(),
        module_name: "twitch-adapter".to_string(),
        module_instance_uuid7: "0198aabb-0000-7000-8000-0000000000aa".to_string(),
        payload: Some(Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(chat),
            audio: Vec::new(),
            audio_type: String::new(),
        })),
    }
}

/// The skip must be invisible to the rest of the pipeline. A plain message whose
/// user-db fetch was skipped is still broadcast to the pre-process stage, and
/// the downstream module sees it with `user_data == None` and the adapter's own
/// identifier untouched. This drives the same `handle_message_from_module` call
/// the receive loop makes with the (un-enriched) container.
#[tokio::test]
async fn skipped_fetch_still_routes_the_message_with_no_user_data() {
    let db = test_db().await;
    // No display configured, so the fetch decision is purely about the command.
    let cfg = PipelineConfig { pre_process_modules: names(&["watcher"]), ..PipelineConfig::default() };
    let registry = Arc::new(Mutex::new(registry_with_commands()));
    let orchestrator = test_orchestrator(db.clone(), cfg.clone(), &registry).await;
    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let mut rx = connect_pre_process(&orchestrator, "watcher").await;

    let mut c = chat("hello everyone");
    let outcome = handle_command_on_ingest(&mut c, &registry, &orchestrator, &ui_state).await;
    // The fetch is skipped on this plain message...
    assert!(!should_fetch_user_data(
        outcome.command_attached(),
        &registry.lock().unwrap(),
        &cfg,
        &names(&["watcher"])
    ));
    // ...so no enrichment happened and the message still has no user data.
    assert!(c.user_data.is_none());

    assert!(orchestrator.handle_message_from_module(&adapter_container(c)).await.unwrap());

    // The downstream module really did receive it — the skip did not stop the
    // message being routed onward.
    let broadcast = recv_broadcast(&mut rx).await;
    let Some(Payload::MessagePreProcess(bcast)) = broadcast.payload else {
        panic!("expected a pre-process broadcast");
    };
    let seen = bcast.raw_message.clone().expect("broadcast carried no chat message");
    assert_eq!(seen.raw_message, "hello everyone");
    assert!(!bcast.message_uuid7.is_empty());
    // The only visible consequence of the skipped fetch.
    assert!(seen.user_data.is_none());
    assert!(seen.command.is_none());
    // The identifier is still exactly what the adapter sent.
    assert_eq!(seen.user_uuid7, "some_chatter");

    // And the message still runs the rest of the pipeline to completion.
    let uuid = bcast.message_uuid7.clone();
    let ack = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "watcher".to_string(),
        module_instance_uuid7: "0198aabb-0000-7000-8000-0000000000dd".to_string(),
        payload: Some(Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: uuid.clone(),
            raw_message: bcast.raw_message,
            audio: Vec::new(),
            audio_type: String::new(),
        })),
    };
    assert!(orchestrator.handle_message_from_module(&ack).await.unwrap());

    let json = db.get_event_as_json(uuid.as_bytes()).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "complete");
    assert_eq!(v["processed_message"], "hello everyone");
    assert_eq!(v["user_uuid7"], "some_chatter");
}

/// A command message is still routed onward WITH its command attached, so the
/// owning module receives the parsed command it needs.
#[tokio::test]
async fn attached_command_still_routes_with_the_command_attached() {
    let db = test_db().await;
    let cfg = PipelineConfig { pre_process_modules: names(&["tts-service"]), ..PipelineConfig::default() };
    let registry = Arc::new(Mutex::new(registry_with_commands()));
    let orchestrator = test_orchestrator(db.clone(), cfg, &registry).await;
    let ui_state = Arc::new(Mutex::new(EngineState::new()));
    let mut rx = connect_pre_process(&orchestrator, "tts-service").await;

    let mut c = chat("!tts -p 2 hello everyone");
    assert_eq!(
        handle_command_on_ingest(&mut c, &registry, &orchestrator, &ui_state).await,
        IngestCommandOutcome::Attached
    );
    assert!(orchestrator.handle_message_from_module(&adapter_container(c)).await.unwrap());

    let broadcast = recv_broadcast(&mut rx).await;
    let Some(Payload::MessagePreProcess(bcast)) = broadcast.payload else {
        panic!("expected a pre-process broadcast");
    };
    let seen = bcast.raw_message.clone().expect("broadcast carried no chat message");
    let cmd = seen.command.expect("the parsed command did not survive routing");
    assert_eq!(cmd.command_name, "tts");
    assert_eq!(cmd.command_flag, "!");
    assert_eq!(cmd.command_flags[0].value, "2");

    // Ack it so the row completes rather than waiting on the timeout sweep.
    let uuid = bcast.message_uuid7.clone();
    let ack = Container {
        version: 1,
        auth_token: String::new(),
        module_name: "tts-service".to_string(),
        module_instance_uuid7: "0198aabb-0000-7000-8000-0000000000ee".to_string(),
        payload: Some(Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: uuid.clone(),
            raw_message: bcast.raw_message,
            audio: Vec::new(),
            audio_type: String::new(),
        })),
    };
    assert!(orchestrator.handle_message_from_module(&ack).await.unwrap());

    let json = db.get_event_as_json(uuid.as_bytes()).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "complete");
}
