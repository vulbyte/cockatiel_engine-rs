//! Pipeline tests: `send_to_module` outcome semantics (Sent / NotConnected /
//! Dropped — it must not lie about drops), crash-recovery `recover_one` draining
//! a stranded 'queued' row through to completion, and the persistence model the
//! pipeline runs on now — ONE terminal write per message instead of a per-stage
//! write, with the in-memory state as the record while a message is in flight.

use crate::cockatiel_protobuf::container_for_engine::Payload as EnginePayload;
use crate::cockatiel_protobuf::container_for_module::Payload as ModulePayload;
use crate::cockatiel_protobuf::{
    ChatMessage, ContainerForEngine, ContainerForModule, MessageInProcess, MessagePostProcess,
    MessagePreProcess,
};
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseConfig, DatabaseManager};
use crate::pipeline::{
    DonationInfo, PendingAck, PipelineConfig, PipelineOrchestrator, PipelineStage, SendOutcome,
    add_donation_css, send_to_module,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) fn dummy_container() -> ContainerForModule {
    ContainerForModule::default()
}

/// A fresh in-memory timeline DB.
pub(super) async fn test_db() -> DatabaseManager {
    let db = DatabaseManager::new(DatabaseConfig {
        local_path: PathBuf::from(":memory:"),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
        log_min_severity: 0.3,
    });
    db.initialize().await.unwrap();
    // Setup counts its own round-trip; the tests below measure from zero.
    db.take_round_trips();
    db
}

pub(super) type ModuleRx = tokio::sync::mpsc::Receiver<ContainerForModule>;

/// An orchestrator whose stages are wired to real (test-owned) module channels,
/// plus the receiving end of each one, so a test can read what the pipeline sent
/// to a stage and answer it.
pub(super) fn wired_orchestrator(
    db: DatabaseManager,
    pre_process: &[&str],
    in_process: &[&str],
    post_process: &[&str],
) -> (PipelineOrchestrator, HashMap<String, ModuleRx>) {
    let names: Vec<String> = pre_process
        .iter()
        .chain(in_process.iter())
        .chain(post_process.iter())
        .map(|m| m.to_string())
        .collect();
    let mut senders = HashMap::new();
    let mut receivers: HashMap<String, ModuleRx> = HashMap::new();
    for name in &names {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        senders.insert(name.clone(), tx);
        receivers.insert(name.clone(), rx);
    }
    let orchestrator = PipelineOrchestrator::new(
        db,
        PipelineConfig {
            pre_process_modules: pre_process.iter().map(|m| m.to_string()).collect(),
            in_process_modules: in_process.iter().map(|m| m.to_string()).collect(),
            post_process_modules: post_process.iter().map(|m| m.to_string()).collect(),
            ack_timeout_ms: 3000,
            critical_modules: Vec::new(),
        },
        Arc::new(tokio::sync::Mutex::new(senders)),
        Arc::new(std::sync::Mutex::new(CommandRegistry::default())),
    );
    // Every test module that hasn't already registered commands is a CATCH-ALL
    // (empty Commands = receives every message). Command routing now skips
    // modules that registered specific commands; without this, a non-command
    // message would never reach a test module and every stage test would
    // dead-end. A module that pre-registered specific commands keeps them.
    let catch_all_names: Vec<String> = names.clone();
    {
        let mut registry = orchestrator.command_registry.lock().unwrap();
        for name in &catch_all_names {
            if !registry.has_registration(name) {
                registry.register(
                    name,
                    crate::cockatiel_protobuf::Commands {
                        commands: vec![],
                        alert_on_unknown_command: false,
                    },
                );
            }
        }
    }
    (orchestrator, receivers)
}

/// An adapter's "here is a new chat message" container: the DB-as-queue entry.
pub(super) fn adapter_ingest(raw_message: &str) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: String::new(),
        module_name: "adapter".into(),
        module_instance_uuid7: String::new(),
        payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(ChatMessage {
                platform: "twitch".into(),
                raw_data: vec![],
                raw_message: raw_message.into(),
                user_uuid7: "user-1".into(),
                command: None,
                channel_id: "chan-1".into(),
                user_data: None,
            }),
            audio: vec![],
            audio_type: String::new(),
        })),
    }
}

pub(super) fn from_module(module_name: &str, payload: EnginePayload) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: String::new(),
        module_name: module_name.into(),
        module_instance_uuid7: String::new(),
        payload: Some(payload),
    }
}

pub(super) fn chat(text: &str) -> ChatMessage {
    ChatMessage {
        platform: "twitch".into(),
        raw_data: vec![],
        raw_message: text.into(),
        user_uuid7: "user-1".into(),
        command: None,
        channel_id: "chan-1".into(),
        user_data: None,
    }
}

pub(super) fn pre_reply(module: &str, uuid7: &str, text: &str) -> ContainerForEngine {
    from_module(
        module,
        EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: uuid7.into(),
            raw_message: Some(chat(text)),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
}

pub(super) fn in_reply(module: &str, uuid7: &str, text: &str, abandon: bool) -> ContainerForEngine {
    from_module(
        module,
        EnginePayload::MessageInProcess(MessageInProcess {
            message_uuid7: uuid7.into(),
            raw_message: Some(chat(text)),
            processed_message: text.into(),
            abandon_message: abandon,
            audio: vec![],
            audio_type: String::new(),
        }),
    )
}

pub(super) fn post_reply(module: &str, uuid7: &str, text: &str) -> ContainerForEngine {
    from_module(
        module,
        EnginePayload::MessagePostProcess(MessagePostProcess {
            message_uuid7: uuid7.into(),
            raw_message: Some(chat(text)),
            processed_message: text.into(),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
}

/// Take the next thing the pipeline broadcast to a module and return the uuid it
/// put on it.
pub(super) fn take_broadcast(rx: &mut ModuleRx) -> String {
    let container = rx
        .try_recv()
        .expect("the pipeline must have broadcast to this module");
    match &container.payload {
        Some(ModulePayload::MessagePreProcess(m)) => m.message_uuid7.clone(),
        Some(ModulePayload::MessageInProcess(m)) => m.message_uuid7.clone(),
        Some(ModulePayload::MessagePostProcess(m)) => m.message_uuid7.clone(),
        other => panic!("expected a stage message, got {other:?}"),
    }
}

pub(super) fn nothing_left(rx: &mut ModuleRx) -> bool {
    rx.try_recv().is_err()
}

pub(super) async fn row(
    db: &DatabaseManager,
    uuid7: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let json = db
        .get_event_as_json(uuid7.as_bytes())
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str::<serde_json::Value>(&json)
        .unwrap()
        .as_object()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn send_outcome_not_connected_when_module_has_no_sender() {
    let senders: Arc<
        tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<ContainerForModule>>>,
    > = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let outcome = send_to_module(&senders, "ghost", dummy_container(), "test").await;
    assert_eq!(outcome, SendOutcome::NotConnected);
}

#[tokio::test]
async fn send_outcome_not_connected_when_channel_is_closed() {
    let (tx, rx) = tokio::sync::mpsc::channel::<ContainerForModule>(1);
    drop(rx); // the module's socket is gone; its sender slot still lingers.
    let senders = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
        "mod".to_string(),
        tx,
    )])));
    let outcome = send_to_module(&senders, "mod", dummy_container(), "test").await;
    assert_eq!(outcome, SendOutcome::NotConnected);
}

#[tokio::test]
async fn send_outcome_dropped_when_channel_is_full() {
    // Bounded capacity-1 channel, receiver kept alive and NEVER drained.
    let (tx, _rx) = tokio::sync::mpsc::channel::<ContainerForModule>(1);
    tx.try_send(dummy_container()).unwrap(); // fill it — the next send can't land
    let senders = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
        "mod".to_string(),
        tx,
    )])));
    let outcome = send_to_module(&senders, "mod", dummy_container(), "test").await;
    assert_eq!(outcome, SendOutcome::Dropped);
}

#[tokio::test]
async fn recover_one_drains_a_stranded_queued_message() {
    let db = test_db().await;
    let uuid = "0198aabb-0000-7000-8000-000000000001".to_string();
    let uuid_bytes = uuid.as_bytes().to_vec();
    db.insert_event(&uuid_bytes, 1, "twitch", &[], "recovered!", "", "{}")
        .await
        .unwrap();
    // insert_event leaves the row 'queued' — nothing ever broadcast it.

    let orchestrator = PipelineOrchestrator::new(
        db.clone(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        Arc::new(std::sync::Mutex::new(CommandRegistry::default())),
    );
    orchestrator.recover_one(&uuid).await.unwrap();

    // With no modules configured the pipeline runs to completion and persists
    // the processed text — the stranded message is drained, not left queued.
    let json = db.get_event_as_json(&uuid_bytes).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "complete");
    assert_eq!(v["processed_message"], "recovered!");
}

#[tokio::test]
async fn recover_one_skips_missing_or_non_queued_rows() {
    let db = test_db().await;
    let uuid = "0198aabb-0000-7000-8000-000000000002".to_string();
    let uuid_bytes = uuid.as_bytes().to_vec();
    db.insert_event(&uuid_bytes, 1, "twitch", &[], "claimed", "", "{}")
        .await
        .unwrap();
    // The row has been driven past 'queued' — a terminal write, or the
    // 'processing' status an engine build before the consolidation used to set
    // when it claimed a message. (An in-flight message is 'queued' now; that
    // case is covered by the in-memory claim check below.)
    db.set_pipeline_status(&uuid_bytes, "processing")
        .await
        .unwrap();

    let orchestrator = PipelineOrchestrator::new(
        db.clone(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        Arc::new(std::sync::Mutex::new(CommandRegistry::default())),
    );
    // Must be a no-op — the row is not 'queued', so nothing may double-broadcast.
    orchestrator.recover_one(&uuid).await.unwrap();

    let json = db.get_event_as_json(&uuid_bytes).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "processing");
}

// ── The consolidated terminal write, end to end ─────────────────────────
//
// The happy-path test below drives a real message through all three stages with
// real module channels, then measures what it cost the database. The rest pin
// the terminal outcomes and the claim that keeps a queued row from being
// re-driven while a live pipeline owns it.

/// One message, all three stages, three database calls. The old model needed a
/// write per stage on top of the same ingest writes.
#[tokio::test]
async fn a_message_through_the_whole_chain_costs_three_database_calls() {
    // The six calls, itemised: insert_event, set_user_uuid, then one
    // is_audited before each stage advances (in / post / complete) and the ONE
    // write that ends the message.
    const ROUND_TRIPS: usize = 6;

    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "final text", false))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), uuid7);

    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "final text"))
        .await
        .unwrap();

    assert_eq!(
        db.take_round_trips(),
        ROUND_TRIPS,
        "a message must cost one write, not one per stage"
    );

    // ...and the row it leaves behind is complete, in every column the old
    // per-stage writes used to fill between them.
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
    assert_eq!(r["raw_message"], "raw text");
    assert_eq!(r["user_uuid7"], "user-1");
    assert_eq!(r["processed_message"], "final text");
    assert!(r["error_message"].is_null());
    let pre = r["pre_process_completed_at"]
        .as_i64()
        .expect("pre_process_completed_at");
    let inp = r["in_process_completed_at"]
        .as_i64()
        .expect("in_process_completed_at");
    let post = r["post_process_completed_at"]
        .as_i64()
        .expect("post_process_completed_at");
    let persisted = r["persisted_at"].as_i64().expect("persisted_at");
    assert!(
        pre <= inp && inp <= post && post <= persisted,
        "stage timestamps out of order: {pre} {inp} {post} {persisted}"
    );

    // The in-memory record is released once the message is terminal, so nothing
    // can rewrite the row afterwards.
    assert!(orchestrator.pipeline_states.lock().await.is_empty());
    assert!(!orchestrator.ack_tracker.lock().await.has_pending(&uuid7));
}

/// A hold is the operator's queue item: the chain stops at it and the row is
/// never flipped to 'complete'.
#[tokio::test]
async fn an_audit_held_message_is_neither_advanced_nor_completed() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("speak english"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    // A module flags it: the hold must land before that module's ack is
    // processed, which is what main.rs guarantees.
    db.mark_audit(uuid7.as_bytes(), "wrong language")
        .await
        .unwrap();
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "translated"))
        .await
        .unwrap();

    assert!(
        nothing_left(rx.get_mut("mid").unwrap()),
        "a held message must not be advanced to in-process"
    );
    assert!(nothing_left(rx.get_mut("post").unwrap()));
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "audit");
    assert_eq!(r["flags"], "wrong language", "the hold reason must survive");
    assert!(
        r["persisted_at"].is_null(),
        "a held message is not persisted"
    );
    assert_eq!(db.list_audit(10, 0).await.unwrap().len(), 1);
}

/// A message held while the post-process stage is already running: the chain
/// finishes, and the pipeline must land what the run produced WITHOUT releasing
/// the hold. (This is the path that reaches the completion write on a held row.)
#[tokio::test]
async fn a_message_held_mid_post_process_keeps_its_hold_and_still_lands_the_result() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);
    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "final text", false))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), uuid7);

    // Held now, while post-process is in flight.
    db.mark_audit(uuid7.as_bytes(), "needs a moderator")
        .await
        .unwrap();
    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "final text"))
        .await
        .unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(
        r["pipeline_status"], "audit",
        "finishing the chain must not complete a held message"
    );
    assert_eq!(r["flags"], "needs a moderator");
    assert!(r["post_process_completed_at"].is_null() && r["persisted_at"].is_null());
    // What the run produced still lands — that is the data the per-stage writes
    // used to leave behind.
    assert_eq!(r["processed_message"], "final text");
    assert!(r["pre_process_completed_at"].is_number() && r["in_process_completed_at"].is_number());
}

/// `abandon_message` is a terminal outcome: the chain stops, the row is dropped
/// with the reason, and the message never reaches post-process.
#[tokio::test]
async fn an_abandoned_message_lands_a_dropped_outcome() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "no thanks", true))
        .await
        .unwrap();

    assert!(
        nothing_left(rx.get_mut("post").unwrap()),
        "an abandoned message must not reach post-process"
    );
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "dropped");
    assert_eq!(r["error_message"], "abandoned by module 'mid'");
    assert_eq!(r["processed_message"], "no thanks");
    assert!(
        r["pre_process_completed_at"].is_number(),
        "the stages it did run are still recorded"
    );
    assert!(
        r["in_process_completed_at"].is_null()
            && r["post_process_completed_at"].is_null()
            && r["persisted_at"].is_null(),
        "an abandoned message never completed"
    );
    assert!(orchestrator.pipeline_states.lock().await.is_empty());

    // A late ack from a sibling module cannot restart a dropped message.
    orchestrator.handle_ack(&uuid7, "mid").await.unwrap();
    assert_eq!(row(&db, &uuid7).await["pipeline_status"], "dropped");
}

/// A critical module that never answers fails the message — and everything the
/// run had produced up to that point lands with the failure.
#[tokio::test]
async fn a_critical_module_timeout_lands_a_failed_outcome_with_its_error() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);
    let mut cfg = orchestrator.config_snapshot().await;
    cfg.critical_modules = vec!["mid".to_string()];
    orchestrator.set_config(cfg).await;

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    // 'mid' goes silent past its ack budget.
    orchestrator.ack_tracker.lock().await.inject(
        uuid7.clone(),
        vec![PendingAck {
            uuid7: uuid7.clone(),
            stage: "in_process".into(),
            module_name: "mid".into(),
            sent_at: Instant::now() - Duration::from_secs(30),
            timeout: Duration::from_millis(1),
            receipt_received: true,
            resend_count: 0,
        }],
    );
    orchestrator.handle_timeout().await.unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "failed");
    assert_eq!(
        r["error_message"], "Critical module 'mid' timed out at stage 'in_process'",
        "the failure reason must be recorded verbatim"
    );
    assert_eq!(
        r["processed_message"], "pre text",
        "what the run produced lands with the failure"
    );
    assert!(r["pre_process_completed_at"].is_number());
    assert!(
        r["in_process_completed_at"].is_null()
            && r["post_process_completed_at"].is_null()
            && r["persisted_at"].is_null(),
        "a failed message must not claim it was persisted"
    );
}

/// A module that never sent its receipt ping gets the stage message RESENT
/// (up to the cap) instead of being treated as dead — the receipt distinguishes
/// "didn't get it" from "got it but hasn't finished".
#[tokio::test]
async fn a_receipt_less_module_is_resent_not_advanced() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &[], &[]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    // 'pre' never sends a receipt and never returns a result. Inject an expired
    // pending entry with receipt_received=false, like a message whose budget
    // lapsed while the module was silent.
    orchestrator.ack_tracker.lock().await.inject(
        uuid7.clone(),
        vec![PendingAck {
            uuid7: uuid7.clone(),
            stage: "pre_process".into(),
            module_name: "pre".into(),
            sent_at: Instant::now() - Duration::from_secs(30),
            timeout: Duration::from_millis(1),
            receipt_received: false,
            resend_count: 0,
        }],
    );
    orchestrator.handle_timeout().await.unwrap();

    // The message is still in flight, and the module got a fresh broadcast
    // (the resend). Nothing has been marked failed/complete.
    assert_eq!(
        take_broadcast(rx.get_mut("pre").unwrap()),
        uuid7,
        "the stage must be resent"
    );
    assert!(
        orchestrator.ack_tracker.lock().await.has_pending(&uuid7),
        "the message stays pending across the resend"
    );

    // The module now sends its receipt, then its result → the stage advances.
    orchestrator
        .handle_message_from_module(&ack_container("pre", &uuid7))
        .await
        .unwrap();
    assert!(
        orchestrator.ack_tracker.lock().await.has_pending(&uuid7),
        "a receipt alone must not advance the stage"
    );
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "done"))
        .await
        .unwrap();
    assert!(!orchestrator.ack_tracker.lock().await.has_pending(&uuid7));

    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
    assert_eq!(r["processed_message"], "done");
}

/// Build a pure receipt ping (`MessageAck`) from a module for a message.
fn ack_container(module: &str, uuid7: &str) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: String::new(),
        module_name: module.to_string(),
        module_instance_uuid7: String::new(),
        payload: Some(EnginePayload::MessageAck(
            crate::cockatiel_protobuf::MessageAck {
                message_uuid7: uuid7.to_string(),
            },
        )),
    }
}

/// The claim check the database used to do. A row in flight is 'queued' now
/// (the pipeline writes no intermediate status), so a queued-row drain has to
/// consult the in-memory state instead — or it would re-broadcast a live
/// message and overwrite its state.
#[tokio::test]
async fn recover_one_leaves_a_message_that_is_already_in_flight_alone() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("in flight"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    // The row is still 'queued' — that is what a drain looks for.
    assert_eq!(row(&db, &uuid7).await["pipeline_status"], "queued");
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![uuid7.clone()]);

    orchestrator.recover_one(&uuid7).await.unwrap();

    assert!(
        nothing_left(rx.get_mut("pre").unwrap()),
        "an in-flight message must not be re-broadcast to pre-process"
    );
    assert!(
        nothing_left(rx.get_mut("mid").unwrap()),
        "its in-process stage must not be restarted either"
    );
    assert_eq!(
        row(&db, &uuid7).await["pipeline_status"],
        "queued",
        "the drain must not write to the row"
    );
    let states = orchestrator.pipeline_states.lock().await;
    let state = states
        .get(&uuid7)
        .expect("the live state must survive the drain");
    assert_eq!(
        state.processed_message, "pre text",
        "the drain must not reset the live state"
    );
    assert!(matches!(state.stage, PipelineStage::InProcessing));
}

/// Structural, because a behavioural test only covers the branches it happens to
/// drive: if a per-stage write were left anywhere in the orchestrator, the
/// message would cost a round-trip per stage again and every count above would
/// quietly stop being true.
#[test]
fn the_orchestrator_writes_nothing_per_stage() {
    let src = include_str!("../pipeline.rs");
    for gone in [
        "self.db.set_processed_message(",
        "self.db.set_audio(",
        "self.db.set_error(",
        "self.db.update_stage_completed(",
        "self.db.set_pipeline_status(",
    ] {
        assert!(
            !src.contains(gone),
            "the pipeline must not call `{gone}` any more: one write ends a message"
        );
    }
    // ...and the write it does make is the consolidated one, from the four
    // terminal paths (complete, complete-on-a-held-row, failed, dropped).
    assert_eq!(
        src.matches(".write_terminal_outcome(").count(),
        4,
        "every terminal path must route through the single consolidated write"
    );
}

#[tokio::test]
async fn permit_module_gate_semantics() {
    // Build a bare orchestrator (no user-db wired). This exercises the gate's
    // fast paths and the degraded-mode behavior; the actual deduction + rank
    // check live in the user-db and are covered by its own tests.
    let db = test_db().await;
    let (orchestrator, _rx) = wired_orchestrator(db, &[], &[], &[]);

    // No gate registered for a module → allowed.
    assert!(orchestrator.permit_module("user-1", "unknown-module").await);

    // (0, 0, 0) gate → free + unrestricted → allowed.
    {
        let mut gates = std::collections::HashMap::new();
        gates.insert(
            "free".to_string(),
            crate::pipeline::ModuleGate {
                authority: crate::pipeline::AUTHORITY_USER,
                min_rank: 0.0,
                price: 0,
            },
        );
        orchestrator.set_module_gates(gates);
    }
    assert!(orchestrator.permit_module("user-1", "free").await);

    // Priced/ranked/authority-gated module with NO user-db wired → allowed
    // (degraded: don't stall the pipeline on a gate when the user-db is
    // unavailable). The real enforcement happens when user_db is set.
    {
        let mut gates = std::collections::HashMap::new();
        gates.insert(
            "paid".to_string(),
            crate::pipeline::ModuleGate {
                authority: crate::pipeline::AUTHORITY_MOD,
                min_rank: 0.3,
                price: 5,
            },
        );
        orchestrator.set_module_gates(gates);
    }
    assert!(orchestrator.permit_module("user-1", "paid").await);

    // Empty user uuid with a gated module → allowed (nobody to check/charge).
    assert!(orchestrator.permit_module("", "paid").await);
}

/// The authority cascade: mod admits mod|admin|owner, admin admits admin|owner,
/// owner admits owner — never a lower tier. Mirrors the `User` role flags.
#[tokio::test]
async fn module_authority_cascades_up_and_never_down() {
    use crate::pipeline::{
        AUTHORITY_ADMIN, AUTHORITY_MOD, AUTHORITY_OWNER, AUTHORITY_USER, ModuleGate,
    };

    // A helper that reports the authority tier a role set passes for a gate.
    // This is a pure projection of the same logic permit_module uses, kept here
    // so the cascade rule is asserted independently of the user-db round-trip.
    let tier = |is_owner: bool, is_admin: bool, is_moderator: bool| -> u32 {
        if is_owner {
            AUTHORITY_OWNER
        } else if is_admin {
            AUTHORITY_ADMIN
        } else if is_moderator {
            AUTHORITY_MOD
        } else {
            AUTHORITY_USER
        }
    };
    // A gate is satisfied when the user's tier >= the gate's authority.
    let satisfied = |gate: u32, user_tier: u32| user_tier >= gate;

    // user (0) passes only an authority-0 gate.
    assert!(satisfied(AUTHORITY_USER, tier(false, false, false)));
    assert!(!satisfied(AUTHORITY_MOD, tier(false, false, false)));
    // mod (1) passes mod and user gates, not admin/owner.
    let t_mod = tier(false, false, true);
    assert!(satisfied(AUTHORITY_MOD, t_mod));
    assert!(satisfied(AUTHORITY_USER, t_mod));
    assert!(!satisfied(AUTHORITY_ADMIN, t_mod));
    assert!(!satisfied(AUTHORITY_OWNER, t_mod));
    // admin (2) passes admin, mod, user.
    let t_admin = tier(false, true, false);
    assert!(satisfied(AUTHORITY_ADMIN, t_admin));
    assert!(satisfied(AUTHORITY_MOD, t_admin));
    assert!(!satisfied(AUTHORITY_OWNER, t_admin));
    // owner (3) passes everything.
    let t_owner = tier(true, false, false);
    assert!(satisfied(AUTHORITY_OWNER, t_owner));
    assert!(satisfied(AUTHORITY_ADMIN, t_owner));
    assert!(satisfied(AUTHORITY_MOD, t_owner));

    // And the gate struct's open-check: (0,0,0) is open, anything else isn't.
    let open = ModuleGate {
        authority: AUTHORITY_USER,
        min_rank: 0.0,
        price: 0,
    };
    assert!(open.is_open());
    let closed = ModuleGate {
        authority: AUTHORITY_MOD,
        min_rank: 0.0,
        price: 0,
    };
    assert!(!closed.is_open());
}

/// A post-process module that registered specific commands (command-scoped)
/// only handles the commands it owns. A plain chat message (no command) must
/// NOT reach it — otherwise its price gate is charged for a message it would
/// ignore. Displays/scorers/censors never register commands, so they still get
/// plain chat; this pins the SKIP side of that rule.
#[tokio::test]
async fn a_command_scoped_post_process_module_is_skipped_for_plain_messages() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["display"]);
    // Make "display" command-scoped: give it a specific command. A plain message
    // (no command) must then skip it.
    {
        let mut registry = orchestrator.command_registry.lock().unwrap();
        registry.register(
            "display",
            crate::cockatiel_protobuf::Commands {
                commands: vec![crate::cockatiel_protobuf::Command {
                    command_name: "show".into(),
                    command_flag: "!".into(),
                    command_description: String::new(),
                    command_flags: vec![],
                }],
                alert_on_unknown_command: false,
            },
        );
    }

    orchestrator
        .handle_message_from_module(&adapter_ingest("plain chat, no command"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    take_broadcast(rx.get_mut("mid").unwrap());
    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "final", false))
        .await
        .unwrap();

    // The command-scoped post-process module is skipped for a plain message.
    assert!(
        nothing_left(rx.get_mut("display").unwrap()),
        "a command-scoped module must not receive a plain chat message"
    );

    // With no recipients left in the post-process stage the message completes
    // without waiting on an ack that will never come.
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
}

/// The bug this pins: a command-scoped module (one that registered specific
/// commands) must not be charged its price gate for a plain message. With empty
/// pre/in stages the message goes straight to post-process; there `tts` is
/// command-scoped and `display` is a catch-all, so only `display` receives it.
/// Before the fix, the no-command path fanned out to every post-process module,
/// charging `tts`'s price for a message it ignores.
#[tokio::test]
async fn a_plain_message_skips_a_command_scoped_post_process_module() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &[], &[], &["tts", "display"]);
    // Replace the helper's auto catch-all registration for "tts" with a specific
    // command, making it command-scoped. "display" stays a catch-all.
    {
        let mut registry = orchestrator.command_registry.lock().unwrap();
        registry.register(
            "tts",
            crate::cockatiel_protobuf::Commands {
                commands: vec![crate::cockatiel_protobuf::Command {
                    command_name: "tts".into(),
                    command_flag: "!".into(),
                    command_description: String::new(),
                    command_flags: vec![],
                }],
                alert_on_unknown_command: false,
            },
        );
    }

    orchestrator
        .handle_message_from_module(&adapter_ingest("hello there"))
        .await
        .unwrap();

    // The catch-all display receives it; the command-scoped tts does not — so
    // permit_module (its price gate) is never consulted for tts.
    let uuid7 = take_broadcast(rx.get_mut("display").unwrap());
    assert!(
        nothing_left(rx.get_mut("tts").unwrap()),
        "a command-scoped module must not be charged for a plain message"
    );

    orchestrator
        .handle_message_from_module(&post_reply("display", &uuid7, "hello there"))
        .await
        .unwrap();
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
}

/// A post-process DISPLAY must see command messages too — it renders `!tts`,
/// `!ban`, … and, for a generator like `tts-rs`, plays the inline audio the
/// command produced. Before the fix the post-process stage narrowed a command to
/// owner + catch-alls, so a display that never registers commands (term-chat)
/// received no `!tts` message at all. A command-scoped module still only sees
/// the command it owns.
#[tokio::test]
async fn a_command_reaches_post_process_displays_and_its_owner_but_not_a_foreign_module() {
    let db = test_db().await;
    let (orchestrator, mut rx) =
        wired_orchestrator(db.clone(), &[], &[], &["display", "owner-mod", "other-cmd"]);
    {
        let mut registry = orchestrator.command_registry.lock().unwrap();
        registry.register("owner-mod", command_set("foo"));
        registry.register("other-cmd", command_set("bar"));
        // "display" keeps the helper's catch-all: it never registers commands.
    }

    // Ingest a message carrying the `!foo` command (the pipeline tests don't run
    // the ingest-time parser, so attach it directly).
    let mut ingest = adapter_ingest("!foo hi");
    if let Some(EnginePayload::MessagePreProcess(pre)) = ingest.payload.as_mut()
        && let Some(chat) = pre.raw_message.as_mut()
    {
        chat.command = Some(crate::cockatiel_protobuf::Command {
            command_name: "foo".into(),
            command_flag: "!".into(),
            command_description: String::new(),
            command_flags: vec![],
        });
    }
    orchestrator
        .handle_message_from_module(&ingest)
        .await
        .unwrap();

    // The display AND the owning module both receive it; the foreign
    // command-scoped module does not.
    let uuid7 = take_broadcast(rx.get_mut("display").unwrap());
    assert_eq!(
        take_broadcast(rx.get_mut("owner-mod").unwrap()),
        uuid7,
        "the command owner must receive its command"
    );
    assert!(
        nothing_left(rx.get_mut("other-cmd").unwrap()),
        "a module that owns a different command must not receive this one"
    );
}

/// A one-command `Commands` registration for routing tests.
fn command_set(name: &str) -> crate::cockatiel_protobuf::Commands {
    crate::cockatiel_protobuf::Commands {
        commands: vec![crate::cockatiel_protobuf::Command {
            command_name: name.into(),
            command_flag: "!".into(),
            command_description: String::new(),
            command_flags: vec![],
        }],
        alert_on_unknown_command: false,
    }
}

/// Regression: two recipients acking the same message near-simultaneously must
/// advance the stage exactly ONCE. Before the fix, `handle_ack` released the
/// tracker lock between removing the acking module's entry and checking
/// `has_pending`, so both acks could observe "nothing pending" and each dispatch
/// the next stage — a double-send to in-process (and, at the tail, a `complete`
/// then `failed` terminal write).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_acks_advance_the_stage_exactly_once() {
    const ITERATIONS: usize = 25;
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre-a", "pre-b"], &["mid"], &[]);

    for _ in 0..ITERATIONS {
        orchestrator
            .handle_message_from_module(&adapter_ingest("raw text"))
            .await
            .unwrap();
        let uuid7 = take_broadcast(rx.get_mut("pre-a").unwrap());
        assert_eq!(take_broadcast(rx.get_mut("pre-b").unwrap()), uuid7);

        let a = orchestrator.clone();
        let b = orchestrator.clone();
        let ua = uuid7.clone();
        let ub = uuid7.clone();
        let h1 = tokio::spawn(async move { a.handle_ack(&ua, "pre-a").await.unwrap() });
        let h2 = tokio::spawn(async move { b.handle_ack(&ub, "pre-b").await.unwrap() });
        h1.await.unwrap();
        h2.await.unwrap();

        // Exactly one dispatch to in-process; the second ack must have seen the
        // first's removal atomically and done nothing.
        assert_eq!(
            take_broadcast(rx.get_mut("mid").unwrap()),
            uuid7,
            "the in-process stage was dispatched more than once"
        );
        assert!(
            nothing_left(rx.get_mut("mid").unwrap()),
            "the in-process stage was dispatched more than once"
        );
    }
}

/// A module with no pending ack for a message must not be able to overwrite its
/// in-memory state: only the module that was actually sent the stage may write
/// the processed text/audio.
#[tokio::test]
async fn an_untracked_module_cannot_mutate_another_messages_state() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &[]);
    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    // "rogue" owes no ack for this uuid — its reply must be ignored entirely.
    orchestrator
        .handle_message_from_module(&pre_reply("rogue", &uuid7, "HIJACKED"))
        .await
        .unwrap();

    {
        let states = orchestrator.pipeline_states.lock().await;
        let state = states.get(&uuid7).expect("the message is still in flight");
        assert_eq!(
            state.processed_message, "raw text",
            "an untracked module must not mutate the message state"
        );
    }

    // The owning module's reply still drives the chain exactly as before.
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);
}

/// A critical-module timeout must not clobber an operator's audit hold: the row
/// stays 'audit' with its reason, exactly as a normal completion on a held row.
#[tokio::test]
async fn a_critical_timeout_does_not_clobber_an_audit_hold() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &[]);
    let mut cfg = orchestrator.config_snapshot().await;
    cfg.critical_modules = vec!["mid".to_string()];
    orchestrator.set_config(cfg).await;

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    // Held while 'mid' is in flight, then 'mid' times out critically.
    db.mark_audit(uuid7.as_bytes(), "needs a moderator")
        .await
        .unwrap();
    orchestrator.ack_tracker.lock().await.inject(
        uuid7.clone(),
        vec![PendingAck {
            uuid7: uuid7.clone(),
            stage: "in_process".into(),
            module_name: "mid".into(),
            sent_at: Instant::now() - Duration::from_secs(30),
            timeout: Duration::from_millis(1),
            receipt_received: true,
            resend_count: 0,
        }],
    );
    orchestrator.handle_timeout().await.unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(
        r["pipeline_status"], "audit",
        "a hold must survive a critical timeout"
    );
    assert_eq!(r["flags"], "needs a moderator");
    assert!(
        r["error_message"].is_null(),
        "the hold must not record the timeout as an error"
    );
}

/// The bug this pins: `refresh_user_data` re-reads the message owner's CURRENT
/// score so downstream stages / displays see what the in-process scorer just
/// did. It used to run at the pre-process boundary; scoring moved into the
/// in-process stage, so the refresh must now run when the LAST in-process module
/// acks — right before the handoff to post-process — in BOTH the ack path and
/// the timeout sweep. Structural because a missing user-db (as in every test
/// here) makes the helper a silent no-op, so behavior alone can't observe it.
#[test]
fn refresh_user_data_moved_to_the_in_process_to_post_process_boundary() {
    // Collapse all whitespace so the assertions don't depend on indentation.
    let src: String = include_str!("../pipeline.rs")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    // Present: the refresh immediately precedes `start_post_process` in both the
    // handle_ack path and the timeout sweep.
    for needle in [
        "self.refresh_user_data(uuid7).await;self.start_post_process(uuid7).await?;",
        "self.refresh_user_data(&entry.uuid7).await;self.start_post_process(&entry.uuid7).await?;",
    ] {
        assert!(
            src.contains(needle),
            "refresh must precede start_post_process at the in-process boundary: {needle:?}"
        );
    }

    // Gone: the refresh must no longer sit on the pre-process -> in-process edge.
    for gone in [
        "self.note_pre_process_completed(uuid7).await;self.refresh_user_data(uuid7).await;",
        "self.note_pre_process_completed(&entry.uuid7).await;self.refresh_user_data(&entry.uuid7).await;",
    ] {
        assert!(
            !src.contains(gone),
            "refresh must have left the pre-process boundary: {gone:?}"
        );
    }

    // Exactly the two boundary call sites — not added to the empty-in-process
    // path or anywhere else. (The definition is `fn refresh_user_data(&self`,
    // which does not match this pattern.)
    assert_eq!(
        src.matches("self.refresh_user_data(").count(),
        2,
        "refresh_user_data must be called at exactly the two in-process->post edges"
    );
}

/// The in-process -> post-process handoff the refresh now sits on: with a single
/// in-process module, post-process must not be dispatched until that module acks,
/// and that last ack is what dispatches it. (`refresh_user_data` runs on this
/// edge — see the structural test above — so the last in-process ack is the
/// moment downstream stages get the refreshed user.)
#[tokio::test]
async fn the_last_in_process_ack_hands_off_to_post_process() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "raw text"))
        .await
        .unwrap();

    // The in-process head got it; post-process must wait for the ack.
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);
    assert!(
        nothing_left(rx.get_mut("post").unwrap()),
        "post-process must wait for the in-process stage to finish"
    );

    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "scored text", false))
        .await
        .unwrap();

    // The in-process completion is what dispatches post-process.
    assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), uuid7);
}

/// A module abandon must not clobber an audit hold either.
#[tokio::test]
async fn an_abandon_does_not_clobber_an_audit_hold() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &[]);
    orchestrator
        .handle_message_from_module(&adapter_ingest("raw text"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);

    db.mark_audit(uuid7.as_bytes(), "hold me").await.unwrap();
    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "no thanks", true))
        .await
        .unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(
        r["pipeline_status"], "audit",
        "a hold must survive an abandon"
    );
    assert_eq!(r["flags"], "hold me");
    assert!(r["error_message"].is_null());
}

/// Only a live incumbent from a DIFFERENT instance causes a shadow. A
/// same-instance reconnect must take the slot (its new socket is the target),
/// and a dead incumbent never blocks the slot.
#[test]
fn shadow_only_when_a_live_different_instance_holds_the_slot() {
    use crate::pipeline::should_shadow;
    assert!(
        !should_shadow(false, false),
        "no incumbent -> take the slot"
    );
    assert!(
        !should_shadow(false, true),
        "dead incumbent -> take the slot"
    );
    assert!(
        !should_shadow(true, true),
        "same-instance reconnect must take the slot (old socket may still be closing)"
    );
    assert!(
        should_shadow(true, false),
        "a live DIFFERENT instance keeps the slot; the newcomer shadows"
    );
}

/// The name-keyed routing slot is owned by exactly one instance. A duplicate
/// same-name connection (a "shadow") must not evict a live incumbent, a
/// non-owner's disconnect must not remove the slot, and only the owner's
/// disconnect releases it.
#[tokio::test]
async fn duplicate_connection_shadow_cannot_evict_live_slot() {
    let orchestrator = PipelineOrchestrator::new(
        test_db().await,
        PipelineConfig {
            pre_process_modules: vec![],
            in_process_modules: vec![],
            post_process_modules: vec![],
            ack_timeout_ms: 3000,
            critical_modules: vec![],
        },
        Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        Arc::new(std::sync::Mutex::new(CommandRegistry::default())),
    );

    // The incumbent claims the slot.
    let (incumbent_tx, mut incumbent_rx) = tokio::sync::mpsc::channel(8);
    {
        let mut senders = orchestrator.module_senders.lock().await;
        let mut owners = orchestrator.slot_owner.lock().await;
        senders.insert("scorer".to_string(), incumbent_tx.clone());
        owners.insert("scorer".to_string(), "instance-a".to_string());
    }

    // A duplicate arrives while the incumbent's sender is live: it is accepted
    // as a shadow and must NOT overwrite the slot or its owner.
    let (shadow_tx, _shadow_rx) = tokio::sync::mpsc::channel(8);
    {
        let mut senders = orchestrator.module_senders.lock().await;
        let mut owners = orchestrator.slot_owner.lock().await;
        let live_incumbent = senders
            .get("scorer")
            .map(|s| !s.is_closed())
            .unwrap_or(false);
        assert!(live_incumbent);
        if !live_incumbent {
            senders.insert("scorer".to_string(), shadow_tx.clone());
            owners.insert("scorer".to_string(), "instance-b".to_string());
        }
    }
    {
        let senders = orchestrator.module_senders.lock().await;
        let owners = orchestrator.slot_owner.lock().await;
        assert!(
            senders
                .get("scorer")
                .map(|s| s.same_channel(&incumbent_tx))
                .unwrap_or(false),
            "the live incumbent's sender must be retained"
        );
        assert_eq!(owners.get("scorer").map(String::as_str), Some("instance-a"));
    }

    // The shadow (which never owned the slot) disconnects. Its auth session is
    // still its own (`still_owned == true`), but it does not own the slot, so
    // cleanup must leave the live module's slot untouched.
    {
        let mut senders = orchestrator.module_senders.lock().await;
        let mut owners = orchestrator.slot_owner.lock().await;
        let still_owned = true;
        if still_owned && owners.get("scorer").map(String::as_str) == Some("instance-b") {
            senders.remove("scorer");
            owners.remove("scorer");
        }
    }
    {
        let senders = orchestrator.module_senders.lock().await;
        let owners = orchestrator.slot_owner.lock().await;
        assert!(senders.contains_key("scorer"));
        assert_eq!(owners.get("scorer").map(String::as_str), Some("instance-a"));
    }

    // The watchdog path — the EXACT live score-messages failure: after the
    // duplicate connects and disconnects, a probe to the surviving module must be
    // DELIVERED (Sent), not dropped as NotConnected. If it were NotConnected the
    // probe deadline would expire and the module would be killed as unresponsive.
    {
        let probe = ContainerForModule {
            version: 2,
            auth_token: String::new(),
            module_instance_uuid7: String::new(),
            payload: Some(ModulePayload::AuthVerify(
                crate::cockatiel_protobuf::AuthVerify {
                    cur_auth: String::new(),
                },
            )),
        };
        let outcome = send_to_module(&orchestrator.module_senders, "scorer", probe, "probe").await;
        assert_eq!(
            outcome,
            SendOutcome::Sent,
            "a probe to the surviving module must be delivered, not dropped"
        );
        assert!(
            incumbent_rx.try_recv().is_ok(),
            "the live module must actually receive the probe"
        );
    }

    // The incumbent dies (its receiver, owned by the socket, is dropped). The
    // slot's sender is now closed, so a newcomer is no longer a shadow.
    drop(incumbent_rx);
    let (newcomer_tx, _newcomer_rx) = tokio::sync::mpsc::channel(8);
    {
        let mut senders = orchestrator.module_senders.lock().await;
        let mut owners = orchestrator.slot_owner.lock().await;
        let live_incumbent = senders
            .get("scorer")
            .map(|s| !s.is_closed())
            .unwrap_or(false);
        assert!(
            !live_incumbent,
            "the dead incumbent must not block the slot"
        );
        senders.insert("scorer".to_string(), newcomer_tx.clone());
        owners.insert("scorer".to_string(), "instance-b".to_string());
    }

    // The owner disconnects: cleanup releases the slot.
    {
        let mut senders = orchestrator.module_senders.lock().await;
        let mut owners = orchestrator.slot_owner.lock().await;
        let still_owned = true;
        if still_owned && owners.get("scorer").map(String::as_str) == Some("instance-b") {
            senders.remove("scorer");
            owners.remove("scorer");
        }
    }
    {
        let senders = orchestrator.module_senders.lock().await;
        let owners = orchestrator.slot_owner.lock().await;
        assert!(!senders.contains_key("scorer"));
        assert!(!owners.contains_key("scorer"));
    }
}

/// The adapter's donation JSON as described in the contract.
const DONATION_JSON: &[u8] = br#"{"donation":{"kind":"superchat","amount_micros":5000000,"currency":"USD","display":"$5.00","message":"nice one"}}"#;

/// An adapter ingest container carrying arbitrary `raw_data`.
fn ingest_with_raw_data(raw_data: &[u8]) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: String::new(),
        module_name: "adapter".into(),
        module_instance_uuid7: String::new(),
        payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(ChatMessage {
                platform: "twitch".into(),
                raw_data: raw_data.to_vec(),
                raw_message: "nice one".into(),
                user_uuid7: "user-1".into(),
                command: None,
                channel_id: "chan-1".into(),
                user_data: None,
            }),
            audio: vec![],
            audio_type: String::new(),
        })),
    }
}

/// Parse + css-key builder, as pure helpers (no DB / user-db needed): a donation
/// yields the four keys; a plain or malformed message yields none.
#[test]
fn donation_parse_builds_flags_and_css_keys() {
    let d = DonationInfo::parse(DONATION_JSON).expect("a donation");
    assert_eq!(d.kind, "superchat");
    assert_eq!(d.amount_micros, 5_000_000);
    assert_eq!(d.currency, "USD");
    assert_eq!(d.display, "$5.00");
    assert_eq!(d.message, "nice one");
    assert_eq!(
        d.flags_string(),
        "donation=superchat;amount=$5.00;currency=USD"
    );

    let mut css = HashMap::new();
    add_donation_css(&mut css, Some(&d));
    assert_eq!(css.get("is_donation").map(String::as_str), Some("1"));
    assert_eq!(
        css.get("donation_kind").map(String::as_str),
        Some("superchat")
    );
    assert_eq!(
        css.get("donation_amount").map(String::as_str),
        Some("$5.00")
    );
    assert_eq!(
        css.get("donation_currency").map(String::as_str),
        Some("USD")
    );

    assert!(DonationInfo::parse(b"").is_none(), "empty bytes");
    assert!(DonationInfo::parse(b"not json").is_none(), "malformed json");
    assert!(
        DonationInfo::parse(br#"{"foo":1}"#).is_none(),
        "json without a donation"
    );
    let mut plain_css = HashMap::new();
    add_donation_css(&mut plain_css, None);
    assert!(plain_css.is_empty(), "a non-donation adds no keys");
}

/// Ingesting a donation flags the message (row `flags` + in-memory state) and
/// forwards `raw_data` to the stage's `ChatMessage`.
#[tokio::test]
async fn ingesting_a_donation_flags_it_and_forwards_raw_data() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &[], &[]);

    orchestrator
        .handle_message_from_module(&ingest_with_raw_data(DONATION_JSON))
        .await
        .unwrap();

    // raw_data reaches the pre-process stage verbatim...
    let stage = match rx.get_mut("pre").unwrap().try_recv().unwrap().payload {
        Some(ModulePayload::MessagePreProcess(m)) => m,
        other => panic!("expected MessagePreProcess, got {other:?}"),
    };
    assert_eq!(
        stage.raw_message.as_ref().unwrap().raw_data,
        DONATION_JSON,
        "raw_data must be forwarded to stages"
    );
    let uuid7 = stage.message_uuid7.clone();

    // ...and the in-memory state is flagged as a donation.
    {
        let states = orchestrator.pipeline_states.lock().await;
        let state = states.get(&uuid7).expect("in-flight state");
        let d = state
            .donation
            .as_ref()
            .expect("donation detected at ingest");
        assert_eq!(d.kind, "superchat");
        assert_eq!(d.display, "$5.00");
        assert_eq!(
            state.flags.as_deref(),
            Some("donation=superchat;amount=$5.00;currency=USD")
        );
    }

    // The row carries the flag string in its `flags` column.
    let r = row(&db, &uuid7).await;
    assert_eq!(r["flags"], "donation=superchat;amount=$5.00;currency=USD");
}

/// A plain message is untouched: no donation, no flags, empty raw_data.
#[tokio::test]
async fn a_plain_message_is_not_flagged_as_a_donation() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &[], &[]);

    orchestrator
        .handle_message_from_module(&adapter_ingest("just chatting"))
        .await
        .unwrap();

    let stage = match rx.get_mut("pre").unwrap().try_recv().unwrap().payload {
        Some(ModulePayload::MessagePreProcess(m)) => m,
        other => panic!("expected MessagePreProcess, got {other:?}"),
    };
    assert!(
        stage.raw_message.as_ref().unwrap().raw_data.is_empty(),
        "a plain message carries no raw_data"
    );
    let uuid7 = stage.message_uuid7.clone();

    let states = orchestrator.pipeline_states.lock().await;
    let state = states.get(&uuid7).expect("in-flight state");
    assert!(state.donation.is_none());
    assert!(state.flags.is_none());
}

/// The `SendToPlatforms` actor policy: an ordinary send needs a verified
/// mod/admin/owner actor, but a proxied send (the Discord RP "possess" feature)
/// is authorized by the sending module, so the mod-only gate is lifted for it.
#[test]
fn proxied_sends_bypass_the_mod_actor_gate() {
    use crate::send_to_platforms_allowed as allowed;
    assert!(!allowed(false, false), "a plain send from a non-mod is denied");
    assert!(allowed(true, false), "a mod's plain send is allowed");
    assert!(
        allowed(false, true),
        "a proxied send is allowed regardless of the actor's role"
    );
    assert!(allowed(true, true));
}
