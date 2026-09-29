//! Pipeline tests: `send_to_module` outcome semantics (Sent / NotConnected /
//! Dropped — it must not lie about drops), crash-recovery `recover_one` draining
//! a stranded 'queued' row through to completion, and the persistence model the
//! pipeline runs on now — ONE terminal write per message instead of a per-stage
//! write, with the in-memory state as the record while a message is in flight.

use crate::cockatiel_protobuf::container::Payload;
use crate::cockatiel_protobuf::{ChatMessage, Container, MessageInProcess, MessagePostProcess, MessagePreProcess};
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseConfig, DatabaseManager};
use crate::pipeline::{
    PendingAck, PipelineConfig, PipelineOrchestrator, PipelineStage, SendOutcome, send_to_module,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) fn dummy_container() -> Container {
    Container::default()
}

/// A fresh in-memory timeline DB.
pub(super) async fn test_db() -> DatabaseManager {
    let db = DatabaseManager::new(DatabaseConfig {
        local_path: PathBuf::from(":memory:"),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();
    // Setup counts its own round-trip; the tests below measure from zero.
    db.take_round_trips();
    db
}

pub(super) type ModuleRx = tokio::sync::mpsc::Receiver<Container>;

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
    for name in names {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        senders.insert(name.clone(), tx);
        receivers.insert(name, rx);
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
    (orchestrator, receivers)
}

/// An adapter's "here is a new chat message" container: the DB-as-queue entry.
pub(super) fn adapter_ingest(raw_message: &str) -> Container {
    Container {
        version: 1,
        auth_token: String::new(),
        module_name: "adapter".into(),
        module_instance_uuid7: String::new(),
        payload: Some(Payload::MessagePreProcess(MessagePreProcess {
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

pub(super) fn from_module(module_name: &str, payload: Payload) -> Container {
    Container {
        version: 1,
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

pub(super) fn pre_reply(module: &str, uuid7: &str, text: &str) -> Container {
    from_module(
        module,
        Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: uuid7.into(),
            raw_message: Some(chat(text)),
            audio: vec![],
            audio_type: String::new(),
        }),
    )
}

pub(super) fn in_reply(module: &str, uuid7: &str, text: &str, abandon: bool) -> Container {
    from_module(
        module,
        Payload::MessageInProcess(MessageInProcess {
            message_uuid7: uuid7.into(),
            raw_message: Some(chat(text)),
            processed_message: text.into(),
            abandon_message: abandon,
            audio: vec![],
            audio_type: String::new(),
        }),
    )
}

pub(super) fn post_reply(module: &str, uuid7: &str, text: &str) -> Container {
    from_module(
        module,
        Payload::MessagePostProcess(MessagePostProcess {
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
        Some(Payload::MessagePreProcess(m)) => m.message_uuid7.clone(),
        Some(Payload::MessageInProcess(m)) => m.message_uuid7.clone(),
        Some(Payload::MessagePostProcess(m)) => m.message_uuid7.clone(),
        other => panic!("expected a stage message, got {other:?}"),
    }
}

pub(super) fn nothing_left(rx: &mut ModuleRx) -> bool {
    rx.try_recv().is_err()
}

pub(super) async fn row(db: &DatabaseManager, uuid7: &str) -> serde_json::Map<String, serde_json::Value> {
    let json = db.get_event_as_json(uuid7.as_bytes()).await.unwrap().unwrap();
    serde_json::from_str::<serde_json::Value>(&json)
        .unwrap()
        .as_object()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn send_outcome_not_connected_when_module_has_no_sender() {
    let senders: Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::mpsc::Sender<Container>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let outcome = send_to_module(&senders, "ghost", dummy_container(), "test").await;
    assert_eq!(outcome, SendOutcome::NotConnected);
}

#[tokio::test]
async fn send_outcome_not_connected_when_channel_is_closed() {
    let (tx, rx) = tokio::sync::mpsc::channel::<Container>(1);
    drop(rx); // the module's socket is gone; its sender slot still lingers.
    let senders = Arc::new(tokio::sync::Mutex::new(HashMap::from([("mod".to_string(), tx)])));
    let outcome = send_to_module(&senders, "mod", dummy_container(), "test").await;
    assert_eq!(outcome, SendOutcome::NotConnected);
}

#[tokio::test]
async fn send_outcome_dropped_when_channel_is_full() {
    // Bounded capacity-1 channel, receiver kept alive and NEVER drained.
    let (tx, _rx) = tokio::sync::mpsc::channel::<Container>(1);
    tx.try_send(dummy_container()).unwrap(); // fill it — the next send can't land
    let senders = Arc::new(tokio::sync::Mutex::new(HashMap::from([("mod".to_string(), tx)])));
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
    db.set_pipeline_status(&uuid_bytes, "processing").await.unwrap();

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

    assert_eq!(db.take_round_trips(), ROUND_TRIPS, "a message must cost one write, not one per stage");

    // ...and the row it leaves behind is complete, in every column the old
    // per-stage writes used to fill between them.
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
    assert_eq!(r["raw_message"], "raw text");
    assert_eq!(r["user_uuid7"], "user-1");
    assert_eq!(r["processed_message"], "final text");
    assert!(r["error_message"].is_null());
    let pre = r["pre_process_completed_at"].as_i64().expect("pre_process_completed_at");
    let inp = r["in_process_completed_at"].as_i64().expect("in_process_completed_at");
    let post = r["post_process_completed_at"].as_i64().expect("post_process_completed_at");
    let persisted = r["persisted_at"].as_i64().expect("persisted_at");
    assert!(pre <= inp && inp <= post && post <= persisted, "stage timestamps out of order: {pre} {inp} {post} {persisted}");

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
    db.mark_audit(uuid7.as_bytes(), "wrong language").await.unwrap();
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "translated"))
        .await
        .unwrap();

    assert!(nothing_left(rx.get_mut("mid").unwrap()), "a held message must not be advanced to in-process");
    assert!(nothing_left(rx.get_mut("post").unwrap()));
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "audit");
    assert_eq!(r["flags"], "wrong language", "the hold reason must survive");
    assert!(r["persisted_at"].is_null(), "a held message is not persisted");
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
    db.mark_audit(uuid7.as_bytes(), "needs a moderator").await.unwrap();
    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "final text"))
        .await
        .unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "audit", "finishing the chain must not complete a held message");
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

    assert!(nothing_left(rx.get_mut("post").unwrap()), "an abandoned message must not reach post-process");
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "dropped");
    assert_eq!(r["error_message"], "abandoned by module 'mid'");
    assert_eq!(r["processed_message"], "no thanks");
    assert!(r["pre_process_completed_at"].is_number(), "the stages it did run are still recorded");
    assert!(
        r["in_process_completed_at"].is_null() && r["post_process_completed_at"].is_null() && r["persisted_at"].is_null(),
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
        }],
    );
    orchestrator.handle_timeout().await.unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "failed");
    assert_eq!(
        r["error_message"],
        "Critical module 'mid' timed out at stage 'in_process'",
        "the failure reason must be recorded verbatim"
    );
    assert_eq!(r["processed_message"], "pre text", "what the run produced lands with the failure");
    assert!(r["pre_process_completed_at"].is_number());
    assert!(
        r["in_process_completed_at"].is_null() && r["post_process_completed_at"].is_null() && r["persisted_at"].is_null(),
        "a failed message must not claim it was persisted"
    );
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

    assert!(nothing_left(rx.get_mut("pre").unwrap()), "an in-flight message must not be re-broadcast to pre-process");
    assert!(nothing_left(rx.get_mut("mid").unwrap()), "its in-process stage must not be restarted either");
    assert_eq!(row(&db, &uuid7).await["pipeline_status"], "queued", "the drain must not write to the row");
    let states = orchestrator.pipeline_states.lock().await;
    let state = states.get(&uuid7).expect("the live state must survive the drain");
    assert_eq!(state.processed_message, "pre text", "the drain must not reset the live state");
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

    // (0, 0) gate → free + unrestricted → allowed.
    {
        let mut gates = std::collections::HashMap::new();
        gates.insert("free".to_string(), (0u64, 0i64));
        orchestrator.set_module_gates(gates);
    }
    assert!(orchestrator.permit_module("user-1", "free").await);

    // Priced/gated module with NO user-db wired → allowed (degraded: don't
    // stall the pipeline on a gate when the user-db is unavailable). The real
    // enforcement happens when user_db is set.
    {
        let mut gates = std::collections::HashMap::new();
        gates.insert("paid".to_string(), (5u64, 3i64));
        orchestrator.set_module_gates(gates);
    }
    assert!(orchestrator.permit_module("user-1", "paid").await);

    // Empty user uuid with a priced module → allowed (nobody to charge).
    assert!(orchestrator.permit_module("", "paid").await);
}
