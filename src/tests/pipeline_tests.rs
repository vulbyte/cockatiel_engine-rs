//! Pipeline tests: `send_to_module` outcome semantics (Sent / NotConnected /
//! Dropped — it must not lie about drops) and crash-recovery `recover_one`
//! draining a stranded 'queued' row through to completion.

use crate::cockatiel_protobuf::Container;
use crate::command_registry::CommandRegistry;
use crate::database::{DatabaseConfig, DatabaseManager};
use crate::pipeline::{PipelineConfig, PipelineOrchestrator, SendOutcome, send_to_module};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

fn dummy_container() -> Container {
    Container::default()
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
    // A live pipeline already claimed it (moved off 'queued').
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