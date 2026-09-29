//! Ack contract tests for `AckTracker` (src/pipeline.rs): a pre-process stage
//! with N tracked recipients must only advance once ALL N have acked — a
//! non-acking module keeps the uuid pending. Pure in-memory logic, no DB.

use crate::pipeline::{AckTracker, PendingAck};
use std::time::{Duration, Instant};

#[test]
fn stage_advances_only_when_all_recipients_ack() {
    let mut tracker = AckTracker::new();
    let uuid = "test-msg-uuid".to_string();
    tracker.track(uuid.clone(), "preprocess".to_string(), "module-a".to_string(), 3000);
    tracker.track(uuid.clone(), "preprocess".to_string(), "module-b".to_string(), 3000);

    assert!(tracker.has_pending(&uuid));

    // One recipient acks — the stage must NOT advance while module-b is pending.
    let stage = tracker.ack_module(&uuid, "module-a");
    assert_eq!(stage.map(|(s, _)| s), Some("preprocess".to_string()));
    assert!(
        tracker.has_pending(&uuid),
        "must not advance early while module-b has not acked"
    );

    // All recipients ack → nothing pending → the stage may advance.
    let stage = tracker.ack_module(&uuid, "module-b");
    assert_eq!(stage.map(|(s, _)| s), Some("preprocess".to_string()));
    assert!(!tracker.has_pending(&uuid));
}

#[test]
fn ack_module_ignores_unknown_recipients_and_is_idempotent() {
    let mut tracker = AckTracker::new();
    tracker.track("u2".to_string(), "preprocess".to_string(), "module-a".to_string(), 3000);

    // A module that was never tracked for this message does not clear it.
    assert_eq!(tracker.ack_module("u2", "module-other"), None);
    assert!(tracker.has_pending("u2"));

    assert_eq!(tracker.ack_module("u2", "module-a").map(|(s, _)| s), Some("preprocess".to_string()));
    assert!(!tracker.has_pending("u2"));

    // Acking again after removal is a no-op.
    assert_eq!(tracker.ack_module("u2", "module-a"), None);
}

#[test]
fn check_timeouts_yields_only_elapsed_entries() {
    let mut tracker = AckTracker::new();
    let elapsed_sent = Instant::now() - Duration::from_secs(10);
    let fresh_sent = Instant::now();

    tracker.inject(
        "u-expired".to_string(),
        vec![PendingAck {
            uuid7: "u-expired".to_string(),
            stage: "preprocess".to_string(),
            module_name: "module-a".to_string(),
            sent_at: elapsed_sent,
            timeout: Duration::from_millis(3000),
        }],
    );
    tracker.inject(
        "u-fresh".to_string(),
        vec![PendingAck {
            uuid7: "u-fresh".to_string(),
            stage: "preprocess".to_string(),
            module_name: "module-b".to_string(),
            sent_at: fresh_sent,
            timeout: Duration::from_millis(3000),
        }],
    );

    let timed_out = tracker.check_timeouts();
    assert_eq!(timed_out.len(), 1, "only the elapsed entry may time out");
    assert_eq!(timed_out[0].uuid7, "u-expired");

    // The expired uuid is dropped from pending; the fresh one remains pending.
    assert!(!tracker.has_pending("u-expired"));
    assert!(tracker.has_pending("u-fresh"));
}