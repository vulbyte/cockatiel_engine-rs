//! Database semantics tests for crash recovery: marking mid-flight rows failed,
//! listing queued uuids, and loading a queued row's content. Also the
//! consolidated terminal write — the one statement that ends a message — and the
//! guarantee that it lands exactly the row the per-stage writes used to land.
//! Uses an in-memory timeline DB (`:memory:`).

use crate::database::{DatabaseConfig, DatabaseManager, PipelineOutcome, PipelineResult};
use std::path::PathBuf;

async fn test_db() -> DatabaseManager {
    let db = DatabaseManager::new(DatabaseConfig {
        local_path: PathBuf::from(":memory:"),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();
    // Setup counts its own round-trip; every test below measures from zero.
    db.take_round_trips();
    db
}

/// The row as the timeline API reports it (every column except uuid7 / raw_data
/// / data_blob).
async fn row(db: &DatabaseManager, uuid7: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    let json = db.get_event_as_json(uuid7).await.unwrap().unwrap();
    serde_json::from_str::<serde_json::Value>(&json).unwrap().as_object().unwrap().clone()
}

#[tokio::test]
async fn mark_processing_failed_and_get_queued_uuids_semantics() {
    let db = test_db().await;
    let u1 = b"u1-uuid-bytes-0000000001".to_vec();
    let u2 = b"u2-uuid-bytes-0000000002".to_vec();
    let u3 = b"u3-uuid-bytes-0000000003".to_vec();
    db.insert_event(&u1, 1, "twitch", &[], "hello one", "", "{}").await.unwrap();
    db.insert_event(&u2, 1, "kick", &[], "hello two", "", "{}").await.unwrap();
    db.insert_event(&u3, 1, "youtube", &[], "hello three", "", "{}").await.unwrap();

    // u1 + u3 are mid-flight; u2 never started.
    db.set_pipeline_status(&u1, "processing").await.unwrap();
    db.set_pipeline_status(&u3, "processing").await.unwrap();

    // Only u2 is still queued.
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![String::from_utf8(u2.clone()).unwrap()]);

    // Marking the mid-flight rows failed leaves the queued set untouched.
    // (The exact count turso reports is a scan counter, not strictly the number
    // of modified rows — the real contract is the resulting row state below.)
    let n = db.mark_all_processing_as_failed("interrupted by engine restart").await.unwrap();
    assert!(n >= 2);
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![String::from_utf8(u2.clone()).unwrap()]);

    // The failed rows carry the reason; the queued row still loads normally.
    let json = db.get_event_as_json(&u1).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["pipeline_status"], "failed");
    assert_eq!(v["error_message"], "interrupted by engine restart");
    assert!(db.load_queued_message(&u1).await.unwrap().is_none());
    assert!(db.load_queued_message(&u3).await.unwrap().is_none());

    let q = db.load_queued_message(&u2).await.unwrap().unwrap();
    assert_eq!(q.event_type, 1);
    assert_eq!(q.platform, "kick");
    assert_eq!(q.raw_message, "hello two");
    assert_eq!(q.command, "");
    assert_eq!(q.flags, "{}");
    assert_eq!(q.user_uuid7, "");
}

#[tokio::test]
async fn get_queued_uuids_is_ordered_ascending() {
    let db = test_db().await;
    // "a-..." < "b-..." byte-wise → the earlier-uuid row sorts first.
    let a = b"a-queued-row".to_vec();
    let b = b"b-queued-row".to_vec();
    db.insert_event(&b, 1, "twitch", &[], "bee", "", "{}").await.unwrap();
    db.insert_event(&a, 1, "twitch", &[], "aye", "", "{}").await.unwrap();
    assert_eq!(
        db.get_queued_uuids().await.unwrap(),
        vec![String::from_utf8(a).unwrap(), String::from_utf8(b).unwrap()]
    );
}

/// sync_to_remote must produce a portable backup file that survives repeated
/// calls, and the backup must contain the inserted event when re-opened as a
/// fresh DatabaseManager. Also guards the destroy-then-rename regression: a
/// second sync over an existing backup must not clobber it.
#[tokio::test]
async fn sync_to_remote_writes_a_consistent_reusable_backup() {
    let dir = std::env::temp_dir().join(format!("cockatiel-backup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let local_path = dir.join("cockatiel_data_test.db");
    let backup_path = dir.join("cockatiel_backup_test.db");

    let db = DatabaseManager::new(DatabaseConfig {
        local_path: local_path.clone(),
        remote_url: Some(backup_path.to_string_lossy().into_owned()),
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    db.initialize().await.unwrap();

    let uuid = b"sync-backup-uuid-000001".to_vec();
    db.insert_event(&uuid, 1, "twitch", &[], "backup me", "!test", "{}").await.unwrap();

    // First sync creates the backup; a second sync over it must succeed and
    // leave the file present (no destroy-then-rename window).
    assert_eq!(db.sync_to_remote().await.unwrap(), 1);
    assert!(backup_path.exists(), "backup file must exist after first sync");
    assert_eq!(db.sync_to_remote().await.unwrap(), 1);
    assert!(backup_path.exists(), "backup file must survive a second sync");

    // The backup is a real DB: re-open it as a fresh manager and find the event.
    let restored = DatabaseManager::new(DatabaseConfig {
        local_path: backup_path.clone(),
        remote_url: None,
        sync_interval_secs: 15,
        local_target_mb: 50,
    });
    restored.initialize().await.unwrap();
    let json = restored.get_event_as_json(&uuid).await.unwrap().expect("backup must contain the synced event");
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["raw_message"], "backup me");
    assert_eq!(v["command"], "!test");

    std::fs::remove_dir_all(&dir).ok();
}

// ── The consolidated terminal write ─────────────────────────────────────
//
// The engine used to rewrite the same row over and over while a message moved
// down the chain: `set_pipeline_status('processing')` at the start, then
// `set_processed_message` / `set_audio` / `update_stage_completed` per stage,
// then a completion tail. These tests pin the replacement — ONE write at the end
// — to the row the old sequence produced, column for column.

/// The per-stage writes, exactly as the pipeline issued them for one message
/// that renders audio at pre-process and again at in-process. Returns the
/// statements it cost, so the reduction is measured with the same instrument
/// that measures the new path.
async fn run_the_old_per_stage_writes(db: &DatabaseManager, uuid7: &[u8]) -> usize {
    let before = db.take_round_trips();
    // ingest
    db.insert_event(uuid7, 1, "twitch", &[], "raw text", "", "{}").await.unwrap();
    db.set_user_uuid(uuid7, "user-1").await.unwrap();
    // the pipeline starts
    db.set_pipeline_status(uuid7, "processing").await.unwrap();
    // pre-process: module rewrote the text and rendered audio
    db.set_processed_message(uuid7, "pre text").await.unwrap();
    db.set_audio(uuid7, "audio/mpeg", b"PRE-AUDIO").await.unwrap();
    db.update_stage_completed(uuid7, "pre_process").await.unwrap();
    // in-process: another module rewrote the text and re-rendered as wav
    db.set_processed_message(uuid7, "in text").await.unwrap();
    db.set_audio(uuid7, "audio/wav", b"IN-AUDIO").await.unwrap();
    db.update_stage_completed(uuid7, "in_process").await.unwrap();
    // post-process, then the completion tail
    db.set_processed_message(uuid7, "final text").await.unwrap();
    db.set_processed_message(uuid7, "final text").await.unwrap();
    db.update_stage_completed(uuid7, "post_process").await.unwrap();
    db.update_stage_completed(uuid7, "persisted").await.unwrap();
    db.set_pipeline_status(uuid7, "complete").await.unwrap();
    db.take_round_trips() - before
}

/// The replacement: the same message, with the per-stage writes' effects
/// accumulated in memory and emitted once. Returns the statements it cost.
async fn run_the_consolidated_write(db: &DatabaseManager, uuid7: &[u8]) -> usize {
    let before = db.take_round_trips();
    db.insert_event(uuid7, 1, "twitch", &[], "raw text", "", "{}").await.unwrap();
    db.set_user_uuid(uuid7, "user-1").await.unwrap();
    db.write_terminal_outcome(
        uuid7,
        &PipelineOutcome::Complete,
        &PipelineResult {
            processed_message: Some("final text".to_string()),
            // captured in memory as each stage's last module acked
            pre_process_completed_at: Some(1_000),
            in_process_completed_at: Some(2_000),
            // the LAST audio rendered, plus every content-type marker the
            // per-stage set_audio calls appended to `flags`
            audio: Some(("audio/wav".to_string(), b"IN-AUDIO".to_vec())),
            audio_type_markers: vec![
                "audio_type=audio/mpeg".to_string(),
                "audio_type=audio/wav".to_string(),
            ],
        },
    )
    .await
    .unwrap();
    db.take_round_trips() - before
}

/// The load-bearing invariant: after a message completes, the row must contain
/// the data it contains today. Run the old sequence and the consolidated write
/// against two rows and diff every column.
#[tokio::test]
async fn the_consolidated_write_lands_the_row_the_per_stage_writes_landed() {
    let db = test_db().await;
    let old_uuid = b"consolidated-old-path-0001".to_vec();
    let new_uuid = b"consolidated-new-path-0001".to_vec();

    run_the_old_per_stage_writes(&db, &old_uuid).await;
    run_the_consolidated_write(&db, &new_uuid).await;

    let old = row(&db, &old_uuid).await;
    let new = row(&db, &new_uuid).await;

    // Everything except the four stage timestamps, which are wall-clock values
    // taken at different moments by construction: compare them for "populated
    // and in pipeline order" below instead of for equality.
    for column in [
        "schema_version",
        "event_type",
        "platform",
        "raw_message",
        "command",
        "flags",
        "user_uuid7",
        "processed_message",
        "error_message",
        "pipeline_status",
        "synced_at",
    ] {
        assert_eq!(
            old[column], new[column],
            "column '{}' differs between the per-stage writes and the consolidated write",
            column
        );
    }

    // The values that matter, spelled out — a green diff above must not be the
    // result of both sides being empty.
    assert_eq!(old["pipeline_status"], "complete");
    assert_eq!(new["pipeline_status"], "complete");
    assert_eq!(old["processed_message"], "final text");
    assert_eq!(new["processed_message"], "final text");
    assert_eq!(old["user_uuid7"], "user-1");
    assert_eq!(new["user_uuid7"], "user-1");
    // Audio content type rides in `flags`; two stages rendered audio, so the
    // string carries BOTH markers, in the order they were appended.
    assert_eq!(old["flags"], "{},audio_type=audio/mpeg,audio_type=audio/wav");
    assert_eq!(new["flags"], old["flags"]);
    assert!(old["error_message"].is_null() && new["error_message"].is_null());

    // Every stage column is populated on both, in order.
    for (r, label) in [(&old, "per-stage writes"), (&new, "consolidated write")] {
        let pre = r["pre_process_completed_at"].as_i64().expect("pre_process_completed_at");
        let inp = r["in_process_completed_at"].as_i64().expect("in_process_completed_at");
        let post = r["post_process_completed_at"].as_i64().expect("post_process_completed_at");
        let persisted = r["persisted_at"].as_i64().expect("persisted_at");
        assert!(pre <= inp && inp <= post && post <= persisted, "{}: stage timestamps are out of pipeline order", label);
    }
    // The two in-memory stamps must survive the write as captured, not be
    // replaced by the write's own clock.
    assert_eq!(new["pre_process_completed_at"], 1_000);
    assert_eq!(new["in_process_completed_at"], 2_000);

    // The audio bytes live in `data_blob`, which `get_event_as_json` does not
    // project — read it back the way the web UI does. Note the content type is
    // the FIRST marker and the bytes are the LAST write: that mismatch is the
    // per-stage model's, and the consolidation reproduces it rather than
    // "fixing" it, because the point of this test is that the row is unchanged.
    assert_eq!(db.get_audio(&old_uuid).await.unwrap(), db.get_audio(&new_uuid).await.unwrap());
    assert_eq!(db.get_audio(&new_uuid).await.unwrap(), Some(("audio/mpeg".to_string(), b"IN-AUDIO".to_vec())));
}

/// The point of the exercise, measured rather than asserted: the same message
/// costs 14 database calls (16 SQL statements — each `set_audio` is a SELECT to
/// read the flags back plus an UPDATE) under the per-stage writes, and 3 calls /
/// 3 statements now.
#[tokio::test]
async fn the_consolidated_write_replaces_fourteen_database_calls_with_three() {
    const OLD_CALLS: usize = 14;
    const NEW_CALLS: usize = 3;

    let db = test_db().await;
    let old_calls = run_the_old_per_stage_writes(&db, b"count-old-path-000001".as_ref()).await;
    let new_calls = run_the_consolidated_write(&db, b"count-new-path-000001".as_ref()).await;

    assert_eq!(old_calls, OLD_CALLS, "the per-stage write sequence changed shape");
    assert_eq!(new_calls, NEW_CALLS, "the consolidated path must be the ingest writes plus ONE terminal write");
    assert!(new_calls * 4 < old_calls, "expected a large reduction, got {} -> {}", old_calls, new_calls);
}

/// A write with no audio must not wipe the audio the row already has: the
/// terminal write only ever sets the columns the run actually produced.
#[tokio::test]
async fn a_final_write_with_no_audio_leaves_existing_audio_intact() {
    let db = test_db().await;
    let uuid = b"no-clobber-audio-0000001".to_vec();
    db.insert_event(&uuid, 1, "twitch", &[], "raw", "", "{}").await.unwrap();
    db.set_audio(&uuid, "audio/wav", b"KEEP-ME").await.unwrap();

    db.write_terminal_outcome(
        &uuid,
        &PipelineOutcome::Complete,
        &PipelineResult {
            processed_message: Some("no audio in this run".to_string()),
            ..PipelineResult::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(db.get_audio(&uuid).await.unwrap(), Some(("audio/wav".to_string(), b"KEEP-ME".to_vec())));
    let r = row(&db, &uuid).await;
    assert_eq!(r["flags"], "{},audio_type=audio/wav", "the audio marker must not be rewritten either");
    assert_eq!(r["processed_message"], "no audio in this run");

    // Same for a row whose flags column is empty: the marker is appended with no
    // leading separator, exactly as `set_audio` did.
    let bare = b"no-clobber-bare-000000001".to_vec();
    db.insert_event(&bare, 1, "twitch", &[], "raw", "", "").await.unwrap();
    db.write_terminal_outcome(
        &bare,
        &PipelineOutcome::Complete,
        &PipelineResult {
            audio: Some(("audio/mpeg".to_string(), b"BYTES".to_vec())),
            audio_type_markers: vec!["audio_type=audio/mpeg".to_string()],
            ..PipelineResult::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(row(&db, &bare).await["flags"], "audio_type=audio/mpeg");
    assert_eq!(db.get_audio(&bare).await.unwrap(), Some(("audio/mpeg".to_string(), b"BYTES".to_vec())));
}

/// Every terminal outcome lands its own status — and only a completed message
/// gets the "the whole pipeline ran" columns.
#[tokio::test]
async fn each_terminal_outcome_lands_its_own_status() {
    let db = test_db().await;
    let cases: Vec<(&str, PipelineOutcome)> = vec![
        ("outcome-complete-000001", PipelineOutcome::Complete),
        (
            "outcome-failed-0000001",
            PipelineOutcome::Failed("message errored".to_string()),
        ),
        (
            "outcome-dropped-000001",
            PipelineOutcome::Dropped("abandoned by module 'mid'".to_string()),
        ),
        (
            "outcome-audit-0000001",
            PipelineOutcome::Audit("wrong language".to_string()),
        ),
        ("outcome-held-00000001", PipelineOutcome::AuditHeld),
    ];

    for (uuid, outcome) in cases {
        let uuid = uuid.as_bytes().to_vec();
        db.insert_event(&uuid, 1, "twitch", &[], "raw", "", "{}").await.unwrap();
        // Hold first for the AuditHeld case: a row that is already held stays
        // held, and the accumulated result still lands on it.
        if matches!(outcome, PipelineOutcome::AuditHeld) {
            db.mark_audit(&uuid, "wrong language").await.unwrap();
        }
        db.write_terminal_outcome(
            &uuid,
            &outcome,
            &PipelineResult {
                processed_message: Some("ran".to_string()),
                pre_process_completed_at: Some(1),
                in_process_completed_at: Some(2),
                ..PipelineResult::default()
            },
        )
        .await
        .unwrap();

        let r = row(&db, &uuid).await;
        let completed = matches!(outcome, PipelineOutcome::Complete);
        // `AuditHeld` writes no status at all: the row keeps the hold it already
        // has, which `mark_audit` put there.
        let expected_status = match &outcome {
            PipelineOutcome::AuditHeld => "audit",
            other => other.status().unwrap(),
        };
        assert_eq!(r["pipeline_status"], expected_status, "{:?}", outcome);
        if completed {
            assert!(r["post_process_completed_at"].is_number(), "a completed message must record post_process_completed_at");
            assert!(r["persisted_at"].is_number(), "a completed message must record persisted_at");
        } else {
            assert!(
                r["post_process_completed_at"].is_null() && r["persisted_at"].is_null(),
                "{:?}: a message that never completed must not claim it was persisted",
                outcome
            );
        }
        // What the run produced lands on EVERY outcome — that is what the
        // per-stage writes had already mirrored by the time a failure was
        // recorded.
        assert_eq!(r["processed_message"], "ran", "{:?}", outcome);
        assert_eq!(r["pre_process_completed_at"], 1, "{:?}", outcome);
        assert_eq!(r["in_process_completed_at"], 2, "{:?}", outcome);
        match &outcome {
            PipelineOutcome::Failed(e) | PipelineOutcome::Dropped(e) => assert_eq!(r["error_message"], *e),
            _ => assert!(r["error_message"].is_null(), "{:?}", outcome),
        }
        if let PipelineOutcome::Audit(reason) = &outcome {
            assert_eq!(r["flags"], *reason, "a hold stores its reason in flags");
        }
    }
}

/// The hold is the operator's queue item: a pipeline that finishes a held
/// message must not publish it, and must not eat the reason either.
#[tokio::test]
async fn an_audit_held_row_is_never_flipped_to_complete() {
    let db = test_db().await;
    let uuid = b"audit-not-completed-001".to_vec();
    db.insert_event(&uuid, 1, "twitch", &[], "raw", "", "{}").await.unwrap();
    db.mark_audit(&uuid, "needs a moderator").await.unwrap();

    // The pipeline finishing the message tries to complete it.
    db.write_terminal_outcome(
        &uuid,
        &PipelineOutcome::Complete,
        &PipelineResult {
            processed_message: Some("finished anyway".to_string()),
            pre_process_completed_at: Some(7),
            ..PipelineResult::default()
        },
    )
    .await
    .unwrap();

    let r = row(&db, &uuid).await;
    assert_eq!(r["pipeline_status"], "audit", "a held message must not be completed behind the operator's back");
    assert_eq!(r["flags"], "needs a moderator", "the hold reason must survive");
    assert!(
        r["persisted_at"].is_null(),
        "a held message is not persisted until a moderator releases it"
    );

    // The path the pipeline actually takes for an already-held row: land the
    // accumulated result, leave the hold (status AND reason) alone.
    db.write_terminal_outcome(
        &uuid,
        &PipelineOutcome::AuditHeld,
        &PipelineResult {
            processed_message: Some("what the run produced".to_string()),
            pre_process_completed_at: Some(7),
            in_process_completed_at: Some(8),
            ..PipelineResult::default()
        },
    )
    .await
    .unwrap();

    let r = row(&db, &uuid).await;
    assert_eq!(r["pipeline_status"], "audit");
    assert_eq!(r["flags"], "needs a moderator");
    assert_eq!(r["processed_message"], "what the run produced");
    assert_eq!(r["pre_process_completed_at"], 7);
    assert_eq!(r["in_process_completed_at"], 8);
    assert!(r["post_process_completed_at"].is_null() && r["persisted_at"].is_null());
    // ...and it is still an operator-queued item.
    assert_eq!(db.list_audit(10, 0).await.unwrap().len(), 1);
}

/// The startup sweep, under the consolidated model. Nothing the engine writes is
/// 'processing' any more, so the sweep must be inert for its own rows and only
/// act on rows stranded by an older build.
#[tokio::test]
async fn the_processing_sweep_cannot_touch_a_row_the_pipeline_owns() {
    let db = test_db().await;
    let live = b"sweep-live-row-000000001".to_vec();
    let legacy = b"sweep-legacy-row-00000001".to_vec();
    db.insert_event(&live, 1, "twitch", &[], "in flight", "", "{}").await.unwrap();
    db.insert_event(&legacy, 1, "twitch", &[], "stranded by an old build", "", "{}").await.unwrap();
    // An in-flight message is 'queued' now (the pipeline writes no intermediate
    // status); only an older engine build could leave a row 'processing'.
    db.set_pipeline_status(&legacy, "processing").await.unwrap();

    let n = db.mark_all_processing_as_failed("interrupted by engine restart").await.unwrap();
    assert!(n >= 1);

    // The stranded row is failed, with the reason.
    let r = row(&db, &legacy).await;
    assert_eq!(r["pipeline_status"], "failed");
    assert_eq!(r["error_message"], "interrupted by engine restart");

    // The row the current pipeline owns is untouched — and still drainable.
    let r = row(&db, &live).await;
    assert_eq!(r["pipeline_status"], "queued", "the sweep must not fail a message the pipeline can still finish");
    assert!(r["error_message"].is_null());
    assert_eq!(db.get_queued_uuids().await.unwrap(), vec![String::from_utf8(live.clone()).unwrap()]);
    assert!(db.load_queued_message(&live).await.unwrap().is_some());
}

/// A terminal write with nothing to say must be a no-op, not a malformed
/// statement: `AuditHeld` on a row with no accumulated result writes no column
/// at all (the hold is already in place).
#[tokio::test]
async fn a_terminal_write_with_nothing_to_say_is_a_no_op() {
    let db = test_db().await;
    let uuid = b"empty-terminal-write-00001".to_vec();
    db.insert_event(&uuid, 1, "twitch", &[], "raw", "", "{}").await.unwrap();
    db.mark_audit(&uuid, "held").await.unwrap();

    db.write_terminal_outcome(&uuid, &PipelineOutcome::AuditHeld, &PipelineResult::default())
        .await
        .unwrap();

    let r = row(&db, &uuid).await;
    assert_eq!(r["pipeline_status"], "audit");
    assert_eq!(r["flags"], "held");
    assert!(r["processed_message"].is_null());
}

#[tokio::test]
async fn query_timeline_filters_by_prefix() {
    let db = test_db().await;
    for i in 0..5 {
        let u = format!("q-uuid-{:026}", i).into_bytes();
        db.insert_event(&u, 1, "test", &[], &format!("screening-flood-x {}", i), "", "{}").await.unwrap();
    }
    let json = db
        .query_timeline(None, Some(1), Some("test"), None, None, Some("screening-flood-x"), None, None, 500, 0)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 5);
}
