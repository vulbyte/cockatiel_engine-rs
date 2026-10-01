//! Pause / resume tests, plus the crash-recovery drain's termination.
//!
//! Two properties are load-bearing here and neither is obvious from reading
//! the code:
//!
//! * **Pausing must not lose a message.** A message ingested while paused is
//!   written to the timeline and joins the held set; a message that was already
//!   mid-chain when the pause landed keeps its state and its pending acks, and
//!   the ack-timeout sweep does not run while paused. Paused time must not
//!   count against a message's ack budget either — that is what the mid-chain
//!   test below pins, and it is the one that turns a pause into data loss if it
//!   regresses.
//! * **The recovery drain must terminate.** It used to loop until no `'queued'`
//!   rows were left, which stopped being true the moment the pipeline stopped
//!   writing `'processing'`: an in-flight row is `'queued'` for its whole
//!   flight, so the loop re-selected it forever. Every drain assertion is
//!   bounded by a timeout so a regression fails instead of hanging the suite.

use super::pipeline_tests::{
    ModuleRx, adapter_ingest, in_reply, nothing_left, post_reply, pre_reply, row, take_broadcast,
    test_db, wired_orchestrator,
};
use crate::pipeline::PipelineOrchestrator;
use std::collections::HashMap;
use std::time::Duration;

/// A pipeline wired to real module channels, with a caller-chosen ack budget
/// and a caller-chosen pause state — the engine's boot state is one line in
/// `main.rs`, so a test that drives the "paused" half has to be able to ask for
/// the running half too (a message must be able to be in flight BEFORE a pause
/// lands).
async fn orchestrator_with(
    ack_timeout_ms: u64,
    paused: bool,
) -> (PipelineOrchestrator, HashMap<String, ModuleRx>) {
    let db = test_db().await;
    let (orchestrator, receivers) = wired_orchestrator(db, &["pre"], &["mid"], &["post"]);
    let mut cfg = orchestrator.config_snapshot().await;
    cfg.ack_timeout_ms = ack_timeout_ms;
    orchestrator.set_config(cfg).await;
    if paused {
        assert!(orchestrator.pause().await, "the orchestrator must be held here");
        assert!(orchestrator.is_paused().await);
    } else {
        assert!(!orchestrator.is_paused().await);
    }
    (orchestrator, receivers)
}

fn no_module_saw_anything(rx: &mut HashMap<String, ModuleRx>) {
    for stage in ["pre", "mid", "post"] {
        assert!(
            nothing_left(rx.get_mut(stage).expect("stage is wired")),
            "a paused engine must not dispatch to '{}'",
            stage
        );
    }
}

/// Wait for `count` broadcasts on `stage`, bounded.
///
/// The backlog release is deliberately a background task (a large one must not
/// stall the control surface's read loop — see `PipelineOrchestrator::resume`),
/// so the tests that drain a backlog wait for it rather than assuming it has
/// already run. The work is in-memory and bounded, so this cannot flake: it
/// either arrives within the deadline or the test fails.
async fn wait_for_broadcasts(
    rx: &mut ModuleRx,
    count: usize,
    what: &str,
) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut seen = Vec::new();
    while seen.len() < count {
        if let Ok(container) = rx.try_recv() {
            let uuid7 = match &container.payload {
                Some(crate::cockatiel_protobuf::container_for_module::Payload::MessagePreProcess(m)) => {
                    m.message_uuid7.clone()
                }
                Some(crate::cockatiel_protobuf::container_for_module::Payload::MessageInProcess(m)) => {
                    m.message_uuid7.clone()
                }
                Some(crate::cockatiel_protobuf::container_for_module::Payload::MessagePostProcess(m)) => {
                    m.message_uuid7.clone()
                }
                other => panic!("expected a stage message, got {other:?}"),
            };
            seen.push(uuid7);
            continue;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}: got {}/{}",
            seen.len(),
            count
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    seen
}

// ── A paused engine ingests, persists, and dispatches to nobody ─────────

/// The headline rule: a paused engine is not a dead engine. It accepts module
/// connections, parses commands and writes the timeline exactly as it normally
/// would — it just does not hand anything to a module.
#[tokio::test]
async fn a_message_ingested_while_paused_is_persisted_and_dispatched_to_nobody() {
    let (orchestrator, mut rx) = orchestrator_with(3000, true).await;

    orchestrator
        .handle_message_from_module(&adapter_ingest("held at the door"))
        .await
        .unwrap();

    no_module_saw_anything(&mut rx);

    // Nothing was lost: the row exists, with the message on it, 'queued'.
    let uuid7 = orchestrator
        .pipeline_states
        .lock()
        .await
        .keys()
        .next()
        .cloned()
        .expect("a paused message still gets a live in-memory state");
    let r = row(&orchestrator.db, &uuid7).await;
    assert_eq!(r["raw_message"], "held at the door");
    assert_eq!(r["pipeline_status"], "queued");
    assert_eq!(orchestrator.db.get_queued_uuids().await.unwrap(), vec![uuid7.clone()]);

    // It is held, not lost, and not even pending an ack from anyone.
    assert_eq!(orchestrator.held_count().await, 1);
    assert!(!orchestrator.ack_tracker.lock().await.has_pending(&uuid7));
    // The ack sweep is gated too — nothing to reap, but it must not run.
    orchestrator.handle_timeout().await.unwrap();
    assert_eq!(row(&orchestrator.db, &uuid7).await["pipeline_status"], "queued");
}

// ── Resume restores completely normal operation ───────────────────────

/// N messages accumulate while paused; after the resume every one of them is
/// dispatched, in the order it arrived, and every one reaches a terminal
/// outcome. Pausing is not queueing-and-dropping.
#[tokio::test]
async fn resume_drains_everything_that_accumulated_while_paused() {
    const MESSAGES: usize = 5;
    let (orchestrator, mut rx) = orchestrator_with(3000, true).await;

    for i in 0..MESSAGES {
        orchestrator
            .handle_message_from_module(&adapter_ingest(&format!("queued {}", i)))
            .await
            .unwrap();
    }
    no_module_saw_anything(&mut rx);
    let queued = orchestrator.db.get_queued_uuids().await.unwrap();
    assert_eq!(queued.len(), MESSAGES, "every paused message is persisted as 'queued'");
    assert_eq!(orchestrator.held_count().await, MESSAGES);

    let outcome = orchestrator.resume().await;
    assert!(outcome.was_paused);
    assert_eq!(outcome.resumed_messages, MESSAGES);
    assert!(outcome.errors.is_empty(), "a clean resume reports nothing: {outcome:?}");
    assert!(!orchestrator.is_paused().await);
    assert_eq!(
        orchestrator.held_count().await,
        0,
        "the held set is released by the resume, not left to accumulate"
    );

    // The pre-process fanout happened, and in arrival order. uuid7 is
    // time-ordered and the held set is a uuid7-ordered map, so "the order they
    // came in" is the order they go out — the same ordering the recovery drain
    // already relies on.
    let uuids = wait_for_broadcasts(rx.get_mut("pre").unwrap(), MESSAGES, "the drained backlog").await;
    assert_eq!(uuids, queued, "the accumulated queue must drain in uuid7 order");
    assert!(nothing_left(rx.get_mut("mid").unwrap()));
    assert!(nothing_left(rx.get_mut("post").unwrap()));

    // ...and from here it is completely normal operation: every message runs
    // its chain to a terminal outcome.
    for (i, uuid7) in uuids.iter().enumerate() {
        orchestrator
            .handle_message_from_module(&pre_reply("pre", uuid7, &format!("pre {}", i)))
            .await
            .unwrap();
        assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), *uuid7);
        orchestrator
            .handle_message_from_module(&in_reply("mid", uuid7, &format!("done {}", i), false))
            .await
            .unwrap();
        assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), *uuid7);
        orchestrator
            .handle_message_from_module(&post_reply("post", uuid7, &format!("done {}", i)))
            .await
            .unwrap();
    }
    for uuid7 in &uuids {
        let r = row(&orchestrator.db, uuid7).await;
        assert_eq!(r["pipeline_status"], "complete", "a drained message must finish, not linger");
        assert_eq!(r["processed_message"], format!("done {}", uuids.iter().position(|u| u == uuid7).unwrap()));
        assert!(r["persisted_at"].is_number());
    }
    assert!(orchestrator.pipeline_states.lock().await.is_empty());
}

/// A resume while the engine is already running is a no-op that says so, rather
/// than a second sweep-and-replay pass over messages that are mid-flight.
#[tokio::test]
async fn resuming_an_engine_that_is_not_paused_changes_nothing() {
    let (orchestrator, mut rx) = orchestrator_with(3000, false).await;
    orchestrator
        .handle_message_from_module(&adapter_ingest("running"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    let outcome = orchestrator.resume().await;
    assert!(!outcome.was_paused, "nothing was paused, so nothing was resumed");
    assert_eq!(outcome.resumed_messages, 0);
    assert!(nothing_left(rx.get_mut("mid").unwrap()), "a no-op resume must not re-dispatch");

    // The in-flight message is untouched and still completes.
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "done"))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("mid").unwrap()), uuid7);
    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "done", false))
        .await
        .unwrap();
    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "done"))
        .await
        .unwrap();
    assert_eq!(row(&orchestrator.db, &uuid7).await["pipeline_status"], "complete");
}

// ── THE rule: paused time does not count against a message ────────────

/// A message that was already past the entry gate when the pause landed — here,
/// sitting in 'mid' waiting for an ack — must survive a hold far longer than
/// its ack budget and still complete.
///
/// 'mid' is configured CRITICAL so that a reaped stage is a `failed` row rather
/// than a silent advance: without the paused-sweep gate and the clock rebase,
/// the first sweep either during the pause or immediately after the resume
/// fails this message, which is exactly the data loss the rule forbids.
#[tokio::test]
async fn a_message_mid_chain_when_the_pause_lands_still_completes_after_it() {
    const ACK_BUDGET_MS: u64 = 250;
    /// Two of these overshoot the budget more than twice over.
    const HOLD: Duration = Duration::from_millis(300);

    let (orchestrator, mut rx) = orchestrator_with(ACK_BUDGET_MS, false).await;
    let mut cfg = orchestrator.config_snapshot().await;
    cfg.critical_modules = vec!["mid".to_string()];
    orchestrator.set_config(cfg).await;

    // Running: the message reaches in-process and 'mid' takes it.
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
    assert!(orchestrator.ack_tracker.lock().await.has_pending(&uuid7));

    // The pause lands here.
    assert!(orchestrator.pause().await);

    // Held far past the ack budget, with the sweep running throughout — in
    // production it runs every 250ms, so this message is asked to time out
    // repeatedly.
    for _ in 0..2 {
        tokio::time::sleep(HOLD).await;
        orchestrator.handle_timeout().await.unwrap();
        let r = row(&orchestrator.db, &uuid7).await;
        assert_eq!(
            r["pipeline_status"], "queued",
            "a paused sweep must not fail the message it is holding"
        );
        assert!(r["error_message"].is_null());
    }
    assert!(
        orchestrator.ack_tracker.lock().await.has_pending(&uuid7),
        "the in-flight stage must still be waiting, not reaped"
    );
    assert!(nothing_left(rx.get_mut("post").unwrap()), "a held chain must not advance");
    assert_eq!(orchestrator.held_count().await, 0, "an in-flight message is not a held dispatch");

    // Resume. An in-flight message has no held dispatch to replay, but its ack
    // budget must start again from NOW — the 600ms just spent paused is not
    // 'mid's fault and must not be charged to it.
    let outcome = orchestrator.resume().await;
    assert!(outcome.was_paused);
    assert_eq!(outcome.resumed_messages, 0, "the replay only covers held dispatches");
    assert!(outcome.errors.is_empty(), "the resume sweep must not fail anything: {outcome:?}");

    // The very first sweep after the resume is the one that used to kill it.
    orchestrator.handle_timeout().await.unwrap();
    assert_eq!(
        row(&orchestrator.db, &uuid7).await["pipeline_status"],
        "queued",
        "paused time must not count against a message's ack budget"
    );

    // 'mid' finally answers, and the chain finishes exactly as it would have
    // without a pause.
    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "done", false))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), uuid7);
    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "done"))
        .await
        .unwrap();
    let r = row(&orchestrator.db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete");
    assert_eq!(r["processed_message"], "done");
}

/// The same rule for a message whose stage transition was held rather than its
/// stage held: it resumes mid-chain, at the module it was about to reach.
#[tokio::test]
async fn a_message_held_between_stages_resumes_at_the_stage_it_reached() {
    let (orchestrator, mut rx) = orchestrator_with(3000, false).await;
    orchestrator
        .handle_message_from_module(&adapter_ingest("between stages"))
        .await
        .unwrap();
    let uuid7 = take_broadcast(rx.get_mut("pre").unwrap());

    // 'pre' acks — which is what would normally hand the message to 'mid' — and
    // the pause lands in the same breath.
    assert!(orchestrator.pause().await);
    orchestrator
        .handle_message_from_module(&pre_reply("pre", &uuid7, "pre text"))
        .await
        .unwrap();
    assert!(nothing_left(rx.get_mut("mid").unwrap()), "the held transition must not dispatch");
    assert_eq!(orchestrator.held_count().await, 1);

    let outcome = orchestrator.resume().await;
    assert_eq!(outcome.resumed_messages, 1);
    assert!(outcome.errors.is_empty());
    let reached = wait_for_broadcasts(rx.get_mut("mid").unwrap(), 1, "the resumed chain").await;
    assert_eq!(reached, vec![uuid7.clone()], "the replay continues the chain");

    orchestrator
        .handle_message_from_module(&in_reply("mid", &uuid7, "done", false))
        .await
        .unwrap();
    assert_eq!(take_broadcast(rx.get_mut("post").unwrap()), uuid7);
    orchestrator
        .handle_message_from_module(&post_reply("post", &uuid7, "done"))
        .await
        .unwrap();
    assert_eq!(row(&orchestrator.db, &uuid7).await["pipeline_status"], "complete");
}

/// Pausing twice is not an error and does not re-hold what is already held —
/// the control surface may poll or double-press, and neither may disturb a
/// message that is already waiting.
#[tokio::test]
async fn pausing_an_already_paused_engine_is_a_no_op() {
    let (orchestrator, mut rx) = orchestrator_with(3000, true).await;
    orchestrator
        .handle_message_from_module(&adapter_ingest("waiting"))
        .await
        .unwrap();
    assert_eq!(orchestrator.held_count().await, 1);

    assert!(!orchestrator.pause().await, "already paused");
    assert_eq!(orchestrator.held_count().await, 1);
    no_module_saw_anything(&mut rx);
}

// ── The recovery drain terminates ─────────────────────────────────────

/// The live defect this workstream opened with. The drain used to loop until
/// no `'queued'` rows were left, which assumed a started message moves its row
/// off `'queued'`. It does not: an in-flight message is `'queued'` for its
/// whole flight, so the loop re-selected the row it had just driven, skipped it
/// as already in flight, and spun on SELECTs against the timeline DB forever.
///
/// Every assertion is bounded, so a regression fails the test instead of
/// hanging the suite.
#[tokio::test]
async fn the_recovery_drain_terminates_with_a_row_that_stays_queued() {
    const BOUND: Duration = Duration::from_secs(5);

    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);

    // One stranded row: a crash between the timeline insert and the broadcast.
    let stranded = "0198c0de-0000-7000-8000-0000000000aa".to_string();
    db.insert_event(stranded.as_bytes(), 1, "twitch", &[], "stranded", "", "{}")
        .await
        .unwrap();

    // ...and one live message, whose row is 'queued' too — and always will be,
    // because the terminal write is the only status write there is.
    orchestrator
        .handle_message_from_module(&adapter_ingest("in flight"))
        .await
        .unwrap();
    let live = take_broadcast(rx.get_mut("pre").unwrap());
    let queued = db.get_queued_uuids().await.unwrap();
    assert_eq!(queued.len(), 2, "both rows read as 'queued' — that is the trap");
    assert!(queued.contains(&stranded) && queued.contains(&live));

    let drain = tokio::time::timeout(BOUND, orchestrator.drain_queued_once())
        .await
        .expect("the recovery drain must terminate: one pass, never a loop")
        .unwrap();
    assert_eq!(drain.considered, 2);
    assert_eq!(drain.claimed, 1, "only the stranded row is driven");
    assert!(drain.failures.is_empty(), "{:?}", drain.failures);

    let pre = rx.get_mut("pre").unwrap();
    assert_eq!(take_broadcast(pre), stranded, "the stranded row is re-driven");
    assert!(nothing_left(pre), "the live message must not be re-broadcast");

    // BOTH rows are 'queued' again — the live message, and now the recovered
    // one, which is in flight too — so the situation is unchanged rather than
    // progressing. A second pass must therefore also terminate, and must report
    // that it drove nothing: this is precisely the state the old loop spun in.
    let second = tokio::time::timeout(BOUND, orchestrator.drain_queued_once())
        .await
        .expect("a second pass must terminate too")
        .unwrap();
    assert_eq!(second.considered, 2, "both messages are in flight, so both rows read as 'queued'");
    assert_eq!(second.claimed, 0, "a row the live pipeline owns is not a drain");
    assert!(nothing_left(rx.get_mut("pre").unwrap()), "nothing may be re-broadcast");
    assert_eq!(row(&db, &live).await["pipeline_status"], "queued", "the drain must not write to a live row");
}

/// Structural guard for the same defect. The behavioural test above proves the
/// single-pass helper terminates; this proves `main.rs` still routes the
/// recovery task through it, because the loop shape only misbehaves at runtime
/// and only under a live pipeline.
#[test]
fn the_recovery_drain_task_is_a_single_pass() {
    let src = include_str!("../main.rs");
    let lines: Vec<&str> = src.lines().collect();

    let call = lines
        .iter()
        .position(|l| l.contains("drain_queued_once()"))
        .expect("the recovery task must go through the orchestrator's single pass");
    let spawn = (0..call)
        .rev()
        .find(|i| lines[*i].contains("tokio::spawn"))
        .expect("the recovery drain must run in a spawned task");

    // Walk the spawned block by brace depth, so this survives re-indentation.
    let mut depth = 0i32;
    let mut block = String::new();
    for line in &lines[spawn..] {
        block.push_str(line);
        block.push('\n');
        depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
        if depth == 0 {
            break;
        }
    }

    assert!(
        !block.contains("loop"),
        "the recovery drain must not loop: an in-flight row is 'queued' for its whole flight, so the loop never ends"
    );
    assert!(block.contains("drain_queued_once()"), "the drain must use the single pass");
    // The two log lines the drain has always emitted, unchanged.
    assert!(block.contains("Recovery error:"), "a SELECT failure must still be logged as before");
    assert!(
        block.contains("Recovery failed for {}: {}"),
        "a per-row failure must still be logged as before"
    );
    // The grace sleep is unchanged.
    assert!(block.contains("sleep(std::time::Duration::from_secs(grace))"), "the grace sleep must stay");
}

/// Structural guard for the other half: the engine must boot through the pause
/// gate, or "boots paused" is only true of the function nobody calls.
#[test]
fn the_engine_boots_through_the_pause_gate() {
    let src = include_str!("../main.rs");
    assert!(
        src.contains("config::start_paused(&config)"),
        "the boot state must come from the config/env escape hatch"
    );
    assert!(
        src.contains("orchestrator.pause().await"),
        "the engine must actually be paused at boot"
    );
    // ...and the gate must be set BEFORE the ack sweep and recovery tasks are
    // spawned, or they can run against a running pipeline.
    let gate = src.find("config::start_paused(&config)").expect("boot gate");
    let sweep = src
        .find("crate::pipeline::TIMEOUT_SWEEP_INTERVAL")
        .expect("the timeout sweep");
    assert!(gate < sweep, "the pause must be set before the sweep task starts");
}

/// A paused engine's recovery is deferred, not skipped: a stranded row is
/// claimed and held like any other message, and released by the resume. This is
/// the boot-paused + crash-in-one combination.
#[tokio::test]
async fn a_paused_engine_defers_recovered_rows_to_the_resume() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &["pre"], &["mid"], &["post"]);
    orchestrator.pause().await;

    let stranded = "0198c0de-0000-7000-8000-0000000000bb".to_string();
    db.insert_event(stranded.as_bytes(), 1, "twitch", &[], "stranded", "", "{}")
        .await
        .unwrap();

    let drain = tokio::time::timeout(Duration::from_secs(5), orchestrator.drain_queued_once())
        .await
        .expect("a paused drain still terminates")
        .unwrap();
    assert_eq!(drain.claimed, 1, "the stranded row is still claimed while paused");
    assert!(nothing_left(rx.get_mut("pre").unwrap()), "…and still not dispatched");
    assert_eq!(orchestrator.held_count().await, 1);

    orchestrator.resume().await;
    let drained = wait_for_broadcasts(rx.get_mut("pre").unwrap(), 1, "the deferred recovery").await;
    assert_eq!(drained, vec![stranded]);
}
