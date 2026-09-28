//! Structural guards for the control-surface shutdown path in `main.rs`.
//!
//! The flag decision and the two-stage signal are unit-tested in `queries.rs`
//! against a real channel, and the branch body is exercised directly. What those
//! tests cannot reach is the WIRING in the connection loop: that the flush is
//! armed after the answer is queued, and that the answer is only confirmed once
//! the frame has been written. Both are orderings in the source rather than
//! values, so they are asserted in the source — the same way the boot-pause gate
//! is guarded in `pause_tests.rs`.

/// The heart of the guarantee: the TUI's answer is queued FIRST, and the
/// connection only arms "the answer is on its way" afterwards. Reversed, the
/// first frame written after arming could be someone else's log line and the
/// process would exit with the answer still queued.
#[test]
fn the_flush_is_armed_only_after_the_answer_is_queued() {
    let src = include_str!("../main.rs");
    let queued = src
        .find("let queued = queries::send_query_response(")
        .expect("the query arm queues the answer");
    let armed = src
        .find("shutdown_flush = answered_with_shutdown && queued;")
        .expect("the query arm arms the flush");
    assert!(
        queued < armed,
        "the answer must be on the outbound channel before the flush is armed"
    );
}

/// …and the confirmation happens after the write completes, in the outbound
/// arm — scoped to that arm so a confirmation anywhere else cannot satisfy it.
#[test]
fn the_answer_is_confirmed_only_after_the_frame_is_written() {
    let src = include_str!("../main.rs");
    let arm_start = src
        .find("outbound = rx.recv() => {")
        .expect("the outbound arm of the connection loop");
    let arm_end = src[arm_start..]
        .find("killed = kill_rx.changed() => {")
        .expect("the end of the outbound arm")
        + arm_start;
    let arm = &src[arm_start..arm_end];

    let write = arm
        .find("bounded_ws_send(")
        .expect("the outbound arm writes the frame");
    let handoff = arm
        .find("shutdown.mark_answered()")
        .expect("the outbound arm hands the exit back once the frame is out");
    assert!(
        write < handoff,
        "the caller must have the answer before the process may exit"
    );
}

/// The process itself must not exit until the answer is confirmed, and must exit
/// with a success status — the TUI supervisor reads a non-zero status as a
/// crash and restarts on its own terms.
#[test]
fn the_engine_exits_cleanly_and_only_after_the_answer_is_confirmed() {
    let src = include_str!("../main.rs");
    let wait = src
        .find("shutdown.wait_answered()")
        .expect("the exit waits for the answer");
    let exit = src
        .find("std::process::exit(0)")
        .expect("a clean exit");
    assert!(wait < exit, "the process must not exit before the answer is confirmed");
    assert_eq!(
        src.matches("std::process::exit(").count(),
        1,
        "shutdown-on-request is the only exit the engine takes"
    );
    // The listener loop has to be interruptible, or the exit never runs.
    let loop_start = src
        .find("let mut shutdown_stage = shutdown.subscribe();")
        .expect("the accept loop watches the shutdown signal");
    let loop_end = src
        .find("let (stream, address) = accepted?;")
        .expect("the accept loop head");
    let accept_loop = &src[loop_start..loop_end];
    assert!(
        accept_loop.contains("shutdown_stage.changed()"),
        "the accept loop must be able to see a shutdown request"
    );
    assert!(
        accept_loop.contains("break;"),
        "a shutdown request must break the accept loop, not just be observed"
    );
}

/// A wedged outbound channel drops the answer (and says so). The engine must
/// then release the exit instead of waiting for an answer that can never arrive
/// — otherwise an accepted shutdown leaves the process running forever.
#[test]
fn an_answer_that_could_not_be_delivered_still_releases_the_exit() {
    let src = include_str!("../main.rs");
    assert!(
        src.contains("if answered_with_shutdown && !queued {"),
        "a dropped answer must be handled explicitly"
    );
    // Two escapes from the wait: the drop above, and a socket that died with the
    // answer still unflushed. Both end in `mark_answered`.
    assert_eq!(
        src.matches("if shutdown_flush {").count(),
        2,
        "the flush must be completed after a successful write AND on connection end"
    );
    assert!(
        src.contains("if shutdown_flush {\n        shutdown.mark_answered();\n    }"),
        "a connection that ends with an unflushed answer must release the exit"
    );
}
