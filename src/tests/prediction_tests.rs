//! Structural guards for the PredictionUpdate relay in `main.rs`.
//!
//! The receive loop's arms run in the connection task against a live
//! WebSocket, so they are asserted in the source — the same way the boot-pause
//! gate (`pause_tests.rs`) and the shutdown flush (`shutdown_tests.rs`) are.
//! What the assertions pin is that the relay arm exists, mirrors the Prompt
//! forward shape, filters the origin module out of the broadcast, and gates the
//! origin to the predictions module / control surface.

/// Extract the PredictionUpdate arm from the receive loop's `match
/// container.payload`, between its own head and the following PromptResponse
/// arm.
fn prediction_arm(src: &str) -> &str {
    let start = src
        .find("Some(Payload::PredictionUpdate(ref update)) => {")
        .expect("the receive loop must have a PredictionUpdate relay arm");
    let end = src[start..]
        .find("Some(Payload::PromptResponse(ref resp)) => {")
        .expect("the PredictionUpdate arm must end where the PromptResponse arm begins")
        + start;
    &src[start..end]
}

/// The arm exists and mirrors the Prompt forward: a fresh Container carrying
/// the same header fields, the cloned payload, and a non-blocking try_send to
/// every connected module except the sender.
#[test]
fn the_prediction_update_arm_exists_and_mirrors_the_prompt_forward() {
    let src = include_str!("../main.rs");
    let arm = prediction_arm(src);

    // The forward is a fresh Container with the same header fields as the
    // Prompt arm, carrying the cloned PredictionUpdate.
    assert!(arm.contains("let forward = Container {"), "the arm must build a fresh container");
    assert!(arm.contains("version: 1,"), "the forward must carry the protocol version");
    assert!(arm.contains("auth_token: container.auth_token.clone()"), "the forward must carry the auth token");
    assert!(arm.contains("module_name: container.module_name.clone()"), "the forward must carry the module name");
    assert!(arm.contains("module_instance_uuid7: container.module_instance_uuid7.clone()"), "the forward must carry the instance uuid");
    assert!(arm.contains("payload: Some(Payload::PredictionUpdate(update)),"), "the forward must carry the cloned prediction update");

    // The broadcast shape matches Prompt: every module except the sender, via
    // non-blocking try_send.
    assert!(arm.contains("filter(|(n, _)| n.as_str() != module_name.as_str())"), "the forward must exclude the origin module");
    assert!(arm.contains("sender.try_send(forward.clone())"), "the forward must be a non-blocking try_send");
}

/// Only the predictions module (the brain) or the TUI control surface may
/// broadcast a prediction bar; any other module is ignored with a log line that
/// names it. A random module spoofing a bar would be indistinguishable from a
/// real one, so the origin is gated before the forward.
#[test]
fn the_prediction_update_relay_gates_the_origin() {
    let src = include_str!("../main.rs");
    let arm = prediction_arm(src);

    assert!(
        arm.contains("module_name != \"predictions\"") && arm.contains("is_control_surface"),
        "only the predictions module or the control surface may broadcast"
    );
    assert!(
        arm.contains("log_event_broadcast"),
        "an unauthorised broadcaster must be called out"
    );
    assert!(
        arm.contains("only the predictions module or TUI may broadcast"),
        "the log line must name the rule that was violated"
    );
}