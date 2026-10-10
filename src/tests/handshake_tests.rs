//! Connection-handshake tests.
//!
//! The pre-auth drain is structural/brittle to observe in a unit test (it needs
//! a real WSS client), so the ordering guarantee is asserted against the source:
//! the drain must run BEFORE the ConnectionRequestReturn is sent, otherwise a
//! module that registers commands immediately after connecting races and has
//! them dropped.

/// The first pre-auth drain in `main.rs` must precede the first
/// `ConnectionRequestReturn` in the source. If it moved back after the return,
/// a fast module's first payload (sent only once it receives the return) would
/// be discarded and the engine would never learn which commands it owns.
#[test]
fn pre_auth_drain_runs_before_the_connection_request_return() {
    // Collapse all whitespace so the assertions don't depend on formatting.
    let src: String = include_str!("../main.rs")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    let drain = src
        .find("Discardedpayloadfrom")
        .expect("pre-auth drain discard message must exist in main.rs");
    let drain_msg = src
        .find("sentbeforeauthorization")
        .expect("pre-auth drain discard message must exist in main.rs");
    let ret = src
        .find("ModulePayload::ConnectionRequestReturn")
        .expect("ConnectionRequestReturn must exist in main.rs");

    assert!(
        drain < ret,
        "pre-auth drain (first at {drain}) must precede ConnectionRequestReturn (first at {ret})"
    );
    assert!(
        drain_msg < ret,
        "pre-auth drain message (first at {drain_msg}) must precede ConnectionRequestReturn (first at {ret})"
    );
}
