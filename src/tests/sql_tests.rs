//! Query surface boundary tests: the engine has NO raw-SQL escape hatch.
//!
//! Phase 2 removed the read-only SQL fallback — a query the engine does not
//! name (via `DatabaseQuery.query_id` or `QueryRequest.operation`) is denied
//! outright. `classify_query` never falls through to SQL, and the wire
//! `QueryOp` enum has no SQL operation.

use crate::queries::{QueryRoute, classify_query, route_from_op};
use crate::cockatiel_protobuf::QueryOp;

#[test]
fn unknown_query_ids_are_denied_not_executed() {
    assert!(
        matches!(classify_query("DROP TABLE timeline_events"), QueryRoute::Unsupported),
        "a DROP must be denied, never run"
    );
    assert!(
        matches!(classify_query("SELECT * FROM timeline_events"), QueryRoute::Unsupported),
        "even a SELECT is denied — there is no raw-SQL fallback"
    );
    assert!(
        matches!(classify_query("totally_made_up_query"), QueryRoute::Unsupported),
        "an unknown name is denied, not guessed as SQL"
    );
}

#[test]
fn named_ops_still_classify() {
    assert!(matches!(classify_query("db_status"), QueryRoute::DbStatus));
    assert!(matches!(classify_query("module_list"), QueryRoute::ModuleList));
    assert!(matches!(classify_query("stats"), QueryRoute::Stats));
    assert!(matches!(classify_query("audit_list"), QueryRoute::AuditList));
    assert!(matches!(classify_query("channel_viewers"), QueryRoute::ChannelViewers));
    assert!(matches!(classify_query("mod_ban"), QueryRoute::ModFamily));
    assert!(matches!(classify_query("userdb_get_user"), QueryRoute::UserdbFamily));
}

#[test]
fn every_wire_op_maps_or_is_explicitly_refused() {
    // Every named QueryOp the wire can carry resolves to a route, except the
    // ones deliberately refused (Unspecified / the timeline-read op, which uses
    // the dedicated TimelineQuery payload).
    for op in [
        QueryOp::DbStatus,
        QueryOp::ModuleList,
        QueryOp::EngineInfo,
        QueryOp::Stats,
        QueryOp::AuditList,
        QueryOp::ChatCommend,
        QueryOp::ChannelViewers,
        QueryOp::UserdbAdjustScore,
        QueryOp::ModBan,
    ] {
        assert!(
            route_from_op(op).is_some(),
            "op {op:?} must resolve to a dispatcher route"
        );
    }
    for op in [QueryOp::Unspecified, QueryOp::TimelineRead] {
        assert!(
            route_from_op(op).is_none(),
            "op {op:?} must be refused (no route)"
        );
    }
}