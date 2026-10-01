//! The engine's `DatabaseQuery` surface: every operation the engine answers
//! for a connected module, and the gate that decides who may ask.
//!
//! This is the landing site of the `Payload::DatabaseQuery` arm that used to
//! be an ~340-line `if / else if` chain inside the WebSocket receive loop in
//! `main.rs`. The split is deliberately behaviour-preserving: the arm's chain
//! order, its gate strings, and its `(success, result_blob, error)` results are
//! all reproduced here exactly, because that chain order is load-bearing (see
//! [`classify_query`] and [`caller_gate`]) and its strings are matched on by
//! the TUI and the compliance test runner.
//!
//! Two things make the chain testable in isolation, which it never was while
//! it lived in the socket loop:
//!
//! * [`classify_query`] — the pure routing decision, query_id -> [`QueryRoute`].
//!   Extracted from the chain so the branch set can be asserted, not eyeballed.
//! * [`caller_gate`] — the pure authorisation decision, (op, caller) -> denial.
//!   Extracted from the chain's inline `if !is_control_surface(..)` guards.

#![allow(clippy::type_complexity)]

use prost::Message;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

use crate::auth::AuthStore;
use crate::cockatiel_protobuf::{
    AuthVerify, ChatMessage, ContainerForModule, DatabaseQueryResult, Log, MessagePreProcess,
    Prompt, QueryOp as WireQueryOp, QueryParams, Shutdown, container_for_module::Payload,
};
use crate::config::{Config, ConfigState, get_config};
use crate::credentials::{
    credential_values_map, is_config_complete, save_module_credentials, validate_credential_fields,
};
use crate::database::DatabaseManager;
use crate::module_manager::ModuleRegistry;
use crate::module_registry::ModuleRegistryPersistence;
use crate::pipeline::{PipelineOrchestrator, SendOutcome};
use crate::user_db_client::{SharedUserDbClient, userdb_response_to_json};
use crate::{
    EngineState, PromptSink, SharedChannelStats, SharedPromptRoutes, is_control_surface,
    is_test_runner, log_event, log_event_broadcast, may_read_other_credentials, userdb_actor_perm,
};

/// The result of answering one query: the triple the dispatcher has always
/// produced. Kept as a named struct so the response envelope and the dispatch
/// chain agree on one shape, with [`From`] to build it from a bare tuple.
pub(crate) struct QueryOutcome {
    pub success: bool,
    pub result_blob: Vec<u8>,
    pub error: String,
}

impl QueryOutcome {
    fn success(blob: Vec<u8>) -> Self {
        Self { success: true, result_blob: blob, error: String::new() }
    }

    fn denied(error: impl Into<String>) -> Self {
        Self { success: false, result_blob: Vec::new(), error: error.into() }
    }
}

impl From<(bool, Vec<u8>, String)> for QueryOutcome {
    fn from((success, result_blob, error): (bool, Vec<u8>, String)) -> Self {
        Self { success, result_blob, error }
    }
}

/// How far along the control-surface shutdown is.
///
/// The two-step [`ShutdownStage::Requested`] -> [`ShutdownStage::Answered`]
/// split IS the guarantee that the caller is told the answer before the process
/// goes away. Collapsing it into one flag is the bug this type exists to make
/// impossible: a shutdown that fires the moment the request is accepted tears
/// the socket down mid-response, and the TUI is left holding a dead connection
/// with no idea whether its request was honoured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShutdownStage {
    /// Nobody has asked.
    Idle,
    /// The engine accepted the request; the answer is on its way to the caller.
    Requested,
    /// The answer has been written to the caller's socket (or the socket is
    /// gone, so no answer is possible any more). The process may exit.
    Answered,
}

/// The engine's control-surface shutdown signal, carried on
/// [`QueryContext`] and observed by the main loop.
///
/// One-shot and monotonic: `Idle` -> `Requested` -> `Answered`, never back. It
/// is a `watch` channel rather than a bare `AtomicBool` so an observer that
/// arrives after the fact still reads the current stage instead of blocking on
/// a notification that was already sent — the same shape the per-session kill
/// switch in `main.rs` uses.
pub(crate) struct ShutdownSignal {
    tx: tokio::sync::watch::Sender<ShutdownStage>,
    /// A retained receiver, purely to keep the channel OPEN. A `watch` channel
    /// whose last receiver is dropped refuses `send`, which would turn every
    /// shutdown request into a silent no-op — the one failure mode this type
    /// must not have.
    keep_open: tokio::sync::watch::Receiver<ShutdownStage>,
}

pub(crate) type SharedShutdown = Arc<ShutdownSignal>;

impl ShutdownSignal {
    /// A fresh signal, parked at [`ShutdownStage::Idle`].
    pub(crate) fn new() -> SharedShutdown {
        let (tx, keep_open) = tokio::sync::watch::channel(ShutdownStage::Idle);
        Arc::new(Self { tx, keep_open })
    }

    /// The current stage. Cheap, and never blocks.
    pub(crate) fn stage(&self) -> ShutdownStage {
        *self.keep_open.borrow()
    }

    /// Raise the request. Called from the query path the moment the engine has
    /// DECIDED to honour a shutdown — the response is still to be written, so
    /// this deliberately does not release the exit.
    pub(crate) fn request(&self) {
        let _ = self.tx.send(ShutdownStage::Requested);
    }

    /// The answer has reached the caller's socket (or the socket died with the
    /// answer still queued, in which case no answer is ever coming). Only now
    /// may the process begin exiting.
    pub(crate) fn mark_answered(&self) {
        let _ = self.tx.send(ShutdownStage::Answered);
    }

    /// Resolve once the answer is confirmed, so the main loop can exit. Resolves
    /// immediately if it is already confirmed — including when the engine
    /// observed the request before this was awaited.
    pub(crate) async fn wait_answered(&self) {
        let mut stage = self.tx.subscribe();
        loop {
            if *stage.borrow_and_update() == ShutdownStage::Answered {
                return;
            }
            // Cannot error: the Sender is held by `self`, which outlives this
            // borrow. Looping re-reads the value, so a notification that
            // arrives before the first poll is never lost.
            let _ = stage.changed().await;
        }
    }

    /// A receiver for the main loop's accept loop to select on. Separate from
    /// [`ShutdownSignal::wait_answered`] so the loop can watch for the REQUEST
    /// and only then wait for the answer.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<ShutdownStage> {
        self.tx.subscribe()
    }
}

/// Everything the query surface needs from the connection loop, borrowed for
/// the duration of one `DatabaseQuery`.
///
/// The fields are the same `Arc`/`Mutex` shapes the loop already held; this
/// only gives the query code a name for them instead of reaching into the
/// loop's locals.
pub(crate) struct QueryContext<'a> {
    /// The timeline database.
    pub db: &'a DatabaseManager,
    /// Live module sessions (for `module_list` and `test_probe` liveness).
    pub auth_store: &'a AuthStore,
    /// Used to reach a module's socket for `test_probe`.
    pub orchestrator: &'a PipelineOrchestrator,
    /// Engine log/timeline sink; every branch logs through it.
    pub ui_state: &'a Arc<Mutex<EngineState>>,
    /// The engine-internal user database.
    pub user_db_client: &'a SharedUserDbClient,
    /// Config + PIN, for `engine_info` and `test_run`.
    pub config_state: &'a Arc<Mutex<ConfigState>>,
    /// Persisted `modules.json` knowledge, for `module_list`.
    pub module_registry: &'a ModuleRegistryPersistence,
    /// Manifests discovered on disk, for `module_list` + `set_credentials`.
    pub discovered_registry: &'a Arc<Mutex<ModuleRegistry>>,
    /// Where a prompt is waiting to be answered, for `module_list`.
    pub prompt_routes: &'a SharedPromptRoutes,
    /// Per-channel viewer/member counts, for `channel_viewers`.
    pub channel_stats: &'a SharedChannelStats,
    /// The calling module — this is what every gate keys off.
    pub module_name: &'a str,
    /// The calling module's instance uuid, echoed on the response.
    pub instance_uuid7: &'a str,
    /// Outbound channel to the calling module.
    pub tx: &'a tokio::sync::mpsc::Sender<ContainerForModule>,
    /// The engine's control-surface shutdown signal. `engine_shutdown` raises
    /// it; the main loop waits on it.
    pub shutdown: &'a SharedShutdown,
    /// Whether the caller connected over loopback; grants owner perms in the
    /// privileged userdb mutations.
    pub peer_loopback: bool,
}

/// Where the dispatcher sends a query. One variant per branch of the original
/// `if / else if` chain, in the chain's order.
///
/// The two family variants (`ModFamily`, `UserdbFamily`) exist because the
/// chain routed on a PREFIX, not on a known name: `mod_foo` and `userdb_foo`
/// reach their family's handler and are rejected *there*, after the prefix
/// match, with `Unknown mod query: foo`. A classification that only knew the
/// named operations would send those to the read-only SQL fallback instead,
/// which is a different answer to the same request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryRoute {
    DbStatus,
    ModuleList,
    EngineInfo,
    /// Hold / release the whole message pipeline. Control surface only.
    PipelineSetPaused,
    /// Ask the engine to shut down, if its own config permits. Control surface
    /// only.
    EngineShutdown,
    TestRun,
    TestProbe,
    AuditList,
    AuditApprove,
    AuditReject,
    /// Any `mod_`-prefixed query_id.
    ModFamily,
    ChatCommend,
    ChatReprimand,
    ChatVerifyIdentity,
    UserdbAdjustScore,
    /// Read a user's current score. Predictions module + control surface only.
    PredictionGetScore,
    /// Read the engine's in-memory per-channel viewer/member counts. Open to
    /// every authenticated module — the data is public stream information.
    ChannelViewers,
    /// Any `userdb_`-prefixed query_id, after the exact `userdb_adjust_score`
    /// match above has had its chance.
    UserdbFamily,
    SetCredentials,
    AudioForMessage,
    TestArchive,
    /// One-shot timeline aggregates (total messages / users / commands,
    /// platform counts and errors, recent chart buckets). Control surface.
    Stats,
    /// Any query the engine does not name. Denied outright — there is no raw
    /// SQL fallback.
    Unsupported,
}


/// The routing decision, split out of the dispatch chain so the branch set is
/// assertable.
///
/// **The order of the tests below IS the behaviour.** Two entries only make
/// sense in their current position:
///
/// * `userdb_adjust_score` must be matched before the `userdb_` prefix test,
///   because `"userdb_adjust_score".starts_with("userdb_")` is true — moved
///   down, it would be swallowed by the family and answered by
///   `userdb_virtual_query`, which has no case for it and would answer
///   `Unknown userdb query: userdb_adjust_score` instead of applying the
///   delta. Its own three-way gate (control surface OR `score-messages` OR
///   `predictions`) would also be replaced by the family's control-surface-only
///   gate.
/// * `userdb_adjust_score` must sit after `chat_verify_identity` and the
///   `mod_` prefix test for the same family-ordering reason: a rename that
///   collided with an earlier prefix would change which handler runs.
///
/// The `mod_` prefix test precedes the `userdb_` prefix test. That ordering is
/// not actually load-bearing — no string begins with both `mod_` and
/// `userdb_` — but it is preserved as-is rather than tidied.
pub(crate) fn classify_query(query_id: &str) -> QueryRoute {
    match query_id {
        "db_status" => QueryRoute::DbStatus,
        "module_list" => QueryRoute::ModuleList,
        "engine_info" => QueryRoute::EngineInfo,
        "pipeline_set_paused" => QueryRoute::PipelineSetPaused,
        "engine_shutdown" => QueryRoute::EngineShutdown,
        "test_run" => QueryRoute::TestRun,
        "test_probe" => QueryRoute::TestProbe,
        "audit_list" => QueryRoute::AuditList,
        "audit_approve" => QueryRoute::AuditApprove,
        "audit_reject" => QueryRoute::AuditReject,
        _ if query_id.starts_with("mod_") => QueryRoute::ModFamily,
        "chat_commend" => QueryRoute::ChatCommend,
        "chat_reprimand" => QueryRoute::ChatReprimand,
        "chat_verify_identity" => QueryRoute::ChatVerifyIdentity,
        // Must precede the `userdb_` prefix test below — see the doc comment.
        "userdb_adjust_score" => QueryRoute::UserdbAdjustScore,
        _ if query_id.starts_with("userdb_") => QueryRoute::UserdbFamily,
        "prediction_get_score" => QueryRoute::PredictionGetScore,
        "channel_viewers" => QueryRoute::ChannelViewers,
        "set_credentials" => QueryRoute::SetCredentials,
        "audio_for_message" => QueryRoute::AudioForMessage,
        "test_archive" => QueryRoute::TestArchive,
        "stats" => QueryRoute::Stats,
        // No raw-SQL escape hatch: a module that asks for something the engine
        // does not name is denied, never handed arbitrary SQL. Phase 2 removed
        // the ReadOnlySql fallback.
        _ => QueryRoute::Unsupported,
    }
}

/// The authorisation decision, split out of the dispatch chain: may `caller`
/// invoke `route` at all? `None` means yes.
///
/// `Some(msg)` is the denial, and `msg` is the exact string the chain has
/// always returned — the TUI and the compliance test runner match on it, so it
/// is load-bearing and lives here as a single source of truth.
///
/// This is only the gate the DISPATCHER applies. Some operations carry a
/// second, handler-level gate that this cannot express because it needs the
/// user database or a payload: `mod_*` verifies a mod/admin/owner actor
/// (`mod_virtual_query`), `chat_commend`/`chat_reprimand` are restricted to
/// their own dedicated modules (`chat_rating_virtual_query`),
/// `chat_verify_identity` to `term-chat` (`chat_verify_identity_virtual_query`),
/// and `audit_approve`/`audit_reject` require a `uuid7` in the payload.
/// Operations with no dispatcher gate at all — `db_status`, `module_list`,
/// `engine_info`, `audio_for_message` — are open to any connected module;
/// `engine_info` and `module_list` redact the secrets a normal module may not
/// see, and `db_status` exposes no secrets at all.
pub(crate) fn caller_gate(route: QueryRoute, caller: &str) -> Option<&'static str> {
    match route {
        QueryRoute::TestRun => (!is_control_surface(caller))
            .then_some("Test runner access denied: not the TUI"),
        QueryRoute::TestProbe => (!is_control_surface(caller) && !is_test_runner(caller))
            .then_some("test_probe denied: not the TUI/test-runner"),
        QueryRoute::PipelineSetPaused => {
            (!is_control_surface(caller)).then_some("pipeline_set_paused denied: not the TUI")
        }
        // Killing the engine is the most consequential thing a caller can ask
        // for, so the gate is the TUI alone. The compliance test-runner is NOT
        // admitted for the same reason it is not admitted for
        // `pipeline_set_paused`: it must not be able to stop the engine that is
        // timing it. Whether the *TUI* is allowed to ask at all is a separate
        // question, answered by the engine's own `shutdown_on_request` config
        // flag in the branch body.
        QueryRoute::EngineShutdown => {
            (!is_control_surface(caller)).then_some("engine_shutdown denied: not the TUI")
        }
        QueryRoute::AuditList | QueryRoute::AuditApprove | QueryRoute::AuditReject => {
            (!is_control_surface(caller)).then_some("Audit access denied: not the TUI")
        }
        QueryRoute::UserdbAdjustScore => {
            (!is_control_surface(caller) && caller != "score-messages" && caller != "events")
                .then_some("userdb_adjust_score denied: not the TUI, score-messages, or events")
        }
        QueryRoute::PredictionGetScore => {
            (!is_control_surface(caller) && caller != "events")
                .then_some("prediction_get_score denied: not the events module")
        }
        QueryRoute::UserdbFamily => {
            (!is_control_surface(caller)).then_some("User database access denied: not the TUI")
        }
        QueryRoute::SetCredentials => {
            (!is_control_surface(caller)).then_some("set_credentials denied: not the TUI")
        }
        QueryRoute::TestArchive => {
            (!is_test_runner(caller)).then_some("test_archive access denied: not the test runner")
        }
        QueryRoute::Stats => {
            (!is_control_surface(caller)).then_some("stats access denied: not the control surface")
        }
        // There is no raw-SQL escape hatch and no unknown-query path: a query
        // the engine does not name is denied outright.
        QueryRoute::Unsupported => Some("query denied: no such operation"),
        QueryRoute::DbStatus
        | QueryRoute::ModuleList
        | QueryRoute::EngineInfo
        | QueryRoute::ModFamily
        | QueryRoute::ChatCommend
        | QueryRoute::ChatReprimand
        | QueryRoute::ChatVerifyIdentity
        | QueryRoute::AudioForMessage
        | QueryRoute::ChannelViewers => None,
    }
}

/// Build the `DatabaseQueryResult` response for a finished query.
///
/// One function so the envelope is constructed in exactly one place: a success
/// carries a blob and an empty error, a failure carries an error and an empty
/// blob.
pub(crate) fn build_query_response(
    auth_token: &str,
    instance_uuid7: &str,
    query_id: &str,
    outcome: QueryOutcome,
) -> ContainerForModule {
    ContainerForModule {
        version: 2,
        auth_token: auth_token.to_string(),
        module_instance_uuid7: instance_uuid7.to_string(),
        payload: Some(Payload::DatabaseQueryResult(DatabaseQueryResult {
            query_id: query_id.to_string(),
            success: outcome.success,
            error: outcome.error,
            result_blob: outcome.result_blob,
        })),
    }
}

/// Build the response and hand it to the caller, under the same 1s bound the
/// connection loop has always used. A wedged outbound channel drops the
/// response rather than stalling the module's read loop, and says so.
///
/// Returns whether the response made it onto the outbound channel. The
/// `engine_shutdown` branch needs that: it is the difference between "the answer
/// is queued and still to be written" (the process must wait for the write) and
/// "the answer was dropped on a full channel" (there is nothing to wait for).
pub(crate) async fn send_query_response(
    ctx: &QueryContext<'_>,
    auth_token: &str,
    query_id: &str,
    outcome: QueryOutcome,
) -> bool {
    let response = build_query_response(auth_token, ctx.instance_uuid7, query_id, outcome);
    match tokio::time::timeout(Duration::from_millis(1000), ctx.tx.send(response)).await {
        Ok(_) => true,
        Err(_) => {
            log_event_broadcast(
                ctx.ui_state,
                format!("[{}] dropped query response (outbound full)", ctx.module_name),
            );
            false
        }
    }
}

/// Order a `module_list` result for display.
///
/// Pre/post/input stages are UNORDERED sets, so their modules sort
/// alphabetically (the deterministic order a set has). The in-process stage is
/// an ORDERED CHAIN — its config.json order is what the engine steps through —
/// so those modules sort by chain position, not by name. This is what makes the
/// chain VISIBLE to the TUI: the UI renders `module_list` in response order, so
/// the in-process group shows the real chain and Shift+up/down reorders land
/// somewhere the operator can see.
fn sort_module_list(config: &Config, list: &mut [serde_json::Value]) {
    let chain_order: std::collections::HashMap<String, usize> = config
        .inprocess_modules
        .iter()
        .enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect();
    list.sort_by(|a, b| {
        let aname = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let bname = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let apos = a.get("position").and_then(|v| v.as_str()).unwrap_or("");
        let bpos = b.get("position").and_then(|v| v.as_str()).unwrap_or("");
        match (chain_order.get(aname), chain_order.get(bname)) {
            (Some(i), Some(j)) => i.cmp(j),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => {
                // ...everything else alphabetical. Position is a tiebreak so the
                // pipeline groups stay grouped even though the TUI groups by
                // position anyway.
                apos.cmp(bpos).then_with(|| aname.cmp(bname))
            }
        }
    });
}

/// The module's CURRENT autostart flag.
///
/// The engine caches manifests at discovery, but the TUI can toggle a module's
/// autostart in its manifest file at runtime (`a` in the modules window). If we
/// kept reporting the cached value, the TUI's `A` marker would flip back on the
/// next poll. Re-read the manifest's `autostart` fresh per query — this is a
/// low-frequency control query, so the file read is negligible — falling back
/// to the discovery-time value when the file can't be read.
fn current_autostart(discovered: &crate::module_manager::DiscoveredModule) -> bool {
    let path = discovered.directory.join("cockatiel_module_info.json");
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|m| m.get("autostart").and_then(|v| v.as_bool()))
        .unwrap_or(discovered.manifest.autostart)
}

/// Answer one `DatabaseQuery` and return the result triple. The single entry
/// point the connection loop calls.
pub(crate) async fn handle_query(
    ctx: &QueryContext<'_>,
    query_id: &str,
    sql: &str,
) -> QueryOutcome {
    let route = classify_query(query_id);
    if let Some(denial) = caller_gate(route, ctx.module_name) {
        return QueryOutcome::denied(denial);
    }
    dispatch(ctx, route, query_id, sql).await
}

/// Map a wire `QueryOp` to the route it answers. `None` for an operation the
/// dispatcher has no branch for (or `Unspecified`).
pub(crate) fn route_from_op(op: WireQueryOp) -> Option<QueryRoute> {
    use WireQueryOp::*;
    Some(match op {
        DbStatus => QueryRoute::DbStatus,
        ModuleList => QueryRoute::ModuleList,
        EngineInfo => QueryRoute::EngineInfo,
        PipelineSetPaused => QueryRoute::PipelineSetPaused,
        EngineShutdown => QueryRoute::EngineShutdown,
        TestRun => QueryRoute::TestRun,
        TestProbe => QueryRoute::TestProbe,
        AuditList => QueryRoute::AuditList,
        AuditApprove => QueryRoute::AuditApprove,
        AuditReject => QueryRoute::AuditReject,
        ModCommend | ModReprimand | ModBan | ModTimeout => QueryRoute::ModFamily,
        ChatCommend => QueryRoute::ChatCommend,
        ChatReprimand => QueryRoute::ChatReprimand,
        ChatVerifyIdentity => QueryRoute::ChatVerifyIdentity,
        UserdbAdjustScore => QueryRoute::UserdbAdjustScore,
        PredictionGetScore => QueryRoute::PredictionGetScore,
        ChannelViewers => QueryRoute::ChannelViewers,
        SetCredentials => QueryRoute::SetCredentials,
        AudioForMessage => QueryRoute::AudioForMessage,
        TestArchive => QueryRoute::TestArchive,
        Stats => QueryRoute::Stats,
        // The userdb family needs the exact operation name to pick the right
        // handler, but the dispatcher runs them through `userdb_virtual_query`
        // on the query_id string; map the named family ops to the family.
        UserdbAddUser
        | UserdbDeleteUser
        | UserdbAddScore
        | UserdbRemoveScore
        | UserdbAddChannel
        | UserdbRemoveChannel
        | UserdbGetUser
        | UserdbListUsers
        | UserdbUpdateFlags
        | UserdbSetRoles
        | UserdbReadUserValue
        | UserdbWriteUserValue
        | UserdbDeleteUserValue
        | UserdbListUserValues
        | UserdbCommendation
        | UserdbReprimand
        | UserdbBan
        | UserdbTimeout => QueryRoute::UserdbFamily,
        // Timeline reads use the dedicated TimelineQuery payload, not this op.
        TimelineRead => return None,
        // There is no raw-SQL operation on the wire surface at all.
        Unspecified => return None,
    })
}

/// Build the `sql` string a dispatch branch parses, from a `QueryRequest`'s
/// params. For the ops that still carry a JSON blob (the `mod_*`/`userdb_*`
/// families, set_credentials, pipeline pause, test_archive) that blob is the
/// `params.json` escape; the rest ignore params entirely.
fn sql_from_params(params: Option<&QueryParams>) -> String {
    params
        .map(|p| p.json.clone())
        .unwrap_or_default()
}

/// Answer one typed `QueryRequest` (the Phase-2 wire surface) and return the
/// outcome. The connection loop ships it back as a `QueryResponse`.
pub(crate) async fn handle_query_request(
    ctx: &QueryContext<'_>,
    req: &crate::cockatiel_protobuf::QueryRequest,
) -> QueryOutcome {
    let op = WireQueryOp::try_from(req.operation).unwrap_or(WireQueryOp::Unspecified);
    let Some(route) = route_from_op(op) else {
        return QueryOutcome::denied("query denied: no such operation");
    };
    if let Some(denial) = caller_gate(route, ctx.module_name) {
        return QueryOutcome::denied(denial);
    }
    // For the ops that still parse a JSON blob, the params ride in `json`;
    // everything else ignores params. The query_id is derived from the op so
    // the branch internals that key off the string keep working.
    let query_id = op.as_str_name().to_ascii_lowercase();
    dispatch(ctx, route, &query_id, &sql_from_params(req.params.as_ref())).await
}

/// The body of each branch, reached only once [`caller_gate`] has admitted the
/// caller. The per-branch comments are the originals, carried over with the
/// code they describe.
async fn dispatch(
    ctx: &QueryContext<'_>,
    route: QueryRoute,
    query_id: &str,
    sql: &str,
) -> QueryOutcome {
    let QueryContext {
        db,
        auth_store,
        orchestrator,
        ui_state,
        user_db_client,
        config_state,
        module_registry,
        discovered_registry,
        prompt_routes,
        module_name,
        shutdown,
        peer_loopback,
        ..
    } = ctx;
    let module_name: &str = module_name;
    match route {
        QueryRoute::DbStatus => {
            // Backup status for the timeline + user database, so
            // the UI can warn the operator about data-loss risk.
            let userdb_backup = std::env::var("USER_DB_BACKUP_PATH")
                .map(|p| !p.trim().is_empty())
                .unwrap_or(false);
            // `pipeline_paused` lives HERE rather than on `engine_info`
            // because this is the query the UIs already poll on a refresh timer
            // (the TUI re-reads it every 2s and parses this object into its
            // stats), whereas `engine_info` is a once-on-connect metadata read —
            // a pause that began after the UI connected would be invisible. The
            // field exposes no secret, so it stays ungated for every module.
            let status = serde_json::json!({
                "timeline_backup": db.backup_configured(),
                "userdb_backup": userdb_backup,
                "timeline_db_size_bytes": db.db_size_bytes(),
                "timeline_db_target_mb": db.target_mb(),
                "pipeline_paused": orchestrator.is_paused().await,
            });
            QueryOutcome::success(status.to_string().into_bytes())
        }
        QueryRoute::ModuleList => {
            // The config's ordering lists are the AUTHORITATIVE position — the
            // TUI rewrites them at runtime (Shift+up/down stage moves), so a
            // module's reported stage must follow config.json, not the stage it
            // happened to connect on. Force a fresh read so a same-byte-length
            // rewrite (which the size gate would otherwise skip) is still seen;
            // this is a low-frequency control query, not the message hot path.
            crate::config::refresh_config(config_state);
            let config = get_config(config_state);
            let sessions = auth_store.values();
            // Rolling per-module processing averages (ms), for the TUI's ms
            // column. Snapshot once so every entry reads the same numbers.
            let module_timings = orchestrator.module_timings.lock().await.all_avgs();
            // Modules with an unanswered prompt are waiting on
            // the operator (e.g. a setup/credential question) —
            // expose it so UIs and the test harness can tell a
            // connected-but-stuck module apart from an idle one.
            let pending_prompts: std::collections::HashSet<String> = prompt_routes
                .lock()
                .unwrap()
                .values()
                .filter_map(|sink| match sink {
                    PromptSink::Module(origin, _) => Some(origin.clone()),
                    PromptSink::Engine(_) => None,
                })
                .collect();
            let registered = module_registry.values();

            // Merge by module name: discovered (manifests on disk) +
            // registered (persisted modules.json) + live sessions.
            let mut entries: HashMap<String, serde_json::Value> = HashMap::new();

            // 1. Discovered modules — what the engine can find on disk
            // `credential_values` contains `.env` secrets; only the
            // TUI control surface and term-chat (OAuth login) may see
            // them. Other modules get the structural list redacted.
            let expose_creds = may_read_other_credentials(module_name);
            for (name, discovered) in discovered_registry.lock().unwrap().iter() {
                let cred_values = credential_values_map(discovered);
                let config_complete = is_config_complete(&discovered.manifest.credentials, &cred_values);
                let exposed_values = if expose_creds {
                    cred_values
                } else {
                    std::collections::HashMap::new()
                };
                entries.insert(
                    name.clone(),
                    serde_json::json!({
                        "name": name,
                        "description": discovered.manifest.description,
                        "uuid7": null,
                        "position": "unknown",
                        "priority": null,
                        "autostart": current_autostart(discovered),
                        "price": discovered.manifest.price,
                        "min_rank": discovered.manifest.min_rank,
                        "authority": discovered.manifest.authority,
                        "avg_ms": module_timings.get(name.as_str()).copied(),
                        "connected_at": null,
                        "shutdown_at": null,
                        "credentials": discovered.manifest.credentials,
                        "directory": discovered.directory.to_string_lossy().to_string(),
                        "credential_values": exposed_values,
                        "config_complete": config_complete,
                    }),
                );
            }

            // 2. Registered modules — persisted knowledge from modules.json
            for m in &registered {
                let entry = entries
                    .entry(m.name.clone())
                    .or_insert_with(|| serde_json::json!({
                        "name": m.name,
                        "uuid7": null,
                        "position": "unknown",
                        "priority": null,
                        "autostart": null,
                        "avg_ms": null,
                        "connected_at": null,
                        "shutdown_at": null,
                    }));
                entry["uuid7"] = serde_json::json!(m.instance_uuid7);
                entry["avg_ms"] = module_timings.get(m.name.as_str()).copied().map(|v| serde_json::json!(v)).unwrap_or(serde_json::Value::Null);
                // Position comes from the CONFIG ordering lists, not the
                // connect-time registration in modules.json — an operator stage
                // move rewrites config.json and must be what a UI reports, even
                // for a module that is not currently connected. Only fall back
                // to the recorded position if the module is not in any list.
                entry["position"] = serde_json::json!(
                    module_position_from_config(&config, &m.name).unwrap_or_else(|| m.position.clone())
                );
                entry["priority"] = serde_json::json!(m.priority);
            }

            // 3. Live sessions — overlay current state on top.
            // Shutdown sessions (disconnected but cleanup not
            // yet finished) are NOT live: they must not appear
            // as connected (that made UIs probe a dead module
            // and restart it).
            for s in sessions.iter().filter(|s| s.shutdown_at.is_none()) {
                let entry = entries
                    .entry(s.module_name.clone())
                    .or_insert_with(|| serde_json::json!({
                        "name": s.module_name,
                        "uuid7": null,
                        "position": "unknown",
                        "priority": null,
                        "autostart": null,
                        "avg_ms": null,
                        "connected_at": null,
                        "shutdown_at": null,
                    }));
                entry["uuid7"] = serde_json::json!(s.instance_uuid7);
                entry["avg_ms"] = module_timings.get(s.module_name.as_str()).copied().map(|v| serde_json::json!(v)).unwrap_or(serde_json::Value::Null);
                entry["position"] = serde_json::json!(module_position_from_config(&config, &s.module_name).unwrap_or_else(|| s.position.clone()));
                entry["priority"] = serde_json::json!(s.priority);
                entry["connected_at"] = serde_json::json!(s.connected_at);
                entry["shutdown_at"] = serde_json::json!(s.shutdown_at);
                entry["alive"] = serde_json::json!(!s.unresponsive);
                entry["last_seen"] = serde_json::json!(s.last_activity_ms);
                entry["pending_prompt"] =
                    serde_json::json!(pending_prompts.contains(&s.module_name));
            }

            let mut list: Vec<serde_json::Value> = entries.into_values().collect();
            sort_module_list(&config, &mut list);
            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".to_string());
            QueryOutcome::success(json.into_bytes())
        }
        QueryRoute::EngineInfo => {
            let config = get_config(config_state);
            // The PIN is the master key for first connections — only
            // the TUI control surface may read it. Other callers get
            // connection metadata only.
            let pin = if is_control_surface(module_name) {
                serde_json::json!(crate::config::get_pin(config_state))
            } else {
                serde_json::Value::Null
            };
            let json = serde_json::json!({
                "port": config.port,
                "pin": pin,
                "timeline_database_location": config.timeline_database_location,
            }).to_string();
            QueryOutcome::success(json.into_bytes())
        }
        QueryRoute::PipelineSetPaused => {
            // Hold or release the whole message pipeline. Control surface only
            // (the gate above), and the one operation here that changes engine
            // state rather than reading or writing engine DATA.
            //
            // Paused: the engine keeps ingesting, parsing commands and writing
            // the timeline, but dispatches nothing to any module; messages
            // accumulate as 'queued' and nothing is lost. Resumed: it works
            // through exactly that backlog. Expects JSON: { "paused": true|false }.
            let payload: serde_json::Value =
                serde_json::from_str(sql).unwrap_or(serde_json::json!({}));
            let Some(paused) = payload.get("paused").and_then(|v| v.as_bool()) else {
                return QueryOutcome::denied("pipeline_set_paused requires paused: true or false");
            };
            if paused {
                let changed = orchestrator.pause().await;
                if changed {
                    log_event_broadcast(ui_state, "[pipeline] paused by the control surface — messages are queued but nothing is dispatched");
                }
                let json = serde_json::json!({
                    "paused": true,
                    "changed": changed,
                    "held_messages": orchestrator.held_count().await,
                }).to_string();
                QueryOutcome::success(json.into_bytes())
            } else {
                let outcome = orchestrator.resume().await;
                if outcome.was_paused {
                    log_event_broadcast(
                        ui_state,
                        format!("[pipeline] resumed by the control surface — releasing {} held message(s)", outcome.resumed_messages),
                    );
                }
                for error in &outcome.errors {
                    log_event_broadcast(ui_state, format!("[pipeline] resume: {}", error));
                }
                let json = serde_json::json!({
                    "paused": false,
                    "changed": outcome.was_paused,
                    "resumed_messages": outcome.resumed_messages,
                    "errors": outcome.errors,
                }).to_string();
                QueryOutcome::success(json.into_bytes())
            }
        }
        QueryRoute::EngineShutdown => engine_shutdown(config_state, shutdown, ui_state, module_name),
        QueryRoute::TestRun => {
            // Compliance test runner — TUI-only gate, mirrors userdb.
            let engine_pin = crate::config::get_pin(config_state);
            run_test_suite(sql, ui_state, engine_pin).await.into()
        }
        QueryRoute::TestProbe => {
            // Per-module probe: the engine sends a payload of the
            // requested type to one module's connection and
            // measures the round-trip. TUI/test-runner gated.
            match handle_test_probe(sql, orchestrator, auth_store, ui_state).await {
                Ok(json) => QueryOutcome::success(json.into_bytes()),
                Err(e) => QueryOutcome::denied(e),
            }
        }
        QueryRoute::AuditList => {
            // List held-for-audit messages. TUI-only.
            let payload: serde_json::Value = serde_json::from_str(sql).unwrap_or(serde_json::json!({}));
            let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;
            let offset = payload.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            match db.list_audit(limit, offset).await {
                Ok(items) => {
                    let json = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
                    QueryOutcome::success(json.into_bytes())
                }
                Err(e) => QueryOutcome::denied(format!("audit_list failed: {}", e)),
            }
        }
        QueryRoute::AuditApprove | QueryRoute::AuditReject => {
            // Approve (resubmit as normal) or reject an audited message. TUI-only.
            let payload: serde_json::Value = serde_json::from_str(sql).unwrap_or(serde_json::json!({}));
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            if uuid7.is_empty() {
                QueryOutcome::denied("audit action requires uuid7")
            } else {
                let approve = route == QueryRoute::AuditApprove;
                match db.release_audit(uuid7.as_bytes(), approve).await {
                    Ok(_) => {
                        let json = serde_json::json!({ "uuid7": uuid7, "released": approve }).to_string();
                        QueryOutcome::success(json.into_bytes())
                    }
                    Err(e) => QueryOutcome::denied(format!("audit action failed: {}", e)),
                }
            }
        }
        QueryRoute::ModFamily => {
            // Mod commands from adapters: resolve the target user
            // by platform + handle, then apply the action. The
            // actor (human trigger) is verified first.
            mod_virtual_query(user_db_client, query_id, sql, ui_state, module_name).await.into()
        }
        QueryRoute::ChatCommend | QueryRoute::ChatReprimand => {
            // Chat-command ratings (commend/reprimand modules):
            // ANY verified user may rate another user (no mod
            // status required). The 24h reprimand cooldown is
            // enforced in the user-db rating_history.
            chat_rating_virtual_query(user_db_client, query_id, sql, ui_state, module_name).await.into()
        }
        QueryRoute::ChatVerifyIdentity => {
            // Identity bootstrap / write-through — term-chat only.
            chat_verify_identity_virtual_query(user_db_client, sql, ui_state, module_name).await.into()
        }
        QueryRoute::UserdbAdjustScore => {
            // Real arbitrary score delta for the score-messages
            // module (no ±1 clamp, no user-db rating cooldown).
            // The TUI control surface may also call it.
            userdb_adjust_score_virtual_query(user_db_client, sql, ui_state, module_name)
                .await
                .into()
        }
        QueryRoute::PredictionGetScore => {
            // Read a user's current score BEFORE the predictions module
            // places a bet. uuid-only: the brain passes the actor's
            // `chat.user_uuid7` directly — no platform+handle resolution.
            prediction_get_score_virtual_query(user_db_client, sql).await.into()
        }
        QueryRoute::ChannelViewers => {
            // Read the engine's in-memory per-channel viewer/member counts.
            // Public stream data, so no caller gate — any authenticated module
            // may read it on demand.
            channel_viewers_virtual_query(ctx.channel_stats, sql).into()
        }
        QueryRoute::UserdbFamily => {
            // User database is engine-internal — only the TUI
            // control surface may access it. Modules are denied.
            userdb_virtual_query(user_db_client, query_id, sql, ui_state, module_name, *peer_loopback)
                .await
                .into()
        }
        QueryRoute::SetCredentials => {
            // Writing credentials to another module's `.env` /
            // `config.json` is a control-surface operation — only
            // the TUI may do it. Otherwise any authenticated module
            // could rewrite any other module's secrets.
            // Expects JSON: { "module_name": "...", "values": { "key": "value", ... } }
            //
            // NOTE: the `module_name` bound below SHADOWS the caller's name for
            // the rest of this branch — the log line and the "Unknown module"
            // error name the TARGET module, not the caller. Preserved as-is.
            let mut result =
                QueryOutcome::denied("Failed to parse set_credentials payload");
            if let Ok(payload) = serde_json::from_str::<serde_json::Value>(sql) {
                let module_name = payload.get("module_name").and_then(|v| v.as_str()).unwrap_or("");
                let values: HashMap<String, String> = payload
                    .get("values")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();

                let found = discovered_registry.lock().unwrap().get(module_name).cloned();
                match found {
                    Some(module) => {
                        let fields = module.manifest.credentials.clone();
                        match validate_credential_fields(&fields, &values) {
                            Ok(()) => {
                                match save_module_credentials(&module, &fields, &values) {
                                    Ok(()) => {
                                        log_event_broadcast(ui_state, format!("[{}] Credentials updated for '{}'", module_name, module_name));
                                        // The TUI supervisor owns process lifecycle — it restarts the
                                        // module after this query so the new config is picked up.
                                        result = QueryOutcome::success(serde_json::json!({"success": true, "message": format!("Credentials saved for '{}'", module_name)}).to_string().into_bytes());
                                    }
                                    Err(e) => result = QueryOutcome::denied(e),
                                }
                            }
                            Err(e) => result = QueryOutcome::denied(e),
                        }
                    }
                    None => result = QueryOutcome::denied(format!("Unknown module: {}", module_name)),
                }
            }
            result
        }
        QueryRoute::AudioForMessage => {
            // Fetch a message's rendered audio (raw bytes) so
            // displays can play it. Any module may read it.
            //
            // NOTE: the blob is the raw audio, not JSON. Displays base64 it
            // themselves. Preserved as-is.
            let payload: serde_json::Value =
                serde_json::from_str(sql).unwrap_or(serde_json::json!({}));
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            if uuid7.is_empty() {
                QueryOutcome::denied("audio_for_message requires uuid7")
            } else {
                match db.get_audio(uuid7.as_bytes()).await {
                    Ok(Some((_audio_type, bytes))) => QueryOutcome::success(bytes),
                    Ok(None) => QueryOutcome::success(Vec::new()),
                    Err(e) => QueryOutcome::denied(format!("audio_for_message failed: {}", e)),
                }
            }
        }
        QueryRoute::TestArchive => {
            // Compliance-test archival — test-runner only. The
            // engine inserts archival rows via its own method
            // (insert_archival_event) rather than allowing raw
            // INSERTs through the SQL fallback.
            let payload: serde_json::Value =
                serde_json::from_str(sql).unwrap_or(serde_json::json!({}));
            let batch_uuid = payload.get("batch_uuid").and_then(|v| v.as_str()).unwrap_or("test");
            let mut inserted = 0u64;
            let mut error = String::new();
            if let Some(entries) = payload.get("entries").and_then(|v| v.as_array()) {
                for e in entries {
                    let json = e.get("json").and_then(|v| v.as_str()).unwrap_or("");
                    if json.is_empty() {
                        continue;
                    }
                    match db
                        .insert_archival_event("test", "test_archive", json, &format!("test-runner|{}", batch_uuid))
                        .await
                    {
                        Ok(()) => inserted += 1,
                        Err(e) => {
                            error = format!("{}", e);
                            break;
                        }
                    }
                }
            }
            if error.is_empty() {
                let out = serde_json::json!({ "inserted": inserted }).to_string();
                QueryOutcome::success(out.into_bytes())
            } else {
                QueryOutcome::denied(format!("test_archive failed after {} inserts: {}", inserted, error))
            }
        }
        QueryRoute::Stats => {
            // One-shot timeline aggregates for the control surface (the TUI
            // used to fire six raw-SQL stats queries every poll). Computed
            // engine-internally — a module can never reach this SQL.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let five_min_ago = now_ms - (5 * 60 * 1000);
            let bucket_ms = 10_000i64;

            let mut out = serde_json::Map::new();

            let total_messages = db
                .execute_query(
                    "SELECT COUNT(*) AS n FROM timeline_events WHERE pipeline_status != 'audit'",
                )
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = total_messages {
                out.insert("total_messages".into(), v[0]["n"].clone());
            }
            let total_users = db
                .execute_query(
                    "SELECT COUNT(DISTINCT user_uuid7) AS n FROM timeline_events WHERE user_uuid7 != '' AND user_uuid7 IS NOT NULL AND pipeline_status != 'audit'",
                )
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = total_users {
                out.insert("total_users".into(), v[0]["n"].clone());
            }
            let total_commands = db
                .execute_query(
                    "SELECT COUNT(*) AS n FROM timeline_events WHERE command != '' AND command IS NOT NULL AND pipeline_status != 'audit'",
                )
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = total_commands {
                out.insert("total_commands".into(), v[0]["n"].clone());
            }
            let platform_counts = db
                .execute_query(
                    "SELECT platform, COUNT(*) AS n FROM timeline_events WHERE pipeline_status != 'audit' GROUP BY platform",
                )
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = platform_counts {
                out.insert("platform_counts".into(), v);
            }
            let platform_errors = db
                .execute_query(
                    "SELECT platform, COUNT(*) AS n FROM timeline_events WHERE pipeline_status = 'failed' GROUP BY platform",
                )
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = platform_errors {
                out.insert("platform_errors".into(), v);
            }
            let chart_data = db
                .execute_query(&format!(
                    "SELECT (persisted_at / {bucket}) * {bucket} AS bucket, platform, COUNT(*) AS n FROM timeline_events WHERE persisted_at > {since} AND pipeline_status != 'audit' GROUP BY bucket, platform ORDER BY bucket ASC",
                    bucket = bucket_ms,
                    since = five_min_ago,
                ))
                .await
                .ok()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
            if let Some(v) = chart_data {
                out.insert("chart_data".into(), v);
            }

            QueryOutcome::success(serde_json::to_string(&out).unwrap_or_default().into_bytes())
        }
        // There is no raw-SQL escape hatch: a query the engine does not name is
        // denied outright (this route is unreachable past the caller_gate, which
        // denies Unsupported, but it exists so the match is exhaustive).
        QueryRoute::Unsupported => QueryOutcome::denied("query denied: no such operation"),
    }
}

/// The `engine_shutdown` branch body, split out of [`dispatch`] so the flag
/// decision and the shutdown signal can be exercised without a live socket —
/// this is the load-bearing half of the operation and the part that decides
/// whether the process goes down at all.
///
/// Two gates, in this order, and both must pass:
///
/// 1. [`caller_gate`] — the TUI control surface only (done by the caller).
/// 2. The engine's OWN `shutdown_on_request` config flag. A connected module
///    can therefore never talk the engine down on its own authority; the
///    operator has to have said yes in config.json first.
///
/// Ordering of the shutdown itself: this function only moves the signal to
/// [`ShutdownStage::Requested`] and returns the outcome. It does NOT release
/// the exit — the response has still to be built, queued and written to the
/// caller's socket by the connection task, and only when that write completes
/// does the connection task call [`ShutdownSignal::mark_answered`]. The main
/// loop waits for `Answered` before it exits, so the caller always learns
/// whether its request was accepted instead of finding a dead socket.
fn engine_shutdown(
    config_state: &Arc<Mutex<ConfigState>>,
    shutdown: &SharedShutdown,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> QueryOutcome {
    // Read through `get_config`, like `engine_info` does, so an operator who
    // flips the flag does not have to restart the engine for the next request to
    // be honoured. (The flag is only consulted here; a shutdown already in
    // flight is not revoked by turning it back off.)
    if !get_config(config_state).shutdown_on_request {
        log_event_broadcast(
            ui_state,
            format!("[{}] engine_shutdown refused: shutdown_on_request is false in config.json", requester),
        );
        // The engine keeps running — an unconfigured engine is not killable
        // over the wire, and the refusal says which key would change that.
        return QueryOutcome::denied(ENGINE_SHUTDOWN_DISABLED);
    }

    log_event_broadcast(
        ui_state,
        format!("[{}] engine_shutdown: accepted — answering, then exiting", requester),
    );
    // Requested, not answered: the response for this very query is still to be
    // written. See the doc comment above for who closes that gap.
    shutdown.request();
    let json = serde_json::json!({ "shutdown": true, "requested_by": requester }).to_string();
    QueryOutcome::success(json.into_bytes())
}

/// The refusal the engine returns when `shutdown_on_request` is false. Names
/// the config key, because the caller's only useful move is to go and set it.
const ENGINE_SHUTDOWN_DISABLED: &str =
    "engine_shutdown denied: engine shutdown is disabled (shutdown_on_request is false in config.json)";

/// Handle a `test_run` virtual query from the TUI. The SQL field carries a JSON
/// payload: { "suite": "chain"|"modules"|"all", "module": "name?", "iterations": n }.
/// Spawns the compliance test runner, captures its output, and returns it.
async fn run_test_suite(
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    engine_pin: u32,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid test_run payload: {}", e)),
    };

    let suite = payload.get("suite").and_then(|v| v.as_str()).unwrap_or("all");
    let module = payload.get("module").and_then(|v| v.as_str()).unwrap_or("");
    let iterations = payload.get("iterations").and_then(|v| v.as_i64()).unwrap_or(100);

    let mut args = vec!["--suite".to_string(), suite.to_string()];
    if !module.is_empty() {
        args.push("--module".to_string());
        args.push(module.to_string());
    }
    args.push("--iterations".to_string());
    args.push(iterations.to_string());
    args.push("--json".to_string());

    // Locate the runner binary relative to the engine's working directory.
    let engine_dir = env::current_dir().unwrap_or_default();
    let runner_dir = engine_dir.parent().unwrap_or(&engine_dir).join("cockatiel_test_runner-rs");
    let runner_bin = runner_dir.join("target").join("release").join("cockatiel-test-runner");
    let runner_bin = if runner_bin.exists() {
        runner_bin
    } else {
        runner_dir.join("target").join("debug").join("cockatiel-test-runner")
    };

    if !runner_bin.exists() {
        return (
            false,
            Vec::new(),
            format!("test-runner binary not found at {}", runner_bin.display()),
        );
    }

    log_event(ui_state, format!("[test] running suite '{}' (module: '{}', n={})", suite, module, iterations));

    let output = match tokio::time::timeout(
        Duration::from_secs(300),
        tokio::process::Command::new(&runner_bin)
            .args(&args)
            .current_dir(runner_dir)
            .env("COCKATIEL_PIN", engine_pin.to_string())
            .output(),
    )
    .await
    {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return (false, Vec::new(), format!("failed to spawn test runner: {}", e)),
        Err(_) => return (false, Vec::new(), "test runner timed out after 300s".to_string()),
    };

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Surface progress lines as engine logs so the TUI shows them live.
    for line in stdout.lines() {
        log_event(ui_state, format!("[test] {}", line));
    }
    if !stderr.trim().is_empty() {
        log_event(ui_state, format!("[test] stderr: {}", stderr.trim()));
    }

    let exit_ok = output.status.success();
    (exit_ok, stdout.into_bytes(), if exit_ok { String::new() } else { stderr })
}

/// Handle a `userdb_*` virtual query from the TUI. The SQL field carries a JSON
/// payload specific to each operation. Only the engine's control surface (TUI)
/// may reach the user database.
async fn userdb_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
    peer_loopback: bool,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid userdb payload: {}", e)),
    };

    let outcome = match query_id {
        "userdb_add_user" => {
            let username = payload.get("username").and_then(|v| v.as_str()).unwrap_or("");
            let channel = parse_channel_ref(payload.get("channel"));
            client.add_user(username, channel.as_ref()).await
        }
        "userdb_delete_user" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor = payload.get("actor_uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let role = userdb_actor_perm(peer_loopback, payload.get("actor_role").and_then(|v| v.as_str()).unwrap_or("user"));
            client.delete_user(uuid7, actor, &role).await
        }
        "userdb_add_score" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.add_score(uuid7, delta, reason).await
        }
        "userdb_remove_score" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_score(uuid7, delta, reason).await
        }
        "userdb_add_channel" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let channel = parse_channel_ref(payload.get("channel"));
            client.add_channel(uuid7, channel.as_ref()).await
        }
        "userdb_remove_channel" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let channel_id = payload.get("channel_id").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_channel(uuid7, platform, channel_id).await
        }
        "userdb_get_user" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let channel_id = payload.get("channel_id").and_then(|v| v.as_str()).unwrap_or("");
            let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
            client.get_user(uuid7, platform, channel_id, handle).await
        }
        "userdb_list_users" => {
            let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;
            let offset = payload.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            client.list_users(platform, limit, offset).await
        }
        "userdb_update_flags" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let flags = payload.get("flags").and_then(|v| v.as_str()).unwrap_or("{}");
            client.update_flags(uuid7, flags).await
        }
        "userdb_set_roles" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor = payload.get("actor_uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let role = userdb_actor_perm(peer_loopback, payload.get("actor_role").and_then(|v| v.as_str()).unwrap_or("user"));
            let sponsor = payload.get("is_sponsor").and_then(|v| v.as_bool()).unwrap_or(false);
            let mod_ = payload.get("is_moderator").and_then(|v| v.as_bool()).unwrap_or(false);
            let admin = payload.get("is_admin").and_then(|v| v.as_bool()).unwrap_or(false);
            let owner = payload.get("is_owner").and_then(|v| v.as_bool()).unwrap_or(false);
            client.set_roles(uuid7, actor, &role, sponsor, mod_, admin, owner).await
        }
        "userdb_read_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            client.read_user_value(uuid7, key).await
        }
        "userdb_write_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let value = payload.get("value").and_then(|v| v.as_str()).unwrap_or("");
            client.write_user_value(uuid7, key, value).await
        }
        "userdb_delete_user_value" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let key = payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
            client.delete_user_value(uuid7, key).await
        }
        "userdb_list_user_values" => {
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            client.list_user_values(uuid7).await
        }
        // Mod commands: map to user DB operations.
        "userdb_commendation" => {
            // { uuid7, reason? } → +1 score (commendation)
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.add_score(uuid7, 1, reason).await
        }
        "userdb_reprimand" => {
            // { uuid7, reason? } → -1 score (reprimand)
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            client.remove_score(uuid7, 1, reason).await
        }
        "userdb_ban" => {
            // { uuid7, reason? } → set banned flag value
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let flag = serde_json::json!({ "banned": true, "reason": reason, "at": now }).to_string();
            client.write_user_value(uuid7, "ban", &flag).await
        }
        "userdb_timeout" => {
            // { uuid7, duration_secs, reason? } → set timeout value with expiry
            let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let duration_secs = payload.get("duration_secs").and_then(|v| v.as_i64()).unwrap_or(300);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default();
            let expires = now_ms + (duration_secs as u128 * 1000);
            let flag = serde_json::json!({
                "timed_out": true,
                "reason": reason,
                "expires_at_ms": expires,
            }).to_string();
            client.write_user_value(uuid7, "timeout", &flag).await
        }
        _ => return (false, Vec::new(), format!("Unknown userdb query: {}", query_id)),
    };

    match outcome {
        Ok(resp) => {
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] UserDB error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle the `userdb_adjust_score` virtual query: apply a REAL arbitrary delta
/// to a user's `score` (no ±1 clamp, no user-db rating cooldown, no counter
/// inflation). Unlike `mod_commend`/`mod_reprimand` it accepts any signed delta
/// and uses the user-db's dedicated score-only op.
///
/// Gate: the score-messages module, the predictions module, or the TUI control
/// surface may call it.
async fn userdb_adjust_score_virtual_query(
    client: &SharedUserDbClient,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid userdb_adjust_score payload: {}", e)),
    };

    let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("userdb_adjust_score");

    // Resolve the target user: prefer an explicit uuid7, else by (platform, handle).
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");

    let uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        let resolved = match client.get_user("", platform, handle, handle).await {
            Ok(r) => r,
            Err(e) => return (false, Vec::new(), e),
        };
        if !resolved.success {
            log_event(ui_state, format!("[{}] userdb_adjust_score: target '{}' not found on '{}'", requester, handle, platform));
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        }
        let Some(user) = resolved.user else {
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        };
        user.uuid7
    } else {
        return (false, Vec::new(), "userdb_adjust_score requires uuid7 or platform + handle".to_string());
    };

    if delta == 0 {
        let json = serde_json::json!({ "success": true, "uuid7": uuid7, "delta": 0 }).to_string();
        return (true, json.into_bytes(), String::new());
    }

    let outcome = client.adjust_score_only(&uuid7, delta, reason).await;

    match outcome {
        Ok(resp) => {
            log_event(ui_state, format!("[{}] userdb_adjust_score {} by {} -> {}", requester, delta, requester, uuid7));
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] userdb_adjust_score error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle the `prediction_get_score` virtual query: read a user's CURRENT
/// score before the predictions module places a bet.
///
/// The payload is uuid-only: `{ "uuid7": "<user uuid7>" }` — the brain passes
/// the actor's `chat.user_uuid7` directly, no platform + handle resolution.
/// On success the answer is `{ "uuid7": "...", "score": <i64> }`; a missing or
/// unknown user is an error, in the same style as `userdb_adjust_score`.
async fn prediction_get_score_virtual_query(
    client: &SharedUserDbClient,
    sql: &str,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid prediction_get_score payload: {}", e)),
    };

    let uuid7 = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    if uuid7.is_empty() {
        return (false, Vec::new(), "prediction_get_score requires uuid7".to_string());
    }

    let resolved = match client.get_user(uuid7, "", "", "").await {
        Ok(r) => r,
        Err(e) => return (false, Vec::new(), e),
    };
    if !resolved.success {
        return (false, Vec::new(), format!("User '{}' not found", uuid7));
    }
    let Some(user) = resolved.user else {
        return (false, Vec::new(), format!("User '{}' not found", uuid7));
    };

    let json = serde_json::json!({ "uuid7": user.uuid7, "score": user.score }).to_string();
    (true, json.into_bytes(), String::new())
}

/// Read the engine's in-memory per-channel viewer/member counts, as pushed by
/// the platform adapters. Returns a JSON object of every known
/// platform+channel with its current `viewers`, `is_live`, `title` and
/// `updated_at`. Open to every authenticated module — the data is public
/// stream information. The optional `sql` body may filter with
/// `{"platform": "twitch"}`.
fn channel_viewers_virtual_query(
    channel_stats: &SharedChannelStats,
    sql: &str,
) -> (bool, Vec<u8>, String) {
    let filter: Option<String> = match serde_json::from_str::<serde_json::Value>(sql) {
        Ok(v) => v
            .get("platform")
            .and_then(|p| p.as_str())
            .map(|p| p.to_string()),
        Err(_) => None,
    };

    let entries: Vec<serde_json::Value> = {
        let stats = channel_stats.lock().unwrap();
        stats
            .values()
            .filter(|e| filter.as_deref().is_none_or(|p| e.platform == p))
            .map(|e| {
                serde_json::json!({
                    "platform": e.platform,
                    "channel": e.channel,
                    "viewers": e.viewers,
                    "is_live": e.is_live,
                    "title": e.title,
                    "updated_at": e.updated_at,
                })
            })
            .collect()
    };

    let json = serde_json::json!({ "channels": entries }).to_string();
    (true, json.into_bytes(), String::new())
}

/// Handle a `mod_*` virtual query from an adapter (e.g. a mod typing a command
/// in chat). The adapter identifies the target by platform + handle; the engine
/// resolves the user DB record and applies the action.
/// Query IDs: mod_commend, mod_reprimand, mod_ban, mod_timeout.
///
/// The actor (the human triggering the action) MUST be verified against the
/// user database before any privileged action runs; a missing or unauthorized
/// actor is rejected. The only exception is the SYSTEM path: the automated
/// scorer module ("score-messages") applies ±1 score deltas (mod_commend /
/// mod_reprimand) without a human actor.
async fn mod_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid mod payload: {}", e)),
    };

    // ── Actor verification ──────────────────────────────────────────────
    // The optional `actor` object is { "platform", "handle", "uuid7" }.
    let actor = payload.get("actor").filter(|a| !a.is_null());
    let (actor_uuid7, actor_handle) = match actor {
        Some(actor) => {
            let actor_uuid = actor.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
            let actor_platform = actor.get("platform").and_then(|v| v.as_str()).unwrap_or("");
            let actor_handle = actor.get("handle").and_then(|v| v.as_str()).unwrap_or("");
            // Prefer a uuid7; otherwise resolve by (platform, handle).
            let resolved = if !actor_uuid.is_empty() {
                client.get_user(actor_uuid, "", "", "").await
            } else {
                client.get_user("", actor_platform, actor_handle, actor_handle).await
            };
            let resolved = match resolved {
                Ok(r) => r,
                Err(e) => return (false, Vec::new(), format!("Actor lookup failed: {}", e)),
            };
            let Some(user) = resolved.user else {
                let actor_where = if actor_platform.is_empty() { String::new() } else { format!(" on {}", actor_platform) };
                log_event(
                    ui_state,
                    format!("[{}] {} denied: actor '{}{}' not found in user DB", requester, query_id, actor_handle, actor_where),
                );
                return (false, Vec::new(), "permission denied: actor is not a moderator/admin/owner".to_string());
            };
            if !(user.is_moderator || user.is_admin || user.is_owner) {
                log_event(
                    ui_state,
                    format!("[{}] {} denied: actor '{}' lacks a mod/admin/owner role", requester, query_id, user.username),
                );
                return (false, Vec::new(), "permission denied: actor is not a moderator/admin/owner".to_string());
            }
            (user.uuid7, user.username)
        }
        None => {
            // SYSTEM path: the automated scorer applies ±1 score deltas for
            // every message. No other mod_* action may run without an actor.
            if !matches!(query_id, "mod_commend" | "mod_reprimand") || requester != "score-messages" {
                log_event(
                    ui_state,
                    format!("[{}] {} denied: missing actor (module '{}')", requester, query_id, requester),
                );
                return (false, Vec::new(), "permission denied: missing actor".to_string());
            }
            (String::new(), format!("system:{}", requester))
        }
    };

    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
    let duration_secs = payload.get("duration_secs").and_then(|v| v.as_i64()).unwrap_or(300);

    // Resolve the target user: prefer an explicit uuid7, else by (platform, handle).
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        let resolved = match client.get_user("", platform, handle, handle).await {
            Ok(r) => r,
            Err(e) => return (false, Vec::new(), e),
        };
        if !resolved.success {
            log_event(ui_state, format!("[{}] mod: target '{}' not found on '{}'", requester, handle, platform));
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        }
        let Some(user) = resolved.user else {
            return (false, Vec::new(), format!("User '{}' not found on {}", handle, platform));
        };
        user.uuid7
    } else {
        return (false, Vec::new(), "mod query requires uuid7 or platform + handle".to_string());
    };

    let outcome = match query_id {
        "mod_commend" => client.add_score(&uuid7, 1, reason).await,
        "mod_reprimand" => client.remove_score(&uuid7, 1, reason).await,
        "mod_ban" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let flag = serde_json::json!({ "banned": true, "reason": reason, "at": now }).to_string();
            client.write_user_value(&uuid7, "ban", &flag).await
        }
        "mod_timeout" => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default();
            let expires = now_ms + (duration_secs as u128 * 1000);
            let flag = serde_json::json!({
                "timed_out": true,
                "reason": reason,
                "expires_at_ms": expires,
            }).to_string();
            client.write_user_value(&uuid7, "timeout", &flag).await
        }
        _ => return (false, Vec::new(), format!("Unknown mod query: {}", query_id)),
    };

    match outcome {
        Ok(resp) => {
            log_event(
                ui_state,
                format!("[{}] {} by {} -> {} (actor={})", requester, query_id, actor_handle, handle, actor_uuid7),
            );
            let json = userdb_response_to_json(&resp);
            (resp.success, json.into_bytes(), if resp.success { String::new() } else { resp.error.clone() })
        }
        Err(e) => {
            log_event(ui_state, format!("[{}] Mod command error: {}", requester, e));
            (false, Vec::new(), e)
        }
    }
}

/// Handle a `chat_commend` / `chat_reprimand` virtual query from the commend /
/// reprimand command modules. Unlike `mod_*`, the actor does NOT need mod
/// status — any verified user may rate another user. The user-db enforces the
/// 24h reprimand cooldown atomically (rating_history); a denial returns
/// success=false with the cooldown reason.
async fn chat_rating_virtual_query(
    client: &SharedUserDbClient,
    query_id: &str,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    requester: &str,
) -> (bool, Vec<u8>, String) {
    // Gate to the two dedicated modules so no random module can fake ratings.
    let expected = if query_id == "chat_commend" { "commend" } else { "reprimand" };
    if requester != expected {
        log_event(
            ui_state,
            format!("[{}] {} denied: requester '{}' is not the '{}' module", requester, query_id, requester, expected),
        );
        return (false, Vec::new(), format!("{} denied: not the '{}' module", query_id, expected));
    }

    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid rating payload: {}", e)),
    };

    // Actor (the giver) — resolve + require existence (ANY role is fine).
    let actor = payload.get("actor").filter(|a| !a.is_null());
    let Some(actor) = actor else {
        return (false, Vec::new(), "rating requires an actor".to_string());
    };
    let actor_uuid = actor.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let actor_platform = actor.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let actor_handle = actor.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let giver_uuid7 = if !actor_uuid.is_empty() {
        actor_uuid.to_string()
    } else if !actor_platform.is_empty() && !actor_handle.is_empty() {
        match client.get_user("", actor_platform, actor_handle, actor_handle).await {
            Ok(resp) if resp.success && resp.user.is_some() => resp.user.unwrap().uuid7,
            Ok(_) => {
                log_event(ui_state, format!("[{}] rating denied: giver '{}' not found", requester, actor_handle));
                return (false, Vec::new(), format!("giver '{}' not found on {}", actor_handle, actor_platform));
            }
            Err(e) => return (false, Vec::new(), format!("giver lookup failed: {}", e)),
        }
    } else {
        return (false, Vec::new(), "rating actor requires uuid7 or platform+handle".to_string());
    };

    // Target (the recipient) — resolve by uuid7 or platform + handle.
    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");
    let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("");
    let explicit_uuid = payload.get("uuid7").and_then(|v| v.as_str()).unwrap_or("");
    let recipient_uuid7 = if !explicit_uuid.is_empty() {
        explicit_uuid.to_string()
    } else if !platform.is_empty() && !handle.is_empty() {
        match client.get_user("", platform, handle, handle).await {
            Ok(resp) if resp.success && resp.user.is_some() => resp.user.unwrap().uuid7,
            Ok(_) => {
                log_event(ui_state, format!("[{}] rating denied: target '{}' not found", requester, handle));
                return (false, Vec::new(), format!("target '{}' not found on {}", handle, platform));
            }
            Err(e) => return (false, Vec::new(), format!("target lookup failed: {}", e)),
        }
    } else {
        return (false, Vec::new(), "rating requires uuid7 or platform+handle".to_string());
    };

    if recipient_uuid7 == giver_uuid7 {
        return (false, Vec::new(), "you cannot rate yourself".to_string());
    }

    let is_commendation = query_id == "chat_commend";
    match client
        .rate_user(&giver_uuid7, &recipient_uuid7, is_commendation, platform, handle, reason)
        .await
    {
        Ok(resp) => {
            if resp.success {
                log_event(ui_state, format!("[{}] {} applied to '{}'", requester, query_id, handle));
                (true, resp.message.into_bytes(), String::new())
            } else {
                // Cooldown denial — surface the reason to the module (it logs it).
                let err = if resp.error.is_empty() { resp.message } else { resp.error };
                log_event(ui_state, format!("[{}] {} denied: {}", requester, query_id, err));
                (false, Vec::new(), err)
            }
        }
        Err(e) => (false, Vec::new(), format!("rating failed: {}", e)),
    }
}

/// Handle a `chat_verify_identity` virtual query from the terminal chat module.
/// This is the identity bootstrap / write-through path: term-chat sends the
/// platform identity it just authenticated and the roles the platform vouches
/// for. The engine creates-or-finds the user and elevates their roles to the
/// union of existing + verified roles (ELEVATE, never revoke).
async fn chat_verify_identity_virtual_query(
    client: &SharedUserDbClient,
    sql: &str,
    ui_state: &Arc<Mutex<EngineState>>,
    module_name: &str,
) -> (bool, Vec<u8>, String) {
    if module_name != "term-chat" {
        log_event(ui_state, format!("[{}] chat_verify_identity denied: not term-chat", module_name));
        return (false, Vec::new(), "access denied: not term-chat".to_string());
    }

    let payload: serde_json::Value = match serde_json::from_str(sql) {
        Ok(v) => v,
        Err(e) => return (false, Vec::new(), format!("Invalid chat_verify_identity payload: {}", e)),
    };

    let platform = payload.get("platform").and_then(|v| v.as_str()).unwrap_or("");
    let handle = payload.get("handle").and_then(|v| v.as_str()).unwrap_or("");

    // Create-or-find the user keyed by channel (the service already does
    // find-by-channel and returns the existing record).
    let channel = parse_channel_ref(Some(&serde_json::json!({
        "platform": platform,
        "channel_id": "",
        "handle": handle,
    })));
    let add_resp = match client.add_user(handle, channel.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            log_event(ui_state, format!("[{}] chat_verify_identity userdb error: {}", module_name, e));
            return (false, Vec::new(), e);
        }
    };
    if !add_resp.success {
        return (false, Vec::new(), add_resp.error.clone());
    }
    let Some(user) = add_resp.user else {
        return (false, Vec::new(), "chat_verify_identity: no user returned".to_string());
    };

    // term-chat asserts its platform-verified roles as booleans under
    // `verified_roles`. The engine must NOT union those onto the stored record
    // (that can never revoke — and a self-asserted login_kick handle would
    // stick elevated roles onto the matching stored user forever). Set each
    // role EXACTLY from the claim so a role that is no longer asserted is
    // revoked (straight assignment, never AND/OR with the stored value).
    // When the payload carries no verified_roles at all, preserve the stored
    // roles (previous behavior) but surface a warning — the claim is untrusted.
    let verified_roles = payload.get("verified_roles");
    let (is_sponsor, is_moderator, is_admin, is_owner) = match verified_roles {
        Some(verified) if verified.is_object() => (
            verified.get("is_sponsor").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_moderator").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_admin").and_then(|v| v.as_bool()).unwrap_or(false),
            verified.get("is_owner").and_then(|v| v.as_bool()).unwrap_or(false),
        ),
        _ => {
            log_event(
                ui_state,
                format!(
                    "[{}] chat_verify_identity: payload for '{}' on '{}' carries no verified_roles — preserving stored roles (unverified claim)",
                    module_name, handle, platform
                ),
            );
            (user.is_sponsor, user.is_moderator, user.is_admin, user.is_owner)
        }
    };

    let set_resp = match client
        .set_roles(&user.uuid7, "", "owner", is_sponsor, is_moderator, is_admin, is_owner)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            log_event(ui_state, format!("[{}] chat_verify_identity set_roles error: {}", module_name, e));
            return (false, Vec::new(), e);
        }
    };
    if !set_resp.success {
        return (false, Vec::new(), set_resp.error.clone());
    }

    log_event(
        ui_state,
        format!("[{}] verified identity: {} on {} (mod={})", module_name, handle, platform, is_moderator),
    );

    let json = userdb_response_to_json(&set_resp);
    (true, json.into_bytes(), String::new())
}

fn parse_channel_ref(value: Option<&serde_json::Value>) -> Option<crate::user_db_client::proto::ChannelRef> {
    let Some(value) = value else {
        return None;
    };
    if value.is_null() {
        return None;
    }
    Some(crate::user_db_client::proto::ChannelRef {
        platform: value.get("platform").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        channel_id: value.get("channel_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        handle: value.get("handle").and_then(|v| v.as_str()).unwrap_or("").to_string(),
    })
}

/// The pipeline stage a module currently sits in, per the engine's `config.json`
/// ordering lists — the AUTHORITATIVE position, because the TUI rewrites those
/// lists at runtime to move modules between stages.
///
/// Returns `None` when the module is not in any ordering list (e.g. a remote or
/// unregistered module); callers fall back to the module's connect-time stage.
fn module_position_from_config(config: &Config, name: &str) -> Option<String> {
    if config.preprocess_modules.iter().any(|m| m.name == name) {
        Some("preprocess".to_string())
    } else if config.inprocess_modules.iter().any(|m| m.name == name) {
        Some("inprocess".to_string())
    } else if config.postprocess_modules.iter().any(|m| m.name == name) {
        Some("postprocess".to_string())
    } else if config.inputs.iter().any(|m| m.name == name) {
        Some("input".to_string())
    } else {
        None
    }
}


/// Build a ContainerForModule payload of the requested probe type. `payload_json`
/// carries optional fields (e.g. a log line, a SQL string). Unknown types
/// yield None (the probe is skipped).
fn build_probe_payload(ptype: &str, json: &serde_json::Value) -> Option<Payload> {
    match ptype {
        "auth_verify" => Some(Payload::AuthVerify(AuthVerify { cur_auth: String::new() })),
        "log" => Some(Payload::Log(Log {
            log: json.get("log").and_then(|v| v.as_str()).unwrap_or("test probe").to_string(),
            blob: vec![],
        })),
        "message_pre_process" => Some(Payload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(ChatMessage {
                platform: json.get("platform").and_then(|v| v.as_str()).unwrap_or("test").to_string(),
                raw_data: vec![],
                raw_message: json.get("raw_message").and_then(|v| v.as_str()).unwrap_or("probe").to_string(),
                user_uuid7: String::new(),
                command: None,
                user_data: None,
                channel_id: String::new(),
            }),
            audio: Vec::new(),
            audio_type: String::new(),
        })),
        "prompt" => Some(Payload::Prompt(Prompt {
            prompt_id_uuid7: Uuid::now_v7().to_string(),
            prompt: json.get("prompt").and_then(|v| v.as_str()).unwrap_or("test probe").to_string(),
            details: json.get("details").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            yes_dialog: String::new(),
            no_dialog: String::new(),
            timeout: 30,
            origin: "cockatiel".to_string(),
            origin_uuid7: String::new(),
            instructions: String::new(),
            link: String::new(),
            input_label: String::new(),
            prompt_type: 0,
        })),
        "shutdown" => Some(Payload::Shutdown(Shutdown { reason: "test probe".to_string() })),
        _ => None,
    }
}

/// `test_probe` virtual query: send a payload of the requested type to ONE
/// module's connection and measure the round-trip (the module's session
/// `last_activity` advancing proves a response). Returns
/// `{ module, type, responded, latency_ms }`.
async fn handle_test_probe(
    sql: &str,
    orchestrator: &PipelineOrchestrator,
    auth_store: &AuthStore,
    ui_state: &Arc<Mutex<EngineState>>,
) -> Result<String, String> {
    let payload: serde_json::Value =
        serde_json::from_str(sql).map_err(|e| format!("Invalid test_probe payload: {}", e))?;
    let module = payload.get("module").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ptype = payload.get("type").and_then(|v| v.as_str()).unwrap_or("log").to_string();
    let pjson = payload.get("payload_json").cloned().unwrap_or_else(|| serde_json::json!({}));
    if module.is_empty() {
        return Err("test_probe requires a module".to_string());
    }

    let (instance_uuid, last_activity) = auth_store
        .find_by_module(&module)
        .ok_or_else(|| format!("module '{}' not connected", module))?;

    let probe_payload = build_probe_payload(&ptype, &pjson);
    let container = ContainerForModule {
        version: 2,
        auth_token: String::new(),
        module_instance_uuid7: instance_uuid.clone(),
        payload: probe_payload,
    };
    let mut buf = Vec::new();
    container
        .encode(&mut buf)
        .map_err(|e| format!("probe encode failed: {}", e))?;

    let sent = crate::pipeline::send_to_module(&orchestrator.module_senders, &module, container, "test_probe").await;
    if !matches!(sent, SendOutcome::Sent) {
        return Err(format!("module '{}' has no sender", module));
    }

    let now_ms = || -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    };
    let sent_at = now_ms();
    let mut responded = false;
    let mut latency = 0i64;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if let Some((_, la)) = auth_store.find_by_module(&module)
            && la > last_activity
        {
            responded = true;
            latency = now_ms().saturating_sub(sent_at);
            break;
        }
    }
    log_event_broadcast(
        ui_state,
        format!("[test_probe] '{}' <- {} responded={} latency={}ms", module, ptype, responded, latency),
    );
    Ok(serde_json::json!({
        "module": module,
        "type": ptype,
        "responded": responded,
        "latency_ms": latency,
    })
    .to_string())
}


#[cfg(test)]
mod tests {
    //! The gates and the branch set, asserted in isolation.
    //!
    //! Before this module existed the only way to exercise a query gate was to
    //! stand up the engine, stand up a module, and drive a real WebSocket
    //! round-trip — which is why the compliance test runner is the only place
    //! the permissions were ever checked, and why a gate silently widening
    //! would not have been caught by `cargo test`.

    use super::*;
    use crate::cockatiel_protobuf::container_for_module::Payload;

    const TUI: &str = "cockatiel-tui";
    const TUI_CHILD: &str = "cockatiel-tui-child";
    const TEST_RUNNER: &str = "cockatiel-test-runner";
    const SCORE_MESSAGES: &str = "score-messages";
    const TERM_CHAT: &str = "term-chat";
    const EVENTS: &str = "events";
    /// An ordinary adapter: authenticated, not a control surface, not the
    /// runner, not the automated scorer.
    const NORMAL: &str = "twitch-adapter";

    /// Is this operation available to `caller` at all, per the real gate?
    fn allows(query_id: &str, caller: &str) -> bool {
        caller_gate(classify_query(query_id), caller).is_none()
    }

    /// The exact denial string the caller would see, or `None` if allowed.
    fn denial(query_id: &str, caller: &str) -> Option<&'static str> {
        caller_gate(classify_query(query_id), caller)
    }

    // ── 1. Permission / routing matrix ────────────────────────────────────
    //
    // (query_id, caller) -> whether the dispatcher's gate admits the caller.
    // The denial strings are asserted byte-for-byte in
    // `gate_denial_strings_are_byte_identical` below, because the TUI and the
    // compliance test runner match on them.

    #[test]
    fn permission_matrix_matches_the_gates_the_dispatcher_applies() {
        // (query_id, caller, admitted)
        let matrix: &[(&str, &str, bool)] = &[
            // ── Ungated engine metadata: any connected module may read it.
            // (db_status exposes only backup/size numbers; engine_info and
            // module_list redact what a normal module may not see.)
            ("db_status", NORMAL, true),
            ("db_status", TERM_CHAT, true),
            ("db_status", "", true),
            ("module_list", NORMAL, true),
            ("engine_info", NORMAL, true),
            // ── test_run: the TUI control surface ONLY. Note the test runner
            // itself is NOT admitted — it is a different caller.
            ("test_run", TUI, true),
            ("test_run", TUI_CHILD, true),
            ("test_run", TEST_RUNNER, false),
            ("test_run", NORMAL, false),
            ("test_run", "", false),
            // ── pipeline_set_paused: the TUI ONLY. Stopping and starting the
            // message pipeline is an operator decision, so neither the test
            // runner nor an ordinary module may do it — and the runner in
            // particular must never be able to hold the engine that is timing
            // it (it boots unpaused instead, via COCKATIEL_START_PAUSED=0).
            ("pipeline_set_paused", TUI, true),
            ("pipeline_set_paused", TUI_CHILD, true),
            ("pipeline_set_paused", TEST_RUNNER, false),
            ("pipeline_set_paused", TERM_CHAT, false),
            ("pipeline_set_paused", SCORE_MESSAGES, false),
            ("pipeline_set_paused", NORMAL, false),
            ("pipeline_set_paused", "", false),
            // ── engine_shutdown: the TUI ONLY, for the same reason — a test
            // runner must never be able to kill the engine that is timing it,
            // and no ordinary module may talk the engine down. Note this is the
            // CALLER gate only; whether the TUI is allowed to ask at all is the
            // engine's own `shutdown_on_request` flag, asserted separately.
            ("engine_shutdown", TUI, true),
            ("engine_shutdown", TUI_CHILD, true),
            ("engine_shutdown", TEST_RUNNER, false),
            ("engine_shutdown", TERM_CHAT, false),
            ("engine_shutdown", SCORE_MESSAGES, false),
            ("engine_shutdown", NORMAL, false),
            ("engine_shutdown", "", false),
            // ── test_probe: the TUI OR the test runner.
            ("test_probe", TUI, true),
            ("test_probe", TEST_RUNNER, true),
            ("test_probe", NORMAL, false),
            ("test_probe", "", false),
            // ── test_archive: the test runner ONLY. The TUI is NOT admitted.
            ("test_archive", TEST_RUNNER, true),
            ("test_archive", TUI, false),
            ("test_archive", TUI_CHILD, false),
            ("test_archive", NORMAL, false),
            // ── The audit queue: the TUI ONLY.
            ("audit_list", TUI, true),
            ("audit_list", TEST_RUNNER, false),
            ("audit_list", NORMAL, false),
            ("audit_approve", TUI, true),
            ("audit_approve", NORMAL, false),
            ("audit_reject", TUI, true),
            ("audit_reject", NORMAL, false),
            // ── userdb_adjust_score: the three-way gate — the TUI, the
            // score-messages module, OR the predictions module (the brain bets
            // against a user's score, so it must be able to deduct/credit the
            // same way the scorer does). Everything else is refused.
            ("userdb_adjust_score", TUI, true),
            ("userdb_adjust_score", TUI_CHILD, true),
            ("userdb_adjust_score", SCORE_MESSAGES, true),
            ("userdb_adjust_score", EVENTS, true),
            ("userdb_adjust_score", TEST_RUNNER, false),
            ("userdb_adjust_score", TERM_CHAT, false),
            ("userdb_adjust_score", NORMAL, false),
            // ── prediction_get_score: the TUI control surface OR the
            // predictions module (the brain reads a user's score before a bet).
            // Everything else is refused.
            ("prediction_get_score", TUI, true),
            ("prediction_get_score", TUI_CHILD, true),
            ("prediction_get_score", EVENTS, true),
            ("prediction_get_score", SCORE_MESSAGES, false),
            ("prediction_get_score", TEST_RUNNER, false),
            ("prediction_get_score", TERM_CHAT, false),
            ("prediction_get_score", NORMAL, false),
            ("prediction_get_score", "", false),
            // ── The rest of the userdb family: the TUI ONLY. score-messages is
            // NOT special here — only adjust_score has the three-way gate.
            ("userdb_get_user", TUI, true),
            ("userdb_set_roles", TUI, true),
            ("userdb_get_user", SCORE_MESSAGES, false),
            ("userdb_set_roles", SCORE_MESSAGES, false),
            ("userdb_get_user", TEST_RUNNER, false),
            ("userdb_get_user", TERM_CHAT, false),
            ("userdb_get_user", NORMAL, false),
            ("userdb_ban", TUI, true),
            ("userdb_ban", NORMAL, false),
            // ── set_credentials: the TUI ONLY (a module must not be able to
            // rewrite another module's secrets).
            ("set_credentials", TUI, true),
            ("set_credentials", TERM_CHAT, false),
            ("set_credentials", NORMAL, false),
            // ── Ungated, but note: audio_for_message is open to any module, and
            // the TUI being able to call test_archive would be a privilege the
            // original never granted.
            ("audio_for_message", NORMAL, true),
            ("audio_for_message", TUI, true),
            // ── channel_viewers: public stream data — open to every
            // authenticated module, including a brand-new one.
            ("channel_viewers", NORMAL, true),
            ("channel_viewers", TERM_CHAT, true),
            ("channel_viewers", SCORE_MESSAGES, true),
            ("channel_viewers", TEST_RUNNER, true),
            ("channel_viewers", TUI, true),
            ("channel_viewers", "", true),
            // ── Gated in the handler, not here: the dispatcher admits these and
            // the helper refuses. Asserted so the two layers stay distinct.
            ("mod_commend", NORMAL, true),
            ("mod_ban", NORMAL, true),
            ("chat_commend", NORMAL, true),
            ("chat_reprimand", NORMAL, true),
            ("chat_verify_identity", NORMAL, true),
            // ── There is NO raw-SQL fallback: a query the engine doesn't name
            // is denied for every caller.
            ("SELECT * FROM timeline_events", NORMAL, false),
            ("SELECT * FROM timeline_events", TUI, false),
            ("", NORMAL, false),
            // ── stats: the control surface only.
            ("stats", TUI, true),
            ("stats", NORMAL, false),
        ];

        for (query_id, caller, admitted) in matrix {
            assert_eq!(
                allows(query_id, caller),
                *admitted,
                "query_id={query_id:?} caller={caller:?}: expected admitted={admitted}, got {}",
                allows(query_id, caller)
            );
        }
    }

    #[test]
    fn gate_denial_strings_are_byte_identical() {
        // Other code matches on these. Changing one is a wire-visible change.
        assert_eq!(
            denial("test_run", NORMAL),
            Some("Test runner access denied: not the TUI")
        );
        assert_eq!(
            denial("test_probe", NORMAL),
            Some("test_probe denied: not the TUI/test-runner")
        );
        assert_eq!(
            denial("pipeline_set_paused", NORMAL),
            Some("pipeline_set_paused denied: not the TUI")
        );
        assert_eq!(
            denial("pipeline_set_paused", TEST_RUNNER),
            Some("pipeline_set_paused denied: not the TUI")
        );
        assert_eq!(
            denial("engine_shutdown", NORMAL),
            Some("engine_shutdown denied: not the TUI")
        );
        // The compliance test runner is refused by name here, exactly as it is
        // for `pipeline_set_paused`: it must not be able to stop the engine that
        // is timing it.
        assert_eq!(
            denial("engine_shutdown", TEST_RUNNER),
            Some("engine_shutdown denied: not the TUI")
        );
        assert_eq!(
            denial("test_archive", TUI),
            Some("test_archive access denied: not the test runner")
        );
        assert_eq!(
            denial("audit_list", NORMAL),
            Some("Audit access denied: not the TUI")
        );
        assert_eq!(
            denial("audit_approve", NORMAL),
            Some("Audit access denied: not the TUI")
        );
        assert_eq!(
            denial("audit_reject", NORMAL),
            Some("Audit access denied: not the TUI")
        );
        assert_eq!(
            denial("userdb_get_user", NORMAL),
            Some("User database access denied: not the TUI")
        );
        assert_eq!(
            denial("userdb_adjust_score", NORMAL),
            Some("userdb_adjust_score denied: not the TUI, score-messages, or events")
        );
        assert_eq!(
            denial("prediction_get_score", NORMAL),
            Some("prediction_get_score denied: not the events module")
        );
        assert_eq!(denial("set_credentials", NORMAL), Some("set_credentials denied: not the TUI"));
        // Admitted callers get no denial at all.
        assert_eq!(denial("test_run", TUI), None);
        assert_eq!(denial("test_archive", TEST_RUNNER), None);
        assert_eq!(denial("pipeline_set_paused", TUI), None);
        assert_eq!(denial("engine_shutdown", TUI), None);
        assert_eq!(denial("engine_shutdown", TUI_CHILD), None);
        assert_eq!(denial("prediction_get_score", EVENTS), None);
        assert_eq!(denial("prediction_get_score", TUI), None);
    }

    #[test]
    fn a_detached_tui_sub_window_is_the_same_control_surface_as_the_tui() {
        // `cockatiel-tui-child` is a second TUI window, not a lesser module:
        // it must get every gate the parent gets, or the operator's split
        // layout silently loses half its powers.
        for query_id in [
            "test_run",
            "test_probe",
            "audit_list",
            "audit_approve",
            "audit_reject",
            "userdb_get_user",
            "userdb_adjust_score",
            "prediction_get_score",
            "set_credentials",
            "pipeline_set_paused",
            "engine_shutdown",
        ] {
            assert_eq!(
                allows(query_id, TUI),
                allows(query_id, TUI_CHILD),
                "TUI and TUI child disagree on {query_id:?}"
            );
        }
    }

    // ── 2. Branch coverage: no query_id was lost in the move ──────────────

    /// Every query_id the dispatcher recognised before the refactor, with the
    /// route it takes. If the refactor had dropped or reordered a branch, this
    /// is the test that would notice.
    const RECOGNISED: &[(&str, QueryRoute)] = &[
        // Engine metadata.
        ("db_status", QueryRoute::DbStatus),
        ("module_list", QueryRoute::ModuleList),
        ("engine_info", QueryRoute::EngineInfo),
        ("set_credentials", QueryRoute::SetCredentials),
        ("audio_for_message", QueryRoute::AudioForMessage),
        // Pipeline control.
        ("pipeline_set_paused", QueryRoute::PipelineSetPaused),
        // Engine lifecycle.
        ("engine_shutdown", QueryRoute::EngineShutdown),
        // Held-for-audit queue.
        ("audit_list", QueryRoute::AuditList),
        ("audit_approve", QueryRoute::AuditApprove),
        ("audit_reject", QueryRoute::AuditReject),
        // Compliance test surface.
        ("test_run", QueryRoute::TestRun),
        ("test_probe", QueryRoute::TestProbe),
        ("test_archive", QueryRoute::TestArchive),
        // Moderation, matched by the `mod_` prefix.
        ("mod_commend", QueryRoute::ModFamily),
        ("mod_reprimand", QueryRoute::ModFamily),
        ("mod_ban", QueryRoute::ModFamily),
        ("mod_timeout", QueryRoute::ModFamily),
        // Chat-command ratings.
        ("chat_commend", QueryRoute::ChatCommend),
        ("chat_reprimand", QueryRoute::ChatReprimand),
        ("chat_verify_identity", QueryRoute::ChatVerifyIdentity),
        // The arbitrary score delta, which must beat the `userdb_` prefix.
        ("userdb_adjust_score", QueryRoute::UserdbAdjustScore),
        // The user database family, matched by the `userdb_` prefix.
        ("userdb_add_user", QueryRoute::UserdbFamily),
        ("userdb_delete_user", QueryRoute::UserdbFamily),
        ("userdb_add_score", QueryRoute::UserdbFamily),
        ("userdb_remove_score", QueryRoute::UserdbFamily),
        ("userdb_add_channel", QueryRoute::UserdbFamily),
        ("userdb_remove_channel", QueryRoute::UserdbFamily),
        ("userdb_get_user", QueryRoute::UserdbFamily),
        ("userdb_list_users", QueryRoute::UserdbFamily),
        ("userdb_update_flags", QueryRoute::UserdbFamily),
        ("userdb_set_roles", QueryRoute::UserdbFamily),
        ("userdb_read_user_value", QueryRoute::UserdbFamily),
        ("userdb_write_user_value", QueryRoute::UserdbFamily),
        ("userdb_delete_user_value", QueryRoute::UserdbFamily),
        ("userdb_list_user_values", QueryRoute::UserdbFamily),
        ("userdb_commendation", QueryRoute::UserdbFamily),
        ("userdb_reprimand", QueryRoute::UserdbFamily),
        ("userdb_ban", QueryRoute::UserdbFamily),
        ("userdb_timeout", QueryRoute::UserdbFamily),
        // Predictions read surface.
        ("prediction_get_score", QueryRoute::PredictionGetScore),
        // One-shot timeline aggregates (control surface).
        ("stats", QueryRoute::Stats),
    ];

    #[test]
    fn every_recognised_query_id_still_routes_where_it_did() {
        for (query_id, expected) in RECOGNISED {
            assert_eq!(classify_query(query_id), *expected, "query_id={query_id:?}");
        }
    }

    #[test]
    fn the_dispatcher_recognises_exactly_the_operations_it_used_to() {
        // Every route in the enum is reachable, and nothing extra answers: an
        // unrecognised query_id is denied (Unsupported) — there is no read-only
        // SQL fallback anymore (Phase 2 removed it).
        //
        // The bare prefixes belong to their families — `mod_` and `userdb_` are
        // prefix matches, so they route to the family handler (which refuses
        // them) rather than to the deny. That is the original's behaviour and
        // it is why the families are modelled explicitly.
        let falls_through = ["", "nonsense", "chat_", "audit_", "set_credential", "userdbx"];
        let mut reachable: Vec<QueryRoute> = RECOGNISED.iter().map(|(_, r)| *r).collect();
        reachable.extend(falls_through.iter().map(|q| classify_query(q)));
        reachable.push(classify_query("mod_"));
        reachable.push(classify_query("userdb_"));

        for route in [
            QueryRoute::DbStatus,
            QueryRoute::ModuleList,
            QueryRoute::EngineInfo,
            QueryRoute::PipelineSetPaused,
            QueryRoute::EngineShutdown,
            QueryRoute::TestRun,
            QueryRoute::TestProbe,
            QueryRoute::AuditList,
            QueryRoute::AuditApprove,
            QueryRoute::AuditReject,
            QueryRoute::ModFamily,
            QueryRoute::ChatCommend,
            QueryRoute::ChatReprimand,
            QueryRoute::ChatVerifyIdentity,
            QueryRoute::UserdbAdjustScore,
            QueryRoute::UserdbFamily,
            QueryRoute::SetCredentials,
            QueryRoute::AudioForMessage,
            QueryRoute::TestArchive,
            QueryRoute::Stats,
            QueryRoute::Unsupported,
            QueryRoute::PredictionGetScore,
        ] {
            assert!(
                reachable.contains(&route),
                "route {route:?} is unreachable — a branch was lost"
            );
        }

        // Unrecognised ids are denied — no raw-SQL fallback remains.
        for unknown in falls_through {
            assert_eq!(
                classify_query(unknown),
                QueryRoute::Unsupported,
                "unknown query_id {unknown:?} must be denied, not executed"
            );
        }
        assert_eq!(classify_query("mod_"), QueryRoute::ModFamily);
        assert_eq!(classify_query("userdb_"), QueryRoute::UserdbFamily);
        // A prefix that merely looks similar is not claimed by a family.
        assert_eq!(classify_query("userdbx"), QueryRoute::Unsupported);
        assert_eq!(classify_query("modules_list"), QueryRoute::Unsupported);
        assert_eq!(classify_query("set_credential"), QueryRoute::Unsupported);
    }

    #[test]
    fn branch_order_is_load_bearing_for_adjust_score() {
        // The whole reason `classify_query` is a separate function with a test:
        // `"userdb_adjust_score".starts_with("userdb_")` is true, so testing
        // the prefix before the exact name would route the score delta into
        // the family handler, which has no case for it and would answer
        // `Unknown userdb query: userdb_adjust_score` — and would replace the
        // three-way (control surface OR score-messages OR predictions) gate
        // with the family's control-surface-only gate, locking score-messages
        // and predictions out of their own op.
        assert_eq!(classify_query("userdb_adjust_score"), QueryRoute::UserdbAdjustScore);
        assert!(query_id_starts_with_family("userdb_adjust_score", "userdb_"));
        // And it must not be reachable by the family, whatever else changes.
        assert_ne!(classify_query("userdb_adjust_score"), QueryRoute::UserdbFamily);
        // The three-way gate is the one that depends on it.
        assert!(allows("userdb_adjust_score", SCORE_MESSAGES));
        assert!(!allows("userdb_get_user", SCORE_MESSAGES));
    }

    /// Restates the prefix test the classification performs, so the ordering
    /// assertion above cannot pass by accident if the prefix ever changes.
    fn query_id_starts_with_family(query_id: &str, prefix: &str) -> bool {
        query_id.starts_with(prefix)
    }

    #[test]
    fn the_mod_family_is_matched_by_prefix_not_by_name() {
        // The chain routed on `starts_with("mod_")`, so an id the handler has
        // no case for still reaches the handler and is refused THERE, by actor
        // verification first. A classification that only knew the four named
        // mod operations would send these to the SQL fallback instead, which
        // would answer a *different* question.
        for query_id in [
            "mod_commend",
            "mod_reprimand",
            "mod_ban",
            "mod_timeout",
            "mod_anything_else",
            "mod_",
        ] {
            assert_eq!(classify_query(query_id), QueryRoute::ModFamily, "{query_id:?}");
        }
    }

    // ── 3. The engine-shutdown decision ──────────────────────────────────
    //
    // `engine_shutdown` is the only operation that can end the process, so its
    // two gates are asserted as behaviour, not just as strings: the caller gate
    // (above) plus the engine's OWN config flag. A green "the TUI is allowed"
    // test is not the same claim as "the engine goes down when it is asked" —
    // this is the half that can actually kill it.

    /// A ConfigState pointed at a throwaway config.json holding the required
    /// keys, with the engine-shutdown flag set to `allowed`. Mirrors what
    /// `get_config` reads at query time (a missing key would resolve to the
    /// serde default, i.e. false).
    fn shutdown_config(dir: &std::path::Path, allowed: bool) -> Arc<Mutex<ConfigState>> {
        use crate::config::Config;
        std::fs::create_dir_all(dir).unwrap();
        let content = format!(
            r#"{{
    "timeline_database_location": "./test.db",
    "timeline_database_backup_location": "./test-backup.db",
    "port": 9734,
    "shutdown_on_request": {}
}}"#,
            allowed
        );
        let path = dir.join("config.json");
        std::fs::write(&path, &content).unwrap();
        let config: Config = serde_json::from_str(&content).unwrap();
        Arc::new(Mutex::new(ConfigState {
            path,
            last_size: content.len() as u64,
            config,
            pin: 0,
            jwt_secret: String::new(),
        }))
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("cockatiel-shutdown-{}-{}", tag, Uuid::new_v4()))
    }

    /// The load-bearing test, refused half: with the flag false the request is
    /// REFUSED, the error names the key that would change that, and — the part
    /// that matters most — the shutdown signal is never raised, so the engine
    /// keeps running.
    #[test]
    fn a_disabled_engine_refuses_the_request_and_keeps_running() {
        let dir = temp_dir("disabled");
        let config_state = shutdown_config(&dir, false);
        let shutdown = ShutdownSignal::new();
        let ui_state = Arc::new(Mutex::new(EngineState::new()));

        let outcome =
            engine_shutdown(&config_state, &shutdown, &ui_state, TUI);

        assert!(!outcome.success, "a disabled engine must not report success");
        assert_eq!(outcome.error, ENGINE_SHUTDOWN_DISABLED);
        // The refusal has to be actionable: it names the config key.
        assert!(
            outcome.error.contains("shutdown_on_request"),
            "the refusal must name the config key that would enable it: {}",
            outcome.error
        );
        assert!(
            outcome.result_blob.is_empty(),
            "a refusal carries no payload — only the error"
        );
        // Nothing was raised, so the main loop's exit is never released.
        assert_eq!(
            shutdown.stage(),
            ShutdownStage::Idle,
            "a refused shutdown must leave the engine running"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The load-bearing test, accepted half: with the flag true the request is
    /// ACCEPTED and the shutdown is raised — as `Requested`, not `Answered`,
    /// because the response is still to be written.
    #[test]
    fn an_enabled_engine_accepts_the_request_and_raises_the_shutdown() {
        let dir = temp_dir("enabled");
        let config_state = shutdown_config(&dir, true);
        let shutdown = ShutdownSignal::new();
        let ui_state = Arc::new(Mutex::new(EngineState::new()));

        let outcome = engine_shutdown(&config_state, &shutdown, &ui_state, TUI);

        assert!(outcome.success, "an enabled engine must accept the request");
        assert_eq!(outcome.error, "");
        let json: serde_json::Value = serde_json::from_slice(&outcome.result_blob)
            .expect("the acceptance is machine-readable");
        assert_eq!(json["shutdown"], true);
        assert_eq!(json["requested_by"], TUI);
        // Requested, NOT answered: the answer is still queued, and the process
        // must not be allowed to exit until it has been written.
        assert_eq!(
            shutdown.stage(),
            ShutdownStage::Requested,
            "the exit must wait for the answer"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An engine that has never heard of the flag — a config.json written
    /// before it existed — refuses. This is the state almost every deployment
    /// is actually in, and it must not be killable over the wire.
    #[test]
    fn an_engine_without_the_key_refuses_by_default() {
        let dir = temp_dir("absent");
        std::fs::create_dir_all(&dir).unwrap();
        let content = r#"{
    "timeline_database_location": "./test.db",
    "timeline_database_backup_location": "./test-backup.db",
    "port": 9734
}"#;
        let path = dir.join("config.json");
        std::fs::write(&path, content).unwrap();
        let config: crate::config::Config = serde_json::from_str(content).unwrap();
        assert!(
            !config.shutdown_on_request,
            "an absent key must resolve to disabled, not to permitted"
        );
        let config_state = Arc::new(Mutex::new(ConfigState {
            path,
            last_size: content.len() as u64,
            config,
            pin: 0,
            jwt_secret: String::new(),
        }));
        let shutdown = ShutdownSignal::new();
        let ui_state = Arc::new(Mutex::new(EngineState::new()));

        let outcome = engine_shutdown(&config_state, &shutdown, &ui_state, TUI);
        assert!(!outcome.success);
        assert_eq!(outcome.error, ENGINE_SHUTDOWN_DISABLED);
        assert_eq!(shutdown.stage(), ShutdownStage::Idle);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The whole point of the two-stage signal: the process may not exit until
    /// the answer has reached the caller's socket. This walks the exact
    /// sequence the connection task runs, on a real channel.
    #[tokio::test]
    async fn the_answer_reaches_the_socket_before_the_process_may_exit() {
        let dir = temp_dir("ordering");
        let config_state = shutdown_config(&dir, true);
        let shutdown = ShutdownSignal::new();
        let ui_state = Arc::new(Mutex::new(EngineState::new()));

        // The process is watching for the answer from the start.
        let observer = shutdown.clone();
        let exit = tokio::spawn(async move { observer.wait_answered().await });

        // 1. The query path accepts and raises the request; the answer is built.
        let outcome = engine_shutdown(&config_state, &shutdown, &ui_state, TUI);
        assert!(outcome.success);
        assert_eq!(shutdown.stage(), ShutdownStage::Requested);

        // 2. The connection task queues the answer…
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ContainerForModule>(64);
        tx.send(build_query_response("tok", "uuid", "engine_shutdown", outcome))
            .await
            .expect("the answer is queued");
        // …and only THEN arms the flush. Everything enqueued after this point
        // is behind the answer, because the channel is FIFO.
        let shutdown_flush = true;

        // Traffic from another task (the log broadcast) lands after the arm.
        tx.send(build_query_response("tok", "uuid", "db_status", QueryOutcome::success(b"{}".to_vec())))
            .await
            .expect("later traffic is queued behind the answer");

        // 3. The process must still be waiting: the answer is on the channel,
        //    not on the socket yet.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !exit.is_finished(),
            "the engine must not exit while the answer is still queued"
        );
        assert_eq!(shutdown.stage(), ShutdownStage::Requested);

        // 4. The first frame this socket writes is the answer — the write
        //    completes, and only then does the connection hand the exit back.
        let first = rx.recv().await.expect("a frame is written");
        assert_eq!(
            result_of(&first).query_id, "engine_shutdown",
            "the first frame after the flush is armed must be the answer"
        );
        assert!(result_of(&first).success);
        assert!(
            !exit.is_finished(),
            "the frame is written, but the handoff has not happened yet"
        );
        if shutdown_flush {
            shutdown.mark_answered();
        }

        // 5. Only now may the process exit.
        tokio::time::timeout(Duration::from_secs(1), exit)
            .await
            .expect("the exit is released once the answer is confirmed")
            .expect("the observer did not panic");
        assert_eq!(shutdown.stage(), ShutdownStage::Answered);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The same guarantee from the other direction: an engine nobody asked to
    /// stop never releases the exit, however long the process watches.
    #[tokio::test]
    async fn an_engine_that_was_not_asked_never_releases_its_exit() {
        let shutdown = ShutdownSignal::new();
        assert_eq!(shutdown.stage(), ShutdownStage::Idle);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), shutdown.wait_answered())
                .await
                .is_err(),
            "the process must keep running until a shutdown is requested AND answered"
        );
    }

    /// The socket died with the answer still queued: no answer can ever arrive,
    /// so the exit must be released rather than left hanging. This is the
    /// end-of-connection path in `handle_connection`.
    #[tokio::test]
    async fn a_dead_socket_releases_the_exit_instead_of_hanging_the_process() {
        let shutdown = ShutdownSignal::new();
        let observer = shutdown.clone();
        let exit = tokio::spawn(async move { observer.wait_answered().await });

        shutdown.request();
        // The connection ended before the frame was written.
        shutdown.mark_answered();

        tokio::time::timeout(Duration::from_secs(1), exit)
            .await
            .expect("the exit is released even when the answer never arrived")
            .expect("the observer did not panic");
    }

    /// An engine watching for the request must see it, and the stage must be
    /// readable without awaiting (the main loop also checks it per connection).
    #[tokio::test]
    async fn a_subscriber_sees_the_request() {
        let shutdown = ShutdownSignal::new();
        let mut stage = shutdown.subscribe();
        assert_eq!(*stage.borrow_and_update(), ShutdownStage::Idle);

        shutdown.request();
        tokio::time::timeout(Duration::from_secs(1), stage.changed())
            .await
            .expect("a pending request wakes the accept loop")
            .expect("the sender is alive");
        assert_eq!(*stage.borrow(), ShutdownStage::Requested);
    }

    /// Restart semantics: a shutdown request must leave NOTHING behind for the
    /// next boot. The engine keeps no "asked to stop" state — config.json is
    /// untouched and the pipeline is not paused or released on the way out — so
    /// a restart is governed by exactly the settings it would have been had
    /// the request never come: it comes back per `start_paused` (paused by
    /// default) and the recovery drain picks up anything still 'queued'.
    ///
    /// The pipeline point matters: releasing held messages on the way out would
    /// dispatch them to modules that are about to be disconnected, and pausing
    /// would be a state change nobody asked for. They stay 'queued', which is
    /// the one durable state the engine already keeps.
    #[test]
    fn a_shutdown_request_leaves_nothing_behind_for_the_next_boot() {
        for allowed in [false, true] {
            let dir = temp_dir(if allowed { "written-yes" } else { "written-no" });
            let config_state = shutdown_config(&dir, allowed);
            let path = config_state.lock().unwrap().path.clone();
            let before = std::fs::read_to_string(&path).unwrap();

            let shutdown = ShutdownSignal::new();
            let ui_state = Arc::new(Mutex::new(EngineState::new()));
            let _ = engine_shutdown(&config_state, &shutdown, &ui_state, TUI);

            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                before,
                "a shutdown request must not write to config.json (allowed={allowed})"
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    // ── 4. The response envelope ──────────────────────────────────────────

    fn result_of(container: &ContainerForModule) -> &crate::cockatiel_protobuf::DatabaseQueryResult {
        match container.payload.as_ref().expect("payload present") {
            Payload::DatabaseQueryResult(r) => r,
            other => panic!("expected DatabaseQueryResult, got {other:?}"),
        }
    }

    #[test]
    fn a_successful_result_carries_a_blob_and_no_error() {
        let blob = br#"{"timeline_backup":true}"#.to_vec();
        let container = build_query_response(
            "token-abc",
            "0192f0aa-1111-7aaa-8000-000000000001",
            "db_status",
            QueryOutcome::success(blob.clone()),
        );
        // Envelope.
        assert_eq!(container.version, 2);
        assert_eq!(container.auth_token, "token-abc");
        assert_eq!(container.module_instance_uuid7, "0192f0aa-1111-7aaa-8000-000000000001");
        // Result.
        let r = result_of(&container);
        assert!(r.success);
        assert_eq!(r.error, "");
        assert_eq!(r.result_blob, blob);
        assert_eq!(r.query_id, "db_status");
    }

    #[test]
    fn a_denied_result_carries_an_error_and_no_blob() {
        let container = build_query_response(
            "token-abc",
            "0192f0aa-1111-7aaa-8000-000000000002",
            "userdb_get_user",
            QueryOutcome::denied("User database access denied: not the TUI"),
        );
        let r = result_of(&container);
        assert!(!r.success);
        assert_eq!(r.error, "User database access denied: not the TUI");
        assert!(r.result_blob.is_empty(), "a denial must not carry a blob");
        assert_eq!(r.query_id, "userdb_get_user");
    }

    #[test]
    fn every_gate_denial_produces_a_well_formed_failure_envelope() {
        // End-to-end over the pure path: classify -> gate -> deny -> envelope.
        for query_id in [
            "test_run",
            "test_probe",
            "test_archive",
            "pipeline_set_paused",
            "engine_shutdown",
            "audit_list",
            "audit_approve",
            "audit_reject",
            "userdb_adjust_score",
            "userdb_get_user",
            "set_credentials",
        ] {
            let expected = denial(query_id, NORMAL).expect("this query_id is gated");
            let container = build_query_response(
                "tok",
                "uuid",
                query_id,
                QueryOutcome::denied(caller_gate(classify_query(query_id), NORMAL).unwrap()),
            );
            let r = result_of(&container);
            assert!(!r.success, "{query_id:?} denial must not report success");
            assert_eq!(r.error, expected, "{query_id:?}");
            assert!(r.result_blob.is_empty(), "{query_id:?} denial must not carry a blob");
        }
    }

    #[test]
    fn the_outcome_triple_still_converts_from_the_bare_tuple() {
        // The helpers all return `(bool, Vec<u8>, String)`; that is the shape
        // the dispatcher has always produced and it must survive the move.
        let from_tuple: QueryOutcome = (true, b"raw".to_vec(), String::new()).into();
        assert!(from_tuple.success);
        assert_eq!(from_tuple.result_blob, b"raw");
        assert_eq!(from_tuple.error, "");

        let denied: QueryOutcome = (false, Vec::new(), "boom".to_string()).into();
        assert!(!denied.success);
        assert!(denied.result_blob.is_empty());
        assert_eq!(denied.error, "boom");
    }

    #[test]
    fn a_success_with_no_payload_stays_a_success() {
        // `audio_for_message` for a message with no audio is a SUCCESS with an
        // empty blob, not a failure. Easy to conflate with a denial; the
        // distinction is what lets a display distinguish "silent" from
        // "refused", so it is asserted here.
        let outcome = QueryOutcome::success(Vec::new());
        assert!(outcome.success);
        assert!(outcome.result_blob.is_empty());
        assert_eq!(outcome.error, "");
    }
}

#[test]
fn module_position_from_config_uses_the_ordering_lists() {
    // The engine config's ordering lists are the AUTHORITATIVE position — the
    // TUI rewrites them at runtime, so a connected module's reported stage must
    // follow config.json, not the stage it connected on. This is what stops the
    // TUI's stage-move view from resetting on the next module_list poll.
    let config: Config = serde_json::from_str(r#"{
        "timeline_database_location": "./t.db",
        "timeline_database_backup_location": "./b.db",
        "port": 9734,
        "inputs": [{"name":"twitch","priority":100}],
        "preprocessModules": [{"name":"clip","priority":100}],
        "inprocessModules": [{"name":"ban","priority":100}],
        "postprocessModules": [{"name":"tts","priority":100}]
    }"#).unwrap();

    assert_eq!(module_position_from_config(&config, "twitch").as_deref(), Some("input"));
    assert_eq!(module_position_from_config(&config, "clip").as_deref(), Some("preprocess"));
    assert_eq!(module_position_from_config(&config, "ban").as_deref(), Some("inprocess"));
    assert_eq!(module_position_from_config(&config, "tts").as_deref(), Some("postprocess"));
    assert_eq!(module_position_from_config(&config, "not-listed"), None);
}

#[test]
fn sort_module_list_keeps_the_inprocess_chain_order_visible() {
    // The in-process stage is an ordered chain; the TUI renders module_list in
    // response order, so the chain position must be preserved there while the
    // unordered stages stay alphabetical.
    let config: Config = serde_json::from_str(r#"{
        "timeline_database_location": "./t.db",
        "timeline_database_backup_location": "./b.db",
        "port": 9734,
        "inprocessModules": [
            {"name":"score-messages","priority":100},
            {"name":"reprimand","priority":100},
            {"name":"predictions","priority":100}
        ],
        "preprocessModules": [{"name":"banned-words","priority":100},{"name":"clip","priority":100}]
    }"#).unwrap();

    let mk = |name: &str, pos: &str| serde_json::json!({ "name": name, "position": pos });
    let mut list = vec![
        mk("reprimand", "inprocess"),
        mk("clip", "preprocess"),
        mk("predictions", "inprocess"),
        mk("score-messages", "inprocess"),
        mk("banned-words", "preprocess"),
    ];
    sort_module_list(&config, &mut list);

    let names: Vec<&str> = list.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec!["score-messages", "reprimand", "predictions", "banned-words", "clip"],
        "in-process modules must keep their chain order (config order), pre-process stays alphabetical"
    );
}

#[test]
fn current_autostart_reads_the_live_manifest_not_the_discovery_snapshot() {
    // The TUI toggles autostart by rewriting the module's manifest file at
    // runtime. `current_autostart` must reflect that write (the cached discovery
    // value would otherwise report the OLD state and the TUI's A marker would
    // flip back on the next poll).
    let dir = std::env::temp_dir().join(format!("cockatiel-autostart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("cockatiel_module_info.json"), r#"{"name":"m","autostart":false}"#).unwrap();
    let discovered = crate::module_manager::DiscoveredModule {
        manifest: crate::module_manager::ModuleManifest {
            name: "m".into(),
            description: String::new(),
            version: String::new(),
            capabilities: "postprocess".into(),
            root_file: String::new(),
            launch_command: String::new(),
            command_flags: vec![],
            autostart: false,
            terminal: false,
            credentials: vec![],
            unresponsive_timeout_secs: 0,
            probe_response_secs: 0,
            price: 0,
            min_rank: 0,
            authority: crate::module_manager::default_authority(),
        },
        directory: dir.clone(),
    };
    // Discovery snapshot says false; the file says false -> false.
    assert!(!current_autostart(&discovered));

    // Toggle the file (what the TUI's `a` press does); the helper now reports
    // true even though the discovery snapshot is still false.
    std::fs::write(dir.join("cockatiel_module_info.json"), r#"{"name":"m","autostart":true}"#).unwrap();
    assert!(current_autostart(&discovered), "must read the live manifest, not the snapshot");

    // A missing/unreadable file falls back to the snapshot value.
    std::fs::remove_file(dir.join("cockatiel_module_info.json")).unwrap();
    assert!(!current_autostart(&discovered), "fallback to the discovery value");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn channel_viewers_returns_stored_counts_and_honours_filter() {
    let stats: SharedChannelStats = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut map = stats.lock().unwrap();
        map.insert(
            "twitch:vulbyte".to_string(),
            crate::ChannelStatsEntry {
                platform: "twitch".to_string(),
                channel: "vulbyte".to_string(),
                viewers: 1234,
                is_live: true,
                title: "hello stream".to_string(),
                updated_at: 1_700_000_000_000,
            },
        );
        map.insert(
            "kick:someone".to_string(),
            crate::ChannelStatsEntry {
                platform: "kick".to_string(),
                channel: "someone".to_string(),
                viewers: 56,
                is_live: true,
                title: String::new(),
                updated_at: 1_700_000_000_001,
            },
        );
        map.insert(
            "discord:111222".to_string(),
            crate::ChannelStatsEntry {
                platform: "discord".to_string(),
                channel: "111222".to_string(),
                viewers: 900,
                is_live: false,
                title: String::new(),
                updated_at: 1_700_000_000_002,
            },
        );
    }

    // No filter: every channel comes back.
    let (ok, body, err) = channel_viewers_virtual_query(&stats, "{}");
    assert!(ok, "expected success, got {err}");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let channels = json["channels"].as_array().unwrap();
    assert_eq!(channels.len(), 3);

    // Platform filter: only twitch.
    let (ok, body, _) = channel_viewers_virtual_query(&stats, r#"{"platform":"twitch"}"#);
    assert!(ok);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let channels = json["channels"].as_array().unwrap();
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0]["channel"], "vulbyte");
    assert_eq!(channels[0]["viewers"], 1234);
    assert_eq!(channels[0]["is_live"], true);

    // A malformed body is treated as "no filter" (never an error).
    let (ok, body, _) = channel_viewers_virtual_query(&stats, "not-json");
    assert!(ok);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["channels"].as_array().unwrap().len(), 3);
}
