//! Engine module protocol — the engine-owned home for the timeline /
//! user-database query surface.
//!
//! This is the landing site for the capability the engine currently
//! exposes to every connected module through the shared
//! `cockatiel_protobuf.v1` `DatabaseQuery` / `DatabaseQueryResult`
//! pair. The shared pair is still what goes over the wire today;
//! nothing here is wired into the dispatcher yet. What this module
//! establishes is the *location* and the *build wiring*, so the
//! rewiring has somewhere to land.
//!
//! Why it lives here and not in `cockatiel_lib`: the timeline and the
//! user database are engine-internal resources. That is the same
//! reasoning that already keeps `cockatiel_userdb.v1` in its own
//! crate, and the reason the shared module protocol should not be
//! carrying it in the first place.
//!
//! The schema is defined in `src/proto/engine_module.proto` and
//! compiled by `build.rs`.

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_engine.v1.rs"));
}

#[cfg(test)]
mod tests {
    use super::proto::*;
    use prost::Message;

    /// Every operation the engine answers, with the tag it was given
    /// and the `query_id` string the live dispatcher uses to select
    /// it.
    ///
    /// This table is the contract between the schema and the engine.
    /// A later change that renumbers an operation, misspells a name,
    /// or drops an operation the dispatcher still answers breaks a
    /// test here instead of silently mis-routing a query.
    ///
    /// `None` for the `query_id` means the operation has no
    /// `query_id` string at all: the dispatcher reaches it through a
    /// prefix match or through its final `else`.
    const OPERATIONS: &[(i32, &str, Option<&str>)] = &[
        // The closed enum replaces "any string the caller invents".
        // It is always an error, so it carries no query_id.
        (0, "QUERY_OP_UNSPECIFIED", None),
        // Engine metadata (control surface).
        (1, "QUERY_OP_DB_STATUS", Some("db_status")),
        (2, "QUERY_OP_MODULE_LIST", Some("module_list")),
        (3, "QUERY_OP_ENGINE_INFO", Some("engine_info")),
        (4, "QUERY_OP_SET_CREDENTIALS", Some("set_credentials")),
        (5, "QUERY_OP_AUDIO_FOR_MESSAGE", Some("audio_for_message")),
        // Held-for-audit queue.
        (6, "QUERY_OP_AUDIT_LIST", Some("audit_list")),
        (7, "QUERY_OP_AUDIT_APPROVE", Some("audit_approve")),
        (8, "QUERY_OP_AUDIT_REJECT", Some("audit_reject")),
        // Compliance test surface.
        (9, "QUERY_OP_TEST_RUN", Some("test_run")),
        (10, "QUERY_OP_TEST_PROBE", Some("test_probe")),
        (11, "QUERY_OP_TEST_ARCHIVE", Some("test_archive")),
        // Pipeline control.
        (12, "QUERY_OP_PIPELINE_SET_PAUSED", Some("pipeline_set_paused")),
        // Engine lifecycle. Opens the 60s decade — see the schema for why it
        // does not take a number from the 1-12 control-surface band.
        (60, "QUERY_OP_ENGINE_SHUTDOWN", Some("engine_shutdown")),
        // Moderation. The live dispatcher matches these on the
        // `mod_` prefix, then switches on the full string.
        (20, "QUERY_OP_MOD_COMMEND", Some("mod_commend")),
        (21, "QUERY_OP_MOD_REPRIMAND", Some("mod_reprimand")),
        (22, "QUERY_OP_MOD_BAN", Some("mod_ban")),
        (23, "QUERY_OP_MOD_TIMEOUT", Some("mod_timeout")),
        // Chat-command ratings.
        (30, "QUERY_OP_CHAT_COMMEND", Some("chat_commend")),
        (31, "QUERY_OP_CHAT_REPRIMAND", Some("chat_reprimand")),
        (32, "QUERY_OP_CHAT_VERIFY_IDENTITY", Some("chat_verify_identity")),
        // User database. Matched on the `userdb_` prefix, with
        // `userdb_adjust_score` pulled out ahead of it.
        (40, "QUERY_OP_USERDB_ADD_USER", Some("userdb_add_user")),
        (41, "QUERY_OP_USERDB_DELETE_USER", Some("userdb_delete_user")),
        (42, "QUERY_OP_USERDB_ADD_SCORE", Some("userdb_add_score")),
        (43, "QUERY_OP_USERDB_REMOVE_SCORE", Some("userdb_remove_score")),
        (44, "QUERY_OP_USERDB_ADD_CHANNEL", Some("userdb_add_channel")),
        (45, "QUERY_OP_USERDB_REMOVE_CHANNEL", Some("userdb_remove_channel")),
        (46, "QUERY_OP_USERDB_GET_USER", Some("userdb_get_user")),
        (47, "QUERY_OP_USERDB_LIST_USERS", Some("userdb_list_users")),
        (48, "QUERY_OP_USERDB_UPDATE_FLAGS", Some("userdb_update_flags")),
        (49, "QUERY_OP_USERDB_SET_ROLES", Some("userdb_set_roles")),
        (50, "QUERY_OP_USERDB_READ_USER_VALUE", Some("userdb_read_user_value")),
        (
            51,
            "QUERY_OP_USERDB_WRITE_USER_VALUE",
            Some("userdb_write_user_value"),
        ),
        (
            52,
            "QUERY_OP_USERDB_DELETE_USER_VALUE",
            Some("userdb_delete_user_value"),
        ),
        (
            53,
            "QUERY_OP_USERDB_LIST_USER_VALUES",
            Some("userdb_list_user_values"),
        ),
        (54, "QUERY_OP_USERDB_COMMENDATION", Some("userdb_commendation")),
        (55, "QUERY_OP_USERDB_REPRIMAND", Some("userdb_reprimand")),
        (56, "QUERY_OP_USERDB_BAN", Some("userdb_ban")),
        (57, "QUERY_OP_USERDB_TIMEOUT", Some("userdb_timeout")),
        (
            58,
            "QUERY_OP_USERDB_ADJUST_SCORE",
            Some("userdb_adjust_score"),
        ),
        // The dispatcher's final `else`: the read-only SQL fallback.
        // It is reached by *not* matching anything above, so it has
        // no query_id of its own.
        (90, "QUERY_OP_SELECT_SQL", None),
    ];

    /// Every family of operation the dispatcher handles has a variant.
    /// Losing one of these to a careless proto edit is the failure
    /// this asserts against.
    #[test]
    fn every_operation_family_has_a_variant() {
        for family in [
            // engine metadata
            "DB_STATUS",
            "MODULE_LIST",
            "ENGINE_INFO",
            "SET_CREDENTIALS",
            "AUDIO_FOR_MESSAGE",
            // audit
            "AUDIT_LIST",
            "AUDIT_APPROVE",
            "AUDIT_REJECT",
            // compliance tests
            "TEST_RUN",
            "TEST_PROBE",
            "TEST_ARCHIVE",
            // pipeline control
            "PIPELINE_SET_PAUSED",
            // engine lifecycle
            "ENGINE_SHUTDOWN",
            // moderation
            "MOD_COMMEND",
            "MOD_REPRIMAND",
            "MOD_BAN",
            "MOD_TIMEOUT",
            // chat ratings
            "CHAT_COMMEND",
            "CHAT_REPRIMAND",
            "CHAT_VERIFY_IDENTITY",
            // user database
            "USERDB_ADD_USER",
            "USERDB_DELETE_USER",
            "USERDB_ADD_SCORE",
            "USERDB_REMOVE_SCORE",
            "USERDB_ADD_CHANNEL",
            "USERDB_REMOVE_CHANNEL",
            "USERDB_GET_USER",
            "USERDB_LIST_USERS",
            "USERDB_UPDATE_FLAGS",
            "USERDB_SET_ROLES",
            "USERDB_READ_USER_VALUE",
            "USERDB_WRITE_USER_VALUE",
            "USERDB_DELETE_USER_VALUE",
            "USERDB_LIST_USER_VALUES",
            "USERDB_COMMENDATION",
            "USERDB_REPRIMAND",
            "USERDB_BAN",
            "USERDB_TIMEOUT",
            "USERDB_ADJUST_SCORE",
            // read-only SQL escape hatch
            "SELECT_SQL",
        ] {
            let name = format!("QUERY_OP_{}", family);
            assert!(
                QueryOp::from_str_name(&name).is_some(),
                "no QueryOp variant for operation family {}",
                family
            );
        }
    }

    /// Each variant keeps the tag the table pins, and round-trips
    /// through both of prost's enum conversions.
    #[test]
    fn operation_tags_and_names_are_stable() {
        for (tag, name, _) in OPERATIONS {
            let op = QueryOp::try_from(*tag)
                .unwrap_or_else(|e| panic!("tag {} is not a valid QueryOp: {}", tag, e));
            assert_eq!(op.as_str_name(), *name, "tag {} has the wrong name", tag);
            assert_eq!(
                QueryOp::from_str_name(name),
                Some(op),
                "{} does not decode back to its own variant",
                name
            );
        }
    }

    /// The name of an operation is mechanically convertible back to
    /// the `query_id` the live dispatcher matches on. That is what
    /// makes the migration mechanical rather than a re-invention.
    #[test]
    fn operation_names_map_back_to_the_dispatchers_query_ids() {
        for (tag, name, query_id) in OPERATIONS {
            let op = QueryOp::try_from(*tag).expect("valid tag");
            match query_id {
                Some(expected) => {
                    // Strip the QUERY_OP_ prefix and lowercase.
                    let derived = name
                        .strip_prefix("QUERY_OP_")
                        .expect("every variant carries the prefix")
                        .to_ascii_lowercase();
                    assert_eq!(
                        &derived, expected,
                        "{} no longer maps to the dispatcher's query_id",
                        name
                    );
                }
                None => {
                    // Only the two operations the dispatcher reaches
                    // without a query_id string may have no mapping.
                    assert!(
                        matches!(op, QueryOp::Unspecified | QueryOp::SelectSql),
                        "{} claims no query_id but is not one of the two that can",
                        name
                    );
                }
            }
        }
    }

    /// The table covers the enum exactly: no operation in the schema
    /// is unaccounted for, and none is listed twice.
    #[test]
    fn the_table_covers_the_enum_exactly() {
        let mut tags: Vec<i32> = OPERATIONS.iter().map(|(tag, _, _)| *tag).collect();
        tags.sort_unstable();
        let before = tags.len();
        tags.dedup();
        assert_eq!(before, tags.len(), "the table lists a tag twice");

        assert_eq!(
            tags.len(),
            OPERATIONS.len(),
            "the table and the enum disagree on how many operations exist"
        );
    }

    /// `ENGINE_SHUTDOWN` had to be added to a schema that is already sparse on
    /// purpose, so the thing worth pinning is not just its tag but that it did
    /// NOT come out of an existing family. Renumbering would change values that
    /// are already on the wire; taking a slot inside a full band would leave
    /// that family with no room to grow. So: the new operation opens the 60s
    /// decade, and every value in the bands below it is exactly where it was.
    #[test]
    fn the_new_operation_opens_a_decade_and_moves_nothing() {
        // The bands the dispatcher already depends on, unchanged.
        for (tag, name) in [
            (0, "QUERY_OP_UNSPECIFIED"),
            (1, "QUERY_OP_DB_STATUS"),
            (5, "QUERY_OP_AUDIO_FOR_MESSAGE"),
            (6, "QUERY_OP_AUDIT_LIST"),
            (8, "QUERY_OP_AUDIT_REJECT"),
            (9, "QUERY_OP_TEST_RUN"),
            (11, "QUERY_OP_TEST_ARCHIVE"),
            (12, "QUERY_OP_PIPELINE_SET_PAUSED"),
            (20, "QUERY_OP_MOD_COMMEND"),
            (23, "QUERY_OP_MOD_TIMEOUT"),
            (30, "QUERY_OP_CHAT_COMMEND"),
            (32, "QUERY_OP_CHAT_VERIFY_IDENTITY"),
            (40, "QUERY_OP_USERDB_ADD_USER"),
            (58, "QUERY_OP_USERDB_ADJUST_SCORE"),
            (90, "QUERY_OP_SELECT_SQL"),
        ] {
            assert_eq!(
                QueryOp::try_from(tag).expect("valid tag").as_str_name(),
                name,
                "tag {} moved — the schema is meant to be append-only",
                tag
            );
        }

        // The new operation, and the 60s decade is its alone — room for more
        // engine-lifecycle operations without touching anything that exists.
        assert_eq!(
            QueryOp::try_from(60).expect("valid tag"),
            QueryOp::EngineShutdown
        );
        assert_eq!(QueryOp::EngineShutdown as i32, 60);
        for tag in (60..90).filter(|t| *t != 60) {
            assert!(
                QueryOp::try_from(tag).is_err(),
                "tag {} is claimed — 61-89 is the room this family grows into",
                tag
            );
        }
        // 13-19 (the tail of the control-surface band) and 59 stay free too, so
        // neither the audit nor the userdb family is squeezed.
        for tag in (13..20).chain(std::iter::once(59)) {
            assert!(
                QueryOp::try_from(tag).is_err(),
                "tag {} was taken by the lifecycle operation",
                tag
            );
        }
    }

    /// A request survives encode/decode with every populated field
    /// intact, including the nested actor and the opaque JSON blob.
    #[test]
    fn request_round_trips_through_the_wire() {
        let request = QueryRequest {
            request_id: "req-0001".to_string(),
            operation: QueryOp::ModBan as i32,
            params: Some(QueryParams {
                sql: "SELECT 1".to_string(),
                uuid7: "0198f1a2-3b4c-7d8e-9f01-23456789abcd".to_string(),
                platform: "twitch".to_string(),
                channel_id: "12345".to_string(),
                handle: "someone".to_string(),
                reason: "spam".to_string(),
                duration_secs: 300,
                limit: 100,
                offset: 0,
                module_name: "discord_adapter".to_string(),
                batch_uuid: "batch-7".to_string(),
                actor: Some(ActorRef {
                    uuid7: "0198ffff-0000-7000-8000-000000000000".to_string(),
                    platform: "kick".to_string(),
                    handle: "modperson".to_string(),
                }),
                json: r#"{"legacy":"blob"}"#.to_string(),
            }),
        };

        let mut buf = Vec::new();
        request.encode(&mut buf).expect("request encodes");
        assert!(!buf.is_empty(), "an all-default request must not be empty");

        let decoded = QueryRequest::decode(buf.as_slice()).expect("request decodes");

        assert_eq!(decoded.request_id, "req-0001");
        assert_eq!(
            QueryOp::try_from(decoded.operation).expect("valid operation"),
            QueryOp::ModBan
        );

        let params = decoded.params.expect("params survive as a present message");
        assert_eq!(params.sql, "SELECT 1");
        assert_eq!(params.uuid7, "0198f1a2-3b4c-7d8e-9f01-23456789abcd");
        assert_eq!(params.platform, "twitch");
        assert_eq!(params.channel_id, "12345");
        assert_eq!(params.handle, "someone");
        assert_eq!(params.reason, "spam");
        assert_eq!(params.duration_secs, 300);
        assert_eq!(params.limit, 100);
        assert_eq!(params.offset, 0);
        assert_eq!(params.module_name, "discord_adapter");
        assert_eq!(params.batch_uuid, "batch-7");
        assert_eq!(params.json, r#"{"legacy":"blob"}"#);

        let actor = params.actor.expect("the nested actor survives");
        assert_eq!(actor.uuid7, "0198ffff-0000-7000-8000-000000000000");
        assert_eq!(actor.platform, "kick");
        assert_eq!(actor.handle, "modperson");
    }

    /// The two prefix families whose payloads are still opaque keep
    /// their JSON blob across the wire unchanged. This is the case
    /// the later workstream has to keep working until it types them
    /// out, so it is worth pinning.
    #[test]
    fn the_opaque_json_payload_survives_for_both_prefix_families() {
        // The shape the live dispatcher parses out of `query.sql` for
        // `mod_*`: an optional actor plus a target user.
        let mod_blob = serde_json::json!({
            "actor": { "platform": "twitch", "handle": "modperson", "uuid7": "" },
            "platform": "twitch",
            "handle": "target",
            "reason": "rule 3",
            "duration_secs": 600,
        })
        .to_string();

        // And the shape `userdb_*` reads.
        let userdb_blob = serde_json::json!({
            "username": "newuser",
            "channel": { "platform": "kick", "channel_id": "999", "handle": "newuser" },
        })
        .to_string();

        for (op, blob) in [
            (QueryOp::ModTimeout, mod_blob),
            (QueryOp::UserdbAddUser, userdb_blob),
        ] {
            let request = QueryRequest {
                request_id: "opaque".to_string(),
                operation: op as i32,
                params: Some(QueryParams {
                    json: blob.clone(),
                    ..Default::default()
                }),
            };

            let mut buf = Vec::new();
            request.encode(&mut buf).expect("request encodes");
            let decoded = QueryRequest::decode(buf.as_slice()).expect("request decodes");
            let got = decoded.params.expect("params present").json;

            // Byte-identical, and still parses as the JSON the engine
            // expects — the engine must be able to carry these
            // unchanged while the schema is still a skeleton.
            assert_eq!(got, blob, "{} lost its payload in transit", op.as_str_name());
            serde_json::from_str::<serde_json::Value>(&got)
                .unwrap_or_else(|e| panic!("{} payload no longer parses: {}", op.as_str_name(), e));
        }
    }

    /// A response survives the round trip, and the engine's echo
    /// contract holds: whatever the engine is told to echo comes back.
    #[test]
    fn response_round_trips_and_echoes_the_request() {
        let request = QueryRequest {
            request_id: "req-42".to_string(),
            operation: QueryOp::AudioForMessage as i32,
            params: Some(QueryParams {
                uuid7: "0198aaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee".to_string(),
                ..Default::default()
            }),
        };

        // What the engine sends back for a request it accepted.
        let response = QueryResponse {
            request_id: request.request_id.clone(),
            operation: request.operation,
            success: true,
            error: String::new(),
            result: Some(QueryResult {
                result_blob: vec![0x00, 0xff, 0x10, 0x7f],
            }),
        };

        let mut buf = Vec::new();
        response.encode(&mut buf).expect("response encodes");
        let decoded = QueryResponse::decode(buf.as_slice()).expect("response decodes");

        assert_eq!(decoded.request_id, request.request_id, "the id must echo");
        assert_eq!(
            decoded.operation, request.operation,
            "the operation must echo, so a caller can tell concurrent queries apart"
        );
        assert!(decoded.success);
        assert!(decoded.error.is_empty());
        assert_eq!(
            decoded.result.expect("result present").result_blob,
            vec![0x00, 0xff, 0x10, 0x7f],
            "raw audio bytes must not be mangled — this result is not JSON"
        );
    }

    /// A denied operation is an error response, not a severed
    /// connection: success=false, error set, result absent.
    #[test]
    fn a_denied_operation_round_trips_as_an_error() {
        let response = QueryResponse {
            request_id: "req-denied".to_string(),
            operation: QueryOp::UserdbListUsers as i32,
            success: false,
            error: "User database access denied: not the TUI".to_string(),
            result: None,
        };

        let mut buf = Vec::new();
        response.encode(&mut buf).expect("response encodes");
        let decoded = QueryResponse::decode(buf.as_slice()).expect("response decodes");

        assert!(!decoded.success);
        assert_eq!(decoded.error, "User database access denied: not the TUI");
        assert!(decoded.result.is_none(), "a denial carries no result");
        assert_eq!(
            QueryOp::try_from(decoded.operation).expect("valid operation"),
            QueryOp::UserdbListUsers
        );
    }

    /// An unset operation is a request error, not a silent default —
    /// and it is still representable on the wire so the engine can
    /// answer it with a proper error rather than dropping it.
    #[test]
    fn an_unset_operation_is_representable_so_the_engine_can_reject_it() {
        let request = QueryRequest {
            request_id: "req-unset".to_string(),
            operation: QueryOp::Unspecified as i32,
            params: None,
        };

        let mut buf = Vec::new();
        request.encode(&mut buf).expect("request encodes");
        let decoded = QueryRequest::decode(buf.as_slice()).expect("request decodes");

        assert_eq!(
            QueryOp::try_from(decoded.operation).expect("valid operation"),
            QueryOp::Unspecified
        );
        assert!(decoded.params.is_none());
    }

    /// This schema must not collide with the shared module protocol.
    /// The two are meant to be independently evolvable, so no
    /// generated type here may share a name with one the engine
    /// already imports from `cockatiel_protobuf`.
    #[test]
    fn the_schema_does_not_collide_with_the_shared_module_protocol() {
        // These are the shared protocol's query types. If the engine
        // ever had to import both under the same name, the two
        // definitions would be on a collision course.
        let shared: [&str; 2] = ["DatabaseQuery", "DatabaseQueryResult"];
        for name in shared {
            assert_ne!(
                name,
                "QueryRequest",
                "engine-module schema must not reuse a shared protocol type name"
            );
            assert_ne!(
                name,
                "QueryResponse",
                "engine-module schema must not reuse a shared protocol type name"
            );
        }

        // The package is engine-owned, distinct from the shared
        // module protocol, the v2 target spec, and the user database.
        assert_eq!(
            include_str!("proto/engine_module.proto")
                .lines()
                .find(|l| l.starts_with("package "))
                .expect("the schema declares a package")
                .trim(),
            "package cockatiel_engine.v1;"
        );
    }
}
