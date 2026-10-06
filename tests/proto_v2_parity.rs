//! Migration audit for the unified Cockatiel protocol spec.
//!
//! The old `proto_v2_parity.rs` compared the archived v1 spec against a
//! v2 spec that no longer exists: the protocol is now ONE unified file
//! (`cockatiel_lib/cockatiel_protobuf.proto`, package `cockatiel_protobuf`)
//! carrying the direction-split containers, the typed query surface, the
//! timeline read path and the user-database model together. v1 is archived and
//! not spoken.
//!
//! What this test pins instead is that the unified spec is self-consistent:
//!   * the file compiles with protoc
//!   * the two containers exist and carry every shared payload
//!   * direction-invalid payloads are `reserved`, so a peer cannot express a
//!     payload addressed away from it
//!   * both containers carry `version` (field 1) — the engine requires 2
//!   * the pipeline stage remains the message type (no bare ChatMessage payload)
//!   * the legacy DatabaseQuery/Result pair is still carried (Phase 2 removes it)

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

fn lib_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("cockatiel_lib")
}

fn read_proto(name: &str) -> String {
    let path = lib_dir().join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let bytes: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '/' && i + 1 < bytes.len() && bytes[i + 1] == '/' {
            while i < bytes.len() && bytes[i] != '\n' {
                i += 1;
            }
        } else if bytes[i] == '/' && i + 1 < bytes.len() && bytes[i + 1] == '*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == '*' && bytes[i + 1] == '/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

fn body_after(src: &str, open_brace: usize) -> &str {
    let bytes = src.as_bytes();
    let mut depth = 1usize;
    let mut i = open_brace + 1;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return &src[open_brace + 1..i];
        }
        i += 1;
    }
    panic!("unbalanced braces in proto");
}

fn message_bodies(src: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut i = 0;
    while let Some(found) = src[i..].find("message ") {
        let name_start = i + found + "message ".len();
        i = name_start;
        let Some(name_end) = src[name_start..]
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|n| name_start + n)
        else {
            continue;
        };
        let after = src[name_end..].trim_start();
        if !after.starts_with('{') {
            continue;
        }
        let brace = name_end + (src[name_end..].len() - after.len());
        out.insert(src[name_start..name_end].to_string(), body_after(src, brace).to_string());
        i = brace + 1;
    }
    out
}

fn oneof_payloads(message_body: &str) -> BTreeMap<u32, String> {
    let mut out = BTreeMap::new();
    let Some(at) = message_body.find("oneof ") else {
        return out;
    };
    let open = message_body[at..]
        .find('{')
        .map(|n| at + n)
        .expect("oneof has no body");
    let body = body_after(message_body, open);
    for line in body.lines() {
        let line = line.trim();
        let Some((field_type, rest)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let rest = rest.trim();
        let Some(tag) = rest.rsplit('=').next() else {
            continue;
        };
        if let Ok(tag) = tag.trim().trim_end_matches(';').parse::<u32>() {
            out.insert(tag, field_type.trim().to_string());
        }
    }
    out
}

fn reserved_tags(message_body: &str) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    for line in message_body.lines() {
        let line = line.trim();
        if line.starts_with("reserved ") {
            let rest = &line["reserved ".len()..];
            let rest = rest.trim_end_matches(';');
            for token in rest.split(',') {
                if let Ok(t) = token.trim().parse::<u32>() {
                    out.insert(t);
                }
            }
        }
    }
    out
}

struct Unified {
    bodies: BTreeMap<String, String>,
    for_engine: BTreeMap<u32, String>,
    for_module: BTreeMap<u32, String>,
    engine_reserved: BTreeSet<u32>,
    module_reserved: BTreeSet<u32>,
}

fn load() -> Unified {
    let src = strip_comments(&read_proto("cockatiel_protobuf.proto"));
    let bodies = message_bodies(&src);
    Unified {
        for_engine: oneof_payloads(bodies.get("ContainerForEngine").expect("ContainerForEngine")),
        for_module: oneof_payloads(bodies.get("ContainerForModule").expect("ContainerForModule")),
        engine_reserved: reserved_tags(bodies.get("ContainerForEngine").expect("ContainerForEngine")),
        module_reserved: reserved_tags(bodies.get("ContainerForModule").expect("ContainerForModule")),
        bodies,
    }
}

#[test]
fn the_unified_spec_compiles() {
    // Use the vendored protoc so the audit does not depend on a system install
    // (CI has none, and cockatiel-proto builds with the vendored binary).
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    let out = Command::new(protoc)
        .arg("--proto_path")
        .arg(lib_dir())
        .arg("--descriptor_set_out")
        .arg(std::env::temp_dir().join("cockatiel_unified_audit.pb"))
        .arg(lib_dir().join("cockatiel_protobuf.proto"))
        .output()
        .expect("protoc must be on PATH");
    assert!(
        out.status.success(),
        "cockatiel_protobuf.proto does not compile:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_parser_actually_read_the_containers() {
    let p = load();
    assert!(
        p.for_engine.len() >= 20,
        "ContainerForEngine only yielded {} payloads; the parser is broken",
        p.for_engine.len()
    );
    assert!(
        p.for_module.len() >= 20,
        "ContainerForModule only yielded {} payloads; the parser is broken",
        p.for_module.len()
    );
}

/// Every payload BOTH directions need is carried by both containers, and the
/// stage messages travel engine -> module (the ack-with-result echo rides back
/// module -> engine on the same tags).
#[test]
fn both_containers_carry_the_shared_payloads() {
    let p = load();
    let engine: BTreeSet<&str> = p.for_engine.values().map(|s| s.as_str()).collect();
    let module: BTreeSet<&str> = p.for_module.values().map(|s| s.as_str()).collect();

    // Payloads that flow in BOTH directions.
    for shared in ["Ban", "Commands", "Log", "Err", "SendToPlatforms", "Prompt", "PromptResponse"] {
        assert!(
            engine.contains(shared),
            "ContainerForEngine is missing the shared payload {shared}"
        );
        assert!(
            module.contains(shared),
            "ContainerForModule is missing the shared payload {shared}"
        );
    }

    // The pipeline stage is the message type: all three stage messages travel
    // engine -> module, and their ack-with-result echoes travel module -> engine.
    for stage in ["MessagePreProcess", "MessageInProcess", "MessagePostProcess"] {
        assert!(
            module.contains(stage),
            "{stage} is not engine -> module, so a message cannot advance"
        );
        assert!(
            engine.contains(stage),
            "{stage} is not module -> engine, so a module cannot ack a stage by echo"
        );
    }

    // Event projections flow both ways (publish + relay).
    for proj in ["PredictionUpdate", "PollUpdate", "ChannelStats"] {
        assert!(engine.contains(proj), "{proj} is not module -> engine");
        assert!(module.contains(proj), "{proj} is not engine -> module");
    }
}

/// A peer cannot EXPRESS a payload addressed away from it: engine-only payloads
/// are reserved on ContainerForEngine, module-only payloads are reserved on
/// ContainerForModule.
#[test]
fn direction_invalid_payloads_are_reserved() {
    let p = load();

    // Engine -> module only: the module must not be able to send them.
    let module_only = ["AuthNew", "ConnectionRequestReturn", "Shutdown", "TimelineEvent", "UserData"];
    for payload in module_only {
        assert!(
            p.for_engine.values().all(|t| t != payload),
            "module can express {payload} on ContainerForEngine — engine->module only"
        );
    }

    // Module -> engine only: the engine must not offer them to a module.
    let engine_only = ["ConnectionRequest", "Command", "MessageAck", "DatabaseQuery", "ModuleControl"];
    for payload in engine_only {
        assert!(
            p.for_module.values().all(|t| t != payload),
            "engine can express {payload} on ContainerForModule — module->engine only"
        );
    }
}

/// No container reserves a tag it also uses. A reserved tag must be genuinely
/// absent from the oneof.
#[test]
fn no_container_reserves_a_tag_it_also_uses() {
    let p = load();
    let engine: BTreeSet<u32> = p.for_engine.keys().copied().collect();
    let module: BTreeSet<u32> = p.for_module.keys().copied().collect();
    for t in &p.engine_reserved {
        assert!(
            !engine.contains(t),
            "ContainerForEngine reserves tag {t} but also carries a payload on it"
        );
    }
    for t in &p.module_reserved {
        assert!(
            !module.contains(t),
            "ContainerForModule reserves tag {t} but also carries a payload on it"
        );
    }
}

/// The engine requires version 2. Both containers must declare `version` on
/// field 1 (parity with the header field numbers).
#[test]
fn both_containers_declare_version() {
    let p = load();
    // The oneof parser only sees oneof members; check the header text directly.
    for name in ["ContainerForEngine", "ContainerForModule"] {
        let body = p.bodies.get(name).expect(name);
        assert!(
            body.contains("int32 version = 1"),
            "{name} must declare int32 version = 1 (the engine requires 2)"
        );
    }
}

/// The pipeline stage stays the message type: no bare `ChatMessage` payload.
#[test]
fn the_pipeline_stage_is_the_message_type() {
    let p = load();
    let engine: BTreeSet<&str> = p.for_engine.values().map(|s| s.as_str()).collect();
    let module: BTreeSet<&str> = p.for_module.values().map(|s| s.as_str()).collect();
    assert!(
        !engine.contains("ChatMessage") && !module.contains("ChatMessage"),
        "a bare ChatMessage payload would leave the stage ambiguous; it belongs only \
         inside the stage messages"
    );
}

/// The legacy DatabaseQuery/Result pair is still carried during the migration
/// (Phase 2 removes it and the engine's string-dispatch path). This test is the
/// tripwire that gets deleted when that removal lands.
#[test]
fn legacy_database_query_is_still_carried_pending_phase_2() {
    let p = load();
    assert!(
        p.for_engine.get(&23).map(|t| t == "DatabaseQuery").unwrap_or(false),
        "ContainerForEngine tag 23 must carry DatabaseQuery during Phase 1"
    );
    assert!(
        p.for_module.get(&24).map(|t| t == "DatabaseQueryResult").unwrap_or(false),
        "ContainerForModule tag 24 must carry DatabaseQueryResult during Phase 1"
    );
}

/// The new typed query surface and timeline read path are on the wire.
#[test]
fn the_typed_surface_is_on_the_wire() {
    let p = load();
    assert!(
        p.for_engine.get(&35).map(|t| t == "QueryRequest").unwrap_or(false),
        "ContainerForEngine tag 35 must carry QueryRequest"
    );
    assert!(
        p.for_module.get(&35).map(|t| t == "QueryResponse").unwrap_or(false),
        "ContainerForModule tag 35 must carry QueryResponse"
    );
    assert!(
        p.for_engine.get(&34).map(|t| t == "TimelineQuery").unwrap_or(false),
        "ContainerForEngine tag 34 must carry TimelineQuery"
    );
    assert!(
        p.for_module.get(&34).map(|t| t == "TimelineQueryResult").unwrap_or(false),
        "ContainerForModule tag 34 must carry TimelineQueryResult"
    );
}

/// The user-database model + envelope live in the shared spec.
#[test]
fn the_user_database_lives_in_the_shared_spec() {
    let p = load();
    for name in ["User", "ChannelRef", "RatingHistoryEntry", "UserDbRequest", "UserDbResponse"] {
        assert!(
            p.bodies.contains_key(name),
            "the unified spec is missing {name}"
        );
    }
}