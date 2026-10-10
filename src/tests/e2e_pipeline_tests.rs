//! End-to-end pipeline tests: an ADAPTER input fanned out through every stage to
//! every module, driven to completion, with no errors left behind.
//!
//! These exercise the whole routing surface the way real traffic does:
//!
//! * an adapter ingests a `ChatMessage` (empty uuid7, exactly like a real
//!   adapter — the engine owns the row uuid);
//! * the pre-process fanout reaches EVERY pre module, and the stage does not
//!   advance until all of them have answered;
//! * the in-process chain is SEQUENTIAL — module N+1 is not sent the message
//!   until module N has answered;
//! * the post-process fanout reaches EVERY post module;
//! * the terminal row is `complete` with a null `error_message`, the processed
//!   text is persisted, and nothing is left pending anywhere.
//!
//! A burst test pins that N messages each reach every module (no cross-talk,
//! no drops), and a command test pins that a registered command reaches its
//! owner + catch-alls but NOT unrelated modules.

use super::pipeline_tests::{
    adapter_ingest, in_reply, nothing_left, post_reply, pre_reply, row, take_broadcast, test_db,
    wired_orchestrator,
};
use crate::cockatiel_protobuf::container_for_engine::Payload as EnginePayload;
use crate::cockatiel_protobuf::{
    ChatMessage, Command, Commands, ContainerForEngine, MessagePreProcess,
};

/// An adapter ingest of a message the ENGINE has already parsed into a command.
/// (Real parsing happens in `main.rs`'s ingest path before the orchestrator is
/// called, so the unit-level harness supplies the parsed command directly.)
fn adapter_ingest_command(raw: &str, name: &str) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: String::new(),
        module_name: "adapter".into(),
        module_instance_uuid7: String::new(),
        payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
            message_uuid7: String::new(),
            raw_message: Some(ChatMessage {
                platform: "twitch".into(),
                raw_data: vec![],
                raw_message: raw.into(),
                user_uuid7: "user-1".into(),
                command: Some(Command {
                    command_name: name.into(),
                    command_flag: "!".into(),
                    command_description: String::new(),
                    command_flags: vec![],
                }),
                channel_id: "chan-1".into(),
                user_data: None,
            }),
            audio: vec![],
            audio_type: String::new(),
        })),
    }
}

/// A full stack: 2 pre, 2 in-process, 2 post. Every one of the six modules is a
/// catch-all (see `wired_orchestrator`), so a plain chat message must reach each.
const PRE: [&str; 2] = ["lang", "banned"];
const IN: [&str; 2] = ["score", "commend"];
const POST: [&str; 2] = ["term", "tts"];

fn all_modules() -> impl Iterator<Item = &'static str> {
    PRE.iter().chain(IN.iter()).chain(POST.iter()).copied()
}

/// The headline e2e: one adapter message, every module, completes clean.
#[tokio::test]
async fn an_adapter_message_reaches_every_module_in_every_stage_and_completes_clean() {
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &PRE, &IN, &POST);

    // ── Adapter ingest ────────────────────────────────────────────────
    orchestrator
        .handle_message_from_module(&adapter_ingest("hello world"))
        .await
        .unwrap();

    // Pre-process fans out to EVERY pre module, all with the same uuid.
    let from_lang = take_broadcast(rx.get_mut("lang").unwrap());
    let from_banned = take_broadcast(rx.get_mut("banned").unwrap());
    assert_eq!(from_lang, from_banned, "one row uuid for the whole chain");
    let uuid7 = from_lang;

    // The later stages must NOT have seen it yet.
    for m in IN.iter().chain(POST.iter()) {
        assert!(
            nothing_left(rx.get_mut(*m).unwrap()),
            "'{}' must not receive the message before its stage",
            m
        );
    }

    // Pre-process does not advance until EVERY pre module has answered.
    orchestrator
        .handle_message_from_module(&pre_reply("lang", &uuid7, "hello world"))
        .await
        .unwrap();
    for m in IN.iter() {
        assert!(
            nothing_left(rx.get_mut(*m).unwrap()),
            "the stage must wait for EVERY pre module before entering in-process"
        );
    }
    orchestrator
        .handle_message_from_module(&pre_reply("banned", &uuid7, "hello world"))
        .await
        .unwrap();

    // In-process is a SEQUENTIAL chain: only the head gets it first.
    let to_score = take_broadcast(rx.get_mut("score").unwrap());
    assert_eq!(to_score, uuid7);
    assert!(
        nothing_left(rx.get_mut("commend").unwrap()),
        "the second in-process module must wait for the first to answer"
    );
    orchestrator
        .handle_message_from_module(&in_reply("score", &uuid7, "hello world", false))
        .await
        .unwrap();

    // Now the chain hands off to the second in-process module.
    let to_commend = take_broadcast(rx.get_mut("commend").unwrap());
    assert_eq!(to_commend, uuid7);
    orchestrator
        .handle_message_from_module(&in_reply("commend", &uuid7, "hello world!", false))
        .await
        .unwrap();

    // Post-process fans out to EVERY post module.
    let to_term = take_broadcast(rx.get_mut("term").unwrap());
    let to_tts = take_broadcast(rx.get_mut("tts").unwrap());
    assert_eq!(to_term, uuid7);
    assert_eq!(to_tts, uuid7);
    orchestrator
        .handle_message_from_module(&post_reply("term", &uuid7, "hello world!"))
        .await
        .unwrap();
    orchestrator
        .handle_message_from_module(&post_reply("tts", &uuid7, "hello world!"))
        .await
        .unwrap();

    // ── The row is complete, with no error, and the processed text landed ──
    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete", "row: {r:?}");
    assert!(r["error_message"].is_null(), "no error may be recorded: {r:?}");
    assert_eq!(r["raw_message"], "hello world");
    assert_eq!(r["user_uuid7"], "user-1");
    assert_eq!(r["processed_message"], "hello world!");

    // Nothing extra reached any module, and nothing is left pending.
    for m in all_modules() {
        assert!(
            nothing_left(rx.get_mut(m).unwrap()),
            "'{}' received an unexpected extra message",
            m
        );
    }
    assert!(orchestrator.pipeline_states.lock().await.is_empty());
    assert!(!orchestrator.ack_tracker.lock().await.has_pending(&uuid7));
}

/// A burst: each of N messages reaches every module exactly once and completes
/// clean — no cross-talk between messages, no drops.
#[tokio::test]
async fn a_burst_reaches_every_module_once_per_message_and_all_complete() {
    const N: usize = 3;
    let db = test_db().await;
    let (orchestrator, mut rx) = wired_orchestrator(db.clone(), &PRE, &IN, &POST);

    let mut uuids = Vec::new();
    for i in 0..N {
        orchestrator
            .handle_message_from_module(&adapter_ingest(&format!("burst {}", i)))
            .await
            .unwrap();
        let a = take_broadcast(rx.get_mut("lang").unwrap());
        let b = take_broadcast(rx.get_mut("banned").unwrap());
        assert_eq!(a, b);
        uuids.push(a);
    }

    // Drive every message through the rest of the chain. Reply per message so
    // the mapping uuid → text is unambiguous.
    for (i, uuid7) in uuids.iter().enumerate() {
        let text = format!("burst {}", i);
        orchestrator
            .handle_message_from_module(&pre_reply("lang", uuid7, &text))
            .await
            .unwrap();
        orchestrator
            .handle_message_from_module(&pre_reply("banned", uuid7, &text))
            .await
            .unwrap();

        let s = take_broadcast(rx.get_mut("score").unwrap());
        assert_eq!(&s, uuid7);
        orchestrator
            .handle_message_from_module(&in_reply("score", uuid7, &text, false))
            .await
            .unwrap();

        let c = take_broadcast(rx.get_mut("commend").unwrap());
        assert_eq!(&c, uuid7);
        orchestrator
            .handle_message_from_module(&in_reply("commend", uuid7, &format!("{}!", text), false))
            .await
            .unwrap();

        let t = take_broadcast(rx.get_mut("term").unwrap());
        let x = take_broadcast(rx.get_mut("tts").unwrap());
        assert_eq!(&t, uuid7);
        assert_eq!(&x, uuid7);
        orchestrator
            .handle_message_from_module(&post_reply("term", uuid7, &format!("{}!", text)))
            .await
            .unwrap();
        orchestrator
            .handle_message_from_module(&post_reply("tts", uuid7, &format!("{}!", text)))
            .await
            .unwrap();
    }

    // Every row completed cleanly with its own processed text.
    for (i, uuid7) in uuids.iter().enumerate() {
        let r = row(&db, uuid7).await;
        assert_eq!(r["pipeline_status"], "complete", "row {i}: {r:?}");
        assert!(r["error_message"].is_null(), "row {i} has an error: {r:?}");
        assert_eq!(r["processed_message"], format!("burst {}!", i));
    }
    // Every module drained, nothing left pending.
    for m in all_modules() {
        assert!(nothing_left(rx.get_mut(m).unwrap()), "'{}' has leftovers", m);
    }
    assert!(orchestrator.pipeline_states.lock().await.is_empty());
}

/// A registered command reaches its owner + catch-alls at the stage, but NOT an
/// unrelated module that registered a different command.
#[tokio::test]
async fn a_command_reaches_its_owner_and_catch_alls_but_not_unrelated_modules() {
    let db = test_db().await;
    let (orchestrator, mut rx) =
        wired_orchestrator(db.clone(), &["owner", "display", "bystander"], &[], &[]);

    // `wired_orchestrator` made everything a catch-all; override two of them:
    // `owner` subscribes to `!ping`, `bystander` to `!other` (so it is NOT a
    // catch-all), and `display` stays a catch-all.
    {
        let mut registry = orchestrator.command_registry.lock().unwrap();
        registry.register(
            "owner",
            Commands {
                commands: vec![Command {
                    command_name: "ping".into(),
                    command_flag: "!".into(),
                    command_description: String::new(),
                    command_flags: vec![],
                }],
                alert_on_unknown_command: false,
            },
        );
        registry.register(
            "bystander",
            Commands {
                commands: vec![Command {
                    command_name: "other".into(),
                    command_flag: "!".into(),
                    command_description: String::new(),
                    command_flags: vec![],
                }],
                alert_on_unknown_command: false,
            },
        );
    }

    orchestrator
        .handle_message_from_module(&adapter_ingest_command("!ping", "ping"))
        .await
        .unwrap();

    // The owner and the catch-all receive it; the unrelated module does not.
    let uuid7 = take_broadcast(rx.get_mut("owner").unwrap());
    assert_eq!(take_broadcast(rx.get_mut("display").unwrap()), uuid7);
    assert!(
        nothing_left(rx.get_mut("bystander").unwrap()),
        "a module that registered a different command must not receive !ping"
    );

    // Only the recipients need to answer; the stage then completes (no
    // in-process / post modules in this stack).
    orchestrator
        .handle_message_from_module(&pre_reply("owner", &uuid7, "!ping"))
        .await
        .unwrap();
    orchestrator
        .handle_message_from_module(&pre_reply("display", &uuid7, "!ping"))
        .await
        .unwrap();

    let r = row(&db, &uuid7).await;
    assert_eq!(r["pipeline_status"], "complete", "row: {r:?}");
    assert!(r["error_message"].is_null());
    assert!(nothing_left(rx.get_mut("bystander").unwrap()));
}
