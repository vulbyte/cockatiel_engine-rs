//! Command system tests: classification (help / attach / alert / none) +
//! parser fuzzing — adversarial inputs must never panic and must classify
//! consistently.

use crate::cockatiel_protobuf::{Command, Commands};
use crate::command_registry::{CommandRegistry, parse_command};
use crate::{CommandAction, classify_command};

fn reg_with_commands() -> CommandRegistry {
    let mut r = CommandRegistry::default();
    r.register(
        "reprimand",
        Commands {
            commands: vec![Command {
                command_name: "reprimand".into(),
                command_flag: "!".into(),
                command_description: "reprimand".into(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        },
    );
    // An alerting module also owns `!`.
    let flag = |name: &str| crate::cockatiel_protobuf::Flag {
        flag_name: name.to_string(),
        flag_description: String::new(),
        limiting_type: 0,
        min_val: 0.0,
        max_val: 0.0,
        options: vec![],
        value: String::new(),
    };
    r.register(
        "tts-service",
        Commands {
            commands: vec![Command {
                command_name: "tts".into(),
                command_flag: "!".into(),
                command_description: "tts".into(),
                command_flags: vec![flag("p"), flag("r")],
            }],
            alert_on_unknown_command: true,
        },
    );
    r
}

#[test]
fn help_classifies_to_help() {
    let r = reg_with_commands();
    assert!(matches!(
        classify_command("!help", &r),
        CommandAction::Help(None)
    ));
    assert!(matches!(
        classify_command("!help what can I do", &r),
        CommandAction::Help(Some(_))
    ));
    // The module argument is captured for the detail view.
    match classify_command("!help tts", &r) {
        CommandAction::Help(Some(m)) => assert_eq!(m, "tts"),
        other => panic!("expected Help(Some(tts)), got {other:?}"),
    }
}

#[test]
fn known_command_attaches() {
    let r = reg_with_commands();
    match classify_command("!reprimand @user reason", &r) {
        CommandAction::Attach(c) => {
            assert_eq!(c.command_name, "reprimand");
            assert_eq!(c.command_flag, "!");
        }
        other => panic!("expected Attach, got {:?}", std::mem::discriminant(&other)),
    }
}

#[test]
fn unknown_on_alerting_flag_alerts() {
    let r = reg_with_commands();
    match classify_command("!bogus whatever", &r) {
        CommandAction::Alert { flag, name } => {
            assert_eq!(flag, "!");
            assert_eq!(name, "bogus");
        }
        other => panic!("expected Alert, got {other:?}"),
    }
}

#[test]
fn plain_message_is_none() {
    let r = reg_with_commands();
    assert!(matches!(
        classify_command("just chatting", &r),
        CommandAction::None
    ));
}

#[test]
fn alerting_module_wants_plain_messages_pure_owner_does_not() {
    let r = reg_with_commands();
    // A module that opted into alerts also keeps seeing plain chat.
    assert!(r.wants_plain_messages("tts-service"));
    assert!(r.is_command_scoped("tts-service"));
    // A pure command owner does not.
    assert!(!r.wants_plain_messages("reprimand"));
}

#[test]
fn closest_command_suggests_near_misses_only() {
    let r = reg_with_commands();
    assert_eq!(crate::closest_command(&r, "!", "reprimandd").as_deref(), Some("reprimand"));
    assert_eq!(crate::closest_command(&r, "!", "ts").as_deref(), Some("tts"));
    // nothing close → no suggestion
    assert_eq!(crate::closest_command(&r, "!", "bogusxyz"), None);
}

#[test]
fn edit_distance_basic() {
    assert_eq!(crate::edit_distance("roll", "roll"), 0);
    assert_eq!(crate::edit_distance("rolll", "roll"), 1);
    assert_eq!(crate::edit_distance("", "abc"), 3);
}

/// A4 — parser fuzzing: adversarial inputs never panic.
#[test]
fn parser_never_panics_on_adversarial_input() {
    let r = reg_with_commands();
    let inputs = [
        "!",
        "!!",
        "!   ",
        "!tts -p -p -p",
        "!tts -",
        "!tts -p:",
        "!tts -p :",
        "!tts -p 2 -r 1.4 -v 88 hey!",
        "!tts -p:2 -r:1.4 -v:88 hey!",
        "!reprimand @user",
        "!reprimand    ",
        "!💥💥",
        "!tts 🎉",
        "!tts -p 2",
        &"!tts -x ".repeat(500), // huge flag list
        "!tts \"quoted value\" more",
        "!tts -p\n2",
        "!tts\t-p\t2",
    ];
    for input in inputs {
        // parse must not panic; classify must not panic.
        let parsed = parse_command(input, &r);
        let _ = classify_command(input, &r);
        // If it parsed, it produced a non-empty command name — never panics.
        if let Some(p) = &parsed {
            assert!(!p.command.command_name.is_empty());
        }
    }
}

#[test]
fn parser_flag_values_round_trip() {
    let r = reg_with_commands();
    let p = parse_command("!tts -p 2 -r:1.4 hey", &r).unwrap();
    assert_eq!(p.args, "hey");
    let vals: Vec<(String, String)> = p
        .command
        .command_flags
        .iter()
        .map(|f| (f.flag_name.clone(), f.value.clone()))
        .collect();
    assert_eq!(
        vals,
        vec![
            ("p".to_string(), "2".to_string()),
            ("r".to_string(), "1.4".to_string())
        ]
    );
}
