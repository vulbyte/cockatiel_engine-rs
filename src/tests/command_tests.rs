//! Command system tests: classification (help / attach / alert / none) +
//! parser fuzzing — adversarial inputs must never panic and must classify
//! consistently.

use crate::command_registry::{CommandRegistry, parse_command};
use crate::cockatiel_protobuf::{Command, Commands};
use crate::{classify_command, CommandAction};

fn reg_with_commands() -> CommandRegistry {
    let mut r = CommandRegistry::default();
    r.register("reprimand", Commands {
        commands: vec![Command {
            command_name: "reprimand".into(),
            command_flag: "!".into(),
            command_description: "reprimand".into(),
            command_flags: vec![],
        }],
        alert_on_unknown_command: false,
    });
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
    r.register("tts-service", Commands {
        commands: vec![Command {
            command_name: "tts".into(),
            command_flag: "!".into(),
            command_description: "tts".into(),
            command_flags: vec![flag("p"), flag("r")],
        }],
        alert_on_unknown_command: true,
    });
    r
}

#[test]
fn help_classifies_to_help() {
    let r = reg_with_commands();
    assert!(matches!(classify_command("!help", &r), CommandAction::Help));
    assert!(matches!(classify_command("!help what can I do", &r), CommandAction::Help));
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
    assert!(matches!(classify_command("!bogus whatever", &r), CommandAction::Alert));
}

#[test]
fn plain_message_is_none() {
    let r = reg_with_commands();
    assert!(matches!(classify_command("just chatting", &r), CommandAction::None));
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
    let vals: Vec<(String, String)> = p.command.command_flags.iter().map(|f| (f.flag_name.clone(), f.value.clone())).collect();
    assert_eq!(vals, vec![("p".to_string(), "2".to_string()), ("r".to_string(), "1.4".to_string())]);
}