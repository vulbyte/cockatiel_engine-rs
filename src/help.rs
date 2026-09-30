//! `!help` rendering.
//!
//! `!help` lists every registered command grouped by its owning module;
//! `!help <module>` shows a module's commands with their flags and inferred
//! value types. Everything is derived from the existing command registry —
//! no new fields, no containers to expand.
//!
//! Value types are inferred from each flag's `limiting_type`:
//! - `Any`   -> "text"
//! - `Options` -> "one of: a | b | c" (from `options`)
//! - `Range` -> "number (min..max)" (from `min_val`/`max_val`)
//! - `Unspecified` -> "value" (no declared type)

use crate::cockatiel_protobuf::{Flag, FlagLimitType};
use crate::command_registry::CommandRegistry;

/// Render the `!help` overview: every module with its commands, each numbered.
///
/// ```
/// !help
/// ──────────
/// predictions  commands
/// 1 !pred — start/stop a zero-sum prediction and bet score (parimutuel)
/// 2 !poll — create a free-vote poll
/// score-messages  commands
/// 1 (catch-all — scores every message)
/// ```
pub fn format_overview(registry: &CommandRegistry) -> String {
    let mut lines = vec!["!help".to_string(), "──────────".to_string()];
    let by_module = registry.commands_by_module();
    if by_module.is_empty() {
        lines.push("  (no commands registered yet)".to_string());
        return lines.join("\n");
    }
    for (module, commands) in by_module {
        lines.push(format!("{module}  commands"));
        for (i, c) in commands.iter().enumerate() {
            lines.push(format!("{} {}{} — {}", i + 1, c.command_flag, c.command_name, c.command_description));
        }
    }
    lines.join("\n")
}

/// Render `!help <module>`: the module's commands, each with its flags and
/// inferred value types, and a trailing hint line for a bare `!help`.
pub fn format_module(registry: &CommandRegistry, module: &str) -> Option<String> {
    let commands = registry
        .commands_by_module()
        .into_iter()
        .find(|(m, _)| m.eq_ignore_ascii_case(module))
        .map(|(_, cmds)| cmds)?;
    let mut lines = vec![format!("!help {module}"), "──────────".to_string()];
    for c in &commands {
        lines.push(format!("{}{}", c.command_flag, c.command_name));
        if !c.command_description.is_empty() {
            lines.push(format!("  {}", c.command_description));
        }
        if c.command_flags.is_empty() {
            lines.push("  (no flags)".to_string());
        } else {
            for f in &c.command_flags {
                lines.push(format!("  -{} {}", f.flag_name, flag_value_type(f)));
                if !f.flag_description.is_empty() {
                    lines.push(format!("      {}", f.flag_description));
                }
            }
        }
    }
    lines.push(String::new());
    lines.push("!help — list all modules and their commands".to_string());
    Some(lines.join("\n"))
}

/// The inferred value type string for a flag, from its `limiting_type`.
fn flag_value_type(f: &Flag) -> String {
    match FlagLimitType::try_from(f.limiting_type).unwrap_or(FlagLimitType::Unspecified) {
        FlagLimitType::Any => "text".to_string(),
        FlagLimitType::Options => {
            if f.options.is_empty() {
                "value".to_string()
            } else {
                format!("one of: {}", f.options.join(" | "))
            }
        }
        FlagLimitType::Range => {
            if f.min_val == 0.0 && f.max_val == 0.0 {
                "number".to_string()
            } else {
                format!("number ({}..{})", f.min_val, f.max_val)
            }
        }
        FlagLimitType::Unspecified => "value".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cockatiel_protobuf::{Command, Commands};

    fn flag(name: &str, desc: &str, limiting: i32, min: f32, max: f32, options: Vec<&str>) -> Flag {
        Flag {
            flag_name: name.into(),
            flag_description: desc.into(),
            limiting_type: limiting,
            min_val: min,
            max_val: max,
            options: options.into_iter().map(String::from).collect(),
            value: String::new(),
        }
    }

    fn registry() -> CommandRegistry {
        let mut r = CommandRegistry::default();
        r.register(
            "predictions",
            Commands {
                commands: vec![
                    Command {
                        command_name: "pred".into(),
                        command_flag: "!".into(),
                        command_description: "start/stop a zero-sum prediction".into(),
                        command_flags: vec![
                            flag("l", "bet on the left", 0, 0.0, 0.0, vec![]),
                            flag("r", "bet on the right", 0, 0.0, 0.0, vec![]),
                        ],
                    },
                    Command {
                        command_name: "poll".into(),
                        command_flag: "!".into(),
                        command_description: "create a free-vote poll".into(),
                        command_flags: vec![
                            flag("p", "the poll prompt", 1, 0.0, 0.0, vec![]),
                            flag("h", "hide live counts", 0, 0.0, 0.0, vec![]),
                        ],
                    },
                ],
                alert_on_unknown_command: true,
            },
        );
        r.register(
            "tts",
            Commands {
                commands: vec![Command {
                    command_name: "tts".into(),
                    command_flag: "!".into(),
                    command_description: "read a message out loud".into(),
                    command_flags: vec![
                        flag("voice", "which voice", 3, 0.0, 10.0, vec![]),
                        flag("side", "which side", 2, 0.0, 0.0, vec!["l", "r"]),
                    ],
                }],
                alert_on_unknown_command: false,
            },
        );
        r
    }

    #[test]
    fn overview_groups_commands_by_module_and_numbers_them() {
        let r = registry();
        let text = format_overview(&r);
        assert!(text.starts_with("!help\n──────────\n"));
        assert!(text.contains("predictions  commands"));
        assert!(text.contains("1 !pred — start/stop a zero-sum prediction"));
        assert!(text.contains("2 !poll — create a free-vote poll"));
        assert!(text.contains("tts  commands"));
        assert!(text.contains("1 !tts — read a message out loud"));
    }

    #[test]
    fn module_detail_shows_flags_with_inferred_value_types() {
        let r = registry();
        let text = format_module(&r, "tts").unwrap();
        assert!(text.starts_with("!help tts\n──────────\n"));
        assert!(text.contains("!tts"));
        // Range -> number (min..max).
        assert!(text.contains("-voice number (0..10)"));
        // Options -> one of: l | r.
        assert!(text.contains("-side one of: l | r"));
        // Descriptions shown under the flag.
        assert!(text.contains("which voice"));
    }

    #[test]
    fn module_lookup_is_case_insensitive() {
        let r = registry();
        assert!(format_module(&r, "TTS").is_some());
        assert!(format_module(&r, "tts").is_some());
        assert_eq!(format_module(&r, "nonexistent"), None);
    }

    #[test]
    fn unspecified_flags_render_as_value() {
        let r = registry();
        let text = format_module(&r, "predictions").unwrap();
        assert!(text.contains("-l value"));
        assert!(text.contains("-r value"));
    }

    #[test]
    fn empty_registry_overview_is_graceful() {
        let r = CommandRegistry::default();
        let text = format_overview(&r);
        assert!(text.contains("no commands registered yet"));
    }
}