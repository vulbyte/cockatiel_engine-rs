// Command registry — the engine's record of which modules subscribe to which
// chat commands, plus the parsed-command parser.
//
// Modules register via the `Commands` protobuf payload:
//   - a non-empty `commands` list subscribes that module to those
//     (flag, command_name) pairs — the engine routes those commands to it;
//   - an EMPTY `commands` list makes the module a **catch-all** (it receives
//     every message, command or not);
//   - `alert_on_unknown_command` opts a module into the apology reply when a
//     user tries an unregistered command under one of its flags.
//
// The registry is a fast in-memory subscription check (no protobuf round-trip),
// which is what routing consults on every ingest.
use crate::cockatiel_protobuf::{Command, Commands, Flag};

#[derive(Debug, Clone, Default)]
pub struct ModuleCommandReg {
    pub commands: Vec<Command>,
    pub catch_all: bool,
    pub alert_on_unknown: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CommandRegistry {
    /// (flag, command_name) -> module that subscribed to it.
    index: std::collections::HashMap<(String, String), String>,
    /// module name -> its registration.
    modules: std::collections::HashMap<String, ModuleCommandReg>,
}

impl CommandRegistry {
    /// (Re)register a module's command set. Replaces any prior registration
    /// for the same module.
    pub fn register(&mut self, module: &str, commands: Commands) {
        self.index.retain(|_, owner| owner != module);
        let reg = ModuleCommandReg {
            commands: commands.commands.clone(),
            catch_all: commands.commands.is_empty(),
            alert_on_unknown: commands.alert_on_unknown_command,
        };
        for cmd in &reg.commands {
            self.index
                .insert((cmd.command_flag.clone(), cmd.command_name.clone()), module.to_string());
        }
        self.modules.insert(module.to_string(), reg);
    }

    /// The module subscribed to a specific (flag, command). None if unregistered.
    pub fn owner(&self, flag: &str, name: &str) -> Option<String> {
        self.index
            .get(&(flag.to_string(), name.to_string()))
            .cloned()
    }

    /// Does any registered command use this flag prefix (e.g. `!`)?
    pub fn flag_known(&self, flag: &str) -> bool {
        self.index.keys().any(|(f, _)| f == flag)
    }

    /// The registered Flag definitions for a (flag, command), if any.
    pub fn command_flags(&self, flag: &str, name: &str) -> Vec<Flag> {
        self.index
            .get(&(flag.to_string(), name.to_string()))
            .and_then(|owner| {
                self.modules
                    .get(owner)
                    .and_then(|reg| {
                        reg.commands
                            .iter()
                            .find(|c| c.command_flag == flag && c.command_name == name)
                    })
            })
            .map(|c| c.command_flags.clone())
            .unwrap_or_default()
    }

    /// The description of a (flag, command), for `!help`.
    pub fn command_description(&self, flag: &str, name: &str) -> Option<String> {
        self.index
            .get(&(flag.to_string(), name.to_string()))
            .and_then(|owner| {
                self.modules.get(owner).and_then(|reg| {
                    reg.commands
                        .iter()
                        .find(|c| c.command_flag == flag && c.command_name == name)
                        .map(|c| c.command_description.clone())
                })
            })
    }

    /// Modules flagged as catch-alls (empty `Commands` registration).
    pub fn catch_alls(&self) -> Vec<String> {
        self.modules
            .iter()
            .filter(|(_, r)| r.catch_all)
            .map(|(m, _)| m.clone())
            .collect()
    }

    /// A module that owns `flag` and has `alert_on_unknown_command` set, so an
    /// unregistered command under that flag gets the apology reply.
    pub fn alert_owner_for(&self, flag: &str) -> Option<String> {
        for ((f, _), owner) in &self.index {
            if f == flag
                && let Some(reg) = self.modules.get(owner)
                && reg.alert_on_unknown
            {
                return Some(owner.clone());
            }
        }
        None
    }

    /// Every registered command (for `!help`), deduplicated.
    pub fn all_commands(&self) -> Vec<Command> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for reg in self.modules.values() {
            for c in &reg.commands {
                let key = (c.command_flag.clone(), c.command_name.clone());
                if seen.insert(key) {
                    out.push(c.clone());
                }
            }
        }
        out
    }
}

/// A parsed chat command: the command + its flag values + the trailing args.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    /// The command with parsed Flag values attached (embedded in command_flags).
    pub command: Command,
    /// Everything after the command + flags (the message/args, trimmed).
    /// Consumed by the command modules (e.g. reprimand's target + reason).
    #[allow(dead_code)]
    pub args: String,
}

/// Parse a raw message into a command, or None if it isn't a registered-flag
/// message. `-p:2` and `-p 2` are both accepted; bare `-d` = boolean "true";
/// the leading `-` is stripped so the flag name is just `p`.
pub fn parse_command(raw: &str, registry: &CommandRegistry) -> Option<ParsedCommand> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    // Find which registered flag this message starts with.
    let mut matched_flag: Option<String> = None;
    for (f, _) in registry.index.keys() {
        if raw.starts_with(f) {
            // Pick the longest matching flag so "->" isn't shadowed by "-".
            match &matched_flag {
                Some(cur) if f.len() > cur.len() => matched_flag = Some(f.clone()),
                None => matched_flag = Some(f.clone()),
                _ => {}
            }
        }
    }
    let flag = matched_flag?;
    let after_flag = raw[flag.len()..].trim_start();
    if after_flag.is_empty() {
        return None;
    }
    // Command token = up to first whitespace.
    let (name, rest) = match after_flag.find(char::is_whitespace) {
        Some(i) => (&after_flag[..i], after_flag[i..].trim()),
        None => (after_flag, ""),
    };

    let definitions = registry.command_flags(&flag, name);
    let mut parsed_flags: Vec<Flag> = Vec::new();
    let mut args: Vec<String> = Vec::new();

    let rest = rest.trim();
    if !rest.is_empty() {
        let tokens: Vec<String> = rest.split_whitespace().map(|p| p.to_string()).collect();
        let mut i = 0;
        while i < tokens.len() {
            let tok = &tokens[i];
            if tok.starts_with('-') && tok.len() > 1 {
                // Flag token: -name[:value] or -name [value].
                let body = &tok[1..];
                let (fname, inline_val) = match body.split_once(':') {
                    Some((n, v)) => (n.to_string(), Some(v.to_string())),
                    None => (body.to_string(), None),
                };
                let value = if let Some(v) = inline_val {
                    v
                } else if i + 1 < tokens.len() && !tokens[i + 1].starts_with('-') {
                    i += 1;
                    tokens[i].clone()
                } else {
                    "true".to_string()
                };
                let mut pf = Flag {
                    flag_name: fname.clone(),
                    flag_description: String::new(),
                    limiting_type: 0,
                    min_val: 0.0,
                    max_val: 0.0,
                    options: Vec::new(),
                    value,
                };
                // Copy the registered description/limits if known.
                if let Some(def) = definitions.iter().find(|d| d.flag_name == fname) {
                    pf.flag_description = def.flag_description.clone();
                    pf.limiting_type = def.limiting_type;
                    pf.min_val = def.min_val;
                    pf.max_val = def.max_val;
                    pf.options = def.options.clone();
                }
                parsed_flags.push(pf);
            } else {
                // Positional token = part of the trailing message/args.
                args.push(tok.clone());
            }
            i += 1;
        }
    }

    Some(ParsedCommand {
        command: Command {
            command_name: name.to_string(),
            command_flag: flag.clone(),
            command_description: registry.command_description(&flag, name).unwrap_or_default(),
            command_flags: parsed_flags,
        },
        args: args.join(" "),
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    fn sample_registry() -> CommandRegistry {
        let mut r = CommandRegistry::default();
        // tts module registers `!tts` with flags p, r, v.
        let tts = Commands {
            commands: vec![Command {
                command_name: "tts".into(),
                command_flag: "!".into(),
                command_description: "read chat out loud".into(),
                command_flags: vec![
                    Flag { flag_name: "p".into(), flag_description: "priority".into(), limiting_type: 0, min_val: 0.0, max_val: 0.0, options: vec![], value: String::new() },
                    Flag { flag_name: "v".into(), flag_description: "voice".into(), limiting_type: 0, min_val: 0.0, max_val: 0.0, options: vec![], value: String::new() },
                ],
            }],
            alert_on_unknown_command: true,
        };
        r.register("tts-service", tts);
        // reprimand module registers `!reprimand` (no flags) — not alerting.
        let reprimand = Commands {
            commands: vec![Command {
                command_name: "reprimand".into(),
                command_flag: "!".into(),
                command_description: "reprimand a user".into(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        };
        r.register("reprimand", reprimand);
        r
    }

    #[test]
    fn parses_command_with_flags_both_forms() {
        let r = sample_registry();
        let p = parse_command("!tts -p 2 -r:1.4 -v 88 hey! how are you today?", &r).unwrap();
        assert_eq!(p.command.command_name, "tts");
        assert_eq!(p.command.command_flag, "!");
        let vals: Vec<(String, String)> = p.command.command_flags.iter().map(|f| (f.flag_name.clone(), f.value.clone())).collect();
        assert_eq!(vals, vec![("p".to_string(), "2".to_string()), ("r".to_string(), "1.4".to_string()), ("v".to_string(), "88".to_string())]);
        // The `-` is stripped; positional args = the trailing message.
        assert_eq!(p.args, "hey! how are you today?");
    }

    #[test]
    fn parses_reprimand_with_mention_and_reason() {
        let r = sample_registry();
        let p = parse_command("!reprimand @user saying offensive things", &r).unwrap();
        assert_eq!(p.command.command_name, "reprimand");
        assert!(p.command.command_flags.is_empty());
        assert_eq!(p.args, "@user saying offensive things");
    }

    #[test]
    fn bare_flag_is_boolean() {
        let r = sample_registry();
        let p = parse_command("!tts -v hey there", &r).unwrap();
        assert_eq!(p.command.command_flags[0].value, "hey");
        // `-v` followed by "hey" — "hey" is consumed as the value; nothing left.
        let p2 = parse_command("!tts -v", &r).unwrap();
        assert_eq!(p2.command.command_flags[0].value, "true");
    }

    #[test]
    fn unknown_command_parses_but_has_no_owner() {
        let r = sample_registry();
        // `!unknown` starts with a registered flag (!) but isn't registered.
        let p = parse_command("!unknown foo", &r).unwrap();
        assert_eq!(p.command.command_name, "unknown");
        assert!(r.owner("!", "unknown").is_none());
        // alert_owner_for returns the module that alerts (tts-service owns "!").
        assert_eq!(r.alert_owner_for("!").unwrap(), "tts-service");
    }

    #[test]
    fn non_flag_message_is_none() {
        let r = sample_registry();
        assert!(parse_command("hello world", &r).is_none());
        assert!(parse_command(">tts hi", &r).is_none()); // ">" isn't registered
    }

    #[test]
    fn catch_all_and_owner() {
        let mut r = sample_registry();
        r.register("catch-all-mod", Commands { commands: vec![], alert_on_unknown_command: false });
        assert_eq!(r.catch_alls(), vec!["catch-all-mod".to_string()]);
        assert_eq!(r.owner("!", "tts").unwrap(), "tts-service");
        assert_eq!(r.owner("!", "reprimand").unwrap(), "reprimand");
        assert_eq!(r.all_commands().len(), 2);
    }
}
