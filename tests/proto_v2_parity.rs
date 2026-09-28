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

/// A regular (non-`oneof`) field, normalized so that cosmetic differences in the
/// source text do not read as a wire difference.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldDecl {
    /// The base type with `repeated`/`optional` stripped and `map<K, V>` reduced
    /// to a single punctuation-only-spaced token.
    ty: String,
    repeated: bool,
    optional: bool,
}

impl FieldDecl {
    /// The comparable form of this declaration. The labels are folded back IN so
    /// that the comparison stays strict on purpose: `repeated Flag` and a bare
    /// `Flag` must not compare equal (a repeated field is a list, never a
    /// singular value), and `optional string` and a bare `string` must not
    /// compare equal either (only one of those two can be absent on the wire).
    /// Collapsing either pair would be the exact class of bug this test exists
    /// to catch, so the allowlist in `nested_field_parity` is the only escape.
    fn parity_key(&self) -> String {
        if self.repeated {
            format!("repeated {}", self.ty)
        } else if self.optional {
            format!("optional {}", self.ty)
        } else {
            self.ty.clone()
        }
    }
}

/// Collapse cosmetic differences in a declared type. Only whitespace is touched
/// — no type is ever aliased onto another, because an alias would hide exactly
/// the break this test exists to catch: `float` is wire type fixed32 and
/// `double` is fixed64, and a decoder silently DROPS a field whose wire type
/// does not match rather than reporting an error.
fn normalize_type(ty: &str) -> String {
    let squashed = ty.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::with_capacity(squashed.len());
    let mut after_punct = false;
    for ch in squashed.chars() {
        if ch == ' ' {
            // A space inside a generic argument list is not a token boundary, so
            // `map<string, string>` and `map<string,string>` are the same type.
            if !after_punct && !out.is_empty() {
                out.push(' ');
            }
            continue;
        }
        after_punct = matches!(ch, '<' | '>' | ',');
        if after_punct {
            while out.ends_with(' ') {
                out.pop();
            }
        }
        out.push(ch);
    }
    out
}

/// Parse the REGULAR (non-`oneof`, non-`reserved`) fields of a message body into
/// field-number -> declaration.
///
/// The container tests only ever compared the `oneof` tag -> type mapping, which
/// is blind to anything INSIDE a message: a `float` -> `double` swap on `Flag`
/// fields 4/5 left every one of them green. This closes that hole.
///
/// How the source text is taken apart, and why:
///   * the field NAME is the trailing identifier and the TYPE is everything
///     before it. Splitting on the first space instead would make
///     `map<string, string> css_properties = 1;` parse its type as `map<string,`
///     and its name as `string>`.
///   * the `oneof payload { ... }` region is cut out first, so its members are
///     never mistaken for regular fields, and `reserved` lines are skipped so a
///     tag list never parses as a field.
///   * the `= N;` tail is stripped by splitting on the LAST `=`, and a trailing
///     comment is cut defensively (`load` already strips comments, but this
///     helper must not silently lose a field if that ever changes).
fn regular_fields(message_body: &str) -> BTreeMap<u32, FieldDecl> {
    let mut scannable = message_body.to_string();
    if let Some(at) = scannable.find("oneof ")
        && let Some(open) = scannable[at..].find('{').map(|n| at + n)
    {
        let span = 1 + body_after(&scannable, open).len();
        scannable.replace_range(open..open + span, "");
    }

    let mut out = BTreeMap::new();
    for line in scannable.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("reserved ") {
            continue;
        }
        let Some((lhs, number)) = line.rsplit_once('=') else {
            continue;
        };
        let Ok(number) = number.trim().trim_end_matches(';').trim().parse::<u32>() else {
            continue;
        };
        let lhs = match lhs.find("//") {
            Some(n) => lhs[..n].trim(),
            None => lhs.trim(),
        };
        let Some(name_at) = lhs.rfind(char::is_whitespace) else {
            continue;
        };
        let (mut ty, _name) = lhs.split_at(name_at);

        let (mut repeated, mut optional) = (false, false);
        loop {
            if let Some(rest) = ty.strip_prefix("repeated ") {
                repeated = true;
                ty = rest.trim_start();
            } else if let Some(rest) = ty.strip_prefix("optional ") {
                optional = true;
                ty = rest.trim_start();
            } else {
                break;
            }
        }
        let ty = normalize_type(ty);
        if ty.is_empty() {
            continue;
        }
        out.insert(
            number,
            FieldDecl {
                ty,
                repeated,
                optional,
            },
        );
    }
    out
}

fn reserved_tags(message_body: &str) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    for line in message_body.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("reserved ") else {
            continue;
        };
        for part in rest.trim_end_matches(';').split(',') {
            let part = part.trim();
            if let Ok(tag) = part.parse::<u32>() {
                out.insert(tag);
            }
        }
    }
    out
}

struct Protos {
    v1_container: BTreeMap<u32, String>,
    v1_defined: BTreeSet<String>,
    v1_fields: BTreeMap<String, BTreeMap<u32, FieldDecl>>,
    v2_for_module: BTreeMap<u32, String>,
    v2_for_engine: BTreeMap<u32, String>,
    v2_defined: BTreeSet<String>,
    v2_reserved_for_module: BTreeSet<u32>,
    v2_reserved_for_engine: BTreeSet<u32>,
    v2_fields: BTreeMap<String, BTreeMap<u32, FieldDecl>>,
}

fn load() -> Protos {
    let v1 = strip_comments(&read_proto("cockatiel_protobuf.proto"));
    let v2 = strip_comments(&read_proto("cockatiel_v2.proto"));
    let v1_msgs = message_bodies(&v1);
    let v2_msgs = message_bodies(&v2);
    Protos {
        v1_container: oneof_payloads(v1_msgs.get("Container").expect("v1 Container")),
        v1_defined: v1_msgs.keys().cloned().collect(),
        v1_fields: v1_msgs
            .iter()
            .map(|(name, body)| (name.clone(), regular_fields(body)))
            .collect(),
        v2_for_module: oneof_payloads(
            v2_msgs
                .get("ContainerForModule")
                .expect("v2 ContainerForModule"),
        ),
        v2_for_engine: oneof_payloads(
            v2_msgs
                .get("ContainerForEngine")
                .expect("v2 ContainerForEngine"),
        ),
        v2_defined: v2_msgs.keys().cloned().collect(),
        v2_reserved_for_module: reserved_tags(v2_msgs.get("ContainerForModule").unwrap()),
        v2_reserved_for_engine: reserved_tags(v2_msgs.get("ContainerForEngine").unwrap()),
        v2_fields: v2_msgs
            .iter()
            .map(|(name, body)| (name.clone(), regular_fields(body)))
            .collect(),
    }
}

#[test]
fn v2_compiles() {
    let out = Command::new("protoc")
        .arg("--proto_path")
        .arg(lib_dir())
        .arg("--descriptor_set_out")
        .arg(std::env::temp_dir().join("cockatiel_v2_parity.pb"))
        .arg(lib_dir().join("cockatiel_v2.proto"))
        .output()
        .expect("protoc must be on PATH");
    assert!(
        out.status.success(),
        "cockatiel_v2.proto does not compile:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_parser_actually_read_the_containers() {
    let p = load();
    assert!(
        p.v1_container.len() >= 24,
        "v1 Container only yielded {} payloads; the parser is broken, so every \
         parity test below would pass vacuously",
        p.v1_container.len()
    );
    assert!(
        p.v2_for_module.len() >= 15 && p.v2_for_engine.len() >= 15,
        "v2 containers yielded {}/{} payloads; the parser is broken",
        p.v2_for_module.len(),
        p.v2_for_engine.len()
    );
}

#[test]
fn v2_carries_every_payload_v1_carried() {
    let p = load();
    let carried: BTreeSet<&str> = p
        .v2_for_module
        .values()
        .chain(p.v2_for_engine.values())
        .map(|s| s.as_str())
        .collect();
    let missing: Vec<&str> = p
        .v1_container
        .values()
        .map(|s| s.as_str())
        .filter(|t| !carried.contains(t))
        .collect();
    assert!(
        missing.is_empty(),
        "v1 carried these payloads but v2 dropped them: {missing:?}"
    );
}

#[test]
fn every_v2_payload_is_actually_defined() {
    let p = load();
    for tag_type in p.v2_for_module.iter().chain(p.v2_for_engine.iter()) {
        let (tag, ty) = tag_type;
        assert!(
            p.v2_defined.contains(ty),
            "tag {tag} carries undefined type {ty}"
        );
    }
}

#[test]
fn v2_never_reuses_a_v1_tag_for_a_different_type() {
    let p = load();
    let mut v2_all: BTreeMap<u32, &String> = BTreeMap::new();
    for (tag, ty) in p.v2_for_module.iter().chain(p.v2_for_engine.iter()) {
        if let Some(prev) = v2_all.insert(*tag, ty) {
            assert_eq!(
                prev, ty,
                "tag {tag} means both {prev} and {ty} in v2"
            );
        }
    }
    for (tag, v1_ty) in &p.v1_container {
        if let Some(v2_ty) = v2_all.get(tag) {
            assert_eq!(
                v1_ty.as_str(),
                v2_ty.as_str(),
                "tag {tag} was {v1_ty} in v1 but {v2_ty} in v2 - a silent wire break"
            );
        }
    }
}

#[test]
fn every_v1_tag_v2_drops_is_reserved() {
    let p = load();
    for (name, used, reserved) in [
        (
            "ContainerForModule",
            &p.v2_for_module,
            &p.v2_reserved_for_module,
        ),
        (
            "ContainerForEngine",
            &p.v2_for_engine,
            &p.v2_reserved_for_engine,
        ),
    ] {
        let dropped: BTreeSet<u32> = p
            .v1_container
            .keys()
            .filter(|t| !used.contains_key(t))
            .copied()
            .collect();
        let unreserved: Vec<u32> = dropped.difference(reserved).copied().collect();
        assert!(
            unreserved.is_empty(),
            "{name} drops v1 tags {unreserved:?} without reserving them"
        );
    }
}

#[test]
fn no_container_reserves_a_tag_it_also_uses() {
    let p = load();
    for (name, used, reserved) in [
        (
            "ContainerForModule",
            &p.v2_for_module,
            &p.v2_reserved_for_module,
        ),
        (
            "ContainerForEngine",
            &p.v2_for_engine,
            &p.v2_reserved_for_engine,
        ),
    ] {
        let clash: Vec<u32> = used.keys().filter(|t| reserved.contains(t)).copied().collect();
        assert!(clash.is_empty(), "{name} both uses and reserves {clash:?}");
    }
}

#[test]
fn v2_keeps_the_pipeline_stage_as_the_message_type() {
    let p = load();
    let carried: BTreeSet<&str> = p
        .v2_for_module
        .values()
        .chain(p.v2_for_engine.values())
        .map(|s| s.as_str())
        .collect();
    for stage in [
        "MessagePreProcess",
        "MessageInProcess",
        "MessagePostProcess",
    ] {
        assert!(
            carried.contains(stage),
            "{stage} is not carried, so a message cannot advance through the pipeline"
        );
    }
    assert!(
        !carried.contains("ChatMessage"),
        "a bare ChatMessage payload would leave the stage ambiguous; it belongs \
         only inside the stage messages"
    );
}

#[test]
fn credential_rotation_is_engine_to_module_only() {
    let p = load();
    assert!(
        p.v1_defined.contains("AuthNew"),
        "sanity: v1 defines AuthNew"
    );
    assert!(
        p.v2_for_module.values().any(|t| t == "AuthNew"),
        "AuthNew must be engine -> module, or modules can never be rotated to a \\
         new token"
    );
    assert!(
        !p.v2_for_engine.values().any(|t| t == "AuthNew"),
        "a module must not be able to ask the engine to rotate its credential"
    );
}

/// The ONLY nested-field deviation from v1 that v2 is allowed to carry. Every
/// other shared message must be identical in field number and in wire type.
///
/// `ChatMessageRejected.processed_message`: `string` -> `optional string`. This
/// is wire-COMPATIBLE — both encode as a single length-delimited field, so a v1
/// peer still reads the value without complaint — and only the PRESENCE
/// semantics differ: in v1 an empty string was the only way to say "dropped", so
/// a module that deliberately censored a message down to nothing was
/// indistinguishable from one that dropped it. That ambiguity is the whole point
/// of the change, so it is deliberate rather than a regression.
///
/// Nothing else may be added here: a second entry means a second silent wire
/// break, which is exactly what `nested_field_parity` exists to prevent.
const ALLOWED_NESTED_DEVIATIONS: &[(&str, u32)] = &[("ChatMessageRejected", 3)];

#[test]
fn nested_field_parity() {
    let p = load();

    let shared: Vec<&str> = p
        .v1_fields
        .keys()
        .filter(|name| p.v2_fields.contains_key(*name))
        .map(|name| name.as_str())
        .collect();
    assert!(
        shared.len() >= 25,
        "only {} messages are defined in both specs, so there is nothing to \
         compare; the message parser is broken",
        shared.len()
    );

    let mut mismatches = Vec::new();
    let mut used_deviations = BTreeSet::new();
    for name in &shared {
        let v1_fields = &p.v1_fields[*name];
        let v2_fields = &p.v2_fields[*name];
        let numbers: BTreeSet<u32> = v1_fields.keys().chain(v2_fields.keys()).copied().collect();
        for number in numbers {
            match (v1_fields.get(&number), v2_fields.get(&number)) {
                (Some(a), Some(b)) if a.parity_key() == b.parity_key() => {}
                (Some(_), Some(_)) if ALLOWED_NESTED_DEVIATIONS.contains(&(*name, number)) => {
                    used_deviations.insert((*name, number));
                }
                (Some(a), Some(b)) => mismatches.push(format!(
                    "{name} field {number}: v1 declares `{}`, v2 declares `{}`. \
                     A differing TYPE is a silent wire break (a decoder drops a \
                     field whose wire type does not match instead of erroring), and \
                     a differing LABEL changes presence. If this change is really \
                     intended it must be justified in the v2 header and added to \
                     ALLOWED_NESTED_DEVIATIONS.",
                    a.parity_key(),
                    b.parity_key()
                )),
                (Some(a), None) => mismatches.push(format!(
                    "{name} field {number}: v1 declares `{}` and v2 dropped it, so a \
                     v1 peer's value is discarded",
                    a.parity_key()
                )),
                (None, Some(b)) => mismatches.push(format!(
                    "{name} field {number}: v2 ADDS `{}`, which no v1 peer will ever read",
                    b.parity_key()
                )),
                (None, None) => {
                    unreachable!("field {number} was drawn from the union of both key sets")
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "v2 must match v1 field-for-field on every shared message except the one \
         allowlisted presence change. These differ:\n  {}",
        mismatches.join("\n  ")
    );

    // An allowlist entry that no longer describes a real difference is worse
    // than no allowlist: it silently widens the blast radius the next deviation
    // is allowed to have. Fail loudly so it gets deleted instead.
    let stale: Vec<String> = ALLOWED_NESTED_DEVIATIONS
        .iter()
        .filter(|(name, number)| !used_deviations.contains(&(*name, *number)))
        .map(|(name, number)| format!("{name} field {number}"))
        .collect();
    assert!(
        stale.is_empty(),
        "ALLOWED_NESTED_DEVIATIONS lists {stale:?}, but v1 and v2 now agree on \
         those fields. Remove the entries - an unused allowlist entry is a hole \
         with no one watching it"
    );
}

/// The container tests are only as good as the parser behind them, and this repo
/// has ALREADY shipped a parser that returned an empty map while every parity
/// test it fed stayed green. `regular_fields` is a new parser feeding a new
/// test, so it needs the same guard: prove it actually read the fields before
/// trusting it to compare them.
#[test]
fn the_field_parser_actually_read_the_messages() {
    let p = load();

    for (label, fields) in [("v1", &p.v1_fields), ("v2", &p.v2_fields)] {
        let total: usize = fields.values().map(BTreeMap::len).sum();
        assert!(
            total >= 130,
            "{label} yielded only {total} nested fields across {} messages; the \
             field parser is broken, so `nested_field_parity` would pass vacuously",
            fields.len()
        );
    }

    // Exact counts for messages large and varied enough that a parser which
    // dropped `repeated`, `map<>` or nested-message fields could not land on
    // them by accident.
    for (label, fields) in [("v1", &p.v1_fields), ("v2", &p.v2_fields)] {
        for (name, expected) in [("SendToPlatforms", 9), ("Prompt", 12), ("UserData", 10)] {
            let got = fields.get(name).map_or(0, BTreeMap::len);
            assert_eq!(
                got, expected,
                "{label} {name} parsed to {got} regular fields, not {expected}; the \
                 field parser is losing or inventing fields"
            );
        }
    }

    // The containers must report ONLY their header fields. If the `oneof` region
    // leaked into the regular-field map these counts would be off by ~24 and
    // ~18 respectively, which is the specific bug this parser is most able to
    // make.
    for (label, fields, name, expected) in [
        ("v1", &p.v1_fields, "Container", 4),
        ("v2", &p.v2_fields, "ContainerForModule", 3),
        ("v2", &p.v2_fields, "ContainerForEngine", 4),
    ] {
        let got = fields.get(name).map_or(0, BTreeMap::len);
        assert_eq!(
            got, expected,
            "{label} {name} reported {got} regular fields, not {expected}; the \
             oneof members or the reserved list are being parsed as regular fields"
        );
    }

    // And a `map<,>` field must survive parsing as one field of one type, not
    // split at the comma or dropped entirely.
    for (label, fields) in [("v1", &p.v1_fields), ("v2", &p.v2_fields)] {
        let template = fields
            .get("UserStylingTemplate")
            .and_then(|f| f.get(&1))
            .unwrap_or_else(|| panic!("{label} UserStylingTemplate field 1 is missing"));
        assert_eq!(
            template.parity_key(),
            "map<string,string>",
            "{label} UserStylingTemplate field 1 parsed as `{}`; map<,> fields must \
             normalize to a single type token",
            template.parity_key()
        );
    }
}
