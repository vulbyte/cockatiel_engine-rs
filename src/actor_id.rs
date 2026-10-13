//! The engine's own actor identity: the giver stamped on operator-issued
//! commendations and reprimands.
//!
//! The id is built only from uuid-safe characters (`[0-9a-f-]`) so it can be
//! stored wherever a uuid7 is expected. It carries the build commit (stamped by
//! `build.rs`), the epoch milliseconds at issue, the numeric crate version and a
//! fixed `c0c` suffix. The timestamp makes every issued id unique, which
//! intentionally sidesteps the user-db's per-giver 24h reprimand cooldown for
//! operator actions.

use crate::stream_rewards::now_ms;

/// Build the actor id for the running engine: build commit, current epoch
/// milliseconds, and the numeric crate version.
pub(crate) fn cockatiel_actor_id() -> String {
    cockatiel_actor_id_at(
        now_ms(),
        env!("COCKATIEL_GIT_HASH"),
        env!("CARGO_PKG_VERSION"),
    )
}

/// Pure formatter for [`cockatiel_actor_id`]:
/// `{epoch_ms}-{git}-{maj}{min}{pat}-c0c`.
///
/// Every segment is filtered to uuid-safe characters, so the result only ever
/// contains `[0-9a-f-]`. `git` is lowercased first (uppercase hex is not in that
/// set); a non-hex fallback such as `unknown` contributes no characters. The
/// version's pre-release/build suffix is stripped and its digits are kept, so
/// `"0.1.0"` becomes `010` and `"1.2.3-rc.1+build"` becomes `123`.
pub(crate) fn cockatiel_actor_id_at(epoch_ms: i64, git: &str, version: &str) -> String {
    let git: String = git
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .filter(|c| matches!(c, '0'..='9' | 'a'..='f'))
        .collect();

    let core = version.split(['-', '+']).next().unwrap_or(version);
    let mut ver = String::new();
    for part in core.split('.').take(3) {
        let digits: String = part.chars().filter(|c| c.is_ascii_digit()).collect();
        ver.push_str(if digits.is_empty() { "0" } else { &digits });
    }

    format!("{epoch_ms}-{git}-{ver}-c0c")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatting_shape_is_epoch_git_version_suffix() {
        assert_eq!(
            cockatiel_actor_id_at(1_730_000_000_000, "68779ba", "0.1.0"),
            "1730000000000-68779ba-010-c0c"
        );
    }

    #[test]
    fn only_uuid_safe_characters_are_emitted() {
        let ids = [
            cockatiel_actor_id_at(1_730_000_000_000, "68779ba", "0.1.0"),
            cockatiel_actor_id_at(0, "unknown", "1.2.3-rc.1+build"),
            cockatiel_actor_id_at(42, "ABCDEF0", "12.7"),
            cockatiel_actor_id_at(-5, "", ""),
        ];
        for id in ids {
            assert!(
                id.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f' | '-')),
                "id {id:?} contains a non uuid-safe character"
            );
        }
    }

    #[test]
    fn version_prerelease_and_build_suffixes_are_stripped() {
        assert_eq!(
            cockatiel_actor_id_at(0, "abc", "1.2.3-rc.1+build9"),
            "0-abc-123-c0c"
        );
    }
}
