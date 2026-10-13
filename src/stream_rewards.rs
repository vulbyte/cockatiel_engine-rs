//! Per-user stream streak + within-stream activity multipliers.
//!
//! The score module sends a positive `userdb_adjust_score` delta through the
//! engine; the user-db proto is EXTERNAL and cannot carry a multiplier, so the
//! engine computes it here and scales the positive delta before forwarding.
//!
//! Two factors stack, each 1.0x..2.0x, for a combined cap of 4.0x:
//!
//! * **streak** — consecutive streams the user attended. 1st stream 1.0x, then
//!   +0.1x per consecutive stream, capped at 2.0x (so the 11th stream hits the
//!   cap).
//! * **activity** — how early in the current stream the user showed up:
//!   `(now - first_seen) / (now - started)` (0..1), giving 1.0x..2.0x.
//!
//! All money/score math is `f32` (32-bit rule); the only 64-bit values here are
//! epoch-millisecond timestamps, which are the documented exception.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Shared handle, mirroring `auth_store` / `orchestrator` wiring.
pub type SharedStreamTracker = Arc<Mutex<StreamTracker>>;

/// Current wall-clock time in epoch milliseconds (the 64-bit timestamp
/// exception).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Streak multiplier from a consecutive-stream count.
///
/// `1 -> 1.0`, `2 -> 1.1`, `11 -> 2.0` (cap). A count below 1 is treated as 1.
pub fn streak_multiplier(streak_count: i64) -> f32 {
    let count = streak_count.max(1) as f32;
    (1.0 + 0.1 * (count - 1.0)).min(2.0)
}

/// Activity multiplier from presence within the current stream.
///
/// `presence_ratio = (now - first_seen) / max(now - started, 1)`, clamped to
/// `0..1`. No presence (or no stream) is 1.0; full presence is 2.0.
pub fn activity_multiplier(now_ms: i64, first_seen_ms: i64, started_ms: i64) -> f32 {
    let elapsed = (now_ms - started_ms).max(1) as f32;
    let presence = ((now_ms - first_seen_ms) as f32 / elapsed).clamp(0.0, 1.0);
    (1.0 + presence).min(2.0)
}

/// The two factors stack (product), capped at 4.0.
pub fn combined_multiplier(streak_mult: f32, activity_mult: f32) -> f32 {
    (streak_mult * activity_mult).min(4.0)
}

/// Scale a positive delta by `combined`; a negative or zero delta is unchanged.
pub fn apply_multiplier(delta: i32, combined: f32) -> i32 {
    if delta > 0 {
        (delta as f32 * combined).round() as i32
    } else {
        delta
    }
}

/// Tracks the open stream, per-user attendance and persisted streaks.
#[derive(Default)]
pub struct StreamTracker {
    /// Id of the stream currently open, if any.
    pub current_stream_uuid: Option<String>,
    /// Epoch-ms the current stream opened.
    pub started_ms: i64,
    /// Id of the most recently ENDED stream, used to decide whether a user's
    /// attendance continues their streak.
    pub previous_stream_uuid: Option<String>,
    /// user -> (first_seen_ms, last_seen_ms) within the current stream.
    pub attendance: HashMap<String, (i64, i64)>,
    /// user -> (last_stream_uuid, consecutive_count).
    pub streaks: HashMap<String, (String, i64)>,
    /// Per-user streak count cached for the CURRENT stream (first message wins).
    stream_streak_cache: HashMap<String, i64>,
}

impl StreamTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed persisted streaks loaded from the timeline DB.
    pub fn load_streaks(&mut self, rows: Vec<(String, String, i64)>) {
        for (user, last_stream_uuid, count) in rows {
            self.streaks.insert(user, (last_stream_uuid, count));
        }
    }

    /// Open a new stream. If one is somehow already open it is closed first, so
    /// its streak is flushed and its id becomes `previous`.
    pub fn open_stream(&mut self, now: i64) {
        if self.current_stream_uuid.is_some() {
            self.close_stream(now);
        }
        self.current_stream_uuid = Some(uuid::Uuid::now_v7().to_string());
        self.started_ms = now;
        self.attendance.clear();
        self.stream_streak_cache.clear();
    }

    /// Close the current stream: every attending user's streak advances by one
    /// if they attended the previous stream, otherwise resets to 1. Returns the
    /// updated `(user, last_stream_uuid, count)` rows for persistence.
    pub fn close_stream(&mut self, _now: i64) -> Vec<(String, String, i64)> {
        let Some(cur) = self.current_stream_uuid.take() else {
            return Vec::new();
        };
        let mut updated = Vec::new();
        for user in self.attendance.keys() {
            let count = match self.streaks.get(user) {
                Some((last, prev_count)) if Some(last.as_str()) == self.previous_stream_uuid.as_deref() => {
                    prev_count + 1
                }
                _ => 1,
            };
            self.streaks.insert(user.clone(), (cur.clone(), count));
            updated.push((user.clone(), cur.clone(), count));
        }
        // The stream that just ended becomes the continuation baseline.
        self.previous_stream_uuid = Some(cur);
        self.attendance.clear();
        self.stream_streak_cache.clear();
        updated
    }

    /// Record a user's presence in the current stream (no-op if none is open).
    pub fn note_attendance(&mut self, user: &str, now: i64) {
        if self.current_stream_uuid.is_none() || user.is_empty() {
            return;
        }
        match self.attendance.get_mut(user) {
            Some((_, last)) => {
                if now > *last {
                    *last = now;
                }
            }
            None => {
                self.attendance.insert(user.to_string(), (now, now));
            }
        }
    }

    /// `(streak_count, combined)` for `user` at `now`. The streak count is
    /// computed once per stream (on the user's first message) and cached. With
    /// no stream open the multiplier is neutral (`(0, 1.0)`).
    pub fn multiplier(&mut self, user: &str, now: i64) -> (i64, f32) {
        if self.current_stream_uuid.is_none() {
            return (0, 1.0);
        }
        let streak_count = match self.stream_streak_cache.get(user) {
            Some(c) => *c,
            None => {
                let c = match self.streaks.get(user) {
                    Some((last, count))
                        if Some(last.as_str()) == self.previous_stream_uuid.as_deref() =>
                    {
                        count + 1
                    }
                    _ => 1,
                };
                self.stream_streak_cache.insert(user.to_string(), c);
                c
            }
        };
        let streak_mult = streak_multiplier(streak_count);
        let activity_mult = match self.attendance.get(user) {
            Some((first_seen, _)) => activity_multiplier(now, *first_seen, self.started_ms),
            None => 1.0,
        };
        (streak_count, combined_multiplier(streak_mult, activity_mult))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32) {
        assert!((a - b).abs() < 1e-4, "expected {b}, got {a}");
    }

    #[test]
    fn streak_multiplier_hits_its_anchors_and_cap() {
        approx(streak_multiplier(1), 1.0);
        approx(streak_multiplier(2), 1.1);
        approx(streak_multiplier(11), 2.0);
        approx(streak_multiplier(50), 2.0);
        // A nonsensical count still floors at the first-stream multiplier.
        approx(streak_multiplier(0), 1.0);
    }

    #[test]
    fn activity_multiplier_spans_zero_to_full_presence() {
        // first_seen == now -> no presence -> 1.0
        approx(activity_multiplier(10_000, 10_000, 0), 1.0);
        // first_seen == started -> full presence -> 2.0
        approx(activity_multiplier(10_000, 0, 0), 2.0);
        // Halfway -> 1.5
        approx(activity_multiplier(10_000, 5_000, 0), 1.5);
        // Never negative even with clock skew.
        approx(activity_multiplier(1_000, 5_000, 0), 1.0);
    }

    #[test]
    fn combined_multiplier_is_the_product_capped_at_four() {
        approx(combined_multiplier(1.0, 1.0), 1.0);
        approx(combined_multiplier(1.5, 1.5), 2.25);
        approx(combined_multiplier(2.0, 2.0), 4.0);
        // 3.0 * 2.0 would be 6.0 -> capped.
        approx(combined_multiplier(3.0, 2.0), 4.0);
    }

    #[test]
    fn apply_multiplier_leaves_non_positive_deltas_alone() {
        assert_eq!(apply_multiplier(-5, 4.0), -5);
        assert_eq!(apply_multiplier(0, 4.0), 0);
        assert_eq!(apply_multiplier(10, 1.0), 10);
        assert_eq!(apply_multiplier(10, 1.5), 15);
        // Rounds to the nearest integer.
        assert_eq!(apply_multiplier(3, 1.5), 5);
    }

    #[test]
    fn close_stream_advances_returning_user_and_resets_one_who_skipped() {
        let mut t = StreamTracker::new();

        // Stream 1: alice and bob both attend -> both land on streak 1.
        t.open_stream(1_000);
        t.note_attendance("alice", 1_500);
        t.note_attendance("bob", 1_600);
        let rows = t.close_stream(2_000);
        assert_eq!(rows.len(), 2);
        assert_eq!(t.streaks.get("alice").unwrap().1, 1);
        assert_eq!(t.streaks.get("bob").unwrap().1, 1);

        // Stream 2: alice returns, bob skips. Alice's first message shows 2.
        t.open_stream(3_000);
        let (alice_count, _) = t.multiplier("alice", 3_100);
        assert_eq!(alice_count, 2);
        // The count is cached for the stream.
        let (alice_again, _) = t.multiplier("alice", 9_000);
        assert_eq!(alice_again, 2);
        t.note_attendance("alice", 3_500);
        t.close_stream(4_000);
        assert_eq!(t.streaks.get("alice").unwrap().1, 2);
        // Bob was absent, so his stored streak is untouched by stream 2.
        assert_eq!(t.streaks.get("bob").unwrap().1, 1);

        // Stream 3: bob comes back but skipped a stream -> resets to 1.
        t.open_stream(5_000);
        let (bob_count, _) = t.multiplier("bob", 5_100);
        assert_eq!(bob_count, 1);
    }

    #[test]
    fn activity_reflects_first_seen_and_stacks_with_streak() {
        let mut t = StreamTracker::new();
        t.open_stream(1_000);
        t.note_attendance("alice", 1_000);
        // Alice joined at the very start -> full presence, streak 1 -> 2.0.
        let (count, combined) = t.multiplier("alice", 11_000);
        assert_eq!(count, 1);
        approx(combined, 2.0);
    }

    #[test]
    fn multiplier_is_neutral_when_no_stream_is_open() {
        let mut t = StreamTracker::new();
        t.note_attendance("alice", 1_000);
        let (count, combined) = t.multiplier("alice", 2_000);
        assert_eq!(count, 0);
        approx(combined, 1.0);
    }
}
