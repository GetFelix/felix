//! Whether a restore's new copy is still moving, as this instance has watched
//! it across passes.
//!
//! A report is one instant, and one that leaves the copy out (a batch that did
//! not land) says nothing about whether it will move again. Only a position
//! seen over several passes does, and the store does not keep one: writing it
//! there would start a new generation every pass. So each instance remembers
//! what it saw. One without the history, after a restart or a lease change,
//! starts the window again, which delays giving up but never gives up a copy
//! that is moving.
use std::collections::HashMap;

use super::CaughtUp;
use crate::model::{MoveReason, ShardAssignment, ShardKey, ShardState};

/// Restores in flight, by shard, new copy and generation: the copy's last
/// reported position, and when this instance saw it change.
#[derive(Debug, Default)]
pub(super) struct CopyProgress {
    seen: HashMap<(ShardKey, String, u64), Seen>,
    /// Fenced shards, by shard and generation: when this instance first saw
    /// the fence. The store keeps no fence time, and the move's start time
    /// counts the copy before it.
    fences: HashMap<(ShardKey, u64), u64>,
    /// Moves given up at the fence in a row, by shard: the leader they left
    /// in place, how many, and when the last was. Forgotten once the shard
    /// has another leader, which a cut-over gives it.
    give_ups: HashMap<ShardKey, GiveUps>,
}

#[derive(Debug, Clone)]
struct GiveUps {
    leader: String,
    count: u32,
    at_millis: u64,
}

#[derive(Debug, Clone, Copy)]
struct Seen {
    offset: Option<u64>,
    /// A cache's counter log, which may be all a counters-only cache copies.
    counters: Option<u64>,
    at_millis: u64,
}

impl CopyProgress {
    /// Note where every restore's new copy is in `caught_up`, and answer when
    /// each was last seen to move. Restores no longer in `existing` are
    /// forgotten.
    pub(super) fn observe(
        &mut self,
        existing: &[ShardAssignment],
        caught_up: &dyn CaughtUp,
    ) -> HashMap<(ShardKey, String), u64> {
        let Some(now) = caught_up.as_of_millis() else {
            return HashMap::new();
        };
        let mut seen = HashMap::new();
        for assignment in existing {
            let Some(joining) = assignment.joining.as_deref() else {
                continue;
            };
            if assignment.move_reason != Some(MoveReason::Restore) {
                continue;
            }
            // Offsets are only comparable within one leader's generation.
            let current =
                caught_up.reported_generation(&assignment.key) == Some(assignment.generation);
            let offset = current
                .then(|| caught_up.reported_offset(&assignment.key, joining))
                .flatten();
            let counters = current
                .then(|| caught_up.reported_counter_offset(&assignment.key, joining))
                .flatten();
            let id = (
                assignment.key.clone(),
                joining.to_string(),
                assignment.generation,
            );
            let entry = match self.seen.remove(&id) {
                // Left out of the report, or no further on in either log: not
                // moving.
                Some(last)
                    if !further(offset, last.offset) && !further(counters, last.counters) =>
                {
                    last
                }
                // Kept at the furthest seen, so a log left out of this report
                // does not count as moving when it reappears where it was.
                last => Seen {
                    offset: offset.max(last.and_then(|last| last.offset)),
                    counters: counters.max(last.and_then(|last| last.counters)),
                    at_millis: now,
                },
            };
            seen.insert(id, entry);
        }
        self.seen = seen;
        self.seen
            .iter()
            .map(|((key, joining, _), seen)| ((key.clone(), joining.clone()), seen.at_millis))
            .collect()
    }
}

impl CopyProgress {
    /// Note every fenced shard in `existing`, and answer when this instance
    /// first saw each fenced at its generation. Fences no longer in
    /// `existing` are forgotten.
    pub(super) fn observe_fences(
        &mut self,
        existing: &[ShardAssignment],
        caught_up: &dyn CaughtUp,
    ) -> HashMap<(ShardKey, u64), u64> {
        let Some(now) = caught_up.as_of_millis() else {
            return HashMap::new();
        };
        self.fences = existing
            .iter()
            .filter(|assignment| assignment.state == ShardState::Draining)
            .map(|assignment| {
                let id = (assignment.key.clone(), assignment.generation);
                let since = self.fences.get(&id).copied().unwrap_or(now);
                (id, since)
            })
            .collect();
        self.fences.clone()
    }

    /// Count the moves `plan` gives up at the fence, as of `now`. Counted
    /// from the plan rather than the write: one that loses its write to
    /// another instance waits all the same, which only delays a move.
    pub(super) fn note_give_ups(&mut self, plan: &super::Plan, now: Option<u64>) {
        let Some(now) = now else {
            return;
        };
        for (key, step, assignment) in plan.moves() {
            if let super::MoveStep::FenceTimedOut { .. } = step {
                let entry = self.give_ups.entry(key.clone()).or_insert(GiveUps {
                    leader: assignment.leader.clone(),
                    count: 0,
                    at_millis: now,
                });
                entry.count = entry.count.saturating_add(1);
                entry.at_millis = now;
                entry.leader = assignment.leader.clone();
                // A draining leader keeps its shard on the move path, so a
                // drain that cannot finish repeats this until someone looks.
                if entry.count > 1 {
                    tracing::warn!(
                        kind = %key.kind,
                        name = %key.stream,
                        shard = key.shard,
                        leader = %entry.leader,
                        given_up = entry.count,
                        "this shard's moves keep failing at the fence; each retry waits longer",
                    );
                }
            }
        }
    }

    /// The give-ups still standing against `existing`: a shard led by
    /// someone else since, or gone, starts afresh.
    pub(super) fn give_ups(
        &mut self,
        existing: &[ShardAssignment],
    ) -> HashMap<ShardKey, (u32, u64)> {
        let leaders: HashMap<&ShardKey, &str> = existing
            .iter()
            .map(|assignment| (&assignment.key, assignment.leader.as_str()))
            .collect();
        self.give_ups
            .retain(|key, given| leaders.get(key) == Some(&given.leader.as_str()));
        self.give_ups
            .iter()
            .map(|(key, given)| (key.clone(), (given.count, given.at_millis)))
            .collect()
    }
}

/// Whether a reported position is past the last one seen. A position left out
/// of the report is not.
fn further(now: Option<u64>, last: Option<u64>) -> bool {
    now.is_some() && now > last
}
