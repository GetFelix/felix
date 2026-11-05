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
use std::collections::{HashMap, HashSet};

use super::moves::{MAX_STUCK_PER_FLEET, STUCK_RENAME_WINDOW_MILLIS};
use super::{CaughtUp, StuckLeader, StuckReason};
use crate::model::{MoveReason, ReplicaReport, ShardAssignment, ShardKey, ShardState};

/// Restores in flight, by shard, new copy and generation: the copy's last
/// reported position, and when this instance saw it change.
#[derive(Debug, Default)]
pub(super) struct CopyProgress {
    seen: HashMap<(ShardKey, String, u64), Seen>,
    /// Fenced shards, by shard and generation: when this instance first saw
    /// the fence. The store keeps no fence time, and the move's start time
    /// counts the copy before it.
    fences: HashMap<(ShardKey, u64), u64>,
    /// Pending replica sets, by shard and the generation that wrote them:
    /// when this instance first saw each.
    pendings: HashMap<(ShardKey, u64), u64>,
    /// Fresh suspicions, by follower and the leader it suspects: when this
    /// instance first saw each.
    suspicions: HashMap<(String, String), u64>,
    /// Moves given up at the fence in a row, by shard: the leader they left
    /// in place, how many, and when the last was. Forgotten once the shard
    /// has another leader, which a cut-over gives it.
    give_ups: HashMap<ShardKey, GiveUps>,
    /// Leaders on live brokers, by shard: what this instance last saw of
    /// each at its generation, and when it last saw progress.
    watched: HashMap<ShardKey, Watch>,
    /// When this instance named each shard again for being stuck, within
    /// the window.
    stuck_renames: HashMap<ShardKey, Vec<u64>>,
}

/// One leader at one generation and incarnation, as watched for progress.
#[derive(Debug, Clone)]
struct Watch {
    generation: u64,
    leader: String,
    incarnation: u64,
    since_millis: u64,
    /// The pass that last looked at it.
    observed_at: u64,
    /// The last report at the generation, by its store time.
    reported_at: Option<u64>,
    /// The position a majority of the set held in that report.
    held: Option<u64>,
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

    /// Note every fresh suspicion in `positions`, and answer since when this
    /// instance has seen each without a break. One that lapsed is forgotten,
    /// so it starts again when it is next held.
    pub(super) fn observe_suspicions(
        &mut self,
        positions: &super::ReplicaPositions,
    ) -> HashMap<(String, String), u64> {
        let Some(now) = positions.as_of_millis() else {
            return HashMap::new();
        };
        self.suspicions = positions
            .suspicions()
            .map(|(follower, leader)| {
                let id = (follower.to_string(), leader.to_string());
                let since = self.suspicions.get(&id).copied().unwrap_or(now);
                (id, since)
            })
            .collect();
        self.suspicions.clone()
    }

    /// Note every pending set in `existing`, and answer when this instance
    /// first saw each. Sets no longer pending are forgotten.
    pub(super) fn observe_pending(
        &mut self,
        existing: &[ShardAssignment],
        caught_up: &dyn CaughtUp,
    ) -> HashMap<(ShardKey, u64), u64> {
        let Some(now) = caught_up.as_of_millis() else {
            return HashMap::new();
        };
        self.pendings = existing
            .iter()
            .filter_map(|assignment| {
                let pending = assignment.pending_set.as_ref()?;
                let id = (assignment.key.clone(), pending.generation);
                let since = self.pendings.get(&id).copied().unwrap_or(now);
                Some((id, since))
            })
            .collect();
        self.pendings.clone()
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

impl CopyProgress {
    /// Watch the leader of every fenced shard in `existing` whose broker is
    /// live, and answer those that have shown no progress at their
    /// generation since this pass. Progress is the leader's first report at
    /// the generation, a report with nothing past what a majority holds, or
    /// a majority's position moving. Without a `bound` nothing is watched.
    ///
    /// The clock starts again whenever the shard's leader, generation or the
    /// leader's incarnation changes, while its broker is down, a follower
    /// suspects it, or a move, fence or set change is under way: each of
    /// those has its own path, and a restarted leader gets a whole bound to
    /// open. A leader whose broker has reported on no shard within `bound` is
    /// not judged by its silence: that is the broker's stall, or the control
    /// plane's, and the same leader one generation up would stall with it.
    pub(super) fn observe_stuck(
        &mut self,
        existing: &[ShardAssignment],
        positions: &super::ReplicaPositions,
        bound: Option<u64>,
    ) -> HashMap<(ShardKey, u64), StuckLeader> {
        let (Some(now), Some(bound)) = (positions.as_of_millis(), bound) else {
            self.watched.clear();
            return HashMap::new();
        };
        self.stuck_renames.retain(|_, at| {
            at.retain(|at| now.saturating_sub(*at) < STUCK_RENAME_WINDOW_MILLIS);
            !at.is_empty()
        });
        let mut reporting: HashMap<&str, u64> = HashMap::new();
        for report in positions.all_reports() {
            if let Some(leader) = report.leader.as_deref() {
                let at = reporting.entry(leader).or_default();
                *at = (*at).max(report.reported_at_millis);
            }
        }
        let suspected: HashSet<(&str, &str)> = positions.suspicions().collect();
        let mut watched = HashMap::new();
        let mut stuck = HashMap::new();
        for assignment in existing {
            let leader = assignment.leader.as_str();
            if assignment.replicas.is_empty()
                || assignment.successor.is_some()
                || assignment.joining.is_some()
                || assignment.pending_set.is_some()
                || assignment.state == ShardState::Draining
                || assignment
                    .replicas
                    .iter()
                    .any(|follower| suspected.contains(&(follower.as_str(), leader)))
            {
                continue;
            }
            let Some(incarnation) = positions.up_incarnation(leader) else {
                continue;
            };
            let last = positions.last_report(&assignment.key);
            // A newer generation reported is an election placement has not
            // adopted yet; that has its own path.
            if last.is_some_and(|report| report.generation > assignment.generation) {
                continue;
            }
            let report = last.filter(|report| {
                report.generation == assignment.generation
                    && report.refused.is_none()
                    && report.leader.as_deref().is_none_or(|by| by == leader)
                    && report
                        .node_incarnations
                        .get(leader)
                        .is_none_or(|at| *at == incarnation)
            });
            // A watch this instance stopped looking at (it lost the lease,
            // or only runs woken passes) says nothing about the time between.
            let mut watch = match self.watched.remove(&assignment.key) {
                Some(watch)
                    if watch.generation == assignment.generation
                        && watch.leader == leader
                        && watch.incarnation == incarnation
                        && now.saturating_sub(watch.observed_at) <= bound / 2 =>
                {
                    watch
                }
                _ => Watch {
                    generation: assignment.generation,
                    leader: leader.to_string(),
                    incarnation,
                    since_millis: now,
                    observed_at: now,
                    reported_at: None,
                    held: None,
                },
            };
            if let Some(report) = report
                && watch
                    .reported_at
                    .is_none_or(|at| report.reported_at_millis > at)
            {
                let held = majority_held(assignment, report);
                let pending = report.leader_offset.is_some_and(|tail| tail > held);
                if !pending || watch.held.is_none_or(|last| held > last) {
                    watch.since_millis = now;
                }
                watch.reported_at = Some(report.reported_at_millis);
                watch.held = Some(held);
            }
            let reason = match watch.reported_at {
                None => StuckReason::NeverOpened,
                Some(at) if now.saturating_sub(at) > bound => StuckReason::StoppedReporting,
                Some(_) => StuckReason::NotCommitting,
            };
            let broker_reports = reporting
                .get(leader)
                .is_some_and(|at| now.saturating_sub(*at) <= bound);
            if reason != StuckReason::NotCommitting && !broker_reports {
                watch.since_millis = now;
            }
            watch.observed_at = now;
            if watch.since_millis < now {
                stuck.insert(
                    (assignment.key.clone(), assignment.generation),
                    StuckLeader {
                        since_millis: watch.since_millis,
                        reason,
                        recent_renames: self
                            .stuck_renames
                            .get(&assignment.key)
                            .map_or(0, |at| at.len() as u32),
                        fleet_stalled: false,
                    },
                );
            }
            watched.insert(assignment.key.clone(), watch);
        }
        self.watched = watched;
        // Many leaders stuck at once may be a fleet-wide fault, such as a
        // network that carries pings but not shipping, where renaming them
        // all would add a fence to every one and change nothing. It may as
        // well be one restarted broker that leads many shards (#1966), which
        // renames heal. So the pass renames one at most. Every entry is kept,
        // so that one is the first in shard order the planner can rename, not
        // one it would skip for its backoff, its cap or a set that cannot
        // serve. A shard renamed in vain waits out its doubled backoff, so
        // the others get their turns.
        let overdue = stuck
            .values()
            .filter(|stuck| now.saturating_sub(stuck.since_millis) > bound)
            .count();
        if overdue > MAX_STUCK_PER_FLEET.max(self.watched.len() / 8) {
            tracing::warn!(
                overdue,
                watched = self.watched.len(),
                "too many leaders show no progress at once; renaming one a pass",
            );
            for stuck in stuck.values_mut() {
                stuck.fleet_stalled = true;
            }
        }
        stuck
    }

    /// Count the shards `plan` names again for being stuck, as of `now`, and
    /// say why. Counted from the plan rather than the write, as give-ups are.
    pub(super) fn note_stuck_renames(&mut self, plan: &super::Plan, now: Option<u64>) {
        let Some(now) = now else {
            return;
        };
        for (key, step, assignment) in plan.moves() {
            if let super::MoveStep::Stuck { reason } = step {
                let at = self.stuck_renames.entry(key.clone()).or_default();
                at.push(now);
                tracing::warn!(
                    kind = %key.kind,
                    name = %key.stream,
                    shard = key.shard,
                    leader = %assignment.leader,
                    reason = reason.label(),
                    renamed_this_hour = at.len(),
                    "this shard's leader shows no progress at its generation on a live broker; \
                     planning to name it again one generation up",
                );
            }
        }
    }
}

/// The position a majority of `assignment`'s voters held in `report`: the
/// leader at its tail, each follower that is not a learner where the report
/// has it, or nowhere.
fn majority_held(assignment: &ShardAssignment, report: &ReplicaReport) -> u64 {
    let mut held: Vec<u64> = std::iter::once(report.leader_offset.unwrap_or(0))
        .chain(
            assignment
                .replicas
                .iter()
                .filter(|node| !assignment.learners.contains(*node))
                .map(|node| report.offsets.get(node).copied().unwrap_or(0)),
        )
        .collect();
    held.sort_unstable_by(|a, b| b.cmp(a));
    held[held.len() / 2]
}

/// Whether a reported position is past the last one seen. A position left out
/// of the report is not.
fn further(now: Option<u64>, last: Option<u64>) -> bool {
    now.is_some() && now > last
}
