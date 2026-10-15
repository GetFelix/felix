//! How long moves take, as this instance saw them.
//!
//! Timed from the steps this instance wrote, not from anything stored: the
//! assignment carries no timestamps. A move is observed only by the instance
//! that wrote its fence and cut-over, and its full duration only if that
//! instance staged it too.
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::MoveStep;
use crate::model::ShardKey;

/// Seconds from the first step of a move (stage, or the fence when the
/// destination was already caught up) to its cut-over.
pub const SHARD_MOVE_DURATION_SECONDS: &str = "felix_shard_move_duration_seconds";

/// Seconds from the fence to the cut-over: the window in which the shard is
/// not served.
pub const SHARD_MOVE_FENCE_SECONDS: &str = "felix_shard_move_fence_seconds";

/// Bucket bounds for [`SHARD_MOVE_DURATION_SECONDS`]. A move includes copying
/// the log, so the tail is long.
pub(crate) const MOVE_DURATION_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// Bucket bounds for [`SHARD_MOVE_FENCE_SECONDS`].
pub(crate) const MOVE_FENCE_BUCKETS: &[f64] =
    &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

/// A finished move's timings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MoveTimes {
    /// Absent when another instance staged the move.
    pub(super) total: Option<Duration>,
    pub(super) fenced: Duration,
}

struct InProgress {
    started: Option<Instant>,
    fenced: Option<Instant>,
    /// What this instance last wrote. Anything else at the next read means
    /// another writer touched the shard, and the timing is no longer ours.
    generation: u64,
}

/// Moves this instance has started, keyed by shard.
#[derive(Default)]
pub(super) struct MoveClock {
    moves: HashMap<ShardKey, InProgress>,
}

impl MoveClock {
    /// Drop every move whose shard is not at the generation this instance
    /// wrote. Also what keeps the map from growing: a deleted shard or one
    /// finished elsewhere is gone at the next pass.
    pub(super) fn forget_changed(&mut self, read: &HashMap<ShardKey, u64>) {
        self.moves
            .retain(|key, held| read.get(key) == Some(&held.generation));
    }

    /// Note a step this instance wrote at `generation`. `was_staged` is
    /// whether the shard already had a successor when the pass read it, which
    /// tells a fence that starts a move from one that continues it.
    ///
    /// Returns the timings when the step finishes a move this instance
    /// fenced.
    pub(super) fn written(
        &mut self,
        key: &ShardKey,
        step: &MoveStep,
        generation: u64,
        was_staged: bool,
        now: Instant,
    ) -> Option<MoveTimes> {
        match step {
            MoveStep::Stage { .. } => {
                self.moves.insert(
                    key.clone(),
                    InProgress {
                        started: Some(now),
                        fenced: None,
                        generation,
                    },
                );
                None
            }
            MoveStep::Fence => {
                let held = self.moves.entry(key.clone()).or_insert(InProgress {
                    // Staged by someone else: when is not known here.
                    started: (!was_staged).then_some(now),
                    fenced: None,
                    generation,
                });
                held.fenced = Some(now);
                held.generation = generation;
                None
            }
            MoveStep::CutOver { .. } => {
                let held = self.moves.remove(key)?;
                Some(MoveTimes {
                    total: held
                        .started
                        .map(|started| now.saturating_duration_since(started)),
                    fenced: now.saturating_duration_since(held.fenced?),
                })
            }
            MoveStep::Abandon { .. }
            | MoveStep::Halted { .. }
            | MoveStep::TimedOut { .. }
            | MoveStep::Stalled { .. }
            | MoveStep::Cancel { .. }
            | MoveStep::Retake { .. }
            | MoveStep::Discard { .. } => {
                self.moves.remove(key);
                None
            }
            // Replaces a follower; not a leadership move.
            MoveStep::Reseat { .. } | MoveStep::Restore { .. } | MoveStep::Seat { .. } => None,
        }
    }
}

pub(super) fn record(times: MoveTimes) {
    if let Some(total) = times.total {
        metrics::histogram!(SHARD_MOVE_DURATION_SECONDS).record(total.as_secs_f64());
    }
    metrics::histogram!(SHARD_MOVE_FENCE_SECONDS).record(times.fenced.as_secs_f64());
}
