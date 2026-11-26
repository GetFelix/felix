//! Giving up the log of a Raft shard whose every placed broker is declared
//! dead, and starting it empty at the next epoch
//! (`MetaCommand::AbandonRaftEpoch`).
//!
//! `docs/per-shard-raft.md`, "Abandoning a lost group".
use std::collections::BTreeSet;

use felix_storage::raft_copy::{MIN_OFFSET_GAP, RestoreAt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::super::{PlacedInEpoch, ShardAssignment, ShardState};
use super::{RaftBases, RaftEpochBase};

/// An operator's abandon of a Raft shard's log, as the control plane writes
/// it: one compare and swap that places the shard afresh and starts the next
/// epoch empty, at a first index above every offset the lost group can have
/// given out.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct AbandonEpochStart {
    /// The stream or cache incarnation the abandon was asked at.
    pub incarnation: u64,
    /// The group epoch the abandon was asked at.
    pub expected_epoch: u64,
    /// The assignment generation the abandon was asked at.
    pub expected_generation: u64,
    /// The broker the next epoch starts on.
    pub leader: String,
    /// The control plane's wall clock when the abandon was asked, in Unix
    /// milliseconds. Fixed in the command so every member computes the same
    /// first index.
    pub planned_at_millis: u64,
    /// How far past the time line the next epoch starts; at least
    /// `MIN_OFFSET_GAP`.
    pub offset_gap: u64,
}

/// Why an abandon was not written.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum AbandonError {
    #[error("the abandon is for incarnation {planned}, the shard is at {current}")]
    OtherIncarnation { planned: u64, current: u64 },
    #[error("the group is at epoch {current}, not {expected}")]
    EpochMoved { expected: u64, current: u64 },
    #[error("the shard is at generation {current}, not {expected}")]
    GenerationMoved { expected: u64, current: u64 },
    #[error(transparent)]
    Record(#[from] super::ReformRecordError),
    #[error("the shard has no record of the brokers placed in its epoch")]
    NotPlaced,
    #[error("broker {0} is placed in the epoch and not declared dead")]
    Unaccounted(String),
    #[error("the new leader {0} is declared dead")]
    LeaderDead(String),
    #[error("an offset gap of {0} is below {MIN_OFFSET_GAP}")]
    ShortGap(u64),
    #[error("every offset is taken")]
    NoIndexLeft,
    #[error(
        "the restore time line at this control plane's clock ({line}) is not above index \
         {given}, which the group's offset floor or an earlier epoch already reached; check \
         the clock and ask again"
    )]
    LineBehind { line: u64, given: u64 },
}

impl ShardAssignment {
    /// The first index an abandon gives the next epoch: the restore time
    /// line plus the gap. Refused unless that is above the offset floor, the
    /// last committed index any leader reported and every earlier epoch's
    /// first index.
    ///
    /// The floor alone is not enough: a leader reports it at most once a
    /// second, and not at all while it cannot reach the control plane, so
    /// the lost copy may have acknowledged offsets above it. The time line
    /// grows faster than any shard gives out offsets, so it covers those
    /// (`docs/per-shard-raft.md`, "A restore from a backup point is a
    /// re-form"), but only while this clock is not behind the one that
    /// started an earlier epoch on the line. A floor or first index at or
    /// above the line says it is. Starting just above them would then reuse
    /// what the lost epoch acknowledged since its last report, and no gap
    /// bounds that: the leader may not have reported for as long as it ran.
    pub(crate) fn abandon_first_index(
        &self,
        start: &AbandonEpochStart,
    ) -> Result<u64, AbandonError> {
        if start.offset_gap < MIN_OFFSET_GAP {
            return Err(AbandonError::ShortGap(start.offset_gap));
        }
        let line = RestoreAt {
            end: 0,
            gap: start.offset_gap,
            planned_at_millis: start.planned_at_millis,
        }
        .first_index()
        .ok_or(AbandonError::NoIndexLeft)?;
        let floor = self
            .raft_floor
            .as_ref()
            .filter(|floor| floor.incarnation == self.incarnation)
            .map(|floor| floor.index.max(floor.offset_floor));
        let earlier = self
            .epoch_bases()?
            .iter()
            .map(|base| base.first_index)
            .max();
        match floor.max(earlier) {
            Some(given) if line <= given => Err(AbandonError::LineBehind { line, given }),
            _ => Ok(line),
        }
    }

    /// The record `start` writes, or why it is refused.
    ///
    /// Refused unless the shard is still at the incarnation, epoch and
    /// generation the abandon was asked at, and every broker placed in the
    /// epoch is in `dead`. The store passes its own dead set at the write,
    /// not the one the operator saw. A placed broker that is not declared dead
    /// may still hold acknowledged records and come back, so its word is
    /// needed and a re-form, not an abandon, is the way. The next epoch is
    /// placed on the new leader alone, its base is empty, and its first
    /// index ([`Self::abandon_first_index`]) is in `raft_bases` in the same
    /// write, before any broker can start the epoch.
    pub(crate) fn abandon_epoch(
        &self,
        start: &AbandonEpochStart,
        dead: &BTreeSet<String>,
    ) -> Result<ShardAssignment, AbandonError> {
        if start.incarnation != self.incarnation {
            return Err(AbandonError::OtherIncarnation {
                planned: start.incarnation,
                current: self.incarnation,
            });
        }
        let current = self.group_epoch();
        if current != start.expected_epoch {
            return Err(AbandonError::EpochMoved {
                expected: start.expected_epoch,
                current,
            });
        }
        if self.generation != start.expected_generation {
            return Err(AbandonError::GenerationMoved {
                expected: start.expected_generation,
                current: self.generation,
            });
        }
        let placed = self.placed.as_ref().ok_or(AbandonError::NotPlaced)?;
        if let Some(live) = placed
            .brokers
            .iter()
            .chain(self.placed_nodes())
            .find(|broker| !dead.contains(*broker))
        {
            return Err(AbandonError::Unaccounted(live.clone()));
        }
        if dead.contains(&start.leader) {
            return Err(AbandonError::LeaderDead(start.leader.clone()));
        }
        let first_index = self.abandon_first_index(start)?;
        let mut bases = self.epoch_bases()?;
        bases.push(RaftEpochBase {
            base_last: None,
            first_index,
        });

        let mut next = self.clone();
        next.leader = start.leader.clone();
        next.replicas = Vec::new();
        next.generation = self.generation + 1;
        next.state = ShardState::Assigning;
        next.successor = None;
        next.joining = None;
        next.move_started_at_millis = None;
        next.move_reason = None;
        next.fence_storage_ids = Default::default();
        next.member_storage_ids = Default::default();
        next.learners = Default::default();
        next.pending_set = None;
        next.previous_generation = None;
        next.readmitted = Default::default();
        next.placed = Some(PlacedInEpoch {
            epoch: current + 1,
            brokers: BTreeSet::from([start.leader.clone()]),
        });
        // Nothing of the lost group names a voter of the new one. The new
        // leader's first copy id report seeds the epoch, as at epoch 0.
        next.raft_copy_ids = Default::default();
        // The count goes on from the dropped target, as for a set change, so
        // no later target reads as older than one a copy already acted on.
        next.raft_target_counter = self
            .raft_target
            .as_ref()
            .map_or(self.raft_target_counter, |target| target.counter)
            .max(self.raft_target_counter);
        next.raft_target = None;
        next.raft_formed = None;
        next.raft_voters = None;
        next.raft_seed = None;
        next.raft_leader = None;
        next.raft_bases = Some(RaftBases {
            incarnation: self.incarnation,
            bases,
        });
        next.start_raft_ordered();
        Ok(next)
    }

    /// Whether this record is the one `start` wrote: so a replayed or
    /// repeated abandon answers it instead of starting a second epoch.
    pub(crate) fn abandoned_by(&self, start: &AbandonEpochStart) -> bool {
        self.incarnation == start.incarnation
            && self.group_epoch() == start.expected_epoch + 1
            && self.generation == start.expected_generation + 1
            && self.leader == start.leader
            && self
                .raft_bases
                .as_ref()
                .and_then(|held| held.bases.last())
                .is_some_and(|base| base.base_last.is_none())
    }
}

#[cfg(test)]
mod tests;
