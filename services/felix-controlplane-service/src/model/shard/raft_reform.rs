//! What the shard record holds about a Raft group's epoch, as the re-form
//! planner's [`GroupRecord`].
//!
//! `docs/per-shard-raft.md`, "The control plane's record for a re-form".
use std::collections::{BTreeMap, BTreeSet};

use felix_storage::raft_copy::{EpochBase, EpochLogId, GroupRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::ShardAssignment;

/// The voters a Raft shard's group started one epoch with.
///
/// At epoch 0 it is the epoch's first target: a broker forms the group only
/// on a target the control plane stored, and every later target is on
/// [`super::RaftOrdered`]. A re-form writes the next epoch's from its plan.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftSeed {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The group epoch (`PlacedInEpoch::epoch`) the voters started.
    pub epoch: u64,
    /// Copy id by node id, one per voter.
    pub members: BTreeMap<String, u64>,
    /// For an epoch a re-form started, each placed broker's part of the
    /// plan by node id. It is in the committed record, not only in the
    /// metadata leader's memory, so a broker finds its start after a
    /// leader change or restart. Empty at epoch 0.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schema(value_type = Object)]
    pub starts: BTreeMap<String, felix_storage::raft_copy::WireStart>,
}

/// How each re-form of a Raft shard's group started its epoch, oldest
/// first: the group is at epoch `bases.len()`.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftBases {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    pub bases: Vec<RaftEpochBase>,
}

/// One re-form's base, as a copy's `raft-epoch` file keeps it.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
pub struct RaftEpochBase {
    /// The base's last log index, epoch and term. `None` for an empty base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_last: Option<RaftLogId>,
    /// The epoch's first index. Offsets from the base's end up to it are
    /// never given out.
    pub first_index: u64,
}

/// A log id with its group epoch.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
pub struct RaftLogId {
    pub index: u64,
    pub epoch: u64,
    pub term: u64,
}

impl From<RaftEpochBase> for EpochBase {
    fn from(base: RaftEpochBase) -> Self {
        Self {
            base_last: base.base_last.map(|last| EpochLogId {
                index: last.index,
                epoch: last.epoch,
                term: last.term,
            }),
            first_index: base.first_index,
        }
    }
}

/// One group's re-form as `apply` writes it: the compare and swap that
/// starts the next epoch from a plan (`MetaCommand::StartReformedEpoch`).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ReformEpochStart {
    /// The stream or cache incarnation the plan was made for.
    pub incarnation: u64,
    /// The group epoch the plan was made at.
    pub expected_epoch: u64,
    /// The highest of the floor index and offset floor the plan read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_floor: Option<u64>,
    pub base: RaftEpochBase,
    /// Copy id by node id: the copies that hold the base.
    pub voters: BTreeMap<String, u64>,
    /// The brokers declared dead when the plan was made. The apply uses this
    /// set, not the one it reads, so every member writes the same record.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub dead: BTreeSet<String>,
    /// Each placed broker's part of the plan, written into the new epoch's
    /// [`RaftSeed::starts`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schema(value_type = Object)]
    pub starts: BTreeMap<String, felix_storage::raft_copy::WireStart>,
}

/// Why a re-form's next epoch was not written.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ReformWriteError {
    #[error("the plan is for incarnation {planned}, the shard is at {current}")]
    OtherIncarnation { planned: u64, current: u64 },
    #[error("the group is at epoch {current}, not {expected}")]
    EpochMoved { expected: u64, current: u64 },
    #[error(transparent)]
    Record(#[from] ReformRecordError),
    #[error("the shard has no record of the brokers placed in its epoch")]
    NotPlaced,
    #[error("the next epoch has no voter")]
    NoVoter,
    #[error("voter {0} is not placed in the next epoch")]
    VoterNotPlaced(String),
    #[error("the base is not above the current epoch's: {0}")]
    BaseBehind(&'static str),
    #[error("the floor rose to {stored} after the plan read {planned}")]
    FloorRaised { planned: u64, stored: u64 },
}

/// Why no [`GroupRecord`] can be built for a shard.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReformRecordError {
    /// The group re-formed before, and the shard record does not keep the
    /// bases its re-forms started from. Every offset below an earlier
    /// epoch's first index is then unknown, so no plan is made.
    #[error("the group is at epoch {0}, and the bases of its re-forms are not recorded")]
    BasesNotRecorded(u64),
}

impl ShardAssignment {
    /// The control plane's half of a re-form plan for this shard's group:
    /// its incarnation, the bases of its earlier re-forms, the floor and
    /// offset floor its leaders reported at that incarnation, every broker
    /// placed in the epoch, the voters the epoch started with, the targets
    /// ordered in it, and `dead`, the brokers declared dead.
    ///
    /// Whatever the record cannot vouch for is left unknown, which the
    /// planner reads as a block (`placed`) or a mark (`seed`, `ordered`),
    /// never as empty. A seed is unknown for a group whose first target was
    /// stored below metadata level 47. A floor or seed from another stream
    /// incarnation is a different group's and is dropped.
    ///
    /// The caller picks the shards: this does not check that the shard runs
    /// on Raft.
    pub fn reform_record(
        &self,
        dead: &BTreeSet<String>,
    ) -> Result<GroupRecord<String>, ReformRecordError> {
        let epoch = self.group_epoch();
        let bases = self.epoch_bases()?;
        let floor = self
            .raft_floor
            .as_ref()
            .filter(|floor| floor.incarnation == self.incarnation);
        Ok(GroupRecord {
            incarnation: self.incarnation,
            bases: bases.into_iter().map(EpochBase::from).collect(),
            floor: floor
                .filter(|floor| floor.epoch == epoch)
                .map(|floor| EpochLogId {
                    index: floor.index,
                    epoch: floor.epoch,
                    term: floor.term,
                }),
            offset_floor: floor.map(|floor| floor.offset_floor),
            placed: self.placed.as_ref().map(|placed| placed.brokers.clone()),
            dead: dead.clone(),
            seed: self.epoch_seed().map(|seed| {
                seed.members
                    .iter()
                    .map(|(node, copy)| (node.clone(), *copy))
                    .collect()
            }),
            ordered: self.ordered_voter_sets(),
            restore: None,
        })
    }

    /// The bases of every re-form up to the current epoch. None at epoch
    /// 0; past it, only a list at this incarnation with one base per epoch.
    fn epoch_bases(&self) -> Result<Vec<RaftEpochBase>, ReformRecordError> {
        let epoch = self.group_epoch();
        if epoch == 0 {
            return Ok(Vec::new());
        }
        self.raft_bases
            .as_ref()
            .filter(|held| {
                held.incarnation == self.incarnation
                    && u64::try_from(held.bases.len()).is_ok_and(|len| len == epoch)
            })
            .map(|held| held.bases.clone())
            .ok_or(ReformRecordError::BasesNotRecorded(epoch))
    }

    fn epoch_seed(&self) -> Option<&RaftSeed> {
        self.raft_seed.as_ref().filter(|seed| {
            seed.incarnation == self.incarnation
                && seed.epoch == self.group_epoch()
                && !seed.members.is_empty()
        })
    }

    /// Record the epoch's first target as its seed, in the write that
    /// stores it. Only the first target the epoch's ordered list holds
    /// counts: a list started late or lost could hide an earlier one the
    /// group formed on.
    pub(crate) fn seed_from_first_target(&mut self) {
        if self.epoch_seed().is_some() {
            return;
        }
        let (incarnation, epoch) = (self.incarnation, self.group_epoch());
        let first = self
            .raft_ordered
            .as_ref()
            .filter(|ordered| {
                ordered.incarnation == incarnation
                    && ordered.epoch == epoch
                    && !ordered.lost
                    && ordered.targets.len() == 1
            })
            .and_then(|ordered| ordered.targets.first())
            .filter(|first| self.raft_target.as_ref() == Some(*first));
        if let Some(first) = first {
            self.raft_seed = Some(RaftSeed {
                incarnation,
                epoch,
                members: first.members.clone(),
                starts: BTreeMap::new(),
            });
        }
    }

    /// Keep `previous`'s seed and bases when this write is at the same
    /// stream incarnation; whatever the caller passed is replaced.
    pub(crate) fn carry_raft_epochs(&mut self, previous: Option<&ShardAssignment>) {
        let incarnation = self.incarnation;
        self.raft_seed = previous
            .and_then(|previous| previous.raft_seed.clone())
            .filter(|seed| seed.incarnation == incarnation);
        self.raft_bases = previous
            .and_then(|previous| previous.raft_bases.clone())
            .filter(|bases| bases.incarnation == incarnation);
    }

    /// [`Self::start_reformed_epoch`] from a re-form's command. A plan from
    /// another stream or cache incarnation is refused: the new incarnation's
    /// group starts at epoch 0 too, so the epoch alone would not tell them
    /// apart.
    pub(crate) fn start_reform(
        &mut self,
        start: &ReformEpochStart,
    ) -> Result<(), ReformWriteError> {
        if start.incarnation != self.incarnation {
            return Err(ReformWriteError::OtherIncarnation {
                planned: start.incarnation,
                current: self.incarnation,
            });
        }
        self.start_reformed_epoch(
            start.expected_epoch,
            &start.dead,
            start.planned_floor,
            start.base,
            start.voters.clone(),
        )?;
        if let Some(seed) = &mut self.raft_seed {
            seed.starts = start.starts.clone();
        }
        Ok(())
    }

    /// Whether this record is the one `start` writes: the next epoch with
    /// its base and voters, for its incarnation.
    pub(crate) fn started_by(&self, start: &ReformEpochStart) -> bool {
        self.incarnation == start.incarnation
            && self.group_epoch() == start.expected_epoch + 1
            && self.raft_bases.as_ref().and_then(|held| held.bases.last()) == Some(&start.base)
            && self.raft_seed.as_ref().is_some_and(|seed| {
                seed.epoch == start.expected_epoch + 1 && seed.members == start.voters
            })
    }

    /// Start the next epoch of the group, as the re-form's epoch compare
    /// and swap writes it in one command: the next [`super::PlacedInEpoch`]
    /// record, `base` appended to the bases, `voters` as the new epoch's
    /// seed, and an empty ordered list.
    ///
    /// Refused unless the group is still at `expected`, every earlier base
    /// is recorded and the floor is no higher than `planned_floor`, the
    /// highest of the floor index and offset floor the plan read. A stale
    /// plan never writes over a newer epoch, a later re-form never starts
    /// without the bases before it, and a floor a leader reported after the
    /// plan, whose entries the plan may cut, refuses the start. `base` must
    /// end below its first index, which must be above the floor and every
    /// earlier epoch's, and an entry of a re-formed epoch it ends on must be
    /// at or past that epoch's first index.
    pub(crate) fn start_reformed_epoch(
        &mut self,
        expected: u64,
        dead: &BTreeSet<String>,
        planned_floor: Option<u64>,
        base: RaftEpochBase,
        voters: BTreeMap<String, u64>,
    ) -> Result<(), ReformWriteError> {
        let current = self.group_epoch();
        if current != expected {
            return Err(ReformWriteError::EpochMoved { expected, current });
        }
        let mut bases = self.epoch_bases()?;
        // The same bound the planner reads off `reform_record`.
        let record = self
            .raft_floor
            .as_ref()
            .filter(|floor| floor.incarnation == self.incarnation);
        let stored = record
            .filter(|floor| floor.epoch == current)
            .map(|floor| floor.index)
            .max(record.map(|floor| floor.offset_floor));
        if let Some(stored) = stored {
            let planned = planned_floor.unwrap_or(0);
            if planned_floor.is_none_or(|planned| stored > planned) {
                return Err(ReformWriteError::FloorRaised { planned, stored });
            }
            if base.first_index <= stored {
                return Err(ReformWriteError::BaseBehind(
                    "its first index is not above the floor",
                ));
            }
        }
        let placed = self
            .placed
            .as_ref()
            .ok_or(ReformWriteError::NotPlaced)?
            .next_epoch(self, dead);
        if voters.is_empty() {
            return Err(ReformWriteError::NoVoter);
        }
        if let Some(node) = voters.keys().find(|node| !placed.brokers.contains(*node)) {
            return Err(ReformWriteError::VoterNotPlaced(node.clone()));
        }
        if let Some(last) = base.base_last {
            if last.epoch > current {
                return Err(ReformWriteError::BaseBehind(
                    "its last entry is from a later epoch",
                ));
            }
            if last.index >= base.first_index {
                return Err(ReformWriteError::BaseBehind(
                    "it ends at or past its first index",
                ));
            }
            // An epoch's entries start at its first index; no copy holds
            // one below it.
            let first = last
                .epoch
                .checked_sub(1)
                .and_then(|e| usize::try_from(e).ok())
                .and_then(|e| bases.get(e));
            if first.is_some_and(|epoch| last.index < epoch.first_index) {
                return Err(ReformWriteError::BaseBehind(
                    "its last entry is below its epoch's first index",
                ));
            }
        }
        if bases
            .iter()
            .any(|earlier| earlier.first_index >= base.first_index)
        {
            return Err(ReformWriteError::BaseBehind(
                "its first index is not above an earlier epoch's",
            ));
        }
        bases.push(base);
        self.placed = Some(placed);
        self.raft_bases = Some(RaftBases {
            incarnation: self.incarnation,
            bases,
        });
        self.raft_seed = Some(RaftSeed {
            incarnation: self.incarnation,
            epoch: current + 1,
            members: voters,
            starts: BTreeMap::new(),
        });
        self.start_raft_ordered();
        Ok(())
    }
}

mod abandon;
pub use abandon::AbandonEpochStart;
pub(crate) use abandon::AbandonError;

#[cfg(test)]
mod tests;
