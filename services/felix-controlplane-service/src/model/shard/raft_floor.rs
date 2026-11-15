//! The highest committed Raft log id a shard's leaders have reported, kept
//! per stream incarnation so a re-form can tell whether a group ever
//! committed anything.
//!
//! `docs/per-shard-raft.md`, "Offset floor".
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::{ShardAssignment, ShardKey};

/// A Raft shard's floor: the highest committed index a leader reported for
/// the group at one stream incarnation, across its reform epochs. It only
/// rises.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftFloor {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The shard's reform epoch the report was made in. 0 until a re-form.
    pub epoch: u64,
    /// The committed log index.
    pub index: u64,
    /// The term of the entry at `index`, or, while a new epoch has reported
    /// no index above the earlier epoch's, that epoch's last reported term.
    pub term: u64,
}

/// One leader's committed log id for one Raft group, as a broker reports it.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftFloorReport {
    pub key: ShardKey,
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The reform epoch the group runs. 0 until a re-form.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub epoch: u64,
    /// The leader's committed log index.
    pub index: u64,
    /// The term of the entry at `index`.
    pub term: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Why a floor report was not kept.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftFloorError {
    #[error("the shard has not switched to Raft")]
    NotSwitched,
    #[error("the report is for stream incarnation {reported}, the shard is at {current}")]
    OtherIncarnation { reported: u64, current: u64 },
    #[error("the report is for reform epoch {reported}, the shard is at {current}")]
    OtherEpoch { reported: u64, current: u64 },
    #[error(
        "index {index} at term {term} is above the floor's index {held_index} \
         but below its term {held_term}"
    )]
    LowerTerm {
        index: u64,
        term: u64,
        held_index: u64,
        held_term: u64,
    },
}

impl ShardAssignment {
    /// Raise the shard's floor to `report` if it is higher. Returns whether
    /// it moved; an equal or lower index in the same epoch is a no-op, and
    /// in a later epoch moves only the floor's epoch.
    ///
    /// Refused for a shard not on Raft, or a report for another incarnation
    /// or reform epoch: a late report from an old group must not set the new
    /// group's floor, and one from an epoch this record has not reached is
    /// unknown. A higher index at a lower term contradicts the held floor,
    /// since committed log ids only grow in both.
    pub(crate) fn raise_raft_floor(
        &mut self,
        report: &RaftFloorReport,
    ) -> Result<bool, RaftFloorError> {
        if !self.switched() {
            return Err(RaftFloorError::NotSwitched);
        }
        if report.incarnation != self.incarnation {
            return Err(RaftFloorError::OtherIncarnation {
                reported: report.incarnation,
                current: self.incarnation,
            });
        }
        let epoch = self.placed.as_ref().map_or(0, |placed| placed.epoch);
        if report.epoch != epoch {
            return Err(RaftFloorError::OtherEpoch {
                reported: report.epoch,
                current: epoch,
            });
        }
        let reported = RaftFloor {
            incarnation: report.incarnation,
            epoch: report.epoch,
            index: report.index,
            term: report.term,
        };
        match &self.raft_floor {
            Some(held)
                if (held.incarnation, held.epoch) == (reported.incarnation, reported.epoch) =>
            {
                if reported.index <= held.index {
                    return Ok(false);
                }
                if reported.term < held.term {
                    return Err(RaftFloorError::LowerTerm {
                        index: reported.index,
                        term: reported.term,
                        held_index: held.index,
                        held_term: held.term,
                    });
                }
            }
            // An earlier epoch of this group: offsets it gave out stay given
            // out, so the new epoch's first report never lowers the index.
            // It takes the new epoch's term, since the next same-epoch report
            // is compared against it and terms do not carry across epochs.
            Some(held)
                if held.incarnation == reported.incarnation && reported.index <= held.index =>
            {
                self.raft_floor = Some(RaftFloor {
                    index: held.index,
                    epoch: held.epoch.max(reported.epoch),
                    ..reported
                });
                return Ok(true);
            }
            // A floor from an earlier incarnation says nothing about this
            // group. One from a later one cannot be held here, since the
            // report matched the record.
            _ => {}
        }
        self.raft_floor = Some(reported);
        Ok(true)
    }

    /// Keep `previous`'s floor when this write is at the same stream
    /// incarnation: a placement write must never drop it. A new incarnation
    /// is a new group, which starts without one.
    pub(crate) fn carry_raft_floor(&mut self, previous: Option<&ShardAssignment>) {
        self.raft_floor = previous
            .and_then(|previous| previous.raft_floor.clone())
            .filter(|floor| floor.incarnation == self.incarnation);
    }
}

#[cfg(test)]
mod tests;
