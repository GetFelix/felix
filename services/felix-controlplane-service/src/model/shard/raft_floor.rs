//! What a shard's Raft leaders reported committing, kept per stream
//! incarnation: the highest committed index across reform epochs, and the
//! exact last committed log id of the current epoch.
//!
//! `docs/per-shard-raft.md`, "Offset floor".
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::{ShardAssignment, ShardKey};

/// A Raft shard's floor at one stream incarnation. Two things, never mixed:
/// `offset_floor`, the highest committed index any reform epoch of the group
/// reported, and `(epoch, index, term)`, the exact last committed log id a
/// leader of the current epoch reported. A re-form start reads
/// `offset_floor` as the bound below which no offset is given out again; a
/// rule that matches a copy's log against a log id reads the exact id.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftFloor {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The highest committed index reported in any reform epoch of this
    /// incarnation. It only rises.
    pub offset_floor: u64,
    /// The reform epoch of the exact log id below. 0 until a re-form.
    pub epoch: u64,
    /// The last committed index reported in `epoch`.
    pub index: u64,
    /// The term of the entry at `index`, in `epoch`.
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
    /// it moved; an equal or lower index in the same epoch is a no-op. The
    /// first report of a later epoch replaces the exact log id and keeps the
    /// higher `offset_floor`.
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
        let held = self
            .raft_floor
            .as_ref()
            .filter(|held| held.incarnation == report.incarnation);
        let offset_floor = match held {
            Some(held) if held.epoch == report.epoch => {
                if report.index <= held.index {
                    return Ok(false);
                }
                if report.term < held.term {
                    return Err(RaftFloorError::LowerTerm {
                        index: report.index,
                        term: report.term,
                        held_index: held.index,
                        held_term: held.term,
                    });
                }
                held.offset_floor.max(report.index)
            }
            // An earlier epoch of this group: its exact id says nothing
            // about the new epoch's log and is replaced, but the offsets it
            // gave out stay given out, so `offset_floor` never drops.
            Some(held) => held.offset_floor.max(report.index),
            // None yet, or one from an earlier incarnation, a different group.
            None => report.index,
        };
        self.raft_floor = Some(RaftFloor {
            incarnation: report.incarnation,
            offset_floor,
            epoch: report.epoch,
            index: report.index,
            term: report.term,
        });
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
