//! What the shard record holds about a Raft group's epoch, as the re-form
//! planner's [`GroupRecord`].
//!
//! `docs/per-shard-raft.md`, "The control plane's record for a re-form".
use std::collections::BTreeSet;

use felix_storage::raft_copy::{EpochLogId, GroupRecord};
use thiserror::Error;

use super::ShardAssignment;

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
    /// its incarnation, the floor and offset floor its leaders reported at
    /// that incarnation, every broker placed in the epoch, the targets
    /// ordered in it, and `dead`, the brokers declared dead.
    ///
    /// Whatever the record cannot vouch for is left unknown, which the
    /// planner reads as a block (`placed`) or a mark (`seed`, `ordered`),
    /// never as empty. The seed is always unknown for now: the group's first
    /// voters are the copy ids its forming broker asked its peers for, and
    /// the control plane does not store them. A floor from another stream
    /// incarnation is a different group's and is dropped.
    ///
    /// The caller picks the shards: this does not check that the shard runs
    /// on Raft.
    pub fn reform_record(
        &self,
        dead: &BTreeSet<String>,
    ) -> Result<GroupRecord<String>, ReformRecordError> {
        let epoch = self.group_epoch();
        if epoch != 0 {
            return Err(ReformRecordError::BasesNotRecorded(epoch));
        }
        let floor = self
            .raft_floor
            .as_ref()
            .filter(|floor| floor.incarnation == self.incarnation);
        Ok(GroupRecord {
            incarnation: self.incarnation,
            bases: Vec::new(),
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
            seed: None,
            ordered: self.ordered_voter_sets(),
        })
    }
}

#[cfg(test)]
mod tests;
