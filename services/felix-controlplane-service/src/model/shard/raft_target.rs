//! The members a Raft shard's group should have, as the control plane
//! records them: each placed broker's copy id, and the target they make.
//!
//! `docs/per-shard-raft.md`, "Where a target comes from".
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::ShardAssignment;

/// The voters the control plane wants a Raft shard's group to have.
///
/// Its version is the placement incarnation followed by `counter`. The
/// incarnation is not stored here: a restore brings back the counter and
/// raises the incarnation, so the restored target is newer than any before.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftTarget {
    /// Rises by one with every change to `members`, starting at 1.
    pub counter: u64,
    /// Copy id by node id, one per broker in the shard's set.
    pub members: BTreeMap<String, u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftTargetError {
    #[error("the shard's Raft target counter is at its limit")]
    CounterExhausted,
}

impl ShardAssignment {
    /// Drop the copy ids of brokers no longer in the set, and work out the
    /// target the set and its copy ids now name. Returns whether the target
    /// changed, which is the only time the counter rises.
    ///
    /// No target is written until every broker in the set has reported a
    /// copy id: a target missing one would tell the group to remove it.
    pub(crate) fn settle_raft_target(&mut self) -> Result<bool, RaftTargetError> {
        let set = self.raft_set();
        self.raft_copy_ids.retain(|node, _| set.contains(node));
        let complete = set.iter().all(|node| self.raft_copy_ids.contains_key(node));
        if !complete
            || self
                .raft_target
                .as_ref()
                .is_some_and(|target| target.members == self.raft_copy_ids)
        {
            return Ok(false);
        }
        let counter = match &self.raft_target {
            None => 1,
            Some(target) => target
                .counter
                .checked_add(1)
                .ok_or(RaftTargetError::CounterExhausted)?,
        };
        self.raft_target = Some(RaftTarget {
            counter,
            members: self.raft_copy_ids.clone(),
        });
        Ok(true)
    }

    /// Keep `existing`'s copy ids and target when this write names the same
    /// brokers, so a leader move leaves a membership change in flight and its
    /// version alone. A different set drops both: the target named the old
    /// set's members.
    pub(crate) fn carry_raft_target(&mut self, existing: Option<&ShardAssignment>) {
        match existing.filter(|existing| existing.raft_set() == self.raft_set()) {
            Some(existing) => {
                self.raft_copy_ids = existing.raft_copy_ids.clone();
                self.raft_target = existing.raft_target.clone();
            }
            None => {
                self.raft_copy_ids.clear();
                self.raft_target = None;
            }
        }
    }

    /// Whether this record's set must stay as it is: its group's first
    /// target is stored and no leader of the group has reported from this
    /// incarnation yet. A broker cut off from the control plane can
    /// still form the stored target, so a second set with its own target
    /// would let both form a group (#2794). Once a leader has reported, the
    /// group exists and moves to a new set by changing its membership.
    pub(crate) fn holds_unformed_set(&self) -> bool {
        self.raft_target.is_some() && self.raft_formed != Some(self.incarnation)
    }

    /// Note that a leader of the group at `incarnation` and reform `epoch`
    /// reported, which releases the held set. A report from another epoch is
    /// an old group's leader, cut off before a re-form, and marks nothing.
    /// Returns whether this changed the record.
    pub(crate) fn mark_raft_formed(&mut self, incarnation: u64, epoch: u64) -> bool {
        let placed_epoch = self.placed.as_ref().map_or(0, |placed| placed.epoch);
        if incarnation != self.incarnation
            || epoch != placed_epoch
            || self.raft_formed == Some(incarnation)
        {
            return false;
        }
        self.raft_formed = Some(incarnation);
        true
    }

    /// Keep `previous`'s formed mark when this write is at the same stream
    /// incarnation. A new incarnation is a new group, not yet formed.
    pub(crate) fn carry_raft_formed(&mut self, previous: Option<&ShardAssignment>) {
        self.raft_formed = previous
            .and_then(|previous| previous.raft_formed)
            .filter(|formed| *formed == self.incarnation);
    }

    /// Whether `next` would put this record's held set
    /// ([`Self::holds_unformed_set`]) on other brokers. A write at another
    /// incarnation is a new group and holds nothing.
    pub(crate) fn moves_unformed_set(&self, next: &ShardAssignment, incarnation: u64) -> bool {
        self.incarnation == incarnation && self.holds_set_from(&next.leader, &next.replicas)
    }

    /// Whether this record's held set ([`Self::holds_unformed_set`]) differs
    /// from `leader` and `replicas`, so a write naming them is refused.
    pub(crate) fn holds_set_from(&self, leader: &str, replicas: &[String]) -> bool {
        self.holds_unformed_set()
            && self.raft_set()
                != std::iter::once(leader)
                    .chain(replicas.iter().map(String::as_str))
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>()
    }

    fn raft_set(&self) -> BTreeSet<String> {
        std::iter::once(&self.leader)
            .chain(&self.replicas)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests;
