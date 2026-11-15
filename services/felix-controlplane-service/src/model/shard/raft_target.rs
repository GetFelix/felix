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
        let set: BTreeSet<String> = std::iter::once(&self.leader)
            .chain(&self.replicas)
            .cloned()
            .collect();
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
}

#[cfg(test)]
mod tests;
