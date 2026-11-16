//! Every Raft target the control plane ordered for a shard's group in one
//! epoch, so a re-form can count each of them as a voter set.
//!
//! `docs/per-shard-raft.md`, "Targets ordered in an epoch".
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{RaftTarget, ShardAssignment};

/// The most distinct targets one epoch keeps. A target only moves when a
/// copy id or the set changes, so an epoch that orders more is a broker
/// flapping its disk; the history is then marked lost rather than grown.
pub(crate) const RAFT_ORDERED_CAP: usize = 64;

/// The targets ordered in one epoch of a Raft shard's group, in order.
///
/// Started empty when an epoch starts ([`ShardAssignment::start_raft_ordered`])
/// and appended in the same write that stores a new target, so a target is
/// on it before any broker can read the target and ask a leader for it.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftOrdered {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The group epoch (`PlacedInEpoch::epoch`) the list is for.
    pub epoch: u64,
    /// Each distinct target ordered in the epoch, first order first.
    pub targets: Vec<RaftTarget>,
    /// Some target of the epoch is not on the list: the cap was reached, or
    /// a target was written by an entry proposed below the level that keeps
    /// the list. A lost list is never read as complete.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lost: bool,
}

impl ShardAssignment {
    /// Start an empty list for the group's current epoch. Epoch 0 starts
    /// when the switch to Raft is recorded (`accept_seal_report`); a re-form
    /// that writes [`super::PlacedInEpoch::next_epoch`] must call this in the
    /// same write.
    pub(crate) fn start_raft_ordered(&mut self) {
        self.raft_ordered = Some(RaftOrdered {
            incarnation: self.incarnation,
            epoch: self.group_epoch(),
            targets: Vec::new(),
            lost: false,
        });
    }

    /// Add the target just written to the epoch's list. `kept` is false for
    /// an entry proposed below the level that keeps the list: the target is
    /// then not on it, so the list is marked lost.
    pub(crate) fn order_raft_target(&mut self, kept: bool) {
        let (incarnation, epoch) = (self.incarnation, self.group_epoch());
        let Some(target) = self.raft_target.clone() else {
            return;
        };
        let Some(ordered) = self
            .raft_ordered
            .as_mut()
            .filter(|ordered| ordered.incarnation == incarnation && ordered.epoch == epoch)
        else {
            return;
        };
        if ordered.lost
            || ordered
                .targets
                .iter()
                .any(|held| held.members == target.members)
        {
            return;
        }
        if !kept || ordered.targets.len() >= RAFT_ORDERED_CAP {
            ordered.lost = true;
            return;
        }
        ordered.targets.push(target);
    }

    /// Keep `previous`'s list when this write is at the same stream
    /// incarnation: a set change or a leader move stays in the epoch. A new
    /// incarnation is a new group, which starts its own list on its switch.
    pub(crate) fn carry_raft_ordered(&mut self, previous: Option<&ShardAssignment>) {
        self.raft_ordered = previous
            .and_then(|previous| previous.raft_ordered.clone())
            .filter(|ordered| ordered.incarnation == self.incarnation);
    }

    /// The voters, as (node id, copy id), of every target ordered in the
    /// group's current epoch, for the re-form's `GroupRecord::ordered`. A
    /// voter is a copy, not a broker: a broker whose disk was replaced runs
    /// a new copy that does not hold the old one's votes. `None` unless the list was
    /// started at this epoch's start and nothing of it was lost, so a shard
    /// that switched before the list's metadata level reads as unknown.
    pub fn ordered_voter_sets(&self) -> Option<Vec<BTreeSet<(String, u64)>>> {
        let ordered = self.raft_ordered.as_ref().filter(|ordered| {
            ordered.incarnation == self.incarnation
                && ordered.epoch == self.group_epoch()
                && !ordered.lost
        })?;
        Some(
            ordered
                .targets
                .iter()
                .map(|target| {
                    target
                        .members
                        .iter()
                        .map(|(node, copy)| (node.clone(), *copy))
                        .collect()
                })
                .collect(),
        )
    }

    /// Mark the list lost. A control-plane restore brings back a list as old
    /// as the backup, with the same incarnation and epoch, which targets
    /// ordered since would be missing from.
    pub(crate) fn lose_raft_ordered(&mut self) {
        if let Some(ordered) = &mut self.raft_ordered {
            ordered.lost = true;
        }
    }

    fn group_epoch(&self) -> u64 {
        self.placed.as_ref().map_or(0, |placed| placed.epoch)
    }
}

#[cfg(test)]
mod tests;
