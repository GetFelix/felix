//! A standalone broker's group of one re-forming itself: the aloneness
//! checks that run in the same apply as the epoch compare and swap.
//!
//! `docs/per-shard-raft.md`, "A standalone broker re-forms itself".
use std::collections::BTreeSet;

use thiserror::Error;

use super::{ReformEpochStart, ReformWriteError};
use crate::model::{ShardAssignment, ShardState};

/// Why a group's self re-form was not written.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum AloneReformError {
    /// The request or the shard's record names someone besides the caller,
    /// so another copy may hold records the caller's plan never read.
    #[error("the group is not alone on {node}: {reason}")]
    NotAlone { node: String, reason: &'static str },
    #[error(transparent)]
    Write(#[from] ReformWriteError),
}

/// What the apply reads outside the shard's own record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AloneFacts {
    /// The replication factor of the stream or cache the shard belongs to,
    /// when it exists at the shard's incarnation.
    pub(crate) replication_factor: Option<u32>,
    /// The caller has a declared-dead entry.
    pub(crate) declared_dead: bool,
}

/// Whether every node in `nodes` is `node`.
fn only<'a>(nodes: impl IntoIterator<Item = &'a String>, node: &str) -> bool {
    nodes.into_iter().all(|n| n == node)
}

impl ShardAssignment {
    /// Start the next epoch for a group of one, if `node` is alone in it.
    /// True when the epoch was written; false when `start` was already
    /// written and the record is left as it is, so a retry never bumps the
    /// epoch twice.
    ///
    /// The checks read the record and the request in the same apply as the
    /// epoch compare and swap. A check made before it could see a placement
    /// that grew in between. Nothing is changed on a refusal.
    pub(crate) fn start_alone(
        &mut self,
        node: &str,
        facts: AloneFacts,
        start: &ReformEpochStart,
    ) -> Result<bool, AloneReformError> {
        let not_alone = |reason| AloneReformError::NotAlone {
            node: node.to_string(),
            reason,
        };
        if start.voters.len() != 1 || !only(start.voters.keys(), node) {
            return Err(not_alone("the plan's voters are not this broker alone"));
        }
        if !only(start.starts.keys(), node) {
            return Err(not_alone("the plan starts a broker besides this one"));
        }
        if !start.dead.is_empty() {
            return Err(not_alone("the plan declares a broker dead"));
        }
        if start.incarnation != self.incarnation {
            return Err(ReformWriteError::OtherIncarnation {
                planned: start.incarnation,
                current: self.incarnation,
            }
            .into());
        }
        if self.started_by(start) {
            return Ok(false);
        }
        let current = self.group_epoch();
        if current != start.expected_epoch {
            return Err(ReformWriteError::EpochMoved {
                expected: start.expected_epoch,
                current,
            }
            .into());
        }
        if facts.replication_factor != Some(1) {
            return Err(not_alone("the replication factor is not 1"));
        }
        if facts.declared_dead {
            return Err(not_alone("the broker is declared dead"));
        }
        let set_changing = self.pending_set.is_some()
            || self.successor.is_some()
            || self.joining.is_some()
            || self.move_reason.is_some()
            || !self.learners.is_empty()
            || self.state == ShardState::Draining;
        if set_changing {
            return Err(not_alone("a set change or move is in flight"));
        }
        if self.leader != node || !self.replicas.is_empty() {
            return Err(not_alone("the shard's set is not this broker alone"));
        }
        let only_placed = self
            .placed
            .as_ref()
            .is_some_and(|placed| placed.brokers == BTreeSet::from([node.to_string()]));
        if !only_placed {
            return Err(not_alone(
                "a broker besides this one is placed in the epoch",
            ));
        }
        // An unknown seed or list is not vouched for, so it refuses.
        let seed_alone = self
            .epoch_seed()
            .is_some_and(|seed| seed.members.len() == 1 && only(seed.members.keys(), node));
        if !seed_alone {
            return Err(not_alone("the epoch's seed is not this broker alone"));
        }
        let ordered_alone = self.ordered_voter_sets().is_some_and(|sets| {
            sets.iter()
                .all(|set| only(set.iter().map(|(n, _)| n), node))
        });
        if !ordered_alone {
            return Err(not_alone(
                "a target ordered in the epoch names another broker",
            ));
        }
        if self
            .raft_target
            .as_ref()
            .is_some_and(|target| !only(target.members.keys(), node))
        {
            return Err(not_alone("the stored target names another broker"));
        }
        self.start_reform(start)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
