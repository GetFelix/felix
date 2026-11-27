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

/// The group a leader reported from, which marks it formed: a re-form
/// starts a new group in a new epoch, and a new incarnation is a new group.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
pub struct RaftFormed {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The reform epoch the group runs. 0 until a re-form.
    pub epoch: u64,
}

/// The voters a leader of the group reported as committed, with the log id
/// it reported them at. Only a report not older than the kept voters
/// replaces them, so a late report cannot bring back older voters. From
/// metadata level 48 the floor does not decide which reports count, since
/// any broker can raise it; only a reporter the stored target vouches for,
/// or a kept voter still placed while no target is stored, does (#2909).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftVoters {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The reform epoch the group runs. 0 until a re-form.
    pub epoch: u64,
    /// The committed index the voters were reported at.
    pub index: u64,
    /// Copy id by node id, one per voter.
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
    ///
    /// `hold_formed`: also leave the target as it is while
    /// [`Self::holds_formed_target`] holds the new one (#2905).
    pub(crate) fn settle_raft_target(
        &mut self,
        hold_formed: bool,
    ) -> Result<bool, RaftTargetError> {
        let set = self.raft_set();
        self.raft_copy_ids.retain(|node, _| set.contains(node));
        let complete = set.iter().all(|node| self.raft_copy_ids.contains_key(node));
        if !complete
            || (hold_formed && self.holds_formed_target(&self.raft_copy_ids))
            || self
                .raft_target
                .as_ref()
                .is_some_and(|target| target.members == self.raft_copy_ids)
        {
            return Ok(false);
        }
        let last = self
            .raft_target
            .as_ref()
            .map_or(self.raft_target_counter, |target| target.counter);
        let counter = last
            .checked_add(1)
            .ok_or(RaftTargetError::CounterExhausted)?;
        self.raft_target = Some(RaftTarget {
            counter,
            members: self.raft_copy_ids.clone(),
        });
        self.raft_target_counter = 0;
        Ok(true)
    }

    /// Keep `existing`'s copy ids and target when this write names the same
    /// brokers, so a leader move leaves a membership change in flight and its
    /// version alone. A different set drops both: the target named the old
    /// set's members.
    ///
    /// `keep_counter`: a different set keeps the counter the dropped target
    /// reached. The group's membership carries that target's version, and a
    /// leader takes only a newer one, so a count started again at 1 would
    /// never be acted on.
    pub(crate) fn carry_raft_target(
        &mut self,
        existing: Option<&ShardAssignment>,
        keep_counter: bool,
    ) {
        self.raft_target_counter = 0;
        let Some(existing) = existing else {
            self.raft_copy_ids.clear();
            self.raft_target = None;
            return;
        };
        if existing.raft_set() == self.raft_set() {
            self.raft_copy_ids = existing.raft_copy_ids.clone();
            self.raft_target = existing.raft_target.clone();
            self.raft_target_counter = existing.raft_target_counter;
            return;
        }
        if keep_counter && existing.incarnation == self.incarnation {
            self.raft_target_counter = existing
                .raft_target
                .as_ref()
                .map_or(existing.raft_target_counter, |target| target.counter);
        }
        self.raft_copy_ids.clear();
        self.raft_target = None;
    }

    /// Whether this record's set must stay as it is: its group's first
    /// target is stored and no leader of the group has reported from this
    /// incarnation and reform epoch yet. A broker cut off from the control
    /// plane can still form the stored target, so a second set with its own
    /// target would let both form a group (#2794). Once a leader has
    /// reported, the group exists and moves to a new set by changing its
    /// membership. A re-form starts a new group, so an earlier epoch's mark
    /// releases nothing.
    pub fn holds_unformed_set(&self) -> bool {
        self.raft_target.is_some() && self.raft_formed != Some(self.formed_key())
    }

    /// Note that a leader of the group at `incarnation` and reform `epoch`
    /// reported, which releases the held set. A report from another epoch is
    /// an old group's leader, cut off before a re-form, and marks nothing.
    /// Returns whether this changed the record.
    pub(crate) fn mark_raft_formed(&mut self, incarnation: u64, epoch: u64) -> bool {
        let formed = RaftFormed { incarnation, epoch };
        if formed != self.formed_key() || self.raft_formed == Some(formed) {
            return false;
        }
        self.raft_formed = Some(formed);
        true
    }

    /// Whether `node_id` may report the group's voters (#2909): a member of
    /// the stored target under the copy it reports, or, while a set change
    /// has cleared the target, a broker still in the set that the voters
    /// already kept name under that copy. A broker no longer placed never
    /// may, so a removed broker cannot name itself back into the voters a
    /// blank copy admits.
    pub(crate) fn may_report_voters(&self, node_id: &str, voters: &BTreeMap<String, u64>) -> bool {
        let Some(copy) = voters.get(node_id) else {
            return false;
        };
        let key = self.formed_key();
        match &self.raft_target {
            Some(target) => target.members.get(node_id) == Some(copy),
            None => {
                self.raft_set().contains(node_id)
                    && self.raft_copy_ids.get(node_id).is_none_or(|id| id == copy)
                    && self.raft_voters.as_ref().is_some_and(|held| {
                        held.incarnation == key.incarnation
                            && held.epoch == key.epoch
                            && held.members.get(node_id) == Some(copy)
                    })
            }
        }
    }

    /// Note the voters a leader of the group reported at `index`, when the
    /// report is for this record's incarnation and epoch and is not older
    /// than the voters already kept.
    ///
    /// `by_floor`: also refuse one older than the floor. Below level 48
    /// only; any broker raises the floor, so it must not decide whose
    /// voters count.
    /// Returns whether this changed the record.
    pub(crate) fn note_raft_voters(
        &mut self,
        report: &super::RaftFloorReport,
        voters: &BTreeMap<String, u64>,
        by_floor: bool,
    ) -> bool {
        let key = self.formed_key();
        if report.incarnation != key.incarnation || report.epoch != key.epoch {
            return false;
        }
        let older = |index: u64| index > report.index;
        let below_floor = by_floor
            && self.raft_floor.as_ref().is_some_and(|floor| {
                floor.incarnation == key.incarnation
                    && floor.epoch == key.epoch
                    && older(floor.index)
            });
        let below_held = self.raft_voters.as_ref().is_some_and(|held| {
            held.incarnation == key.incarnation && held.epoch == key.epoch && older(held.index)
        });
        if below_floor || below_held {
            return false;
        }
        let next = RaftVoters {
            incarnation: key.incarnation,
            epoch: key.epoch,
            index: report.index,
            members: voters.clone(),
        };
        if self.raft_voters.as_ref() == Some(&next) {
            return false;
        }
        self.raft_voters = Some(next);
        true
    }

    /// Whether this record's group has formed and a set of `next` brokers
    /// would let the brokers new to it elect a leader among themselves: no
    /// majority of `next` voting in the group as its leader last reported
    /// it. Brokers the group never reached hold empty copies and could form
    /// a second group for the shard (#2905), so the set changes only by
    /// steps the group's reported voters keep a majority of. A broker counts
    /// only if it is in the current set under the copy the report names: a
    /// broker that reopened empty has a new copy id and votes for nothing.
    /// No report at this incarnation and epoch holds every change.
    pub(crate) fn holds_formed_set(&self, next: &BTreeSet<String>) -> bool {
        *next != self.raft_set()
            && self.formed_voters().is_some_and(|voters| match voters {
                None => true,
                Some(voters) => !keeps_majority(voters, &self.raft_copy_ids, next),
            })
    }

    /// Whether a formed group's target naming `copies` is held: the voters
    /// its leader last reported hold fewer than half of those copies. A
    /// target only names copies that opened, so every target stored for a
    /// formed group names at least one copy that is not empty, and no member
    /// of it can form a second group. No report holds nothing here, since the
    /// set is the current one and [`Self::holds_formed_set`] already held
    /// the move to it.
    pub(crate) fn holds_formed_target(&self, copies: &BTreeMap<String, u64>) -> bool {
        let set = copies.keys().cloned().collect();
        matches!(self.formed_voters(), Some(Some(voters)) if !keeps_majority(voters, copies, &set))
    }

    /// Whether the voters a leader of this record's formed group last
    /// reported, at its incarnation and epoch, include `node` under the copy
    /// id it reported. No report is no evidence, so it is `false`.
    pub(crate) fn reports_voting(&self, node: &str) -> bool {
        matches!(self.formed_voters(), Some(Some(voters))
            if self.raft_copy_ids.get(node).is_some_and(|copy| voters.members.get(node) == Some(copy)))
    }

    /// Whether the voters a leader of this record's formed group last
    /// reported leave `node` possible as a voter: they include it, or there
    /// is no such report to rule it out.
    pub(crate) fn may_be_voting(&self, node: &str) -> bool {
        !matches!(self.formed_voters(), Some(Some(_))) || self.reports_voting(node)
    }

    /// Whether a leader of this record's formed group reported voters at its
    /// incarnation and epoch, and they do not name `node` under any copy.
    /// Stricter than `!reports_voting`, which also holds for a voter whose
    /// copy id this record has not heard since the set last changed.
    pub(crate) fn reported_without(&self, node: &str) -> bool {
        matches!(self.formed_voters(), Some(Some(voters)) if !voters.members.contains_key(node))
    }

    /// `None` when this record's group has not formed; otherwise the voters
    /// its leader reported at this incarnation and epoch, if any.
    fn formed_voters(&self) -> Option<Option<&RaftVoters>> {
        let key = self.formed_key();
        (self.raft_formed == Some(key)).then(|| {
            self.raft_voters
                .as_ref()
                .filter(|voters| voters.incarnation == key.incarnation && voters.epoch == key.epoch)
        })
    }

    /// The mark a leader of this record's group would set.
    fn formed_key(&self) -> RaftFormed {
        RaftFormed {
            incarnation: self.incarnation,
            epoch: self.placed.as_ref().map_or(0, |placed| placed.epoch),
        }
    }

    /// Keep `previous`'s formed mark when this write is at the same stream
    /// incarnation. A new incarnation is a new group, not yet formed; a mark
    /// from an earlier epoch is kept but holds nothing.
    pub(crate) fn carry_raft_formed(&mut self, previous: Option<&ShardAssignment>) {
        self.raft_formed = previous
            .and_then(|previous| previous.raft_formed)
            .filter(|formed| formed.incarnation == self.incarnation);
        self.raft_voters = previous
            .and_then(|previous| previous.raft_voters.clone())
            .filter(|voters| voters.incarnation == self.incarnation);
    }

    /// Whether `next` would put this record's held set
    /// ([`Self::holds_unformed_set`]) on other brokers. A write at another
    /// incarnation is a new group and holds nothing.
    pub(crate) fn moves_unformed_set(&self, next: &ShardAssignment, incarnation: u64) -> bool {
        self.incarnation == incarnation && self.holds_set_from(&next.leader, &next.replicas)
    }

    /// Whether `next` changes this record's formed group's set by a step
    /// [`Self::holds_formed_set`] refuses. A write at another incarnation is
    /// a new group and holds nothing.
    pub(crate) fn moves_formed_set(&self, next: &ShardAssignment, incarnation: u64) -> bool {
        self.incarnation == incarnation && self.holds_formed_set(&next.raft_set())
    }

    /// [`Self::holds_set_from`] for a formed group: see
    /// [`Self::holds_formed_set`].
    pub(crate) fn holds_formed_set_from(&self, leader: &str, replicas: &[String]) -> bool {
        self.holds_formed_set(
            &std::iter::once(leader)
                .chain(replicas.iter().map(String::as_str))
                .map(str::to_string)
                .collect(),
        )
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

    pub(crate) fn raft_set(&self) -> BTreeSet<String> {
        std::iter::once(&self.leader)
            .chain(&self.replicas)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests;

/// Whether `voters` name at least half of `set` under the copy id `copies`
/// gives each.
fn keeps_majority(
    voters: &RaftVoters,
    copies: &BTreeMap<String, u64>,
    set: &BTreeSet<String>,
) -> bool {
    let kept = set
        .iter()
        .filter(|node| {
            copies
                .get(*node)
                .is_some_and(|copy| voters.members.get(*node) == Some(copy))
        })
        .count();
    kept >= set.len().div_ceil(2)
}
