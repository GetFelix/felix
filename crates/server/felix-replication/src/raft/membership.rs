//! Which membership step a leader takes next to make its group match the
//! control plane's target. See `docs/per-shard-raft.md`, "Membership". Each
//! step is an action of `docs/formal/FelixRaftPlacement.tla`, and each
//! refusal one of its guards.
//!
//! The decision reads only the leader's log and the newest target, so a new
//! leader, a restart or a retry resumes with nothing to reconcile.

// The driver that runs these steps against openraft is not built yet.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet};

use super::types::MemberId;

/// The version of a target: the control plane's restore incarnation, then a
/// counter that rises on every target write. Compared in that order, so a
/// target issued before a metadata restore is older than every one after.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TargetVersion {
    pub incarnation: u64,
    pub counter: u64,
}

/// The voters the control plane wants a group to have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub voters: BTreeSet<MemberId>,
    pub version: TargetVersion,
}

/// The latest membership in the leader's log, committed or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Config {
    /// The voters before a change in progress. Equal to `new` when none is.
    pub(crate) old: BTreeSet<MemberId>,
    pub(crate) new: BTreeSet<MemberId>,
    pub(crate) learners: BTreeSet<MemberId>,
    /// The version of the target this membership was made from.
    pub(crate) version: TargetVersion,
}

impl Config {
    fn joint(&self) -> bool {
        self.old != self.new
    }

    fn has(&self, member: &MemberId) -> bool {
        self.old.contains(member) || self.new.contains(member) || self.learners.contains(member)
    }
}

/// What a leader knows when it decides, read in one look.
#[derive(Debug, Clone)]
pub(crate) struct View {
    pub(crate) me: MemberId,
    pub(crate) config: Config,
    /// The entry holding `config` is committed.
    pub(crate) config_committed: bool,
    /// An entry of this leader's own term is committed. Two direct changes
    /// from one committed membership under different leaders can have
    /// quorums that do not meet, so no step is built before this.
    pub(crate) own_term_committed: bool,
    /// A transfer this leader started has not ended.
    pub(crate) transferring: bool,
    /// The highest restore incarnation this broker has acknowledged.
    pub(crate) acked_incarnation: u64,
    pub(crate) last_index: u64,
    pub(crate) commit_index: u64,
    /// The last index each other member is known to hold.
    pub(crate) matched: BTreeMap<MemberId, u64>,
    /// How far behind the last index a learner may be when it is promoted.
    pub(crate) lag_threshold: u64,
}

/// One membership step. The driver writes each with the membership the
/// decision read as its precondition, so a step decided on a membership
/// that has changed since writes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    /// Add a member the target names as a learner. It takes no vote.
    AddLearner(MemberId),
    /// Change the voters to `voters`. Voters that leave are dropped, never
    /// kept as learners, and `voters` always holds this leader.
    Change {
        voters: BTreeSet<MemberId>,
        version: TargetVersion,
    },
    /// Write the same voters at the target's newer version, with only the
    /// learners in `learners`.
    Stamp {
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    },
    /// Leave a committed joint membership for its new voters.
    LeaveJoint,
    /// Hand leadership to a caught-up voter the target keeps.
    Transfer(MemberId),
    Wait(Wait),
}

/// Why a leader takes no step now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wait {
    /// A transfer is in progress.
    Transferring,
    /// Nothing of this leader's term is committed yet.
    Barrier,
    /// The latest membership is not committed yet.
    Uncommitted,
    /// The control plane has sent no target.
    NoTarget,
    /// The target is older than the membership, or from before a restore
    /// this broker has seen.
    StaleTarget,
    /// A learner the target needs is still behind.
    CatchingUp,
    /// No voter that could take the leadership holds the whole log.
    NoSuccessor,
    /// The membership is the target.
    Settled,
}

/// The next step for the leader `view` describes, given the newest target
/// it has received.
pub(crate) fn next_step(view: &View, target: Option<&Target>) -> Step {
    let config = &view.config;
    if view.transferring {
        return Step::Wait(Wait::Transferring);
    }
    if !view.own_term_committed {
        return Step::Wait(Wait::Barrier);
    }
    if !view.config_committed {
        return Step::Wait(Wait::Uncommitted);
    }
    if config.joint() {
        // The joint's new set came from a real target, so finishing it comes
        // before any newer one. A leader the joint leaves out never writes
        // the entry that removes it: it would lead a group it is not in.
        if config.new.contains(&view.me) {
            return Step::LeaveJoint;
        }
        return successor(view, &config.new);
    }
    let Some(target) = target else {
        return Step::Wait(Wait::NoTarget);
    };
    // An incarnation only the log has seen is covered by the first test:
    // versions compare by incarnation first.
    if target.version < config.version || target.version.incarnation < view.acked_incarnation {
        return Step::Wait(Wait::StaleTarget);
    }
    if let Some(member) = target.voters.iter().find(|member| !config.has(member)) {
        return Step::AddLearner(member.clone());
    }
    if target.voters == config.new {
        if target.version > config.version {
            return Step::Stamp {
                learners: &config.learners & &target.voters,
                version: target.version,
            };
        }
        return Step::Wait(Wait::Settled);
    }
    // Liveness only: a change with a learner far behind stalls commits
    // until it catches up, since the new set's majority may need it.
    let behind = |member: &MemberId| {
        let matched = view.matched.get(member).copied().unwrap_or(0);
        matched < view.commit_index || view.last_index.saturating_sub(matched) > view.lag_threshold
    };
    if (&target.voters & &config.learners).iter().any(behind) {
        return Step::Wait(Wait::CatchingUp);
    }
    if target.voters.contains(&view.me) {
        return Step::Change {
            voters: target.voters.clone(),
            version: target.version,
        };
    }
    // The target leaves this leader out. It grows to the target plus itself
    // first, so a live majority of the target can finish the move, then
    // hands over. Only a membership whose voters are the target's carries
    // its version.
    let mut grown = target.voters.clone();
    grown.insert(view.me.clone());
    if grown != config.new {
        return Step::Change {
            voters: grown,
            version: config.version,
        };
    }
    successor(view, &target.voters)
}

/// A transfer to a member of `among` that holds the leader's whole log.
/// Callers pass voters of the latest membership only.
fn successor(view: &View, among: &BTreeSet<MemberId>) -> Step {
    among
        .iter()
        .filter(|member| **member != view.me)
        .find(|member| view.matched.get(*member).copied() == Some(view.last_index))
        .map_or(Step::Wait(Wait::NoSuccessor), |member| {
            Step::Transfer(member.clone())
        })
}
