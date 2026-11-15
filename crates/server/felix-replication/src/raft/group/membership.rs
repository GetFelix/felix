//! Membership steps as a group writes them. Each is written only on the
//! membership and the term it was decided on, so a step decided on a log
//! that has moved writes nothing. See `docs/per-shard-raft.md`,
//! "Membership".

use std::collections::{BTreeMap, BTreeSet};

use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::raft::Precondition;
use openraft::type_config::alias::LogIdOf;
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, ServerState};

use super::Group;
use crate::raft::membership::TargetVersion;
use crate::raft::types::{MemberId, Membership, MembershipEdit, ShardTypes};

/// How often [`Group::transfer_in_caught_up`] looks at the target again.
const CAUGHT_UP_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// What a leader's log held when a step was decided.
#[derive(Debug, Clone)]
pub(crate) struct Basis {
    /// The term this member leads.
    pub(crate) term: u64,
    /// The entry that holds `membership`.
    pub(crate) at: Option<LogIdOf<ShardTypes>>,
    /// The latest membership in the log.
    pub(crate) membership: Membership,
    /// Whether the entry at `at` is committed.
    pub(crate) committed: bool,
    /// Whether an entry of `term` is committed.
    pub(crate) own_term_committed: bool,
    pub(crate) commit_index: u64,
}

/// How far each other member's log is known to reach, with the leader's
/// own last index from the same look.
#[derive(Debug, Clone, Default)]
pub(crate) struct Replication {
    pub(crate) last_index: u64,
    pub(crate) matched: BTreeMap<MemberId, u64>,
}

impl Basis {
    fn preconditions(&self) -> [Precondition<ShardTypes>; 2] {
        [
            Precondition::CommittedLeaderId {
                committed_leader_id: self.term.into(),
            },
            Precondition::LastMembershipLogId {
                last_membership_log_id: self.at,
            },
        ]
    }
}

/// Why a step was not written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepError {
    /// The log's latest membership, or its leader, is not the one the step
    /// was decided on. Decide again. A voter change that stopped here may
    /// have committed its joint membership.
    Moved,
    /// This member does not lead, or lost the term before the step
    /// committed.
    NotLeader,
    /// The step would write a membership without this leader as a voter.
    WithoutLeader,
    /// The membership is joint. Only leaving it is written on a joint: any
    /// other edit would flatten it, and could drop this leader on the way.
    Joint,
    /// openraft cannot take a change yet: one is in progress, or nothing
    /// of this term is committed. The same step can go through later.
    Busy(String),
    /// openraft refused the membership itself. Asking again changes
    /// nothing.
    Refused(String),
    /// The membership is not joint, so there is nothing to leave.
    NotJoint,
    /// The step would write a version below the one the membership has.
    OlderVersion,
    /// The group's core has stopped.
    Closed,
}

impl Group {
    /// The latest membership in this leader's log and the term it leads,
    /// read in one look. `None` on a member that does not lead.
    pub(crate) async fn membership_basis(&self) -> Option<Basis> {
        let read = self.raft.with_raft_state(|state| {
            let vote = state.vote_ref();
            let leads = state.server_state == ServerState::Leader && vote.is_committed();
            leads.then(|| {
                let latest = state.membership_state.effective();
                let at = *latest.log_id();
                let term = vote.leader_id().term();
                let committed = state.local_committed();
                Basis {
                    term,
                    at,
                    membership: latest.membership().clone(),
                    // What openraft itself asks before it takes a change.
                    committed: state.membership_state.committed().log_id() == &at,
                    own_term_committed: committed
                        .is_some_and(|at| **at.committed_leader_id() == term),
                    commit_index: committed.map_or(0, |at| at.index),
                }
            })
        });
        read.await.ok().flatten()
    }

    /// What this leader knows of the other members' logs.
    pub(crate) fn replication(&self) -> Replication {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let matched = metrics.replication.iter().flatten();
        Replication {
            last_index: metrics.last_log_index.unwrap_or(0),
            matched: matched
                .filter_map(|(member, at)| Some((member.clone(), at.as_ref()?.index)))
                .collect(),
        }
    }

    /// Hand the leadership of `term` to `to`. Nothing is asked if this
    /// member no longer leads that term: the successor was picked from
    /// that term's log.
    pub(crate) async fn transfer_in(&self, term: u64, to: MemberId) -> Option<u64> {
        // The term is checked in the read that gives up the lease, so a
        // later term this member won meanwhile is never closed.
        let term = self.transfer_starting_in(Some(term)).await?;
        let _ = self.raft.trigger().transfer_leader(to).await;
        Some(term)
    }

    /// [`Self::transfer_in`], with the request held until `to` holds this
    /// leader's whole log, for at most `within`. The shard closes first, so
    /// no new write lands meanwhile and a busy log stops growing away from
    /// `to`. Asked anyway when the wait runs out: the term is closed by then,
    /// and a transfer that does not go through ends it.
    pub(crate) async fn transfer_in_caught_up(
        &self,
        term: u64,
        to: MemberId,
        within: std::time::Duration,
    ) -> Option<u64> {
        let term = self.transfer_starting_in(Some(term)).await?;
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let log = self.replication();
            if log.matched.get(&to) >= Some(&log.last_index)
                || tokio::time::Instant::now() >= deadline
            {
                break;
            }
            tokio::time::sleep(CAUGHT_UP_POLL).await;
        }
        let _ = self.raft.trigger().transfer_leader(to).await;
        Some(term)
    }

    /// Add `member` as a learner. It takes no vote.
    pub(crate) async fn add_learner(
        &self,
        basis: &Basis,
        member: MemberId,
    ) -> Result<(), StepError> {
        settled(basis)?;
        self.change(basis, MembershipEdit::learner(member)).await
    }

    /// Make `voters` the group's voters, at `version`. Every one of them
    /// is already a member, and a voter that leaves is dropped from the
    /// membership. openraft writes a joint membership first when it has
    /// to, and leaves it once it is committed.
    pub(crate) async fn change_voters(
        &self,
        basis: &Basis,
        voters: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        settled(basis)?;
        self.leads_in(&voters)?;
        not_older(basis, version)?;
        self.change(basis, MembershipEdit::voters(voters, version))
            .await
    }

    /// Leave the joint membership `basis` holds for its new voters.
    pub(crate) async fn leave_joint(&self, basis: &Basis) -> Result<(), StepError> {
        // Leaving a membership that is not joint would append it again.
        let [_, voters] = basis.membership.get_joint_config().as_slice() else {
            return Err(StepError::NotJoint);
        };
        self.leads_in(voters)?;
        self.change(basis, MembershipEdit::flatten(voters.clone()))
            .await
    }

    /// Write the voters `basis` holds again at `version`, with `learners`
    /// as the only other members. One entry, never a joint.
    pub(crate) async fn stamp(
        &self,
        basis: &Basis,
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        settled(basis)?;
        let voters = basis.membership.get_joint_config().clone();
        voters.iter().try_for_each(|set| self.leads_in(set))?;
        not_older(basis, version)?;
        let stamped = Membership::new(voters, learners, version);
        let written = self
            .raft
            .append_membership(stamped, EntryPayload::Blank, basis.preconditions())
            .await;
        outcome(written.map(|_| ()))
    }

    async fn change(&self, basis: &Basis, edit: MembershipEdit) -> Result<(), StepError> {
        let written = self
            .raft
            .change_membership_if(edit, false, basis.preconditions())
            .await;
        outcome(written.map(|_| ()))
    }

    /// A leader never writes a membership that leaves it out. openraft
    /// would let it go on leading, with a lease on a set it is not in.
    fn leads_in(&self, voters: &BTreeSet<MemberId>) -> Result<(), StepError> {
        if voters.contains(&self.member) {
            Ok(())
        } else {
            Err(StepError::WithoutLeader)
        }
    }
}

fn settled(basis: &Basis) -> Result<(), StepError> {
    if basis.membership.get_joint_config().len() > 1 {
        return Err(StepError::Joint);
    }
    Ok(())
}

/// A membership's version never goes back: a leader that took an older
/// target for a newer one would undo a change the control plane made since.
fn not_older(basis: &Basis, version: TargetVersion) -> Result<(), StepError> {
    if version < basis.membership.version() {
        return Err(StepError::OlderVersion);
    }
    Ok(())
}

fn outcome(
    written: Result<(), RaftError<ShardTypes, ClientWriteError<ShardTypes>>>,
) -> Result<(), StepError> {
    match written {
        Ok(()) => Ok(()),
        Err(RaftError::APIError(ClientWriteError::PreconditionFailed(_))) => Err(StepError::Moved),
        Err(RaftError::APIError(
            ClientWriteError::ForwardToLeader(_) | ClientWriteError::LogEntryDiscarded(_),
        )) => Err(StepError::NotLeader),
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            err @ (ChangeMembershipError::InProgress(_)
            | ChangeMembershipError::UncommittedLeaderLog(_)),
        ))) => Err(StepError::Busy(err.to_string())),
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(err))) => {
            Err(StepError::Refused(err.to_string()))
        }
        Err(RaftError::Fatal(_)) => Err(StepError::Closed),
    }
}
