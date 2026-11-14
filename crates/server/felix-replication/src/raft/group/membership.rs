//! Membership steps as a group writes them. Each is written only on the
//! membership and the term it was decided on, so a step decided on a log
//! that has moved writes nothing. See `docs/per-shard-raft.md`,
//! "Membership".

// The driver that decides and calls these steps is not built yet.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeSet;

use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::Precondition;
use openraft::type_config::alias::LogIdOf;
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, ServerState};

use super::Group;
use crate::raft::membership::TargetVersion;
use crate::raft::types::{MemberId, Membership, MembershipEdit, ShardTypes};

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
    /// openraft refused the membership.
    Refused(String),
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
                Basis {
                    term: vote.leader_id().term(),
                    at,
                    membership: latest.membership().clone(),
                    committed: at.as_ref() <= state.cluster_committed(),
                }
            })
        });
        read.await.ok().flatten()
    }

    /// Add `member` as a learner. It takes no vote.
    pub(crate) async fn add_learner(
        &self,
        basis: &Basis,
        member: MemberId,
    ) -> Result<(), StepError> {
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
        self.leads_in(&voters)?;
        self.change(basis, MembershipEdit::voters(voters, version))
            .await
    }

    /// Leave the joint membership `basis` holds for its new voters.
    pub(crate) async fn leave_joint(&self, basis: &Basis) -> Result<(), StepError> {
        let Some(voters) = basis.membership.get_joint_config().last() else {
            return Err(StepError::Refused("the membership has no voters".into()));
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
        let voters = basis.membership.get_joint_config().clone();
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

fn outcome(
    written: Result<(), RaftError<ShardTypes, ClientWriteError<ShardTypes>>>,
) -> Result<(), StepError> {
    match written {
        Ok(()) => Ok(()),
        Err(RaftError::APIError(ClientWriteError::PreconditionFailed(_))) => Err(StepError::Moved),
        Err(RaftError::APIError(
            ClientWriteError::ForwardToLeader(_) | ClientWriteError::LogEntryDiscarded(_),
        )) => Err(StepError::NotLeader),
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(err))) => {
            Err(StepError::Refused(err.to_string()))
        }
        Err(RaftError::Fatal(_)) => Err(StepError::Closed),
    }
}
