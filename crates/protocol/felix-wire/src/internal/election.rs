//! Electing a leader without the control plane: what a candidate's fence
//! carries beyond a promotion's, and the pre-vote.
//!
//! See "Electing without the control plane" in `docs/replication-design.md`.
//! Everything here is sent only to a peer that offered
//! [`PeerCapabilities::ELECTION`](super::PeerCapabilities::ELECTION).

use std::collections::HashSet;

use super::{ReplicaLog, ShardRef};
use crate::error::{Error, Result};

/// Most members a set names, as the replica-set file allows.
pub const MAX_SET_MEMBERS: usize = 64;
/// Longest node id in a set, as the replica-set file allows.
pub const MAX_SET_NODE_BYTES: usize = 1024;

/// One replica in a set: its node id and the storage identity the fence
/// counts it under, when placement named one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetMember {
    pub node_id: String,
    pub storage_id: Option<[u8; 16]>,
    /// Listed but not counted: re-admitted and not yet caught up.
    pub learner: bool,
}

/// An assignment's replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetView {
    pub generation: u64,
    pub members: Vec<SetMember>,
}

impl ReplicaSetView {
    /// What the codec insists on both ways: sizes within the limits and no
    /// member listed twice, since counting one twice counts a majority that
    /// is not one.
    pub(super) fn check(&self) -> Result<()> {
        if self.members.len() > MAX_SET_MEMBERS
            || self
                .members
                .iter()
                .any(|member| member.node_id.len() > MAX_SET_NODE_BYTES)
        {
            return Err(Error::FrameTooLarge);
        }
        let mut nodes = HashSet::new();
        let mut storage = HashSet::new();
        for member in &self.members {
            if !nodes.insert(member.node_id.as_str()) {
                return Err(Error::Incomplete);
            }
            if let Some(id) = member.storage_id
                && !storage.insert(id)
            {
                return Err(Error::Incomplete);
            }
        }
        Ok(())
    }
}

/// What a fence says about the set, for a replica that checks sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceElection {
    /// The sender is standing for election at `shard.generation`, not
    /// leading a generation the control plane named. An explicit flag, since
    /// the generation alone does not say which.
    pub candidate: bool,
    /// The set the sender counts, with its generation.
    pub set: ReplicaSetView,
    /// While a change is pending, the set being left. Older than `set`.
    pub leaving: Option<ReplicaSetView>,
}

impl FenceElection {
    pub(super) fn check(&self) -> Result<()> {
        self.set.check()?;
        if let Some(leaving) = &self.leaving {
            leaving.check()?;
            if leaving.generation >= self.set.generation {
                return Err(Error::Incomplete);
            }
        }
        Ok(())
    }
}

/// A would-be candidate asks whether a replica would vote for it, before it
/// raises its generation. Neither side persists anything for it, so a
/// refused or abandoned pre-vote leaves no trace.
///
/// `shard.generation` is the generation the asker would stand at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVote {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub log: ReplicaLog,
    /// The generation the asker's last record was written at.
    pub last_generation: u64,
    /// One past the last record the asker holds.
    pub log_end: u64,
    /// The generation of the set the asker counts.
    pub set_generation: u64,
}

/// Why a replica would not vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PreVoteRefusal {
    /// It has heard from the leader recently enough to keep it.
    LeaderReachable = 1,
    /// Its log is ahead of the asker's.
    LogBehind = 2,
    /// It has accepted a generation at or past the one asked for.
    StaleGeneration = 3,
    /// It counts a newer set than the asker.
    StaleSet = 4,
    /// It does not vote: it is outside the set, or has not caught up since
    /// its storage was replaced.
    NotVoting = 5,
    /// It holds no copy of the shard.
    UnknownShard = 6,
}

impl PreVoteRefusal {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::LeaderReachable),
            2 => Ok(Self::LogBehind),
            3 => Ok(Self::StaleGeneration),
            4 => Ok(Self::StaleSet),
            5 => Ok(Self::NotVoting),
            6 => Ok(Self::UnknownShard),
            _ => Err(Error::Incomplete),
        }
    }
}

/// The answer to [`PreVote`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreVoteAnswer {
    pub correlation_id: u64,
    /// `None` is granted.
    pub refusal: Option<PreVoteRefusal>,
}

impl PreVoteAnswer {
    pub fn granted(&self) -> bool {
        self.refusal.is_none()
    }
}
