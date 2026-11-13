//! The ids and the snapshot form of a member on the segment store: a log id
//! is a term and an index, and a snapshot travels as one streamed request.
use serde::{Deserialize, Serialize};

use super::{StoredMembership, lib_log_id, log_id};
use crate::raft::types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct Vote {
    pub(in crate::raft) term: u64,
    pub(in crate::raft) voted_for: u64,
    pub(in crate::raft) committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct LogId {
    pub(in crate::raft) term: u64,
    pub(in crate::raft) index: u64,
}

/// A snapshot's descriptor, as sent and as kept beside the snapshot's data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct SnapshotMeta {
    pub(in crate::raft) last_log_id: Option<LogId>,
    pub(in crate::raft) last_membership: StoredMembership,
}

/// The head of a snapshot request, before its data. The body around it is
/// `felix_storage::raft_log::transfer`'s framing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct SnapshotHead {
    pub(in crate::raft) vote: Vote,
    pub(in crate::raft) meta: SnapshotMeta,
}

impl SnapshotHead {
    pub(in crate::raft) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a snapshot head is plain data")
    }

    /// `None` when `bytes` is not a head.
    pub(in crate::raft) fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

impl From<&types::Vote> for Vote {
    fn from(vote: &types::Vote) -> Self {
        Self {
            term: vote.leader_id.term,
            voted_for: vote.leader_id.voted_for,
            committed: vote.committed,
        }
    }
}

impl From<Vote> for types::Vote {
    fn from(vote: Vote) -> Self {
        Self {
            leader_id: types::LeaderId {
                term: vote.term,
                voted_for: vote.voted_for,
            },
            committed: vote.committed,
        }
    }
}

impl From<&types::LogId> for LogId {
    fn from(id: &types::LogId) -> Self {
        Self {
            term: *id.leader_id,
            index: id.index,
        }
    }
}

impl From<LogId> for types::LogId {
    fn from(id: LogId) -> Self {
        Self {
            leader_id: id.term.into(),
            index: id.index,
        }
    }
}

impl From<&types::SnapshotMeta> for SnapshotMeta {
    fn from(meta: &types::SnapshotMeta) -> Self {
        Self {
            last_log_id: log_id(&meta.last_log_id),
            last_membership: (&meta.last_membership).into(),
        }
    }
}

impl From<SnapshotMeta> for types::SnapshotMeta {
    fn from(meta: SnapshotMeta) -> Self {
        Self {
            last_log_id: lib_log_id(meta.last_log_id),
            last_membership: meta.last_membership.into(),
        }
    }
}

#[cfg(test)]
mod tests;
