//! The ids and the snapshot form of a member on the segment store: a log id
//! is a term and an index, and a snapshot travels in one request.
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

/// What comes before the data in a snapshot request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct SnapshotHead {
    pub(in crate::raft) vote: Vote,
    pub(in crate::raft) meta: SnapshotMeta,
}

/// A snapshot request's body: the head's length as four big-endian bytes,
/// the head as JSON, then the snapshot's data as it is.
pub(in crate::raft) fn snapshot_body(head: &SnapshotHead, data: &[u8]) -> Vec<u8> {
    let head = serde_json::to_vec(head).expect("a snapshot head is plain data");
    let len = u32::try_from(head.len()).expect("a snapshot head is far below 4 GiB");
    let mut body = Vec::with_capacity(4 + head.len() + data.len());
    body.extend_from_slice(&len.to_be_bytes());
    body.extend_from_slice(&head);
    body.extend_from_slice(data);
    body
}

/// The head and the data of a snapshot request's body, or `None` when it is
/// not one.
pub(in crate::raft) fn parse_snapshot_body(body: &[u8]) -> Option<(SnapshotHead, &[u8])> {
    let (len, rest) = body.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_be_bytes(*len)).ok()?;
    let (head, data) = rest.split_at_checked(len)?;
    Some((serde_json::from_slice(head).ok()?, data))
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
