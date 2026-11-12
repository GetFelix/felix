//! The ids and the snapshot forms of a member on redb: a leader is a term
//! and a node id, and a snapshot is named and travels in chunks, as openraft
//! 0.9 wrote them.
use serde::{Deserialize, Serialize};

use super::{StoredMembership, lib_log_id, log_id};
use crate::raft::types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct LeaderId {
    pub(in crate::raft) term: u64,
    pub(in crate::raft) node_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct Vote {
    pub(in crate::raft) leader_id: LeaderId,
    pub(in crate::raft) committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct LogId {
    pub(in crate::raft) leader_id: LeaderId,
    pub(in crate::raft) index: u64,
}

/// A snapshot's descriptor. 0.9 named every snapshot with an id; later
/// versions name it by the log position it covers. The id stays in the
/// stored and sent form so a 0.9 member can read both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct SnapshotMeta {
    pub(in crate::raft) last_log_id: Option<LogId>,
    pub(in crate::raft) last_membership: StoredMembership,
    pub(in crate::raft) snapshot_id: String,
}

/// One chunk of a snapshot. A transfer is every chunk with the same
/// `meta.snapshot_id`, in offset order, the last one marked `done`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::raft) struct InstallSnapshotRequest {
    pub(in crate::raft) vote: Vote,
    pub(in crate::raft) meta: SnapshotMeta,
    pub(in crate::raft) offset: u64,
    pub(in crate::raft) data: Vec<u8>,
    pub(in crate::raft) done: bool,
}

/// The error a 0.9 receiver answers when a chunk does not continue the
/// transfer it is holding. A 0.9 sender starts the transfer again on it.
pub(in crate::raft) fn snapshot_mismatch(
    expect: (&str, u64),
    got: (&str, u64),
) -> serde_json::Value {
    serde_json::json!({"APIError": {"SnapshotMismatch": {
        "expect": {"id": expect.0, "offset": expect.1},
        "got": {"id": got.0, "offset": got.1},
    }}})
}

#[cfg(test)]
pub(in crate::raft) fn is_snapshot_mismatch(err: &serde_json::Value) -> bool {
    err.pointer("/APIError/SnapshotMismatch").is_some()
}

/// The id a snapshot is stored and sent under: the position it covers. Two
/// snapshots at one position hold the same state, so they may share it.
pub(in crate::raft) fn snapshot_id(last_log_id: Option<&LogId>) -> String {
    match last_log_id {
        Some(id) => format!(
            "{}-{}-{}",
            id.leader_id.term, id.leader_id.node_id, id.index
        ),
        None => "empty".to_string(),
    }
}

impl From<types::LeaderId> for LeaderId {
    fn from(id: types::LeaderId) -> Self {
        Self {
            term: id.term,
            node_id: id.node_id,
        }
    }
}

impl From<LeaderId> for types::LeaderId {
    fn from(id: LeaderId) -> Self {
        Self {
            term: id.term,
            node_id: id.node_id,
        }
    }
}

impl From<&types::Vote> for Vote {
    fn from(vote: &types::Vote) -> Self {
        Self {
            leader_id: vote.leader_id.into(),
            committed: vote.committed,
        }
    }
}

impl From<Vote> for types::Vote {
    fn from(vote: Vote) -> Self {
        Self {
            leader_id: vote.leader_id.into(),
            committed: vote.committed,
        }
    }
}

impl From<&types::LogId> for LogId {
    fn from(id: &types::LogId) -> Self {
        Self {
            leader_id: id.leader_id.into(),
            index: id.index,
        }
    }
}

impl From<LogId> for types::LogId {
    fn from(id: LogId) -> Self {
        Self {
            leader_id: id.leader_id.into(),
            index: id.index,
        }
    }
}

impl From<&types::SnapshotMeta> for SnapshotMeta {
    fn from(meta: &types::SnapshotMeta) -> Self {
        let last_log_id = log_id(&meta.last_log_id);
        Self {
            snapshot_id: snapshot_id(last_log_id.as_ref()),
            last_log_id,
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
