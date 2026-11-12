//! What this member sends to its peers and writes to its store, as JSON.
//!
//! These are Felix's types, not the library's. They have the field names
//! openraft 0.9 serialized, so a member on a release that ran 0.9 reads what
//! this one sends and stores, and the reverse. That is what lets a group be
//! upgraded one member at a time and a member be rolled back. The bytes are
//! pinned by the fixtures in `fixtures/openraft-0.9`
//! (`docs/metadata-raft-design.md`, "openraft 0.9 to 0.10").
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LeaderId {
    pub(super) term: u64,
    pub(super) node_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Vote {
    pub(super) leader_id: LeaderId,
    pub(super) committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LogId {
    pub(super) leader_id: LeaderId,
    pub(super) index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Node {
    pub(super) addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Membership {
    pub(super) configs: Vec<BTreeSet<u64>>,
    pub(super) nodes: BTreeMap<u64, Node>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Payload {
    Blank,
    Normal(Vec<u8>),
    Membership(Membership),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Entry {
    pub(super) log_id: LogId,
    pub(super) payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredMembership {
    pub(super) log_id: Option<LogId>,
    pub(super) membership: Membership,
}

/// A snapshot's descriptor. 0.9 named every snapshot with an id; later
/// versions name it by the log position it covers. The id stays in the
/// stored and sent form so a 0.9 member can read both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SnapshotMeta {
    pub(super) last_log_id: Option<LogId>,
    pub(super) last_membership: StoredMembership,
    pub(super) snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct VoteRequest {
    pub(super) vote: Vote,
    pub(super) last_log_id: Option<LogId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct VoteResponse {
    pub(super) vote: Vote,
    pub(super) vote_granted: bool,
    pub(super) last_log_id: Option<LogId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct AppendEntriesRequest {
    pub(super) vote: Vote,
    pub(super) prev_log_id: Option<LogId>,
    pub(super) entries: Vec<Entry>,
    pub(super) leader_commit: Option<LogId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum AppendEntriesResponse {
    Success,
    PartialSuccess(Option<LogId>),
    Conflict,
    HigherVote(Vote),
}

/// One chunk of a snapshot. A transfer is every chunk with the same
/// `meta.snapshot_id`, in offset order, the last one marked `done`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InstallSnapshotRequest {
    pub(super) vote: Vote,
    pub(super) meta: SnapshotMeta,
    pub(super) offset: u64,
    pub(super) data: Vec<u8>,
    pub(super) done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InstallSnapshotResponse {
    pub(super) vote: Vote,
}

/// A peer's answer: 0.9 sent its whole `Result`. The error half differs
/// between library versions, so it is read as text and treated like an
/// unreachable member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Reply<T> {
    Ok(T),
    Err(serde_json::Value),
}

/// The error a 0.9 receiver answers when a chunk does not continue the
/// transfer it is holding. A 0.9 sender starts the transfer again on it.
pub(super) fn snapshot_mismatch(expect: (&str, u64), got: (&str, u64)) -> serde_json::Value {
    serde_json::json!({"APIError": {"SnapshotMismatch": {
        "expect": {"id": expect.0, "offset": expect.1},
        "got": {"id": got.0, "offset": got.1},
    }}})
}

#[cfg(test)]
pub(super) fn is_snapshot_mismatch(err: &serde_json::Value) -> bool {
    err.pointer("/APIError/SnapshotMismatch").is_some()
}

/// The id a snapshot is stored and sent under: the position it covers. Two
/// snapshots at one position hold the same state, so they may share it.
pub(super) fn snapshot_id(last_log_id: Option<&LogId>) -> String {
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

pub(super) fn log_id(id: &Option<types::LogId>) -> Option<LogId> {
    id.as_ref().map(LogId::from)
}

pub(super) fn lib_log_id(id: Option<LogId>) -> Option<types::LogId> {
    id.map(types::LogId::from)
}

impl From<&types::Membership> for Membership {
    fn from(membership: &types::Membership) -> Self {
        Self {
            configs: membership.get_joint_config().clone(),
            nodes: membership
                .nodes()
                .map(|(id, node)| {
                    (
                        *id,
                        Node {
                            addr: node.addr.clone(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<Membership> for types::Membership {
    fn from(membership: Membership) -> Self {
        let mut nodes: BTreeMap<u64, openraft::impls::BasicNode> = membership
            .nodes
            .into_iter()
            .map(|(id, node)| (id, openraft::impls::BasicNode { addr: node.addr }))
            .collect();
        // A voter without a node gets an empty address, as 0.9 gave it, so
        // the constructor has nothing left to refuse.
        for voter in membership.configs.iter().flatten() {
            nodes.entry(*voter).or_default();
        }
        types::Membership::new(membership.configs, nodes).unwrap_or_default()
    }
}

impl From<&types::Entry> for Entry {
    fn from(entry: &types::Entry) -> Self {
        Self {
            log_id: (&entry.log_id).into(),
            payload: match &entry.payload {
                types::EntryPayload::Blank => Payload::Blank,
                types::EntryPayload::Normal(command) => Payload::Normal(command.0.clone()),
                types::EntryPayload::Membership(membership) => {
                    Payload::Membership(membership.into())
                }
            },
        }
    }
}

impl From<Entry> for types::Entry {
    fn from(entry: Entry) -> Self {
        Self {
            log_id: entry.log_id.into(),
            payload: match entry.payload {
                Payload::Blank => types::EntryPayload::Blank,
                Payload::Normal(bytes) => types::EntryPayload::Normal(types::Command(bytes)),
                Payload::Membership(membership) => {
                    types::EntryPayload::Membership(membership.into())
                }
            },
        }
    }
}

impl From<&types::StoredMembership> for StoredMembership {
    fn from(stored: &types::StoredMembership) -> Self {
        Self {
            log_id: log_id(stored.log_id()),
            membership: stored.membership().into(),
        }
    }
}

impl From<StoredMembership> for types::StoredMembership {
    fn from(stored: StoredMembership) -> Self {
        Self::new(lib_log_id(stored.log_id), stored.membership.into())
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

#[cfg(test)]
pub(crate) mod tests;
