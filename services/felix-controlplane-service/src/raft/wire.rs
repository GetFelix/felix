//! What this member sends to its peers and writes to its store, as JSON.
//!
//! These are Felix's types, not the library's, so an openraft upgrade does
//! not change what members exchange or keep. A log id is a term and an
//! index, and a snapshot travels as one streamed request whose head is
//! [`SnapshotHead`].
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::types;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Vote {
    pub(super) term: u64,
    pub(super) voted_for: u64,
    pub(super) committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LogId {
    pub(super) term: u64,
    pub(super) index: u64,
}

/// A snapshot's descriptor, as sent and as kept beside the snapshot's data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SnapshotMeta {
    pub(super) last_log_id: Option<LogId>,
    pub(super) last_membership: StoredMembership,
}

/// The head of a snapshot request, before its data. The body around it is
/// `felix_storage::raft_log::transfer`'s framing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SnapshotHead {
    pub(super) vote: Vote,
    pub(super) meta: SnapshotMeta,
}

impl SnapshotHead {
    pub(super) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a snapshot head is plain data")
    }

    /// `None` when `bytes` is not a head.
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct InstallSnapshotResponse {
    pub(super) vote: Vote,
}

/// A peer's answer, the whole `Result`. The error half is the library's
/// shape, which can change, so it is read as text and treated like an
/// unreachable member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Reply<T> {
    Ok(T),
    Err(serde_json::Value),
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

#[cfg(test)]
mod tests;
