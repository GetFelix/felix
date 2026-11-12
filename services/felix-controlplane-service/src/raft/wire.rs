//! What this member sends to its peers and writes to its store, as JSON.
//!
//! These are Felix's types, not the library's. On redb they have the field
//! names openraft 0.9 serialized, so a member on a release that ran 0.9 reads
//! what this one sends and stores, and the reverse. That is what lets a group
//! be upgraded one member at a time and a member be rolled back. The bytes
//! are pinned by the fixtures in `fixtures/openraft-0.9`
//! (`docs/metadata-raft-design.md`, "openraft 0.9 to 0.10").
//!
//! A member on the segment store names a log id by its term alone and sends
//! a snapshot in one piece (`segment_form`). The two kinds of member serve
//! different routes ([`super::PEER_PREFIX`]) and never read each other's
//! form.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::types;

#[cfg(not(felix_segment_store))]
mod redb_form;
#[cfg(felix_segment_store)]
mod segment_form;

#[cfg(not(felix_segment_store))]
pub(super) use redb_form::*;
#[cfg(felix_segment_store)]
pub(super) use segment_form::*;

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

/// A peer's answer: 0.9 sent its whole `Result`. The error half differs
/// between library versions, so it is read as text and treated like an
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

// The fixtures are what openraft 0.9 wrote, which only a member on redb
// reads.
#[cfg(all(test, not(felix_segment_store)))]
pub(crate) mod tests;
