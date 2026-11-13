//! The cluster identity the metadata group records once, at formation.
//!
//! It names the group, so a broker that kept a group's data can tell it is
//! talking to a different one, and the shard engine the group was formed for.
//! First record wins and nothing changes it afterwards; see
//! `docs/metadata-raft-design.md`.
use serde::{Deserialize, Serialize};

/// What the group recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    pub group_id: String,
    pub shard_engine: String,
}

impl ClusterIdentity {
    /// A fresh identity for a group that has none, for `shard_engine`.
    pub fn generate(shard_engine: &str) -> Self {
        Self {
            group_id: uuid::Uuid::new_v4().to_string(),
            shard_engine: shard_engine.to_string(),
        }
    }
}

/// What a store knows of its group's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupIdentity {
    /// The backend has no group to protect (memory), and the engine this
    /// instance is configured for.
    NoGroup {
        shard_engine: String,
    },
    /// A group that has not recorded one yet.
    Pending,
    Recorded(ClusterIdentity),
}

impl GroupIdentity {
    /// The group id, when one is recorded.
    pub fn group_id(&self) -> Option<String> {
        match self {
            Self::Recorded(identity) => Some(identity.group_id.clone()),
            Self::NoGroup { .. } | Self::Pending => None,
        }
    }
}
