//! A shard's identity, as the control plane names it in assignments.
//!
//! Here rather than in the broker service because replication and the
//! service's shard ownership both key on it.

use serde::Deserialize;

/// What an assignment is an assignment *of*.
///
/// The control plane places cache shards alongside stream shards, and the two
/// share every other field of the key. A broker that ignored this would file a
/// cache's shard under the stream of the same name and let one overwrite the
/// other's ownership.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ShardKind {
    /// Absent on the wire means this, which is what a control plane that
    /// predates cache placement sends.
    #[default]
    Stream,
    Cache,
}

/// A shard's identity, as the control plane names it in assignments.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    #[serde(default)]
    pub kind: ShardKind,
}
