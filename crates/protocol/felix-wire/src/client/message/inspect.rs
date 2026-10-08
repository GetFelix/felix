//! What `shard_inspect` answers: one broker's own view of one shard.
//!
//! The word-valued fields (`role`, `phase`, `reason`, `state`) are strings
//! rather than enums, so a broker can add a value without an older felixctl
//! failing to decode the whole answer. Everything past the identity is
//! optional and left out when absent.

use serde::{Deserialize, Serialize};

/// One broker's view of one shard. Never forwarded: an operator who wants
/// another broker's view asks that broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardInspection {
    /// The broker answering. Empty on a broker that is not in a cluster.
    pub node_id: String,
    /// How many shards the stream or cache has, as this broker's routing sees
    /// it. `0` when it knows nothing of it.
    pub shards: u32,
    /// `leader`, `follower` or `none`: this broker's place in the shard's
    /// replica set, as its routing has it.
    pub role: String,
    /// The shard's phase here: `unassigned`, `opening`, `fencing`, `active`,
    /// `draining`, `closed` or `failed`.
    pub phase: String,
    /// Whether this broker takes writes for the shard right now.
    pub serving: bool,
    /// Why not, when `serving` is false: `opening`, `fencing`, `failed`,
    /// `draining`, `lease_lapsed`, `not_assigned_here` or
    /// `behind_generation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The reason in a sentence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The generation this broker holds the shard at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    /// The assignment as this broker's routing last received it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<InspectedAssignment>,
    /// The promotion fence, while this broker waits for its replicas to take
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<InspectedFence>,
    /// This broker's lease. Absent on a broker with none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<InspectedLease>,
    /// One past the last record in this broker's copy of the log. Absent
    /// when the log is not open here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<u64>,
    /// The leader's commit mark: everything below it is held by a majority.
    /// Only a leader of a `Quorum` shard has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed: Option<u64>,
    /// The newest generation this broker's log has accepted a leader at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_generation: Option<u64>,
    /// The leader's view of every other replica. Empty from a follower.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<InspectedReplica>,
}

/// A shard's assignment: who leads it, who holds copies, and where a move is
/// taking it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedAssignment {
    pub generation: u64,
    pub leader: String,
    /// Every replica other than the leader.
    pub replicas: Vec<String>,
    /// The leader has been told to stop serving so the shard can move.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub draining: bool,
    /// The broker a move in progress is handing the shard to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successor: Option<String>,
}

/// A promoted leader's fence: which replicas have taken its generation in the
/// latest attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedFence {
    pub took: Vec<String>,
    pub pending: Vec<String>,
    /// Attempts in a row at this generation that left the shard closed.
    pub attempts: u32,
    /// When the next attempt starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_in_ms: Option<u64>,
}

/// The broker's lease, which a leader needs to serve writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedLease {
    pub held: bool,
    pub remaining_ms: u64,
}

/// One replica as the leader sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectedReplica {
    pub node_id: String,
    /// `follower`, or `learner` for a move's destination still copying.
    pub role: String,
    /// The next offset the leader will ship to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
    /// Records it is behind the leader's tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    /// Whether it took the fence, while the leader is fencing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<bool>,
    /// `shipping`, `stalled`, `copying`, `rebuilding`, `halted` or `fencing`.
    pub state: String,
    /// Why replication to it stopped, when `state` is `halted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub halted: Option<String>,
}
