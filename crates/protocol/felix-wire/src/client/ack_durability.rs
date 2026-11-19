//! What a broker has done by the time it acknowledges a publish to a stream.
//!
//! Carried on `stream_shards_view` for a client that offered
//! `FEATURE_ACK_DURABILITY`. It describes the broker that answers, as that
//! broker is configured now; it is not a promise from the cluster. See
//! `docs/protocol.md`, "AckDurability".

use serde::{Deserialize, Serialize};

/// When the acknowledgement of a publish is sent, relative to the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckPoint {
    /// Before the record is written. The ack says the broker accepted the
    /// publish, nothing more; a crash can lose the record.
    Enqueue,
    /// After the record is written under the stream's `copies` and `fsync`.
    Write,
}

/// Who holds the record when the acknowledgement is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckCopies {
    /// The broker that owns the shard. Other replicas may still be catching
    /// up, so losing this broker's storage can lose acknowledged records.
    Leader,
    /// A majority of the shard's replica set, the leader included.
    Quorum,
}

/// How the broker syncs a durable stream's log, which is what bounds the loss
/// of an acknowledged record to a machine crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckFsync {
    /// Never on a schedule. Acknowledged records survive a process crash but
    /// not a machine crash or power loss.
    None,
    /// On a timer. A machine crash can lose what was acknowledged since the
    /// last sync.
    Periodic,
    /// Before the acknowledgement. A machine crash does not lose it.
    OnCommit,
}

/// What an acknowledged publish to a stream means on the broker that answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckDurability {
    /// When the acknowledgement is sent.
    pub ack_at: AckPoint,
    /// Who holds the record at that point.
    pub copies: AckCopies,
    /// How a durable stream's log is synced. Absent for a stream with no log,
    /// which is lost with the broker's process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fsync: Option<AckFsync>,
}
