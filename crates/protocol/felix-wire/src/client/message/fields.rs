//! The structured values carried inside a [`Message`](super::Message)'s fields.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Somewhere a client may connect, as one broker understands the cluster.
///
/// Carries only what a client needs in order to connect: an identity to
/// recognise it by and an address to dial. Deliberately not the control plane's
/// node record -- placement, capacity, and liveness detail are the cluster's
/// business, and a tenant's client has no standing to read them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    pub node_id: String,
    /// `host:port`, as the broker was configured to advertise to clients.
    pub addr: String,
}

/// Whether a shard question is about a stream or a cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardKind {
    Stream,
    Cache,
}

/// Who owns one shard, as one broker's routing snapshot sees it.
///
/// Can be stale the way any routing answer can: a shard that moved after the
/// snapshot was taken is described where it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOwner {
    pub shard: u32,
    /// The owning broker's node id. Absent when no broker can serve the shard
    /// right now (`unavailable` says why), or when the answering broker is not
    /// in a cluster and serves every shard itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// `host:port` the owner serves clients on, or absent when the cluster has
    /// not been told where clients reach it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    /// The ownership epoch this describes. `0` from a broker not in a cluster.
    pub generation: u64,
    /// Why no broker can serve the shard right now, as the `reason` of a
    /// `shard_unavailable` error (`not_assigned`, `owner_unavailable`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
}

/// How a publish asks to be acknowledged. `None` asks for no answer at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    None,
    PerMessage,
    PerBatch,
}

/// Why a `publish_idempotent`, `publish_if` or conditional `commit` was not
/// appended.
///
/// Each names a different remedy, which is why they are not one string. A
/// gap means the producer skipped ahead and must not continue as if it had
/// not; an unknown producer means this broker holds nothing to check against
/// and the producer must start again with a new id; an expired sequence is a
/// re-send from further back than the broker remembers; an offset mismatch
/// means another write landed first; and not-leader means the batch went to
/// a broker that does not lead the shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishRefusalReason {
    /// The sequence is past the next one expected; what was skipped is lost
    /// to this broker and the producer must not carry on past it.
    SequenceGap {
        /// The sequence the broker would have appended.
        expected: u64,
    },
    /// The broker holds no sequence for this producer on this shard and the
    /// batch was not its first. Nothing can be checked against, so nothing
    /// is appended; the producer needs a new id.
    UnknownProducer,
    /// The sequence is older than the window the broker keeps, so whether it
    /// was appended cannot be told any more.
    SequenceExpired,
    /// The broker already holds a batch under this sequence and its payloads
    /// differ, so this one is not a re-send of it and was not appended. The
    /// producer reused a number it had spent. Only sent to a client that
    /// offered `FEATURE_SEQUENCE_REUSED`; any other client is answered as if
    /// it had re-sent the batch held.
    SequenceReused,
    /// A `publish_if` or conditional `commit` expected the shard's next
    /// offset to be something else. Nothing was written and no offset was
    /// consumed. `tail` is the next offset as the broker checked it,
    /// including any record a promotion wrote, so it is where a writer that
    /// still owns the shard tries again.
    OffsetMismatch {
        /// The shard's next offset.
        tail: u64,
    },
    /// This broker does not lead the shard, and only the leader holds the
    /// sequences; the batch has to go to the broker named here.
    NotLeader {
        /// Who leads it.
        node_id: String,
        /// `host:port` the leader serves clients on, or absent when the
        /// cluster has not been told where clients reach it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        addr: Option<String>,
    },
}

/// Where a subscription should begin.
///
/// Serde's default tagging spells these `"latest"`, `"earliest"` and
/// `{"offset": 42}`, which keeps the common cases short in a JSON control
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// The live tail: deliver only what is published from now on. Identical to
    /// omitting the field, and spelled out for clients that prefer to be
    /// explicit.
    Latest,
    /// The oldest record the broker still retains.
    ///
    /// Deliberately not "offset 0": for a stream whose head has been trimmed,
    /// offset 0 is gone and asking for it is an error, whereas `earliest` means
    /// "as far back as you can" and always succeeds.
    Earliest,
    /// Resume at an exact log offset — the first record the client has *not*
    /// seen, so a client checkpoints the offset it last handled plus one.
    Offset(u64),
}

/// Why a subscribe could not start at the requested position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorErrorReason {
    /// The offset has been discarded by retention, or has fallen out of an
    /// in-memory stream's replay ring.
    TooOld,
    /// The offset is past the end of the stream.
    InFuture,
}

/// One record handed to a consumer, with the offset it must acknowledge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    pub offset: u64,
    #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
    pub payload: Bytes,
    /// How many times this record has been handed out, this delivery included.
    /// `1` is a first attempt; anything higher is a redelivery, so a consumer
    /// can treat a retry differently.
    ///
    /// `0` means the broker did not report it — absent rather than first, since
    /// claiming a first attempt for an unknown one would have a consumer skip
    /// exactly the retry handling it wanted.
    #[serde(default)]
    pub attempts: u32,
    /// How many offsets directly below this one the broker settled without
    /// delivering: generation-start records, and records retention removed
    /// before the group reached them. So a consumer can tell a hole that will
    /// never fill from a record still to come. Sent only to a client that
    /// negotiated `FEATURE_GROUP_SKIPPED`, and left out when `0`, so any other
    /// client gets the frame it always got.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skipped_before: u64,
    /// The principal that published the record, when the broker recorded
    /// one. Sent only to a client that negotiated `FEATURE_GROUP_PUBLISHER`,
    /// and left out when there is none, so any other client gets the frame
    /// it always got.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher: Option<String>,
    /// When the record was appended, in microseconds since the Unix epoch.
    /// Sent only to a client that negotiated `FEATURE_RECORD_TIMESTAMPS`, so
    /// any other client gets the frame it always got.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_micros: Option<u64>,
}

/// One record a `stream_read` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub offset: u64,
    #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
    pub payload: Bytes,
    /// The principal that published the record, when the broker recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher: Option<String>,
    /// When the record was appended, in microseconds since the Unix epoch.
    pub timestamp_micros: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One change to a stream shard's keyed state, as a `commit` carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum StateChange {
    Put {
        key: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
        value: Bytes,
    },
    Delete {
        key: String,
    },
}

/// What a `cache_put_if` requires of the key's current entry.
///
/// JSON is `"absent"` or `{"version": n}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheCondition {
    /// The key has no live entry: never written, deleted, or expired.
    Absent,
    /// The key's live entry has exactly this version, as a `cache_value` or a
    /// `cache_condition_result` reported it.
    Version(u64),
}
