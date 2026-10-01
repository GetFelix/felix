//! Requests one broker hands to the broker that owns a shard: publishes and
//! cache operations, and the answers to them.

use bytes::Bytes;

use super::{ErrorCode, ShardRef};
use crate::error::{Error, Result};

/// A publish handed to the broker that owns the shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPublish {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub ack: AckMode,
    pub payloads: Vec<Bytes>,
    /// The publisher's own bearer token, verified again by the owner.
    ///
    /// The ingress broker checked it before forwarding, but the owner cannot
    /// know that: a forwarder that skipped the check, or that is not a Felix
    /// broker at all, sends the same bytes. Re-checking here is what makes
    /// the owner's write depend on the client's authority rather than the
    /// forwarder's honesty.
    ///
    /// Empty on the legacy `ForwardPublish` kind, which is decoded but which
    /// an owner on this build refuses. Non-empty encodes as
    /// [`Kind::AuthorizedForwardPublish`].
    ///
    /// [`Kind::AuthorizedForwardPublish`]: super::Kind::AuthorizedForwardPublish
    pub credential: String,
}

/// How the origin publisher asked for its write to be acknowledged.
///
/// Carried across the forward so the owner applies the guarantee the client
/// asked for, not the one the forwarding broker would have chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AckMode {
    None = 0,
    OnAccept = 1,
    OnCommit = 2,
}

impl AckMode {
    /// Parse the wire value; an unknown one is an error.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(AckMode::None),
            1 => Ok(AckMode::OnAccept),
            2 => Ok(AckMode::OnCommit),
            other => Err(Error::UnknownInternalAckMode(other)),
        }
    }
}

/// The owner accepted and wrote the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardPublishOk {
    pub correlation_id: u64,
    pub first_offset: u64,
    /// Inclusive, so a single-record batch has `first_offset == last_offset`.
    pub last_offset: u64,
}

impl ForwardPublishOk {
    /// The answer for a batch the owner wrote at `offsets`, or to a stream
    /// with no log when `None`. That case is sent as an empty range, `1..=0`,
    /// which no written batch has.
    pub fn new(correlation_id: u64, offsets: Option<(u64, u64)>) -> Self {
        let (first_offset, last_offset) = offsets.unwrap_or((1, 0));
        Self {
            correlation_id,
            first_offset,
            last_offset,
        }
    }

    /// Where the batch landed, or `None` for a stream with no log. Only
    /// meaningful from an owner that advertised
    /// [`super::PeerCapabilities::FORWARD_OFFSETS`].
    pub fn offsets(&self) -> Option<(u64, u64)> {
        (self.first_offset <= self.last_offset).then_some((self.first_offset, self.last_offset))
    }
}

/// The owner refused, with a reason the requester can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPublishError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    /// Operator-facing detail. Never parsed for control flow — `code` is what a
    /// requester branches on.
    pub detail: String,
}

/// The shard is owned elsewhere, and here is where.
///
/// A distinct kind rather than an error code, because it carries a routing
/// answer rather than only a reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeader {
    pub correlation_id: u64,
    pub node_id: String,
    /// `host:port` of the owner's internal listener, as the catalog advertises
    /// it. A string rather than a parsed address: the responder repeats what it
    /// was told, and the requester decides whether it can be reached.
    pub advertise_addr: String,
    /// The generation the responder believes is current.
    pub generation: u64,
}

/// A cache operation handed to the broker that owns the key's shard.
///
/// One message for all three operations rather than three kinds: they share a
/// shard reference and a key, differ only in what they carry alongside, and a
/// single kind keeps the owner's dispatch one match instead of three arms that
/// must stay in step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardCacheOp {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub op: CacheOpKind,
    pub key: String,
    /// The value to store. Empty for `Get` and `Delete`.
    pub value: Bytes,
    /// Expiry in milliseconds, or `0` for none. Only read for `Put`.
    pub ttl_ms: u64,
    /// The caller's bearer token; see [`ForwardPublish::credential`].
    /// Non-empty encodes as [`Kind::AuthorizedForwardCacheOp`].
    ///
    /// [`Kind::AuthorizedForwardCacheOp`]: super::Kind::AuthorizedForwardCacheOp
    pub credential: String,
}

/// Which cache operation a forwarded request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CacheOpKind {
    Put = 1,
    Get = 2,
    Delete = 3,
    /// Apply a signed delta to a counter in the shard's counter store. The
    /// delta rides the `value` bytes as eight big-endian bytes; the answer's
    /// value bytes carry the resulting sum the same way.
    CounterAdd = 4,
    /// Read a counter's sum. Answered like a `Get`: absent value for a
    /// counter never written.
    CounterGet = 5,
}

impl CacheOpKind {
    /// Parse the wire value; an unknown one is an error.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(CacheOpKind::Put),
            2 => Ok(CacheOpKind::Get),
            3 => Ok(CacheOpKind::Delete),
            4 => Ok(CacheOpKind::CounterAdd),
            5 => Ok(CacheOpKind::CounterGet),
            other => Err(Error::UnknownInternalCacheOp(other)),
        }
    }
}

/// The owner applied the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardCacheOk {
    pub correlation_id: u64,
    /// The value a `Get` found or a `Delete` removed.
    ///
    /// `None` is a miss, and is also what a `Put` always answers — the three
    /// operations share one response shape, and "no value to report" is the
    /// honest reading for a write.
    pub value: Option<Bytes>,
}

/// The owner could not apply the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardCacheError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    /// Operator-facing detail. Never parsed for control flow.
    pub detail: String,
}
