//! The plain objects the handles resolve with.
//!
//! `#[napi(object)]` types cross into JavaScript as ordinary objects, copied
//! out of the Rust values, so a caller can keep them after the handle that
//! produced them is gone.

use napi::bindgen_prelude::*;
use napi_derive::napi;

/// One delivered record.
#[napi(object)]
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Buffer,
    /// The record's log offset on a durable stream, absent on an ephemeral one.
    ///
    /// Subscriber queues shed under the default policy rather than blocking
    /// the publisher, so a gap here is the signal that it happened:
    /// `offset - previous - 1 - skippedBefore` records were dropped.
    pub offset: Option<BigInt>,
    /// How many offsets just before `offset` hold no event. Non-zero only on
    /// the first event after a leader change, whose generation-start record
    /// took an offset; zero from a broker that predates it.
    pub skipped_before: BigInt,
}

/// One record handed out by a consumer group.
#[napi(object)]
pub struct GroupRecord {
    pub offset: BigInt,
    pub payload: Buffer,
    /// How many times this record has been handed out, this delivery included.
    /// `1` is a first attempt; anything higher is a redelivery, so a consumer
    /// can treat a retry differently. `0` means the broker did not report it.
    pub attempts: u32,
    /// How many offsets directly below this one were settled without being
    /// delivered (generation starts, records retention removed), so a hole
    /// with this count will not fill. Zero from a broker that predates it.
    pub skipped_before: BigInt,
}

/// The delivery a nack names: a [`GroupRecord`] as `groupPoll` returned it.
/// Only these fields are read, so the payload is not copied back.
#[napi(object)]
pub struct GroupDelivery {
    pub offset: BigInt,
    pub attempts: u32,
}

/// One change observed by a cache watch.
#[napi(object)]
pub struct CacheChange {
    pub key: String,
    /// Absent when the key was deleted or expired.
    pub value: Option<Buffer>,
    pub offset: BigInt,
    pub expires_at_millis: BigInt,
}

/// An item from a cache watch: a change, or notice that the watch fell behind.
#[napi(object)]
pub struct CacheWatchItem {
    pub change: Option<CacheChange>,
    /// Set when the watch lagged and the broker ended it. Re-watching with
    /// `start = resumeFrom` is gapless.
    pub lagged_resume_from: Option<BigInt>,
    /// Set when the watch's shard moved to another broker. The watch follows
    /// it there on its own and carries on where it left off.
    pub shard_moved: Option<ShardMoved>,
}

/// Where a shard went when it moved to another broker. Cache watches and
/// sharded subscriptions follow it on their own; this is a notice.
#[napi(object)]
pub struct ShardMoved {
    /// Where the old owner says to resume, when it could say.
    pub resume_from: Option<BigInt>,
    /// The broker taking the shard, when known.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: BigInt,
}

impl From<felix_client::ShardMoved> for ShardMoved {
    fn from(moved: felix_client::ShardMoved) -> Self {
        Self {
            resume_from: moved.resume_from.map(BigInt::from),
            node_id: moved.node_id,
            addr: moved.addr,
            generation: BigInt::from(moved.generation),
        }
    }
}

/// An item from a sharded subscription.
///
/// Exactly one of these is set. A lost shard does not affect the others: they
/// keep delivering while that one is re-established, and it resumes from its
/// own last offset so nothing is skipped. A moved shard is followed: its
/// records carry on from the new owner, or a loss comes next.
#[napi(object)]
pub struct ShardEvent {
    pub shard: u32,
    pub event: Option<Event>,
    pub lost_error: Option<String>,
    pub recovered: Option<bool>,
    pub shard_moved: Option<ShardMoved>,
}
