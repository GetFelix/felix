//! The decoded internal message, and the shard reference most of them carry.

use super::{
    Fence, FenceOk, ForwardCacheError, ForwardCacheOk, ForwardCacheOp, ForwardPublish,
    ForwardPublishError, ForwardPublishOk, Hello, HelloOk, Kind, NotLeader, ReplicateBootstrap,
    ReplicateError, ReplicateFetch, ReplicateOk, ReplicateRebuild, ReplicateRecords,
};

/// A decoded internal message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalMessage {
    ForwardPublish(ForwardPublish),
    ForwardPublishOk(ForwardPublishOk),
    ForwardPublishError(ForwardPublishError),
    NotLeader(NotLeader),
    Hello(Hello),
    HelloOk(HelloOk),
    ReplicateRecords(ReplicateRecords),
    ReplicateOk(ReplicateOk),
    ReplicateError(ReplicateError),
    ReplicateBootstrap(ReplicateBootstrap),
    ForwardCacheOp(ForwardCacheOp),
    ForwardCacheOk(ForwardCacheOk),
    ForwardCacheError(ForwardCacheError),
    /// The same body as [`ReplicateRecords`], for a shard that belongs to a
    /// cache rather than a stream.
    ///
    /// A separate kind rather than a field on `ShardRef`, because the protocol
    /// evolves by adding kinds: widening an existing body would need a version
    /// bump, and a version bump makes a rolling upgrade impossible. An old peer
    /// answers this with "unknown kind", which is a typed refusal — the leader
    /// stops replicating that shard and says so, rather than shipping a cache's
    /// records into a stream's log.
    ReplicateCacheRecords(ReplicateRecords),
    ReplicateCacheBootstrap(ReplicateBootstrap),
    /// The same body again, for a stream shard's consumer-group cursors.
    ///
    /// A third kind rather than a field, for the reason the cache pair exists:
    /// the bodies are identical, and a follower that guessed wrong would write
    /// one shard's log into another's.
    ReplicateGroupRecords(ReplicateRecords),
    ReplicateGroupBootstrap(ReplicateBootstrap),
    /// The same body once more, for a stream shard's dead-letter list.
    ///
    /// A fourth kind for the reason the group pair exists: the bodies are
    /// identical, and the kind is what stops a follower writing the offsets a
    /// group gave up on into the cursors that say where it resumes.
    ReplicateDeadLetterRecords(ReplicateRecords),
    ReplicateDeadLetterBootstrap(ReplicateBootstrap),
    /// The same body once more, for a cache shard's counter log — the kind is
    /// what stops a follower folding counter deltas into the cache of the
    /// same name.
    ReplicateCounterRecords(ReplicateRecords),
    ReplicateCounterBootstrap(ReplicateBootstrap),
    ReplicateRebuild(ReplicateRebuild),
    /// A stream shard's records with their producer marks. The body is
    /// `ReplicateRecords` followed by one mark per record.
    ReplicateMarkedRecords(ReplicateRecords),
    Fence(Fence),
    FenceOk(FenceOk),
    ReplicateFetch(ReplicateFetch),
}

/// The record kinds, whichever log they are for, send as
/// `ReplicateLabelledRecords` once they carry generations, and otherwise as
/// `ReplicateCommittedRecords` once they carry a commit offset.
fn records_kind(records: &ReplicateRecords, own: Kind) -> Kind {
    if records.generations.is_some() {
        Kind::ReplicateLabelledRecords
    } else if records.commit_offset.is_some() {
        Kind::ReplicateCommittedRecords
    } else {
        own
    }
}

impl InternalMessage {
    /// The kind this message is sent as.
    pub fn kind(&self) -> Kind {
        match self {
            // The credential decides the kind, so a forwarder that has one
            // always sends the kind an owner will check it on.
            Self::ForwardPublish(m) if m.credential.is_empty() => Kind::ForwardPublish,
            Self::ForwardPublish(_) => Kind::AuthorizedForwardPublish,
            Self::ForwardPublishOk(_) => Kind::ForwardPublishOk,
            Self::ForwardPublishError(_) => Kind::ForwardPublishError,
            Self::NotLeader(_) => Kind::NotLeader,
            Self::Hello(m) if m.capabilities.is_none() => Kind::Hello,
            Self::Hello(_) => Kind::HelloCapable,
            Self::HelloOk(m) if m.capabilities.is_none() => Kind::HelloOk,
            Self::HelloOk(_) => Kind::HelloCapableOk,
            Self::ReplicateRecords(m) => records_kind(m, Kind::ReplicateRecords),
            Self::ReplicateOk(_) => Kind::ReplicateOk,
            Self::ReplicateError(_) => Kind::ReplicateError,
            Self::ReplicateBootstrap(_) => Kind::ReplicateBootstrap,
            Self::ForwardCacheOp(m) if m.credential.is_empty() => Kind::ForwardCacheOp,
            Self::ForwardCacheOp(_) => Kind::AuthorizedForwardCacheOp,
            Self::ForwardCacheOk(_) => Kind::ForwardCacheOk,
            Self::ForwardCacheError(_) => Kind::ForwardCacheError,
            Self::ReplicateCacheRecords(m) => records_kind(m, Kind::ReplicateCacheRecords),
            Self::ReplicateCacheBootstrap(_) => Kind::ReplicateCacheBootstrap,
            Self::ReplicateGroupRecords(m) => records_kind(m, Kind::ReplicateGroupRecords),
            Self::ReplicateGroupBootstrap(_) => Kind::ReplicateGroupBootstrap,
            Self::ReplicateDeadLetterRecords(m) => {
                records_kind(m, Kind::ReplicateDeadLetterRecords)
            }
            Self::ReplicateDeadLetterBootstrap(_) => Kind::ReplicateDeadLetterBootstrap,
            Self::ReplicateCounterRecords(m) => records_kind(m, Kind::ReplicateCounterRecords),
            Self::ReplicateCounterBootstrap(_) => Kind::ReplicateCounterBootstrap,
            Self::ReplicateRebuild(_) => Kind::ReplicateRebuild,
            Self::ReplicateMarkedRecords(m) => records_kind(m, Kind::ReplicateMarkedRecords),
            Self::Fence(_) => Kind::Fence,
            Self::FenceOk(_) => Kind::FenceOk,
            Self::ReplicateFetch(m) if m.labelled => Kind::ReplicateLabelledFetch,
            Self::ReplicateFetch(_) => Kind::ReplicateFetch,
        }
    }

    /// The id this message answers, or carries if it is a request.
    ///
    /// Every message has one, which is what makes the request lifecycle
    /// unambiguous: a response can always be matched or discarded, never left
    /// pending.
    pub fn correlation_id(&self) -> u64 {
        match self {
            Self::ForwardPublish(m) => m.correlation_id,
            Self::ForwardPublishOk(m) => m.correlation_id,
            Self::ForwardPublishError(m) => m.correlation_id,
            Self::NotLeader(m) => m.correlation_id,
            Self::Hello(m) => m.correlation_id,
            Self::HelloOk(m) => m.correlation_id,
            Self::ReplicateRecords(m) => m.correlation_id,
            Self::ReplicateOk(m) => m.correlation_id,
            Self::ReplicateError(m) => m.correlation_id,
            Self::ReplicateBootstrap(m) => m.correlation_id,
            Self::ForwardCacheOp(m) => m.correlation_id,
            Self::ForwardCacheOk(m) => m.correlation_id,
            Self::ForwardCacheError(m) => m.correlation_id,
            Self::ReplicateCacheRecords(m) => m.correlation_id,
            Self::ReplicateCacheBootstrap(m) => m.correlation_id,
            Self::ReplicateGroupRecords(m) => m.correlation_id,
            Self::ReplicateGroupBootstrap(m) => m.correlation_id,
            Self::ReplicateDeadLetterRecords(m) => m.correlation_id,
            Self::ReplicateDeadLetterBootstrap(m) => m.correlation_id,
            Self::ReplicateCounterRecords(m) => m.correlation_id,
            Self::ReplicateCounterBootstrap(m) => m.correlation_id,
            Self::ReplicateRebuild(m) => m.correlation_id,
            Self::ReplicateMarkedRecords(m) => m.correlation_id,
            Self::Fence(m) => m.correlation_id,
            Self::FenceOk(m) => m.correlation_id,
            Self::ReplicateFetch(m) => m.correlation_id,
        }
    }
}

/// Which shard a forwarded request is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRef {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    /// The assignment generation the requester resolved against.
    ///
    /// The owner compares this with its own. Equal proceeds; either mismatch is
    /// an explicit typed answer, and never a successful ownership claim.
    pub generation: u64,
}
