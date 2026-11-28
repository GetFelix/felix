//! The replication engine as the broker sees it: what a write waits on before
//! it is acknowledged, and how far a reader may see. The Raft engine
//! (`crate::raft`) implements it. See `docs/replication-engine.md`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

mod host;

#[cfg(feature = "raft")]
pub(crate) use host::EngineHost;
#[cfg(feature = "raft")]
pub(crate) use host::Host;
pub use host::WriteFence;

use crate::ShardKey;
use felix_wire::internal::{InternalMessage, PeerCapabilities};

/// Why the engine cannot say a write is committed. Unless the variant says
/// otherwise the leader may have written it, so the client is told the
/// outcome is unknown rather than that the write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// No majority within the budget. This broker may not have written it.
    #[error("the {what} did not reach a majority within {timeout:?}")]
    TimedOut {
        what: &'static str,
        timeout: Duration,
    },
    /// This broker stopped leading the shard before a majority held the write.
    #[error("{detail} before the {what} could reach a quorum")]
    LeadershipLost {
        what: &'static str,
        detail: &'static str,
    },
    /// The Raft engine refused the write before it appended any of it: its
    /// lease had lapsed, or it no longer led the term the write was admitted
    /// at. Nothing was written, so the write can be sent again.
    #[error("the {what} was refused: {detail}")]
    NotAppended {
        what: &'static str,
        detail: &'static str,
    },
    /// A conditional publish on a Raft shard did not land at its expected
    /// offset. Unlike the others this outcome is known: the committed entry
    /// there is a blank, and `tail` is the shard's next offset after it.
    #[error("the shard's next offset was not the expected one; it is now {tail}")]
    OffsetMismatch { tail: u64 },
    /// A record larger than a Raft shard's appends can carry, refused before
    /// anything was proposed. Sending it again cannot succeed.
    #[error(
        "a record of {bytes} bytes is larger than a Raft shard takes ({max}); nothing was written"
    )]
    TooLarge { bytes: usize, max: usize },
}

/// Why [`Engine::read_bound`] has no bound for a `Quorum` shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreadable {
    /// This broker does not serve the shard.
    NotServed,
    /// This broker serves the shard but has no committed position for it yet.
    Settling,
}

/// Whether a cache access is a write or a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Write,
    Read,
}

/// Where a cache write landed: the generation it was taken at and the cache
/// log's tail before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheWrite {
    pub generation: u64,
    pub from: u64,
}

/// Where a write landed: the offset one past its last record, and the
/// generation it was written at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub end: u64,
    pub generation: u64,
}

impl Written {
    /// Where the batch in `outcome`, written at `generation`, landed. `None`
    /// for an ephemeral stream, which has no offsets.
    pub fn of(outcome: &felix_broker::PublishOutcome, generation: u64) -> Option<Self> {
        let (_, last) = outcome.offsets?;
        Some(Self {
            end: last + 1,
            generation,
        })
    }
}

/// The brokers the control plane placed a shard on, by node id.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Placement {
    pub nodes: Vec<String>,
}

impl Placement {
    /// A placement on `nodes`.
    pub fn new(nodes: Vec<String>) -> Self {
        Self { nodes }
    }
}

/// What a durable write and a read ask of a shard's replication.
pub trait Engine: Send + Sync {
    /// Records of `shard` up to `written.end` are durable on this leader's
    /// log, in the broker's commit order: get them to the other copies. Call
    /// it after the batch is durable. It never waits.
    fn appended(&self, shard: &ShardKey, written: Written);

    /// Wait until the batch in `outcome`, written at `generation`, is
    /// committed: held by enough of the shard's replicas that no later leader
    /// can lose it. Returns at once for a stream that does not acknowledge by
    /// its replicas, or when `shard` is `None` (no replica set).
    ///
    /// An error means this broker cannot say the batch is committed, not that
    /// it is lost.
    fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Whether a write to `shard` at `generation` may be acknowledged on
    /// local durability alone, as a `Leader` stream's is. On an error the
    /// caller waits on [`Engine::await_commit`] instead. A refusal never
    /// closes the shard.
    fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError>;

    /// The end of what a reader of `shard` may see, or `None` when its reads
    /// are not bounded by replication.
    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable>;

    /// How far the broker core's readers of stream `shard` (subscribe fanout,
    /// cursor replay) may see. Unlike [`Engine::read_bound`] it also refuses
    /// a broker whose lease no longer lets it serve the shard. Asked only for
    /// `Quorum` streams.
    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound;

    /// `end`, a bound of `shard`, less the offsets just below it that hold
    /// no record. See `felix_broker::ReadBounds::records_end`.
    fn records_end(&self, _shard: &ShardKey, end: u64) -> u64 {
        end
    }

    /// Confirm, after a read of `shard`'s state took its value, that this
    /// broker still leads `shard`.
    fn confirm_read(&self, shard: &ShardKey) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Where a cache write landed, for [`Engine::await_cache_quorum`]. Call it
    /// inside the write's fence, right before the write.
    fn cache_write_at(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        generation: u64,
    ) -> impl Future<Output = Option<CacheWrite>> + Send;

    /// Wait until a cache write at `written` (or, for a read, the value just
    /// taken) is held by a quorum.
    fn await_cache_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        access: Access,
        written: Option<CacheWrite>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// As [`Engine::await_cache_quorum`] for a counter on `shard`'s cache.
    /// `end` is the counter log offset after the write, `None` for a read.
    fn await_counter_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        end: Option<u64>,
        generation: u64,
        what: &'static str,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Whether a read of this cache or counter may skip the lease check it
    /// would otherwise make before taking its value.
    fn read_skips_lease(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
    ) -> impl Future<Output = bool> + Send;

    /// Wait until the records of `key` up to `end` are safe to show a reader
    /// at `consistency`.
    fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send;

    /// The control plane placed `shard` on `placement`. Safe to repeat.
    fn placement_changed(&self, shard: &ShardKey, placement: &Placement);

    /// Answer a peer's replication request, follower side: a batch to store,
    /// a bootstrap or rebuild, a fence, a fetch or a pre-vote. `from` is the
    /// peer's node id when it named itself, `caller` what it advertised.
    /// Anything else is refused as malformed.
    fn handle(
        &self,
        from: Option<&str>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> impl Future<Output = InternalMessage> + Send;
}

/// [`Engine::committed_bound`] as the broker core asks for it
/// (`felix_broker::ReadBounds`).
pub struct CommittedReads<E> {
    engine: Arc<E>,
}

impl<E> CommittedReads<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }
}

impl<E> std::fmt::Debug for CommittedReads<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommittedReads").finish_non_exhaustive()
    }
}

impl<E: Engine> felix_broker::ReadBounds for CommittedReads<E> {
    fn stream_bound(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> felix_broker::ReadBound {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: crate::ShardKind::Stream,
        };
        self.engine.committed_bound(&key)
    }

    fn records_end(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        end: u64,
    ) -> u64 {
        let key = ShardKey {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            kind: crate::ShardKind::Stream,
        };
        self.engine.records_end(&key, end)
    }
}

#[cfg(test)]
mod tests;
