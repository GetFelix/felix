//! The replication engine as the broker sees it: what a write waits on before
//! it is acknowledged, and how far a reader may see.
//!
//! Callers move behind [`Engine`] one path at a time, with the protocol in
//! [`crate::quorum`] as its implementation ([`Hybrid`]). See
//! `docs/replication-engine.md`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

mod host;

pub use host::{EngineHost, Host, HybridHost};

use crate::ShardKey;
use crate::peer::PeerRequester;
use crate::quorum::{Access, CacheWrite, QuorumError, QuorumMarks, ShardServing, Unreadable};

/// What a durable write and a read ask of a shard's replication.
pub trait Engine: Send + Sync {
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
    /// `Quorum` streams. See [`crate::quorum::committed_bound`].
    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound;

    /// Confirm, after a read of `shard`'s state took its value, that this
    /// broker still leads `shard`. See [`crate::quorum::confirm_state_read`].
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
    /// taken) is held by a quorum. See [`crate::quorum::await_cache_quorum`].
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
    /// at `consistency`. See [`crate::quorum::await_readable`].
    fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), QuorumError>> + Send;
}

/// Felix's own replication protocol: the quorum marks the driver publishes,
/// read against how this broker serves each shard.
pub struct Hybrid<S: ?Sized> {
    marks: Option<Arc<QuorumMarks>>,
    serving: Option<Arc<S>>,
}

impl<S: ?Sized> Hybrid<S> {
    /// `None` for either outside a cluster, where nothing is replicated.
    pub fn new(marks: Option<Arc<QuorumMarks>>, serving: Option<Arc<S>>) -> Self {
        Self { marks, serving }
    }

    /// The quorum marks, for the waits that are not behind [`Engine`] yet.
    pub fn marks(&self) -> Option<&QuorumMarks> {
        self.marks.as_deref()
    }
}

impl<S: ?Sized> Default for Hybrid<S> {
    /// For tests only: an engine with no marks and no router, so a `Quorum`
    /// wait has nothing to wait on and returns at once. Production code builds
    /// it with [`Hybrid::new`].
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl<S: ShardServing + ?Sized> Engine for Hybrid<S> {
    async fn await_commit(
        &self,
        handle: &felix_broker::StreamHandle,
        shard: Option<&ShardKey>,
        outcome: &felix_broker::PublishOutcome,
        generation: u64,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        crate::quorum::await_quorum(
            handle,
            shard,
            outcome,
            generation,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
        )
        .await
    }

    fn read_bound(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        shard: &ShardKey,
    ) -> Result<Option<u64>, Unreadable> {
        crate::quorum::read_bound(
            consistency,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
    }

    fn committed_bound(&self, shard: &ShardKey) -> felix_broker::ReadBound {
        match (self.marks.as_deref(), self.serving.as_deref()) {
            (Some(marks), Some(serving)) => crate::quorum::committed_bound(shard, marks, serving),
            _ => felix_broker::ReadBound::Unbounded,
        }
    }

    async fn confirm_read(&self, shard: &ShardKey) -> anyhow::Result<()> {
        crate::quorum::confirm_state_read(shard, self.marks.as_deref(), self.serving.as_deref())
            .await
    }

    async fn cache_write_at(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        generation: u64,
    ) -> Option<CacheWrite> {
        crate::quorum::cache_write_at(
            broker,
            shard,
            generation,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
        .await
    }

    async fn await_cache_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        access: Access,
        written: Option<CacheWrite>,
    ) -> anyhow::Result<()> {
        crate::quorum::await_cache_quorum(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
            access,
            written,
        )
        .await
    }

    async fn await_counter_quorum(
        &self,
        broker: &felix_broker::Broker,
        shard: &ShardKey,
        timeout: Duration,
        end: Option<u64>,
        generation: u64,
        what: &'static str,
    ) -> anyhow::Result<()> {
        crate::quorum::await_counter_quorum(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
            timeout,
            end,
            generation,
            what,
        )
        .await
    }

    async fn read_skips_lease(&self, broker: &felix_broker::Broker, shard: &ShardKey) -> bool {
        crate::quorum::read_skips_lease(
            broker,
            shard,
            self.marks.as_deref(),
            self.serving.as_deref(),
        )
        .await
    }

    async fn await_readable(
        &self,
        consistency: Option<felix_broker::ConsistencyLevel>,
        key: &ShardKey,
        end: u64,
        timeout: Duration,
    ) -> Result<(), QuorumError> {
        crate::quorum::await_readable(
            consistency,
            key,
            self.marks.as_deref(),
            self.serving.as_deref(),
            end,
            timeout,
        )
        .await
    }
}

/// [`Engine::committed_bound`] as the broker core asks for it
/// (`felix_broker::ReadBounds`).
pub struct CommittedReads<E> {
    engine: E,
}

impl<E> CommittedReads<E> {
    pub fn new(engine: E) -> Self {
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
}

/// Let `Quorum` reads on `marks` confirm leadership by a round of fences sent
/// through `requester`, each round waiting up to `round_timeout` for a
/// majority, unless `by_lease` keeps them on the lease. Only the first call
/// counts. See [`crate::leadership::ReadIndex`].
pub fn bind_read_rounds<R: PeerRequester + Send + Sync + 'static>(
    marks: &QuorumMarks,
    requester: R,
    broker: Arc<felix_broker::Broker>,
    router: Arc<felix_router::ShardRouter>,
    round_timeout: Duration,
    by_lease: bool,
) {
    marks.set_read_check(
        Arc::new(crate::leadership::ReadIndex::new(
            requester,
            broker,
            router,
            round_timeout,
        )),
        by_lease,
    );
}

#[cfg(test)]
mod tests;
