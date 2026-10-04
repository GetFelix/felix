//! The [`Broker`]: every stream and cache this process serves.
//!
//! This file holds the struct, its construction and its accessors. The
//! behaviour is split by what it is about: `registry` administers tenants,
//! namespaces, streams and caches; `shards` resolves a stream shard to its
//! state; `publish` and `subscribe` are the data path; `shard_logs` is what
//! replication needs.

mod backup;
mod commit;
mod keys;
mod metadata;
mod publish;
mod registry;
mod shard_logs;
mod shards;
mod subscribe;

pub use backup::CommittedOffsets;
pub use commit::StateRead;
pub use metadata::{CacheMetadata, ConsistencyLevel, StreamMetadata};
pub use publish::{
    ClaimedPublish, IdempotentClaim, IdempotentOutcome, PublishOutcome, RECORD_SEQUENCE_WRAP,
    SequenceReuse,
};
pub use shard_logs::LogKind;
pub use shards::StreamHandle;
pub use subscribe::{Cursor, HistoryRange, JoinOffsets, ResumedSubscription, RingRecord};

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use ahash::RandomState;
use felix_storage::StorageApi;
use hashbrown::HashMap;
use tokio::sync::RwLock;

use crate::cache::CacheWatchHub;
use crate::durable::DurableStorage;
use crate::error::{BrokerError, Result};
use crate::queue::{ConsumerGroups, DeadLetters, GroupReader};
use crate::stream::{StreamState, SubQueuePolicy};
use keys::{CacheKey, NamespaceKey, StreamKey, TopicKey};

// Capacity defaults for streams this broker creates.
const DEFAULT_TOPIC_CAPACITY: usize = 1024;
const DEFAULT_LOG_CAPACITY: usize = 1024;
const DEFAULT_SUB_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;

/// In-process broker for pub/sub messaging.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_storage::EphemeralCache;
///
/// let broker = Broker::new(EphemeralCache::new().into());
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     broker
///         .register_tenant("t1")
///         .await
///         .expect("tenant");
///     broker
///         .register_namespace("t1", "default")
///         .await
///         .expect("namespace");
///     broker
///         .register_stream("t1", "default", "topic", Default::default())
///         .await
///         .expect("register");
///     // Shard 0: a stream with one shard has only that one, and `publish`
///     // below resolves to the same.
///     let mut sub = broker
///         .subscribe("t1", "default", "topic", 0)
///         .await
///         .expect("subscribe");
///     broker
///         .publish("t1", "default", "topic", Bytes::from_static(b"hello"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"hello"));
/// });
/// ```
#[derive(Debug)]
pub struct Broker {
    // Map of stream key -> stream state (subscriber registry + log).
    topics: RwLock<HashMap<TopicKey, Arc<StreamState>, RandomState>>,
    // Map of stream key -> metadata for existence checks.
    streams: RwLock<HashMap<StreamKey, StreamMetadata, RandomState>>,
    // Map of cache key -> metadata for existence checks.
    caches: RwLock<HashMap<CacheKey, CacheMetadata, RandomState>>,
    // Map of tenant id -> active marker.
    tenants: RwLock<HashMap<String, (), RandomState>>,
    // Map of namespace key -> active marker.
    namespaces: RwLock<HashMap<NamespaceKey, (), RandomState>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: Box<dyn StorageApi + Send>,
    // Per-subscriber queue capacity for each stream.
    topic_capacity: usize,
    // Per-topic in-memory log capacity.
    log_capacity: usize,
    // Subscriber queue backpressure policy.
    subscriber_queue_policy: SubQueuePolicy,
    next_stream_handle: AtomicU64,
    // Disk-backed storage for streams registered with `durable: true`. `None`
    // means the broker is in-memory only and durable streams are rejected at
    // registration rather than silently downgraded.
    durable_storage: Option<DurableStorage>,
    /// Serves consumer groups, when this broker keeps their positions.
    group_reader: Option<Arc<GroupReader>>,
    /// Consumer-group positions, when this broker has somewhere to keep them.
    ///
    /// `None` without durable storage, and deliberately not faked in memory: a
    /// group whose position is lost on restart redelivers everything it had
    /// already processed, which is worse than refusing to run a queue at all.
    consumer_groups: Option<Arc<ConsumerGroups>>,
    /// Counters, when this broker has somewhere to write their log.
    ///
    /// `None` without durable storage, and deliberately not faked in memory:
    /// a sum that any restart resets is worse than refusing to count at all —
    /// the same argument consumer groups make about their cursors.
    counters: Option<Arc<felix_storage::CounterStore>>,
    /// Fanout for cache watches, when the cache store can observe its writes.
    ///
    /// `None` for a store with no log: a watch's contract is built on log
    /// offsets, so offering one over an ephemeral cache would promise a resume
    /// anchor that does not exist.
    cache_watches: Option<Arc<CacheWatchHub>>,
    /// Signalled after every durable append, so replication can ship without
    /// waiting for its next tick.
    ///
    /// A `Notify` rather than a channel: a waiter only needs to know that
    /// *something* landed, and coalescing a burst into one wake-up is the
    /// behaviour wanted rather than a queue of them to drain.
    appended: Arc<tokio::sync::Notify>,
    /// How far readers of a `Quorum` stream may see, once the cluster side
    /// says. Shared with every stream state, which asks it per shard.
    read_bounds: Arc<std::sync::OnceLock<Arc<dyn crate::stream::ReadBounds>>>,
    /// Seeds producer ids; randomly keyed at construction.
    producer_ids: ahash::RandomState,
    producer_id_counter: std::sync::atomic::AtomicU64,
    /// Whether a durable stream stores each record's publisher, once the
    /// node says. Unset stores none.
    record_publishers: std::sync::OnceLock<PublisherGate>,
}

/// Decides whether durable streams store publishers; see
/// [`Broker::record_publishers_when`].
struct PublisherGate(Arc<dyn Fn() -> bool + Send + Sync>);

impl std::fmt::Debug for PublisherGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublisherGate")
    }
}

// `Broker` is `Send + Sync` from its fields alone: every field is an `RwLock`,
// an atomic, a `usize`, `Box<dyn StorageApi + Send>`, or a `DurableStorage`
// (itself an `Arc` over `Send + Sync` state) — and `StorageApi`
// already requires `Send + Sync`. The compiler's auto-impls cover this, so no
// `unsafe impl` is needed. This assertion fails the build if a future field
// breaks the property instead of letting it be papered over.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Broker>();
};

impl Broker {
    /// A broker with no tenants yet, serving caches from `cache`.
    pub fn new(cache: Box<dyn StorageApi + Send>) -> Self {
        // Offered to the store unconditionally; the store's answer is the
        // truth about whether watches can be served over it.
        let hub = CacheWatchHub::new();
        let cache_watches = cache
            .set_change_observer(Arc::clone(&hub) as Arc<dyn felix_storage::CacheObserver>)
            .then_some(hub);
        Self {
            topics: RwLock::new(HashMap::with_hasher(RandomState::new())),
            streams: RwLock::new(HashMap::with_hasher(RandomState::new())),
            caches: RwLock::new(HashMap::with_hasher(RandomState::new())),
            tenants: RwLock::new(HashMap::with_hasher(RandomState::new())),
            namespaces: RwLock::new(HashMap::with_hasher(RandomState::new())),
            cache,
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            log_capacity: DEFAULT_LOG_CAPACITY,
            subscriber_queue_policy: DEFAULT_SUB_QUEUE_POLICY,
            next_stream_handle: AtomicU64::new(1),
            durable_storage: None,
            consumer_groups: None,
            group_reader: None,
            counters: None,
            cache_watches,
            appended: Arc::new(tokio::sync::Notify::new()),
            read_bounds: Arc::default(),
            producer_ids: ahash::RandomState::new(),
            producer_id_counter: std::sync::atomic::AtomicU64::new(0),
            record_publishers: std::sync::OnceLock::new(),
        }
    }

    /// Store each durable record's publisher whenever `gate` says so.
    ///
    /// Asked on every durable publish. A record with a publisher needs
    /// storage format v6, which an older build refuses to open, so the node
    /// turns this on only once that is acceptable: in a cluster when the
    /// fleet has finalized `publisher_principal`. Only the first call counts.
    pub fn record_publishers_when(&self, gate: impl Fn() -> bool + Send + Sync + 'static) {
        let _ = self.record_publishers.set(PublisherGate(Arc::new(gate)));
    }

    /// Whether a durable publish now stores its publisher.
    pub fn records_publishers(&self) -> bool {
        self.record_publishers.get().is_some_and(|gate| (gate.0)())
    }

    /// Per-subscriber queue capacity. Zero is rejected.
    pub fn with_topic_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        // Keep a single capacity value so new topics match existing ones.
        self.topic_capacity = capacity;
        Ok(self)
    }

    /// How many records each stream's replay ring keeps. Zero is rejected.
    pub fn with_log_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        self.log_capacity = capacity;
        Ok(self)
    }

    /// What a publish does when a subscriber's queue is full.
    pub fn with_subscriber_queue_policy(mut self, policy: SubQueuePolicy) -> Self {
        self.subscriber_queue_policy = policy;
        self
    }

    /// Attach disk-backed storage, enabling streams registered as durable.
    ///
    /// Without this, registering a durable stream fails rather than quietly
    /// producing an in-memory stream that claims a guarantee it cannot keep.
    pub fn with_durable_storage(mut self, storage: DurableStorage) -> Self {
        self.durable_storage = Some(storage);
        self
    }

    /// Where consumer groups keep their positions, and how long a claim stands.
    pub fn with_consumer_groups(
        mut self,
        groups: Arc<ConsumerGroups>,
        dead_letters: Arc<DeadLetters>,
        visibility: std::time::Duration,
        max_attempts: u32,
    ) -> Self {
        self.group_reader = Some(Arc::new(GroupReader::new(
            Arc::clone(&groups),
            dead_letters,
            visibility,
            max_attempts,
        )));
        self.consumer_groups = Some(groups);
        self
    }

    /// Where counters keep their logs.
    pub fn with_counters(mut self, counters: Arc<felix_storage::CounterStore>) -> Self {
        self.counters = Some(counters);
        self
    }

    /// Bound what readers of `Quorum` streams see to what `bounds` says is
    /// committed. Set once, when the cluster side starts; a second call is
    /// ignored. Until it is set every stream is unbounded, which is right for
    /// a broker with no replicas.
    pub fn set_read_bounds(&self, bounds: Arc<dyn crate::stream::ReadBounds>) {
        let _ = self.read_bounds.set(bounds);
    }

    /// Tie a new stream state to the broker's read bounds.
    pub(crate) fn bind_reads(&self, state: &StreamState, topic: &TopicKey) {
        state.held.bind(crate::stream::ReadSource {
            bounds: Arc::clone(&self.read_bounds),
            tenant_id: topic.tenant_id.clone(),
            namespace: topic.namespace.clone(),
            stream: topic.stream.clone(),
            shard: topic.shard,
            log_capacity: self.log_capacity,
        });
    }

    /// The cache store.
    pub fn cache(&self) -> &(dyn StorageApi + Send) {
        self.cache.as_ref()
    }

    /// Durable storage, if this broker has any.
    pub fn durable_storage(&self) -> Option<&DurableStorage> {
        self.durable_storage.as_ref()
    }

    /// Serves consumer groups, if this broker can.
    pub fn group_reader(&self) -> Option<&Arc<GroupReader>> {
        self.group_reader.as_ref()
    }

    /// Consumer-group positions, if this broker keeps any.
    pub fn consumer_groups(&self) -> Option<&Arc<ConsumerGroups>> {
        self.consumer_groups.as_ref()
    }

    /// The counter store, if this broker can count.
    pub fn counters(&self) -> Option<&Arc<felix_storage::CounterStore>> {
        self.counters.as_ref()
    }

    /// Fanout for cache watches, if this broker's cache store can serve them.
    pub fn cache_watches(&self) -> Option<&Arc<CacheWatchHub>> {
        self.cache_watches.as_ref()
    }
}

#[cfg(test)]
mod tests;
