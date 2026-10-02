//! Which of a publisher's streams a publish goes to.
//!
//! Hashing by stream keeps every publish to one stream on one writer, which
//! is what keeps them in order. The hash is cached per stream so a hot
//! stream does not rehash on every publish.
//!
//! A publish whose shard is known goes on that shard's own stream first
//! (`shard_streams`), and only falls back to the hash when the client has no
//! room for another.
//!
//! The hash is seeded per client. Each worker's connection sits on one broker
//! listener, so a seed shared by every client in a process would send all of
//! them publishing one stream through the same listener.

use std::collections::VecDeque;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ahash::RandomState;
use anyhow::Result;
use hashbrown::HashMap;

use super::Publisher;
use super::writer::PublishWorker;

pub(super) const STREAM_SHARD_CACHE_CAPACITY: usize = 1024;

/// How a publisher spreads publishes across its streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishSharding {
    /// Rotate through the streams. Publishes to one stream can reach the
    /// broker out of order.
    RoundRobin,
    /// Pick the stream by hashing the stream's name, so each stream's
    /// publishes share one writer and stay in order. The default.
    HashStream,
}

impl PublishSharding {
    pub(crate) fn from_env() -> Option<Self> {
        let value = std::env::var("FELIX_PUB_SHARDING").ok()?;
        match value.as_str() {
            "rr" => Some(Self::RoundRobin),
            "hash_stream" => Some(Self::HashStream),
            _ => None,
        }
    }
}

/// The writer a publish goes to: one from the pool, or its shard's own.
pub(super) enum Selected<'a> {
    Pooled(&'a PublishWorker),
    Shard(Arc<PublishWorker>),
}

impl Deref for Selected<'_> {
    type Target = PublishWorker;

    fn deref(&self) -> &PublishWorker {
        match self {
            Self::Pooled(worker) => worker,
            Self::Shard(worker) => worker,
        }
    }
}

impl Publisher {
    /// The writer for a publish to `shard` of the stream, when the caller
    /// knows the shard: the shard's own stream when it has or can get one,
    /// else the pool.
    ///
    /// Round-robin skips the shard's stream. It asked for spreading over
    /// order, and a stream of its own would put every publish on one writer.
    pub(super) async fn route(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: Option<u32>,
    ) -> Result<Selected<'_>> {
        if self.inner.sharding == PublishSharding::HashStream
            && let (Some(shard), Some(streams)) = (shard, &self.inner.shard_streams)
            && let Some(worker) = streams.worker(tenant_id, namespace, stream, shard).await?
        {
            return Ok(Selected::Shard(worker));
        }
        self.select_worker(tenant_id, namespace, stream)
            .map(Selected::Pooled)
    }

    pub(super) fn select_worker(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<&PublishWorker> {
        let workers = &self.inner.workers;
        if workers.is_empty() {
            return Err(anyhow::anyhow!("publish pool is empty"));
        }
        let worker_count = workers.len();
        let index = match self.inner.sharding {
            PublishSharding::RoundRobin => {
                self.inner.rr.fetch_add(1, Ordering::Relaxed) % worker_count
            }
            PublishSharding::HashStream => {
                if let Ok(mut cache) = self.inner.stream_cache.try_lock() {
                    if let Some(index) = cache.get(StreamKeyRef::new(tenant_id, namespace, stream))
                    {
                        return live_from(workers, index);
                    }
                    let index = hash_stream_index(
                        &self.inner.stream_hasher,
                        tenant_id,
                        namespace,
                        stream,
                        worker_count,
                    );
                    cache.insert(StreamKey::new(tenant_id, namespace, stream), index);
                    return live_from(workers, index);
                }
                hash_stream_index(
                    &self.inner.stream_hasher,
                    tenant_id,
                    namespace,
                    stream,
                    worker_count,
                )
            }
        };
        live_from(workers, index)
    }
}

/// The first worker at or after `index` whose writer is still running.
///
/// A writer that hit a broken stream has failed what it held and exited, and
/// its queue is closed for good; left in the pool, every stream hashed to it
/// would fail forever. Probing forward keeps a stream on one live writer, so
/// its publishes stay in order, and moves it only once its writer is gone.
fn live_from(workers: &[PublishWorker], index: usize) -> Result<&PublishWorker> {
    let count = workers.len();
    (0..count)
        .map(|step| &workers[(index + step) % count])
        .find(|worker| !worker.tx.is_closed())
        .ok_or_else(|| anyhow::anyhow!("every publish stream on this client has failed"))
}

pub(super) struct StreamShardCache {
    capacity: usize,
    order: VecDeque<StreamKey>,
    entries: HashMap<StreamKey, usize, RandomState>,
}

impl StreamShardCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity.min(64)),
            entries: HashMap::with_capacity_and_hasher(capacity, RandomState::new()),
        }
    }

    pub(super) fn get(&self, key: StreamKeyRef<'_>) -> Option<usize> {
        self.entries.get(&key).copied()
    }

    pub(super) fn insert(&mut self, key: StreamKey, worker_index: usize) {
        if self.capacity == 0 || self.entries.contains_key(&key) {
            return;
        }
        if self.entries.len() == self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.entries.remove(&evicted);
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, worker_index);
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

impl StreamKey {
    pub(super) fn new(tenant_id: &str, namespace: &str, stream: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct StreamKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
}

impl<'a> StreamKeyRef<'a> {
    pub(super) fn new(tenant_id: &'a str, namespace: &'a str, stream: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            stream,
        }
    }
}

impl<'a> hashbrown::Equivalent<StreamKey> for StreamKeyRef<'a> {
    fn equivalent(&self, key: &StreamKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
    }
}

fn hash_stream_index(
    hasher: &RandomState,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    worker_count: usize,
) -> usize {
    (hasher.hash_one((tenant_id, namespace, stream)) as usize) % worker_count
}
