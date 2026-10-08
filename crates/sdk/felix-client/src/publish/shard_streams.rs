//! Publish streams that each carry one shard.
//!
//! Every publish has to name its shard for these to keep order: one shard
//! split between its own stream and the hashed pool would be on two writers.
//! A `ClusterClient` knows every shard; a plain `Client` learns a stream's
//! width before its first keyed publish to it (`widths`).
//!
//! The broker answers a pipelining stream's publishes in the order the stream
//! carried them, and stops reading the stream once its window is full. On a
//! stream shared by several shards, one shard stuck on a quorum wait holds
//! back answers the others have already committed, then stops the stream. So
//! a publish whose shard is known gets a stream for that shard alone, opened
//! on its first publish on the least-loaded of the pool's connections. That
//! also spreads one busy stream's shards over the connections, and so over
//! the broker's listeners. Past the cap, shards fall back to the hashed pool.
//!
//! A shard keeps its stream while the writer lives, so its publishes stay in
//! order. A writer that fails has failed everything it held, so the shard's
//! next publish can open a replacement without reordering anything.
//!
//! Streams are not closed when idle. Closing one safely would mean racing
//! publishes already headed for its queue, and an idle writer is a parked
//! task. The cap bounds what they cost.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use ahash::RandomState;
use anyhow::Result;
use hashbrown::HashMap;
use tokio::sync::OnceCell;

use super::writer::PublishWorker;

/// Opens and authenticates one publish stream and starts its writer.
pub(crate) type OpenWorker =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<PublishWorker>> + Send>> + Send + Sync>;

/// The per-shard publish streams of one client.
pub(crate) struct ShardStreams {
    cap: usize,
    open: OpenWorker,
    slots: RwLock<HashMap<ShardStreamKey, Arc<Slot>, RandomState>>,
    /// Set by `Publisher::finish`, so a finished client does not open new
    /// streams for shards whose writers it just ended.
    closed: AtomicBool,
}

/// One shard's stream. Empty until its first publish has opened it, or while
/// an open that failed waits for the next publish to try again.
#[derive(Default)]
struct Slot {
    worker: OnceCell<Arc<PublishWorker>>,
}

enum Lookup {
    Live(Arc<PublishWorker>),
    Opening(Arc<Slot>),
    /// No slot, or one whose writer has exited.
    Stale(Option<Arc<Slot>>),
}

impl ShardStreams {
    pub(crate) fn new(cap: usize, open: OpenWorker) -> Self {
        Self {
            cap,
            open,
            slots: RwLock::new(HashMap::with_hasher(RandomState::new())),
            closed: AtomicBool::new(false),
        }
    }

    /// The writer of the shard's own stream, opened if this is the shard's
    /// first publish. `None` once the cap is taken by other shards, or after
    /// [`Self::close`]: the caller then uses the pool.
    ///
    /// An open that fails fails this publish rather than sending it through
    /// the pool. The pool would put the shard on a second writer, and its
    /// publishes could then arrive out of order.
    pub(crate) async fn worker(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<Option<Arc<PublishWorker>>> {
        if self.cap == 0 {
            return Ok(None);
        }
        let key = ShardStreamKeyRef {
            tenant_id,
            namespace,
            stream,
            shard,
        };
        let slot = match self.lookup(&key) {
            Lookup::Live(worker) => return Ok(Some(worker)),
            Lookup::Opening(slot) => slot,
            Lookup::Stale(previous) => match self.claim(&key, previous) {
                Some(slot) => slot,
                None => return Ok(None),
            },
        };
        let worker = slot
            .worker
            .get_or_try_init(|| async { (self.open)().await.map(Arc::new) })
            .await?;
        Ok(Some(Arc::clone(worker)))
    }

    /// Stop opening streams, and return the writers that are open so they
    /// can be finished.
    pub(crate) fn close(&self) -> Vec<Arc<PublishWorker>> {
        self.closed.store(true, Ordering::Release);
        self.slots
            .read()
            .expect("shard streams")
            .values()
            .filter_map(|slot| slot.worker.get().cloned())
            .collect()
    }

    fn lookup(&self, key: &ShardStreamKeyRef<'_>) -> Lookup {
        let slots = self.slots.read().expect("shard streams");
        match slots.get(key) {
            None => Lookup::Stale(None),
            Some(slot) => match slot.worker.get() {
                None => Lookup::Opening(Arc::clone(slot)),
                Some(worker) if !worker.tx.is_closed() => Lookup::Live(Arc::clone(worker)),
                Some(_) => Lookup::Stale(Some(Arc::clone(slot))),
            },
        }
    }

    /// A slot for the shard: a fresh one in place of `previous`, whose
    /// writer has exited, or a new one if the cap allows. Whatever another
    /// caller put there first wins, so a shard never has two.
    fn claim(&self, key: &ShardStreamKeyRef<'_>, previous: Option<Arc<Slot>>) -> Option<Arc<Slot>> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let mut slots = self.slots.write().expect("shard streams");
        if let Some(current) = slots.get_mut(key) {
            if previous
                .as_ref()
                .is_some_and(|previous| Arc::ptr_eq(previous, current))
            {
                *current = Arc::default();
            }
            return Some(Arc::clone(current));
        }
        // Slots are never given back, so a shard turned away here stays on
        // the pool for good and is never on two writers at once.
        if slots.len() >= self.cap {
            return None;
        }
        let slot: Arc<Slot> = Arc::default();
        slots.insert(key.owned(), Arc::clone(&slot));
        Some(slot)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ShardStreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
}

/// Borrowed form of [`ShardStreamKey`], so a lookup does not allocate. Hashes
/// the same, field for field.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ShardStreamKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
    shard: u32,
}

impl ShardStreamKeyRef<'_> {
    fn owned(self) -> ShardStreamKey {
        ShardStreamKey {
            tenant_id: self.tenant_id.to_string(),
            namespace: self.namespace.to_string(),
            stream: self.stream.to_string(),
            shard: self.shard,
        }
    }
}

impl hashbrown::Equivalent<ShardStreamKey> for ShardStreamKeyRef<'_> {
    fn equivalent(&self, key: &ShardStreamKey) -> bool {
        self.shard == key.shard
            && self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
    }
}
