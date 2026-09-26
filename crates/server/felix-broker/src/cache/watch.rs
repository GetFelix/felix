//! Fanout for cache watches: bounded per-watcher queues over a cache shard's
//! write order.
//!
//! The hub is the cache's [`felix_storage::CacheObserver`]: the store reports
//! each applied write while holding the shard's write lock, and the hub fans it
//! out to every registered watcher whose filter matches — the same
//! publisher-never-blocks discipline stream fanout uses, with `try_send`
//! against bounded queues.
//!
//! What differs from stream fanout is what a drop means. Filtering makes
//! offsets sparse — other keys' writes consume them — so a watcher cannot read
//! a queue drop out of an offset jump the way a stream subscriber can. A
//! watcher whose queue is full is therefore *ended*, not quietly thinned: the
//! hub records the offset of the first change it could not deliver and closes
//! the queue, and the delivery path tells the client to re-watch from that
//! offset. Loss is loud, and the recovery is gapless.

use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::handoff::ShardMoved;

/// Sentinel for "not lagged" in [`WatcherState::lagged_at`]. No real offset can
/// collide with it: a log would have to hold 2^64 records first.
const NOT_LAGGED: u64 = u64::MAX;

/// Lock stripes over the cache shards. Every cache write reports here with
/// its store's shard lock held, so one broker-wide lock would serialise
/// writes to unrelated caches.
const STRIPES: usize = 16;

/// Fanout registry for every cache watch this broker serves.
#[derive(Debug)]
pub struct CacheWatchHub {
    stripes: Box<[Mutex<HashMap<WatchShardKey, ShardWatchers>>]>,
    /// Picks a key's stripe.
    hasher: hashbrown::DefaultHashBuilder,
    /// Watchers registered anywhere. A broker with none skips the stripes.
    watching: AtomicUsize,
    next_id: AtomicU64,
}

impl Default for CacheWatchHub {
    fn default() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Mutex::default()).collect(),
            hasher: hashbrown::DefaultHashBuilder::default(),
            watching: AtomicUsize::new(0),
            next_id: AtomicU64::new(0),
        }
    }
}

impl CacheWatchHub {
    /// An empty hub. Shared, because each watch guard holds a weak reference
    /// back to it.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a watcher and hand back its queue.
    ///
    /// Registration is a point in the shard's write order: every change applied
    /// after it is either delivered on this queue or reported as the lag
    /// offset. The caller pins the live edge by registering *before* reading
    /// any history or snapshot — the same register-before-read discipline the
    /// stream resume path enforces — and drops duplicates by offset.
    pub fn register(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        filter: CacheWatchFilter,
        queue_capacity: usize,
    ) -> CacheWatchSubscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        let state = Arc::new(WatcherState {
            lagged_at: AtomicU64::new(NOT_LAGGED),
            moved: OnceLock::new(),
        });
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        let exact = match &filter {
            CacheWatchFilter::Key(key) => Some(key.clone()),
            CacheWatchFilter::Prefix(_) => None,
        };
        // Counted before it is visible, so a write that skips the stripes on
        // a zero count is one that no registered watcher could have seen.
        self.watching.fetch_add(1, Ordering::AcqRel);
        self.stripe(&key)
            .lock()
            .entry(key.clone())
            .or_default()
            .insert(Watcher {
                id,
                filter,
                sender,
                state: Arc::clone(&state),
            });
        CacheWatchSubscription {
            receiver,
            state,
            guard: CacheWatchGuard {
                hub: Arc::downgrade(self),
                key,
                exact,
                id,
            },
        }
    }

    fn stripe(&self, key: &WatchShardKey) -> &Mutex<HashMap<WatchShardKey, ShardWatchers>> {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        let hash = std::hash::Hasher::finish(&hasher);
        &self.stripes[hash as usize % self.stripes.len()]
    }

    /// Watcher slots currently registered for one cache shard, for tests that
    /// must prove a closed watch left nothing behind.
    pub fn registered_watchers(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> usize {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        self.stripe(&key)
            .lock()
            .get(&key)
            .map_or(0, ShardWatchers::len)
    }

    /// End every watch on one cache shard. Each watcher is handed what was
    /// already queued and then sees its queue close, which its delivery path
    /// answers by finishing the stream. Returns how many ended.
    ///
    /// For a shard that has moved to another broker: call it once writes here
    /// have stopped, so no change is applied after the watchers are gone.
    ///
    /// With `moved`, each watch also records that its shard moved and where to
    /// resume, which [`CacheWatchSubscription::moved`] reports once drained.
    pub fn end_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        moved: Option<ShardMoved>,
    ) -> usize {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        let Some(watchers) = self.stripe(&key).lock().remove(&key) else {
            return 0;
        };
        let ended = watchers.len();
        self.watching.fetch_sub(ended, Ordering::AcqRel);
        if let Some(moved) = moved {
            for watcher in watchers.iter() {
                let _ = watcher.state.moved.set(moved.clone());
            }
        }
        ended
    }

    fn remove(&self, key: &WatchShardKey, exact: Option<&str>, id: u64) {
        let mut shards = self.stripe(key).lock();
        if let Some(watchers) = shards.get_mut(key) {
            if watchers.remove(exact, id) {
                self.watching.fetch_sub(1, Ordering::AcqRel);
            }
            if watchers.is_empty() {
                shards.remove(key);
            }
        }
    }
}

impl felix_storage::CacheObserver for CacheWatchHub {
    /// Called with the shard's write lock held, so it must not block: every
    /// enqueue is a `try_send`, and a full queue ends that watcher rather than
    /// stalling the writer.
    fn cache_changed(&self, change: felix_storage::CacheChange) {
        if self.watching.load(Ordering::Acquire) == 0 {
            return;
        }
        let key = (
            change.tenant_id,
            change.namespace,
            change.cache,
            change.shard,
        );
        let mut shards = self.stripe(&key).lock();
        let Some(watchers) = shards.get_mut(&key) else {
            return;
        };
        let event = CacheChangeEvent {
            key: change.key,
            value: change.value,
            offset: change.offset,
            expires_at_millis: change.expires_at_millis,
        };
        let ended = watchers.deliver(&event);
        self.watching.fetch_sub(ended, Ordering::AcqRel);
        if watchers.is_empty() {
            shards.remove(&key);
        }
    }
}

/// One cache shard's watchers. Exact-key watchers are indexed by their key,
/// so a write is offered only to the watchers of that key and to the prefix
/// watchers, not to every watcher of the shard.
#[derive(Debug, Default)]
struct ShardWatchers {
    by_key: HashMap<String, Vec<Watcher>>,
    prefixes: Vec<Watcher>,
}

impl ShardWatchers {
    fn insert(&mut self, watcher: Watcher) {
        match &watcher.filter {
            CacheWatchFilter::Key(key) => self.by_key.entry(key.clone()).or_default().push(watcher),
            CacheWatchFilter::Prefix(_) => self.prefixes.push(watcher),
        }
    }

    /// Whether a watcher with this id was registered.
    fn remove(&mut self, exact: Option<&str>, id: u64) -> bool {
        let list = match exact {
            Some(key) => match self.by_key.get_mut(key) {
                Some(list) => list,
                None => return false,
            },
            None => &mut self.prefixes,
        };
        let before = list.len();
        list.retain(|watcher| watcher.id != id);
        let removed = list.len() != before;
        if let Some(key) = exact
            && self.by_key.get(key).is_some_and(Vec::is_empty)
        {
            self.by_key.remove(key);
        }
        removed
    }

    /// Empty lists are removed from `by_key` as they empty, so this is cheap.
    fn is_empty(&self) -> bool {
        self.by_key.is_empty() && self.prefixes.is_empty()
    }

    fn len(&self) -> usize {
        self.by_key.values().map(Vec::len).sum::<usize>() + self.prefixes.len()
    }

    fn iter(&self) -> impl Iterator<Item = &Watcher> {
        self.by_key.values().flatten().chain(&self.prefixes)
    }

    /// Offer one change to every watcher it matches. Returns how many
    /// watchers ended.
    fn deliver(&mut self, event: &CacheChangeEvent) -> usize {
        let mut ended = 0;
        if let Some(exact) = self.by_key.get_mut(&event.key) {
            ended += offer(exact, event);
            if exact.is_empty() {
                self.by_key.remove(&event.key);
            }
        }
        ended + offer(&mut self.prefixes, event)
    }
}

/// Hand `event` to each matching watcher, dropping the ones that end.
/// Returns how many ended.
fn offer(watchers: &mut Vec<Watcher>, event: &CacheChangeEvent) -> usize {
    let before = watchers.len();
    watchers.retain(|watcher| {
        if !watcher.filter.matches(&event.key) {
            return true;
        }
        match watcher.sender.try_send(event.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Ended, not thinned: record what was missed and close the
                // queue by dropping the sender. The reader drains what was
                // delivered, sees the close, and reports the lag offset so
                // the client can re-watch from it without a gap.
                watcher
                    .state
                    .lagged_at
                    .store(event.offset, Ordering::Release);
                metrics::counter!("felix_cache_watch_lagged_total").increment(1);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    });
    before - watchers.len()
}

/// Tenant, namespace, cache, shard — one fanout list per cache shard.
type WatchShardKey = (String, String, String, u32);

#[derive(Debug)]
struct Watcher {
    /// Monotonic and never reused. A recycled id here would let a reap aimed at
    /// a closed watcher remove a live one — the Slab-recycling defect class.
    id: u64,
    filter: CacheWatchFilter,
    sender: mpsc::Sender<CacheChangeEvent>,
    state: Arc<WatcherState>,
}

/// Shared between the hub (writer side) and the subscription (reader side).
#[derive(Debug)]
struct WatcherState {
    /// Offset of the first change the queue could not hold, or [`NOT_LAGGED`].
    /// Written once, by the fanout that overflowed the queue.
    lagged_at: AtomicU64,
    /// Set when the watch ended because its shard moved, before its queue
    /// closes.
    moved: OnceLock<ShardMoved>,
}

/// Which changes one watcher wants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheWatchFilter {
    /// Exactly this key.
    Key(String),
    /// Every key beginning with this prefix; `""` is every key in the shard.
    Prefix(String),
}

impl CacheWatchFilter {
    /// Whether a change to `key` belongs to this watcher.
    pub fn matches(&self, key: &str) -> bool {
        match self {
            Self::Key(exact) => key == exact,
            Self::Prefix(prefix) => key.starts_with(prefix.as_str()),
        }
    }
}

/// The receiving half of one cache watch.
#[derive(Debug)]
pub struct CacheWatchSubscription {
    receiver: mpsc::Receiver<CacheChangeEvent>,
    state: Arc<WatcherState>,
    #[allow(dead_code)]
    guard: CacheWatchGuard,
}

impl CacheWatchSubscription {
    /// The next change, or `None` once the watch has ended — check
    /// [`Self::lagged`] to learn whether it ended by falling behind.
    pub async fn recv(&mut self) -> Option<CacheChangeEvent> {
        self.receiver.recv().await
    }

    /// The next change if one is already queued.
    pub fn try_recv(&mut self) -> Result<CacheChangeEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// The offset of the first change this watch missed, once it has fallen
    /// behind. Everything already queued is still delivered first; re-watching
    /// from this offset is gapless.
    pub fn lagged(&self) -> Option<u64> {
        let at = self.state.lagged_at.load(Ordering::Acquire);
        (at != NOT_LAGGED).then_some(at)
    }

    /// Why the watch ended, when it ended because its shard moved. Final once
    /// [`Self::recv`] has returned `None`.
    pub fn moved(&self) -> Option<&ShardMoved> {
        self.state.moved.get()
    }
}

/// RAII handle that unregisters the watcher on drop.
#[derive(Debug)]
pub(crate) struct CacheWatchGuard {
    hub: Weak<CacheWatchHub>,
    key: WatchShardKey,
    /// The watched key, for an exact-key watch: where the hub indexed it.
    exact: Option<String>,
    id: u64,
}

impl Drop for CacheWatchGuard {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.remove(&self.key, self.exact.as_deref(), self.id);
        }
    }
}

/// One cache change, as a watcher receives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheChangeEvent {
    pub key: String,
    /// The value the key now holds; `None` means the key was deleted.
    pub value: Option<Bytes>,
    /// The cache-log offset of the change.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}

#[cfg(test)]
mod tests;
