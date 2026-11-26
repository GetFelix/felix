//! Cache watches on a cache shard's group: every member tells the broker's
//! watch hub of each change its applied entries make, in index order.
//!
//! An entry is committed before it is applied, so a member hands out only
//! committed changes, on the leader and on a follower alike, and a watch
//! keeps going when the group's leader changes. A change's offset is its
//! entry's index, which is also the version a put writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use felix_storage::{CacheChange, CacheObserver};

use super::recent::Change;
use crate::ShardKey;

/// Watch activity on one member, which its group turns into a
/// [`super::CacheCommand::Watched`] entry so every member records. Counts
/// only, so a watched shard's apply pays one atomic add per change.
#[derive(Debug, Default)]
pub(crate) struct WatchSignal {
    /// Bumped when a watch here joins, takes a change or ends.
    seen: AtomicU64,
    /// `seen` as of the last time the applied state showed a fresh mark.
    marked: AtomicU64,
    /// When this member last asked for a mark, on the group's clock.
    pub(crate) asked_at: AtomicU64,
}

impl WatchSignal {
    pub(crate) fn saw(&self) {
        self.seen.fetch_add(1, Ordering::Relaxed);
    }

    /// The activity count, if a watch was seen since the last fresh mark.
    pub(crate) fn unmarked(&self) -> Option<u64> {
        let seen = self.seen.load(Ordering::Relaxed);
        (seen != self.marked.load(Ordering::Relaxed)).then_some(seen)
    }

    /// The applied state holds a fresh mark covering activity up to `seen`.
    pub(crate) fn marked(&self, seen: u64) {
        self.marked.store(seen, Ordering::Relaxed);
    }
}

/// Where a cache group's member reports its changes.
pub(crate) struct CacheWatch {
    observer: Arc<dyn CacheObserver>,
    shard: Arc<ShardKey>,
    signal: Arc<WatchSignal>,
}

impl CacheWatch {
    pub(crate) fn new(
        observer: Arc<dyn CacheObserver>,
        shard: Arc<ShardKey>,
        signal: Arc<WatchSignal>,
    ) -> Self {
        Self {
            observer,
            shard,
            signal,
        }
    }

    /// Whether a change could reach a watcher now. While it cannot, apply
    /// builds no change for this group, unless it is still recording.
    pub(crate) fn watching(&self) -> bool {
        self.observer.watching()
    }

    /// Report `change`, and say whether a watch of this shard was there to
    /// take it. Called with the state's write lock still held, so a watch
    /// that reads the applied index under that lock sees every change below
    /// it in the state and every change at or above it on its queue.
    pub(crate) fn report(&self, change: &Change) -> bool {
        let taken = self.observer.cache_changed(CacheChange {
            tenant_id: self.shard.tenant_id.clone(),
            namespace: self.shard.namespace.clone(),
            cache: self.shard.stream.clone(),
            shard: self.shard.shard,
            key: change.key.clone(),
            value: change.value.clone(),
            offset: change.offset,
            expires_at_millis: change.expires_at.div_ceil(1000),
        });
        if taken {
            self.signal.saw();
        }
        taken
    }

    /// The state was replaced by a snapshot: end every watch as lagged at
    /// `at`, the first index whose change it was not told of. Re-watching
    /// from there reads the new state.
    pub(crate) fn reset(&self, at: u64) {
        let shard = &self.shard;
        self.observer.cache_reset(
            &shard.tenant_id,
            &shard.namespace,
            &shard.stream,
            shard.shard,
            at,
        );
    }
}
