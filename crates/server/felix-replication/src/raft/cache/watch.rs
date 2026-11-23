//! Cache watches on a cache shard's group: every member tells the broker's
//! watch hub of each change its applied entries make, in index order.
//!
//! An entry is committed before it is applied, so a member hands out only
//! committed changes, on the leader and on a follower alike, and a watch
//! keeps going when the group's leader changes. A change's offset is its
//! entry's index, which is also the version a put writes.

use std::sync::Arc;

use felix_storage::{CacheChange, CacheObserver};

use super::{CacheCommand, CacheState};
use crate::ShardKey;

/// Where a cache group's member reports its changes.
pub(crate) struct CacheWatch {
    observer: Arc<dyn CacheObserver>,
    shard: Arc<ShardKey>,
}

/// What a key held before an entry applied, kept only while someone watches.
pub(crate) struct Before(Option<(Option<u64>, String)>);

impl CacheWatch {
    pub(crate) fn new(observer: Arc<dyn CacheObserver>, shard: Arc<ShardKey>) -> Self {
        Self { observer, shard }
    }

    /// The version `command`'s key holds before it applies. Taken only while
    /// a watch is registered, so an unwatched group pays one load per entry.
    pub(crate) fn before(&self, state: &CacheState, command: &CacheCommand) -> Before {
        if !self.observer.watching() {
            return Before(None);
        }
        let key = match command {
            CacheCommand::Put { key, .. }
            | CacheCommand::Delete { key, .. }
            | CacheCommand::Expire { key, .. } => key,
            CacheCommand::Add { .. } | CacheCommand::Retire { .. } => return Before(None),
        };
        let version = state.watched(key).map(|held| held.version);
        Before(Some((version, key.clone())))
    }

    /// Report what the entry at `index` changed. Called with the state's
    /// write lock still held, so a watch that reads the applied index under
    /// that lock sees every change below it in the state and every change at
    /// or above it on its queue. A refused write, a retry and an expiry of a
    /// key written since change nothing and report nothing.
    pub(crate) fn applied(&self, state: &CacheState, index: u64, before: Before) {
        let Before(Some((was, key))) = before else {
            return;
        };
        let now = state.watched(&key);
        let (value, expires_at) = match now {
            Some(held) if held.version == index => (Some(held.value), held.expires_at),
            None if was.is_some() => (None, 0),
            _ => return,
        };
        self.observer.cache_changed(CacheChange {
            tenant_id: self.shard.tenant_id.clone(),
            namespace: self.shard.namespace.clone(),
            cache: self.shard.stream.clone(),
            shard: self.shard.shard,
            key,
            value,
            offset: index,
            expires_at_millis: expires_at.div_ceil(1000),
        });
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
