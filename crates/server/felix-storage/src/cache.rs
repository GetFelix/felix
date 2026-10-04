//! Caches: the [`StorageApi`] a broker serves cache requests through, and the
//! two stores behind it.
//!
//! [`EphemeralCache`] keeps entries in memory and is what a broker without
//! durable storage uses. [`LogCache`] keeps each cache shard as a log, so
//! entries survive a restart and can be replicated like a stream.

mod ephemeral;
mod log_cache;

pub use ephemeral::EphemeralCache;
pub use log_cache::{CacheOp, LogCache};

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Result, StorageError};

/// A key-value cache, scoped by tenant, namespace, cache and shard.
// A cache entry is identified by tenant, namespace, cache, shard, and key --
// five fields before the value and its TTL. Bundling them into a struct would
// move the argument list rather than shorten it, and every caller has the parts
// separately anyway.
#[allow(clippy::too_many_arguments)]
#[async_trait()]
pub trait StorageApi: Debug + Send + Sync {
    /// `shard` is which shard of the cache the key belongs to.
    ///
    /// Resolved by the caller, not here: the shard count lives in the routing
    /// table and storage must not depend on it. A caller that owns exactly one
    /// shard of everything passes `0`, which is what an unsharded cache always
    /// was.
    ///
    /// `Ok` only once the write is applied (and, for a log-backed store,
    /// durable). A caller must not acknowledge a put this refused: the value
    /// is in no read's future.
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<()>;

    /// The key's current value. An error, never a miss, when the store cannot
    /// answer: a miss is an answer, and a wrong one here.
    async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>>;

    /// Remove the key, returning the value it had. Fails as [`Self::put`] does.
    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>>;

    /// The log backing one cache shard, when the cache is log-backed.
    ///
    /// `None` for a cache with no log, which has nothing to replicate. Exposed
    /// on the trait rather than reached for by downcast because replication is
    /// a legitimate second reader of the same log, and it must be the same log
    /// the cache writes to.
    async fn shard_log(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Option<crate::disk_log::DiskLog> {
        None
    }

    /// The log backing one cache shard, created to begin at `base_offset` if it
    /// does not exist yet. See [`StorageApi::shard_log`].
    async fn shard_log_at(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
        _base_offset: u64,
    ) -> Option<crate::disk_log::DiskLog> {
        None
    }

    /// Close one shard's log, for a shard this broker no longer holds. The
    /// next call that touches it opens it afresh. A no-op for a store with no
    /// log.
    async fn close_shard(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Result<()> {
        Ok(())
    }

    /// Drop one shard's index, so the next touch rebuilds it from the log.
    /// For replication after it cut records from the log. A no-op for a
    /// store with no log.
    async fn forget_index(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Result<()> {
        Ok(())
    }

    /// Every shard this store has open, as tenant, namespace, cache, shard.
    /// Empty for a store with no log, whose expiry stays lazy.
    fn open_shards(&self) -> Vec<(String, String, String, u32)> {
        Vec::new()
    }

    /// Write a delete for keys whose TTL has passed, so watches see them go.
    /// See `LogCache::expire_due`. A no-op for a store with no log.
    async fn expire_due(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
        _limit: usize,
        _keep_going: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<usize> {
        Ok(0)
    }

    /// Install the observer every applied write is reported to.
    ///
    /// `false` means this store cannot observe writes, and the caller must not
    /// offer watches over it. The default is exactly that: a watch's contract
    /// is built on log offsets, and a store with no log has none to report.
    fn set_change_observer(&self, _observer: std::sync::Arc<dyn CacheObserver>) -> bool {
        false
    }

    /// The offset below which every write to one shard has been applied, and
    /// its watchers told; `None` from a store with no log. A watch joins here
    /// rather than at the log's tail: a write already in the log but not yet
    /// applied is in no snapshot, and its change is still to come.
    async fn applied_through(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Every live key in one shard with its current value and offset, for a
    /// watch that must begin from current state.
    ///
    /// An error, never an empty answer, when the store cannot serve it: a
    /// watcher told a shard is empty would trust a snapshot it never got.
    async fn live_entries(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        Err(StorageError::Unsupported("cache watch snapshot"))
    }

    async fn len(&self) -> usize;

    async fn is_empty(&self) -> bool;
}

/// Sees every write a cache store applies locally, in the order the shard's
/// log applied them.
///
/// Called with the shard's write lock held — that hold is what makes the
/// per-shard order a guarantee rather than a race — so an implementation must
/// not block: hand the change to a queue and return.
///
/// Replication is deliberately outside this seam: records shipped to a
/// follower reach its log without passing through `put`, so a follower's
/// observer stays silent. Watches are served where writes are applied.
pub trait CacheObserver: Debug + Send + Sync {
    fn cache_changed(&self, change: CacheChange);
}

/// One applied cache write, as an observer sees it.
#[derive(Debug, Clone)]
pub struct CacheChange {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
    pub shard: u32,
    pub key: String,
    /// The value the key now holds; `None` means the key was deleted.
    pub value: Option<Bytes>,
    /// The log offset the write was appended at.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}

/// One key's current state, as a watch snapshot reports it.
#[derive(Debug, Clone)]
pub struct CacheSnapshotEntry {
    pub key: String,
    pub value: Bytes,
    /// The log offset of the record that currently defines the key.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}
