//! The in-memory cache a broker uses when it has no durable storage.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::Result;
use crate::cache::StorageApi;

/// Simple in-memory cache with optional TTL expiry.
///
/// ```
/// use bytes::Bytes;
/// use felix_storage::*;
///
/// let cache = EphemeralCache::new();
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     cache
///         .put("t1", "default", "primary", 0, "k", Bytes::from_static(b"v"), None)
///         .await;
///     assert_eq!(
///         cache.get("t1", "default", "primary", 0, "k").await,
///         Some(Bytes::from_static(b"v"))
///     );
/// });
/// ```
#[derive(Debug)]
pub struct EphemeralCache {
    // RwLock allows concurrent readers while updates take exclusive access.
    inner: RwLock<HashMap<CacheKey, CacheEntry>>,
    // Optional size cap to enable future eviction policies.
    max_entries: Option<usize>,
}

impl EphemeralCache {
    // Use Default to centralize initialization.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: Some(max_entries),
        }
    }
}

impl Default for EphemeralCache {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: None,
        }
    }
}

impl From<EphemeralCache> for Box<dyn StorageApi + Send> {
    fn from(value: EphemeralCache) -> Self {
        Box::new(value)
    }
}

#[async_trait()]
impl StorageApi for EphemeralCache {
    /// The shard is ignored, deliberately. Entries live in one flat map, and a
    /// key belongs to exactly one shard, so two shards of one cache can never
    /// name the same entry. Only the log-backed cache needs the shard, because
    /// its records go to a per-shard directory.
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<()> {
        // Compute expiry once so reads only compare Instants.
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let entry = CacheEntry { value, expires_at };
        let mut guard: tokio::sync::RwLockWriteGuard<'_, HashMap<CacheKey, CacheEntry>> =
            self.inner.write().await;
        guard.insert(CacheKey::new(tenant_id, namespace, cache, key), entry);
        if let Some(max_entries) = self.max_entries
            && guard.len() > max_entries
        {
            // Placeholder eviction: remove an arbitrary key until capped.
            if let Some(key) = guard.keys().next().cloned() {
                guard.remove(&key);
            }
        }
        Ok(())
    }

    async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        // Take a write lock so we can evict expired entries.
        let mut guard: tokio::sync::RwLockWriteGuard<'_, HashMap<CacheKey, CacheEntry>> =
            self.inner.write().await;
        let scoped_key = CacheKey::new(tenant_id, namespace, cache, key);
        if let Some(entry) = guard.get(&scoped_key) {
            if let Some(expires_at) = entry.expires_at {
                // Lazy-expire on read to avoid a background sweeper.
                if Instant::now() >= expires_at {
                    guard.remove(&scoped_key);
                    return Ok(None);
                }
            }
            return Ok(Some(entry.value.clone()));
        }
        Ok(None)
    }

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        // Remove and return the stored value, if any.
        Ok(self
            .inner
            .write()
            .await
            .remove(&CacheKey::new(tenant_id, namespace, cache, key))
            .map(|entry| entry.value))
    }

    async fn len(&self) -> usize {
        let guard: tokio::sync::RwLockReadGuard<HashMap<CacheKey, CacheEntry>> =
            self.inner.read().await;
        guard.len()
    }

    async fn is_empty(&self) -> bool {
        let guard: tokio::sync::RwLockReadGuard<HashMap<CacheKey, CacheEntry>> =
            self.inner.read().await;
        guard.is_empty()
    }
}

// `EphemeralCache` is `Send + Sync` from its fields alone (`RwLock<HashMap<..>>`
// and `Option<usize>`), so the compiler's auto-impls suffice and no `unsafe impl`
// is needed. `StorageApi: Send + Sync` means this must hold; assert it here so a
// future field change is a build error rather than a trait-bound puzzle.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EphemeralCache>();
};

/// Identifies one entry: the tenant, namespace and cache it belongs to, and its key.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
}

impl CacheKey {
    fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            cache: cache.into(),
            key: key.into(),
        }
    }
}

/// A stored value and when it expires.
#[derive(Debug, Clone)]
struct CacheEntry {
    // Stored value plus optional expiration.
    value: Bytes,
    expires_at: Option<Instant>,
}

#[cfg(test)]
mod tests;
