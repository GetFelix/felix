//! The in-memory cache a broker uses when it has no durable storage.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::Result;
use crate::cache::{CacheCondition, ConditionalWrite, StorageApi, VersionedValue};

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
///         .await
///         .expect("put");
///     assert_eq!(
///         cache.get("t1", "default", "primary", 0, "k").await.expect("get"),
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
    /// Source of entry versions. One counter for the whole store, so a key
    /// deleted and written again never gets a version it had before. Starts
    /// at [`incarnation_base`] so a version a client read before a restart
    /// cannot name a value written after it.
    next_version: AtomicU64,
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
            next_version: AtomicU64::new(incarnation_base()),
        }
    }
}

impl EphemeralCache {
    /// Store an entry under a fresh version, returning the version.
    fn insert(
        &self,
        map: &mut HashMap<CacheKey, CacheEntry>,
        key: CacheKey,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> u64 {
        // Compute expiry once so reads only compare Instants.
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let version = self.next_version.fetch_add(1, Ordering::Relaxed);
        map.insert(
            key,
            CacheEntry {
                value,
                expires_at,
                version,
            },
        );
        if let Some(max_entries) = self.max_entries
            && map.len() > max_entries
        {
            // Placeholder eviction: remove an arbitrary key until capped.
            if let Some(key) = map.keys().next().cloned() {
                map.remove(&key);
            }
        }
        version
    }
}

/// The first version this process hands out: wall-clock microseconds at
/// startup. An earlier incarnation's versions run from its own start time up
/// by one per write, so they stay below this one unless it averaged more than
/// a write per microsecond or the clock went back between runs. Microseconds
/// keep versions under 2^53, exact as a JSON number in any client.
fn incarnation_base() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_micros()).unwrap_or(u64::MAX / 2)
        })
}

/// The entry under `key`, unless it has expired.
fn live<'a>(map: &'a HashMap<CacheKey, CacheEntry>, key: &CacheKey) -> Option<&'a CacheEntry> {
    map.get(key)
        .filter(|entry| entry.expires_at.is_none_or(|at| Instant::now() < at))
}

impl Default for EphemeralCache {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: None,
            next_version: AtomicU64::new(incarnation_base()),
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
        let mut guard = self.inner.write().await;
        self.insert(
            &mut guard,
            CacheKey::new(tenant_id, namespace, cache, key),
            value,
            ttl,
        );
        Ok(())
    }

    async fn put_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
        condition: CacheCondition,
    ) -> Result<ConditionalWrite> {
        // Checked and written under one write lock, which is the atomicity.
        let mut guard = self.inner.write().await;
        let key = CacheKey::new(tenant_id, namespace, cache, key);
        let current = live(&guard, &key).map(|entry| entry.version);
        let holds = match condition {
            CacheCondition::Absent => current.is_none(),
            CacheCondition::Version(version) => current == Some(version),
        };
        if !holds {
            return Ok(ConditionalWrite {
                applied: false,
                version: current,
            });
        }
        let version = self.insert(&mut guard, key, value, ttl);
        Ok(ConditionalWrite {
            applied: true,
            version: Some(version),
        })
    }

    async fn delete_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
        version: u64,
    ) -> Result<ConditionalWrite> {
        let mut guard = self.inner.write().await;
        let key = CacheKey::new(tenant_id, namespace, cache, key);
        let current = live(&guard, &key).map(|entry| entry.version);
        if current != Some(version) {
            return Ok(ConditionalWrite {
                applied: false,
                version: current,
            });
        }
        guard.remove(&key);
        Ok(ConditionalWrite {
            applied: true,
            version: None,
        })
    }

    async fn get_versioned(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        _shard: u32,
        key: &str,
    ) -> Result<Option<VersionedValue>> {
        let guard = self.inner.read().await;
        Ok(
            live(&guard, &CacheKey::new(tenant_id, namespace, cache, key)).map(|entry| {
                VersionedValue {
                    value: entry.value.clone(),
                    version: entry.version,
                }
            }),
        )
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
    version: u64,
}

#[cfg(test)]
mod tests;
