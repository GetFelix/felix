//! Caches and their change log.
use super::{InMemoryStore, ShardCleanup};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, NamespaceKey, ShardKind,
};
use crate::store::{ChangeSet, ControlPlaneStore, Snapshot, StoreError, StoreResult};

pub(super) async fn list_caches(
    store: &InMemoryStore,
    tenant_id: &str,
    namespace: &str,
) -> StoreResult<Vec<Cache>> {
    let items = store
        .caches
        .read()
        .await
        .values()
        .filter(|cache| cache.tenant_id == tenant_id && cache.namespace == namespace)
        .cloned()
        .collect();
    Ok(items)
}

pub(super) async fn get_cache(store: &InMemoryStore, key: &CacheKey) -> StoreResult<Cache> {
    store
        .caches
        .read()
        .await
        .get(key)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("cache".into()))
}

pub(super) async fn create_cache(store: &InMemoryStore, cache: Cache) -> StoreResult<Cache> {
    // Caches are scoped to a namespace; we reject creation if the parent namespace doesn't exist.
    if !store
        .namespace_exists(&NamespaceKey {
            tenant_id: cache.tenant_id.clone(),
            namespace: cache.namespace.clone(),
        })
        .await?
    {
        return Err(StoreError::NotFound("namespace".into()));
    }
    let key = CacheKey {
        tenant_id: cache.tenant_id.clone(),
        namespace: cache.namespace.clone(),
        cache: cache.cache.clone(),
    };
    let mut caches = store.caches.write().await;
    if caches.contains_key(&key) {
        return Err(StoreError::Conflict("cache exists".into()));
    }
    caches.insert(key.clone(), cache.clone());
    store.cache_changes.write().await.record(|seq| CacheChange {
        seq,
        op: CacheChangeOp::Created,
        key,
        cache: Some(cache.clone()),
    });
    metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);
    Ok(cache)
}

pub(super) async fn patch_cache(
    store: &InMemoryStore,
    key: &CacheKey,
    patch: CachePatchRequest,
) -> StoreResult<Cache> {
    let mut caches = store.caches.write().await;
    let cache = caches
        .get_mut(key)
        .ok_or_else(|| StoreError::NotFound("cache".into()))?;
    if let Some(display_name) = patch.display_name {
        cache.display_name = display_name;
    }
    // After applying the patch, we emit an `Updated` change so watchers can reconcile.
    let updated = cache.clone();
    store.cache_changes.write().await.record(|seq| CacheChange {
        seq,
        op: CacheChangeOp::Updated,
        key: key.clone(),
        cache: Some(updated.clone()),
    });
    metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
    Ok(updated)
}

pub(super) async fn delete_cache(
    store: &InMemoryStore,
    key: &CacheKey,
    cleanup: ShardCleanup,
) -> StoreResult<()> {
    let mut caches = store.caches.write().await;
    let removed = caches.remove(key);
    if removed.is_none() {
        return Err(StoreError::NotFound("cache".into()));
    }
    store
        .drop_shard_assignments_for(
            ShardKind::Cache,
            &key.tenant_id,
            &key.namespace,
            &key.cache,
            cleanup == ShardCleanup::Full,
        )
        .await;
    store.cache_changes.write().await.record(|seq| CacheChange {
        seq,
        op: CacheChangeOp::Deleted,
        key: key.clone(),
        cache: None,
    });
    metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);
    Ok(())
}

pub(super) async fn cache_snapshot(store: &InMemoryStore) -> StoreResult<Snapshot<Cache>> {
    // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
    // The rows stay locked while `next_seq` is read: every writer records its
    // change while holding them, so a change counted in `next_seq` is in `items`.
    let rows = store.caches.read().await;
    let items = rows.values().cloned().collect();
    let next_seq = store.cache_changes.read().await.next_seq;
    drop(rows);
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn cache_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<CacheChange>> {
    // A `since` older than the retained window gets a partial history; the
    // caller has to re-bootstrap from the snapshot.
    Ok(store
        .cache_changes
        .read()
        .await
        .page(since, store.limit(), |change| change.seq))
}
