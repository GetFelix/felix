//! Caches and their change log.
use super::{Decide, InMemoryStore, ShardCleanup};
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

pub(super) async fn create_cache(
    store: &InMemoryStore,
    cache: Cache,
    decide: Decide,
) -> StoreResult<Cache> {
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
    // Held until the cache is in, so a finalize cannot land between the read
    // and the write: nothing is created at 0 after it.
    let nodes = store.nodes.read().await;
    let mut cache = cache;
    cache.incarnation = incarnation_for(store, &nodes, &key, cache.incarnation, decide).await;
    caches.insert(key.clone(), cache.clone());
    store.cache_changes.write().await.record(|seq| CacheChange {
        seq,
        op: CacheChangeOp::Created,
        key,
        cache: Some(cache.clone()),
        deleted_through: None,
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
        deleted_through: None,
    });
    metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
    Ok(updated)
}

pub(super) async fn delete_cache(
    store: &InMemoryStore,
    key: &CacheKey,
    cleanup: ShardCleanup,
    decide: Decide,
) -> StoreResult<()> {
    let mut caches = store.caches.write().await;
    let Some(removed) = caches.remove(key) else {
        return Err(StoreError::NotFound("cache".into()));
    };
    let nodes = store.nodes.read().await;
    let deleted_through = record_tombstone(store, &nodes, key, removed.incarnation, decide).await;
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
        cache: crate::model::deleted_cache_payload(removed),
        deleted_through,
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

pub(super) async fn cache_tombstones(store: &InMemoryStore) -> Vec<crate::model::CacheTombstone> {
    let mut tombstones: Vec<_> = store
        .cache_tombstones
        .read()
        .await
        .iter()
        .map(|(key, incarnation)| crate::model::CacheTombstone {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            cache: key.cache.clone(),
            incarnation: *incarnation,
        })
        .collect();
    tombstones.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.cache).cmp(&(&b.tenant_id, &b.namespace, &b.cache))
    });
    tombstones
}

/// Whether the fleet finalized `cache_incarnations`, from the node state the
/// caller holds: held across the write it decides, a finalize cannot land in
/// between.
fn incarnations_finalized(nodes: &super::NodeState) -> bool {
    nodes
        .fleet_enabled
        .contains(felix_common::fleet::CACHE_INCARNATIONS.name())
}

/// The incarnation a cache created now under `key` gets, from the `proposed`
/// one its request carried. 0 until the finalize; after it, never 0 and above
/// every earlier incarnation of the name. Always 0 from an entry before level
/// 14, which a member at 13 applies that way even after the finalize; a
/// level-14 entry gets one only if it proposed one.
pub(super) async fn incarnation_for(
    store: &InMemoryStore,
    nodes: &super::NodeState,
    key: &CacheKey,
    proposed: u64,
    decide: Decide,
) -> u64 {
    if decide == Decide::Legacy
        || !incarnations_finalized(nodes)
        || (decide == Decide::Entry && proposed == 0)
    {
        return 0;
    }
    let deleted = store.cache_tombstones.read().await.get(key).copied();
    crate::model::next_incarnation(proposed, deleted)
}

/// Record the delete of `incarnation` of `key` once incarnations are
/// finalized, returning what the `Deleted` change carries. Nothing before the
/// finalize: a cache created again at 0 then still opens the deleted one's
/// data, so it must stay. Nothing from an entry before level 14 either, as a
/// member at 13 applies it.
pub(super) async fn record_tombstone(
    store: &InMemoryStore,
    nodes: &super::NodeState,
    key: &CacheKey,
    incarnation: u64,
    decide: Decide,
) -> Option<u64> {
    if decide == Decide::Legacy || !incarnations_finalized(nodes) {
        return None;
    }
    let mut tombstones = store.cache_tombstones.write().await;
    let through = tombstones.entry(key.clone()).or_default();
    *through = (*through).max(incarnation);
    Some(*through)
}
