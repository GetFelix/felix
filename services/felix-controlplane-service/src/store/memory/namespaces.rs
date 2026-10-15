//! Namespaces, and the cascade that deleting one sets off.
use super::{InMemoryStore, ShardCleanup};
use crate::model::{
    CacheChange, CacheChangeOp, Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey,
    ShardKind, StreamChange, StreamChangeOp,
};
use crate::store::{ChangeSet, ControlPlaneStore, Snapshot, StoreError, StoreResult};

pub(super) async fn list_namespaces(
    store: &InMemoryStore,
    tenant_id: &str,
) -> StoreResult<Vec<Namespace>> {
    let items = store
        .namespaces
        .read()
        .await
        .values()
        .filter(|ns| ns.tenant_id == tenant_id)
        .cloned()
        .collect();
    Ok(items)
}

pub(super) async fn create_namespace(
    store: &InMemoryStore,
    namespace: Namespace,
) -> StoreResult<Namespace> {
    // Namespaces are scoped to a tenant; we reject creation if the parent tenant doesn't exist.
    if !store.tenant_exists(&namespace.tenant_id).await? {
        return Err(StoreError::NotFound("tenant".into()));
    }
    let key = NamespaceKey {
        tenant_id: namespace.tenant_id.clone(),
        namespace: namespace.namespace.clone(),
    };
    let mut namespaces = store.namespaces.write().await;
    if namespaces.contains_key(&key) {
        return Err(StoreError::Conflict("namespace exists".into()));
    }
    namespaces.insert(key.clone(), namespace.clone());
    store
        .namespace_changes
        .write()
        .await
        .record(|seq| NamespaceChange {
            seq,
            op: NamespaceChangeOp::Created,
            key,
            namespace: Some(namespace.clone()),
        });
    Ok(namespace)
}

pub(super) async fn delete_namespace(
    store: &InMemoryStore,
    key: &NamespaceKey,
    cleanup: ShardCleanup,
) -> StoreResult<()> {
    if !store.tenant_exists(&key.tenant_id).await? {
        return Err(StoreError::NotFound("tenant".into()));
    }
    let mut namespaces = store.namespaces.write().await;
    let removed = namespaces.remove(key);
    if removed.is_none() {
        return Err(StoreError::NotFound("namespace".into()));
    }
    store
        .namespace_changes
        .write()
        .await
        .record(|seq| NamespaceChange {
            seq,
            op: NamespaceChangeOp::Deleted,
            key: key.clone(),
            namespace: None,
        });
    drop(namespaces);
    // Cascading delete: removing a namespace also removes its streams and caches.
    let mut streams = store.streams.write().await;
    let mut stream_keys: Vec<_> = streams
        .keys()
        .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
        .cloned()
        .collect();
    // Sorted for the same reason as the tenant cascade: identical seq
    // assignment on every Raft replica.
    stream_keys.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
    });
    let nodes = store.nodes.read().await;
    for stream_key in &stream_keys {
        if let Some(stream) = streams.remove(stream_key) {
            let deleted_through =
                super::streams::record_tombstone(store, &nodes, stream_key, stream.incarnation)
                    .await;
            store
                .stream_changes
                .write()
                .await
                .record(|seq| StreamChange {
                    seq,
                    op: StreamChangeOp::Deleted,
                    key: stream_key.clone(),
                    stream: Some(stream),
                    deleted_through,
                });
            if cleanup == ShardCleanup::Full {
                store
                    .drop_shard_assignments_for(
                        ShardKind::Stream,
                        &stream_key.tenant_id,
                        &stream_key.namespace,
                        &stream_key.stream,
                        true,
                    )
                    .await;
            }
        }
    }
    drop(nodes);
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);

    let mut caches = store.caches.write().await;
    let mut cache_keys: Vec<_> = caches
        .keys()
        .filter(|k| k.tenant_id == key.tenant_id && k.namespace == key.namespace)
        .cloned()
        .collect();
    cache_keys.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.cache).cmp(&(&b.tenant_id, &b.namespace, &b.cache))
    });
    for cache_key in &cache_keys {
        if let Some(cache) = caches.remove(cache_key) {
            store.cache_changes.write().await.record(|seq| CacheChange {
                seq,
                op: CacheChangeOp::Deleted,
                key: cache_key.clone(),
                cache: Some(cache),
            });
            if cleanup == ShardCleanup::Full {
                store
                    .drop_shard_assignments_for(
                        ShardKind::Cache,
                        &cache_key.tenant_id,
                        &cache_key.namespace,
                        &cache_key.cache,
                        true,
                    )
                    .await;
            }
        }
    }
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);
    Ok(())
}

pub(super) async fn namespace_snapshot(store: &InMemoryStore) -> StoreResult<Snapshot<Namespace>> {
    // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
    // The rows stay locked while `next_seq` is read: every writer records its
    // change while holding them, so a change counted in `next_seq` is in `items`.
    let rows = store.namespaces.read().await;
    let items = rows.values().cloned().collect();
    let next_seq = store.namespace_changes.read().await.next_seq;
    drop(rows);
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn namespace_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<NamespaceChange>> {
    // A `since` older than the retained window gets a partial history; the
    // caller has to re-bootstrap from the snapshot.
    Ok(store
        .namespace_changes
        .read()
        .await
        .page(since, store.limit(), |change| change.seq))
}

pub(super) async fn namespace_exists(
    store: &InMemoryStore,
    key: &NamespaceKey,
) -> StoreResult<bool> {
    Ok(store.namespaces.read().await.contains_key(key))
}
