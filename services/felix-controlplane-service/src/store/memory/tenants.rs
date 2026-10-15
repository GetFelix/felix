//! Tenants, and the cascade that deleting one sets off.
use super::InMemoryStore;
use crate::model::{
    CacheChange, CacheChangeOp, NamespaceChange, NamespaceChangeOp, StreamChange, StreamChangeOp,
    Tenant, TenantChange, TenantChangeOp,
};
use crate::store::{ChangeSet, Snapshot, StoreError, StoreResult};

pub(super) async fn list_tenants(store: &InMemoryStore) -> StoreResult<Vec<Tenant>> {
    Ok(store.tenants.read().await.values().cloned().collect())
}

pub(super) async fn create_tenant(store: &InMemoryStore, tenant: Tenant) -> StoreResult<Tenant> {
    // Create is a pure in-memory upsert with conflict detection.
    // We also append a change-log entry so watchers can incrementally sync.
    let mut tenants = store.tenants.write().await;
    if tenants.contains_key(&tenant.tenant_id) {
        return Err(StoreError::Conflict("tenant exists".into()));
    }
    tenants.insert(tenant.tenant_id.clone(), tenant.clone());
    store
        .tenant_changes
        .write()
        .await
        .record(|seq| TenantChange {
            seq,
            op: TenantChangeOp::Created,
            tenant_id: tenant.tenant_id.clone(),
            tenant: Some(tenant.clone()),
        });
    Ok(tenant)
}

pub(super) async fn delete_tenant(store: &InMemoryStore, tenant_id: &str) -> StoreResult<()> {
    let mut tenants = store.tenants.write().await;
    if tenants.remove(tenant_id).is_none() {
        return Err(StoreError::NotFound("tenant".into()));
    }
    // Held until the tenant's own change is recorded, so a snapshot never sees
    // a seq for a change its rows do not show yet, and a create of the same id
    // cannot slip its change in ahead of this delete's.
    store.idp_issuers.write().await.remove(tenant_id);
    store.tenant_signing_keys.write().await.remove(tenant_id);
    store.rbac_policies.write().await.remove(tenant_id);
    store.rbac_groupings.write().await.remove(tenant_id);
    store.auth_bootstrapped.write().await.remove(tenant_id);
    // Cascading delete: remove dependent namespaces, streams, and caches.
    // We emit delete changes for dependents so incremental consumers can evict their caches.
    let mut namespaces = store.namespaces.write().await;
    let mut ns_keys: Vec<_> = namespaces
        .keys()
        .filter(|k| k.tenant_id == tenant_id)
        .cloned()
        .collect();
    // Sorted before publishing: cascade events take sequence numbers in
    // this order, and a Raft replica applying the same command must
    // assign the same seq to the same event — HashMap order would not.
    ns_keys.sort_by(|a, b| (&a.tenant_id, &a.namespace).cmp(&(&b.tenant_id, &b.namespace)));
    for key in &ns_keys {
        if let Some(ns) = namespaces.remove(key) {
            store
                .namespace_changes
                .write()
                .await
                .record(|seq| NamespaceChange {
                    seq,
                    op: NamespaceChangeOp::Deleted,
                    key: key.clone(),
                    namespace: Some(ns),
                });
        }
    }
    drop(namespaces);

    let mut streams = store.streams.write().await;
    let mut stream_keys: Vec<_> = streams
        .keys()
        .filter(|k| k.tenant_id == tenant_id)
        .cloned()
        .collect();
    stream_keys.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
    });
    let nodes = store.nodes.read().await;
    for key in &stream_keys {
        if let Some(stream) = streams.remove(key) {
            let deleted_through =
                super::streams::record_tombstone(store, &nodes, key, stream.incarnation).await;
            store
                .stream_changes
                .write()
                .await
                .record(|seq| StreamChange {
                    seq,
                    op: StreamChangeOp::Deleted,
                    key: key.clone(),
                    stream: Some(stream),
                    deleted_through,
                });
        }
    }
    drop(nodes);
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);
    drop(streams);

    let mut caches = store.caches.write().await;
    let mut cache_keys: Vec<_> = caches
        .keys()
        .filter(|k| k.tenant_id == tenant_id)
        .cloned()
        .collect();
    cache_keys.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.cache).cmp(&(&b.tenant_id, &b.namespace, &b.cache))
    });
    for key in &cache_keys {
        if let Some(cache) = caches.remove(key) {
            store.cache_changes.write().await.record(|seq| CacheChange {
                seq,
                op: CacheChangeOp::Deleted,
                key: key.clone(),
                cache: Some(cache),
            });
        }
    }
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);

    store
        .tenant_changes
        .write()
        .await
        .record(|seq| TenantChange {
            seq,
            op: TenantChangeOp::Deleted,
            tenant_id: tenant_id.to_string(),
            tenant: None,
        });
    drop(tenants);
    Ok(())
}

pub(super) async fn tenant_snapshot(store: &InMemoryStore) -> StoreResult<Snapshot<Tenant>> {
    // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
    // The rows stay locked while `next_seq` is read: every writer records its
    // change while holding them, so a change counted in `next_seq` is in `items`.
    let rows = store.tenants.read().await;
    let items = rows.values().cloned().collect();
    let next_seq = store.tenant_changes.read().await.next_seq;
    drop(rows);
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn tenant_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<TenantChange>> {
    // A `since` older than the retained window gets a partial history; the
    // caller has to re-bootstrap from the snapshot.
    Ok(store
        .tenant_changes
        .read()
        .await
        .page(since, store.limit(), |change| change.seq))
}

pub(super) async fn tenant_exists(store: &InMemoryStore, tenant_id: &str) -> StoreResult<bool> {
    Ok(store.tenants.read().await.contains_key(tenant_id))
}
