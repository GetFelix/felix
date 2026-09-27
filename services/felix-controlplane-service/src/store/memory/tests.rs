use super::*;
use crate::model::{
    Cache, CacheChangeOp, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, Stream,
    StreamChangeOp, StreamKind, Tenant,
};
use crate::store::StoreError;

/// The same suite Postgres runs, so parity is enforced rather than assumed.
#[tokio::test]
async fn satisfies_the_node_store_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::nodes::run_node_contract(store.clone()).await;
    crate::store::contract::nodes::run_node_concurrency_contract(store).await;
}

/// The same suite Postgres runs. These are the security properties —
/// single use, replay detection, revocation — so parity is not a nicety.
#[tokio::test]
async fn satisfies_the_refresh_token_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::refresh_tokens::run_refresh_contract(store).await;
}

/// The same suite Postgres runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_signing_key_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::signing_keys::run_signing_key_contract(store).await;
}

/// The same suite Postgres runs.
#[tokio::test]
async fn satisfies_the_rbac_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::rbac::run_rbac_contract(store).await;
}

/// The same suite Postgres runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_shard_store_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::shards::run_shard_contract(store.clone()).await;
    crate::store::contract::shards::run_shard_concurrency_contract(store.clone()).await;
    crate::store::contract::shards::run_node_delete_race_contract(store, 5000).await;
}

/// The same suite Postgres and Raft run.
#[tokio::test]
async fn satisfies_the_pagination_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::pagination::run_pagination_contract(store).await;
}

/// The same suite Postgres runs, with both instances on one store.
#[tokio::test]
async fn satisfies_the_placement_contract() {
    let store: std::sync::Arc<dyn crate::store::ControlPlaneStore> =
        std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::placement::run_placement_contract(store.clone(), store.clone()).await;
    crate::store::contract::placement::run_expiring_lease_contract(store.clone(), store).await;
}

fn store_with_limits(changes_limit: u64, retention: i64) -> InMemoryStore {
    InMemoryStore::new(StoreConfig {
        changes_limit,
        change_retention_max_rows: Some(retention),
    })
}

#[tokio::test]
async fn tenant_conflict_and_change_window() {
    let store = store_with_limits(1, 1);
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    let err = store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One Duplicate".to_string(),
        })
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await
        .expect("tenant");

    let changes = store.tenant_changes(0).await.expect("changes");
    assert_eq!(changes.items.len(), 1);
    assert_eq!(changes.items[0].tenant_id, "t2");
    assert_eq!(changes.next_seq, 2);
}

#[tokio::test]
async fn namespace_stream_cache_errors_and_cascades() {
    let store = store_with_limits(10, 10);

    let err = store
        .create_namespace(Namespace {
            tenant_id: "missing".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect_err("missing tenant");
    assert!(matches!(err, StoreError::NotFound(_)));

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    let err = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default Duplicate".to_string(),
        })
        .await
        .expect_err("namespace conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await
        .expect_err("stream missing");
    assert!(matches!(err, StoreError::NotFound(_)));

    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
            region: None,
        })
        .await
        .expect("stream");

    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: crate::model::ConsistencyLevel::Leader,
        })
        .await
        .expect("cache");

    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
        })
        .await
        .expect("delete namespace");

    let stream_err = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect_err("stream deleted");
    assert!(matches!(stream_err, StoreError::NotFound(_)));

    let cache_err = store
        .get_cache(&CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await
        .expect_err("cache deleted");
    assert!(matches!(cache_err, StoreError::NotFound(_)));

    let changes = store.stream_changes(0).await.expect("stream changes");
    assert!(
        changes
            .items
            .iter()
            .any(|item| matches!(item.op, StreamChangeOp::Deleted))
    );
    let cache_changes = store.cache_changes(0).await.expect("cache changes");
    assert!(
        cache_changes
            .items
            .iter()
            .any(|item| matches!(item.op, CacheChangeOp::Deleted))
    );
}

#[tokio::test]
async fn backend_health_and_identity() {
    let store = store_with_limits(10, 10);
    store.health_check().await.expect("health");
    assert!(!store.is_durable());
    assert_eq!(store.backend_name(), "memory");
}
