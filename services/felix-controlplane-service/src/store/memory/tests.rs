use super::*;
use crate::model::{
    Cache, CacheChangeOp, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceChangeOp,
    RetentionPolicy, Stream, StreamChangeOp, StreamKind, Tenant, TenantChangeOp,
};
use crate::store::StoreError;

/// The same suite Postgres runs, so parity is enforced rather than assumed.
#[tokio::test]
async fn satisfies_the_node_store_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::nodes::run_node_contract(store.clone()).await;
    crate::store::contract::nodes::run_node_concurrency_contract(store.clone()).await;
    crate::store::contract::nodes::run_fleet_contract(store.clone()).await;
    crate::store::contract::suspicions::run_suspicion_contract(store).await;
}

/// The contract allows a backend to leave a future stamp; this one must not,
/// or a broker dying after a clock step back stays live for the step.
#[tokio::test]
async fn a_clamp_pulls_a_future_stamp_back_to_now() {
    let store = store_with_limits(100, 1000);
    let mut ahead = crate::store::contract::nodes::node("broker-ahead", 7001);
    ahead.status.last_heartbeat_at_millis = 2_000_000;
    store.register_node(ahead).await.expect("register");

    assert_eq!(
        store
            .clamp_future_heartbeats(1_000_000)
            .await
            .expect("clamp"),
        1
    );
    let node = store.get_node("broker-ahead").await.expect("get");
    assert_eq!(node.status.last_heartbeat_at_millis, 1_000_000);
}

/// The same suite Postgres runs. These are the security properties —
/// single use, replay detection, revocation — so parity is not a nicety.
#[tokio::test]
async fn satisfies_the_refresh_token_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::refresh_tokens::run_refresh_contract(store).await;
}

/// The same suite Postgres and Raft run.
#[tokio::test]
async fn satisfies_the_revocation_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::revocations::run_revocation_contract(store).await;
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
            routing: Default::default(),
            incarnation: 0,
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
            incarnation: 0,
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

/// The same suite Postgres runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_incarnation_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::incarnations::run_incarnation_contract(store).await;
}

/// The same suite Postgres runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_cache_incarnation_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::cache_incarnations::run_cache_incarnation_contract(store).await;
}

/// The same suite Postgres runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_resources_contract() {
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::resources::run_resources_contract(store.clone()).await;
    crate::store::contract::resources::run_resources_race_contract(store, 200).await;
}

/// The same suite Postgres runs, at a page of two so every feed spans pages.
#[tokio::test]
async fn satisfies_the_change_feed_contract() {
    let store = std::sync::Arc::new(store_with_limits(2, 1000));
    crate::store::contract::change_feeds::run_change_feed_contract(store, 2).await;
}

/// A snapshot holds its rows while it reads `next_seq`. Otherwise a writer can
/// land between the two reads: its change is counted in `next_seq`, so the
/// consumer never polls for it, but its row is missing from the snapshot.
///
/// Deterministic: the test holds the change log, which parks the snapshot
/// between its two reads, and then plays a writer that wants the rows.
#[tokio::test]
async fn a_snapshot_never_counts_a_change_its_rows_do_not_show() {
    macro_rules! case {
        ($rows:ident, $changes:ident, $snapshot:ident, $key:expr, $row:expr, $change:expr) => {{
            let store = std::sync::Arc::new(store_with_limits(100, 1000));
            let mut changes = store.$changes.write().await;
            let snapshot = tokio::spawn({
                let store = store.clone();
                async move { store.$snapshot().await }
            });
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            if let Ok(mut rows) = store.$rows.try_write() {
                rows.insert($key, $row);
                changes.record($change);
            }
            drop(changes);
            let snapshot = snapshot.await.expect("join").expect("snapshot");
            assert_eq!(
                snapshot.items.len() as u64,
                snapshot.next_seq,
                "{}: next_seq counts a create the rows do not show",
                stringify!($snapshot),
            );
        }};
    }

    let tenant = Tenant {
        tenant_id: "t".to_string(),
        display_name: "t".to_string(),
    };
    case!(
        tenants,
        tenant_changes,
        tenant_snapshot,
        "t".to_string(),
        tenant.clone(),
        |seq| TenantChange {
            seq,
            op: TenantChangeOp::Created,
            tenant_id: "t".to_string(),
            tenant: Some(tenant.clone()),
        }
    );

    let namespace = Namespace {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        display_name: "ns".to_string(),
    };
    let namespace_key = NamespaceKey {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
    };
    case!(
        namespaces,
        namespace_changes,
        namespace_snapshot,
        namespace_key.clone(),
        namespace.clone(),
        |seq| NamespaceChange {
            seq,
            op: NamespaceChangeOp::Created,
            key: namespace_key.clone(),
            namespace: Some(namespace.clone()),
        }
    );

    let stream = Stream {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: false,
        region: None,
        routing: Default::default(),
        incarnation: 0,
    };
    let stream_key = StreamKey {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
    };
    case!(
        streams,
        stream_changes,
        stream_snapshot,
        stream_key.clone(),
        stream.clone(),
        |seq| StreamChange {
            seq,
            op: StreamChangeOp::Created,
            key: stream_key.clone(),
            stream: Some(stream.clone()),
            deleted_through: None,
        }
    );

    let cache = Cache {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        cache: "c".to_string(),
        display_name: "c".to_string(),
        shards: 1,
        replication_factor: 1,
        consistency: ConsistencyLevel::Leader,
        incarnation: 0,
    };
    let cache_key = CacheKey {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        cache: "c".to_string(),
    };
    case!(
        caches,
        cache_changes,
        cache_snapshot,
        cache_key.clone(),
        cache.clone(),
        |seq| CacheChange {
            seq,
            op: CacheChangeOp::Created,
            key: cache_key.clone(),
            cache: Some(cache.clone()),
            deleted_through: None,
        }
    );
}

/// A shard write holds the cache it read until it lands: a delete and
/// recreate of the cache that starts once the write has read it waits for
/// the write, so the new cache never gets an assignment naming the old one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_write_holds_its_cache_until_it_lands() {
    use crate::model::{
        Cache, CacheKey, ConsistencyLevel, ShardAssignment, ShardKey, ShardKind, ShardState,
    };
    let store = std::sync::Arc::new(store_with_limits(100, 1000));
    crate::store::contract::shards::seed(store.as_ref()).await;
    let feature = felix_common::fleet::CACHE_INCARNATIONS.name();
    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        store
            .register_node(crate::store::contract::nodes::with_features(
                crate::store::contract::nodes::node(id, 7500 + i as u16),
                &[feature],
            ))
            .await
            .expect("node");
    }
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");
    let cache = |incarnation| Cache {
        tenant_id: "shard-t".to_string(),
        namespace: "shard-ns".to_string(),
        cache: "held".to_string(),
        display_name: "held".to_string(),
        shards: 1,
        replication_factor: 1,
        consistency: ConsistencyLevel::Leader,
        incarnation,
    };
    let key = CacheKey {
        tenant_id: "shard-t".to_string(),
        namespace: "shard-ns".to_string(),
        cache: "held".to_string(),
    };
    store.create_cache(cache(1)).await.expect("create");

    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *store.faults.shard_write_pause.lock().expect("pause") = Some(super::ShardWritePause {
        reached,
        resume: resume_rx,
    });
    let shard = ShardKey {
        tenant_id: "shard-t".to_string(),
        namespace: "shard-ns".to_string(),
        stream: "held".to_string(),
        shard: 0,
        kind: ShardKind::Cache,
    };
    let assign = {
        let store = std::sync::Arc::clone(&store);
        let shard = shard.clone();
        tokio::spawn(async move {
            store
                .put_shard_assignment(ShardAssignment {
                    key: shard,
                    leader: "broker-x".to_string(),
                    replicas: Vec::new(),
                    generation: 0,
                    state: ShardState::Active,
                    successor: None,
                    joining: None,
                    move_started_at_millis: None,
                    move_reason: None,
                    incarnation: 0,
                })
                .await
                .expect("assign")
        })
    };
    reached_rx.await.expect("the write read the cache");
    let recreate = {
        let store = std::sync::Arc::clone(&store);
        tokio::spawn(async move {
            store.delete_cache(&key).await.expect("delete");
            store.create_cache(cache(2)).await.expect("create again");
            let _ = resume.send(());
        })
    };
    assign.await.expect("assign");
    recreate.await.expect("recreate");

    let current = store
        .get_cache(&CacheKey {
            tenant_id: "shard-t".to_string(),
            namespace: "shard-ns".to_string(),
            cache: "held".to_string(),
        })
        .await
        .expect("cache")
        .incarnation;
    if let Ok(held) = store.get_shard_assignment(&shard).await {
        assert_eq!(
            held.incarnation, current,
            "an assignment of the new cache names the old one"
        );
    }
}
