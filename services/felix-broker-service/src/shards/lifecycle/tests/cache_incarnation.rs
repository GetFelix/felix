//! A cache shard's logs open at the incarnation its assignment names, and
//! wait for the catalog to register the cache at it, as a stream's do.

use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn cache_key() -> ShardKey {
    ShardKey {
        kind: crate::shards::ShardKind::Cache,
        ..key(0)
    }
}

fn assigned(incarnation: u64) -> HashMap<ShardKey, ShardAssignment> {
    let assignment = ShardAssignment {
        key: cache_key(),
        incarnation,
        ..assigned_to("broker-a", 1)
    };
    [(cache_key(), assignment)].into_iter().collect()
}

/// A broker with a log-backed cache and counters, its tenant and namespace
/// registered.
async fn broker(dir: &std::path::Path) -> std::sync::Arc<felix_broker::Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage =
        felix_broker::DurableStorage::open(dir.join("streams"), config.clone()).expect("storage");
    let counters = std::sync::Arc::new(
        felix_storage::CounterStore::open(dir.join("counters"), config.clone()).expect("counters"),
    );
    let broker = felix_broker::Broker::new(Box::new(
        felix_storage::LogCache::open(dir.join("caches"), config).expect("cache"),
    ))
    .with_durable_storage(storage)
    .with_counters(counters);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    std::sync::Arc::new(broker)
}

fn store(broker: &std::sync::Arc<felix_broker::Broker>) -> DurableShardStore {
    DurableShardStore::new(std::sync::Arc::new(
        broker.durable_storage().expect("storage").clone(),
    ))
    .with_generation_starts(GenerationStarts {
        broker: std::sync::Arc::clone(broker),
        fleet: std::sync::Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
    })
}

async fn register(broker: &felix_broker::Broker, incarnation: u64) {
    broker
        .register_cache(
            "t1",
            "ns",
            "orders",
            felix_broker::CacheMetadata {
                incarnation,
                ..Default::default()
            },
        )
        .await
        .expect("register");
}

fn shard_dir(root: &std::path::Path, incarnation: u64) -> std::path::PathBuf {
    let key = felix_storage::log::ShardKey {
        tenant: "t1".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
    };
    felix_storage::disk_log::layout::incarnation_dir(root, &key, incarnation)
}

/// The control plane places a new cache's shards before the catalog reaches
/// this broker. The shard waits, then opens the assignment's incarnation.
#[tokio::test]
async fn a_cache_shard_waits_for_its_incarnation_to_register() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path()).await;
    let store = store(&broker);
    let own = tokio::sync::Mutex::new(lifecycle());
    let assignments = assigned(7);

    reconcile(&own, &store, &assignments).await;
    assert_eq!(own.lock().await.phase(&cache_key()), Phase::Opening);
    assert!(broker.cache_registration_required());

    register(&broker, 7).await;
    reconcile(&own, &store, &assignments).await;
    assert_eq!(own.lock().await.phase(&cache_key()), Phase::Active);
    for root in ["caches", "counters"] {
        let root = dir.path().join(root);
        assert!(shard_dir(&root, 7).is_dir(), "{root:?}");
        assert!(
            !shard_dir(&root, 0).exists(),
            "opened incarnation 0 in {root:?}"
        );
    }
}

/// Before the fleet finalizes cache incarnations a cache shard opens as soon
/// as it is assigned, as it always did.
#[tokio::test]
async fn a_cache_shard_opens_at_once_before_incarnations_are_required() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path()).await;
    let store = store(&broker);
    let own = tokio::sync::Mutex::new(lifecycle());

    reconcile(&own, &store, &assigned(0)).await;
    assert_eq!(own.lock().await.phase(&cache_key()), Phase::Active);
}

/// The catalog moved to another incarnation after the shard was found ready
/// to open: the old assignment's generation must not reach the new cache's
/// logs.
#[tokio::test]
async fn a_cache_assignment_opens_only_its_own_incarnation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path()).await;
    register(&broker, 8).await;
    let store = store(&broker);

    store
        .open(&cache_key(), 4, 7, true)
        .await
        .expect_err("opened another incarnation's log");

    let log = broker
        .shard_log(felix_broker::LogKind::Cache, "t1", "ns", "orders", 0)
        .await
        .expect("the new cache's log");
    assert_eq!(log.accepted_generation(), 0);
}
