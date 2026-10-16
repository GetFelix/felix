use std::sync::Arc;

use felix_broker::{Broker, CacheMetadata, ConsistencyLevel};
use felix_router::{ShardKey, ShardKind};
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn cache_key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: "sessions".into(),
        shard: 0,
        kind: ShardKind::Cache,
    }
}

async fn cache_broker(dir: &std::path::Path, incarnation: u64) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.join("caches"), config.clone()).expect("cache"),
        ))
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(dir.join("counters"), config).expect("counters"),
        )),
    );
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_cache(
            "t1",
            "ns",
            "sessions",
            CacheMetadata {
                consistency: ConsistencyLevel::Quorum,
                incarnation,
            },
        )
        .await
        .expect("cache");
    broker
}

/// A leader names its cache's incarnation to followers and opens its own logs
/// at it.
#[tokio::test]
async fn a_cache_shard_names_its_incarnation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(dir.path(), 7).await;
    let shard = shard_ref(&broker, &cache_key(), 3);
    assert_eq!(shard.incarnation, Some(7));
    assert_eq!(of(&broker, &cache_key()), 7);
    assert_eq!(expected(&broker, ShardKind::Cache, &shard), Some(7));
}

/// A cache at incarnation 0 sends what it always did and opens whatever is
/// registered.
#[tokio::test]
async fn a_cache_at_incarnation_zero_names_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(dir.path(), 0).await;
    let shard = shard_ref(&broker, &cache_key(), 3);
    assert_eq!(shard.incarnation, None);
    assert_eq!(of(&broker, &cache_key()), 0);
    assert_eq!(expected(&broker, ShardKind::Cache, &shard), None);
}
