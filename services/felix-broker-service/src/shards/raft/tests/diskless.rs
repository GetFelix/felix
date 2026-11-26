//! A cluster member without durable storage runs no Raft group, and still
//! leaves no durable stream's or cache's shard to the hybrid.

use super::*;

/// A broker with no durable storage, with `ticks` (in memory) and the cache
/// `sessions` registered. A durable stream such as `orders` is never
/// registered there: the catalog sync skips it.
async fn diskless_broker() -> Arc<Broker> {
    let broker = Broker::new(felix_storage::EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "ticks",
            StreamMetadata {
                durable: false,
                incarnation: INCARNATION,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("stream");
    let metadata = felix_broker::CacheMetadata {
        consistency: ConsistencyLevel::Leader,
        incarnation: INCARNATION,
        replication_factor: Some(3),
    };
    broker
        .register_cache("t1", "ns", "sessions", metadata)
        .await
        .expect("cache");
    Arc::new(broker)
}

/// **The Raft engine takes a cache's shard on a broker without a disk.**
/// Every cache in a Raft cluster is a group on brokers with disks, so this
/// broker only routes to it, as it does a durable stream's. Left in the feed,
/// the shard would go to the hybrid lifecycle, which routes by the assigned
/// leader instead of the group's.
#[tokio::test]
async fn a_diskless_broker_leaves_only_in_memory_shards_in_the_feed() {
    let broker = diskless_broker().await;
    assert!(broker.cache().as_log_cache().is_none());
    let shards = held_shards(&broker);
    let sessions = ShardKey {
        kind: ShardKind::Cache,
        ..key("sessions", 0)
    };
    let elsewhere = |key: ShardKey| {
        let mut assignment = assigned(key, INCARNATION);
        assignment.leader = "broker-b".to_string();
        assignment.replicas = ["broker-b", "broker-c", "broker-d"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        assignment
    };
    let mut feed = assignments([
        elsewhere(key("orders", 0)),
        elsewhere(key("ticks", 0)),
        elsewhere(sessions.clone()),
    ]);

    shards.take(&mut feed).await;

    let left: Vec<&ShardKey> = feed.keys().collect();
    assert_eq!(left, vec![&key("ticks", 0)], "left for the in-memory path");
    assert!(
        shards.runtime.status(&sessions).is_none(),
        "no member opens"
    );
    EngineShards::shutdown(&*shards).await;
}
