use bytes::Bytes;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::{CacheChange, CacheObserver, EphemeralCache, LogCache};

use crate::Broker;
use crate::error::BrokerError;

#[test]
fn zero_capacity_is_rejected() {
    let broker = Broker::new(EphemeralCache::new().into());
    let err = broker.with_topic_capacity(0).expect_err("capacity");
    assert!(matches!(err, BrokerError::CapacityTooLarge));
}

/// The watch byte bound set on the broker reaches the hub that serves watches,
/// whatever else the builder chain does around it.
#[tokio::test]
async fn the_cache_watch_byte_bound_reaches_the_hub() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let cache = LogCache::open(dir.path(), config).expect("open cache");
    let broker = Broker::new(Box::new(cache)).with_cache_watch_max_bytes(100, 0);
    let hub = broker
        .cache_watches()
        .expect("a log-backed cache has a hub");
    let mut watch = hub.register(
        "t1",
        "ns",
        "sessions",
        0,
        crate::CacheWatchFilter::Prefix(String::new()),
        8,
    );
    for offset in 0..2 {
        hub.cache_changed(CacheChange {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "sessions".to_string(),
            shard: 0,
            key: "k".to_string(),
            value: Some(Bytes::from(vec![0u8; 59])),
            offset,
            expires_at_millis: 0,
        });
    }
    assert_eq!(watch.recv().await.expect("queued").offset, 0);
    assert_eq!(watch.recv().await, None);
    assert_eq!(watch.lagged(), Some(1));
}

/// **A clean stop flushes every store, not only stream logs (#1284).** The
/// next start keeps the storage id only after one, so a store left unsynced
/// would let a power loss right after the stop lose records peers counted.
async fn flushed_on_shutdown(kind: crate::LogKind) {
    use crate::{ConsumerGroups, DeadLetters, DurableStorage};

    let dir = tempfile::tempdir().expect("tempdir");
    // Never synced until something asks, so a flush is visible.
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let root = dir.path();
    let cache = LogCache::open(root.join("caches"), config.clone()).expect("cache");
    let groups = std::sync::Arc::new(
        ConsumerGroups::open(root.join("groups"), config.clone()).expect("groups"),
    );
    let dead = std::sync::Arc::new(
        DeadLetters::open(root.join("dead-letters"), config.clone()).expect("dead letters"),
    );
    let counters = std::sync::Arc::new(
        felix_storage::CounterStore::open(root.join("counters"), config.clone()).expect("counters"),
    );
    let streams = DurableStorage::open(root, config).expect("streams");
    let broker = Broker::new(Box::new(cache))
        .with_consumer_groups(groups, dead, std::time::Duration::from_secs(30), 5)
        .with_durable_storage(streams)
        .with_counters(counters);

    let log = broker
        .shard_log(kind, "t1", "ns", "orders", 0)
        .await
        .unwrap_or_else(|| panic!("{kind:?} log"));
    log.append(&[Bytes::from_static(b"unsynced")])
        .await
        .expect("append");
    assert!(log.unsynced_bytes() > 0, "{kind:?} was synced already");

    broker.shutdown_storage().await.expect("flush");
    assert_eq!(log.unsynced_bytes(), 0, "{kind:?} was not flushed");
}

#[tokio::test]
async fn shutdown_storage_flushes_stream_logs() {
    flushed_on_shutdown(crate::LogKind::Stream).await;
}

#[tokio::test]
async fn shutdown_storage_flushes_the_cache() {
    flushed_on_shutdown(crate::LogKind::Cache).await;
}

#[tokio::test]
async fn shutdown_storage_flushes_group_cursors() {
    flushed_on_shutdown(crate::LogKind::GroupCursors).await;
}

#[tokio::test]
async fn shutdown_storage_flushes_dead_letters() {
    flushed_on_shutdown(crate::LogKind::GroupDeadLetters).await;
}

#[tokio::test]
async fn shutdown_storage_flushes_counters() {
    flushed_on_shutdown(crate::LogKind::Counters).await;
}
