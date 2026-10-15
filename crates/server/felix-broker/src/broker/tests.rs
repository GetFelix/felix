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
