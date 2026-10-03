use bytes::Bytes;
use felix_storage::EphemeralCache;

use crate::error::BrokerError;
use crate::{Broker, CacheMetadata, StreamMetadata};

#[tokio::test]
async fn existence_checks_reflect_registrations() {
    let broker = Broker::new(EphemeralCache::new().into());
    assert!(!broker.namespace_exists("t1", "default").await);
    assert!(!broker.cache_exists("t1", "default", "primary").await);
    assert!(!broker.stream_exists("t1", "default", "orders").await);

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_cache("t1", "default", "primary", CacheMetadata::default())
        .await
        .expect("cache");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");

    assert!(broker.namespace_exists("t1", "default").await);
    assert!(broker.cache_exists("t1", "default", "primary").await);
    assert!(broker.stream_exists("t1", "default", "orders").await);
}

#[tokio::test]
async fn register_namespace_requires_tenant() {
    let broker = Broker::new(EphemeralCache::new().into());
    let err = broker
        .register_namespace("missing", "default")
        .await
        .expect_err("tenant");
    assert!(matches!(err, BrokerError::TenantNotFound(_)));
}

#[tokio::test]
async fn register_stream_requires_namespace() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    let err = broker
        .register_stream("t1", "missing", "orders", StreamMetadata::default())
        .await
        .expect_err("namespace");
    assert!(matches!(err, BrokerError::NamespaceNotFound { .. }));
}

#[tokio::test]
async fn removed_stream_invalidates_resolved_handle() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");

    let err = broker
        .publish_batch_to_handle(&handle, &[Bytes::from_static(b"stale")])
        .await
        .expect_err("stale handle");
    assert!(matches!(err, BrokerError::StreamHandleInactive(id) if id == handle.id()));
}

#[tokio::test]
async fn remove_tenant_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    let removed = broker.remove_tenant("t1").await.expect("remove");
    assert!(removed);
    // Removing again returns false
    let removed_again = broker.remove_tenant("t1").await.expect("remove");
    assert!(!removed_again);
}

#[tokio::test]
async fn remove_namespace_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    assert!(broker.namespace_exists("t1", "default").await);
    let removed = broker
        .remove_namespace("t1", "default")
        .await
        .expect("remove");
    assert!(removed);
    assert!(!broker.namespace_exists("t1", "default").await);
}

#[tokio::test]
async fn remove_stream_succeeds() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    assert!(broker.stream_exists("t1", "default", "orders").await);
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    assert!(!broker.stream_exists("t1", "default", "orders").await);
}

/// A durable broker whose segments hold about two records each.
fn small_segment_broker(dir: &std::path::Path) -> Broker {
    let config = felix_storage::log::LogConfig {
        segment_size_bytes: felix_storage::segment::SEGMENT_HEADER_LEN + 80,
        index_spacing_bytes: 32,
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        // The test sweeps by hand.
        retention_check_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    Broker::new(EphemeralCache::new().into())
        .with_durable_storage(crate::DurableStorage::open(dir, config).expect("durable storage"))
}

async fn fill_and_sweep(broker: &Broker, stream: &str) -> u64 {
    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        broker
            .publish_batch("t1", "default", stream, 0, &[Bytes::from(payload)])
            .await
            .expect("publish");
    }
    let log = broker
        .shard_log(crate::LogKind::Stream, "t1", "default", stream, 0)
        .await
        .expect("log");
    log.enforce_retention_now().await.expect("sweep");
    log.base_offset()
}

/// **A stream's retention bounds its own log**, the others keep the broker's
/// (here none), and a later update reaches the open log without a restart.
#[tokio::test]
async fn a_streams_retention_bounds_its_log_and_updates_in_place() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = small_segment_broker(dir.path());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let bounded = StreamMetadata {
        durable: true,
        retention: felix_storage::log::Retention {
            bytes: Some(felix_storage::segment::SEGMENT_HEADER_LEN + 120),
            age: None,
        },
        ..StreamMetadata::default()
    };
    let unbounded = StreamMetadata {
        durable: true,
        ..StreamMetadata::default()
    };
    broker
        .register_stream("t1", "default", "orders", bounded.clone())
        .await
        .expect("orders");
    broker
        .register_stream("t1", "default", "events", unbounded)
        .await
        .expect("events");

    assert!(fill_and_sweep(&broker, "orders").await > 0);
    assert_eq!(fill_and_sweep(&broker, "events").await, 0);

    broker
        .register_stream("t1", "default", "events", bounded)
        .await
        .expect("events updated");
    assert!(fill_and_sweep(&broker, "events").await > 0);
}
