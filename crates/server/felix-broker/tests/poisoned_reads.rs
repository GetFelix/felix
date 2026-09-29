//! Reads from a `Leader` stream after a failed flush poisons its log.
//!
//! Its own test binary because the injected fsync failure is process-wide.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_broker::{Broker, ConsumerGroups, DeadLetters, DurableStorage, GroupKey, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::fault::{FsyncFailure, set_fsync_failure};
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::tempdir;

/// The failed batch is written but never durable, and its publish was
/// refused, so no reader may see it: not a resumed subscription, not a
/// group poll.
#[tokio::test]
async fn readers_stop_at_the_durable_offset_once_a_flush_poisons_the_log() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage =
        DurableStorage::open(dir.path().join("streams"), config.clone()).expect("storage");
    let groups =
        Arc::new(ConsumerGroups::open(dir.path().join("groups"), config.clone()).expect("groups"));
    let dead = Arc::new(DeadLetters::open(dir.path().join("dead"), config).expect("dead letters"));
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage.clone())
        .with_consumer_groups(groups, dead, Duration::from_secs(30), 5);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("register");

    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"kept"))
        .await
        .expect("publish");
    set_fsync_failure(FsyncFailure::Always);
    let failed = broker
        .publish("t1", "default", "orders", Bytes::from_static(b"refused"))
        .await;
    set_fsync_failure(FsyncFailure::None);
    failed.expect_err("a publish whose flush fails is refused");

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    // The refused batch is on the log's tail, past what is durable.
    assert_eq!(log.durable_offset(), 1);
    assert_eq!(log.tail_offset().await.expect("tail"), 2);

    let tail = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    assert_eq!(tail.next_seq(), 1);

    let history = broker
        .read_committed("t1", "default", "orders", 0, 0, 64 * 1024)
        .await
        .expect("history");
    let offsets: Vec<u64> = history.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, vec![0]);

    let reader = broker.group_reader().expect("group reader");
    let key = GroupKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        group: "billing".to_string(),
    };
    let claimed = reader
        .poll(&key, &log, 10, Instant::now())
        .await
        .expect("poll");
    let offsets: Vec<u64> = claimed.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, vec![0]);
}
