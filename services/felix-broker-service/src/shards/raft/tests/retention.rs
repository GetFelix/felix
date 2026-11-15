//! Retention on a Raft shard: its leader hides what the stream's bounds drop.

use bytes::Bytes;
use felix_broker::{BrokerError, StartPosition};
use felix_storage::log::Retention;

use super::*;

/// **The leader hides what the stream's own retention bounds drop, on the
/// retention timer, and deletes nothing.** The bounds come from the stream's
/// registration, as the control plane sends them, not from the broker; a
/// Raft log refuses bounds of its own, so without the leader's trim they
/// would never be enforced.
#[tokio::test]
async fn the_leader_trims_what_the_streams_retention_drops() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        segment_size_bytes: 512,
        retention_check_interval: Duration::from_millis(20),
        ..LogConfig::default()
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        config,
    )
    .await;
    broker.allow_raft_head_trims_when(|| true);
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation: INCARNATION,
                replication_factor: Some(1),
                consistency: ConsistencyLevel::Leader,
                retention: Retention {
                    bytes: Some(2048),
                    age: None,
                },
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("the stream's bounds");
    let shards = shards(&broker);
    lead_orders(&shards).await;
    let orders = key("orders", 0);
    let term = shards.leading(&orders).expect("term");
    let engine = engine(&shards);
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let mut offsets = Vec::new();
    for _ in 0..64 {
        let offset = crate::serving::direct_publish::publish_direct(
            &broker,
            None,
            &engine,
            target(handle.clone(), &orders, term),
            &[Bytes::from(vec![7u8; 64])],
            None,
            Duration::from_secs(10),
        )
        .await
        .expect("published")
        .expect("an offset");
        offsets.push(offset);
    }
    let log = shards.log_of(&orders).await.expect("log");
    let first = offsets[0];

    until("retention to raise the floor", || {
        log.head_trim().is_some_and(|trim| trim.floor > first)
    })
    .await;

    let base = log.base_offset();
    assert!(base <= first, "a Raft log's entries go only by a purge");
    assert!(
        matches!(
            broker
                .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
                .await,
            Err(BrokerError::CursorTooOld { .. })
        ),
        "a resume below the floor was served"
    );
    shards.runtime.shutdown().await;
}

/// **A Raft member's log does not offload**, even on a broker that offloads
/// its hybrid logs. Only a purge may drop a Raft log's entries, and an
/// offload pass reads a stream log's layout.
#[tokio::test]
async fn a_raft_member_does_not_offload() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        offload: Some(felix_storage::log::OffloadTarget::LocalDir {
            dir: root.path().join("archive"),
            node: "broker-a".to_string(),
        }),
        ..LogConfig::default()
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        config,
    )
    .await;
    let shards = shards(&broker);
    lead_orders(&shards).await;
    let log = shards.log_of(&key("orders", 0)).await.expect("log");
    assert_eq!(log.config().offload, None);
    shards.runtime.shutdown().await;
}
