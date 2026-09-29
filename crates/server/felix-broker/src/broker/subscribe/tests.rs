use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::error::BrokerError;
use crate::{Broker, Cursor, DurableStorage, StartPosition, StreamMetadata};

#[tokio::test]
async fn cursor_replays_log_then_streams_new_events() {
    let broker = Broker::new(EphemeralCache::new().into());

    broker.register_tenant("t1").await.expect("tenant");

    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    let (backlog, mut sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, cursor)
        .await
        .expect("subscribe");
    assert_eq!(
        backlog,
        vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")]
    );
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"three"))
        .await
        .expect("publish");
    assert_eq!(
        sub.recv().await.expect("recv"),
        Bytes::from_static(b"three")
    );
}

#[tokio::test]
async fn subscribe_drop_unregisters_subscriber() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");

    let stream_state = broker
        .get_stream_state("t1", "default", "orders", 0)
        .await
        .expect("stream state");
    assert_eq!(stream_state.subscriber_count(), 0);

    let sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    assert_eq!(stream_state.subscriber_count(), 1);
    drop(sub);
    assert_eq!(stream_state.subscriber_count(), 0);
}

#[tokio::test]
async fn subscribe_to_nonexistent_stream_errors() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let err = broker
        .subscribe("t1", "default", "missing", 0)
        .await
        .expect_err("stream");
    assert!(matches!(err, BrokerError::StreamNotFound { .. }));
}

#[tokio::test]
async fn cursor_methods() {
    let cursor = Cursor { next_seq: 42 };
    assert_eq!(cursor.next_seq(), 42);
}

/// A reader that stops at `live_offset` has everything, even when the log
/// ends in generation-start records a new leader wrote and no client write
/// followed. Those offsets carry no event, so a `live_offset` at the raw tail
/// left a caught-up reader waiting forever.
#[tokio::test]
async fn live_offset_stops_short_of_trailing_generation_starts() {
    let dir = tempfile::tempdir().expect("dir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    for value in ["a", "b"] {
        broker
            .publish("t1", "ns", "orders", Bytes::from(value))
            .await
            .expect("publish");
    }
    for generation in [3, 4] {
        broker
            .append_generation_start("t1", "ns", "orders", 0, generation)
            .await
            .expect("generation start");
    }

    let join = |start| {
        let broker = &broker;
        async move {
            broker
                .subscribe_from("t1", "ns", "orders", 0, start)
                .await
                .expect("subscribe")
                .join
                .expect("durable join")
        }
    };
    let earliest = join(StartPosition::Earliest).await;
    assert_eq!((earliest.start_offset, earliest.live_offset), (0, 2));
    // Never below where the subscription starts.
    let latest = join(StartPosition::Latest).await;
    assert_eq!((latest.start_offset, latest.live_offset), (4, 4));
    let mid = join(StartPosition::Offset(3)).await;
    assert_eq!((mid.start_offset, mid.live_offset), (3, 3));

    // The next event lands at the tail and counts as written after the join.
    broker
        .publish("t1", "ns", "orders", Bytes::from("c"))
        .await
        .expect("publish");
    let after = join(StartPosition::Earliest).await;
    assert_eq!(after.live_offset, 5);
}
