//! Hydrating a durable stream's replay ring on restart.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::{Broker, DurableStorage, StreamMetadata};

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker(storage: &DurableStorage) -> Broker {
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
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
    broker
}

/// A log ending in a generation-start record still hydrates the ring. The
/// window reaches the tail even though its last client record is one short
/// of it, so a cursor from before the restart replays from memory rather
/// than being told it is too old.
#[tokio::test]
async fn a_log_ending_in_a_generation_start_still_hydrates_the_ring() {
    let dir = tempfile::tempdir().expect("dir");
    let cursor;
    {
        let storage = DurableStorage::open(dir.path(), config()).expect("storage");
        let broker = broker(&storage).await;
        cursor = broker
            .cursor_tail("t1", "ns", "orders", 0)
            .await
            .expect("cursor");
        for value in ["a", "b"] {
            broker
                .publish("t1", "ns", "orders", Bytes::from(value))
                .await
                .expect("publish");
        }
        let log = storage.open_stream("t1", "ns", "orders", 0).expect("log");
        assert_eq!(log.append_generation_start(2).await.expect("marker"), 2);
        storage.shutdown().await.expect("shutdown");
    }

    let storage = DurableStorage::open(dir.path(), config()).expect("storage");
    let broker = broker(&storage).await;
    let (backlog, _sub) = broker
        .subscribe_with_cursor("t1", "ns", "orders", 0, cursor)
        .await
        .expect("replay from the ring");
    assert_eq!(backlog, vec![Bytes::from("a"), Bytes::from("b")]);
    let tail = broker
        .cursor_tail("t1", "ns", "orders", 0)
        .await
        .expect("tail");
    assert_eq!(tail.next_seq(), 3);
}
