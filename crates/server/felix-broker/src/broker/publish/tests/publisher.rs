//! The publisher a subscriber is told about, and what a durable stream keeps.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::{Broker, DurableStorage, StartPosition, StreamMetadata};

const ALICE: Bytes = Bytes::from_static(b"alice");

async fn broker_with(
    stream: &str,
    metadata: StreamMetadata,
    storage: Option<DurableStorage>,
) -> Broker {
    let mut broker = Broker::new(EphemeralCache::new().into())
        .with_log_capacity(2)
        .expect("capacity");
    if let Some(storage) = storage {
        broker = broker.with_durable_storage(storage);
    }
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", stream, metadata)
        .await
        .expect("register");
    broker
}

fn storage(dir: &std::path::Path) -> DurableStorage {
    DurableStorage::open(
        dir,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage")
}

fn durable() -> StreamMetadata {
    StreamMetadata {
        durable: true,
        ..StreamMetadata::default()
    }
}

async fn publish_as(
    broker: &Broker,
    stream: &str,
    payload: &'static str,
    publisher: Option<&Bytes>,
) {
    let handle = broker
        .resolve_stream_handle("t1", "default", stream, 0)
        .await
        .expect("handle");
    broker
        .publish_batch_with_outcome(
            &handle,
            &[Bytes::from_static(payload.as_bytes())],
            publisher,
        )
        .await
        .expect("publish");
}

/// An in-memory stream keeps nothing, so its subscribers are told the
/// publisher whatever the gate says.
#[tokio::test]
async fn an_in_memory_stream_tells_subscribers_who_published() {
    let broker = broker_with("live", StreamMetadata::default(), None).await;
    let resumed = broker
        .subscribe_from("t1", "default", "live", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();
    publish_as(&broker, "live", "a", Some(&ALICE)).await;
    let envelope = receiver.recv().await.expect("delivered");
    assert_eq!(envelope.publisher(), Some(&ALICE));
}

/// A durable stream tells subscribers only what it stores, and stores a
/// publisher only once the node allows it; replay, from the ring or from
/// disk, then reports it as live delivery did.
#[tokio::test]
async fn a_durable_stream_reports_only_the_publisher_it_stored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();

    publish_as(&broker, "orders", "before", Some(&ALICE)).await;
    assert_eq!(receiver.recv().await.expect("delivered").publisher(), None);

    broker.record_publishers_when(|| true);
    publish_as(&broker, "orders", "after", Some(&ALICE)).await;
    publish_as(&broker, "orders", "nobody", None).await;
    publish_as(&broker, "orders", "again", Some(&ALICE)).await;
    assert_eq!(
        receiver.recv().await.expect("delivered").publisher(),
        Some(&ALICE)
    );

    let stored: Vec<_> = broker
        .read_durable("t1", "default", "orders", 0, 0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| record.publisher)
        .collect();
    assert_eq!(stored, vec![None, Some(ALICE), None, Some(ALICE)]);

    // The ring holds the last two; the rest of the resume comes off disk.
    let replayed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect("resume");
    let history = replayed.history.expect("older than the ring");
    assert_eq!((history.from_offset, history.until_offset), (0, 2));
    let ring: Vec<_> = replayed
        .backlog
        .into_iter()
        .map(|record| (record.offset, record.publisher))
        .collect();
    assert_eq!(ring, vec![(2, None), (3, Some(ALICE))]);
}

/// A restarted broker fills its ring from disk with the publishers stored.
#[tokio::test]
async fn the_ring_is_refilled_with_publishers_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
        broker.record_publishers_when(|| true);
        publish_as(&broker, "orders", "a", Some(&ALICE)).await;
    }
    let broker = broker_with("orders", durable(), Some(storage(dir.path()))).await;
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect("resume");
    assert_eq!(resumed.backlog[0].publisher, Some(ALICE));
}
