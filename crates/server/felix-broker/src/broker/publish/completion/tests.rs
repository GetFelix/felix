//! A completion whose caller goes away still finishes.

use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::{Broker, DurableStorage, StreamMetadata};

async fn durable_broker(dir: &std::path::Path) -> Broker {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
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
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    broker
}

/// **A publish cancelled after it claimed its offsets still reaches the ring
/// and its subscribers.** Its records are on disk and replicate regardless;
/// skipping the fanout left a hole live subscribers never heard about.
///
/// The second batch waits on the first one's turn, and its caller gives up
/// there, which is exactly where a client timeout lands.
#[tokio::test]
async fn a_cancelled_completion_still_reaches_the_ring_and_subscribers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = durable_broker(dir.path()).await;
    let mut live = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let start = broker
        .cursor_tail("t1", "ns", "orders", 0)
        .await
        .expect("cursor");

    let first = broker
        .claim_publish(&handle, &[Bytes::from_static(b"first")])
        .await
        .expect("claim first");
    let second = broker
        .claim_publish(&handle, &[Bytes::from_static(b"second")])
        .await
        .expect("claim second");
    let abandoned =
        tokio::time::timeout(Duration::from_millis(50), broker.complete_publish(second)).await;
    assert!(abandoned.is_err(), "the second batch waits on the first");
    broker.complete_publish(first).await.expect("first");

    for expected in [&b"first"[..], &b"second"[..]] {
        let got = tokio::time::timeout(Duration::from_secs(5), live.recv())
            .await
            .expect("delivered")
            .expect("subscription open");
        assert_eq!(&got[..], expected);
    }
    let (backlog, _) = broker
        .subscribe_with_cursor("t1", "ns", "orders", 0, start)
        .await
        .expect("replay");
    assert_eq!(
        backlog,
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")],
        "the cancelled batch is missing from the replay ring",
    );
}
