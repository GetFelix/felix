//! Conditional publishes and commits: written only at the offset the writer
//! expects.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::error::BrokerError;
use crate::{Broker, DurableStorage, StreamHandle, StreamMetadata};

async fn durable_broker(dir: &std::path::Path) -> (Arc<Broker>, StreamHandle) {
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
            "match",
            StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "match", 0)
        .await
        .expect("handle");
    (Arc::new(broker), handle)
}

async fn stored(handle: &StreamHandle) -> Vec<Bytes> {
    handle
        .log()
        .expect("durable")
        .read_from(0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| record.payload)
        .collect()
}

/// **Two writers that read the same tail cannot both append.** Every writer
/// expects offset 1; exactly one lands there, and every other is refused
/// with the tail the winner left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn of_writers_racing_at_one_offset_exactly_one_wins() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .publish_batch_with_outcome(&handle, &[Bytes::from_static(b"start")], None)
        .await
        .expect("publish");

    let writers: Vec<_> = (0..32)
        .map(|i| {
            let broker = Arc::clone(&broker);
            let handle = handle.clone();
            tokio::spawn(async move {
                let payload = Bytes::from(format!("writer-{i}"));
                broker.publish_batch_at(&handle, &[payload], 1, None).await
            })
        })
        .collect();
    let mut won = Vec::new();
    for writer in writers {
        match writer.await.expect("task") {
            Ok(outcome) => won.push(outcome.offsets),
            Err(BrokerError::OffsetMismatch { expected, tail }) => {
                assert_eq!((expected, tail), (1, 2));
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    assert_eq!(won, [Some((1, 1))], "exactly one writer appends");
    assert_eq!(stored(&handle).await.len(), 2);
}

/// A refused publish writes nothing and consumes no offset, so the next
/// publish takes the offset it would have had and is not held up waiting on
/// a commit turn nobody will finish.
#[tokio::test]
async fn a_refused_publish_leaves_no_record_and_no_gap() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    let mut sub = broker
        .subscribe("t1", "ns", "match", 0)
        .await
        .expect("subscribe");

    let refused = broker
        .publish_batch_at(&handle, &[Bytes::from_static(b"late")], 5, None)
        .await
        .expect_err("the tail is 0");
    assert!(matches!(
        refused,
        BrokerError::OffsetMismatch {
            expected: 5,
            tail: 0
        }
    ));

    let next = tokio::time::timeout(
        Duration::from_secs(5),
        broker.publish_batch_with_outcome(&handle, &[Bytes::from_static(b"next")], None),
    )
    .await
    .expect("the next publish was held up")
    .expect("publish");
    assert_eq!(next.offsets, Some((0, 0)));
    assert_eq!(sub.recv().await.expect("next"), Bytes::from_static(b"next"));
    assert_eq!(stored(&handle).await, [Bytes::from_static(b"next")]);

    let landed = broker
        .publish_batch_at(
            &handle,
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            1,
            None,
        )
        .await
        .expect("the tail is 1");
    assert_eq!(landed.offsets, Some((1, 2)));
}

/// A promotion appends a generation-start record, so a writer that expected
/// the old leader's tail is refused even with no rival writer, and the
/// refusal carries the tail it has to start from.
#[tokio::test]
async fn a_generation_start_moves_the_expected_offset() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .publish_batch_at(&handle, &[Bytes::from_static(b"a")], 0, None)
        .await
        .expect("first");
    let generation_start = broker
        .append_generation_start("t1", "ns", "match", 0, 2)
        .await
        .expect("generation start");
    assert_eq!(generation_start, 1);

    let refused = broker
        .publish_batch_at(&handle, &[Bytes::from_static(b"b")], 1, None)
        .await
        .expect_err("stale after the promotion");
    assert!(matches!(
        refused,
        BrokerError::OffsetMismatch {
            expected: 1,
            tail: 2
        }
    ));
    let landed = broker
        .publish_batch_at(&handle, &[Bytes::from_static(b"b")], 2, None)
        .await
        .expect("at the new tail");
    assert_eq!(landed.offsets, Some((2, 2)));
}

/// A stream with no log has no offsets to compare against.
#[tokio::test]
async fn an_in_memory_stream_refuses_an_expected_offset() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "ns", "live", StreamMetadata::default())
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "live", 0)
        .await
        .expect("handle");
    let refused = broker
        .publish_batch_at(&handle, &[Bytes::from_static(b"a")], 0, None)
        .await
        .expect_err("no log");
    assert!(matches!(
        refused,
        BrokerError::ExpectedOffsetNeedsDurableStream
    ));
}

/// A commit with an expected offset is a compare-and-set on the whole shard:
/// refused once anything else was appended, with its state left as it was.
#[tokio::test]
async fn a_commit_at_a_stale_offset_changes_no_state() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    let put = |value: &'static [u8]| {
        vec![crate::StateOp::Put {
            key: "score".to_string(),
            value: Bytes::from_static(value),
        }]
    };
    let first = broker
        .commit_to_handle_at(&handle, Bytes::from_static(b"e1"), put(b"1"), Some(0), None)
        .await
        .expect("commit at 0");
    assert_eq!(first.offsets, Some((0, 0)));

    broker
        .publish_batch_with_outcome(&handle, &[Bytes::from_static(b"other")], None)
        .await
        .expect("publish");
    let refused = broker
        .commit_to_handle_at(&handle, Bytes::from_static(b"e2"), put(b"2"), Some(1), None)
        .await
        .expect_err("something was appended since");
    assert!(matches!(
        refused,
        BrokerError::OffsetMismatch {
            expected: 1,
            tail: 2
        }
    ));
    let read = broker.state_get(&handle, "score").await.expect("read");
    assert_eq!(read.value, Some(Bytes::from_static(b"1")));
    assert_eq!(read.version, Some(0));
}
