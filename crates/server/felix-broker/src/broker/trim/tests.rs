//! A head trim takes its place in the stream's order like a conditional
//! publish, and no reader is served below its floor once it is answered.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::StartPosition;
use parking_lot::Mutex;

use crate::stream::{ReadBound, ReadBounds};
use crate::{
    Broker, BrokerError, ConsistencyLevel, DurableStorage, StreamHandle, StreamMetadata,
    TrimOutcome,
};

#[derive(Debug)]
struct Bound(Mutex<ReadBound>);

impl ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        *self.0.lock()
    }
}

async fn broker(dir: &std::path::Path, consistency: ConsistencyLevel) -> (Broker, Arc<Bound>) {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
    let bound = Arc::new(Bound(Mutex::new(ReadBound::Unbounded)));
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn ReadBounds>);
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
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, bound)
}

async fn handle(broker: &Broker) -> StreamHandle {
    broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle")
}

/// Publish `count` records, `r0`, `r1`, ... continuing from the tail.
async fn publish(broker: &Broker, count: usize) {
    let handle = handle(broker).await;
    let tail = tail(broker).await;
    for n in tail..tail + count as u64 {
        broker
            .publish_batch_to_handle(&handle, &[Bytes::from(format!("r{n}"))])
            .await
            .expect("publish");
    }
}

async fn trim(broker: &Broker, before: u64, expected: u64) -> crate::Result<TrimOutcome> {
    broker
        .trim_before_at(
            &handle(broker).await,
            before,
            expected,
            Bytes::from_static(b"epoch-2"),
        )
        .await
}

async fn tail(broker: &Broker) -> u64 {
    handle(broker)
        .await
        .log()
        .expect("log")
        .tail_offset()
        .await
        .expect("tail")
}

async fn subscribe_from(broker: &Broker, offset: u64) -> crate::Result<crate::ResumedSubscription> {
    broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(offset))
        .await
}

async fn read(broker: &Broker, from: u64, max_bytes: usize) -> crate::Result<Vec<u64>> {
    broker
        .read_durable("t1", "ns", "orders", 0, from, max_bytes)
        .await
        .map(|records| records.iter().map(|record| record.offset).collect())
}

fn too_old<T: std::fmt::Debug>(result: crate::Result<T>) -> (u64, Option<Bytes>) {
    match result {
        Err(BrokerError::CursorTooOld {
            oldest,
            trim_headers,
            ..
        }) => (oldest, trim_headers),
        other => panic!("expected CursorTooOld, got {other:?}"),
    }
}

fn headers() -> Option<Bytes> {
    Some(Bytes::from_static(b"epoch-2"))
}

/// **A read below the floor is refused with the trim's headers.** That is
/// how a writer fenced by a newer trim learns who trimmed past it. The trim
/// record itself takes an offset and is never read back.
#[tokio::test]
async fn a_read_below_the_floor_is_too_old_and_names_the_trim() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 10).await;

    let outcome = trim(&broker, 5, 10).await.expect("trim");
    assert_eq!(
        outcome,
        TrimOutcome {
            offset: Some(10),
            oldest: 5
        }
    );
    assert_eq!(tail(&broker).await, 11);

    assert_eq!(too_old(read(&broker, 2, 1 << 20).await), (5, headers()));
    let page = broker
        .read_range("t1", "ns", "orders", 0, 2, None, 100, 1 << 20)
        .await;
    assert_eq!(too_old(page), (5, headers()));
    assert_eq!(too_old(subscribe_from(&broker, 2).await), (5, headers()));

    assert_eq!(
        read(&broker, 5, 1 << 20).await.expect("read"),
        [5, 6, 7, 8, 9]
    );
    let earliest = broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Earliest)
        .await
        .expect("earliest");
    assert_eq!(
        earliest.backlog.first().map(|record| record.offset),
        Some(5)
    );
}

/// **The ring never serves below the floor.** The replay ring still held
/// offsets 0 to 9 when the trim landed; a resume from 2 must not be handed
/// them out of memory.
#[tokio::test]
async fn the_ring_drops_what_the_trim_hides() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 10).await;
    let resumed = subscribe_from(&broker, 2).await.expect("before the trim");
    assert!(resumed.history.is_none(), "served from the ring");

    trim(&broker, 5, 10).await.expect("trim");
    assert_eq!(too_old(subscribe_from(&broker, 2).await), (5, headers()));

    let resumed = subscribe_from(&broker, 5).await.expect("at the floor");
    assert!(resumed.history.is_none(), "served from the ring");
    let backlog: Vec<u64> = resumed.backlog.iter().map(|r| r.offset).collect();
    assert_eq!(backlog, [5, 6, 7, 8, 9]);
}

/// **A stale trim writes nothing.** Of a trim and another write expecting
/// the same tail, exactly one lands.
#[tokio::test]
async fn a_trim_expecting_a_stale_tail_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 10).await;
    match trim(&broker, 5, 9).await {
        Err(BrokerError::OffsetMismatch { expected, tail }) => {
            assert_eq!((expected, tail), (9, 10))
        }
        other => panic!("expected OffsetMismatch, got {other:?}"),
    }
    assert_eq!(tail(&broker).await, 10);
    assert_eq!(read(&broker, 0, 1 << 20).await.expect("read").len(), 10);
}

/// **A no-op trim checks the tail first.** A trim at or below the floor
/// writes nothing, but one from a writer whose tail is stale is refused like
/// any other: answering it would tell that writer it still holds the tail.
#[tokio::test]
async fn a_no_op_trim_is_refused_when_its_tail_is_stale() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 10).await;
    trim(&broker, 5, 10).await.expect("trim");

    match trim(&broker, 3, 10).await {
        Err(BrokerError::OffsetMismatch { tail, .. }) => assert_eq!(tail, 11),
        other => panic!("expected OffsetMismatch, got {other:?}"),
    }
    let outcome = trim(&broker, 3, 11).await.expect("no-op");
    assert_eq!(
        outcome,
        TrimOutcome {
            offset: None,
            oldest: 5
        }
    );
    assert_eq!(tail(&broker).await, 11, "a no-op writes nothing");
}

/// **A trim that landed is not landed again.** A caller that stopped waiting
/// cannot tell whether it did, and a retry with the same tail is refused.
#[tokio::test]
async fn a_retried_trim_is_an_offset_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 4).await;
    trim(&broker, 2, 4).await.expect("trim");
    match trim(&broker, 3, 4).await {
        Err(BrokerError::OffsetMismatch { expected, tail }) => {
            assert_eq!((expected, tail), (4, 5))
        }
        other => panic!("expected OffsetMismatch, got {other:?}"),
    }
}

/// **A floor past the trim's own record is refused.** Storage takes any
/// floor, and one past the record would hide records written after it.
#[tokio::test]
async fn a_floor_past_the_trim_record_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 4).await;
    match trim(&broker, 5, 4).await {
        Err(BrokerError::TrimPastTail { before, offset }) => {
            assert_eq!((before, offset), (5, 4))
        }
        other => panic!("expected TrimPastTail, got {other:?}"),
    }
    assert_eq!(tail(&broker).await, 4);
    // At its own offset is the most it may ask for.
    trim(&broker, 4, 4).await.expect("trim at the record");
    publish(&broker, 1).await;
    assert_eq!(read(&broker, 4, 1 << 20).await.expect("read"), [5]);
}

/// **A stream holding atomic commits is not trimmed**, here or after a
/// restart, when only the log says it holds one.
#[tokio::test]
async fn a_commit_stream_refuses_a_trim() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
        publish(&broker, 3).await;
        broker
            .commit_to_handle(
                &handle(&broker).await,
                Bytes::from_static(b"e"),
                vec![],
                None,
            )
            .await
            .expect("commit");
        publish(&broker, 2).await;
        assert!(matches!(
            trim(&broker, 2, 6).await,
            Err(BrokerError::TrimOnCommitStream)
        ));
    }
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    assert!(matches!(
        trim(&broker, 2, 6).await,
        Err(BrokerError::TrimOnCommitStream)
    ));
    assert_eq!(tail(&broker).await, 6);
}

#[tokio::test]
async fn an_ephemeral_stream_refuses_a_trim() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "ns", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    assert!(matches!(
        trim(&broker, 0, 0).await,
        Err(BrokerError::TrimNeedsDurableStream)
    ));
}

/// **A trim racing a publish lands in one order or the other.** Either the
/// trim takes the tail it expects and the publish lands after it, or the
/// publish takes it and the trim is refused with nothing written. Never
/// both, never neither. Every client record reaches a live subscriber; no
/// trim does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trim_racing_a_publish_lands_in_one_order() {
    const ROUNDS: u64 = 40;
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    let broker = Arc::new(broker);
    let mut live = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");
    for round in 0..ROUNDS {
        publish(&broker, 1).await;
        let expected = tail(&broker).await;
        let trimming = {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move { trim(&broker, expected, expected).await })
        };
        let publishing = {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move {
                let handle = handle(&broker).await;
                let payload = [Bytes::from(format!("p{round}"))];
                broker
                    .publish_batch_with_outcome(&handle, &payload, None)
                    .await
            })
        };
        let trimmed = trimming.await.expect("trim task");
        let published = publishing.await.expect("publish task").expect("publish");
        let (at, _) = published.offsets.expect("offsets");
        match trimmed {
            Ok(outcome) => {
                assert_eq!(outcome.offset, Some(expected));
                assert_eq!(at, expected + 1, "the publish lands after the trim");
            }
            Err(BrokerError::OffsetMismatch { tail, .. }) => {
                assert_eq!(at, expected, "the publish took the tail");
                assert_eq!(tail, expected + 1);
            }
            Err(err) => panic!("unexpected trim error: {err}"),
        }
        // Whatever the order, the published record is above the floor.
        let readable = read(&broker, expected, 1 << 20).await.expect("read");
        assert!(readable.contains(&at), "{readable:?} lacks {at}");
    }

    let mut delivered = 0;
    while let Ok(Some(payload)) =
        tokio::time::timeout(Duration::from_millis(200), live.recv()).await
    {
        assert!(
            payload.starts_with(b"r") || payload.starts_with(b"p"),
            "{payload:?}"
        );
        delivered += 1;
    }
    assert_eq!(delivered, 2 * ROUNDS);
}

/// **A subscriber reading history across a trim is told it is too old**
/// rather than handed a short history that looks complete.
#[tokio::test]
async fn a_history_read_across_a_trim_is_too_old() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    publish(&broker, 10).await;
    assert_eq!(read(&broker, 0, 1).await.expect("first page"), [0]);
    trim(&broker, 5, 10).await.expect("trim");
    assert_eq!(too_old(read(&broker, 1, 1 << 20).await), (5, headers()));
}

/// **On a `Quorum` stream the ring keeps its entries until the mark passes
/// the trim**, as it holds a publish back until then, and drops them when it
/// does.
#[tokio::test]
async fn a_quorum_trim_reaches_the_ring_once_committed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bound) = broker(dir.path(), ConsistencyLevel::Quorum).await;
    publish(&broker, 10).await;
    *bound.0.lock() = ReadBound::Committed(10);
    let outcome = trim(&broker, 5, 10).await.expect("trim");
    assert_eq!(outcome.offset, Some(10));
    let ring_front = |handle: &StreamHandle| {
        handle
            .state
            .log_state
            .lock()
            .log
            .front()
            .map(|entry| entry.seq)
    };
    assert_eq!(ring_front(&handle(&broker).await), Some(0), "held");

    *bound.0.lock() = ReadBound::Committed(11);
    let handle = handle(&broker).await;
    handle.release_committed();
    tokio::time::timeout(Duration::from_secs(5), async {
        while ring_front(&handle) != Some(5) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("released");
}

/// **A restart with trims at the tail keeps its replay window.** The read
/// skips trim records, so the ring must still be seen to reach the tail, and
/// the first publish after them reports them as skipped.
#[tokio::test]
async fn a_restart_with_trims_at_the_tail_refills_the_ring() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
        publish(&broker, 10).await;
        trim(&broker, 5, 10).await.expect("trim");
        trim(&broker, 6, 11).await.expect("second trim");
        let run = handle(&broker)
            .await
            .state
            .log_state
            .lock()
            .generation_start_run;
        assert_eq!(run, Some((12, 2)));
    }
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    let resumed = subscribe_from(&broker, 6).await.expect("at the floor");
    assert!(resumed.history.is_none(), "served from the ring");
    let backlog: Vec<u64> = resumed.backlog.iter().map(|r| r.offset).collect();
    assert_eq!(backlog, [6, 7, 8, 9]);
    assert_eq!(too_old(subscribe_from(&broker, 5).await).0, 6);
    let run = handle(&broker)
        .await
        .state
        .log_state
        .lock()
        .generation_start_run;
    assert_eq!(run, Some((12, 2)));
}
