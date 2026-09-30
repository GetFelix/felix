//! Readers of a `Quorum` stream see nothing past the committed mark, and a
//! `Leader` stream is untouched by it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::StartPosition;
use parking_lot::Mutex;

use super::{ReadBound, ReadBounds};
use crate::{
    Broker, BrokerError, ConsistencyLevel, DurableStorage, NotReadable, StreamMetadata,
    Subscription,
};

/// A bound the test moves by hand, counting how often it is asked.
#[derive(Debug)]
struct Bounds {
    bound: Mutex<ReadBound>,
    asked: AtomicUsize,
}

impl Bounds {
    fn set(&self, bound: ReadBound) {
        *self.bound.lock() = bound;
    }
}

impl ReadBounds for Bounds {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        self.asked.fetch_add(1, Ordering::SeqCst);
        *self.bound.lock()
    }
}

async fn broker_with(dir: &std::path::Path, bound: ReadBound) -> (Broker, Arc<Bounds>) {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
    let bounds = Arc::new(Bounds {
        bound: Mutex::new(bound),
        asked: AtomicUsize::new(0),
    });
    broker.set_read_bounds(Arc::clone(&bounds) as Arc<dyn ReadBounds>);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    for (name, consistency) in [
        ("quorum", ConsistencyLevel::Quorum),
        ("leader", ConsistencyLevel::Leader),
    ] {
        broker
            .register_stream(
                "t1",
                "ns",
                name,
                StreamMetadata {
                    durable: true,
                    consistency,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
    }
    (broker, bounds)
}

async fn publish(broker: &Broker, stream: &str, payload: &'static [u8]) -> crate::PublishOutcome {
    let handle = broker
        .resolve_stream_handle("t1", "ns", stream, 0)
        .await
        .expect("handle");
    broker
        .publish_batch_with_outcome(&handle, &[Bytes::from_static(payload)])
        .await
        .expect("publish")
}

async fn release(broker: &Broker, stream: &str) {
    broker
        .resolve_stream_handle("t1", "ns", stream, 0)
        .await
        .expect("handle")
        .release_committed();
}

async fn next(sub: &mut Subscription) -> Bytes {
    tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("delivered in time")
        .expect("subscription open")
}

async fn nothing(sub: &mut Subscription) {
    let got = tokio::time::timeout(Duration::from_millis(150), sub.recv()).await;
    assert!(got.is_err(), "delivered past the committed mark: {got:?}");
}

/// **A batch past the committed mark reaches no subscriber and not the ring
/// until the mark passes it, and then in offset order.** Delivered at
/// durability instead, a subscriber could hold an offset a failover gives to a
/// different record.
#[tokio::test]
async fn a_quorum_batch_waits_for_the_mark_and_is_released_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");

    let first = publish(&broker, "quorum", b"first").await;
    let second = publish(&broker, "quorum", b"second").await;
    assert_eq!(first.offsets, Some((0, 0)));
    assert_eq!(second.offsets, Some((1, 1)));
    nothing(&mut live).await;

    let cursor = broker
        .cursor_tail("t1", "ns", "quorum", 0)
        .await
        .expect("cursor");
    assert_eq!(
        cursor.next_seq(),
        0,
        "the tail a reader is given is the mark"
    );
    let (backlog, _) = broker
        .subscribe_with_cursor("t1", "ns", "quorum", 0, cursor)
        .await
        .expect("replay");
    assert!(backlog.is_empty(), "the ring holds a record past the mark");

    bounds.set(ReadBound::Committed(1));
    release(&broker, "quorum").await;
    assert_eq!(&next(&mut live).await[..], b"first");
    nothing(&mut live).await;

    bounds.set(ReadBound::Committed(2));
    release(&broker, "quorum").await;
    assert_eq!(&next(&mut live).await[..], b"second");
}

/// **A `Leader` stream behaves exactly as before**: delivered at durability,
/// counted as delivered, and the bound is never even asked for.
#[tokio::test]
async fn a_leader_stream_is_never_held_or_asked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    let mut live = broker
        .subscribe("t1", "ns", "leader", 0)
        .await
        .expect("subscribe");

    let outcome = publish(&broker, "leader", b"now").await;
    assert_eq!(outcome.subscribers, 1, "fanned out in the publish itself");
    assert_eq!(&next(&mut live).await[..], b"now");
    let cursor = broker
        .cursor_tail("t1", "ns", "leader", 0)
        .await
        .expect("cursor");
    assert_eq!(cursor.next_seq(), 1);
    assert_eq!(bounds.asked.load(Ordering::SeqCst), 0);
}

/// **A `Quorum` stream whose shard is unbounded -- no replicas, or no
/// cluster -- is not held either.** The publish fans out itself, as it did
/// before the mark gated anything.
#[tokio::test]
async fn an_unbounded_quorum_stream_is_delivered_in_the_publish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");

    let outcome = publish(&broker, "quorum", b"now").await;
    assert_eq!(outcome.subscribers, 1, "fanned out in the publish itself");
    assert_eq!(&next(&mut live).await[..], b"now");
}

/// **Held batches die with the leadership.** A shard given up drops what it
/// held rather than delivering it later as committed; a mark reaching it
/// afterwards releases nothing.
#[tokio::test]
async fn held_batches_are_dropped_when_the_shard_is_given_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"orphan").await;

    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 1);
    bounds.set(ReadBound::Unbounded);
    release(&broker, "quorum").await;
    nothing(&mut live).await;

    // The stream still works afterwards, from its next offset.
    let outcome = publish(&broker, "quorum", b"next").await;
    assert_eq!(outcome.offsets, Some((1, 1)));
    assert_eq!(&next(&mut live).await[..], b"next");
}

/// **A batch shipped again after the hold was dropped does not wind the
/// commit order back.** The dropped batches are still on disk, so the commit
/// order stands past them while the ring's position does not. A leader that
/// ships a batch this broker already has is answered with that batch's end,
/// below the tail. Moved back to it, the order waits for a turn below the tail
/// that nobody will take, and every later publish hangs.
#[tokio::test]
async fn a_batch_shipped_again_after_the_hold_was_dropped_does_not_wedge_publishes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    publish(&broker, "quorum", b"first").await;
    publish(&broker, "quorum", b"second").await;
    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 2);

    // The next leader ships the first batch, which this broker already holds.
    broker
        .adopt_replicated("t1", "ns", "quorum", 0, 1)
        .await
        .expect("adopt");

    bounds.set(ReadBound::Unbounded);
    let outcome = tokio::time::timeout(Duration::from_secs(5), publish(&broker, "quorum", b"next"))
        .await
        .expect("the publish waited on a turn nobody holds");
    assert_eq!(outcome.offsets, Some((2, 2)));
}

/// **A resume joins at the committed mark, not at the tail**, and history is
/// read only up to the mark, waiting for it to move.
#[tokio::test]
async fn a_resume_joins_at_the_mark_and_its_history_waits_for_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    for payload in [&b"a"[..], b"b", b"c"] {
        let handle = broker
            .resolve_stream_handle("t1", "ns", "quorum", 0)
            .await
            .expect("handle");
        broker
            .publish_batch_with_outcome(&handle, &[Bytes::copy_from_slice(payload)])
            .await
            .expect("publish");
    }
    bounds.set(ReadBound::Committed(1));

    let latest = broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Latest)
        .await
        .expect("latest");
    let join = latest.join.expect("durable");
    assert_eq!(join.start_offset, 1, "Latest is the mark");
    assert_eq!(join.live_offset, 3, "live starts at what was written");

    // An offset between the mark and the tail is a reader that got there on
    // an earlier leader; it is served once the mark catches up.
    let ahead = broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Offset(2))
        .await
        .expect("an offset up to the tail is accepted");
    assert_eq!(ahead.join.expect("durable").start_offset, 2);
    let beyond = broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Offset(4))
        .await;
    assert!(matches!(beyond, Err(BrokerError::CursorInFuture { .. })));

    let first = broker
        .read_committed("t1", "ns", "quorum", 0, 0, 1 << 20)
        .await
        .expect("read");
    assert_eq!(
        first.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0],
        "a history page runs past the mark",
    );

    let reader = {
        let broker = Arc::new(broker);
        let waiting = Arc::clone(&broker);
        (
            tokio::spawn(async move {
                waiting
                    .read_committed("t1", "ns", "quorum", 0, 1, 1 << 20)
                    .await
            }),
            broker,
        )
    };
    let (waiting, broker) = reader;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "read at the mark did not wait for it"
    );
    bounds.set(ReadBound::Committed(3));
    broker
        .resolve_stream_handle("t1", "ns", "quorum", 0)
        .await
        .expect("handle")
        .appended()
        .notify_waiters();
    let rest = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("woken by the mark")
        .expect("task")
        .expect("read");
    assert_eq!(
        rest.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

/// **A broker that cannot say what is committed serves no read that depends
/// on it**, and says why, so the client retries rather than trusting silence.
#[tokio::test]
async fn reads_are_refused_while_the_bound_is_unknown_or_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Settling).await;
    let latest = broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Latest)
        .await;
    assert!(matches!(
        latest,
        Err(BrokerError::NotReadable {
            reason: NotReadable::Settling,
            ..
        })
    ));
    // From an offset needs no mark to start; history waits for one.
    broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Offset(0))
        .await
        .expect("an offset needs no mark to join");

    bounds.set(ReadBound::Refused);
    let refused = broker
        .subscribe_from("t1", "ns", "quorum", 0, StartPosition::Offset(0))
        .await;
    assert!(matches!(
        refused,
        Err(BrokerError::NotReadable {
            reason: NotReadable::Refused,
            ..
        })
    ));
    let history = broker
        .read_committed("t1", "ns", "quorum", 0, 0, 1 << 20)
        .await;
    assert!(matches!(history, Err(BrokerError::NotReadable { .. })));
}
