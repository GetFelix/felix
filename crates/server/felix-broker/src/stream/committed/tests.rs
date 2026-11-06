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
        .publish_batch_with_outcome(&handle, &[Bytes::from_static(payload)], None, None)
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

/// Where a reader ended by a move is told to resume, once its feed closes.
async fn resumes_at(sub: &mut Subscription) -> Option<u64> {
    let ended = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("ended in time");
    assert!(ended.is_none(), "delivered past the mark: {ended:?}");
    sub.receiver.moved().and_then(|moved| moved.resume_from)
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

/// **A held batch goes out once the bound covers it, even if the mark never
/// moves again.** The bound also turns on the shard's route and lease, and a
/// change there kicks nothing. A release that gave up at the last kick left
/// the batch held for good: on disk and in every read, but never delivered.
#[tokio::test]
async fn a_held_batch_goes_out_when_the_bound_covers_it_without_a_kick() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"held").await;

    // The mark passes the batch while the shard is refused here, so the kick
    // that comes with it releases nothing.
    bounds.set(ReadBound::Refused);
    release(&broker, "quorum").await;
    nothing(&mut live).await;

    // Served again with the mark already past it, and no kick.
    bounds.set(ReadBound::Committed(1));
    assert_eq!(&next(&mut live).await[..], b"held");
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

/// **A move tells its readers to resume at the first batch they were not
/// offered, even when a generation-start record follows it.** A batch held for
/// the mark stays on disk and ships to the next owner, which commits it. A
/// generation change on the same leader writes a start record behind it before
/// the mark gets there. Told to resume past that record, a reader skips the
/// batch for good.
#[tokio::test]
async fn a_move_resumes_readers_at_a_held_batch_below_a_generation_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _bounds) = broker_with(dir.path(), ReadBound::Committed(1)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"seen").await;
    assert_eq!(&next(&mut live).await[..], b"seen");
    let held = publish(&broker, "quorum", b"held").await;
    assert_eq!(held.offsets, Some((1, 1)));

    let start = broker
        .append_generation_start("t1", "ns", "quorum", 0, 2)
        .await
        .expect("generation start");
    assert_eq!(start, 2);
    let handoff = crate::ShardHandoff {
        node_id: Some("next".to_string()),
        addr: None,
        generation: 3,
    };
    assert_eq!(
        broker
            .end_subscriptions("t1", "ns", "quorum", 0, Some(handoff))
            .await,
        1
    );
    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 1);

    let ended = tokio::time::timeout(Duration::from_secs(5), live.recv())
        .await
        .expect("ended in time");
    assert!(ended.is_none(), "delivered past the mark: {ended:?}");
    assert_eq!(
        live.receiver.moved().and_then(|moved| moved.resume_from),
        Some(1),
        "the held batch was never offered, so a reader resumes at it"
    );
}

/// **A batch held across a generation-start record still goes out, and the
/// stream carries on past the record.** The record takes its own offset in
/// the commit order, so the next publish lands after it.
#[tokio::test]
async fn a_batch_held_across_a_generation_start_is_released_and_publishes_follow_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"held").await;
    broker
        .append_generation_start("t1", "ns", "quorum", 0, 2)
        .await
        .expect("generation start");

    bounds.set(ReadBound::Committed(2));
    release(&broker, "quorum").await;
    assert_eq!(&next(&mut live).await[..], b"held");

    bounds.set(ReadBound::Unbounded);
    let outcome =
        tokio::time::timeout(Duration::from_secs(5), publish(&broker, "quorum", b"after"))
            .await
            .expect("the publish waited on a turn nobody holds");
    assert_eq!(outcome.offsets, Some((2, 2)));
    assert_eq!(&next(&mut live).await[..], b"after");
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
            .publish_batch_with_outcome(&handle, &[Bytes::copy_from_slice(payload)], None, None)
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

/// **A bounded read stops at the committed mark without waiting for it**,
/// and is refused while the bound is unknown.
#[tokio::test]
async fn a_range_read_stops_at_the_mark_and_does_not_wait() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Committed(0)).await;
    for payload in [&b"a"[..], b"b", b"c"] {
        let handle = broker
            .resolve_stream_handle("t1", "ns", "quorum", 0)
            .await
            .expect("handle");
        broker
            .publish_batch_with_outcome(&handle, &[Bytes::copy_from_slice(payload)], None, None)
            .await
            .expect("publish");
    }
    bounds.set(ReadBound::Committed(1));

    let page = tokio::time::timeout(
        Duration::from_secs(5),
        broker.read_range("t1", "ns", "quorum", 0, 0, None, 100, 1 << 20),
    )
    .await
    .expect("a read at the mark waited")
    .expect("read");
    assert_eq!(
        page.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(page.next_offset, 1);
    let at_mark = broker
        .read_range("t1", "ns", "quorum", 0, 1, Some(3), 100, 1 << 20)
        .await
        .expect("read");
    assert!(at_mark.records.is_empty());
    assert_eq!(at_mark.next_offset, 1);

    bounds.set(ReadBound::Settling);
    assert!(matches!(
        broker
            .read_range("t1", "ns", "quorum", 0, 0, None, 100, 1 << 20)
            .await,
        Err(BrokerError::NotReadable {
            reason: NotReadable::Settling,
            ..
        })
    ));
}

/// **Readers of a deposed leader resume below the records a newer leader
/// ships to it.** Until its readers are ended, a deposed leader can take the
/// new leader's records as a follower. Those were offered to none of its
/// readers, and neither was the batch it held for the mark, which the new
/// leader may commit. Told to resume past them when the move ends them, a
/// reader skips them for good.
#[tokio::test]
async fn readers_resume_below_records_a_newer_leader_ships() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _bounds) = broker_with(dir.path(), ReadBound::Committed(1)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"seen").await;
    assert_eq!(&next(&mut live).await[..], b"seen");
    let held = publish(&broker, "quorum", b"held").await;
    assert_eq!(held.offsets, Some((1, 1)));

    // The newer leader's batch lands, ending at 4.
    broker
        .adopt_replicated("t1", "ns", "quorum", 0, 4)
        .await
        .expect("adopt");
    // A reader that joins after them starts past them.
    let mut joined = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    let handoff = crate::ShardHandoff {
        node_id: Some("next".to_string()),
        addr: None,
        generation: 3,
    };
    broker
        .end_subscriptions("t1", "ns", "quorum", 0, Some(handoff))
        .await;

    assert_eq!(resumes_at(&mut live).await, Some(1));
    assert_eq!(resumes_at(&mut joined).await, Some(4));
}

/// **A rebuild that keeps the committed records ends readers below a held
/// batch it moves past.** The mark can cover a held batch before the release
/// gets to it. A newer leader that asks this broker to rebuild keeps the log
/// up to the commit offset, past that batch, and the new leader has it.
#[tokio::test]
async fn readers_resume_below_a_held_batch_a_rebuild_moves_past() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _bounds) = broker_with(dir.path(), ReadBound::Committed(1)).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"seen").await;
    assert_eq!(&next(&mut live).await[..], b"seen");
    publish(&broker, "quorum", b"held").await;

    broker
        .reset_log(crate::LogKind::Stream, "t1", "ns", "quorum", 0, 2)
        .await
        .expect("reset");
    broker
        .end_subscriptions(
            "t1",
            "ns",
            "quorum",
            0,
            Some(crate::ShardHandoff::default()),
        )
        .await;
    assert_eq!(resumes_at(&mut live).await, Some(1));
}

/// **A batch shipped again leaves readers where they are.** The sequence does
/// not move, so nothing was passed over and the readers carry on.
#[tokio::test]
async fn a_batch_shipped_again_does_not_end_readers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");
    publish(&broker, "quorum", b"first").await;
    assert_eq!(&next(&mut live).await[..], b"first");

    broker
        .adopt_replicated("t1", "ns", "quorum", 0, 1)
        .await
        .expect("adopt");
    publish(&broker, "quorum", b"second").await;
    assert_eq!(&next(&mut live).await[..], b"second");
}

async fn commit(broker: &Broker, key: &str, value: &'static [u8]) -> crate::PublishOutcome {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "quorum", 0)
        .await
        .expect("handle");
    let ops = vec![crate::StateOp::Put {
        key: key.to_string(),
        value: Bytes::from_static(value),
    }];
    broker
        .commit_to_handle(&handle, Bytes::from_static(value), ops, None)
        .await
        .expect("commit")
}

async fn state(broker: &Broker, key: &str) -> crate::StateRead {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "quorum", 0)
        .await
        .expect("handle");
    broker.state_get(&handle, key).await.expect("state read")
}

/// The offsets a range read from the start returns.
async fn readable(broker: &Broker) -> Vec<u64> {
    let page = broker
        .read_range("t1", "ns", "quorum", 0, 0, None, 100, 1 << 20)
        .await
        .expect("read");
    page.records.iter().map(|r| r.offset).collect()
}

/// **A commit held when the shard was given up is in the state once the
/// broker leads again and the mark covers it.** The dropped batch stays on
/// disk and the next generation commits it, so readers see its event. The
/// state view built before it never had its update, and kept past the
/// generation start, a state read answers without the commit its event
/// already showed.
#[tokio::test]
async fn a_commit_dropped_from_the_hold_is_in_the_state_once_led_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    commit(&broker, "state", b"first").await;
    assert_eq!(state(&broker, "state").await.version, Some(0));

    bounds.set(ReadBound::Committed(1));
    assert_eq!(
        commit(&broker, "state", b"held").await.offsets,
        Some((1, 1))
    );
    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 1);

    let start = broker
        .append_generation_start("t1", "ns", "quorum", 0, 2)
        .await
        .expect("generation start");
    assert_eq!(start, 2);
    bounds.set(ReadBound::Unbounded);
    assert_eq!(readable(&broker).await, vec![0, 1]);

    let read = state(&broker, "state").await;
    assert_eq!(read.value, Some(Bytes::from_static(b"held")));
    assert_eq!(read.version, Some(1));
}

/// **A publish that lands past records that never reached the ring leaves
/// the state to be rebuilt with them.** Here the dropped batch is a commit to
/// one key and the next is a commit to another; applying only the second to
/// the old view loses the first.
#[tokio::test]
async fn a_commit_past_a_dropped_hold_keeps_the_dropped_commit_in_the_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    commit(&broker, "a", b"first").await;
    assert_eq!(state(&broker, "a").await.version, Some(0));

    bounds.set(ReadBound::Committed(1));
    commit(&broker, "b", b"held").await;
    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 1);

    bounds.set(ReadBound::Unbounded);
    assert_eq!(commit(&broker, "a", b"after").await.offsets, Some((2, 2)));
    assert_eq!(readable(&broker).await, vec![0, 1, 2]);

    let read = state(&broker, "b").await;
    assert_eq!(read.value, Some(Bytes::from_static(b"held")));
    assert_eq!(read.version, Some(1));
    assert_eq!(state(&broker, "a").await.version, Some(2));
}

/// **A broker that leads again without a generation-start record still
/// rebuilds its state with the commit it dropped.** Before the fleet writes
/// start records, nothing moves `next_seq` past the dropped batch, so without
/// `lead_from_tail` the old view answers, and a rebuild would stop below it.
/// The rebuild waits for the mark to cover the tail.
#[tokio::test]
async fn a_dropped_commit_is_in_the_state_when_led_again_without_a_start_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    commit(&broker, "state", b"first").await;
    assert_eq!(state(&broker, "state").await.version, Some(0));

    bounds.set(ReadBound::Committed(1));
    commit(&broker, "state", b"held").await;
    assert_eq!(broker.discard_uncommitted("t1", "ns", "quorum", 0).await, 1);
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");

    broker
        .lead_from_tail("t1", "ns", "quorum", 0)
        .await
        .expect("lead from tail");
    // A reader at the old sequence was never offered the dropped commit.
    assert_eq!(resumes_at(&mut live).await, Some(1));
    let handle = broker
        .resolve_stream_handle("t1", "ns", "quorum", 0)
        .await
        .expect("handle");
    assert!(matches!(
        broker.state_get(&handle, "state").await,
        Err(BrokerError::StateNotReadable(NotReadable::Settling))
    ));

    bounds.set(ReadBound::Committed(2));
    assert_eq!(readable(&broker).await, vec![0, 1]);
    let read = state(&broker, "state").await;
    assert_eq!(read.value, Some(Bytes::from_static(b"held")));
    assert_eq!(read.version, Some(1));
}

/// **A hold carried across a generation start keeps the state view.** The
/// held commit is released in order and applied to the view, and the publish
/// after the start record counts it as skipped, so nothing forces a rebuild.
#[tokio::test]
async fn a_hold_carried_across_a_generation_start_keeps_the_state_view() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bounds) = broker_with(dir.path(), ReadBound::Unbounded).await;
    commit(&broker, "state", b"first").await;
    assert_eq!(state(&broker, "state").await.version, Some(0));
    let mut live = broker
        .subscribe("t1", "ns", "quorum", 0)
        .await
        .expect("subscribe");

    bounds.set(ReadBound::Committed(1));
    commit(&broker, "state", b"held").await;
    assert_eq!(
        broker
            .append_generation_start("t1", "ns", "quorum", 0, 2)
            .await
            .expect("generation start"),
        2
    );
    let handle = broker
        .resolve_stream_handle("t1", "ns", "quorum", 0)
        .await
        .expect("handle");
    let below_hold = handle.tail_seq();
    broker
        .lead_from_tail("t1", "ns", "quorum", 0)
        .await
        .expect("lead from tail");
    // A busy hold keeps the sequence where readers resume and its readers.
    assert_eq!(handle.tail_seq(), below_hold);
    bounds.set(ReadBound::Unbounded);
    commit(&broker, "other", b"after").await;
    assert_eq!(&next(&mut live).await[..], b"held");
    assert_eq!(&next(&mut live).await[..], b"after");

    // A rebuild asks for the read bound; a kept view answers without it.
    let asked = bounds.asked.load(Ordering::SeqCst);
    let read = state(&broker, "state").await;
    assert_eq!(read.value, Some(Bytes::from_static(b"held")));
    assert_eq!(read.version, Some(1));
    assert_eq!(
        bounds.asked.load(Ordering::SeqCst),
        asked,
        "the view was rebuilt"
    );
}
