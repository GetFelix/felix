//! A batch whose offsets an engine gives reaches subscribers once the engine
//! says it is committed, in the order it was handed over, as the bytes the
//! publisher handed in.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{
    Broker, BrokerError, ConsistencyLevel, DeliveryEnvelope, ProposalCommitted, ProposalFailed,
    ReadBound, ReadBounds, StreamHandle, StreamMetadata, SubscriptionReceiver,
};

/// An engine that gives offsets in hand-over order and commits a batch when
/// the test says so.
#[derive(Default)]
struct Engine {
    next: Mutex<u64>,
    pending: Mutex<Vec<oneshot::Sender<Result<(), &'static str>>>>,
}

impl Engine {
    fn propose(
        &self,
        records: u64,
    ) -> impl std::future::Future<Output = Result<ProposalCommitted, ProposalFailed<&'static str>>>
    + Send
    + use<> {
        self.propose_after(0, records)
    }

    /// Propose `records` behind `skipped` entries that hold none.
    fn propose_after(
        &self,
        skipped: u64,
        records: u64,
    ) -> impl std::future::Future<Output = Result<ProposalCommitted, ProposalFailed<&'static str>>>
    + Send
    + use<> {
        let end = {
            let mut next = self.next.lock();
            *next += skipped + records;
            *next
        };
        let (commit, committed) = oneshot::channel();
        self.pending.lock().push(commit);
        async move {
            committed
                .await
                .map_err(|_| "dropped")
                .flatten()
                .map_err(|err| ProposalFailed::written(err, end))?;
            Ok(ProposalCommitted {
                end,
                skipped_before: skipped,
            })
        }
    }

    fn commit(&self, which: usize, outcome: Result<(), &'static str>) {
        let commit = std::mem::replace(&mut self.pending.lock()[which], oneshot::channel().0);
        let _ = commit.send(outcome);
    }
}

#[derive(Debug)]
struct Bound(Mutex<ReadBound>);

impl ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        *self.0.lock()
    }
}

async fn broker(bound: ReadBound) -> (Broker, StreamHandle, Arc<Bound>) {
    let bounds = Arc::new(Bound(Mutex::new(bound)));
    let broker = Broker::new(EphemeralCache::new().into());
    broker.set_read_bounds(Arc::clone(&bounds) as Arc<dyn ReadBounds>);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "raft",
            StreamMetadata {
                consistency: ConsistencyLevel::Leader,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "raft", 0)
        .await
        .expect("handle");
    handle.bound_every_level();
    (broker, handle, bounds)
}

/// A subscriber's queue, and the guard that keeps it subscribed.
async fn subscriber(broker: &Broker) -> (SubscriptionReceiver, crate::SubscriptionGuard) {
    broker
        .subscribe("t1", "ns", "raft", 0)
        .await
        .expect("subscribe")
        .into_parts()
}

async fn next(receiver: &mut SubscriptionReceiver) -> DeliveryEnvelope {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("delivered in time")
        .expect("subscription open")
}

async fn nothing(receiver: &mut SubscriptionReceiver) {
    let got = tokio::time::timeout(Duration::from_millis(150), receiver.recv()).await;
    assert!(got.is_err(), "delivered before it was committed: {got:?}");
}

/// **Batches reach subscribers in the order they were handed to the engine,
/// whichever is committed first, and one whose publisher went away is still
/// delivered in its place.** Joined in the order publishers hear back, the
/// later offsets would go out first; skipped when its publisher is dropped,
/// the batch would be a hole live subscribers never hear about.
#[tokio::test]
async fn batches_go_out_in_hand_over_order_whoever_hears_first() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    let publish = |payload: &'static [u8]| {
        let engine = Arc::clone(&engine);
        broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(payload)],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over")
    };
    let first = publish(b"a");
    let second = tokio::spawn(publish(b"b").complete());
    let third = tokio::spawn(publish(b"c").complete());
    // The first publisher goes away before anything is committed.
    drop(first);

    engine.commit(2, Ok(()));
    engine.commit(1, Ok(()));
    nothing(&mut receiver).await;
    engine.commit(0, Ok(()));

    for (offset, payload) in [(0, b"a"), (1, b"b"), (2, b"c")] {
        let envelope = next(&mut receiver).await;
        assert_eq!(envelope.base_offset(), Some(offset));
        assert_eq!(envelope.payloads(), [Bytes::from_static(payload)]);
    }
    let second = second.await.expect("joined").expect("committed");
    assert_eq!(second.offsets, Some((1, 1)));
    assert_eq!(
        third.await.expect("joined").expect("committed").offsets,
        Some((2, 2))
    );
}

/// **Nothing is delivered before the engine says it is committed, and a
/// batch the engine cannot vouch for is never delivered and holds up
/// nothing behind it.** A write a later leader cut ends in such an error.
#[tokio::test]
async fn only_what_the_engine_committed_is_delivered() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    let mut publishes = Vec::new();
    for payload in [b"lost".as_slice(), b"kept"] {
        let engine = Arc::clone(&engine);
        let proposed = broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(payload)],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over");
        publishes.push(tokio::spawn(proposed.complete()));
    }
    nothing(&mut receiver).await;

    engine.commit(1, Ok(()));
    nothing(&mut receiver).await;
    engine.commit(0, Err("a newer leader's entry took the offset"));

    let envelope = next(&mut receiver).await;
    assert_eq!(envelope.base_offset(), Some(1));
    assert_eq!(envelope.payloads(), [Bytes::from_static(b"kept")]);
    nothing(&mut receiver).await;
    let mut outcomes = Vec::new();
    for publish in publishes {
        outcomes.push(publish.await.expect("joined"));
    }
    assert!(outcomes[0].is_err());
    assert_eq!(outcomes[1].as_ref().expect("kept").offsets, Some((1, 1)));
}

/// **The publisher's bytes are what every subscriber gets, encoded once.**
/// One envelope is shared, its records are the very `Bytes` handed in, and
/// each frame shape (with and without event offsets) is encoded once however
/// many subscribers ask for it.
#[tokio::test]
async fn every_subscriber_shares_one_envelope_of_the_publishers_bytes() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let mut receivers = Vec::new();
    let mut guards = Vec::new();
    for _ in 0..3 {
        let (receiver, guard) = subscriber(&broker).await;
        receivers.push(receiver);
        guards.push(guard);
    }
    let engine = Arc::new(Engine::default());
    let payload = Bytes::from(vec![7u8; 64]);
    let proposed = {
        let engine = Arc::clone(&engine);
        broker
            .propose_publish(&handle, vec![payload.clone()], Duration::MAX, move |_| {
                engine.propose(1)
            })
            .expect("handed over")
    };
    let publish = tokio::spawn(proposed.complete());
    engine.commit(0, Ok(()));
    publish.await.expect("joined").expect("committed");

    let mut envelopes = Vec::new();
    for receiver in &mut receivers {
        envelopes.push(next(receiver).await);
    }
    let frame = envelopes[0]
        .shared_event_frame_with_offsets()
        .expect("encoded");
    let plain = envelopes[1].shared_event_frame().expect("encoded");
    assert_ne!(plain, frame, "the two shapes are different frames");
    for envelope in &envelopes {
        assert!(envelope.same_batch(&envelopes[0]));
        assert_eq!(envelope.payloads()[0].as_ptr(), payload.as_ptr());
        let again = envelope.shared_event_frame_with_offsets().expect("encoded");
        assert_eq!(again.as_ptr(), frame.as_ptr());
        let again = envelope.shared_event_frame().expect("encoded");
        assert_eq!(again.as_ptr(), plain.as_ptr());
    }
    // One frame with event offsets, one without, however many subscribers
    // of each.
    assert_eq!(envelopes[0].encodes(), 2);
}

/// **On a shard bounded at every level a `Leader` stream's readers stop at
/// the bound too.** Left unbounded, a batch could go out as soon as the
/// engine reported it while this member's applied index trailed it.
#[tokio::test]
async fn a_leader_stream_bounded_at_every_level_waits_for_the_bound() {
    let (broker, handle, bound) = broker(ReadBound::Committed(0)).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    let proposed = {
        let engine = Arc::clone(&engine);
        broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(b"a")],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over")
    };
    let publish = tokio::spawn(proposed.complete());
    engine.commit(0, Ok(()));
    publish.await.expect("joined").expect("committed");
    nothing(&mut receiver).await;

    *bound.0.lock() = ReadBound::Committed(1);
    handle.release_committed();
    assert_eq!(next(&mut receiver).await.base_offset(), Some(0));
}

/// **A batch whose publisher was answered with a timeout still goes out if
/// it commits, before the batch handed over after it.** Given up at the
/// timeout, the later batch would reach subscribers and the earlier one,
/// committed at a lower offset, never would.
#[tokio::test]
async fn a_batch_that_commits_after_its_publisher_timed_out_goes_out_first() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    let publish = |payload: &'static [u8]| {
        let engine = Arc::clone(&engine);
        broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(payload)],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over")
    };
    let slow = publish(b"slow").answer_within(Duration::from_millis(50), || "timed out");
    let slow = tokio::spawn(slow.complete());
    let next_one = tokio::spawn(publish(b"next").complete());
    engine.commit(1, Ok(()));
    assert!(matches!(slow.await.expect("joined"), Err("timed out")));
    nothing(&mut receiver).await;

    engine.commit(0, Ok(()));
    for (offset, payload) in [(0, b"slow".as_slice()), (1, b"next")] {
        let envelope = next(&mut receiver).await;
        assert_eq!(envelope.base_offset(), Some(offset));
        assert_eq!(envelope.payloads(), [Bytes::copy_from_slice(payload)]);
    }
    let next_one = next_one.await.expect("joined").expect("committed");
    assert_eq!(next_one.offsets, Some((1, 1)));
}

/// **A batch the engine gave up on restarts the replay ring in its place.**
/// Its records may have committed after all; a ring that went on past them
/// would serve a resuming reader the records around the hole and never the
/// ones in it, where the log has them.
#[tokio::test]
async fn a_batch_given_up_on_restarts_the_ring_above_it() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    for payload in [b"a".as_slice(), b"lost", b"b"] {
        let engine = Arc::clone(&engine);
        let proposed = broker
            .propose_publish(
                &handle,
                vec![Bytes::copy_from_slice(payload)],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over");
        drop(proposed);
    }
    engine.commit(0, Ok(()));
    engine.commit(1, Err("this broker stopped leading the shard"));
    engine.commit(2, Ok(()));
    assert_eq!(next(&mut receiver).await.base_offset(), Some(0));
    assert_eq!(next(&mut receiver).await.base_offset(), Some(2));
    assert_eq!(handle.state.oldest_seq(), 2);
}

/// **A reader resuming right after a batch the engine gave up on starts
/// above it.** The ring is empty then, so the reader joins live at the
/// stream's sequence. Left at the batch before, it would read history only
/// up to there and get the given-up batch, if it committed, as a drop.
#[tokio::test]
async fn a_reader_resuming_after_a_batch_given_up_on_starts_above_it() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    for payload in [b"a".as_slice(), b"lost"] {
        let engine = Arc::clone(&engine);
        let proposed = broker
            .propose_publish(
                &handle,
                vec![Bytes::copy_from_slice(payload)],
                Duration::MAX,
                move |_| engine.propose(1),
            )
            .expect("handed over");
        drop(proposed);
    }
    engine.commit(0, Ok(()));
    assert_eq!(next(&mut receiver).await.base_offset(), Some(0));
    engine.commit(1, Err("this broker stopped leading the shard"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.tail_seq() != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the sequence moves past the batch given up on");
    assert_eq!(handle.state.oldest_seq(), 2, "the ring restarts above it");
}

/// **An engine that settles nothing is handed no more batches.** A leader
/// cut off from its majority is not deposed and commits nothing; every
/// batch handed to it waits with its payload in memory. Taking them
/// without limit grows until the partition heals.
#[tokio::test]
async fn an_engine_that_settles_nothing_is_handed_no_more_batches() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let engine = Arc::new(Engine::default());
    let stalled_after = Duration::from_secs(2);
    let propose = |payload: &'static [u8]| {
        let engine = Arc::clone(&engine);
        broker.propose_publish(
            &handle,
            vec![Bytes::from_static(payload)],
            stalled_after,
            move |_| engine.propose(1),
        )
    };
    let first = propose(b"a").expect("handed over");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let second = propose(b"b").expect("the oldest is still within its time");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    match propose(b"c") {
        Err(BrokerError::ProposalsStalled { waited }) => assert!(waited > stalled_after),
        other => panic!("handed a stalled engine another batch: {:?}", other.err()),
    }
    assert_eq!(engine.pending.lock().len(), 2, "the engine was not asked");

    // The oldest settles; the next oldest is still within its time.
    engine.commit(0, Ok(()));
    first.complete().await.expect("committed");
    let third = propose(b"c").expect("handed over once the oldest settled");
    engine.commit(1, Ok(()));
    engine.commit(2, Ok(()));
    second.complete().await.expect("committed");
    third.complete().await.expect("committed");
}

/// **Offsets an engine used for entries holding no record reach subscribers
/// as skipped.** A Raft leader's first entry of a term is blank; reported
/// as nothing, the jump past it reads as a dropped record.
#[tokio::test]
async fn offsets_holding_no_record_are_reported_skipped() {
    let (broker, handle, _) = broker(ReadBound::Unbounded).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let engine = Arc::new(Engine::default());
    let proposed = {
        let engine = Arc::clone(&engine);
        broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(b"a")],
                Duration::MAX,
                move |_| engine.propose_after(2, 1),
            )
            .expect("handed over")
    };
    let publish = tokio::spawn(proposed.complete());
    engine.commit(0, Ok(()));
    publish.await.expect("joined").expect("committed");
    let envelope = next(&mut receiver).await;
    assert_eq!(envelope.base_offset(), Some(2));
    assert_eq!(envelope.skipped_before(), 2);
}

/// **A shard bounded at every level shows nothing until its engine states a
/// bound.** Read as unbounded, a stream routed before the engine's bounds
/// were installed would serve what no majority holds.
#[tokio::test]
async fn a_shard_bounded_at_every_level_with_no_bounds_shows_nothing() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "ns", "raft", StreamMetadata::default())
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "raft", 0)
        .await
        .expect("handle");
    assert_eq!(handle.read_bound(), ReadBound::Unbounded);
    handle.bound_every_level();
    assert_eq!(handle.read_bound(), ReadBound::Refused);
}
