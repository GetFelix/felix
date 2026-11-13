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
    Broker, ConsistencyLevel, DeliveryEnvelope, ReadBound, ReadBounds, StreamHandle,
    StreamMetadata, SubscriptionReceiver,
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
    ) -> impl std::future::Future<Output = Result<u64, &'static str>> + Send + use<> {
        let end = {
            let mut next = self.next.lock();
            *next += records;
            *next
        };
        let (commit, committed) = oneshot::channel();
        self.pending.lock().push(commit);
        async move {
            committed.await.map_err(|_| "dropped")??;
            Ok(end)
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
            .propose_publish(&handle, vec![Bytes::from_static(payload)], move |_| {
                engine.propose(1)
            })
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
            .propose_publish(&handle, vec![Bytes::from_static(payload)], move |_| {
                engine.propose(1)
            })
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
/// one frame shape is encoded once however many subscribers ask for it.
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
            .propose_publish(&handle, vec![payload.clone()], move |_| engine.propose(1))
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
    for envelope in &envelopes {
        assert!(envelope.same_batch(&envelopes[0]));
        assert_eq!(envelope.payloads()[0].as_ptr(), payload.as_ptr());
        let again = envelope.shared_event_frame_with_offsets().expect("encoded");
        assert_eq!(again.as_ptr(), frame.as_ptr());
    }
    assert_eq!(envelopes[0].encodes(), 1);
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
            .propose_publish(&handle, vec![Bytes::from_static(b"a")], move |_| {
                engine.propose(1)
            })
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
