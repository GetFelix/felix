//! Subscribers of a Raft shard get committed records only: on the leader
//! from the publisher's own batch, and a follower's readers are held to
//! what it has applied.

use felix_broker::{
    Broker, BrokerError, ConsistencyLevel, DeliveryEnvelope, ReadBound, StartPosition,
    StreamHandle, StreamMetadata, SubscriptionGuard, SubscriptionReceiver,
};
use felix_storage::EphemeralCache;

use super::super::deliver::settled;
use super::commit::{Led, led};
use super::propose::cut_after_first;
use super::*;
use crate::engine::{CommitError, Written};

/// A broker whose `orders` stream is bounded by `engine` at every level and
/// told each time the commit index moves.
async fn broker_on(engine: &RaftEngine) -> (Broker, StreamHandle) {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.set_read_bounds(Arc::new(RaftReads::new(engine.clone())));
    broker.register_tenant("t").await.expect("tenant");
    broker.register_namespace("t", "ns").await.expect("ns");
    let metadata = StreamMetadata {
        consistency: ConsistencyLevel::Leader,
        ..Default::default()
    };
    let registered = broker.register_stream("t", "ns", "orders", metadata);
    registered.await.expect("stream");
    let handle = broker.resolve_stream_handle("t", "ns", "orders", 0).await;
    let handle = handle.expect("handle");
    handle.bound_every_level();
    engine
        .follow_commits(&shard(0), handle.clone())
        .expect("a member here");
    (broker, handle)
}

async fn subscriber(broker: &Broker) -> (SubscriptionReceiver, SubscriptionGuard) {
    let subscription = broker.subscribe("t", "ns", "orders", 0).await;
    subscription.expect("subscribe").into_parts()
}

async fn nothing(receiver: &mut SubscriptionReceiver) {
    let got = tokio::time::timeout(Duration::from_millis(300), receiver.recv()).await;
    assert!(got.is_err(), "delivered before it was committed: {got:?}");
}

async fn next(receiver: &mut SubscriptionReceiver) -> DeliveryEnvelope {
    let got = tokio::time::timeout(PATIENCE, receiver.recv()).await;
    got.expect("delivered in time").expect("subscription open")
}

type Publishing = tokio::task::JoinHandle<Result<felix_broker::PublishOutcome, CommitError>>;

fn publish(led: &Led, broker: &Broker, handle: &StreamHandle, payload: Bytes) -> Publishing {
    publish_within(&led.engine, broker, handle, payload, PATIENCE)
}

fn publish_within(
    engine: &RaftEngine,
    broker: &Broker,
    handle: &StreamHandle,
    payload: Bytes,
    timeout: Duration,
) -> Publishing {
    let proposed = engine.publish(
        broker,
        handle,
        &shard(0),
        None,
        vec![payload],
        AckAt::Flushed,
        timeout,
    );
    tokio::spawn(proposed.expect("proposed").complete())
}

/// **A record no majority holds reaches no subscriber, and once one holds
/// it the subscriber gets the publisher's own bytes at the offset the log
/// gave it.** Fed at the write instead, a subscriber would hold a record a
/// failover can still take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_is_delivered_only_once_a_majority_holds_it() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;

    led.cut_followers();
    let payload = Bytes::from(vec![42u8; 32]);
    let published = publish(&led, &broker, &handle, payload.clone());
    eventually(|| {
        let status = led.status();
        (status.flushed?.index > led.committed).then_some(())
    })
    .await;
    nothing(&mut receiver).await;
    assert_eq!(handle.read_bound(), ReadBound::Committed(led.committed + 1));

    led.heal();
    let envelope = next(&mut receiver).await;
    let offset = led.committed + 1;
    assert_eq!(envelope.base_offset(), Some(offset));
    assert_eq!(envelope.payloads().len(), 1);
    assert_eq!(envelope.payloads()[0].as_ptr(), payload.as_ptr());
    let outcome = published.await.expect("joined").expect("committed");
    assert_eq!(outcome.offsets, Some((offset, offset)));
    led.cluster.stop().await;
}

/// **A leader that wrote a record and was deposed before it committed never
/// delivers it, though a newer leader's entry later commits at its
/// offset.** Released when the commit index passed the offset, the old
/// leader's subscriber would hold a record no log keeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_a_later_leader_replaced_reaches_no_subscriber() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;

    led.cut_followers();
    let published = publish(&led, &broker, &handle, Bytes::from_static(b"mine"));
    eventually(|| {
        let status = led.status();
        (status.flushed?.index > led.committed).then_some(())
    })
    .await;

    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old]);
    let theirs = led.cluster.write(0, b"theirs").await;
    assert!(theirs > led.committed + 1, "the offset was not taken");
    assert_ne!(led.cluster.elected(0).await, led.leader);

    led.heal();
    eventually(|| match handle.read_bound() {
        ReadBound::Committed(bound) => (bound > theirs).then_some(()),
        _ => None,
    })
    .await;
    let outcome = published.await.expect("joined");
    assert!(
        matches!(outcome, Err(CommitError::LeadershipLost { .. })),
        "{outcome:?}"
    );
    nothing(&mut receiver).await;
    led.cluster.stop().await;
}

/// **A follower's readers never see past what it has applied, and are
/// woken each time that moves.** Bounded by its log instead, a reader on a
/// follower that has flushed records but not yet heard them committed would
/// be shown records the leader has not committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_followers_readers_stop_at_what_it_applied() {
    let mut led = led().await;
    let follower = led.followers().next().expect("a follower");
    let runtime = led.cluster.runtime(&follower);
    let engine = RaftEngine::new(runtime.clone());
    let (_broker, handle) = broker_on(&engine).await;
    let applied = |handle: &StreamHandle| match handle.read_bound() {
        ReadBound::Committed(bound) => bound,
        other => panic!("not bounded by the commit index: {other:?}"),
    };
    let within = |handle: &StreamHandle| {
        let bound = applied(handle);
        let leader = led.status().committed.map_or(0, |index| index + 1);
        let status = runtime.status(&shard(0)).expect("status");
        let here = status.committed.map_or(0, |index| index + 1);
        assert!(bound <= leader, "bound {bound} past the leader's {leader}");
        assert!(bound <= here, "bound {bound} past this member's {here}");
        bound
    };
    eventually(|| (within(&handle) == led.committed + 1).then_some(())).await;

    // The leader's log runs ahead of every follower's.
    led.cut_followers();
    let mut end = 0;
    for round in 0..4 {
        end = led.propose(round % 3 + 1).await.end;
        assert_eq!(within(&handle), led.committed + 1);
    }

    let appended = handle.appended();
    let woken = appended.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    led.heal();
    // Sampled while the follower stores, flushes and applies, racing the
    // leader's commit: the bound never runs ahead of either.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while within(&handle) < end {
        assert!(tokio::time::Instant::now() < deadline, "never applied");
        tokio::task::yield_now().await;
    }
    tokio::time::timeout(PATIENCE, woken)
        .await
        .expect("a reader waiting at the bound is woken");
    led.cluster.stop().await;
}

/// **A batch whose commit outlasted its publisher's timeout is still
/// delivered once it commits, before the batch proposed after it.** Dropped
/// at the timeout, the later batch would go out at the higher offset and
/// live subscribers would never get the earlier, committed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_committed_after_its_timeout_goes_out_before_the_next() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;

    led.cut_followers();
    let slow = Bytes::from_static(b"slow");
    let timeout = Duration::from_millis(200);
    let slow_published = publish_within(&led.engine, &broker, &handle, slow.clone(), timeout);
    let next_published = publish(&led, &broker, &handle, Bytes::from_static(b"next"));
    let outcome = slow_published.await.expect("joined");
    assert!(
        matches!(outcome, Err(CommitError::TimedOut { .. })),
        "{outcome:?}"
    );
    nothing(&mut receiver).await;

    led.heal();
    let first = next(&mut receiver).await;
    assert_eq!(first.base_offset(), Some(led.committed + 1));
    assert_eq!(first.payloads(), [slow]);
    let second = next(&mut receiver).await;
    assert_eq!(second.base_offset(), Some(led.committed + 2));
    let outcome = next_published.await.expect("joined").expect("committed");
    assert_eq!(
        outcome.offsets,
        Some((led.committed + 2, led.committed + 2))
    );
    led.cluster.stop().await;
}

/// **Offsets the log used for entries holding no record reach subscribers
/// as skipped.** A new group's first entries are its membership and the
/// leader's blank entry; reported as nothing, the first record would arrive
/// after a jump a subscriber reads as a drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entries_holding_no_record_reach_subscribers_as_skipped() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    let (broker, handle) = broker_on(&engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;

    let published = publish_within(
        &engine,
        &broker,
        &handle,
        Bytes::from_static(b"a"),
        PATIENCE,
    );
    let envelope = next(&mut receiver).await;
    let first = envelope.base_offset().expect("an offset");
    assert!(first > 0, "nothing came before the first record");
    assert_eq!(envelope.skipped_before(), first);
    published.await.expect("joined").expect("committed");
    cluster.stop().await;
}

/// **A batch admitted at another term than the one this broker leads is
/// not written and not delivered.** A broker deposed and elected again
/// between the caller's look and the proposal leads a term whose stream
/// may not be at the committed tail yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_admitted_at_another_term_is_not_written() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    let term = led.status().term;
    let at = |term: u64, payload: &'static [u8]| {
        let payloads = vec![Bytes::from_static(payload)];
        let proposed = led.engine.publish(
            &broker,
            &handle,
            &shard(0),
            Some(term),
            payloads,
            AckAt::Flushed,
            PATIENCE,
        );
        tokio::spawn(proposed.expect("proposed").complete())
    };

    let outcome = at(term - 1, b"stale").await.expect("joined");
    assert!(
        matches!(outcome, Err(CommitError::LeadershipLost { .. })),
        "{outcome:?}"
    );
    nothing(&mut receiver).await;

    // The next batch takes the offset the stale one would have.
    let outcome = at(term, b"current").await.expect("joined");
    let offset = led.committed + 1;
    assert_eq!(outcome.expect("committed").offsets, Some((offset, offset)));
    let envelope = next(&mut receiver).await;
    assert_eq!(envelope.base_offset(), Some(offset));
    assert_eq!(envelope.payloads(), [Bytes::from_static(b"current")]);
    led.cluster.stop().await;
}

/// **A batch written in part moves the stream's offsets past what was
/// written.** Its first record commits. A reader resuming at that record
/// is sent below the ring, to the log, and is not joined live above it;
/// a reader joining at the tail starts after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_resuming_in_a_batch_written_in_part_goes_to_the_log() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let term = led.status().term;
    let before = led.engine.publish(
        &broker,
        &handle,
        &shard(0),
        Some(term),
        vec![Bytes::from_static(b"before")],
        AckAt::Flushed,
        PATIENCE,
    );
    let before = before.expect("proposed").complete().await;
    let first = led.committed + 2;
    assert_eq!(
        before.expect("committed").offsets,
        Some((first - 1, first - 1))
    );

    let raft = led.cluster.runtime(&led.leader.broker).group(&shard(0));
    let raft = raft.expect("group").raft().clone();
    let engine = led.engine.clone();
    let payloads = vec![Bytes::from_static(b"in"), Bytes::from_static(b"out")];
    let proposed = broker.propose_publish(&handle, payloads, PATIENCE, |_| {
        let written = async move {
            match cut_after_first(&raft, term, &[b"in", b"out"]).await {
                Ok(span) => panic!("written whole at {span:?}"),
                Err(unacked) => Err::<Written, _>(unacked),
            }
        };
        settled(engine, shard(0), Some(term), 2, PATIENCE, written)
    });
    let outcome = proposed.expect("proposed").complete().await;
    assert!(
        matches!(outcome, Err(CommitError::LeadershipLost { .. })),
        "{outcome:?}"
    );

    // The failed batch's place is released on a task of its own. A tail
    // read just before it moves is refused as too old.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let latest = broker.subscribe_from("t", "ns", "orders", 0, StartPosition::Latest);
        let latest = latest.await.map(|latest| latest.backlog_start);
        if latest.is_ok_and(|start| start == first + 1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream's tail stayed below the written record"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let resumed = broker.subscribe_from("t", "ns", "orders", 0, StartPosition::Offset(first));
    let resumed = resumed.await.map(|resumed| resumed.backlog_start);
    assert!(
        matches!(
            resumed,
            Err(BrokerError::CursorTooOld { oldest, requested, .. })
                if oldest == first + 1 && requested == first
        ),
        "joined above a record in the log: {resumed:?}"
    );
    led.cluster.stop().await;
}
