//! Subscribers of a Raft shard get committed records only: on the leader
//! from the publisher's own batch, and a follower's readers are held to
//! what it has applied.

use felix_broker::{
    Broker, BrokerError, ConsistencyLevel, DeliveryEnvelope, ReadBound, StartPosition,
    StreamHandle, StreamMetadata, SubscriptionGuard, SubscriptionReceiver,
};
use felix_storage::EphemeralCache;
use tokio::sync::oneshot;

use super::super::deliver::{Proposed, settled};
use super::super::propose::Unacknowledged;
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
    publish_answered(engine, broker, handle, payload, AnswerAt::Commit, timeout)
}

/// A publish answered on the leader lease when it holds, as a `Leader`
/// stream's is.
fn publish_on_lease(
    led: &Led,
    broker: &Broker,
    handle: &StreamHandle,
    payload: Bytes,
) -> Publishing {
    let answer = AnswerAt::Lease;
    publish_answered(&led.engine, broker, handle, payload, answer, PATIENCE)
}

fn publish_answered(
    engine: &RaftEngine,
    broker: &Broker,
    handle: &StreamHandle,
    payload: Bytes,
    answer: AnswerAt,
    timeout: Duration,
) -> Publishing {
    let proposed = engine.publish(
        broker,
        handle,
        &shard(0),
        None,
        vec![payload],
        AckAt::Flushed,
        answer,
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
    let engine = RaftEngine::new(led.cluster.runtime(&follower));
    let (_broker, handle) = broker_on(&engine).await;
    let applied = |handle: &StreamHandle| match handle.read_bound() {
        ReadBound::Committed(bound) => bound,
        other => panic!("not bounded by the commit index: {other:?}"),
    };
    let within = async |handle: &StreamHandle| {
        let bound = applied(handle);
        let leader = led.committed_on(&led.leader.broker).await;
        let here = led.committed_on(&follower).await;
        assert!(bound <= leader, "bound {bound} past the leader's {leader}");
        assert!(bound <= here, "bound {bound} past this member's {here}");
        bound
    };
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while within(&handle).await != led.committed + 1 {
        assert!(tokio::time::Instant::now() < deadline, "never applied");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The leader's log runs ahead of every follower's.
    led.cut_followers();
    let mut end = 0;
    for round in 0..4 {
        end = led.propose(round % 3 + 1).await.end;
        assert_eq!(within(&handle).await, led.committed + 1);
    }

    let appended = handle.appended();
    let woken = appended.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    led.heal();
    // Sampled while the follower stores, flushes and applies, racing the
    // leader's commit: the bound never runs ahead of either.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while within(&handle).await < end {
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
            AnswerAt::Commit,
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
        AnswerAt::Commit,
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
        let batch = Proposed {
            shard: shard(0),
            term: Some(term),
            count: 2,
            timeout: PATIENCE,
            on_lease: None,
        };
        settled(engine, batch, written)
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

/// The leader's lease holds: a majority answered it a moment ago.
async fn leased(led: &Led) {
    eventually(|| led.engine.admit_local_ack(&shard(0), led.term).ok()).await;
}

async fn unanswered(published: &Publishing) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!published.is_finished(), "answered before its commit");
}

/// **A `Leader` publish is answered inside the lease once it is on the
/// leader's log, before any follower holds it, and its subscribers still
/// get it only at its commit. A `Quorum` publish beside it is answered at
/// its commit.** Answered at the commit, a `Leader` publisher would wait
/// out a round to the followers that its level does not ask for; delivered
/// at the answer, a subscriber would hold a record a failover can take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_publish_is_answered_on_the_lease_and_delivered_at_its_commit() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    led.cut_followers();
    let quorum = publish(&led, &broker, &handle, Bytes::from_static(b"quorum"));
    let leader = publish_on_lease(&led, &broker, &handle, Bytes::from_static(b"leader"));
    let first = led.committed + 1;
    let answered = tokio::time::timeout(timers().lease, leader).await;
    let outcome = answered
        .expect("answered inside the lease")
        .expect("joined");
    assert_eq!(
        outcome.expect("on the lease").offsets,
        Some((first + 1, first + 1))
    );
    assert_eq!(led.status().committed, Some(led.committed));
    unanswered(&quorum).await;
    nothing(&mut receiver).await;
    assert_eq!(handle.read_bound(), ReadBound::Committed(first));

    led.heal();
    for (offset, payload) in [(first, &b"quorum"[..]), (first + 1, b"leader")] {
        let envelope = next(&mut receiver).await;
        assert_eq!(envelope.base_offset(), Some(offset));
        assert_eq!(envelope.payloads(), [Bytes::copy_from_slice(payload)]);
    }
    let outcome = quorum.await.expect("joined").expect("committed");
    assert_eq!(outcome.offsets, Some((first, first)));
    led.cluster.stop().await;
}

/// A `Leader` publish stopped between its write and the lease check.
struct Held {
    published: Publishing,
    /// Fires once the batch is on the leader's log.
    landed: oneshot::Receiver<()>,
    /// The lease is asked once this is sent.
    go: oneshot::Sender<()>,
}

fn publish_held(led: &Led, broker: &Broker, handle: &StreamHandle, payload: &'static [u8]) -> Held {
    let (on_lease, answered) = oneshot::channel();
    let (landing, landed) = oneshot::channel();
    let (go, gone) = oneshot::channel::<()>();
    let engine = led.engine.clone();
    let term = Some(led.term);
    let payload = Bytes::from_static(payload);
    let proposed = broker.propose_publish(handle, vec![payload.clone()], PATIENCE, |at| {
        let record = Record {
            timestamp_micros: at,
            payload,
        };
        let batch = Batch {
            records: vec![record],
            ack: AckAt::Flushed,
            term,
        };
        let written = engine.propose_reaching(&shard(0), batch);
        let written = async move {
            let written = written.await;
            let _ = landing.send(());
            let _ = gone.await;
            written
        };
        let batch = Proposed {
            shard: shard(0),
            term,
            count: 1,
            timeout: PATIENCE,
            on_lease: Some((AnswerAt::Lease, on_lease)),
        };
        settled(engine, batch, written)
    });
    let proposed = proposed.expect("proposed").answer_at(answered);
    Held {
        published: tokio::spawn(proposed.complete()),
        landed,
        go,
    }
}

fn leader_member(led: &Led) -> Arc<super::super::group::Group> {
    let runtime = led.cluster.runtime(&led.leader.broker);
    runtime.group(&shard(0)).expect("group")
}

/// **A `Leader` publish whose term is given up after its write landed is
/// answered at its commit, not on the lease.** The lease is asked once the
/// batch is on the log, so a transfer or a higher term seen while it was
/// being written counts. Asked before the write, the publisher would hold
/// an acknowledgement from a leader that had already given its term away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_term_given_up_after_the_write_landed_is_not_answered_on_the_lease() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    led.cut_followers();
    let held = publish_held(&led, &broker, &handle, b"mine");
    held.landed.await.expect("written");
    leader_member(&led).lease().void_through(led.term);
    held.go.send(()).expect("held");
    unanswered(&held.published).await;
    nothing(&mut receiver).await;
    assert_eq!(led.status().role, Role::Leader);

    led.heal();
    let outcome = held.published.await.expect("joined").expect("committed");
    let offset = led.committed + 1;
    assert_eq!(outcome.offsets, Some((offset, offset)));
    assert_eq!(next(&mut receiver).await.base_offset(), Some(offset));
    led.cluster.stop().await;
}

/// **A `Leader` publish written across a suspend is answered at its
/// commit.** The leader's clock stood still while the others may have
/// elected, so the majority's answer looks newer than it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_publish_written_across_a_suspend_waits_for_its_commit() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    led.cut_followers();
    let held = publish_held(&led, &broker, &handle, b"mine");
    held.landed.await.expect("written");
    let slept = Duration::from_secs(5);
    leader_member(&led).lease().suspended_for(slept);
    held.go.send(()).expect("held");
    unanswered(&held.published).await;
    nothing(&mut receiver).await;

    led.heal();
    let outcome = held.published.await.expect("joined").expect("committed");
    let offset = led.committed + 1;
    assert_eq!(outcome.offsets, Some((offset, offset)));
    assert_eq!(next(&mut receiver).await.base_offset(), Some(offset));
    led.cluster.stop().await;
}

/// **A `Leader` publish that is on the leader's log only after the lease
/// ran out waits for its commit.** A leader nobody has answered for a
/// lease may have been replaced, and the new leader's log does not hold
/// what it wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_publish_outside_the_lease_waits_for_its_commit() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    // Nobody hears the leader and it hears nobody, so it stays leader.
    led.cluster.net.cut.lock().extend(BROKERS.map(String::from));
    let held = publish_held(&led, &broker, &handle, b"late");
    held.landed.await.expect("written");
    let first = led.committed + 1;
    assert!(
        led.status()
            .flushed
            .is_some_and(|at| at.index > led.committed)
    );
    tokio::time::sleep(timers().lease + Duration::from_millis(100)).await;
    held.go.send(()).expect("held");
    unanswered(&held.published).await;
    nothing(&mut receiver).await;
    assert_eq!(led.status().role, Role::Leader);

    // The commit answers it, unless the others elected meanwhile.
    led.heal();
    if let Ok(outcome) = held.published.await.expect("joined") {
        assert_eq!(outcome.offsets, Some((first, first)));
        assert_eq!(next(&mut receiver).await.base_offset(), Some(first));
    }
    led.cluster.stop().await;
}

/// **A leader the others replaced after its write landed answers no
/// `Leader` publish, though it has not heard of the new leader and still
/// says it leads.** Its lease ran out before the others could elect.
/// Answered on its own log, the publisher would hold an acknowledgement
/// for a record the new leader's log has already replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_leader_answers_no_leader_publish() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    led.cut_followers();
    let held = publish_held(&led, &broker, &handle, b"mine");
    held.landed.await.expect("written");
    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old]);
    let theirs = led.cluster.write(0, b"theirs").await;
    assert!(theirs > led.committed + 1, "the offset was not taken");
    assert_ne!(led.cluster.elected(0).await, led.leader);
    assert_eq!(led.status().role, Role::Leader, "the old leader heard");
    held.go.send(()).expect("held");
    unanswered(&held.published).await;

    led.heal();
    eventually(|| match handle.read_bound() {
        ReadBound::Committed(bound) => (bound > theirs).then_some(()),
        _ => None,
    })
    .await;
    let outcome = held.published.await.expect("joined");
    assert!(
        matches!(outcome, Err(CommitError::LeadershipLost { .. })),
        "{outcome:?}"
    );
    nothing(&mut receiver).await;
    led.cluster.stop().await;
}

/// **A batch written at another term than it was admitted at is not
/// answered on the lease, though the lease holds for the term that wrote
/// it.** The stream may not be at that term's committed tail, so the
/// batch is given up, and its publisher must not hear otherwise first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_written_at_another_term_is_not_answered_on_the_lease() {
    let mut led = led().await;
    let written = led.propose(1).await;
    let (on_lease, mut answered) = oneshot::channel();
    let batch = Proposed {
        shard: shard(0),
        term: Some(written.generation + 1),
        count: 1,
        timeout: PATIENCE,
        on_lease: Some((AnswerAt::Lease, on_lease)),
    };
    let admitted = led.engine.admit_local_ack(&shard(0), written.generation);
    admitted.expect("the lease holds");
    let landed = async move { Ok::<_, Unacknowledged>(written) };
    let outcome = settled(led.engine.clone(), batch, landed).await;
    assert!(outcome.is_err(), "settled at another term");
    assert_eq!(
        answered.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    );
    led.cluster.stop().await;
}

/// **A `Leader` publish answered on the lease and then replaced by a later
/// leader reaches no subscriber.** That loss is the level's contract. The
/// answer must not put the batch in front of subscribers: the log the
/// group agreed on never held it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_answered_on_the_lease_and_then_replaced_reaches_no_subscriber() {
    let mut led = led().await;
    let (broker, handle) = broker_on(&led.engine).await;
    let (mut receiver, _guard) = subscriber(&broker).await;
    leased(&led).await;

    led.cut_followers();
    let published = publish_on_lease(&led, &broker, &handle, Bytes::from_static(b"mine"));
    let outcome = published.await.expect("joined").expect("on the lease");
    let mine = led.committed + 1;
    assert_eq!(outcome.offsets, Some((mine, mine)));

    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old]);
    let theirs = led.cluster.write(0, b"theirs").await;
    assert!(theirs > mine, "the offset was not taken");

    led.heal();
    eventually(|| match handle.read_bound() {
        ReadBound::Committed(bound) => (bound > theirs).then_some(()),
        _ => None,
    })
    .await;
    nothing(&mut receiver).await;
    led.cluster.stop().await;
}
