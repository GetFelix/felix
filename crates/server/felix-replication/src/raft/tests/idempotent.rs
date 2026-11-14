//! An idempotent producer's batch is written once on a Raft shard, whoever
//! leads when its re-send arrives. See `docs/per-shard-raft.md`,
//! "Idempotent publishes".

use felix_broker::{
    Broker, BrokerError, ConsistencyLevel, IdempotentOutcome, SequenceReuse, StreamHandle,
    StreamLog, StreamMetadata,
};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use super::commit::led;
use super::*;
use crate::engine::CommitError;

const PRODUCER: u64 = 7;

/// A broker whose `orders` stream is over `member`'s own log, as the
/// service opens a Raft shard, with its readers bound by `engine`.
async fn broker_over(
    cluster: &Cluster,
    member: &MemberId,
    engine: &RaftEngine,
) -> (Broker, StreamHandle, TempDir) {
    let root = tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(root.path(), config).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.leave_durable_streams_to_engines();
    broker.set_read_bounds(Arc::new(RaftReads::new(engine.clone())));
    broker.register_tenant("t").await.expect("tenant");
    broker.register_namespace("t", "ns").await.expect("ns");
    let metadata = StreamMetadata {
        durable: true,
        consistency: ConsistencyLevel::Leader,
        ..Default::default()
    };
    let registered = broker.register_stream("t", "ns", "orders", metadata);
    registered.await.expect("stream");
    let log = cluster.logs[&(member.broker.to_string(), 0)].clone();
    let handle = broker
        .open_stream_on_engine_log("t", "ns", "orders", 0, StreamLog::from_log(log))
        .await
        .expect("handle");
    engine
        .follow_commits(&shard(0), handle.clone())
        .expect("a member here");
    (broker, handle, root)
}

struct Producer<'a> {
    engine: &'a RaftEngine,
    broker: &'a Broker,
    handle: &'a StreamHandle,
    term: u64,
}

impl Producer<'_> {
    /// Check and propose `payloads` under `sequence`.
    async fn propose(
        &self,
        sequence: u64,
        payloads: &[&'static [u8]],
        reuse: SequenceReuse,
    ) -> Result<IdempotentPublish, BrokerError> {
        let producer = Sequenced {
            producer_id: PRODUCER,
            sequence,
        };
        let payloads = payloads.iter().map(|p| Bytes::from_static(p)).collect();
        self.engine
            .publish_idempotent(
                self.broker,
                self.handle,
                &shard(0),
                self.term,
                producer,
                payloads,
                reuse,
                PATIENCE,
            )
            .await
    }

    async fn publish(
        &self,
        sequence: u64,
        payloads: &[&'static [u8]],
    ) -> Result<IdempotentOutcome, CommitError> {
        let proposed = self
            .propose(sequence, payloads, SequenceReuse::Refuse)
            .await;
        proposed.expect("proposed").complete().await
    }
}

fn offsets(outcome: &IdempotentOutcome) -> (u64, u64) {
    outcome.outcome.offsets.expect("offsets")
}

/// The payloads `handle`'s log holds from `from` on, as a reader gets them.
async fn log_from(broker: &Broker, from: u64) -> Vec<Bytes> {
    let read = broker.read_durable("t", "ns", "orders", 0, from, 1 << 20);
    let records = read.await.expect("read");
    records.into_iter().map(|record| record.payload).collect()
}

/// **A batch sent again under its sequence is answered with where it first
/// landed and is not written again, and a sequence the log cannot place is
/// refused the way the hybrid refuses it.** Written again, the re-send of
/// a batch whose answer was lost would be in the stream twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_sent_again_is_answered_and_not_written_again() {
    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    let producer = Producer {
        engine: &led.engine,
        broker: &broker,
        handle: &handle,
        term: led.term,
    };
    let first = led.committed + 1;

    let sent = producer.publish(0, &[b"a", b"b"]).await.expect("committed");
    assert_eq!(
        (offsets(&sent), sent.duplicate),
        ((first, first + 1), false)
    );
    let again = producer.publish(0, &[b"a", b"b"]).await.expect("answered");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first, first + 1), true)
    );
    let next = producer.publish(1, &[b"c"]).await.expect("committed");
    assert_eq!(
        (offsets(&next), next.duplicate),
        ((first + 2, first + 2), false)
    );

    let refused =
        |proposed: Result<IdempotentPublish, BrokerError>| proposed.err().expect("refused");
    let gap = producer.propose(3, &[b"e"], SequenceReuse::Refuse).await;
    assert!(matches!(
        refused(gap),
        BrokerError::SequenceGap { expected: 2 }
    ));
    let reused = producer.propose(1, &[b"x"], SequenceReuse::Refuse).await;
    assert!(matches!(
        refused(reused),
        BrokerError::SequenceReused { sequence: 1 }
    ));
    // A client that cannot read that refusal is answered as for a re-send.
    let quiet = producer.propose(1, &[b"x"], SequenceReuse::AnswerDuplicate);
    let quiet = quiet.await.expect("answered").complete().await;
    let quiet = quiet.expect("answered");
    assert_eq!(
        (offsets(&quiet), quiet.duplicate),
        ((first + 2, first + 2), true)
    );
    let stranger = Sequenced {
        producer_id: PRODUCER + 1,
        sequence: 4,
    };
    let orders = shard(0);
    let unknown = led.engine.publish_idempotent(
        &broker,
        &handle,
        &orders,
        led.term,
        stranger,
        vec![Bytes::from_static(b"z")],
        SequenceReuse::Refuse,
        PATIENCE,
    );
    assert!(matches!(
        refused(unknown.await),
        BrokerError::UnknownProducer { .. }
    ));

    // Nothing but the two batches was written.
    assert_eq!(log_from(&broker, first).await, ["a", "b", "c"]);
    led.cluster.stop().await;
}

/// **A re-send that finds its batch written but not yet committed is
/// answered only once the batch commits.** Answered from the log alone,
/// the producer would hold an acknowledgement for a record a failover can
/// still take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resend_of_an_uncommitted_batch_waits_for_its_commit() {
    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    let producer = Producer {
        engine: &led.engine,
        broker: &broker,
        handle: &handle,
        term: led.term,
    };

    led.cut_followers();
    let sent = producer.propose(0, &[b"a"], SequenceReuse::Refuse).await;
    let sent = tokio::spawn(sent.expect("proposed").complete());
    // Waits for the first send to be on the log, then finds it there.
    let again = producer.propose(0, &[b"a"], SequenceReuse::Refuse).await;
    let again = tokio::spawn(again.expect("held").complete());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!sent.is_finished(), "answered before a majority held it");
    assert!(
        !again.is_finished(),
        "a re-send was answered before the commit"
    );

    led.heal();
    let at = led.committed + 1;
    let sent = sent.await.expect("joined").expect("committed");
    assert_eq!((offsets(&sent), sent.duplicate), ((at, at), false));
    let again = again.await.expect("joined").expect("committed");
    assert_eq!((offsets(&again), again.duplicate), ((at, at), true));
    assert_eq!(log_from(&broker, at).await, ["a"]);
    led.cluster.stop().await;
}

/// **A producer's batches handed over back to back are each checked
/// against a log that holds the one before.** Checked while the batch
/// before was still on its way to the log, the second would be refused as
/// a gap, or written ahead of a batch that then failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producers_next_batch_is_checked_once_the_one_before_is_written() {
    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    let producer = Producer {
        engine: &led.engine,
        broker: &broker,
        handle: &handle,
        term: led.term,
    };

    let mut proposed = Vec::new();
    for (sequence, payload) in [b"a", b"b", b"c", b"d"].into_iter().enumerate() {
        let payloads = [&payload[..]];
        let batch = producer.propose(sequence as u64, &payloads, SequenceReuse::Refuse);
        proposed.push(batch.await.expect("the next in sequence"));
    }
    let first = led.committed + 1;
    for (at, proposed) in (first..).zip(proposed) {
        let outcome = proposed.complete().await.expect("committed");
        assert_eq!((offsets(&outcome), outcome.duplicate), ((at, at), false));
    }
    assert_eq!(log_from(&broker, first).await, ["a", "b", "c", "d"]);
    led.cluster.stop().await;
}

/// **A batch re-sent to the leader elected after the one that took it is
/// answered from the new leader's log, and the producer carries on from
/// there.** The marks are in the entries, so every member that holds a
/// batch knows whose it is. Kept in the old leader's memory, the sequence
/// would be unknown here and the batch written a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_resent_to_the_next_leader_is_not_written_again() {
    let mut led = led().await;
    let first = led.committed + 1;
    {
        let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
        let producer = Producer {
            engine: &led.engine,
            broker: &broker,
            handle: &handle,
            term: led.term,
        };
        let sent = producer.publish(0, &[b"a", b"b"]).await.expect("committed");
        assert_eq!(offsets(&sent), (first, first + 1));
        let sent = producer.publish(1, &[b"c"]).await.expect("committed");
        assert_eq!(offsets(&sent), (first + 2, first + 2));
    }

    // The leader is cut off, and the others elect one of themselves.
    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old]);
    let blank = led.cluster.write(0, b"theirs").await;
    let leader = led.cluster.elected(0).await;
    assert_ne!(leader, led.leader);
    let runtime = led.cluster.runtime(&leader.broker);
    let term = runtime.status(&shard(0)).expect("status").term;
    assert!(term > led.term);
    let engine = RaftEngine::new(runtime);
    eventually(|| (engine.read_bound(&shard(0)) == Ok(blank + 1)).then_some(())).await;

    let (broker, handle, _root) = broker_over(&led.cluster, &leader, &engine).await;
    let producer = Producer {
        engine: &engine,
        broker: &broker,
        handle: &handle,
        term,
    };
    // The batch whose answer the producer may have lost, and the one before.
    let again = producer.publish(1, &[b"c"]).await.expect("answered");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first + 2, first + 2), true)
    );
    let again = producer.publish(0, &[b"a", b"b"]).await.expect("answered");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first, first + 1), true)
    );
    let next = producer.publish(2, &[b"d"]).await.expect("committed");
    assert_eq!(
        (offsets(&next), next.duplicate),
        ((blank + 1, blank + 1), false)
    );
    assert_eq!(
        log_from(&broker, first).await,
        ["a", "b", "c", "theirs", "d"]
    );
    led.heal();
    led.cluster.stop().await;
}

/// **A batch of several records whose leader stopped after only its start
/// was replicated is finished by the re-send, not written again.** The
/// next leader's log holds the start, and its own first entry has closed
/// it. The re-send writes just the records still owed and is answered from
/// the start's first offset. Written again whole, the start would be in
/// the stream twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_cut_by_a_leader_change_is_finished_by_its_resend() {
    use crate::raft::types::ProducerMark;

    let mut led = led().await;
    let first = led.committed + 1;
    {
        let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
        let producer = Producer {
            engine: &led.engine,
            broker: &broker,
            handle: &handle,
            term: led.term,
        };
        let sent = producer.publish(0, &[b"a"]).await.expect("committed");
        assert_eq!(offsets(&sent), (first, first));
    }
    // What a leader that stops partway leaves: the first two of batch 1's
    // three records, as their entries.
    let group = led.cluster.runtime(&led.leader.broker).group(&shard(0));
    let group = group.expect("group");
    let marks = [
        ProducerMark::Opens {
            producer_id: PRODUCER,
            sequence: 1,
            len: 3,
        },
        ProducerMark::Continues,
    ];
    for (payload, mark) in [b"b", b"c"].into_iter().zip(marks) {
        let command = Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(payload),
            producer: Some(mark),
            written: None,
        };
        group.raft().client_write(command).await.expect("written");
    }

    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old]);
    let theirs = led.cluster.write(0, b"theirs").await;
    let leader = led.cluster.elected(0).await;
    assert_ne!(leader, led.leader);
    let runtime = led.cluster.runtime(&leader.broker);
    let term = runtime.status(&shard(0)).expect("status").term;
    let engine = RaftEngine::new(runtime);
    eventually(|| (engine.read_bound(&shard(0)) == Ok(theirs + 1)).then_some(())).await;

    let (broker, handle, _root) = broker_over(&led.cluster, &leader, &engine).await;
    let producer = Producer {
        engine: &engine,
        broker: &broker,
        handle: &handle,
        term,
    };
    // The start has to be the start of this batch.
    let other = producer
        .propose(1, &[b"b", b"x", b"d"], SequenceReuse::Refuse)
        .await;
    assert!(matches!(
        other.map(|_| ()),
        Err(BrokerError::SequenceReused { sequence: 1 })
    ));

    let sent = producer.publish(1, &[b"b", b"c", b"d"]).await;
    let sent = sent.expect("committed");
    assert_eq!(
        (offsets(&sent), sent.duplicate),
        ((first + 1, theirs + 1), false)
    );
    assert_eq!(
        log_from(&broker, first).await,
        ["a", "b", "c", "theirs", "d"]
    );

    let again = producer.publish(1, &[b"b", b"c", b"d"]).await;
    let again = again.expect("answered");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first + 1, theirs + 1), true)
    );
    let next = producer.publish(2, &[b"e"]).await.expect("committed");
    assert_eq!(offsets(&next), (theirs + 2, theirs + 2));
    assert_eq!(
        log_from(&broker, first).await,
        ["a", "b", "c", "theirs", "d", "e"]
    );
    led.heal();
    led.cluster.stop().await;
}
