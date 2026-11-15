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
        let turn = self.broker.producer_turn(self.handle, PRODUCER).await;
        let payloads = payloads.iter().map(|p| Bytes::from_static(p)).collect();
        self.engine.publish_idempotent(
            self.broker,
            self.handle,
            &shard(0),
            self.term,
            turn,
            sequence,
            payloads,
            Default::default(),
            reuse,
            PATIENCE,
        )
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

    let sent = producer.publish(0, &[b"a"]).await.expect("committed");
    assert_eq!((offsets(&sent), sent.duplicate), ((first, first), false));
    let again = producer.publish(0, &[b"a"]).await.expect("answered");
    assert_eq!((offsets(&again), again.duplicate), ((first, first), true));
    let next = producer.publish(1, &[b"c"]).await.expect("committed");
    assert_eq!(
        (offsets(&next), next.duplicate),
        ((first + 1, first + 1), false)
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
        ((first + 1, first + 1), true)
    );
    let stranger = broker.producer_turn(&handle, PRODUCER + 1).await;
    let orders = shard(0);
    let unknown = led.engine.publish_idempotent(
        &broker,
        &handle,
        &orders,
        led.term,
        stranger,
        4,
        vec![Bytes::from_static(b"z")],
        Default::default(),
        SequenceReuse::Refuse,
        PATIENCE,
    );
    assert!(matches!(
        refused(unknown),
        BrokerError::UnknownProducer { .. }
    ));

    // Nothing but the two batches was written.
    assert_eq!(log_from(&broker, first).await, ["a", "c"]);
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

/// **A batch refused because this broker does not lead the term it was
/// routed at is not written, and the producer's next batch is refused as a
/// gap.** Written at whatever term the broker leads by then, it would be
/// in the log behind a check that never saw it, and the next batch would
/// land ahead of a re-send.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_refused_at_its_term_stays_unwritten_and_the_next_is_a_gap() {
    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    let producer = Producer {
        engine: &led.engine,
        broker: &broker,
        handle: &handle,
        term: led.term,
    };
    let first = led.committed + 1;
    let sent = producer.publish(0, &[b"a"]).await.expect("committed");
    assert_eq!(offsets(&sent), (first, first));

    // Routed at a term this broker does not lead.
    let elsewhere = Producer {
        term: led.term + 1,
        ..producer
    };
    let lost = elsewhere.publish(1, &[b"b"]).await;
    assert!(
        matches!(lost, Err(CommitError::LeadershipLost { .. })),
        "{lost:?}"
    );
    let next = producer.propose(2, &[b"d"], SequenceReuse::Refuse).await;
    assert!(matches!(
        next.map(|_| ()),
        Err(BrokerError::SequenceGap { expected: 1 })
    ));
    assert_eq!(log_from(&broker, first).await, ["a"]);

    let again = producer.publish(1, &[b"b"]).await.expect("committed");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first + 1, first + 1), false)
    );
    led.cluster.stop().await;
}

/// **A batch of several records is refused with nothing written, and it
/// leaves the producer no place.** Its records would be an entry each. A
/// leader change or a lapsed lease can cut those anywhere, and the re-send
/// of a cut batch would write records twice or be refused as another batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_of_several_records_is_refused_with_nothing_written() {
    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    let producer = Producer {
        engine: &led.engine,
        broker: &broker,
        handle: &handle,
        term: led.term,
    };
    let first = led.committed + 1;

    let several = producer.propose(0, &[b"a", b"b"], SequenceReuse::Refuse);
    assert!(matches!(
        several.await.map(|_| ()),
        Err(BrokerError::NotOnEngineStream { .. })
    ));
    // Sequence 0 is still the producer's first.
    let sent = producer.publish(0, &[b"a"]).await.expect("committed");
    assert_eq!((offsets(&sent), sent.duplicate), ((first, first), false));
    assert_eq!(log_from(&broker, first).await, ["a"]);
    led.cluster.stop().await;
}

/// **Every record of a batch is an entry that names the batch and its own
/// place in it, and reads back out of the log as it went in.** A record
/// that only says it continues the one before it is refused: entries of
/// other writers can lie between a batch's records here.
#[test]
fn a_record_of_a_batch_names_its_place_in_the_log() {
    use crate::raft::types::ProducerMark;
    use felix_storage::log::{BatchPiece, ProducerBatch, RecordMark};

    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: PRODUCER,
        sequence: 4,
        len: 3,
    });
    let piece = RecordMark::Piece(BatchPiece {
        producer_id: PRODUCER,
        sequence: 4,
        position: 2,
        len: 3,
    });
    for (mark, position) in [(opens, 0), (piece, 2)] {
        let read = ProducerMark::of_record(mark).expect("a record of a batch");
        let expected = ProducerMark {
            producer_id: PRODUCER,
            sequence: 4,
            position,
            len: 3,
        };
        assert_eq!(read, Some(expected));
        assert_eq!(ProducerMark::record_mark(read), mark);
    }
    assert!(ProducerMark::of_record(RecordMark::Continues).is_err());
    assert_eq!(
        ProducerMark::of_record(RecordMark::None).expect("plain"),
        None
    );
}

/// **A batch of several records whose leader stopped after only its start
/// was replicated is finished by the re-send, not written again.** The
/// next leader's log holds the start, and its own first entry has cut it.
/// The re-send writes just the records the log does not hold and is
/// answered from the start's first offset. Written again whole, the start
/// would be in the stream twice.
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
    // four records, as their entries.
    let group = led.cluster.runtime(&led.leader.broker).group(&shard(0));
    let group = group.expect("group");
    for (position, payload) in [b"b", b"c"].into_iter().enumerate() {
        let command = Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(payload),
            producer: Some(ProducerMark {
                producer_id: PRODUCER,
                sequence: 1,
                position: position as u32,
                len: 4,
            }),
            expected: None,
            trim: None,
            commit: false,
            publisher: None,
            headers: None,
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
    // The start has to be the start of this batch, and the batch as long.
    let other = producer
        .propose(1, &[b"b", b"x", b"d", b"e"], SequenceReuse::Refuse)
        .await;
    assert!(matches!(
        other.map(|_| ()),
        Err(BrokerError::SequenceReused { sequence: 1 })
    ));
    // Also for a client that is answered a duplicate on a held batch:
    // finishing b, c with y, z would write a batch nobody sent.
    let quiet = producer
        .propose(1, &[b"w", b"x", b"y", b"z"], SequenceReuse::AnswerDuplicate)
        .await;
    assert!(matches!(
        quiet.map(|_| ()),
        Err(BrokerError::SequenceReused { sequence: 1 })
    ));
    assert_eq!(log_from(&broker, first).await, ["a", "b", "c", "theirs"]);
    let shorter = producer
        .propose(1, &[b"b", b"c", b"d"], SequenceReuse::Refuse)
        .await;
    assert!(matches!(
        shorter.map(|_| ()),
        Err(BrokerError::SequenceExpired { sequence: 1 })
    ));

    let sent = producer.publish(1, &[b"b", b"c", b"d", b"e"]).await;
    let sent = sent.expect("committed");
    assert_eq!(
        (offsets(&sent), sent.duplicate),
        ((first + 1, theirs + 2), false)
    );
    assert_eq!(
        log_from(&broker, first).await,
        ["a", "b", "c", "theirs", "d", "e"]
    );

    let again = producer.publish(1, &[b"b", b"c", b"d", b"e"]).await;
    let again = again.expect("answered");
    assert_eq!(
        (offsets(&again), again.duplicate),
        ((first + 1, theirs + 2), true)
    );
    let next = producer.publish(2, &[b"f"]).await.expect("committed");
    assert_eq!(offsets(&next), (theirs + 3, theirs + 3));
    assert_eq!(
        log_from(&broker, first).await,
        ["a", "b", "c", "theirs", "d", "e", "f"]
    );
    led.heal();
    led.cluster.stop().await;
}

/// **A re-send that finishes a cut batch stores each record's own headers.**
/// The records the log holds are dropped from the re-send's headers along
/// with their payloads; left in, `d` and `e` would be stored under `b`'s and
/// `c`'s headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_batch_finished_with_headers_keeps_each_records_own() {
    use crate::raft::types::ProducerMark;
    use felix_storage::log::{AppendOnlyLog, ReadRange};

    let mut led = led().await;
    let (broker, handle, _root) = broker_over(&led.cluster, &led.leader, &led.engine).await;
    broker.record_headers_when(|| true);
    broker.allow_raft_record_headers_when(|| true);
    let first = led.committed + 1;
    let group = led.cluster.runtime(&led.leader.broker).group(&shard(0));
    let group = group.expect("group");
    for (position, payload) in [b"b", b"c"].into_iter().enumerate() {
        let command = Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(payload),
            producer: Some(ProducerMark {
                producer_id: PRODUCER,
                sequence: 0,
                position: position as u32,
                len: 4,
            }),
            expected: None,
            trim: None,
            commit: false,
            publisher: None,
            headers: None,
            written: None,
        };
        group.raft().client_write(command).await.expect("written");
    }

    let block = |tag: &'static [u8]| Bytes::from([b"\x00\x01k\x00\x01".as_slice(), tag].concat());
    let attribution = felix_broker::Attribution {
        publisher: None,
        headers: Some(vec![
            Some(block(b"b")),
            Some(block(b"c")),
            Some(block(b"d")),
            Some(block(b"e")),
        ]),
    };
    let turn = broker.producer_turn(&handle, PRODUCER).await;
    let payloads = [b"b", b"c", b"d", b"e"]
        .map(|p| Bytes::from_static(p))
        .to_vec();
    let proposed = led
        .engine
        .publish_idempotent(
            &broker,
            &handle,
            &shard(0),
            led.term,
            turn,
            0,
            payloads,
            attribution,
            SequenceReuse::Refuse,
            PATIENCE,
        )
        .expect("proposed");
    let sent = proposed.complete().await.expect("committed");
    assert_eq!(offsets(&sent), (first, first + 3));

    let log = led.cluster.logs[&(led.leader.broker.to_string(), 0)].clone();
    eventually(|| (log.durable_offset() > first + 3).then_some(())).await;
    let read = ReadRange {
        start: first,
        max_bytes: 1 << 20,
    };
    let page = log.read_range(read).await.expect("read");
    let stored: Vec<_> = page.records.iter().map(|r| r.headers.as_deref()).collect();
    assert_eq!(stored, [None, None, Some(&block(b"d")), Some(&block(b"e"))]);
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
        let sent = producer.publish(0, &[b"a"]).await.expect("committed");
        assert_eq!(offsets(&sent), (first, first));
        let sent = producer.publish(1, &[b"c"]).await.expect("committed");
        assert_eq!(offsets(&sent), (first + 1, first + 1));
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
        ((first + 1, first + 1), true)
    );
    let again = producer.publish(0, &[b"a"]).await.expect("answered");
    assert_eq!((offsets(&again), again.duplicate), ((first, first), true));
    let next = producer.publish(2, &[b"d"]).await.expect("committed");
    assert_eq!(
        (offsets(&next), next.duplicate),
        ((blank + 1, blank + 1), false)
    );
    assert_eq!(log_from(&broker, first).await, ["a", "c", "theirs", "d"]);
    led.heal();
    led.cluster.stop().await;
}

/// **A member that installs a snapshot past a cut batch it was never sent
/// refuses the batch's re-send, as the members that purged it do.** The
/// snapshot carries the cut batches its sender refuses by name. Without
/// them the member, once it leads, knows nothing of the producer and takes
/// sequence 0 as new, writing the start of the batch a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_installed_past_a_cut_batch_refuses_its_resend() {
    use crate::raft::types::ProducerMark;
    use openraft::type_config::alias::{LeaderIdOf, VoteOf};
    use openraft::vote::{RaftLeaderId, RaftVote};

    let mut led = led().await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *led.leader.broker)
        .expect("a follower");
    led.cluster.net.cut.lock().insert(behind.into());

    // Two of batch 0's four records, then a record that cuts it.
    let group = led.cluster.runtime(&led.leader.broker).group(&shard(0));
    let group = group.expect("group");
    for (position, payload) in [b"a", b"b"].into_iter().enumerate() {
        let command = Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(payload),
            producer: Some(ProducerMark {
                producer_id: PRODUCER,
                sequence: 0,
                position: position as u32,
                len: 4,
            }),
            expected: None,
            trim: None,
            commit: false,
            publisher: None,
            headers: None,
            written: None,
        };
        group.raft().client_write(command).await.expect("written");
    }
    let last = led.cluster.write(0, b"after").await;
    assert_eq!(led.cluster.elected(0).await, led.leader);

    // Every member that holds the batch purges past it.
    let holders: Vec<_> = BROKERS.iter().filter(|b| **b != behind).collect();
    for broker in &holders {
        led.cluster.snapshot(broker, 0, last).await;
        let member = led.cluster.runtime(broker).group(&shard(0)).expect("group");
        member
            .raft()
            .trigger()
            .purge_log(last)
            .await
            .expect("purge");
        let purged = eventually(|| led.cluster.purged(broker, 0).filter(|at| *at >= last));
        assert_eq!(purged.await, last);
    }

    // What a leader whose log is purged past a member sends it.
    let snapshot = group.raft().get_snapshot().await.expect("read");
    let snapshot = snapshot.expect("snapshot");
    assert!(snapshot.meta.last_log_id.is_some_and(|id| id.index >= last));
    let leads = LeaderIdOf::<ShardTypes>::new(group.status().term, led.leader.clone());
    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true);
    let installing = led.cluster.runtime(behind).group(&shard(0)).expect("group");
    installing
        .raft()
        .install_full_snapshot(vote, snapshot)
        .await
        .expect("install");
    for broker in BROKERS {
        let log = &led.cluster.logs[&(broker.to_string(), 0)];
        assert_eq!(
            log.producer_sequence(PRODUCER, 0),
            felix_storage::disk_log::ProducerSequence::Expired,
            "{broker}"
        );
    }

    // It leads from the snapshot. Its leader's log is purged past it, and
    // the snapshot it was sent is refused, so it is not caught up by
    // appends first: the leader's last entry is the snapshot's.
    led.heal();
    let member = MemberId::new(behind, 1);
    let transfer = group.raft().trigger().transfer_leader(member.clone()).await;
    transfer.expect("transfer");
    let runtime = led.cluster.runtime(behind);
    let leads = || {
        runtime
            .status(&shard(0))
            .filter(|status| status.leader == Some(member.clone()))
    };
    let term = eventually(|| leads().map(|status| status.term)).await;
    let engine = RaftEngine::new(runtime.clone());
    eventually(|| {
        engine
            .read_bound(&shard(0))
            .ok()
            .filter(|bound| *bound > last + 1)
    })
    .await;

    let (broker, handle, _root) = broker_over(&led.cluster, &member, &engine).await;
    let producer = Producer {
        engine: &engine,
        broker: &broker,
        handle: &handle,
        term,
    };
    let again = producer
        .propose(0, &[b"a", b"b", b"c", b"d"], SequenceReuse::Refuse)
        .await;
    assert!(matches!(
        again.map(|_| ()),
        Err(BrokerError::SequenceExpired { sequence: 0 })
    ));
    assert!(log_from(&broker, last + 1).await.is_empty());
    led.cluster.stop().await;
}
