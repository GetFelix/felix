//! Hydrating a durable stream's replay ring on restart.

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::{Broker, DurableStorage, StreamMetadata};

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker(storage: &DurableStorage) -> Broker {
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
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
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    broker
}

/// A log ending in a generation-start record still hydrates the ring. The
/// window reaches the tail even though its last client record is one short
/// of it, so a cursor from before the restart replays from memory rather
/// than being told it is too old.
#[tokio::test]
async fn a_log_ending_in_a_generation_start_still_hydrates_the_ring() {
    let dir = tempfile::tempdir().expect("dir");
    let cursor;
    {
        let storage = DurableStorage::open(dir.path(), config()).expect("storage");
        let broker = broker(&storage).await;
        cursor = broker
            .cursor_tail("t1", "ns", "orders", 0)
            .await
            .expect("cursor");
        for value in ["a", "b"] {
            broker
                .publish("t1", "ns", "orders", Bytes::from(value))
                .await
                .expect("publish");
        }
        let log = storage.open_stream("t1", "ns", "orders", 0).expect("log");
        assert_eq!(log.append_generation_start(2).await.expect("marker"), 2);
        storage.shutdown().await.expect("shutdown");
    }

    let storage = DurableStorage::open(dir.path(), config()).expect("storage");
    let broker = broker(&storage).await;
    let (backlog, _sub) = broker
        .subscribe_with_cursor("t1", "ns", "orders", 0, cursor)
        .await
        .expect("replay from the ring");
    assert_eq!(backlog, vec![Bytes::from("a"), Bytes::from("b")]);
    let tail = broker
        .cursor_tail("t1", "ns", "orders", 0)
        .await
        .expect("tail");
    assert_eq!(tail.next_seq(), 3);
}

/// Membership and counter records are not in the ring and do not cut its
/// window short: a cursor from before the restart replays the events around
/// them from memory, and the tail counts their offsets.
#[tokio::test]
async fn membership_and_counter_records_stay_out_of_the_ring() {
    use felix_storage::log::{AppendRecord, RecordMark};
    let dir = tempfile::tempdir().expect("dir");
    let cursor;
    {
        let storage = DurableStorage::open(dir.path(), config()).expect("storage");
        let broker = broker(&storage).await;
        cursor = broker
            .cursor_tail("t1", "ns", "orders", 0)
            .await
            .expect("cursor");
        for value in ["a", "b"] {
            broker
                .publish("t1", "ns", "orders", Bytes::from(value))
                .await
                .expect("publish");
        }
        let log = storage.open_stream("t1", "ns", "orders", 0).expect("log");
        let records: Vec<AppendRecord> = [
            (RecordMark::Membership, "set"),
            (RecordMark::CounterDelta, "add"),
            (RecordMark::None, "c"),
            (RecordMark::CounterCheckpoint, "sums"),
        ]
        .into_iter()
        .map(|(mark, payload)| AppendRecord {
            payload: Bytes::from(payload),
            timestamp_micros: 1,
            mark,
            publisher: None,
            headers: None,
        })
        .collect();
        log.append_v9_for_tests(&records, 1)
            .await
            .expect("v9 append");
        storage.shutdown().await.expect("shutdown");
    }

    let storage = DurableStorage::open(dir.path(), config()).expect("storage");
    let broker = broker(&storage).await;
    let (backlog, _sub) = broker
        .subscribe_with_cursor("t1", "ns", "orders", 0, cursor)
        .await
        .expect("replay from the ring");
    assert_eq!(
        backlog,
        vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
    );
    let tail = broker
        .cursor_tail("t1", "ns", "orders", 0)
        .await
        .expect("tail");
    assert_eq!(tail.next_seq(), 6);
}

mod engine_log {
    //! A stream over its engine's log: a shard on Raft.

    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use felix_storage::disk_log::DiskLog;
    use felix_storage::{CommitSequencer, EphemeralCache};
    use parking_lot::Mutex;

    use super::config;
    use crate::durable::StreamLog;
    use crate::{
        Broker, BrokerError, LogDelivery, ProposalCommitted, ProposalFailed, ReadBound, ReadBounds,
        SequenceReuse, StreamHandle, StreamMetadata,
    };

    #[derive(Debug)]
    struct Bound(Mutex<ReadBound>);

    impl ReadBounds for Bound {
        fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
            *self.0.lock()
        }
    }

    /// A broker that leaves durable streams to their engines, with one
    /// registered, and the bound its engine states.
    async fn broker(bound: ReadBound) -> (Broker, Arc<Bound>) {
        broker_with(bound, None).await
    }

    /// [`broker`], with a ring of `capacity` records.
    async fn broker_with(bound: ReadBound, capacity: Option<usize>) -> (Broker, Arc<Bound>) {
        let bounds = Arc::new(Bound(Mutex::new(bound)));
        let mut broker = Broker::new(EphemeralCache::new().into());
        if let Some(capacity) = capacity {
            broker = broker.with_log_capacity(capacity).expect("capacity");
        }
        broker.leave_durable_streams_to_engines();
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
                "orders",
                StreamMetadata {
                    durable: true,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
        (broker, bounds)
    }

    /// The engine's log with `records` entries written, committed or not.
    async fn engine_log(dir: &std::path::Path, records: usize) -> StreamLog {
        let log = StreamLog::from_log(DiskLog::open(dir, "orders", config()).expect("log"));
        if records == 0 {
            return log;
        }
        let payloads: Vec<Bytes> = (0..records).map(|n| Bytes::from(n.to_string())).collect();
        let order = Arc::new(CommitSequencer::new(0));
        let (pending, _turn) = log
            .begin_append(&payloads, &[], None, 1, &order)
            .await
            .expect("append");
        log.commit(&pending).await.expect("commit");
        log
    }

    async fn open(broker: &Broker, log: &StreamLog) -> StreamHandle {
        broker
            .open_stream_on_engine_log("t1", "ns", "orders", 0, log.clone())
            .await
            .expect("open")
    }

    /// **A stream on its engine's log starts at what the engine committed,
    /// not at the log's tail.** Entries past the commit index may be cut by
    /// a later leader. Counted, a reader asking for the latest would join
    /// above records that then never existed.
    #[tokio::test]
    async fn it_starts_at_the_committed_end_not_the_tail() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 5).await;
        let (broker, _) = broker(ReadBound::Committed(3)).await;
        let handle = open(&broker, &log).await;
        assert_eq!(handle.tail_seq(), 3);
        assert_eq!(log.tail_offset().await.expect("tail"), 5);
    }

    /// **Leading brings it up to the committed end, not the tail.** The
    /// hybrid's rule (the log's tail) would count a deposed leader's
    /// entries.
    #[tokio::test]
    async fn leading_moves_it_to_the_committed_end() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 5).await;
        let (broker, bound) = broker(ReadBound::Refused).await;
        let handle = open(&broker, &log).await;
        assert_eq!(handle.tail_seq(), 0, "nothing is known committed yet");

        // The engine cannot say yet: nothing moves, and the caller is told.
        let moved = broker
            .lead_from_tail("t1", "ns", "orders", 0)
            .await
            .expect("lead");
        assert!(!moved, "a stream with no bound was reported at its tail");
        assert_eq!(handle.tail_seq(), 0);
        let absent = broker
            .lead_from_tail("t1", "ns", "orders", 7)
            .await
            .expect("lead");
        assert!(!absent, "a shard not open here was reported at its tail");

        *bound.0.lock() = ReadBound::Committed(4);
        let moved = broker
            .lead_from_tail("t1", "ns", "orders", 0)
            .await
            .expect("lead");
        assert!(moved);
        assert_eq!(handle.tail_seq(), 4);
        let again = broker
            .lead_from_tail("t1", "ns", "orders", 0)
            .await
            .expect("lead");
        assert!(again, "a stream already at its tail was reported behind");
    }

    /// **Nothing but its engine writes its log.** A record the broker
    /// appended by itself would take an index the group never agreed on.
    #[tokio::test]
    async fn every_write_outside_its_engine_is_refused() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 2).await;
        let (broker, _) = broker(ReadBound::Committed(2)).await;
        broker.allow_head_trims_when(|| true);
        let handle = open(&broker, &log).await;
        let payloads = [Bytes::from_static(b"x")];
        let refused = |outcome: Result<(), BrokerError>, what: &str| match outcome {
            Err(BrokerError::NotOnEngineStream { what: got }) => assert_eq!(got, what),
            other => panic!("{what} not refused: {other:?}"),
        };

        refused(
            broker
                .publish_batch_to_handle(&handle, &payloads)
                .await
                .map(drop),
            "publishes outside its group",
        );
        refused(
            broker
                .publish_batch_at(&handle, &payloads, 2, None)
                .await
                .map(drop),
            "conditional publishes",
        );
        refused(
            broker
                .publish_batch_idempotent(&handle, 7, 0, &payloads, SequenceReuse::Refuse)
                .await
                .map(drop),
            "idempotent publishes",
        );
        assert_eq!(log.tail_offset().await.expect("tail"), 2, "nothing written");
    }

    /// **A head trim is refused too**, before the directory is marked as
    /// holding one.
    #[tokio::test]
    async fn a_head_trim_is_refused() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 2).await;
        let (broker, _) = broker(ReadBound::Committed(2)).await;
        broker.allow_head_trims_when(|| true);
        let handle = open(&broker, &log).await;
        let trimmed = broker.trim_before_at(&handle, 1, 2, Bytes::new()).await;
        assert!(
            matches!(
                trimmed,
                Err(BrokerError::NotOnEngineStream { what: "head trims" })
            ),
            "{trimmed:?}"
        );
        assert_eq!(log.tail_offset().await.expect("tail"), 2, "nothing written");
    }

    /// **What it holds survives this broker being deposed.** A batch joins
    /// the hold only after its engine committed it, so the hybrid's "drop
    /// what is held" would lose a committed record from live delivery.
    #[tokio::test]
    async fn a_deposed_leader_still_delivers_what_it_holds() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 0).await;
        let (broker, bound) = broker(ReadBound::Committed(0)).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = broker
            .subscribe("t1", "ns", "orders", 0)
            .await
            .expect("subscribe")
            .into_parts();

        broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(b"a")],
                Duration::MAX,
                |_| async {
                    Ok::<_, ProposalFailed<()>>(ProposalCommitted {
                        end: 1,
                        skipped_before: 0,
                    })
                },
            )
            .expect("handed over")
            .complete()
            .await
            .expect("committed");

        // Committed by the group, not yet applied here, and deposed.
        assert_eq!(broker.discard_uncommitted("t1", "ns", "orders", 0).await, 0);
        *bound.0.lock() = ReadBound::Committed(1);
        handle.release_committed();
        let envelope = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("delivered in time")
            .expect("subscription open");
        assert_eq!(envelope.base_offset(), Some(0));
    }

    /// **A broker that leaves durable streams to their engines opens no
    /// log of its own for one.** Opened on first use, as the hybrid does,
    /// the shard would be served from an empty log beside the group's.
    #[tokio::test]
    async fn a_shard_its_engine_has_not_opened_is_refused() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 1).await;
        let (broker, _) = broker(ReadBound::Committed(1)).await;
        let unopened = broker.resolve_stream_handle("t1", "ns", "orders", 0).await;
        assert!(
            matches!(unopened, Err(BrokerError::ShardNotOpen { shard: 0, .. })),
            "{:?}",
            unopened.err()
        );

        let handle = open(&broker, &log).await;
        let resolved = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("open now");
        assert_eq!(resolved.id(), handle.id());
        let again = broker
            .open_stream_on_engine_log("t1", "ns", "orders", 0, log.clone())
            .await;
        assert!(again.is_err(), "opened twice");
    }

    /// Append `payloads` as one publish stamped `at`, as the engine would.
    async fn append(log: &StreamLog, payloads: &[&'static str], at: u64) {
        let payloads: Vec<Bytes> = payloads
            .iter()
            .map(|p| Bytes::from_static(p.as_bytes()))
            .collect();
        let tail = log.tail_offset().await.expect("tail");
        let order = Arc::new(CommitSequencer::new(tail));
        let (pending, _turn) = log
            .begin_append(&payloads, &[], None, at, &order)
            .await
            .expect("append");
        log.commit(&pending).await.expect("commit");
    }

    /// The next batch a subscriber is handed: its first offset, how many
    /// offsets before it hold no record, and its payloads.
    async fn batch(receiver: &mut crate::SubscriptionReceiver) -> (u64, u64, Vec<Bytes>) {
        let envelope = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("delivered in time")
            .expect("subscription open");
        (
            envelope.base_offset().expect("an offset"),
            envelope.skipped_before(),
            envelope.payloads().to_vec(),
        )
    }

    /// Nothing more reaches `receiver`, and it is still open.
    async fn quiet(receiver: &mut crate::SubscriptionReceiver) {
        let more = tokio::time::timeout(Duration::from_millis(200), receiver.recv()).await;
        assert!(
            more.is_err(),
            "delivered or ended: {:?}",
            more.map(|m| m.is_some())
        );
    }

    fn payloads(of: &[&'static str]) -> Vec<Bytes> {
        of.iter()
            .map(|p| Bytes::from_static(p.as_bytes()))
            .collect()
    }

    /// **A broker that does not lead delivers its log's records to its
    /// subscribers up to what the engine committed, and no further.** An
    /// entry above the bound may be cut by a later leader, and a record
    /// delivered cannot be taken back.
    #[tokio::test]
    async fn a_follower_delivers_what_is_committed_and_no_further() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 5).await;
        let (broker, bound) = broker(ReadBound::Refused).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = {
            let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
            subscription.expect("subscribe").into_parts()
        };

        // The engine cannot say what is committed: nothing is read.
        let waiting = handle.deliver_from_log().await.expect("pass");
        assert_eq!(waiting, LogDelivery::Waiting);
        quiet(&mut receiver).await;

        *bound.0.lock() = ReadBound::Committed(3);
        let delivered = handle.deliver_from_log().await.expect("pass");
        assert_eq!(delivered, LogDelivery::CaughtUp);
        assert_eq!(
            batch(&mut receiver).await,
            (0, 0, payloads(&["0", "1", "2"]))
        );
        quiet(&mut receiver).await;
        assert_eq!(handle.tail_seq(), 3);

        *bound.0.lock() = ReadBound::Committed(5);
        handle.deliver_from_log().await.expect("pass");
        assert_eq!(batch(&mut receiver).await, (3, 0, payloads(&["3", "4"])));
        // A bound past the log (the member is still writing) reads to the tail.
        *bound.0.lock() = ReadBound::Committed(9);
        handle.deliver_from_log().await.expect("pass");
        quiet(&mut receiver).await;
        assert_eq!(handle.tail_seq(), 5);
    }

    /// **A follower's batches are the leader's**: records one publish wrote
    /// go out together, and an offset that holds no record is reported as
    /// skipped before the batch after it, so a subscriber sees no drop.
    #[tokio::test]
    async fn a_follower_reports_offsets_with_no_record_as_skipped() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 0).await;
        let (broker, bound) = broker(ReadBound::Committed(0)).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = {
            let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
            subscription.expect("subscribe").into_parts()
        };
        append(&log, &["a"], 1).await;
        assert_eq!(log.append_generation_start(2).await.expect("marker"), 1);
        append(&log, &["b", "c"], 2).await;
        append(&log, &["d"], 3).await;

        *bound.0.lock() = ReadBound::Committed(5);
        handle.deliver_from_log().await.expect("pass");

        assert_eq!(batch(&mut receiver).await, (0, 0, payloads(&["a"])));
        assert_eq!(batch(&mut receiver).await, (2, 1, payloads(&["b", "c"])));
        assert_eq!(batch(&mut receiver).await, (4, 0, payloads(&["d"])));
        assert_eq!(handle.tail_seq(), 5);
    }

    /// **No pass runs while a batch this broker was handed is unsettled.**
    /// The batch delivers its own records when it is released. Read from
    /// the log under it, they would reach a subscriber twice.
    #[tokio::test]
    async fn a_follower_waits_for_a_handed_over_batch_to_settle() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 1).await;
        let (broker, bound) = broker(ReadBound::Committed(0)).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = {
            let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
            subscription.expect("subscribe").into_parts()
        };
        let (commit, committed) = tokio::sync::oneshot::channel::<()>();
        let proposed = broker
            .propose_publish(
                &handle,
                vec![Bytes::from_static(b"0")],
                Duration::MAX,
                |_| async {
                    let _ = committed.await;
                    Ok::<_, ProposalFailed<()>>(ProposalCommitted {
                        end: 1,
                        skipped_before: 0,
                    })
                },
            )
            .expect("handed over");
        let completing = proposed.complete();
        tokio::pin!(completing);
        let pending = tokio::time::timeout(Duration::from_millis(50), &mut completing).await;
        assert!(pending.is_err(), "completed before its engine committed");

        // Deposed, and the member applied the record.
        *bound.0.lock() = ReadBound::Committed(1);
        let waiting = handle.deliver_from_log().await.expect("pass");
        assert_eq!(waiting, LogDelivery::Waiting);

        commit.send(()).expect("commit");
        completing.await.expect("committed");
        handle.release_committed();
        assert_eq!(batch(&mut receiver).await, (0, 0, payloads(&["0"])));
        let caught_up = handle.deliver_from_log().await.expect("pass");
        assert_eq!(caught_up, LogDelivery::CaughtUp);
        quiet(&mut receiver).await;
    }

    /// **A broker that starts leading finishes the log first and keeps the
    /// subscribers it served as a follower**, then delivers from its
    /// publishers' batches only. The offsets it passes hold no record, so
    /// nobody is skipped.
    #[tokio::test]
    async fn leading_finishes_the_log_and_keeps_its_subscribers() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 2).await;
        assert_eq!(log.append_generation_start(2).await.expect("marker"), 2);
        let (broker, bound) = broker(ReadBound::Committed(0)).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = {
            let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
            subscription.expect("subscribe").into_parts()
        };

        *bound.0.lock() = ReadBound::Committed(3);
        let led = broker.lead_from_tail("t1", "ns", "orders", 0).await;
        assert!(led.expect("lead"));

        assert_eq!(batch(&mut receiver).await, (0, 0, payloads(&["0", "1"])));
        assert_eq!(handle.tail_seq(), 3);
        // What commits from now on comes from this broker's own publishes.
        append(&log, &["x"], 9).await;
        *bound.0.lock() = ReadBound::Committed(4);
        let pass = handle.deliver_from_log().await.expect("pass");
        assert_eq!(pass, LogDelivery::CaughtUp);
        quiet(&mut receiver).await;
        assert_eq!(handle.tail_seq(), 3);
    }

    /// **A stream whose term ended while it moved to the tail is not marked
    /// as led.** Marked, it would wait for publishes that go to another
    /// broker, and its subscribers would get nothing.
    #[tokio::test]
    async fn a_term_that_ended_is_not_marked_as_led() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 2).await;
        let (broker, bound) = broker(ReadBound::Committed(1)).await;
        let handle = open(&broker, &log).await;
        let (mut receiver, _guard) = {
            let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
            subscription.expect("subscribe").into_parts()
        };

        let led = broker
            .lead_from_tail_if("t1", "ns", "orders", 0, || false)
            .await;
        assert!(!led.expect("lead"));

        *bound.0.lock() = ReadBound::Committed(2);
        handle.deliver_from_log().await.expect("pass");
        assert_eq!(batch(&mut receiver).await, (1, 0, payloads(&["1"])));
    }

    /// **A follower far behind skips to the committed end only when nobody
    /// is subscribed.** The ring caches the tail, so a restart does not
    /// read the whole log through it. A subscriber gets every record.
    #[tokio::test]
    async fn a_follower_far_behind_skips_ahead_only_with_no_subscriber() {
        let dir = tempfile::tempdir().expect("dir");
        let log = engine_log(dir.path(), 10).await;
        let (broker, bound) = broker_with(ReadBound::Committed(0), Some(4)).await;
        let handle = open(&broker, &log).await;
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        let (mut receiver, guard) = subscription.expect("subscribe").into_parts();

        *bound.0.lock() = ReadBound::Committed(6);
        handle.deliver_from_log().await.expect("pass");
        let (first, skipped, got) = batch(&mut receiver).await;
        assert_eq!((first, skipped, got.len()), (0, 0, 6));

        drop(guard);
        drop(receiver);
        *bound.0.lock() = ReadBound::Committed(10);
        handle.deliver_from_log().await.expect("pass");
        assert_eq!(handle.tail_seq(), 10);
    }
}
