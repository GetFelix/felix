//! Broker-level behaviour of `durable: true`.
//!
//! The properties under test are the ones the guarantee actually rests on:
//! a durable publish is on disk before it is delivered, a storage failure is
//! never acknowledged as success, a restart brings the records back, and a
//! non-durable stream is untouched by any of it.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{
    Broker, BrokerError, DurableStorage, ResumedSubscription, StartPosition, StreamMetadata,
};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

fn log_config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        // Small segments so rollover is covered without writing megabytes.
        segment_size_bytes: 4 * 1024,
        index_spacing_bytes: 256,
        fsync_mode,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker_with_storage(
    dir: &TempDir,
    fsync_mode: FsyncMode,
) -> (Arc<Broker>, DurableStorage) {
    let storage = DurableStorage::open(dir.path(), log_config(fsync_mode)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    (Arc::new(broker), storage)
}

async fn register(broker: &Broker, stream: &str, durable: bool) {
    broker
        .register_stream(
            "t1",
            "default",
            stream,
            StreamMetadata {
                durable,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("register");
}

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

#[tokio::test]
async fn a_durable_publish_is_on_disk_before_it_is_acknowledged() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    broker
        .publish("t1", "default", "orders", payload("order-1"))
        .await
        .expect("publish");

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream log");
    // `OnCommit` means the device flush happened inside `publish`.
    assert_eq!(log.unsynced_bytes(), 0);
    assert_eq!(log.durable_offset(), 1);

    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload("order-1"));
}

#[tokio::test]
async fn durable_records_survive_a_broker_restart() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;
        for i in 0..50 {
            broker
                .publish("t1", "default", "orders", payload(&format!("order-{i:03}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    // A completely new broker over the same directory: no manual recreation of
    // log files, just the same stream registration the control plane replays.
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("stream log");
    assert_eq!(log.tail_offset().await.expect("tail"), 50);
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 50);
    assert_eq!(records[0].payload, payload("order-000"));
    assert_eq!(records[49].payload, payload("order-049"));

    // And the recovered stream keeps accepting publishes at the right offset.
    broker
        .publish("t1", "default", "orders", payload("after-restart"))
        .await
        .expect("publish");
    let records = log.read_from(50, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload("after-restart"));
}

#[tokio::test]
async fn a_cursor_taken_after_restart_reflects_the_recovered_tail() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;
        for i in 0..5 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    // The in-memory cursor resumes from the durable tail rather than restarting
    // at zero, so a cursor a client saved before the restart still means the
    // same position.
    let cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    assert_eq!(cursor.next_seq(), 5);
}

#[tokio::test]
async fn durable_publishes_are_delivered_to_subscribers_too() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", payload("delivered"))
        .await
        .expect("publish");

    let received = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(received, payload("delivered"));
}

#[tokio::test]
async fn a_non_durable_stream_writes_nothing_to_disk() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "ephemeral", false).await;

    let mut sub = broker
        .subscribe("t1", "default", "ephemeral", 0)
        .await
        .expect("subscribe");
    for i in 0..10 {
        broker
            .publish("t1", "default", "ephemeral", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    // Delivery still works...
    let received = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(received, payload("v0"));

    // ...and nothing reached durable storage: no shard directory was created,
    // so the low-latency in-memory path was genuinely untouched.
    assert!(storage.root().exists());
    let shard_dirs = std::fs::read_dir(storage.root())
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .count();
    assert_eq!(shard_dirs, 0, "a non-durable stream created storage");
}

#[tokio::test]
async fn durable_and_non_durable_streams_coexist() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;
    register(&broker, "ephemeral", false).await;

    // Cursors taken before publishing, so the backlog replay below has
    // something to return for each stream.
    let durable_cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    let ephemeral_cursor = broker
        .cursor_tail("t1", "default", "ephemeral", 0)
        .await
        .expect("cursor");

    broker
        .publish("t1", "default", "orders", payload("kept"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "ephemeral", payload("lost"))
        .await
        .expect("publish");

    // Only one of them reached disk...
    let durable = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    assert_eq!(durable.tail_offset().await.expect("tail"), 1);

    // ...but in-memory replay behaves identically for both, which is what keeps
    // the non-durable path unchanged.
    let (durable_backlog, _) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, durable_cursor)
        .await
        .expect("subscribe");
    assert_eq!(durable_backlog, vec![payload("kept")]);

    let (ephemeral_backlog, _) = broker
        .subscribe_with_cursor("t1", "default", "ephemeral", 0, ephemeral_cursor)
        .await
        .expect("subscribe");
    assert_eq!(ephemeral_backlog, vec![payload("lost")]);
}

#[tokio::test]
async fn upgrading_a_live_stream_to_durable_requires_recreation() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", false).await;

    broker
        .publish("t1", "default", "orders", payload("before"))
        .await
        .expect("publish");
    let err = broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect_err("live durability change");
    assert!(matches!(
        err,
        BrokerError::DurabilityChangeRequiresRecreate {
            current: false,
            requested: true,
            ..
        }
    ));

    broker
        .publish("t1", "default", "orders", payload("after"))
        .await
        .expect("stream remains ephemeral");
    assert_eq!(
        std::fs::read_dir(storage.root())
            .expect("read root")
            .filter_map(|entry| entry.ok())
            .count(),
        0,
        "a rejected upgrade must not create durable state"
    );
}

#[tokio::test]
async fn downgrading_a_live_durable_stream_requires_recreation() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;
    broker
        .publish("t1", "default", "orders", payload("before"))
        .await
        .expect("publish");

    let err = broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: false,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect_err("live durability change");
    assert!(matches!(
        err,
        BrokerError::DurabilityChangeRequiresRecreate {
            current: true,
            requested: false,
            ..
        }
    ));

    broker
        .publish("t1", "default", "orders", payload("after"))
        .await
        .expect("stream remains durable");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(
        records
            .into_iter()
            .map(|record| record.payload)
            .collect::<Vec<_>>(),
        vec![payload("before"), payload("after")]
    );
}

#[tokio::test]
async fn registering_a_durable_stream_without_storage_is_rejected() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");

    let err = broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect_err("no storage configured");
    // A distinct variant from `Storage`, so the control-plane watcher can skip
    // a stream this broker will never be able to host while still failing hard
    // on a real storage failure.
    assert!(
        matches!(err, BrokerError::DurableStorageNotConfigured { .. }),
        "{err}"
    );

    // The stream must not exist: a half-registered durable stream would accept
    // publishes it cannot persist.
    assert!(!broker.stream_exists("t1", "default", "orders").await);

    // A non-durable stream on the same broker is unaffected.
    broker
        .register_stream("t1", "default", "ephemeral", StreamMetadata::default())
        .await
        .expect("register");
    broker
        .publish("t1", "default", "ephemeral", payload("fine"))
        .await
        .expect("publish");
}

#[tokio::test]
async fn periodic_mode_persists_without_blocking_each_publish() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(
        &dir,
        FsyncMode::Periodic {
            interval: Duration::from_millis(20),
        },
    )
    .await;
    register(&broker, "orders", true).await;

    for i in 0..20 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    // The records are readable straight away — they are in the page cache — and
    // the background syncer pushes them to the device within its window.
    assert_eq!(log.read_from(0, usize::MAX).await.expect("read").len(), 20);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while log.durable_offset() < 20 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(log.durable_offset(), 20);

    storage.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_durable_publishes_are_all_persisted_exactly_once() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:03}")))
                .await
                .expect("publish");
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    assert_eq!(log.tail_offset().await.expect("tail"), 64);
    let mut values: Vec<String> = log
        .read_from(0, usize::MAX)
        .await
        .expect("read")
        .into_iter()
        .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(values.len(), 64);
    values.sort();
    values.dedup();
    assert_eq!(values.len(), 64, "duplicate or lost records");
}

#[tokio::test]
async fn a_batch_publish_lands_as_one_contiguous_run() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let batch: Vec<Bytes> = (0..8).map(|i| payload(&format!("b{i}"))).collect();
    broker
        .publish_batch("t1", "default", "orders", 0, &batch)
        .await
        .expect("publish");

    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    let records = log.read_from(0, usize::MAX).await.expect("read");
    assert_eq!(records.len(), 8);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as u64);
        assert_eq!(record.payload, batch[index]);
    }
}

/// End-to-end check that the three orders agree.
///
/// This is a smoke test, not a regression test: it passes with the commit
/// sequencer removed, because the reordering window needs a scheduler
/// interleaving that 32 publishers on a 4-worker runtime do not reliably
/// produce. The deterministic proof that turns are granted in offset order
/// regardless of arrival order lives in `commit_order`'s unit tests, which fail
/// immediately if the primitive stops ordering. What this test does buy is
/// coverage of the wiring - that the sequencer is actually on the publish path
/// and that holding a turn across fanout does not deadlock or drop records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_order_cursor_order_and_delivery_order_agree_under_concurrency() {
    let dir = tempdir().expect("dir");
    let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");

    const PUBLISHERS: u32 = 32;
    let mut tasks = Vec::new();
    for i in 0..PUBLISHERS {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:03}")))
                .await
                .expect("publish");
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }

    // 1. The order on disk.
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("log");
    let on_disk: Vec<String> = log
        .read_from(0, usize::MAX)
        .await
        .expect("read")
        .into_iter()
        .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(on_disk.len(), PUBLISHERS as usize);

    // 2. The order a subscriber received them in.
    let mut delivered = Vec::new();
    while delivered.len() < PUBLISHERS as usize {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("delivery timeout")
            .expect("message");
        delivered.push(String::from_utf8(msg.to_vec()).expect("utf8"));
    }

    // 3. The order cursor replay returns them in.
    let (replayed, _) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, cursor)
        .await
        .expect("replay");
    let replayed: Vec<String> = replayed
        .into_iter()
        .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
        .collect();

    // All three must be the same sequence.
    assert_eq!(
        delivered, on_disk,
        "delivery order disagrees with disk order"
    );
    assert_eq!(replayed, on_disk, "cursor replay disagrees with disk order");

    storage.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_pre_restart_cursor_still_replays_after_a_restart() {
    let dir = tempdir().expect("dir");
    let saved_cursor;
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;

        // A client subscribes, reads a few records, and remembers where it got
        // to — the ordinary resume pattern.
        saved_cursor = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await
            .expect("cursor");
        for i in 0..20 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    // The broker restarts. Before the replay ring was hydrated from disk this
    // returned CursorTooOld: the ring was empty, so its "oldest" was the
    // recovered tail and every saved cursor looked ancient.
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    let (backlog, mut sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, saved_cursor)
        .await
        .expect("replay after restart");
    let replayed: Vec<String> = backlog
        .into_iter()
        .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(replayed.len(), 20, "pre-restart history was not replayed");
    assert_eq!(replayed[0], "v00");
    assert_eq!(replayed[19], "v19");

    // And the subscription is live from the point the backlog ended, with no
    // gap and no duplicate across the seam.
    broker
        .publish("t1", "default", "orders", payload("after"))
        .await
        .expect("publish");
    let next = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(next, payload("after"));
}

#[tokio::test]
async fn hydration_is_bounded_by_the_replay_ring_capacity() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::None).await;
        register(&broker, "orders", true).await;
        for i in 0..300 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:04}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    // A small ring: hydration must fill it with the *tail* of the log and stop,
    // not pull the whole history into memory.
    let storage = DurableStorage::open(dir.path(), log_config(FsyncMode::None)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage)
        .with_topic_capacity(16)
        .expect("capacity")
        .with_log_capacity(16)
        .expect("log capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    register(&broker, "orders", true).await;

    // Cursors inside the ring window resolve from memory...
    let (backlog, _sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, {
            let tail = broker
                .cursor_tail("t1", "default", "orders", 0)
                .await
                .expect("cursor");
            assert_eq!(tail.next_seq(), 300, "cursor did not resume at the tail");
            tail
        })
        .await
        .expect("subscribe at tail");
    assert!(backlog.is_empty(), "a tail cursor should have no backlog");

    // ...and everything older is reachable through the paged replay API rather
    // than by growing the ring.
    let mut seen = Vec::new();
    let mut offset = 0u64;
    loop {
        let page = broker
            .read_durable("t1", "default", "orders", 0, offset, 4096)
            .await
            .expect("read_durable");
        if page.is_empty() {
            break;
        }
        offset = page.last().expect("last").offset + 1;
        seen.extend(
            page.into_iter()
                .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8")),
        );
    }
    assert_eq!(seen.len(), 300, "paged replay did not cover the whole log");
    assert_eq!(seen[0], "v0000");
    assert_eq!(seen[299], "v0299");
}

#[tokio::test]
async fn historical_replay_is_rejected_for_a_non_durable_stream() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::None).await;
    register(&broker, "ephemeral", false).await;

    let err = broker
        .read_durable("t1", "default", "ephemeral", 0, 0, 4096)
        .await
        .expect_err("no persisted history");
    assert!(matches!(err, BrokerError::StreamNotDurable { .. }), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_publish_does_not_strand_the_stream() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;

    broker
        .publish("t1", "default", "orders", payload("first"))
        .await
        .expect("publish");

    // Cancel a publish mid-flight. Offsets are assigned synchronously under the
    // segment lock and the fsync wait happens after, so a timeout landing in
    // that window drops the future *after* its offsets were consumed.
    for _ in 0..10 {
        let _ = tokio::time::timeout(
            Duration::from_micros(200),
            broker.publish("t1", "default", "orders", payload("cancelled")),
        )
        .await;
    }

    // The stream must still accept publishes. If a consumed offset range can be
    // abandoned without releasing its place in the commit order, every later
    // publish waits for a turn that never comes.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        broker.publish("t1", "default", "orders", payload("after")),
    )
    .await;
    assert!(
        result.is_ok(),
        "stream deadlocked: a cancelled publish stranded the commit order"
    );
    result.expect("timeout").expect("publish");
}

#[tokio::test]
async fn a_failed_hydration_leaves_no_registered_stream() {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(dir.path(), log_config(FsyncMode::None)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");

    // Block the shard directory with a file so opening the log fails. The
    // registration must fail *whole*: a stream that could not be initialised
    // must not be left publishable, accepting writes it cannot persist or
    // replay.
    let shard_dir = felix_storage::disk_log::layout::shard_dir(
        dir.path(),
        &felix_storage::log::ShardKey {
            tenant: "t1".into(),
            namespace: "default".into(),
            stream: "orders".into(),
            shard: 0,
        },
    );
    std::fs::write(&shard_dir, b"not a directory").expect("block the shard dir");

    let err = broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect_err("registration should fail");
    assert!(matches!(err, BrokerError::Storage(_)), "{err}");

    // Neither registry may hold it, and publishing must report the stream as
    // absent rather than silently succeeding into memory.
    assert!(!broker.stream_exists("t1", "default", "orders").await);
    let publish = broker
        .publish("t1", "default", "orders", payload("x"))
        .await
        .expect_err("must not be publishable");
    assert!(
        matches!(publish, BrokerError::StreamNotFound { .. }),
        "{publish}"
    );
}

#[tokio::test]
async fn hydration_never_leaves_a_gap_between_the_ring_and_the_tail() {
    let dir = tempdir().expect("dir");
    // A per-read record cap below the ring's capacity, so the window hydration
    // wants cannot be satisfied by one read. The same shape occurs in
    // production whenever the ring's worth of tail exceeds the byte budget.
    let config = LogConfig {
        max_records_per_read: 100,
        ..log_config(FsyncMode::None)
    };

    async fn boot(dir: &TempDir, config: LogConfig) -> (Broker, DurableStorage) {
        let storage = DurableStorage::open(dir.path(), config).expect("storage");
        let broker = Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage.clone())
            .with_log_capacity(500)
            .expect("capacity");
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        register(&broker, "orders", true).await;
        (broker, storage)
    }

    // A client reads up to offset 550, saves its cursor, then 50 more arrive.
    let saved_cursor;
    {
        let (broker, storage) = boot(&dir, config.clone()).await;
        for i in 0..550 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:04}")))
                .await
                .expect("publish");
        }
        saved_cursor = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await
            .expect("cursor");
        assert_eq!(saved_cursor.next_seq(), 550);
        for i in 550..600 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i:04}")))
                .await
                .expect("publish");
        }
        storage.shutdown().await.expect("shutdown");
    }

    // Restart. Hydration wants offsets 100..600 but one read returns only 100
    // records. If it keeps that earliest prefix and still advances next_seq to
    // 600, the ring holds 100..200 with a hole up to the tail — and a cursor at
    // 550 is accepted (550 >= 100) and answered with silence.
    let (broker, _storage) = boot(&dir, config).await;
    let (backlog, _sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, saved_cursor)
        .await
        .expect("replay from a saved cursor");

    assert_eq!(
        backlog.len(),
        50,
        "cursor at 550 replayed {} records instead of 50 — an empty or short \
         backlog here reads as \"you are caught up\" while records are missing",
        backlog.len()
    );
    assert_eq!(backlog[0], payload("v0550"));
    assert_eq!(backlog[49], payload("v0599"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_publish_does_not_shift_cursor_identity() {
    let dir = tempdir().expect("dir");
    let saved_cursor;
    let before_restart;
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register(&broker, "orders", true).await;

        // Cancel publishes mid-fsync so their disk offsets are consumed without
        // ever reaching the replay ring.
        for _ in 0..12 {
            let _ = tokio::time::timeout(
                Duration::from_micros(200),
                broker.publish("t1", "default", "orders", payload("cancelled")),
            )
            .await;
        }

        // A client takes its cursor after that, then reads five records.
        saved_cursor = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await
            .expect("cursor");
        for i in 0..5 {
            broker
                .publish("t1", "default", "orders", payload(&format!("kept-{i}")))
                .await
                .expect("publish");
        }

        let (backlog, _) = broker
            .subscribe_with_cursor("t1", "default", "orders", 0, saved_cursor)
            .await
            .expect("replay");
        before_restart = backlog
            .into_iter()
            .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
            .collect::<Vec<_>>();
        assert_eq!(before_restart.len(), 5, "the five kept records must replay");
        storage.shutdown().await.expect("shutdown");
    }

    // The same cursor after a restart, where hydration rebuilds sequences from
    // disk offsets. If the in-memory sequence had been assigned from its own
    // counter, a consumed-but-unapplied offset would have shifted every later
    // record's cursor by one, and this cursor would name different records
    // either side of the restart.
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;
    let (backlog, _) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, saved_cursor)
        .await
        .expect("replay after restart");
    let after_restart: Vec<String> = backlog
        .into_iter()
        .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
        .collect();

    assert_eq!(
        before_restart, after_restart,
        "the same cursor named different records before and after a restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backlog_to_live_handoff_loses_nothing_under_concurrent_publishes() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::None).await;
    register(&broker, "orders", true).await;

    // A publisher running flat out while a subscriber joins. The handoff is the
    // moment a record can fall between a backlog captured too early and a
    // subscriber registered too late.
    const TOTAL: u32 = 400;
    let publisher = {
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            for i in 0..TOTAL {
                broker
                    .publish("t1", "default", "orders", payload(&format!("v{i:04}")))
                    .await
                    .expect("publish");
            }
        })
    };

    // Join mid-flight.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let cursor = broker
        .cursor_tail("t1", "default", "orders", 0)
        .await
        .expect("cursor");
    let (backlog, mut sub) = broker
        .subscribe_with_cursor("t1", "default", "orders", 0, cursor)
        .await
        .expect("subscribe");
    publisher.await.expect("join");

    // Everything from the cursor onwards must arrive, once, in order, across
    // the backlog/live seam.
    let mut seen: Vec<String> = backlog
        .into_iter()
        .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
        .collect();
    let expected_from = cursor.next_seq() as u32;
    // A timeout used to `break` and let the length assertion below report it as
    // a lost record, which sends anyone reading the failure after the handoff
    // logic instead of at the clock. On a machine busy enough -- a full test
    // suite, or a benchmark run alongside it -- waiting is not the same fact as
    // losing, so the two now fail differently.
    let mut timed_out = false;
    while (seen.len() as u32) < TOTAL - expected_from {
        match tokio::time::timeout(Duration::from_secs(30), sub.recv()).await {
            Ok(Some(msg)) => seen.push(String::from_utf8(msg.to_vec()).expect("utf8")),
            Ok(None) => break,
            Err(_) => {
                timed_out = true;
                break;
            }
        }
    }
    assert!(
        !timed_out,
        "timed out after {} of {} records -- the machine was too slow to \
         conclude anything about the handoff",
        seen.len(),
        TOTAL - expected_from,
    );

    let expected: Vec<String> = (expected_from..TOTAL).map(|i| format!("v{i:04}")).collect();
    assert_eq!(
        seen, expected,
        "a record fell between the backlog snapshot and the live subscription"
    );
}

// ---------------------------------------------------------------------------
// Resuming from a position: the seam between stored history and live delivery.
// ---------------------------------------------------------------------------

/// A broker whose replay ring is deliberately tiny, so history has to come off
/// disk rather than out of memory.
async fn broker_with_small_ring(dir: &TempDir, ring: usize) -> Arc<Broker> {
    let storage = DurableStorage::open(dir.path(), log_config(FsyncMode::None)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage)
        .with_log_capacity(ring)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    Arc::new(broker)
}

/// Drain everything a resume produced: disk history, then ring backlog, then
/// whatever is already sitting on the live receiver.
async fn drain_resume(
    broker: &Broker,
    stream: &str,
    resumed: &mut ResumedSubscription,
) -> Vec<String> {
    let mut seen = Vec::new();
    if let Some(range) = resumed.history {
        let mut at = range.from_offset;
        while at < range.until_offset {
            let records = broker
                .read_durable("t1", "default", stream, 0, at, 64 * 1024)
                .await
                .expect("history");
            if records.is_empty() {
                break;
            }
            for record in records {
                if record.offset >= range.until_offset {
                    break;
                }
                seen.push(String::from_utf8(record.payload.to_vec()).expect("utf8"));
                at = record.offset + 1;
            }
        }
    }
    for (_, payload) in &resumed.backlog {
        seen.push(String::from_utf8(payload.to_vec()).expect("utf8"));
    }
    seen
}

#[tokio::test]
async fn resuming_at_an_offset_older_than_the_ring_replays_from_disk() {
    let dir = tempdir().expect("dir");
    // Ring holds 4; publish 20. Everything below offset 16 exists only on disk.
    let broker = broker_with_small_ring(&dir, 4).await;
    register(&broker, "orders", true).await;
    for i in 0..20 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(3))
        .await
        .expect("resume");

    // The request is older than the ring, so a disk range must have been
    // produced -- and it must stop exactly where the backlog starts.
    let range = resumed.history.expect("history below the ring");
    assert_eq!(range.from_offset, 3);
    assert_eq!(range.until_offset, resumed.backlog_start);

    let seen = drain_resume(&broker, "orders", &mut resumed).await;
    let expected: Vec<String> = (3..20).map(|i| format!("v{i:02}")).collect();
    assert_eq!(seen, expected, "history and backlog must join with no gap");
}

#[tokio::test]
async fn a_publish_during_the_resume_arrives_live_exactly_once() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 4).await;
    register(&broker, "orders", true).await;
    for i in 0..20 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(3))
        .await
        .expect("resume");

    // Published *after* registration but *before* the history is read. This is
    // the record that a read-then-subscribe ordering loses: it is too new for
    // the disk range and would have missed a later registration.
    broker
        .publish("t1", "default", "orders", payload("during"))
        .await
        .expect("publish");

    let mut seen = drain_resume(&broker, "orders", &mut resumed).await;
    while let Ok(Some(delivery)) =
        tokio::time::timeout(Duration::from_millis(200), resumed.subscription.recv()).await
    {
        seen.push(String::from_utf8(delivery.to_vec()).expect("utf8"));
    }

    let mut expected: Vec<String> = (3..20).map(|i| format!("v{i:02}")).collect();
    expected.push("during".to_string());
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn resuming_within_the_ring_needs_no_disk_history() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 16).await;
    register(&broker, "orders", true).await;
    for i in 0..8 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(5))
        .await
        .expect("resume");
    assert!(resumed.history.is_none(), "the ring covered the request");
    assert_eq!(resumed.backlog_start, 5);
    assert_eq!(
        drain_resume(&broker, "orders", &mut resumed).await,
        ["v5", "v6", "v7"]
    );
}

#[tokio::test]
async fn latest_delivers_nothing_already_published() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 16).await;
    register(&broker, "orders", true).await;
    for i in 0..5 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Latest)
        .await
        .expect("resume");
    assert!(resumed.history.is_none());
    assert!(
        drain_resume(&broker, "orders", &mut resumed)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn earliest_replays_the_whole_retained_stream() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 4).await;
    register(&broker, "orders", true).await;
    for i in 0..12 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Earliest)
        .await
        .expect("resume");
    let seen = drain_resume(&broker, "orders", &mut resumed).await;
    let expected: Vec<String> = (0..12).map(|i| format!("v{i:02}")).collect();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn an_in_memory_stream_reports_a_cursor_older_than_its_ring() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 4).await;
    // Not durable: there is no disk to fall back to, so a request below the
    // ring is unservable rather than slow.
    register(&broker, "ephemeral", false).await;
    for i in 0..20 {
        broker
            .publish("t1", "default", "ephemeral", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    let err = broker
        .subscribe_from("t1", "default", "ephemeral", 0, StartPosition::Offset(2))
        .await
        .expect_err("cursor is below the ring and there is no disk");
    assert!(
        matches!(err, BrokerError::CursorTooOld { requested: 2, .. }),
        "unexpected error: {err}",
    );
}

#[tokio::test]
async fn a_resume_past_the_tail_is_rejected_not_reinterpreted() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 16).await;
    register(&broker, "orders", true).await;
    for i in 0..5 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    // Tail is 5. Asking for 99 previously registered for live delivery and then
    // handed over records at offsets 5, 6, 7... -- below the requested position,
    // which is the opposite of what was asked for.
    let err = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(99))
        .await
        .expect_err("99 is past the tail");
    assert!(
        matches!(
            err,
            BrokerError::CursorInFuture {
                requested: 99,
                tail: 5
            }
        ),
        "unexpected error: {err}",
    );
}

#[tokio::test]
async fn a_rejected_resume_leaves_no_subscriber_behind() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 4).await;
    register(&broker, "ephemeral", false).await;
    for i in 0..20 {
        broker
            .publish("t1", "default", "ephemeral", payload(&format!("v{i:02}")))
            .await
            .expect("publish");
    }

    // Registration happens before the too-old check can run, so each rejection
    // used to strand a closed sender in the registry -- reaped only when some
    // later publish happened to notice. Repeated rejected subscribes could grow
    // the slab without ever touching the per-connection subscription cap.
    for _ in 0..50 {
        let err = broker
            .subscribe_from("t1", "default", "ephemeral", 0, StartPosition::Offset(1))
            .await
            .expect_err("below the ring, and no disk to fall back to");
        assert!(matches!(err, BrokerError::CursorTooOld { .. }));
    }

    assert_eq!(
        broker
            .registered_subscribers("t1", "default", "ephemeral", 0)
            .await
            .expect("count"),
        0,
        "the 50 rejected subscribes must not have left registrations behind",
    );
}

#[tokio::test]
async fn backlog_entries_carry_their_own_offsets() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 16).await;
    register(&broker, "orders", true).await;
    for i in 0..6 {
        broker
            .publish("t1", "default", "orders", payload(&format!("v{i}")))
            .await
            .expect("publish");
    }

    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(2))
        .await
        .expect("resume");

    // Offsets travel with the payloads rather than being derived by index from
    // `backlog_start`. The ring is not guaranteed contiguous -- a publish that
    // consumed disk offsets and was cancelled before reaching the ring leaves a
    // hole -- and numbering by position mislabels everything after one.
    let offsets: Vec<u64> = resumed.backlog.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(offsets, vec![2, 3, 4, 5]);
    assert_eq!(resumed.backlog_start, 2);
    for (offset, payload) in &resumed.backlog {
        let expected = format!("v{offset}");
        assert_eq!(String::from_utf8(payload.to_vec()).expect("utf8"), expected);
    }
}

// --- Retention -------------------------------------------------------------
//
// Retention is what makes `CursorTooOld` reachable on a durable stream. Before
// it existed, `base_offset` never moved, so every durable resume succeeded and
// the error path below could not be exercised at all.

fn retention_log_config(retention_bytes: u64) -> LogConfig {
    LogConfig {
        retention_bytes: Some(retention_bytes),
        // The tests drive retention explicitly; the timer must not race them.
        retention_check_interval: Duration::from_secs(3600),
        ..log_config(FsyncMode::None)
    }
}

/// Publish past the in-memory replay ring, then trim the disk behind it.
///
/// Both halves matter. The ring (1024 entries) answers a resume without
/// touching disk, so a trim is only *observable* to a subscriber once the ring
/// has evicted the same records — until then the data is genuinely still
/// available and reporting `CursorTooOld` would be a lie.
async fn publish_and_trim(broker: &Broker, storage: &DurableStorage, stream: &str) -> u64 {
    for i in 0..1600 {
        broker
            .publish("t1", "default", stream, payload(&format!("v{i:04}")))
            .await
            .expect("publish");
    }
    let log = storage
        .open_stream("t1", "default", stream, 0)
        .expect("stream log");
    let deleted = log.enforce_retention_now().await.expect("retention");
    assert!(deleted > 0, "expected retention to delete a segment");
    log.base_offset()
}

#[tokio::test]
async fn resuming_below_the_retained_range_reports_cursor_too_old() {
    let dir = tempdir().expect("dir");
    let storage =
        DurableStorage::open(dir.path(), retention_log_config(8 * 1024)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    register(&broker, "orders", true).await;

    let base = publish_and_trim(&broker, &storage, "orders").await;
    assert!(base > 0, "retention must have advanced the base offset");

    // Offset 0 existed and is gone. That is a different answer from "nothing
    // there yet", and it is the one a resuming client needs.
    let err = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect_err("offset 0 has been trimmed");
    match err {
        BrokerError::CursorTooOld { oldest, requested } => {
            assert_eq!(requested, 0);
            assert_eq!(oldest, base);
        }
        other => panic!("expected CursorTooOld, got {other:?}"),
    }
}

#[tokio::test]
async fn earliest_resumes_from_the_oldest_retained_record() {
    let dir = tempdir().expect("dir");
    let storage =
        DurableStorage::open(dir.path(), retention_log_config(8 * 1024)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    register(&broker, "orders", true).await;

    let base = publish_and_trim(&broker, &storage, "orders").await;

    // `earliest` must mean "oldest still retained", not "offset 0" -- otherwise
    // it is an error for every trimmed stream.
    let resumed: ResumedSubscription = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Earliest)
        .await
        .expect("earliest still resumes after a trim");
    if let Some(history) = &resumed.history {
        assert!(
            history.from_offset >= base,
            "earliest resumed at {}, below the retained base {base}",
            history.from_offset
        );
    }
}

#[tokio::test]
async fn a_trimmed_read_reports_cursor_too_old_rather_than_a_storage_error() {
    let dir = tempdir().expect("dir");
    let storage =
        DurableStorage::open(dir.path(), retention_log_config(8 * 1024)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    register(&broker, "orders", true).await;

    let base = publish_and_trim(&broker, &storage, "orders").await;

    // This is the mid-replay case: a paging replay whose next read lands below
    // a base offset that moved underneath it. Reported, not silently short.
    let err = broker
        .read_durable("t1", "default", "orders", 0, 0, 64 * 1024)
        .await
        .expect_err("reading trimmed offsets must fail");
    match err {
        BrokerError::CursorTooOld { oldest, requested } => {
            assert_eq!(requested, 0);
            assert_eq!(oldest, base);
        }
        other => panic!("expected CursorTooOld, got {other:?}"),
    }
}

/// A cursor must never name a position past the live edge.
///
/// `cursor_tail` reads the durable log, and a publish claims its offsets before
/// the record reaches the replay ring. Taken between those two points, the
/// cursor named an offset the ring had not yet seen: the backlog came back
/// empty, and the in-flight record then arrived *live* — below the cursor the
/// caller was told to resume from. A subscriber resuming from a checkpoint saw
/// one record twice.
///
/// Driven by publishing concurrently and sampling the seam repeatedly, because
/// the window is exactly one publish wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_never_points_past_the_live_edge() {
    const TOTAL: u32 = 400;

    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::None).await;
    register(&broker, "orders", true).await;

    let publishing = {
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            for i in 0..TOTAL {
                broker
                    .publish("t1", "default", "orders", payload(&format!("v{i:04}")))
                    .await
                    .expect("publish");
            }
        })
    };

    // Sample the seam repeatedly while publishing is in flight. Each sample
    // asserts the property directly: the first record delivered from a cursor
    // is the cursor's own position, never something older.
    for _ in 0..250 {
        let cursor = broker
            .cursor_tail("t1", "default", "orders", 0)
            .await
            .expect("cursor");
        let from = cursor.next_seq();
        let (backlog, mut sub) = broker
            .subscribe_with_cursor("t1", "default", "orders", 0, cursor)
            .await
            .expect("subscribe");

        let first = match backlog.first() {
            Some(bytes) => Some(bytes.clone()),
            None => tokio::time::timeout(Duration::from_millis(50), sub.recv())
                .await
                .ok()
                .flatten(),
        };

        if let Some(first) = first {
            let text = String::from_utf8(first.to_vec()).expect("utf8");
            let index: u32 = text.trim_start_matches('v').parse().expect("index");
            assert!(
                u64::from(index) >= from,
                "resuming at {from} delivered {text} first, which is older than the cursor",
            );
        }
        tokio::task::yield_now().await;
    }

    publishing.await.expect("join");
}

// ---------------------------------------------------------------------------
// Where a resumed subscription joins: current position, then every change.
// ---------------------------------------------------------------------------

/// Collect `want` records off a resume -- history, backlog, then live --
/// failing on a timeout rather than reporting it as a lost record.
async fn collect_resume(
    broker: &Broker,
    stream: &str,
    resumed: &mut ResumedSubscription,
    want: usize,
) -> Vec<String> {
    let mut seen = drain_resume(broker, stream, resumed).await;
    while seen.len() < want {
        match tokio::time::timeout(Duration::from_secs(30), resumed.subscription.recv()).await {
            Ok(Some(msg)) => seen.push(String::from_utf8(msg.to_vec()).expect("utf8")),
            Ok(None) => break,
            Err(_) => panic!(
                "timed out after {} of {want} records -- the machine was too slow to \
                 conclude anything about the join",
                seen.len()
            ),
        }
    }
    seen
}

/// A publisher that runs until told to stop, so a join made while it is
/// running really does race it. Returns how many it published; record `vNNNN`
/// is at offset NNNN.
fn spawn_publisher(
    broker: &Arc<Broker>,
    from: u64,
) -> (
    Arc<std::sync::atomic::AtomicU64>,
    Arc<std::sync::atomic::AtomicBool>,
    tokio::task::JoinHandle<u64>,
) {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let published = Arc::new(AtomicU64::new(from));
    let stop = Arc::new(AtomicBool::new(false));
    let task = {
        let (broker, published, stop) = (
            Arc::clone(broker),
            Arc::clone(&published),
            Arc::clone(&stop),
        );
        tokio::spawn(async move {
            let mut i = from;
            while !stop.load(Ordering::Relaxed) {
                broker
                    .publish("t1", "default", "orders", payload(&format!("v{i:05}")))
                    .await
                    .expect("publish");
                i += 1;
                published.store(i, Ordering::Relaxed);
            }
            i
        })
    };
    (published, stop, task)
}

/// Let the publisher get going, join, then let it run on past the join.
async fn join_mid_flight(
    broker: &Arc<Broker>,
    from: u64,
    start: StartPosition,
) -> (ResumedSubscription, u64) {
    use std::sync::atomic::Ordering;
    let (published, stop, task) = spawn_publisher(broker, from);
    while published.load(Ordering::Relaxed) < from + 50 {
        tokio::task::yield_now().await;
    }
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, start)
        .await
        .expect("subscribe");
    let at_join = published.load(Ordering::Relaxed);
    while published.load(Ordering::Relaxed) < at_join + 100 {
        tokio::task::yield_now().await;
    }
    stop.store(true, Ordering::Relaxed);
    let total = task.await.expect("publisher");
    (resumed, total)
}

/// `Latest` is the stream's current position and every record after it. Joined
/// while a publisher runs flat out, the reported live edge is exactly where
/// delivery starts, nothing at or after it is missed, and nothing before it is
/// delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_joins_at_the_reported_live_edge_under_concurrent_publishes() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 8).await;
    register(&broker, "orders", true).await;

    let (mut resumed, total) = join_mid_flight(&broker, 0, StartPosition::Latest).await;
    let join = resumed
        .join
        .expect("a durable stream reports where it joined");
    assert_eq!(join.start_offset, join.live_offset);
    assert!(
        join.live_offset >= 50,
        "joined before the publisher started"
    );

    let from = join.live_offset;
    let seen = collect_resume(&broker, "orders", &mut resumed, (total - from) as usize).await;
    let expected: Vec<String> = (from..total).map(|i| format!("v{i:05}")).collect();
    assert_eq!(seen, expected, "the join at offset {from} was not gap-free");
}

/// Resuming from an offset under concurrent publishes: delivery starts at the
/// requested offset, and the reported live edge splits what was already
/// written at join from what came after, with nothing lost across it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offset_resume_reports_where_catch_up_ends_under_concurrent_publishes() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 8).await;
    register(&broker, "orders", true).await;

    let (mut resumed, total) = join_mid_flight(&broker, 0, StartPosition::Offset(10)).await;
    let join = resumed
        .join
        .expect("a durable stream reports where it joined");
    assert_eq!(join.start_offset, 10);
    assert!(join.live_offset >= 50 && join.live_offset <= total);

    let seen = collect_resume(&broker, "orders", &mut resumed, (total - 10) as usize).await;
    let expected: Vec<String> = (10..total).map(|i| format!("v{i:05}")).collect();
    assert_eq!(seen, expected);
}

/// An empty stream joined at `Latest` is a definite "nothing yet": start and
/// live edge are both 0, and the first publish is the first record delivered.
#[tokio::test]
async fn latest_on_an_empty_stream_joins_at_zero() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 8).await;
    register(&broker, "orders", true).await;
    let mut resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let join = resumed.join.expect("join offsets");
    assert_eq!((join.start_offset, join.live_offset), (0, 0));
    broker
        .publish("t1", "default", "orders", payload("first"))
        .await
        .expect("publish");
    assert_eq!(
        collect_resume(&broker, "orders", &mut resumed, 1).await,
        ["first"]
    );
}

/// An in-memory stream's events carry no offsets, so it reports none.
#[tokio::test]
async fn an_in_memory_stream_reports_no_join_offsets() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_small_ring(&dir, 8).await;
    register(&broker, "ephemeral", false).await;
    let resumed = broker
        .subscribe_from("t1", "default", "ephemeral", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    assert_eq!(resumed.join, None);
}

fn handoff() -> felix_broker::ShardHandoff {
    felix_broker::ShardHandoff {
        node_id: Some("broker-b".to_string()),
        addr: Some("127.0.0.1:5001".to_string()),
        generation: 7,
    }
}

/// A subscription ended by a move learns where the stream had reached, not
/// what it happened to receive: a full queue dropped the tail of what was
/// published, and the resume point is still the first offset it was never
/// offered.
#[tokio::test]
async fn a_moved_subscription_resumes_after_everything_it_was_offered() {
    let dir = tempdir().expect("dir");
    let storage =
        DurableStorage::open(dir.path(), log_config(FsyncMode::OnCommit)).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage)
        .with_topic_capacity(2)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    register(&broker, "orders", true).await;

    let (mut receiver, _guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe")
        .into_parts();
    for i in 0..5 {
        broker
            .publish("t1", "default", "orders", payload(&format!("r{i}")))
            .await
            .expect("publish");
    }

    assert_eq!(
        broker
            .end_subscriptions("t1", "default", "orders", 0, Some(handoff()))
            .await,
        1
    );
    let mut offsets = Vec::new();
    while let Some(envelope) = receiver.recv().await {
        let base = envelope.base_offset().expect("durable offsets");
        offsets.extend((0..envelope.len() as u64).map(|i| base + i));
    }
    assert_eq!(offsets, vec![0, 1], "the queue holds two; the rest dropped");
    let moved = receiver.moved().expect("ended by a move");
    assert_eq!(moved.resume_from, Some(5));
    assert_eq!(moved.to, handoff());

    // One that joins after the move starts clean.
    let (receiver, _guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe again")
        .into_parts();
    assert!(receiver.moved().is_none());
}

/// Ending without a handoff is the old behaviour: the feed closes and says
/// nothing about a move. An in-memory stream's handoff carries no offset,
/// since its sequence means nothing on another broker.
#[tokio::test]
async fn only_a_durable_stream_names_an_offset_to_resume_from() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register(&broker, "orders", true).await;
    register(&broker, "ticks", false).await;

    let (mut plain, _plain_guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe")
        .into_parts();
    broker
        .end_subscriptions("t1", "default", "orders", 0, None)
        .await;
    assert!(plain.recv().await.is_none());
    assert!(plain.moved().is_none());

    let (mut ephemeral, _guard) = broker
        .subscribe("t1", "default", "ticks", 0)
        .await
        .expect("subscribe")
        .into_parts();
    broker
        .publish("t1", "default", "ticks", payload("t0"))
        .await
        .expect("publish");
    broker
        .end_subscriptions("t1", "default", "ticks", 0, Some(handoff()))
        .await;
    assert!(ephemeral.recv().await.is_some());
    assert!(ephemeral.recv().await.is_none());
    let moved = ephemeral.moved().expect("ended by a move");
    assert_eq!(moved.resume_from, None);
    assert_eq!(moved.to, handoff());
}

#[tokio::test]
async fn an_offset_reader_is_woken_by_a_durable_publish() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::None).await;
    register(&broker, "orders", true).await;
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    let log = handle.log().expect("durable log");
    assert_eq!(log.tail_offset().await.expect("tail"), 0);

    // Registered before the tail is read, as a long-polling reader does.
    let woken = handle.appended().notified_owned();
    tokio::pin!(woken);
    woken.as_mut().enable();

    let publisher = Arc::clone(&broker);
    tokio::spawn(async move {
        publisher
            .publish("t1", "default", "orders", payload("late"))
            .await
            .expect("publish");
    });
    tokio::time::timeout(Duration::from_secs(5), woken)
        .await
        .expect("woken by the publish");
    let records = log.read_from(0, 1 << 20).await.expect("read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload("late"));
}

#[tokio::test]
async fn tenant_streams_lists_only_that_tenant_in_order() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::None).await;
    register(&broker, "zeta", true).await;
    register(&broker, "alpha", false).await;
    broker.register_tenant("t2").await.expect("tenant");
    broker
        .register_namespace("t2", "default")
        .await
        .expect("ns");
    broker
        .register_stream("t2", "default", "other", StreamMetadata::default())
        .await
        .expect("register");

    let listed: Vec<_> = broker
        .tenant_streams("t1")
        .await
        .into_iter()
        .map(|(namespace, stream, metadata)| (namespace, stream, metadata.durable))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("default".to_string(), "alpha".to_string(), false),
            ("default".to_string(), "zeta".to_string(), true),
        ]
    );
    assert!(
        broker
            .stream_metadata("t1", "default", "zeta")
            .await
            .expect("registered")
            .durable
    );
    assert!(
        broker
            .stream_metadata("t1", "default", "nope")
            .await
            .is_none()
    );
}

/// A `Quorum` stream's ring holds only committed records, restart included.
///
/// Records past the commit offset may be a dead generation's, which the next
/// leader replaces. Hydrated into the ring they were served to readers of this
/// broker once promoted, before anything had committed them.
#[tokio::test]
async fn hydration_leaves_uncommitted_records_out_of_a_quorum_ring() {
    let dir = tempdir().expect("dir");
    async fn register_quorum(broker: &Broker) {
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                StreamMetadata {
                    durable: true,
                    shards: 1,
                    consistency: felix_broker::ConsistencyLevel::Quorum,
                },
            )
            .await
            .expect("register");
    }
    {
        let (broker, storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
        register_quorum(&broker).await;
        for i in 0..4 {
            broker
                .publish("t1", "default", "orders", payload(&format!("v{i}")))
                .await
                .expect("publish");
        }
        storage
            .open_stream("t1", "default", "orders", 0)
            .expect("log")
            .advance_commit_offset(2)
            .await
            .expect("commit");
        storage.shutdown().await.expect("shutdown");
    }

    let (broker, _storage) = broker_with_storage(&dir, FsyncMode::OnCommit).await;
    register_quorum(&broker).await;
    let resumed = broker
        .subscribe_from("t1", "default", "orders", 0, StartPosition::Offset(0))
        .await
        .expect("subscribe");
    let past_commit: Vec<u64> = resumed
        .backlog
        .iter()
        .map(|(offset, _)| *offset)
        .filter(|offset| *offset >= 2)
        .collect();
    assert!(
        past_commit.is_empty(),
        "the ring served uncommitted offsets {past_commit:?}"
    );
    let history = resumed.history.expect("served from disk");
    assert_eq!((history.from_offset, history.until_offset), (0, 4));
}
