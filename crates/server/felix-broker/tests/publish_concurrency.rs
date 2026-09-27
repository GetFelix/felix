//! Does the broker let concurrent publishes reach the device flush together?
//!
//! `felix-storage`'s group commit coalesces properly when driven concurrently --
//! `felix-storage/tests/group_commit_fanin.rs` measures a 7.25x speedup at
//! 16-way. But an Azure session measured the fan-in actually achieved in a
//! running cluster (`felix_storage_sync_batch_appends`) at **1.004-1.007** with
//! 48 publishers, which means callers were arriving at `ensure_durable` one at
//! a time rather than together.
//!
//! Somewhere between the wire and the log, publishes are being serialised. This
//! bisects that: it drives `Broker::publish` directly, with no QUIC transport
//! and none of the service's ingress workers in the path.
//!
//! - Shared flushes here mean `felix-broker` is *not* the serialiser, and the cause
//!   is above it in `services/felix-broker-service` (the publish scheduler, which claims a
//!   stream shard's publishes one at a time on that shard's lane).
//! - One flush per publish here means the serialisation is in `felix-broker` itself, and
//!   this test is where to debug it.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

const PUBLISHES: usize = 64;
const CONCURRENCY: usize = 16;
const PAYLOAD: usize = 4096;

fn log_config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 256 * 1024 * 1024,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker_with_storage(dir: &TempDir) -> Arc<Broker> {
    let storage = DurableStorage::open(dir.path(), log_config()).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    Arc::new(broker)
}

async fn register(broker: &Broker, stream: &str) {
    broker
        .register_stream(
            "t1",
            "default",
            stream,
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("register");
}

/// **Concurrent publishes to one durable stream should share device flushes.**
///
/// Counted in flushes, as in the storage-level test. This one says whether the
/// broker preserves the concurrency the storage layer needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishes_to_one_stream_share_a_flush() {
    let dir = tempdir().expect("dir");
    let broker = broker_with_storage(&dir).await;
    register(&broker, "serial").await;
    register(&broker, "concurrent").await;

    let body = || Bytes::from(vec![b'x'; PAYLOAD]);

    // Warm both streams so segment creation is not inside a measured window.
    broker
        .publish("t1", "default", "serial", body())
        .await
        .expect("warm serial");
    broker
        .publish("t1", "default", "concurrent", body())
        .await
        .expect("warm concurrent");

    let storage = broker.durable_storage().expect("durable storage");
    let serial_log = storage
        .open_stream("t1", "default", "serial", 0)
        .expect("serial log");
    let concurrent_log = storage
        .open_stream("t1", "default", "concurrent", 0)
        .expect("concurrent log");

    let serial_flushes_before = serial_log.flushes();
    let serial_start = Instant::now();
    for _ in 0..PUBLISHES {
        broker
            .publish("t1", "default", "serial", body())
            .await
            .expect("publish");
    }
    let serial = serial_start.elapsed();
    let serial_flushes = serial_log.flushes() - serial_flushes_before;

    let per_task = PUBLISHES / CONCURRENCY;
    let concurrent_flushes_before = concurrent_log.flushes();
    let concurrent_start = Instant::now();
    let mut tasks = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let broker = Arc::clone(&broker);
        tasks.push(tokio::spawn(async move {
            for _ in 0..per_task {
                broker
                    .publish(
                        "t1",
                        "default",
                        "concurrent",
                        Bytes::from(vec![b'x'; PAYLOAD]),
                    )
                    .await
                    .expect("publish");
            }
        }));
    }
    for task in tasks {
        task.await.expect("task");
    }
    let concurrent = concurrent_start.elapsed();
    let concurrent_flushes = concurrent_log.flushes() - concurrent_flushes_before;

    let speedup = serial.as_secs_f64() / concurrent.as_secs_f64().max(f64::EPSILON);
    eprintln!(
        "{PUBLISHES} durable publishes: serial {serial_flushes} flushes in {serial:?}, \
         {CONCURRENCY}-way concurrent {concurrent_flushes} flushes in {concurrent:?} \
         (wall-clock speedup {speedup:.2}x, for information only)"
    );

    // Flushes, not wall clock: on a busy runner with a fast disk, publishes
    // that share flushes perfectly well can still measure slower than serial
    // (0.49x under coverage on CI). Serial is the control; each publish there
    // waits alone and pays its own flush.
    assert!(
        serial_flushes >= PUBLISHES as u64 / 2,
        "the serial run coalesced ({serial_flushes} flushes for {PUBLISHES} publishes), \
         so it is not a control for the concurrent one"
    );
    assert!(
        concurrent_flushes * 2 <= serial_flushes,
        "concurrent publishes did not share flushes: {concurrent_flushes} flushes for \
         {PUBLISHES} publishes against {serial_flushes} when serial. The broker is \
         serialising publishes before they reach the device flush, so group commit has \
         nothing to coalesce -- which is the fan-in of 1 measured in the Azure sessions."
    );
}
