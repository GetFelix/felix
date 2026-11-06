//! A stream's log does its disk work off the runtime's workers: opening it
//! when the stream registers, and labelling replicated records with their
//! generations. Each fsyncs, and a bulk create does hundreds of them.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{Epoch, FsyncMode, LogConfig};
use serial_test::serial;

/// Holds every flush in the process for as long as it lives.
struct HeldFlushes;

impl HeldFlushes {
    fn hold(delay: Duration) -> Self {
        felix_storage::fault::set_fsync_delay(delay);
        Self
    }
}

impl Drop for HeldFlushes {
    fn drop(&mut self) {
        felix_storage::fault::set_fsync_delay(Duration::ZERO);
    }
}

const FLUSH: Duration = Duration::from_millis(100);

fn on_commit() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// How long `work` took, and the longest the runtime went without running a
/// ticker beside it. On a single-threaded runtime a flush on the runtime
/// shows up as a gap.
async fn stall_while(work: impl std::future::Future<Output = ()>) -> (Duration, Duration) {
    let ticks = Arc::new(Mutex::new(vec![Instant::now()]));
    let ticker = tokio::spawn({
        let ticks = Arc::clone(&ticks);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(2)).await;
                ticks.lock().expect("ticks").push(Instant::now());
            }
        }
    });
    let started = Instant::now();
    work.await;
    let took = started.elapsed();
    ticker.abort();
    let mut ticks = ticks.lock().expect("ticks").clone();
    // The work's end counts as a tick, so a runtime that never got to tick
    // shows its whole stall.
    ticks.push(Instant::now());
    let longest = ticks
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or_default();
    (took, longest)
}

fn assert_off_runtime(what: &str, (took, longest): (Duration, Duration)) {
    assert!(took >= FLUSH, "{what} flushed nothing: {took:?}");
    assert!(
        longest < FLUSH / 2,
        "the runtime stalled {longest:?} while {what} took {took:?}"
    );
}

/// **Registering a durable stream does not flush on the runtime.** The
/// catalog sync registers every stream a bulk create made, and each opens a
/// new log.
#[tokio::test]
#[serial]
async fn registering_a_durable_stream_does_not_flush_on_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(dir.path(), on_commit()).expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let _held = HeldFlushes::hold(FLUSH);

    let stalls = stall_while(async {
        broker
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
            .expect("register");
    })
    .await;
    assert_off_runtime("registering the stream", stalls);
}

/// **Labelling replicated records does not flush on the runtime.** A
/// follower labels the first batch of each new generation, which for a bulk
/// create is a write per shard.
#[tokio::test]
#[serial]
async fn labelling_generations_does_not_flush_on_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(dir.path(), on_commit()).expect("storage");
    let log = storage
        .open_stream("t1", "default", "orders", 0)
        .expect("open");
    let _held = HeldFlushes::hold(FLUSH);

    let stalls = stall_while(async {
        log.label_generations_off_runtime(
            0,
            vec![Epoch {
                generation: 3,
                start_offset: 0,
            }],
        )
        .await
        .expect("label");
    })
    .await;
    assert_off_runtime("labelling", stalls);
    assert_eq!(
        log.generations().last().map(|epoch| epoch.generation),
        Some(3)
    );
}
