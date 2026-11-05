//! Opening a shard keeps its disk work off the runtime's workers.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_storage::log::{FsyncMode, LogConfig};
use serial_test::serial;

use super::*;

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

/// How long `open` took, and the longest the runtime went without running a
/// ticker beside it. On a single-threaded runtime any flush `open` does on the
/// runtime shows up as a gap.
async fn stall_while(open: impl std::future::Future<Output = ()>) -> (Duration, Duration) {
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
    open.await;
    let took = started.elapsed();
    ticker.abort();
    let mut ticks = ticks.lock().expect("ticks").clone();
    // The open's end counts as a tick, so a runtime that never got to tick
    // shows its whole stall.
    ticks.push(Instant::now());
    let longest = ticks
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or_default();
    (took, longest)
}

fn on_commit() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// **A slow disk does not stall the runtime while a shard opens.** A new log
/// takes several fsyncs. A bulk create opens hundreds, and on the workers
/// every one of them would hold up the heartbeat, replication and serving
/// behind the disk.
#[tokio::test]
#[serial]
async fn opening_a_shard_does_not_flush_on_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage =
        Arc::new(felix_broker::DurableStorage::open(dir.path(), on_commit()).expect("storage"));
    let store = DurableShardStore::new(Arc::clone(&storage));
    let flush = Duration::from_millis(100);
    let _held = HeldFlushes::hold(flush);

    let (took, longest) =
        stall_while(async { store.open(&key(0), 1, 0, true).await.expect("open") }).await;
    assert!(took >= flush, "the open flushed nothing: {took:?}");
    assert!(
        longest < flush / 2,
        "the runtime stalled {longest:?} during an open that took {took:?}"
    );
}

/// The same for the logs a shard opens beside its own: a stream's cursor
/// and dead-letter logs, and a cache's record and counter logs.
#[tokio::test]
#[serial]
async fn opening_group_and_cache_logs_does_not_flush_on_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path();
    let storage =
        felix_broker::DurableStorage::open(path.join("streams"), on_commit()).expect("storage");
    let groups = Arc::new(
        felix_broker::ConsumerGroups::open(path.join("groups"), on_commit()).expect("groups"),
    );
    let dead_letters = Arc::new(
        felix_broker::DeadLetters::open(path.join("dead-letters"), on_commit())
            .expect("dead letters"),
    );
    let counters = Arc::new(
        felix_storage::CounterStore::open(path.join("counters"), on_commit()).expect("counters"),
    );
    let broker = Arc::new(
        felix_broker::Broker::new(Box::new(
            felix_storage::LogCache::open(path.join("caches"), on_commit()).expect("cache"),
        ))
        .with_durable_storage(storage.clone())
        .with_counters(counters)
        .with_consumer_groups(groups, dead_letters, Duration::from_secs(30), 3),
    );
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    let store =
        DurableShardStore::new(Arc::new(storage)).with_generation_starts(GenerationStarts {
            broker: Arc::clone(&broker),
            fleet: Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        });
    let cache = ShardKey {
        kind: crate::shards::ShardKind::Cache,
        ..key(0)
    };
    let flush = Duration::from_millis(100);
    let _held = HeldFlushes::hold(flush);

    for (shard, label) in [(key(0), "stream"), (cache, "cache")] {
        let (took, longest) =
            stall_while(async { store.open(&shard, 1, 0, true).await.expect("open") }).await;
        assert!(took >= flush, "the {label} open flushed nothing: {took:?}");
        assert!(
            longest < flush / 2,
            "the runtime stalled {longest:?} during a {label} open that took {took:?}"
        );
    }
}
