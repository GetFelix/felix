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

/// **A slow disk does not stall the runtime while a shard opens.** A new log
/// takes several fsyncs. A bulk create opens hundreds, and on the workers
/// every one of them would hold up the heartbeat, replication and serving
/// behind the disk. A single-threaded runtime makes any flush on it visible as
/// a gap in a ticker running beside the open.
#[tokio::test]
#[serial]
async fn opening_a_shard_does_not_flush_on_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        felix_broker::DurableStorage::open(
            dir.path(),
            LogConfig {
                fsync_mode: FsyncMode::OnCommit,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage"),
    );
    let store = DurableShardStore::new(Arc::clone(&storage));
    let flush = Duration::from_millis(100);
    let _held = HeldFlushes::hold(flush);

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
    store.open(&key(0), 1, 0, true).await.expect("open");
    let took = started.elapsed();
    ticker.abort();

    let ticks = ticks.lock().expect("ticks");
    let longest = ticks
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or_default();
    assert!(took >= flush, "the open flushed nothing: {took:?}");
    assert!(
        longest < flush / 2,
        "the runtime stalled {longest:?} during an open that took {took:?}"
    );
}
