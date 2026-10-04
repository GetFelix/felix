//! Does group commit actually group?
//!
//! Under `FsyncMode::OnCommit` the point of group commit is that concurrent
//! appends share one device flush: N publishers committing together should cost
//! roughly one flush, not N. `docs/durable-storage.md` calls this "the single
//! biggest throughput lever", and `disk_log/sync.rs` puts a number on it --
//! "the difference between a few hundred and a few tens of thousands of durable
//! appends per second on the same hardware".
//!
//! An Azure session measured the fan-in actually achieved
//! (`felix_storage_sync_batch_appends`) at **1.004-1.007** with 48 concurrent
//! publishers, against a mean flush of ~300 us -- which put a hard ceiling on
//! durable throughput at roughly one 256 KiB batch per flush. This test asks
//! the same question in-process, without a cluster: it times the same number of
//! durable appends issued serially and issued concurrently.
//!
//! If group commit is working, the concurrent run is many times faster, because
//! the flushes coalesce. If the two are comparable, the appends are serialising
//! somewhere before the flush and the fan-in is 1 by construction.

use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_storage::DiskLogProvider;
use felix_storage::log::{AppendRecord, FsyncMode, LogConfig, ShardKey};

const RECORDS_PER_APPEND: usize = 8;
const PAYLOAD: usize = 4096;
const APPENDS: usize = 64;
const CONCURRENCY: usize = 16;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 256 * 1024 * 1024,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn shard(stream: &str) -> ShardKey {
    ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: stream.into(),
        shard: 0,
    }
}

fn batch() -> Vec<AppendRecord> {
    (0..RECORDS_PER_APPEND)
        .map(|_| AppendRecord {
            payload: vec![b'x'; PAYLOAD].into(),
            timestamp_micros: 0,
            mark: Default::default(),
            publisher: None,
        })
        .collect()
}

async fn durable_append(log: &felix_storage::DiskLog) {
    let records = batch();
    let pending = log.append_pending(&records).await.expect("append");
    log.commit(&pending).await.expect("commit");
}

/// **Concurrent durable appends should not cost the same as serial ones.**
///
/// The ratio is the measurement. Group commit working means the concurrent run
/// finishes in a small fraction of the serial one; a ratio near 1.0 means every
/// append paid its own flush regardless of how many were in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_durable_appends_share_a_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = DiskLogProvider::new(dir.path().to_path_buf(), config()).expect("provider");

    let serial_log = provider.open_shard(&shard("serial")).expect("open serial");
    // Warm the segment so the first flush does not pay creation costs.
    durable_append(&serial_log).await;

    let serial_flushes_before = serial_log.flushes();
    let serial_start = Instant::now();
    for _ in 0..APPENDS {
        durable_append(&serial_log).await;
    }
    let serial = serial_start.elapsed();
    let serial_flushes = serial_log.flushes() - serial_flushes_before;

    let concurrent_log = Arc::new(
        provider
            .open_shard(&shard("concurrent"))
            .expect("open concurrent"),
    );
    durable_append(&concurrent_log).await;

    let per_task = APPENDS / CONCURRENCY;
    let concurrent_flushes_before = concurrent_log.flushes();
    let concurrent_start = Instant::now();
    let mut tasks = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let log = Arc::clone(&concurrent_log);
        tasks.push(tokio::spawn(async move {
            for _ in 0..per_task {
                durable_append(&log).await;
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
        "{APPENDS} appends: serial {serial_flushes} flushes in {serial:?}, \
         {CONCURRENCY}-way concurrent {concurrent_flushes} flushes in {concurrent:?} \
         (wall-clock speedup {speedup:.2}x, for information only)"
    );

    // Flushes, not wall clock. The property is that one flush serves many
    // waiting appends, and counting them sees that directly. A speedup ratio
    // sees it only through the machine: on a runner that cannot put sixteen
    // appends in flight at once, perfectly good group commit measures no faster
    // than serial -- which is how this test read 0.70x on CI and was taken for a
    // regression twice.
    //
    // Serial is the control. Each of those appends waits alone, so it pays its
    // own flush and the count should track the appends.
    assert!(
        serial_flushes >= APPENDS as u64 / 2,
        "the serial run coalesced ({serial_flushes} flushes for {APPENDS} appends), \
         so it is not a control for the concurrent one -- something overlapped that \
         was supposed to be sequential"
    );
    assert!(
        concurrent_flushes * 2 <= serial_flushes,
        "concurrent appends did not share flushes: {concurrent_flushes} flushes for \
         {APPENDS} appends against {serial_flushes} when serial. Group commit is not \
         coalescing -- see the fan-in measured in the Azure sessions."
    );

    assert!(
        concurrent < Duration::from_secs(120),
        "the concurrent run did not finish in a sane time"
    );
}
