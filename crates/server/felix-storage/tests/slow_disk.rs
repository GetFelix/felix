//! A disk that stalls writes must not stall the runtime appending to it.
//!
//! Once Linux throttles a process that dirties pages faster than the device
//! takes them, a buffered `write` blocks for as long as the device needs. A
//! write made on a Tokio worker blocks that worker with it, and with enough
//! appends in flight every worker is blocked and nothing else on the runtime
//! runs: not a timer, not a QUIC endpoint. Each log appends on a thread of its
//! own, so its callers wait without holding a worker, and a slow log holds up
//! only the appends behind it.
//!
//! The delay is process-wide, so this test has a binary of its own.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use felix_storage::DiskLogProvider;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ShardKey};

/// How long every segment write takes once the disk is slowed down.
const WRITE_DELAY: Duration = Duration::from_millis(50);
const WORKERS: usize = 2;
const LOGS: usize = 4;
const APPENDS_PER_LOG: usize = 2;

fn shard(stream: &str) -> ShardKey {
    ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: stream.into(),
        shard: 0,
    }
}

fn batch() -> Vec<AppendRecord> {
    vec![AppendRecord {
        payload: vec![b'x'; 128].into(),
        timestamp_micros: 0,
        mark: Default::default(),
        publisher: None,
    }]
}

/// **A slow disk leaves the runtime running.** Eight appends, more than
/// there are workers, each with a write that takes 50 ms, while a task that
/// stands in for the QUIC endpoint ticks every millisecond. Writing on the
/// workers, both would block and the ticks would stop for at least a write;
/// the logs would also take their turns on two workers rather than writing at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_disk_does_not_stall_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let provider = DiskLogProvider::new(dir.path().to_path_buf(), config).expect("provider");
    let mut logs = Vec::with_capacity(LOGS);
    for index in 0..LOGS {
        let log = provider
            .open_shard(&shard(&format!("s{index}")))
            .expect("open");
        // The segment is created before the disk slows down.
        log.append(&batch()).await.expect("warm");
        logs.push(Arc::new(log));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let ticker = tokio::spawn({
        let stop = Arc::clone(&stop);
        async move {
            let mut worst = Duration::ZERO;
            let mut last = Instant::now();
            while !stop.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                worst = worst.max(last.elapsed());
                last = Instant::now();
            }
            worst
        }
    });
    // Let the ticker settle before the writes start.
    tokio::time::sleep(Duration::from_millis(20)).await;

    felix_storage::fault::set_write_delay(WRITE_DELAY);
    let started = Instant::now();
    let appends: Vec<_> = (0..LOGS * APPENDS_PER_LOG)
        .map(|i| {
            let log = Arc::clone(&logs[i % LOGS]);
            tokio::spawn(async move { log.append(&batch()).await })
        })
        .collect();
    for append in appends {
        append.await.expect("task").expect("append");
    }
    let elapsed = started.elapsed();
    felix_storage::fault::set_write_delay(Duration::ZERO);
    stop.store(true, Ordering::Release);
    let worst = ticker.await.expect("ticker");

    assert!(
        worst < WRITE_DELAY / 2,
        "a 1 ms ticker went {worst:?} without running while writes took {WRITE_DELAY:?}: \
         the writes held the runtime's workers"
    );
    // Each log writes its two batches in turn, and the logs write at once.
    // Two workers taking all eight writes in turn would need four delays.
    let serial = WRITE_DELAY * (LOGS * APPENDS_PER_LOG / WORKERS) as u32;
    assert!(
        elapsed < serial * 3 / 4,
        "{} writes of {WRITE_DELAY:?} on {LOGS} logs took {elapsed:?}, \
         no faster than {WORKERS} workers writing in turn ({serial:?})",
        LOGS * APPENDS_PER_LOG
    );
}
