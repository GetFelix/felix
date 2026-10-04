//! How long a large shard takes to open, with and without producer state to
//! rebuild. Not a correctness test, so ignored by default:
//!
//! ```text
//! cargo test --release -p felix-storage --test producer_state_open -- --ignored --nocapture
//! ```

use std::time::Instant;

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::disk_log::ProducerSequence;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, RecordMark};

const BATCHES: u64 = 100_000;
const BATCH: usize = 10;
const PAYLOAD: usize = 256;
const PRODUCERS: u64 = 1_000;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 16 * 1024 * 1024,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// Write the log: every other batch from one of `PRODUCERS` idempotent
/// producers when `marked`, unmarked otherwise.
async fn write(dir: &std::path::Path, marked: bool) {
    let log = DiskLog::open(dir, "t/ns/s/0", config()).expect("open");
    let payload = Bytes::from(vec![7u8; PAYLOAD]);
    for batch in 0..BATCHES {
        let producer = batch % PRODUCERS;
        let sequence = batch / PRODUCERS / 2;
        let marks: Vec<RecordMark> = if marked && (batch / PRODUCERS).is_multiple_of(2) {
            RecordMark::for_batch(producer, sequence, BATCH).collect()
        } else {
            vec![RecordMark::None; BATCH]
        };
        let records: Vec<AppendRecord> = marks
            .into_iter()
            .map(|mark| AppendRecord {
                payload: payload.clone(),
                timestamp_micros: 1,
                mark,
                publisher: None,
            })
            .collect();
        log.append(&records).await.expect("append");
    }
    log.shutdown().await.expect("shutdown");
}

fn time_open(dir: &std::path::Path) -> (f64, DiskLog) {
    let started = Instant::now();
    let log = DiskLog::open(dir, "t/ns/s/0", config()).expect("open");
    (started.elapsed().as_secs_f64() * 1e3, log)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark"]
async fn opening_a_large_shard() {
    let plain = tempfile::tempdir().expect("dir");
    let marked = tempfile::tempdir().expect("dir");
    write(plain.path(), false).await;
    write(marked.path(), true).await;

    let (plain_ms, log) = time_open(plain.path());
    let segments = log.segments().len();
    drop(log);
    let (marked_ms, log) = time_open(marked.path());
    assert_ne!(log.producer_sequence(1, 0), ProducerSequence::Unknown);
    drop(log);
    std::fs::remove_file(marked.path().join("producers")).expect("snapshot");
    let (rebuilt_ms, log) = time_open(marked.path());
    assert_ne!(log.producer_sequence(1, 0), ProducerSequence::Unknown);

    println!(
        "{segments} segments, {} MiB: unmarked {plain_ms:.1} ms; \
         {PRODUCERS} producers with a snapshot {marked_ms:.1} ms; \
         without one {rebuilt_ms:.1} ms",
        BATCHES * (BATCH * PAYLOAD) as u64 / (1024 * 1024),
    );
}
