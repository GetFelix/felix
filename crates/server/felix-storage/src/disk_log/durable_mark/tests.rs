use std::path::Path;

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::disk_log::DiskLog;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ReadRange};
use crate::segment::segment_file_name;
use crate::{Result, StorageError};

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn batch(prefix: &str, count: usize) -> Vec<AppendRecord> {
    (0..count)
        .map(|i| AppendRecord {
            payload: Bytes::from(format!("{prefix}-{i:03}")),
            timestamp_micros: 1,
            mark: Default::default(),
        })
        .collect()
}

async fn read_all(log: &DiskLog) -> Vec<String> {
    log.read_range(ReadRange {
        start: 0,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .into_iter()
    .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
    .collect()
}

/// Ten synced records, then ten written but never synced, then the log is
/// dropped the way a crash drops it. Returns the mark the sync left.
async fn synced_then_unsynced(dir: &Path) -> DurableMark {
    let log = DiskLog::open(dir, "t/ns/s/0", config()).expect("open");
    log.append(&batch("synced", 10)).await.expect("append");
    log.sync().await.expect("sync");
    log.append(&batch("unsynced", 10)).await.expect("append");
    drop(log);
    load(dir).expect("a flush leaves a mark")
}

/// What a power loss can leave inside `i_size`: blocks that never got the
/// data and read back as whatever the device held before. Not zeros.
fn stale_blocks_from(path: &Path, from: u64) {
    let mut bytes = std::fs::read(path).expect("read");
    for (i, byte) in bytes[from as usize..].iter_mut().enumerate() {
        *byte = 0xA5 ^ (i as u8);
    }
    std::fs::write(path, bytes).expect("write");
}

async fn reopen(dir: &Path) -> Result<DiskLog> {
    DiskLog::open(dir, "t/ns/s/0", config())
}

#[test]
fn a_mark_round_trips_and_a_damaged_one_reads_as_absent() {
    let mark = DurableMark {
        segment: 7,
        synced_bytes: 4096,
    };
    assert_eq!(DurableMark::decode(&mark.encode()), Some(mark));

    let mut damaged = mark.encode();
    damaged[20] ^= 1;
    assert_eq!(DurableMark::decode(&damaged), None);
    assert_eq!(DurableMark::decode(&mark.encode()[..16]), None);
}

#[test]
fn only_bytes_past_the_mark_are_unsynced() {
    let mark = Some(DurableMark {
        segment: 3,
        synced_bytes: 500,
    });
    assert_eq!(DurableMark::unsynced_from(mark, 3), Some(500));
    // Newer than the mark: nothing in it is known to be synced.
    assert_eq!(
        DurableMark::unsynced_from(mark, 4),
        Some(SEGMENT_HEADER_LEN)
    );
    // Older: sealed, so synced whole.
    assert_eq!(DurableMark::unsynced_from(mark, 2), None);
    // No mark: the strict rules.
    assert_eq!(DurableMark::unsynced_from(None, 3), None);
}

#[tokio::test]
async fn stale_blocks_past_the_mark_are_a_torn_tail() {
    let dir = tempdir().expect("dir");
    let mark = synced_then_unsynced(dir.path()).await;
    stale_blocks_from(
        &dir.path().join(segment_file_name(mark.segment)),
        mark.synced_bytes,
    );

    let log = reopen(dir.path())
        .await
        .expect("damage nobody synced must not stop the log opening");
    let records = read_all(&log).await;
    assert_eq!(records.len(), 10);
    assert!(records.iter().all(|payload| payload.starts_with("synced")));
}

#[tokio::test]
async fn damage_before_the_mark_is_still_fatal() {
    let dir = tempdir().expect("dir");
    let mark = synced_then_unsynced(dir.path()).await;
    // Inside the synced range: rot, not a lost write.
    stale_blocks_from(
        &dir.path().join(segment_file_name(mark.segment)),
        mark.synced_bytes - 20,
    );

    match reopen(dir.path()).await {
        Err(StorageError::Corruption(_)) => {}
        other => panic!("expected corruption, got {other:?}"),
    }
}

#[tokio::test]
async fn without_a_mark_stale_blocks_are_fatal_as_before() {
    let dir = tempdir().expect("dir");
    let mark = synced_then_unsynced(dir.path()).await;
    std::fs::remove_file(dir.path().join(mark_file_name())).expect("remove");
    stale_blocks_from(
        &dir.path().join(segment_file_name(mark.segment)),
        mark.synced_bytes,
    );

    match reopen(dir.path()).await {
        Err(StorageError::Corruption(_)) => {}
        other => panic!("expected corruption, got {other:?}"),
    }
}

#[tokio::test]
async fn a_truncation_moves_the_mark_back_durably() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config()).expect("open");
    log.append(&batch("a", 10)).await.expect("append");
    log.sync().await.expect("sync");
    let before = load(dir.path()).expect("mark");

    log.truncate(4).await.expect("truncate");
    let after = load(dir.path()).expect("mark");
    assert_eq!(after.segment, before.segment);
    assert!(after.synced_bytes < before.synced_bytes);
    assert_eq!(read_all(&log).await.len(), 4);
}
