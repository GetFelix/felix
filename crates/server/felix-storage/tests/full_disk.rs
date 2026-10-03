//! A full disk while a log is created or written is reported as a full disk,
//! and the log opens once space is back.
//!
//! Its own test binary because the disk is filled through the process-wide
//! fault file, which no other test may share.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig};
use felix_storage::{DiskLog, StorageError};

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        fsync_mode: FsyncMode::None,
        preallocate_segments: true,
        ..LogConfig::default()
    }
}

fn record() -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(b"value"),
        timestamp_micros: 1,
        mark: Default::default(),
    }
}

/// Set the fault file and wait out the interval the storage layer reuses its
/// last reading for.
fn fill_disk(fault_file: &Path, full: bool) {
    std::fs::write(fault_file, if full { "write=enospc\n" } else { "" }).expect("fault file");
    std::thread::sleep(Duration::from_millis(150));
}

#[tokio::test]
async fn a_full_disk_is_reported_as_full_and_the_log_opens_once_space_is_back() {
    let scratch = tempfile::tempdir().expect("dir");
    let fault_file = scratch.path().join("faults");
    std::fs::write(&fault_file, "").expect("fault file");
    // SAFETY: the only test in this binary, and no thread has started that
    // reads the environment.
    unsafe { std::env::set_var("FELIX_STORAGE_FAULT_FILE", &fault_file) };
    let dir = scratch.path().join("log");

    fill_disk(&fault_file, true);
    match DiskLog::open(&dir, "t/ns/s/0", config()) {
        Err(StorageError::Full(_)) => {}
        other => panic!("expected a full disk, got {other:?}"),
    }

    fill_disk(&fault_file, false);
    let log = DiskLog::open(&dir, "t/ns/s/0", config()).expect("open once space is back");
    log.append(&[record()]).await.expect("append");

    fill_disk(&fault_file, true);
    match log.append(&[record()]).await {
        Err(StorageError::Full(_)) => {}
        other => panic!("expected a full disk, got {other:?}"),
    }

    fill_disk(&fault_file, false);
    let appended = log
        .append(&[record()])
        .await
        .expect("append once space is back");
    assert_eq!(appended.first_offset, 1);
}
