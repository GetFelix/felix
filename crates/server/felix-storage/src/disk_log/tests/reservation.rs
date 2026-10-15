//! The active segment's block reservation starts small and grows as it fills,
//! off the append path, without ever changing what recovery reads.

use std::path::{Path, PathBuf};

use super::*;
use crate::segment::reservation::INITIAL_RESERVATION_BYTES;
use crate::segment::segment_file_name;

const MIB: u64 = 1024 * 1024;

/// The default segment size, with preallocation on, so the schedule is the
/// one a real stream gets.
fn reserving() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: true,
        ..LogConfig::default()
    }
}

/// A 64 KiB record, so a few cross each threshold.
fn big(i: usize) -> AppendRecord {
    let mut payload = format!("{i:08}:").into_bytes();
    payload.resize(64 * 1024, b'a' + (i % 26) as u8);
    AppendRecord {
        payload: Bytes::from(payload),
        timestamp_micros: 1_700_000_000,
        mark: Default::default(),
        publisher: None,
    }
}

fn active_path(dir: &Path, log: &DiskLog) -> PathBuf {
    let active = log.segments().last().expect("a segment").id;
    dir.join(segment_file_name(active))
}

#[cfg(unix)]
fn allocated(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).expect("meta").blocks() * 512
}

/// Append until the segment holds at least `bytes`, then wait for the
/// extensions those appends earned.
async fn fill_to(log: &DiskLog, next: &mut usize, bytes: u64) {
    while log.segments().last().expect("a segment").size_bytes < bytes {
        log.append(&[big(*next)]).await.expect("append");
        *next += 1;
    }
}

async fn wait_for_extensions(log: &DiskLog, count: u64) {
    for _ in 0..2_000 {
        if log.inner.extensions_done.load(Ordering::Acquire) >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("the reservation never grew to step {count}");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_stream_reserves_a_megabyte_and_grows_as_it_is_written() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", reserving()).expect("open");
    let path = active_path(dir.path(), &log);

    let fresh = allocated(&path);
    assert!(
        (INITIAL_RESERVATION_BYTES..2 * MIB).contains(&fresh),
        "a new segment reserved {fresh} bytes",
    );

    let mut next = 0;
    fill_to(&log, &mut next, MIB / 2 + 1).await;
    wait_for_extensions(&log, 1).await;
    let grown = allocated(&path);
    assert!(grown >= 2 * MIB, "after one step it holds {grown} bytes");

    fill_to(&log, &mut next, MIB + 1).await;
    wait_for_extensions(&log, 2).await;
    let grown = allocated(&path);
    assert!(grown >= 4 * MIB, "after two steps it holds {grown} bytes");

    // Reserved blocks never show up as length.
    let size = log.segments().last().expect("a segment").size_bytes;
    assert_eq!(std::fs::metadata(&path).expect("meta").len(), size);
    log.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_extension_does_not_fail_an_append() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", reserving()).expect("open");
    log.inner.fail_extensions.store(true, Ordering::Release);

    let mut next = 0;
    fill_to(&log, &mut next, 3 * MIB).await;
    wait_for_extensions(&log, 3).await;
    // And the log is still healthy afterwards, not just for the appends that
    // raced the failures.
    log.append(&[big(next)])
        .await
        .expect("append after the failures");
    next += 1;

    let read = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    assert_eq!(read.len(), next);
    log.shutdown().await.expect("shutdown");
}

/// A process crash with an extension parked mid-flight: the page cache, and
/// so every written record, survives, and the reservation must not have made
/// the file look any longer than its records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_mid_extension_recovers_exactly_the_written_records() {
    let dir = tempdir().expect("dir");
    let config = reserving();
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config.clone()).expect("open");
    let (release, held) = std::sync::mpsc::channel();
    *log.inner.hold_next_extension.lock() = Some(held);

    let mut next = 0;
    fill_to(&log, &mut next, MIB / 2 + 1).await;
    for _ in 0..2_000 {
        if log.inner.hold_next_extension.lock().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        log.inner.hold_next_extension.lock().is_none(),
        "no extension started"
    );
    // A few more while it is parked.
    for _ in 0..3 {
        log.append(&[big(next)]).await.expect("append");
        next += 1;
    }

    let image = tempdir().expect("image");
    for entry in std::fs::read_dir(dir.path()).expect("list") {
        let entry = entry.expect("entry");
        if entry.file_type().expect("type").is_file() {
            std::fs::copy(entry.path(), image.path().join(entry.file_name())).expect("copy");
        }
    }
    drop(release);

    let recovered = DiskLog::open(image.path(), "t/ns/s/0", config).expect("recover");
    let read = recovered
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    assert_eq!(
        read.len(),
        next,
        "recovery kept a different number of records"
    );
    for (i, record) in read.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
        assert_eq!(
            record.payload,
            big(i).payload,
            "record {i} came back different"
        );
    }
    let size = recovered.segments().last().expect("a segment").size_bytes;
    let path = active_path(image.path(), &recovered);
    assert_eq!(std::fs::metadata(path).expect("meta").len(), size);
    recovered.shutdown().await.expect("shutdown");
    log.shutdown().await.expect("shutdown");
}
