use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use crate::disk_log::DiskLog;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ReadRange};
use crate::segment::SEGMENT_HEADER_LEN;

/// Segments of a few records each, so a short run makes many of them.
fn config(max_open_sealed_segments: usize) -> LogConfig {
    LogConfig {
        segment_size_bytes: SEGMENT_HEADER_LEN + 120,
        index_spacing_bytes: 48,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        max_open_sealed_segments,
        ..LogConfig::default()
    }
}

async fn fill(dir: &TempDir, records: usize) {
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config(4)).expect("open");
    for i in 0..records {
        log.append(&[AppendRecord {
            payload: Bytes::from(format!("value-{i:03}")),
            timestamp_micros: 1,
            mark: Default::default(),
            publisher: None,
        }])
        .await
        .expect("append");
    }
    log.shutdown().await.expect("shutdown");
}

async fn read_from(log: &DiskLog, start: u64) -> Vec<String> {
    log.read_range(ReadRange {
        start,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .into_iter()
    .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
    .collect()
}

/// Descriptors this process holds on the log's segment files.
#[cfg(target_os = "linux")]
fn open_segment_files(dir: &TempDir) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("fds")
        .filter_map(|fd| std::fs::read_link(fd.ok()?.path()).ok())
        .filter(|target| {
            target.starts_with(dir.path()) && target.extension().is_some_and(|ext| ext == "log")
        })
        .count()
}

#[tokio::test]
async fn opening_a_log_opens_no_sealed_segment() {
    let dir = tempdir().expect("dir");
    fill(&dir, 60).await;

    let log = DiskLog::open(dir.path(), "t/ns/s/0", config(4)).expect("reopen");
    assert!(log.segments().len() > 10, "expected many segments");
    assert_eq!(log.open_sealed_segments(), 0);
    #[cfg(target_os = "linux")]
    {
        // The active segment's writer, its flush handle and its reader.
        assert!(open_segment_files(&dir) <= 3);
    }

    // The first read of a cold segment opens it.
    assert_eq!(read_from(&log, 5).await[0], "value-005");
    assert!(log.open_sealed_segments() >= 1);
}

#[tokio::test]
async fn reading_every_segment_keeps_no_more_than_the_limit_open() {
    let dir = tempdir().expect("dir");
    fill(&dir, 60).await;

    let log = DiskLog::open(dir.path(), "t/ns/s/0", config(2)).expect("reopen");
    let sealed = log.segments().len() - 1;
    assert!(sealed > 4);

    // One read per segment, oldest first, and then the whole log at once.
    for descriptor in log.segments() {
        let values = read_from(&log, descriptor.base_offset).await;
        assert_eq!(values[0], format!("value-{:03}", descriptor.base_offset));
        assert!(log.open_sealed_segments() <= 2);
    }
    let all = read_from(&log, 0).await;
    assert_eq!(all.len(), 60);
    assert!(
        all.iter()
            .enumerate()
            .all(|(i, v)| *v == format!("value-{i:03}"))
    );
    assert!(log.open_sealed_segments() <= 2);

    #[cfg(target_os = "linux")]
    {
        // The limit, plus the active segment's three.
        assert!(open_segment_files(&dir) <= 2 + 3);
    }
}

#[tokio::test]
async fn a_truncation_never_leaves_a_stale_index_for_a_reused_segment() {
    let dir = tempdir().expect("dir");
    fill(&dir, 30).await;
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config(8)).expect("reopen");
    // Warm every sealed segment, indexes included.
    assert_eq!(read_from(&log, 0).await.len(), 30);

    // Cut into a sealed segment, which becomes the active one again, then
    // write different records over the cut and roll past it.
    let segments = log.segments();
    let cut = segments[1].base_offset + 1;
    log.truncate(cut).await.expect("truncate");
    for i in 0..30 {
        log.append(&[AppendRecord {
            payload: Bytes::from(format!("again-{i:03}-with-a-longer-payload")),
            timestamp_micros: 1,
            mark: Default::default(),
            publisher: None,
        }])
        .await
        .expect("append");
    }

    let values = read_from(&log, 0).await;
    assert_eq!(values.len() as u64, cut + 30);
    assert_eq!(values[cut as usize - 1], format!("value-{:03}", cut - 1));
    assert_eq!(values[cut as usize], "again-000-with-a-longer-payload");
    assert_eq!(
        values.last().expect("last"),
        "again-029-with-a-longer-payload"
    );
}
