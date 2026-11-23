//! A gap: the log goes on past its tail and keeps every record below.

use super::*;
use crate::CorruptionKind;
use crate::segment::segment_file_name;

/// Wider than any u32 offset, so the header's low bits are all it can
/// check.
const FIRST: Offset = (1 << 40) + 7;

/// `n` records, then a gap to [`FIRST`].
async fn gapped(dir: &TempDir, n: usize) -> DiskLog {
    let log = open(dir, FsyncMode::OnCommit);
    for i in 0..n {
        log.append(&records(&[&format!("old-{i}")]))
            .await
            .expect("append");
    }
    log.open_gap(FIRST).await.expect("gap");
    log
}

fn segment_ids(dir: &TempDir) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|entry| {
            crate::segment::parse_segment_file_name(entry.ok()?.file_name().to_str()?)
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn appends_go_on_past_a_gap_and_reads_step_over_it() {
    let dir = tempdir().expect("dir");
    let log = gapped(&dir, 6).await;
    assert_eq!(log.base_offset(), 0);
    assert_eq!(log.tail_offset().await.expect("tail"), FIRST);
    assert_eq!(log.durable_offset(), FIRST);

    let appended = log
        .append(&records(&["new-0", "new-1"]))
        .await
        .expect("append");
    assert_eq!(appended.first_offset, FIRST);

    let old: Vec<String> = (0..6).map(|i| format!("old-{i}")).collect();
    let mut all = old.clone();
    all.extend(["new-0".to_string(), "new-1".to_string()]);
    assert_eq!(read_all(&log, 0).await, all);
    // From the last old record, and from inside the gap.
    assert_eq!(read_all(&log, 5).await, all[5..]);
    assert_eq!(read_all(&log, 6).await, ["new-0", "new-1"]);
    assert_eq!(read_all(&log, FIRST - 1).await, ["new-0", "new-1"]);
    let offsets: Vec<Offset> = log
        .read_range(ReadRange {
            start: 4,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read")
        .records
        .iter()
        .map(|r| r.offset)
        .collect();
    assert_eq!(offsets, [4, 5, FIRST, FIRST + 1]);
}

#[tokio::test]
async fn a_gap_reopens_with_the_records_on_both_sides() {
    let dir = tempdir().expect("dir");
    {
        let log = gapped(&dir, 6).await;
        log.append(&records(&["new-0"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.base_offset(), 0);
    assert_eq!(log.tail_offset().await.expect("tail"), FIRST + 1);
    let read = read_all(&log, 0).await;
    assert_eq!(read.len(), 7);
    assert_eq!(read[5], "old-5");
    assert_eq!(read[6], "new-0");
    log.append(&records(&["new-1"])).await.expect("append");
    assert_eq!(read_all(&log, 6).await, ["new-0", "new-1"]);
}

/// The process dies once the gap's segment exists and before anything is
/// appended to it: the log reopens at the gap's end, every old record kept.
#[tokio::test]
async fn a_crash_before_the_first_append_past_a_gap_keeps_the_gap() {
    let dir = tempdir().expect("dir");
    drop(gapped(&dir, 6).await);
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), FIRST);
    assert_eq!(read_all(&log, 0).await.len(), 6);
    let appended = log.append(&records(&["new-0"])).await.expect("append");
    assert_eq!(appended.first_offset, FIRST);
}

/// The process dies while the gap's segment is created, before its header
/// is on disk: the log reopens as it was, and the gap can be opened again.
#[tokio::test]
async fn a_crash_before_the_gaps_header_is_written_reopens_without_it() {
    let dir = tempdir().expect("dir");
    drop(gapped(&dir, 6).await);
    let newest = *segment_ids(&dir).last().expect("a segment");
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join(segment_file_name(newest)))
        .expect("open segment")
        .set_len(0)
        .expect("cut header");

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 0).await.len(), 6);
    log.open_gap(FIRST).await.expect("gap again");
    log.append(&records(&["new-0"])).await.expect("append");
    assert_eq!(read_all(&log, 5).await, ["old-5", "new-0"]);
}

/// The header names where its gap starts, so a segment lost just below it
/// is still a missing segment, not a longer gap.
#[tokio::test]
async fn a_segment_missing_below_a_gap_is_refused() {
    let dir = tempdir().expect("dir");
    {
        let log = gapped(&dir, 6).await;
        log.append(&records(&["new-0"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let ids = segment_ids(&dir);
    assert!(ids.len() >= 3, "too few segments to lose one: {ids:?}");
    let lost = ids[ids.len() - 2];
    std::fs::remove_file(dir.path().join(segment_file_name(lost))).expect("remove");

    let err = DiskLog::open(dir.path(), "t/ns/s/0", config(FsyncMode::OnCommit))
        .expect_err("a lost segment must not open");
    assert!(
        matches!(
            &err,
            StorageError::Corruption(c) if matches!(c.kind, CorruptionKind::OffsetOutOfOrder { .. })
        ),
        "{err}"
    );
}

#[tokio::test]
async fn a_gap_must_lie_past_the_tail() {
    let dir = tempdir().expect("dir");
    let log = gapped(&dir, 2).await;
    for first in [FIRST - 1, FIRST] {
        assert!(matches!(
            log.open_gap(first).await,
            Err(StorageError::OutsideLog { .. })
        ));
    }
    // An empty segment past a gap cannot take a second one: rolling would
    // delete it and leave the next with nothing it follows.
    assert!(matches!(
        log.open_gap(FIRST + 10).await,
        Err(StorageError::OutsideLog { .. })
    ));
    log.append(&records(&["new-0"])).await.expect("append");
    log.open_gap(FIRST + 10).await.expect("a second gap");
    log.append(&records(&["new-1"])).await.expect("append");
    assert_eq!(
        read_all(&log, 0).await,
        ["old-0", "old-1", "new-0", "new-1"]
    );
    drop(log);
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 1).await, ["old-1", "new-0", "new-1"]);
}

/// A restore's default gap is 2^40 offsets. The gap is a segment header,
/// not space: the directory, sparse indexes included, stays as small as the
/// records on both sides, before and after a reopen.
#[cfg(unix)]
#[tokio::test]
async fn a_restores_gap_costs_no_disk() {
    use std::os::unix::fs::MetadataExt;

    let used = |dir: &TempDir| -> (u64, u64) {
        std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|entry| entry.expect("entry").metadata().expect("metadata"))
            .filter(|meta| meta.is_file())
            .fold((0, 0), |(len, blocks), meta| {
                (len + meta.len(), blocks + meta.blocks() * 512)
            })
    };
    let dir = tempdir().expect("dir");
    let log = gapped(&dir, 6).await;
    log.append(&records(&["new-0"])).await.expect("append");
    log.shutdown().await.expect("shutdown");
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 0).await.len(), 7);
    assert_eq!(read_all(&log, FIRST - (1 << 40)).await, ["new-0"]);
    let (len, blocks) = used(&dir);
    assert!(len < 64 * 1024, "{len} bytes of files across a 2^40 gap");
    assert!(blocks < 16 * 1024 * 1024, "{blocks} bytes allocated");
}
