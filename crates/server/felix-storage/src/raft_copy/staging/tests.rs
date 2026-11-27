use std::cell::Cell;

use tempfile::{TempDir, tempdir};

use super::*;
use crate::log::{AppendOnlyLog, FsyncMode, LogRecord, ReadRange, RecordMark};
use crate::raft_copy::start::{ADOPTING, adopt_staged};
use crate::raft_log::RaftLog;
use crate::raft_log::tests::{Codec, TestEntry, Types, all, blank, normal, store};
use crate::segment::parse_segment_file_name;

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    }
}

const BASE: EpochLogId = EpochLogId {
    index: 5,
    epoch: 0,
    term: 3,
};

/// A source log of a blank and records 1..=5 across two terms, and its
/// entries.
async fn source() -> (TempDir, Vec<TestEntry>) {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let raft = RaftLog::new(log, Codec).expect("a Raft log");
    let entries = vec![
        blank(2, 0),
        normal(2, 1, "one"),
        normal(2, 2, "headers:two"),
        blank(3, 3),
        normal(3, 4, "four"),
        normal(3, 5, "five"),
    ];
    store(&raft, &entries).await.expect("store");
    let read = all(&raft).await.expect("entries");
    raft.disk_log().close().await.expect("close");
    (dir, read)
}

/// Stage `entries[1..]` in `copy` in two chunks and finish.
async fn stage(copy: &Path, entries: &[TestEntry]) {
    let mut staging = Staging::begin(copy, 1, &config()).await.expect("begin");
    // Another time than the source's: only blanks take it.
    staging
        .append::<Types, Codec>(&Codec, &entries[1..3], 5)
        .await
        .expect("first chunk");
    staging
        .append::<Types, Codec>(&Codec, &entries[3..], 5)
        .await
        .expect("second chunk");
    staging.finish(BASE).await.expect("finish");
}

async fn records(dir: &Path, start: u64) -> Vec<LogRecord> {
    let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
    let records = log
        .read_range_with_trims(ReadRange {
            start,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    log.close().await.expect("close");
    records
}

fn segments(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .expect("dir")
        .filter(|entry| {
            let name = entry.as_ref().expect("entry").file_name();
            parse_segment_file_name(name.to_str().expect("name")).is_some()
        })
        .count()
}

fn no_crash() -> io::Result<()> {
    Ok(())
}

/// A command's record comes out of the staging log byte for byte as the
/// voter holds it, with the generation that wrote it there.
#[tokio::test]
async fn a_staged_base_holds_the_sources_records_byte_for_byte() {
    let (source_dir, entries) = source().await;
    let copy = tempdir().expect("copy");
    stage(copy.path(), &entries).await;
    assert!(adopt_staged(copy.path(), (5, 3), &no_crash).expect("adopt"));
    assert!(!copy.path().join(STAGING_DIR).exists());
    assert!(!copy.path().join(ADOPTING).exists());

    let want = records(source_dir.path(), 1).await;
    let got = records(copy.path(), 1).await;
    assert_eq!(got.len(), want.len());
    for (got, want) in got.iter().zip(&want) {
        assert_eq!(got.offset, want.offset);
        assert_eq!(got.generation, want.generation, "at {}", got.offset);
        assert_eq!(got.mark, want.mark, "at {}", got.offset);
        if got.mark == RecordMark::GenerationStart {
            assert_eq!(got.timestamp_micros, 5);
            continue;
        }
        assert_eq!(got.timestamp_micros, want.timestamp_micros);
        assert_eq!(got.payload, want.payload);
        assert_eq!(got.headers, want.headers);
        assert_eq!(got.checksum, want.checksum, "at {}", got.offset);
    }
}

/// A crash before the marker leaves the copy blank and the staged base
/// where it was; one after it is rolled forward to the whole base.
#[tokio::test]
async fn a_crash_in_the_adoption_leaves_the_copy_blank_or_rolls_forward() {
    let (source_dir, entries) = source().await;
    let want = records(source_dir.path(), 1).await;
    for crash_at in 1..=4 {
        let copy = tempdir().expect("copy");
        stage(copy.path(), &entries).await;
        let steps = Cell::new(0);
        let crash = || {
            steps.set(steps.get() + 1);
            if steps.get() == crash_at {
                return Err(io::Error::other("crash"));
            }
            Ok(())
        };
        adopt_staged(copy.path(), (5, 3), &crash).expect_err("crashed");
        let marked = copy.path().join(ADOPTING).exists();
        if crash_at == 1 {
            assert!(!marked);
            assert_eq!(segments(copy.path()), 0, "a blank copy");
        } else {
            assert!(marked, "crash at step {crash_at}");
        }
        assert!(adopt_staged(copy.path(), (5, 3), &no_crash).expect("adopt"));
        let got = records(copy.path(), 1).await;
        assert_eq!(
            got.iter().map(|record| record.offset).collect::<Vec<_>>(),
            want.iter().map(|record| record.offset).collect::<Vec<_>>(),
            "crash at step {crash_at}"
        );
    }
}

/// An unfinished staging log, or one of another base, is never adopted.
#[tokio::test]
async fn an_unfinished_or_other_base_is_discarded() {
    let (_source, entries) = source().await;
    let copy = tempdir().expect("copy");
    let mut staging = Staging::begin(copy.path(), 1, &config())
        .await
        .expect("begin");
    staging
        .append::<Types, Codec>(&Codec, &entries[1..3], 5)
        .await
        .expect("chunk");
    drop(staging);
    assert!(!adopt_staged(copy.path(), (5, 3), &no_crash).expect("adopt"));
    assert!(!copy.path().join(STAGING_DIR).exists());
    assert_eq!(segments(copy.path()), 0);

    stage(copy.path(), &entries).await;
    assert!(!adopt_staged(copy.path(), (5, 4), &no_crash).expect("adopt"));
    assert_eq!(segments(copy.path()), 0);
}

/// A chunk that does not start where the log ends is refused, and a
/// restart at a raised floor empties the log.
#[tokio::test]
async fn a_chunk_out_of_place_is_refused_and_a_restart_empties() {
    let (_source, entries) = source().await;
    let copy = tempdir().expect("copy");
    let mut staging = Staging::begin(copy.path(), 1, &config())
        .await
        .expect("begin");
    let skipped = staging
        .append::<Types, Codec>(&Codec, &entries[2..3], 5)
        .await;
    assert!(skipped.is_err());
    staging
        .append::<Types, Codec>(&Codec, &entries[1..3], 5)
        .await
        .expect("chunk");
    staging.restart(4).await.expect("restart");
    assert_eq!(staging.next(), 4);
    staging
        .append::<Types, Codec>(&Codec, &entries[4..], 5)
        .await
        .expect("after the restart");
    staging.finish(BASE).await.expect("finish");
    assert!(adopt_staged(copy.path(), (5, 3), &no_crash).expect("adopt"));
    let got = records(copy.path(), 4).await;
    assert_eq!(
        got.iter().map(|record| record.offset).collect::<Vec<_>>(),
        vec![4, 5]
    );
}
