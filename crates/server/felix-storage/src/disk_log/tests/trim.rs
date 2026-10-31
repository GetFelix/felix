//! Trim records: written only when a caller appends one, only into a v7
//! segment, and left out of a normal read.

use super::*;
use crate::log::RecordMark;
use crate::segment::{TRIM_VERSION, read_segment_header, segment_file_name};

fn trim(floor: Offset) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(b"headers"),
        timestamp_micros: 1,
        mark: RecordMark::Trim { floor },
        publisher: None,
    }
}

/// Every segment's header version, oldest first.
fn versions(dir: &TempDir, log: &DiskLog) -> Vec<u16> {
    log.segments()
        .iter()
        .map(|segment| {
            read_segment_header(
                &dir.path().join(segment_file_name(segment.id)),
                segment.id,
                "t",
            )
            .expect("header")
            .version
        })
        .collect()
}

/// A log holding every other kind never reaches v7.
#[tokio::test]
async fn a_log_without_trims_never_writes_v7() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");
    let batch: Vec<_> = RecordMark::for_batch(1, 0, 2)
        .map(|mark| AppendRecord {
            mark,
            ..record("b")
        })
        .collect();
    log.append(&batch).await.expect("append");
    for mark in [RecordMark::GenerationStart, RecordMark::Commit] {
        log.append(&[AppendRecord {
            mark,
            ..record("c")
        }])
        .await
        .expect("append");
    }
    log.append(&[AppendRecord {
        publisher: Some(Bytes::from_static(b"alice")),
        ..record("d")
    }])
    .await
    .expect("append");
    let versions = versions(&dir, &log);
    assert_eq!(versions.last(), Some(&6));
    assert!(versions.iter().all(|version| *version < TRIM_VERSION));
}

/// The first trim rolls the log onto v7, a normal read leaves it out, the
/// internal read returns it from below the floor, and both hold after a
/// restart.
#[tokio::test]
async fn a_trim_rolls_onto_v7_and_is_read_only_on_request() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b"])).await.expect("append");
    let segments = log.segments().len();
    let appended = log.append(&[trim(1)]).await.expect("append");
    assert_eq!(appended.first_offset, 2);
    assert_eq!(log.segments().len(), segments + 1, "the trim rolled first");
    log.append(&records(&["c"])).await.expect("append");
    assert_eq!(versions(&dir, &log).last(), Some(&TRIM_VERSION));
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    // The trim's floor applies: offset 0 is refused, and offset 1 on reads.
    assert_eq!(log.readable_base(), 1);
    assert_eq!(read_all(&log, 1).await, vec!["b", "c"]);
    let all: Vec<_> = log
        .read_range_with_trims(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read")
        .into_iter()
        .map(|record| (record.offset, record.mark))
        .collect();
    assert_eq!(
        all,
        vec![
            (0, RecordMark::None),
            (1, RecordMark::None),
            (2, RecordMark::Trim { floor: 1 }),
            (3, RecordMark::None),
        ]
    );
    assert_eq!(log.tail_offset().await.expect("tail"), 4);
}
