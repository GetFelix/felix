//! Membership and counter records: v9 only, refused by a normal append, and
//! read back at their own offsets so a reader skipping them sees a gap.

use super::*;
use crate::log::RecordMark;
use crate::segment::{GENERATION_VERSION, read_segment_header, segment_file_name};

const V9_KINDS: [RecordMark; 3] = [
    RecordMark::Membership,
    RecordMark::CounterDelta,
    RecordMark::CounterCheckpoint,
];

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

/// Nothing writes the v9 kinds yet. An append holding one is refused before
/// it can roll the log onto v9, alone or behind other records, and the log
/// takes ordinary appends afterwards.
#[tokio::test]
async fn an_append_refuses_the_v9_kinds_without_moving_the_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");
    let before = versions(&dir, &log);
    for mark in V9_KINDS {
        for batch in [
            vec![AppendRecord {
                mark,
                ..record("x")
            }],
            vec![
                record("y"),
                AppendRecord {
                    mark,
                    ..record("x")
                },
            ],
        ] {
            assert!(
                matches!(
                    log.append(&batch).await.expect_err("v9 kind"),
                    StorageError::Unsupported(_)
                ),
                "{mark:?}"
            );
        }
    }
    assert_eq!(log.tail_offset().await.expect("tail"), 1);
    assert_eq!(versions(&dir, &log), before);
    log.append(&records(&["b"])).await.expect("append");
    assert_eq!(read_all(&log, 0).await, ["a", "b"]);
}

/// In a v9 segment the kinds hold consecutive offsets between ordinary
/// records, a read returns them with their marks, and a reopen scans them as
/// valid records.
#[tokio::test]
async fn the_v9_kinds_keep_their_offsets_and_survive_a_reopen() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");
    let mut batch = vec![record("b")];
    batch.extend(V9_KINDS.map(|mark| AppendRecord {
        mark,
        ..record("x")
    }));
    batch.push(record("c"));
    let appended = log.append_v9_for_tests(&batch, 2).await.expect("v9 append");
    assert_eq!((appended.first_offset, appended.last_offset), (1, 5));
    assert_eq!(versions(&dir, &log).last(), Some(&GENERATION_VERSION));

    let expected = vec![
        (0, RecordMark::None),
        (1, RecordMark::None),
        (2, RecordMark::Membership),
        (3, RecordMark::CounterDelta),
        (4, RecordMark::CounterCheckpoint),
        (5, RecordMark::None),
    ];
    let marks = |records: Vec<LogRecord>| -> Vec<(Offset, RecordMark)> {
        records.into_iter().map(|r| (r.offset, r.mark)).collect()
    };
    let page = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    assert_eq!(page.next, 6);
    assert_eq!(marks(page.records), expected);
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    let page = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    assert_eq!(marks(page.records), expected);
}
