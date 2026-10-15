//! Commit records: stored with their mark, and only in a v5 segment.

use super::*;
use crate::log::RecordMark;

fn commit(payload: &'static str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(payload.as_bytes()),
        timestamp_micros: 1,
        mark: RecordMark::Commit,
        publisher: None,
    }
}

/// A v4 segment is rolled before a commit record goes in, so a build that
/// predates the record refuses the new segment instead of reading the flag
/// bit as a length. Records after it stay in the v5 segment, and recovery
/// reads the mark back.
#[tokio::test]
async fn a_commit_is_never_written_into_a_v4_segment() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&[AppendRecord {
        payload: Bytes::copy_from_slice(&3u64.to_be_bytes()),
        timestamp_micros: 1,
        mark: RecordMark::GenerationStart,
        publisher: None,
    }])
    .await
    .expect("append");
    let segments = log.segments().len();
    log.append(&[commit("c")]).await.expect("append");
    assert_eq!(
        log.segments().len(),
        segments + 1,
        "the commit rolled first"
    );
    log.append(&records(&["after"])).await.expect("append");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    let marks: Vec<_> = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: 1 << 20,
        })
        .await
        .expect("read")
        .iter()
        .map(|record| record.mark)
        .collect();
    assert_eq!(
        marks,
        vec![
            RecordMark::GenerationStart,
            RecordMark::Commit,
            RecordMark::None
        ]
    );
}
