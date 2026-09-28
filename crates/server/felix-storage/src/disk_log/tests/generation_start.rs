//! Generation-start records: stored with their mark, and only in a v4 segment.

use super::*;
use crate::log::RecordMark;

fn generation_start(generation: u64) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(&generation.to_be_bytes()),
        timestamp_micros: 1,
        mark: RecordMark::GenerationStart,
    }
}

async fn marks(log: &DiskLog) -> Vec<RecordMark> {
    log.read_range(ReadRange {
        start: 0,
        max_bytes: 1 << 20,
    })
    .await
    .expect("read")
    .iter()
    .map(|record| record.mark)
    .collect()
}

/// A segment from a build that predates the record is rolled before one goes
/// in, so that build refuses the new segment instead of reading the flag bit
/// as a length and cutting the record off as a torn tail.
#[tokio::test]
async fn a_generation_start_is_never_written_into_a_v3_segment() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["old"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let active = dir.path().join(crate::segment::segment_file_name(0));
    let mut bytes = std::fs::read(&active).expect("read");
    let mut header = crate::segment::SegmentHeader::decode(&bytes).expect("header");
    header.version = 3;
    bytes[..crate::segment::SEGMENT_HEADER_LEN as usize].copy_from_slice(&header.encode());
    std::fs::write(&active, &bytes).expect("write");

    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["unmarked"])).await.expect("append");
    assert_eq!(log.segments().len(), 1, "an unmarked record needs no roll");
    log.append(&[generation_start(7)]).await.expect("append");
    assert_eq!(log.segments().len(), 2, "the start record rolled first");
    log.append(&records(&["new"])).await.expect("append");
    log.shutdown().await.expect("shutdown");
    drop(log);

    // Recovery reads the mark back from the v4 segment.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        marks(&log).await,
        vec![
            RecordMark::None,
            RecordMark::None,
            RecordMark::GenerationStart,
            RecordMark::None
        ]
    );
}
