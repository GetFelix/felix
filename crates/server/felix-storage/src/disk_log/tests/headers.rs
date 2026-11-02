//! Records stored with a key and headers: only in a v8 segment, and read
//! back after a restart.

use super::*;
use crate::log::RecordMark;

/// A log moves onto a v8 segment only for the first record with headers, so
/// a build that predates them reads everything before it.
#[tokio::test]
async fn the_first_record_with_headers_rolls_the_log_and_survives_a_restart() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["before"])).await.expect("append");
    let segments = log.segments().len();
    let headers = Bytes::from_static(b"encoded key and headers");
    log.append(&[AppendRecord {
        payload: Bytes::from_static(b"keyed"),
        timestamp_micros: 1,
        mark: RecordMark::None,
        publisher: Some(Bytes::from_static(b"alice")),
        headers: Some(Box::new(headers.clone())),
    }])
    .await
    .expect("append");
    assert_eq!(log.segments().len(), segments + 1, "rolled first");
    log.append(&records(&["after"])).await.expect("append");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    let read: Vec<_> = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: 1 << 20,
        })
        .await
        .expect("read")
        .records
        .into_iter()
        .map(|record| (record.payload, record.headers.map(|headers| *headers)))
        .collect();
    assert_eq!(
        read,
        vec![
            (Bytes::from_static(b"before"), None),
            (Bytes::from_static(b"keyed"), Some(headers)),
            (Bytes::from_static(b"after"), None),
        ]
    );
}

/// A record refused for its size is refused before the log rolls, so the log
/// does not move onto v8 for a record it never stored.
#[tokio::test]
async fn an_oversized_record_with_headers_does_not_roll_the_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["before"])).await.expect("append");
    let segments = log.segments().len();
    let err = log
        .append(&[AppendRecord {
            payload: Bytes::from(vec![
                0u8;
                crate::segment::format::MAX_PAYLOAD_BYTES as usize - 4
            ]),
            timestamp_micros: 1,
            mark: RecordMark::None,
            publisher: None,
            headers: Some(Box::new(Bytes::new())),
        }])
        .await
        .expect_err("too large");
    assert!(
        matches!(err, crate::StorageError::Unsupported(_)),
        "{err:?}"
    );
    assert_eq!(log.segments().len(), segments, "did not roll");
}
