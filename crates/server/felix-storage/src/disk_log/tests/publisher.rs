//! Records stored with their publisher: only in a v6 segment, and read back
//! after a restart.

use super::*;
use crate::log::RecordMark;

fn published(payload: &'static str, publisher: &'static str, mark: RecordMark) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(payload.as_bytes()),
        timestamp_micros: 1,
        mark,
        publisher: Some(Bytes::from_static(publisher.as_bytes())),
    }
}

/// A log moves onto a v6 segment only for the first record with a publisher,
/// so a build that predates them reads everything before it.
#[tokio::test]
async fn the_first_published_record_rolls_the_log_and_survives_a_restart() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["before"])).await.expect("append");
    let segments = log.segments().len();
    let opens = RecordMark::for_batch(5, 0, 2).collect::<Vec<_>>();
    log.append(&[
        published("a", "alice", opens[0]),
        published("b", "alice", opens[1]),
    ])
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
        .into_iter()
        .map(|record| (record.payload, record.publisher, record.mark))
        .collect();
    let alice = Some(Bytes::from_static(b"alice"));
    assert_eq!(
        read,
        vec![
            (Bytes::from_static(b"before"), None, RecordMark::None),
            (Bytes::from_static(b"a"), alice.clone(), opens[0]),
            (Bytes::from_static(b"b"), alice, opens[1]),
            (Bytes::from_static(b"after"), None, RecordMark::None),
        ]
    );
    // The batch's digest is over its payloads alone, so the recovered
    // producer state recognises a re-send of it.
    let crate::disk_log::ProducerSequence::Held { digest, .. } = log.producer_sequence(5, 0) else {
        panic!("the batch is held");
    };
    assert_eq!(digest, Some(crate::log::PayloadDigest::of(["a", "b"])));
}

#[tokio::test]
async fn a_publisher_past_the_limit_is_refused() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let long = Bytes::from(vec![b'p'; crate::segment::format::MAX_PUBLISHER_BYTES + 1]);
    let err = log
        .append(&[AppendRecord {
            payload: Bytes::from_static(b"x"),
            timestamp_micros: 1,
            mark: RecordMark::None,
            publisher: Some(long),
        }])
        .await
        .expect_err("too long");
    assert!(
        matches!(err, crate::StorageError::Unsupported(_)),
        "{err:?}"
    );
}
