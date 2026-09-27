//! Producer state derived from the log: what a restart, a truncation and
//! retention leave behind, and how much of the log an open has to read.

use super::*;
use crate::disk_log::ProducerSequence;
use crate::log::{PayloadDigest, RecordMark};

/// One idempotent batch, marked as the broker marks it.
fn marked(producer: u64, sequence: u64, payloads: &[&str]) -> Vec<AppendRecord> {
    payloads
        .iter()
        .zip(RecordMark::for_batch(producer, sequence, payloads.len()))
        .map(|(payload, mark)| AppendRecord {
            mark,
            ..record(payload)
        })
        .collect()
}

/// Enough batches from producer 1 to roll several segments; returns the
/// offsets batch `i` landed at.
async fn fill(log: &DiskLog, batches: u64) -> Vec<(u64, u64)> {
    let mut landed = Vec::new();
    for sequence in 0..batches {
        let result = log
            .append(&marked(1, sequence, FILLED))
            .await
            .expect("append");
        landed.push((result.first_offset, result.last_offset));
    }
    landed
}

/// The payloads of every batch [`fill`] writes.
const FILLED: &[&str] = &["a", "b"];

fn digest(payloads: &[&str]) -> Option<PayloadDigest> {
    Some(PayloadDigest::of(payloads))
}

fn snapshot_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path()
        .join(crate::disk_log::producers::snapshot_file_name())
}

#[tokio::test]
async fn marks_are_read_back_with_their_records() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["plain"])).await.expect("append");
    log.append(&marked(5, 0, &["x", "y"]))
        .await
        .expect("append");

    let read = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    let marks: Vec<RecordMark> = read.iter().map(|record| record.mark).collect();
    let expected: Vec<RecordMark> = std::iter::once(RecordMark::None)
        .chain(RecordMark::for_batch(5, 0, 2))
        .collect();
    assert_eq!(marks, expected);
    assert_eq!(read[2].payload.as_ref(), b"y");
}

#[tokio::test]
async fn a_producer_is_known_again_after_a_restart() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&marked(5, 0, &["a"])).await.expect("append");
        log.append(&marked(5, 1, &["b", "c"]))
            .await
            .expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        log.producer_sequence(5, 1),
        ProducerSequence::Held {
            first: 1,
            last: 2,
            digest: digest(&["b", "c"]),
        }
    );
    assert_eq!(log.producer_sequence(5, 2), ProducerSequence::Next);
}

/// The snapshot saved at each rollover is what keeps an open from reading the
/// whole log: with it current, the sealed segments are not read at all, which
/// shows here as a damaged sealed record going unnoticed. Without it the open
/// has to read them, and finds the damage.
#[tokio::test]
async fn with_a_current_snapshot_an_open_reads_only_the_active_segment() {
    let dir = tempdir().expect("dir");
    let landed = {
        let log = open(&dir, FsyncMode::OnCommit);
        let landed = fill(&log, 12).await;
        assert!(log.segments().len() > 3, "expected rollovers");
        log.shutdown().await.expect("shutdown");
        landed
    };
    assert!(snapshot_path(&dir).exists(), "a rollover saves a snapshot");

    // Damage the first record's payload in the oldest segment. Recovery only
    // checks a sealed segment's last records, so only a read reaches it.
    let oldest = dir.path().join(crate::segment::segment_file_name(0));
    let mut bytes = std::fs::read(&oldest).expect("read segment");
    let payload_at = (crate::segment::SEGMENT_HEADER_LEN
        + crate::segment::RECORD_HEADER_LEN
        + crate::segment::PRODUCER_TAG_LEN) as usize;
    bytes[payload_at] ^= 0xff;
    std::fs::write(&oldest, &bytes).expect("write segment");

    let log = open(&dir, FsyncMode::OnCommit);
    let (first, last) = landed[11];
    assert_eq!(
        log.producer_sequence(1, 11),
        ProducerSequence::Held {
            first,
            last,
            digest: digest(FILLED),
        }
    );
    assert_eq!(log.producer_sequence(1, 12), ProducerSequence::Next);
    drop(log);

    std::fs::remove_file(snapshot_path(&dir)).expect("remove snapshot");
    let err = DiskLog::open(dir.path(), "t/ns/s/0", config(FsyncMode::OnCommit))
        .expect_err("without a snapshot the open reads the damaged segment");
    assert!(
        matches!(err, StorageError::Corruption(_)),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn without_a_snapshot_the_state_is_rebuilt_from_every_segment() {
    let dir = tempdir().expect("dir");
    let landed = {
        let log = open(&dir, FsyncMode::OnCommit);
        let landed = fill(&log, 12).await;
        log.shutdown().await.expect("shutdown");
        landed
    };
    std::fs::remove_file(snapshot_path(&dir)).expect("remove snapshot");

    let log = open(&dir, FsyncMode::OnCommit);
    for (sequence, (first, last)) in landed.iter().enumerate() {
        assert_eq!(
            log.producer_sequence(1, sequence as u64),
            ProducerSequence::Held {
                first: *first,
                last: *last,
                digest: digest(FILLED),
            }
        );
    }
    assert_eq!(log.producer_sequence(1, 12), ProducerSequence::Next);
}

/// A truncation takes the batches it removes out of the producer's history,
/// and the snapshot with them: kept, it would describe records that were
/// replaced.
#[tokio::test]
async fn a_truncation_forgets_the_batches_it_removes() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let landed = fill(&log, 12).await;
    assert!(snapshot_path(&dir).exists());

    // Cut inside batch 3, below the snapshot's offset.
    let (first, _) = landed[3];
    log.truncate(first + 1).await.expect("truncate");
    assert!(!snapshot_path(&dir).exists());
    assert_eq!(
        log.producer_sequence(1, 3),
        ProducerSequence::Partial {
            first,
            held: 1,
            len: 2,
            digest: digest(&FILLED[..1]),
        }
    );

    // Another producer's record lands where the rest of batch 3 would have.
    log.append(&marked(2, 0, &["other"])).await.expect("append");
    assert_eq!(log.producer_sequence(1, 3), ProducerSequence::Next);
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.producer_sequence(1, 3), ProducerSequence::Next);
    assert_eq!(
        log.producer_sequence(1, 2),
        ProducerSequence::Held {
            first: landed[2].0,
            last: landed[2].1,
            digest: digest(FILLED),
        }
    );
    assert_eq!(log.producer_sequence(2, 1), ProducerSequence::Next);
}

/// A batch whose records arrive in two appends -- a replica receiving a
/// shipment cut mid-batch -- is partial in between, including across a
/// restart, and held once the rest arrives.
#[tokio::test]
async fn a_batch_split_across_appends_is_partial_until_complete() {
    let dir = tempdir().expect("dir");
    let mut batch = marked(4, 0, &["a", "b", "c"]);
    let rest = batch.split_off(1);
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&batch).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        log.producer_sequence(4, 0),
        ProducerSequence::Partial {
            first: 0,
            held: 1,
            len: 3,
            digest: digest(&["a"]),
        }
    );
    log.append(&rest).await.expect("append");
    assert_eq!(
        log.producer_sequence(4, 0),
        ProducerSequence::Held {
            first: 0,
            last: 2,
            digest: digest(&["a", "b", "c"]),
        }
    );
}

#[tokio::test]
async fn retention_forgets_a_producer_once_its_batches_are_gone() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        retention_bytes: Some(400),
        retention_check_interval: Duration::from_secs(3600),
        ..config(FsyncMode::OnCommit)
    };
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config.clone()).expect("open");
    log.append(&marked(9, 0, &["early"])).await.expect("append");
    fill(&log, 12).await;
    assert_eq!(log.producer_sequence(9, 1), ProducerSequence::Next);

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(outcome.segments_deleted > 0);
    assert_eq!(log.producer_sequence(9, 1), ProducerSequence::Unknown);
    assert_eq!(log.producer_sequence(1, 12), ProducerSequence::Next);
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = DiskLog::open(dir.path(), "t/ns/s/0", config).expect("reopen");
    assert_eq!(log.producer_sequence(9, 1), ProducerSequence::Unknown);
    assert_eq!(log.producer_sequence(1, 12), ProducerSequence::Next);
}

/// A segment written by a build that could not store marks is rolled before
/// a marked record goes in, so a downgrade refuses the new segment outright
/// instead of reading its marks as damage.
#[tokio::test]
async fn a_marked_batch_is_never_written_into_a_v2_segment() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["old"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    // Rewrite the active segment's header as v2.
    let active = dir.path().join(crate::segment::segment_file_name(0));
    let mut bytes = std::fs::read(&active).expect("read");
    let mut header = crate::segment::SegmentHeader::decode(&bytes).expect("header");
    header.version = 2;
    bytes[..crate::segment::SEGMENT_HEADER_LEN as usize].copy_from_slice(&header.encode());
    std::fs::write(&active, &bytes).expect("write");

    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["unmarked"])).await.expect("append");
    assert_eq!(log.segments().len(), 1, "an unmarked record needs no roll");
    log.append(&marked(3, 0, &["new"])).await.expect("append");
    assert_eq!(log.segments().len(), 2, "the marked batch rolled first");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(
        log.producer_sequence(3, 0),
        ProducerSequence::Held {
            first: 2,
            last: 2,
            digest: digest(&["new"]),
        }
    );
    assert_eq!(read_all(&log, 0).await, vec!["old", "unmarked", "new"]);
}

/// The rest of a partial batch is written only while the batch is still the
/// last thing in the log; once anything follows it, it can never be finished.
#[tokio::test]
async fn the_rest_of_a_batch_is_written_only_while_it_is_open_at_the_tail() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let mut batch = marked(4, 0, &["a", "b", "c"]);
    let rest = batch.split_off(1);
    log.append(&batch).await.expect("append");

    let pending = log
        .continue_pending(4, 0, &rest[..1])
        .await
        .expect("continue")
        .expect("open at the tail");
    log.commit(&pending).await.expect("commit");
    assert_eq!(
        log.producer_sequence(4, 0),
        ProducerSequence::Partial {
            first: 0,
            held: 2,
            len: 3,
            digest: digest(&["a", "b"]),
        }
    );

    log.append(&records(&["someone else"]))
        .await
        .expect("append");
    let refused = log
        .continue_pending(4, 0, &rest[1..])
        .await
        .expect("continue");
    assert!(refused.is_none());
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
    assert_eq!(log.producer_sequence(4, 0), ProducerSequence::Unknown);
}

/// A log written before marks existed has no snapshot and cannot hold a mark,
/// so opening it reads nothing more than recovery always did: a damaged
/// sealed record goes unnoticed, as it did before.
#[tokio::test]
async fn a_log_written_before_marks_is_not_read_to_rebuild_producer_state() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..20 {
            log.append(&records(&[&format!("value-{i:02}")]))
                .await
                .expect("append");
        }
        assert!(log.segments().len() > 3, "expected rollovers");
        log.shutdown().await.expect("shutdown");
    }
    std::fs::remove_file(snapshot_path(&dir)).expect("remove snapshot");
    // Every segment as a v2 build wrote it, and a damaged record in the oldest.
    let mut ids: Vec<u64> = std::fs::read_dir(dir.path())
        .expect("list")
        .filter_map(|entry| {
            let name = entry.expect("entry").file_name();
            name.to_str()?.strip_suffix(".log")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    for id in &ids {
        let path = dir.path().join(crate::segment::segment_file_name(*id));
        let mut bytes = std::fs::read(&path).expect("read");
        let mut header = crate::segment::SegmentHeader::decode(&bytes).expect("header");
        header.version = 2;
        bytes[..crate::segment::SEGMENT_HEADER_LEN as usize].copy_from_slice(&header.encode());
        if *id == ids[0] {
            let payload_at =
                (crate::segment::SEGMENT_HEADER_LEN + crate::segment::RECORD_HEADER_LEN) as usize;
            bytes[payload_at] ^= 0xff;
        }
        std::fs::write(&path, &bytes).expect("write");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.producer_sequence(1, 0), ProducerSequence::Unknown);
    assert_eq!(log.tail_offset().await.expect("tail"), 20);
}
