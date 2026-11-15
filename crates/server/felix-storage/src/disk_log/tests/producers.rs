//! Producer state derived from the log: what a restart, a truncation and
//! retention leave behind, and how much of the log an open has to read.

use super::*;
use crate::disk_log::{ProducerSequence, UnfinishedBatch};
use crate::log::{BatchPiece, PayloadDigest, ProducerBatch, RecordMark};

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
        .expect("read")
        .records;
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

    let order = std::sync::Arc::new(crate::CommitSequencer::new(1));
    let (pending, _turn) = log
        .continue_claimed(4, 0, &rest[..1], &order)
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
        .continue_claimed(4, 0, &rest[1..], &order)
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

/// The record at `position` of producer 9's batch `sequence` of `len`,
/// marked as a Raft log marks it.
fn piece(sequence: u64, position: u32, len: u32, payload: &str) -> AppendRecord {
    let mark = match position {
        0 => RecordMark::Opens(ProducerBatch {
            producer_id: 9,
            sequence,
            len,
        }),
        _ => RecordMark::Piece(BatchPiece {
            producer_id: 9,
            sequence,
            position,
            len,
        }),
    };
    AppendRecord {
        mark,
        ..record(payload)
    }
}

fn raft_config() -> LogConfig {
    LogConfig {
        raft_log: true,
        ..config(FsyncMode::OnCommit)
    }
}

/// Lead the log at a new generation, as a Raft leader does before it serves:
/// a Raft log refuses an append from a writer it does not know.
async fn lead(log: &DiskLog) {
    let generation = log.accepted_generation() + 1;
    log.accept_generation(generation, Some("a"))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
    log.lead_at(generation);
}

/// The producer state the snapshot file holds and the offset it is as of.
fn saved(dir: &TempDir) -> (crate::disk_log::producers::ProducerState, Offset) {
    crate::disk_log::producers::load(dir.path()).expect("a snapshot")
}

/// A batch another record cut is still known after a restart, whether the
/// open reads it from the snapshot or from the records, and the pieces it
/// was owed then finish it.
#[tokio::test]
async fn a_cut_batch_is_known_again_after_a_restart() {
    let dir = tempdir().expect("dir");
    let cut = Some(UnfinishedBatch {
        first: Some(0),
        held: 2,
        len: 3,
        digest: digest(&["a", "b"]),
    });
    {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
        lead(&log).await;
        log.append(&[piece(0, 0, 3, "a"), piece(0, 1, 3, "b")])
            .await
            .expect("append");
        // Enough after the cut to roll, so a snapshot is saved past it.
        for _ in 0..6 {
            log.append(&records(FILLED)).await.expect("append");
        }
        assert_eq!(log.producer_unfinished(9, 0), cut);
        log.shutdown().await.expect("shutdown");
    }
    let (snapshot, as_of) = saved(&dir);
    assert!(as_of > 2, "as of {as_of}");
    assert_eq!(snapshot.unfinished(9, 0), cut);
    for snapshot in [true, false] {
        if !snapshot {
            std::fs::remove_file(snapshot_path(&dir)).expect("remove");
        }
        let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
        lead(&log).await;
        assert_eq!(log.producer_unfinished(9, 0), cut, "snapshot: {snapshot}");
        assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Unknown);
        log.shutdown().await.expect("shutdown");
    }

    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    lead(&log).await;
    let last = log.append(&[piece(0, 2, 3, "c")]).await.expect("append");
    assert_eq!(log.producer_unfinished(9, 0), None);
    assert_eq!(
        log.producer_sequence(9, 0),
        ProducerSequence::Held {
            first: 0,
            last: last.last_offset,
            digest: digest(&["a", "b", "c"]),
        }
    );
}

/// A log whose head was removed past a cut batch's first record, with a
/// later record of the batch still in it, knows the batch as one it cannot
/// finish: a purge, or a copy that starts inside the batch. Forgotten, the
/// re-send would be taken as a new batch and the records still here
/// written again. A reopen reads the same from what is left.
#[tokio::test]
async fn a_cut_batch_whose_first_record_is_removed_cannot_be_finished() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
    lead(&log).await;
    log.append(&[piece(0, 0, 3, "a")]).await.expect("append");
    // Enough to seal the segment the first record is in.
    let mut tail = 0;
    for _ in 0..12 {
        tail = log
            .append(&records(FILLED))
            .await
            .expect("append")
            .last_offset;
    }
    // Written by a leader that held the first record, as the rest of it.
    let second = log.append(&[piece(0, 1, 3, "b")]).await.expect("append");
    // Enough after it to roll, so a snapshot is saved with the piece in it.
    for _ in 0..6 {
        log.append(&records(FILLED)).await.expect("append");
    }
    assert_eq!(
        log.producer_unfinished(9, 0),
        Some(UnfinishedBatch {
            first: Some(0),
            held: 2,
            len: 3,
            digest: digest(&["a", "b"]),
        })
    );

    let removed = log.trim_before(tail).await.expect("trim");
    assert!(!removed.is_empty());
    let base = log.base_offset();
    assert!(0 < base && base <= second.first_offset, "base {base}");
    let headless = Some(UnfinishedBatch {
        first: None,
        held: 2,
        len: 3,
        digest: None,
    });
    assert_eq!(log.producer_unfinished(9, 0), headless);
    log.shutdown().await.expect("shutdown");
    drop(log);

    // The snapshot was saved with the piece, before or after the trim.
    let (snapshot, as_of) = saved(&dir);
    assert!(as_of > second.first_offset, "as of {as_of}");
    assert_eq!(snapshot.unfinished(9, 0).map(|cut| cut.held), Some(2));
    for snapshot in [true, false] {
        if !snapshot {
            std::fs::remove_file(snapshot_path(&dir)).expect("remove");
        }
        let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
        lead(&log).await;
        assert_eq!(
            log.producer_unfinished(9, 0),
            headless,
            "snapshot: {snapshot}"
        );
        log.shutdown().await.expect("shutdown");
    }
}

/// A purge past every record of a cut first batch leaves its producer
/// unknown. The re-send of sequence 0 is refused, not taken as new, and a
/// reopen from the snapshot still refuses it.
#[tokio::test]
async fn a_purge_past_a_cut_batch_refuses_its_resend() {
    let dir = tempdir().expect("dir");
    let refused = Some(UnfinishedBatch {
        first: None,
        held: 0,
        len: 0,
        digest: None,
    });
    {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
        lead(&log).await;
        log.append(&[piece(0, 0, 3, "a"), piece(0, 1, 3, "b")])
            .await
            .expect("append");
        let mut tail = 0;
        for _ in 0..12 {
            tail = log
                .append(&records(FILLED))
                .await
                .expect("append")
                .last_offset;
        }
        let removed = log.trim_before(tail).await.expect("trim");
        assert!(!removed.is_empty());
        assert!(log.base_offset() > 1, "base {}", log.base_offset());
        assert_eq!(log.producer_unfinished(9, 0), refused);
        assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
        // Enough after the trim to roll, so a snapshot is saved past it.
        for _ in 0..6 {
            log.append(&records(FILLED)).await.expect("append");
        }
        log.shutdown().await.expect("shutdown");
    }
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    lead(&log).await;
    assert_eq!(log.producer_unfinished(9, 0), refused);
    assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
    log.shutdown().await.expect("shutdown");
}

const REFUSED: Option<UnfinishedBatch> = Some(UnfinishedBatch {
    first: None,
    held: 0,
    len: 0,
    digest: None,
});

/// A Raft log holding two of the three records of producer 9's batch 0,
/// purged past both.
async fn purged_past_a_cut_batch(dir: &TempDir) -> DiskLog {
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
    lead(&log).await;
    log.append(&[piece(0, 0, 3, "a"), piece(0, 1, 3, "b")])
        .await
        .expect("append");
    let mut tail = 0;
    for _ in 0..12 {
        tail = log
            .append(&records(FILLED))
            .await
            .expect("append")
            .last_offset;
    }
    assert!(!log.trim_before(tail).await.expect("trim").is_empty());
    assert!(log.base_offset() > 1, "base {}", log.base_offset());
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log
}

/// A truncation discards the snapshot and reads the records again. None of
/// them has the purged batch, and its re-send is still refused, also when
/// the producer's next batch was among the records cut.
#[tokio::test]
async fn a_truncation_after_a_purge_still_refuses_the_cut_batch() {
    let dir = tempdir().expect("dir");
    let log = purged_past_a_cut_batch(&dir).await;
    let next = log
        .append(&[piece(1, 0, 2, "c")])
        .await
        .expect("append")
        .last_offset;
    log.truncate(next).await.expect("truncate");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
    log.truncate(next - 1).await.expect("truncate");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log.shutdown().await.expect("shutdown");
}

/// A reset drops every record, the cut batch's with them. Its re-send is
/// refused after it, and after a reopen.
#[tokio::test]
async fn a_reset_still_refuses_a_cut_batch() {
    let dir = tempdir().expect("dir");
    {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
        lead(&log).await;
        log.append(&[piece(0, 0, 3, "a"), piece(0, 1, 3, "b")])
            .await
            .expect("append");
        let tail = log
            .append(&records(FILLED))
            .await
            .expect("append")
            .last_offset
            + 1;
        assert!(
            log.producer_unfinished(9, 0)
                .is_some_and(|cut| cut.held == 2)
        );
        log.reset_to(tail + 10).await.expect("reset");
        assert_eq!(log.producer_unfinished(9, 0), REFUSED);
        assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
        log.shutdown().await.expect("shutdown");
    }
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log.shutdown().await.expect("shutdown");
}

/// The snapshot cannot be saved, so a reopen reads the records. The purged
/// batch is in none of them and is refused all the same.
#[tokio::test]
async fn a_purge_refuses_the_cut_batch_without_a_snapshot() {
    let dir = tempdir().expect("dir");
    // In the way of the snapshot's temporary file.
    std::fs::create_dir(snapshot_path(&dir).with_extension("tmp")).expect("blocker");
    let log = purged_past_a_cut_batch(&dir).await;
    log.shutdown().await.expect("shutdown");
    drop(log);
    assert!(!snapshot_path(&dir).exists());

    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
    log.shutdown().await.expect("shutdown");
}

/// Appends a cut batch and enough records after it for a purge to pass it.
/// Returns the log and the last offset written.
async fn cut_then_filled(dir: &TempDir) -> (DiskLog, Offset) {
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
    lead(&log).await;
    log.append(&[piece(0, 0, 3, "a"), piece(0, 1, 3, "b")])
        .await
        .expect("append");
    let mut tail = 0;
    for _ in 0..12 {
        tail = log
            .append(&records(FILLED))
            .await
            .expect("append")
            .last_offset;
    }
    (log, tail)
}

async fn reads_from(log: &DiskLog, start: Offset) -> usize {
    log.read_range(ReadRange {
        start,
        max_bytes: 1 << 20,
    })
    .await
    .expect("read")
    .records
    .len()
}

/// A purge that cannot save the batches it forgets changes nothing, and
/// succeeds once it can.
#[tokio::test]
async fn a_purge_that_cannot_save_what_it_forgets_fails() {
    let dir = tempdir().expect("dir");
    let (log, tail) = cut_then_filled(&dir).await;
    let temporary = dir
        .path()
        .join(crate::disk_log::producers::forgotten_temporary_name());
    std::fs::create_dir(&temporary).expect("blocker");
    let segments = std::fs::read_dir(dir.path()).expect("dir").count();
    let base = log.base_offset();
    log.trim_before(tail).await.expect_err("the purge fails");
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("dir").count(),
        segments
    );
    assert_eq!(log.base_offset(), base);
    assert!(reads_from(&log, base).await > 0);
    assert!(
        log.producer_unfinished(9, 0)
            .is_some_and(|cut| cut.held == 2)
    );

    std::fs::remove_dir(&temporary).expect("remove blocker");
    assert!(!log.trim_before(tail).await.expect("trim").is_empty());
    assert!(log.base_offset() > base);
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log.shutdown().await.expect("shutdown");
}

/// A purge that forgets nothing new leaves the forgotten file alone, on a
/// Raft log and a hybrid one. A directory in its place fails any read.
#[tokio::test]
async fn a_purge_that_forgets_nothing_does_not_read_the_forgotten_file() {
    for raft in [true, false] {
        let dir = tempdir().expect("dir");
        let log = if raft {
            let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("open");
            lead(&log).await;
            log
        } else {
            open(&dir, FsyncMode::OnCommit)
        };
        let mut tail = 0;
        for sequence in 0..12 {
            tail = log
                .append(&marked(9, sequence, FILLED))
                .await
                .expect("append")
                .last_offset;
        }
        std::fs::create_dir(
            dir.path()
                .join(crate::disk_log::producers::forgotten_file_name()),
        )
        .expect("blocker");
        assert!(!log.trim_before(tail).await.expect("trim").is_empty());
        log.shutdown().await.expect("shutdown");
    }
}

/// A power loss after the forgotten batches are saved and before the files
/// go leaves a longer log. The cut batch is still not taken as new, and the
/// next purge finishes the job.
#[tokio::test]
async fn a_power_loss_between_the_save_and_the_unlinks_keeps_the_refusal() {
    let dir = tempdir().expect("dir");
    let (log, tail) = cut_then_filled(&dir).await;
    let base = log.base_offset();
    log.stop_after_unlinks(0);
    log.trim_before(tail).await.expect_err("stopped");
    drop(log);
    assert!(
        dir.path()
            .join(crate::disk_log::producers::forgotten_file_name())
            .exists()
    );

    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    assert_eq!(log.base_offset(), base);
    let after = log.producer_sequence(9, 0);
    assert!(
        !matches!(after, ProducerSequence::Unknown | ProducerSequence::Next),
        "{after:?}"
    );
    lead(&log).await;
    assert!(!log.trim_before(tail).await.expect("trim").is_empty());
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    assert_eq!(log.producer_sequence(9, 0), ProducerSequence::Expired);
    log.shutdown().await.expect("shutdown");
}

/// An open and a truncation's rebuild both refuse a damaged forgotten file:
/// without it a purged cut batch's re-send would be taken as new.
#[tokio::test]
async fn a_damaged_forgotten_file_fails_the_open_and_the_rebuild() {
    let dir = tempdir().expect("dir");
    let log = purged_past_a_cut_batch(&dir).await;
    let forgotten = dir
        .path()
        .join(crate::disk_log::producers::forgotten_file_name());
    let saved = std::fs::read(&forgotten).expect("read");
    let mut damaged = saved.clone();
    *damaged.last_mut().expect("not empty") ^= 0xFF;
    std::fs::write(&forgotten, &damaged).expect("damage");
    let next = log
        .append(&records(&["c"]))
        .await
        .expect("append")
        .last_offset;
    log.truncate(next).await.expect_err("the rebuild fails");
    drop(log);

    DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect_err("the open fails");
    std::fs::write(&forgotten, &saved).expect("restore");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", raft_config()).expect("reopen");
    assert_eq!(log.producer_unfinished(9, 0), REFUSED);
    log.shutdown().await.expect("shutdown");
}

/// Only a Raft log writes batch pieces: the hybrid's replication cannot
/// carry one.
#[tokio::test]
async fn a_batch_piece_is_refused_outside_a_raft_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let refused = log.append(&[piece(0, 1, 3, "b")]).await;
    assert!(
        matches!(refused, Err(StorageError::Unsupported(_))),
        "{refused:?}"
    );
}
