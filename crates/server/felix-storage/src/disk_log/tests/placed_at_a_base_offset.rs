//! A log that begins partway through a stream.
//!
//! A replica receiving history that starts at an offset other than zero needs
//! its log to *begin* there. The records before it are gone from every copy in
//! the cluster, so such a log is complete rather than truncated, and it must
//! read back that way after a restart as well.

use super::*;

const BASE: Offset = 5_000;

#[tokio::test]
async fn a_placed_log_starts_at_the_offset_it_was_given() {
    let dir = tempdir().expect("dir");
    let log =
        DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE).expect("place");

    assert_eq!(log.base_offset(), BASE);
    assert_eq!(log.tail_offset().await.expect("tail"), BASE);
}

/// The first record takes the base offset, so the offsets a leader shipped
/// are the offsets this log stores them at.
#[tokio::test]
async fn the_first_record_takes_the_base_offset() {
    let dir = tempdir().expect("dir");
    let log =
        DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE).expect("place");

    let appended = log.append(&records(&["a", "b"])).await.expect("append");

    assert_eq!(appended.first_offset, BASE);
    assert_eq!(appended.last_offset, BASE + 1);
    let read = log
        .read_range(ReadRange {
            start: BASE,
            max_bytes: 1024,
        })
        .await
        .expect("read")
        .records;
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].offset, BASE);
    assert_eq!(read[1].offset, BASE + 1);
}

/// **The base survives a restart.** It is recorded in the segment's own
/// header, so nothing has to remember it out of band — and a base that did
/// not survive would leave the log reading back as one starting at zero,
/// which is a hole rather than a shorter log.
#[tokio::test]
async fn the_base_survives_a_restart() {
    let dir = tempdir().expect("dir");
    {
        let log = DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE)
            .expect("place");
        log.append(&records(&["a"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }

    let reopened = DiskLog::open(dir.path(), "shard", config(FsyncMode::OnCommit)).expect("reopen");

    assert_eq!(reopened.base_offset(), BASE);
    assert_eq!(reopened.tail_offset().await.expect("tail"), BASE + 1);
}

/// **An existing log is never reinterpreted.** A restart that passed a
/// different base must open the shard that is there, not move it.
#[tokio::test]
async fn an_existing_log_keeps_its_own_base() {
    let dir = tempdir().expect("dir");
    {
        let log = DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE)
            .expect("place");
        log.append(&records(&["a"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }

    let reopened =
        DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), 99_999).expect("reopen");

    assert_eq!(
        reopened.base_offset(),
        BASE,
        "an existing shard was moved to a different base",
    );
    assert_eq!(reopened.tail_offset().await.expect("tail"), BASE + 1);
}

/// Below the base is `Trimmed`, exactly as on a leader whose retention has
/// removed the same records — the follower reports the same condition the
/// leader would.
#[tokio::test]
async fn a_read_below_the_base_is_trimmed() {
    let dir = tempdir().expect("dir");
    let log =
        DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE).expect("place");
    log.append(&records(&["a"])).await.expect("append");

    let err = log
        .read_range(ReadRange {
            start: BASE - 1,
            max_bytes: 1024,
        })
        .await
        .expect_err("a read below the base should be refused");

    assert!(
        matches!(err, crate::StorageError::Trimmed { .. }),
        "expected Trimmed, got {err:?}",
    );
}

/// A base of zero is an ordinary log, so the placing path costs nothing for
/// the shards that never needed it.
#[tokio::test]
async fn a_base_of_zero_is_an_ordinary_log() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), 0).expect("place");

    assert_eq!(log.base_offset(), 0);
    let appended = log.append(&records(&["a"])).await.expect("append");
    assert_eq!(appended.first_offset, 0);
}

/// Rollover keeps the offsets contiguous from a non-zero base, so a placed
/// log behaves like any other once it is being written to.
#[tokio::test]
async fn a_placed_log_rolls_over_without_breaking_the_offsets() {
    let dir = tempdir().expect("dir");
    let log =
        DiskLog::open_at(dir.path(), "shard", config(FsyncMode::OnCommit), BASE).expect("place");

    for i in 0..200 {
        log.append(&records(&[&format!("value-{i:04}")]))
            .await
            .expect("append");
    }

    assert!(
        log.segments().len() > 1,
        "the test did not force a rollover"
    );
    let read = log
        .read_range(ReadRange {
            start: BASE,
            max_bytes: 64 * 1024,
        })
        .await
        .expect("read")
        .records;
    for (index, record) in read.iter().enumerate() {
        assert_eq!(
            record.offset,
            BASE + index as u64,
            "offsets broke at {index}"
        );
    }
}
