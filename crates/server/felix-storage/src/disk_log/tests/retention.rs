//! Retention is the only thing that ever raises `base_offset`, which makes it the
//! only thing that can make `Trimmed` reachable. Every test here therefore
//! asserts the *observable* consequence — a read that now fails, a base offset
//! that moved — rather than just counting files.

use super::*;

fn retention_config(retention_bytes: Option<u64>, retention_age: Option<Duration>) -> LogConfig {
    LogConfig {
        retention_bytes,
        retention_age,
        // Long enough that the background task never fires mid-test; these
        // tests drive retention explicitly via `enforce_retention_now`.
        retention_check_interval: Duration::from_secs(3600),
        ..config(FsyncMode::None)
    }
}

/// A record with an explicit timestamp, for age-based retention.
fn aged_record(payload: &str, timestamp_micros: u64) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros,
        mark: Default::default(),
        publisher: None,
    }
}

#[tokio::test]
async fn retention_is_off_by_default() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    for payload in ["a", "b", "c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert_eq!(outcome.segments_deleted, 0);
    assert_eq!(log.base_offset(), 0);
    assert_eq!(read_all(&log, 0).await.len(), 6);
}

#[tokio::test]
async fn size_retention_deletes_oldest_segments_and_raises_base_offset() {
    let dir = tempdir().expect("dir");
    // One segment holds ~2 of these records, so this keeps roughly one sealed
    // segment plus the active one.
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("open");

    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    assert_eq!(log.base_offset(), 0, "nothing trimmed before a sweep");

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(outcome.segments_deleted > 0, "expected a trim");
    assert!(outcome.bytes_reclaimed > 0);
    let base = log.base_offset();
    assert!(
        base > 0,
        "base offset must advance past the deleted records"
    );
    assert_eq!(outcome.base_offset, base);

    // The surviving records still read back, contiguously, from the new base.
    let surviving = read_all(&log, base).await;
    assert!(!surviving.is_empty());
    assert_eq!(surviving.last().map(String::as_str), Some("h"));
}

#[tokio::test]
async fn retention_makes_a_below_base_read_report_trimmed() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("open");
    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.enforce_retention_now().await.expect("retention");
    let base = log.base_offset();
    assert!(base > 0);

    // This is the assertion the whole feature exists to make possible: before
    // retention, `Trimmed` could not fire on a durable log at all.
    let err = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect_err("offset 0 is gone");
    match err {
        StorageError::Trimmed { requested, oldest } => {
            assert_eq!(requested, 0);
            assert_eq!(oldest, base);
        }
        other => panic!("expected Trimmed, got {other:?}"),
    }
}

#[tokio::test]
async fn retention_never_deletes_the_active_segment() {
    let dir = tempdir().expect("dir");
    // A bound far below one segment: retention still cannot empty the log,
    // because the active segment is not a candidate.
    let log = DiskLog::open(dir.path(), "t/ns/s/0", retention_config(Some(1), None)).expect("open");
    for payload in ["a", "b", "c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }

    log.enforce_retention_now().await.expect("retention");
    let base = log.base_offset();
    let tail = log.tail_offset().await.expect("tail");
    assert!(base < tail, "the active segment's records must survive");
    assert!(!read_all(&log, base).await.is_empty());
}

#[tokio::test]
async fn age_retention_uses_the_newest_record_in_a_segment() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(None, Some(Duration::from_secs(60))),
    )
    .expect("open");

    let now = super::now_micros();
    let old = now - Duration::from_secs(3600).as_micros() as u64;
    // Enough expired records to fill whole segments: a segment is only a
    // candidate once *every* record in it has expired, so a handful sharing a
    // segment with fresh ones would (correctly) survive.
    for i in 0..12 {
        log.append(&[aged_record(&format!("old{i:02}"), old)])
            .await
            .expect("append");
    }
    for i in 0..12 {
        log.append(&[aged_record(&format!("new{i:02}"), now)])
            .await
            .expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(outcome.segments_deleted > 0, "expired segments should go");
    let surviving = read_all(&log, log.base_offset()).await;
    assert!(
        !surviving.contains(&"old00".to_string()),
        "the oldest expired record should be gone, got {surviving:?}"
    );
    assert!(
        (0..12).all(|i| surviving.contains(&format!("new{i:02}"))),
        "every fresh record must survive, got {surviving:?}"
    );
}

#[tokio::test]
async fn age_retention_keeps_a_segment_whose_newest_record_is_fresh() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(None, Some(Duration::from_secs(60))),
    )
    .expect("open");

    let now = super::now_micros();
    let old = now - Duration::from_secs(3600).as_micros() as u64;
    // An old record and a fresh one land in the same segment. The newest
    // decides, so nothing is deleted -- the conservative direction.
    log.append(&[aged_record("old", old)])
        .await
        .expect("append");
    log.append(&[aged_record("new", now)])
        .await
        .expect("append");
    for payload in ["x", "y", "z"] {
        log.append(&[aged_record(payload, now)])
            .await
            .expect("append");
    }

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        outcome.segments_deleted, 0,
        "a segment holding one fresh record must survive"
    );
    assert_eq!(log.base_offset(), 0);
}

#[tokio::test]
async fn a_trimmed_log_reopens_from_its_surviving_base_offset() {
    let dir = tempdir().expect("dir");
    let cfg = retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None);
    let base = {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", cfg.clone()).expect("open");
        for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            log.append(&records(&[payload])).await.expect("append");
        }
        log.enforce_retention_now().await.expect("retention");
        let base = log.base_offset();
        log.shutdown().await.expect("shutdown");
        base
    };
    assert!(base > 0, "the trim must have removed segment 0");

    // Recovery has to derive the base from the lowest *surviving* segment, not
    // assume the directory starts at offset 0.
    let reopened = DiskLog::open(dir.path(), "t/ns/s/0", cfg).expect("reopen");
    assert_eq!(reopened.base_offset(), base);
    assert_eq!(
        read_all(&reopened, base).await.last().map(String::as_str),
        Some("h")
    );
    let err = reopened
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect_err("trimmed offsets stay trimmed across a restart");
    assert!(matches!(err, StorageError::Trimmed { .. }));
}

#[tokio::test]
async fn zero_retention_bounds_are_rejected_at_open() {
    let dir = tempdir().expect("dir");
    assert!(DiskLog::open(dir.path(), "t/ns/s/0", retention_config(Some(0), None)).is_err());
    assert!(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            retention_config(None, Some(Duration::ZERO))
        )
        .is_err()
    );
}

/// A tight bound over eight records, which leaves about one sealed segment.
async fn eight_records_over_a_tight_bound(dir: &TempDir) -> DiskLog {
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("open");
    for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    log
}

/// **Held at the commit offset, retention deletes nothing a majority may not
/// hold yet.** A `Quorum` leader's records above its commit offset may be
/// waiting for followers; deleted there, a follower rebuilt at the new base
/// would be counted as holding them, and a publish acknowledged for records
/// that are gone (#1094).
#[tokio::test]
async fn retention_held_at_the_commit_offset_stops_below_it() {
    let unheld_dir = tempdir().expect("dir");
    let unheld = eight_records_over_a_tight_bound(&unheld_dir).await;
    unheld.enforce_retention_now().await.expect("retention");
    assert_eq!(
        unheld.base_offset(),
        4,
        "the bound does not delete the first segment, so the case is not set up",
    );

    let dir = tempdir().expect("dir");
    let log = eight_records_over_a_tight_bound(&dir).await;
    log.hold_retention_at_commit(true).await.expect("hold");
    let outcome = log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        outcome.segments_deleted, 0,
        "deleted above a commit offset of 0"
    );

    // Offset 3 is not committed and shares the first segment, so it stays.
    log.advance_commit_offset(3).await.expect("commit");
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        log.base_offset(),
        0,
        "retention deleted offset 3 above the commit offset"
    );

    // Once the commit offset clears the segment, the bound applies in full.
    log.advance_commit_offset(4).await.expect("commit");
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(log.base_offset(), unheld.base_offset());
}

/// **The hold survives a restart.** After one, nothing sets it again until a
/// replication pass or an applied batch, and an idle shard or a follower cut
/// off from its leader may see neither before a sweep (#1109).
#[tokio::test]
async fn a_reopened_log_is_held_before_replication_touches_it() {
    let dir = tempdir().expect("dir");
    let log = eight_records_over_a_tight_bound(&dir).await;
    log.hold_retention_at_commit(true).await.expect("hold");
    log.advance_commit_offset(3).await.expect("commit");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("reopen");
    assert_eq!(log.commit_offset(), 3);
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        log.base_offset(),
        0,
        "retention after a restart deleted offset 3 above the commit offset"
    );
    assert!(log.retention_held_at_commit());

    // Lifted, the change is written through too.
    log.hold_retention_at_commit(false).await.expect("lift");
    log.shutdown().await.expect("shutdown");
    drop(log);
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("reopen");
    assert!(!log.retention_held_at_commit());
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(log.base_offset(), 4);
}

/// **A broker that does not replicate lifts a saved hold at open.** Nothing
/// there will ever advance the commit offset, so the hold would stop
/// retention for good.
#[tokio::test]
async fn a_log_opened_lifted_drops_its_saved_hold() {
    let dir = tempdir().expect("dir");
    let log = eight_records_over_a_tight_bound(&dir).await;
    log.hold_retention_at_commit(true).await.expect("hold");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let reopen = || {
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                retention_hold: crate::log::RetentionHold::Lifted,
                ..retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None)
            },
        )
        .expect("reopen")
    };
    let log = reopen();
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(
        log.base_offset(),
        4,
        "a standalone log kept a hold nothing will lift"
    );
    assert!(!log.retention_held_at_commit());
    log.shutdown().await.expect("shutdown");
    drop(log);
    let saved = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None),
    )
    .expect("reopen");
    assert!(!saved.retention_held_at_commit(), "the lift was not saved");
}

/// A log opened `Held` is held before its first sweep, whatever it saved.
#[tokio::test]
async fn a_log_opened_held_is_held_from_its_first_sweep() {
    let dir = tempdir().expect("dir");
    let log = eight_records_over_a_tight_bound(&dir).await;
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            retention_hold: crate::log::RetentionHold::Held,
            ..retention_config(Some(crate::segment::SEGMENT_HEADER_LEN + 120), None)
        },
    )
    .expect("reopen");
    log.enforce_retention_now().await.expect("retention");
    assert_eq!(log.base_offset(), 0, "deleted above a commit offset of 0");
}

/// Compaction's head trim is held at the same floor.
#[tokio::test]
async fn a_head_trim_held_at_the_commit_offset_stops_below_it() {
    let dir = tempdir().expect("dir");
    let log = eight_records_over_a_tight_bound(&dir).await;
    log.hold_retention_at_commit(true).await.expect("hold");
    log.advance_commit_offset(6).await.expect("commit");

    log.roll_now().await.expect("roll");
    log.trim_before(8).await.expect("trim");

    assert!(
        log.base_offset() <= 6,
        "a head trim reached {} past the commit offset 6",
        log.base_offset(),
    );
    assert!(
        log.base_offset() > 0,
        "the trim below the floor did not happen"
    );
}
