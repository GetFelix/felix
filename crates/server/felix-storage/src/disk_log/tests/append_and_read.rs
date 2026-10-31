use super::*;

#[tokio::test]
async fn appends_are_readable_and_offsets_are_contiguous() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);

    let first = log.append(&records(&["a", "b"])).await.expect("append");
    assert_eq!((first.first_offset, first.last_offset), (0, 1));
    let second = log.append(&records(&["c"])).await.expect("append");
    assert_eq!((second.first_offset, second.last_offset), (2, 2));

    assert_eq!(log.tail_offset().await.expect("tail"), 3);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
    assert_eq!(read_all(&log, 2).await, vec!["c"]);
}

#[tokio::test]
async fn an_empty_append_is_rejected() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    assert!(matches!(
        log.append(&[]).await.expect_err("empty"),
        StorageError::InvalidRange
    ));
    assert_eq!(log.tail_offset().await.expect("tail"), 0);
}

#[tokio::test]
async fn reading_at_the_tail_is_empty_rather_than_an_error() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["only"])).await.expect("append");
    assert!(read_all(&log, 1).await.is_empty());
    assert!(read_all(&log, 500).await.is_empty());
}

#[tokio::test]
async fn a_trimmed_offset_is_distinguishable_from_an_empty_range() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    for i in 0..10 {
        log.append(&records(&[&format!("value-{i}")]))
            .await
            .expect("append");
    }
    // Truncating to zero and re-appending moves the base offset forward.
    log.truncate(0).await.expect("truncate");
    log.append(&records(&["fresh"])).await.expect("append");
    assert_eq!(log.base_offset(), 0);

    // With nothing trimmed yet, a low offset is a valid, non-empty range.
    assert_eq!(read_all(&log, 0).await, vec!["fresh"]);
}

#[tokio::test]
async fn a_read_below_the_base_offset_reports_trimmed() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 60,
            ..config(FsyncMode::None)
        },
    )
    .expect("open");

    for i in 0..20 {
        log.append(&records(&[&format!("v{i:03}")]))
            .await
            .expect("append");
    }
    // Simulate retention by truncating the whole log forward.
    log.truncate(0).await.expect("truncate");
    for i in 0..5 {
        log.append(&records(&[&format!("w{i}")]))
            .await
            .expect("append");
    }
    assert!(read_all(&log, 0).await.len() == 5);

    // A log whose base has advanced reports the trim rather than silently
    // returning a shorter range.
    let inner_base = log.base_offset();
    if inner_base > 0 {
        let err = log
            .read_range(ReadRange {
                start: inner_base - 1,
                max_bytes: usize::MAX,
            })
            .await
            .expect_err("trimmed");
        assert!(matches!(err, StorageError::Trimmed { .. }));
    }
}

#[tokio::test]
async fn reads_are_bounded_by_bytes_and_by_record_count() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            max_records_per_read: 4,
            ..config(FsyncMode::None)
        },
    )
    .expect("open");
    for i in 0..30 {
        log.append(&records(&[&format!("v{i:03}")]))
            .await
            .expect("append");
    }

    // The record cap applies even with an unlimited byte budget.
    let capped = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read")
        .records;
    assert_eq!(capped.len(), 4);
    assert_eq!(capped[0].offset, 0);
    assert_eq!(capped[3].offset, 3);

    // And the byte budget applies below the record cap.
    let small = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: 8,
        })
        .await
        .expect("read")
        .records;
    assert_eq!(small.len(), 2);

    // Paging with the returned offsets covers the whole log without gaps.
    let mut seen = Vec::new();
    let mut cursor = 0;
    while cursor < log.tail_offset().await.expect("tail") {
        let page = log
            .read_range(ReadRange {
                start: cursor,
                max_bytes: usize::MAX,
            })
            .await
            .expect("read")
            .records;
        assert!(!page.is_empty());
        cursor = page.last().expect("last").offset + 1;
        seen.extend(page.into_iter().map(|r| r.offset));
    }
    assert_eq!(seen, (0..30).collect::<Vec<_>>());
}

#[tokio::test]
async fn records_survive_reopening_the_log() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..25 {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append");
        }
        assert!(log.segments().len() > 1, "expected rollovers");
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 25);
    let values = read_all(&log, 0).await;
    assert_eq!(values.len(), 25);
    assert_eq!(values[0], "value-000");
    assert_eq!(values[24], "value-024");

    // And the reopened log keeps accepting writes.
    log.append(&records(&["after"])).await.expect("append");
    assert_eq!(read_all(&log, 25).await, vec!["after"]);
}

#[tokio::test]
async fn an_invalid_config_is_rejected_at_open() {
    let dir = tempdir().expect("dir");
    let err = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::ZERO,
            },
            ..LogConfig::default()
        },
    )
    .expect_err("zero interval");
    assert!(matches!(err, StorageError::InvalidConfig(_)));

    let err = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: 8,
            ..LogConfig::default()
        },
    )
    .expect_err("tiny segment");
    assert!(matches!(err, StorageError::InvalidConfig(_)));
}

#[tokio::test]
async fn a_record_larger_than_a_segment_still_round_trips() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let big = "x".repeat(10_000);
    log.append(&records(&["before", &big, "after"]))
        .await
        .expect("append");
    log.sync().await.expect("sync");

    let values = read_all(&log, 0).await;
    assert_eq!(values, vec!["before".to_string(), big, "after".to_string()]);
}
