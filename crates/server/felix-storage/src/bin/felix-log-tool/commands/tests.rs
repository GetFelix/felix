use super::*;

#[test]
fn percentiles_use_nearest_rank() {
    let sorted: Vec<u64> = (1..=100).collect();
    assert_eq!(percentile(&sorted, 0.50), 50.0);
    assert_eq!(percentile(&sorted, 0.99), 99.0);
    assert_eq!(percentile(&sorted, 1.0), 100.0);
    // A quantile below the first rank still returns a real sample.
    assert_eq!(percentile(&sorted, 0.0), 1.0);
}

#[test]
fn percentiles_of_an_empty_sample_are_zero() {
    assert_eq!(percentile(&[], 0.5), 0.0);
}

#[test]
fn fsync_modes_render_distinctly() {
    assert_eq!(describe_fsync(FsyncMode::None), "none");
    assert_eq!(describe_fsync(FsyncMode::OnCommit), "on_commit");
    assert_eq!(
        describe_fsync(FsyncMode::Periodic {
            interval: Duration::from_millis(40)
        }),
        "periodic:40ms"
    );
}

#[test]
fn a_generation_start_names_its_generation() {
    assert_eq!(generation_of(&7u64.to_be_bytes()), Some(7));
    assert_eq!(generation_of(b"short"), None);
}

/// A log holding a generation-start record verifies: the record is not held
/// to the generator's payload shape, and not counted as a written record.
#[tokio::test]
async fn verify_accepts_a_generation_start_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    {
        let log = DiskLog::open(dir.path(), "log-tool", config.clone()).expect("open");
        let record = |offset: u64| AppendRecord {
            payload: Bytes::from(payload::payload_for(offset, 32)),
            timestamp_micros: 0,
            mark: Default::default(),
            publisher: None,
            headers: None,
        };
        log.append(&[record(0)]).await.expect("append");
        log.append(&[AppendRecord {
            payload: Bytes::copy_from_slice(&3u64.to_be_bytes()),
            timestamp_micros: 0,
            mark: felix_storage::log::RecordMark::GenerationStart,
            publisher: None,
            headers: None,
        }])
        .await
        .expect("marker");
        log.append(&[record(2)]).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    verify(VerifyArgs {
        dir: dir.path().to_path_buf(),
        config: config.clone(),
        expect_at_least: Some(2),
        payload_bytes: Some(32),
    })
    .await
    .expect("verified");
    let too_many = verify(VerifyArgs {
        dir: dir.path().to_path_buf(),
        config,
        expect_at_least: Some(3),
        payload_bytes: Some(32),
    })
    .await;
    assert!(too_many.is_err(), "the marker was counted as a record");
}

/// A gap is a hole only where no segment header names it.
#[test]
fn a_gap_follows_only_where_a_segment_header_names_it() {
    let gaps = BTreeMap::from([(10, 3)]);
    assert!(follows(3, 3, &gaps));
    assert!(follows(3, 10, &gaps));
    assert!(!follows(4, 10, &gaps), "the header names a gap from 3");
    assert!(!follows(3, 11, &gaps), "no segment starts at 11");
    assert!(!follows(3, 10, &BTreeMap::new()));
}

/// A log whose last segment starts past a re-form's gap verifies: the hole
/// is the one its header names.
#[tokio::test]
async fn verify_accepts_a_gap_its_segment_header_names() {
    use felix_storage::segment::SegmentWriter;

    let dir = tempfile::tempdir().expect("tempdir");
    let config = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let record = |offset: u64| AppendRecord {
        payload: Bytes::from(payload::payload_for(offset, 32)),
        timestamp_micros: 0,
        mark: Default::default(),
        publisher: None,
        headers: None,
    };
    let last_id = {
        let log = DiskLog::open(dir.path(), "log-tool", config.clone()).expect("open");
        log.append(&[record(0), record(1), record(2)])
            .await
            .expect("append");
        let last_id = log.segments().last().expect("a segment").id;
        log.shutdown().await.expect("shutdown");
        last_id
    };
    let mut past_gap =
        SegmentWriter::create(dir.path(), last_id + 1, 10, 0, 1 << 20, 4096).expect("segment");
    past_gap.append(&[record(10), record(11)]).expect("append");
    past_gap.sync().expect("sync");
    let header = SegmentHeader::at_version(10, 0, past_gap.version()).past_gap(3);
    let path = past_gap.path().to_path_buf();
    drop(past_gap);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[..SEGMENT_HEADER_LEN as usize].copy_from_slice(&header.encode());
    std::fs::write(&path, bytes).expect("write");

    verify(VerifyArgs {
        dir: dir.path().to_path_buf(),
        config,
        expect_at_least: Some(5),
        payload_bytes: Some(32),
    })
    .await
    .expect("a gapped log verifies");
}

/// Every mode writes: `verify` runs recovery, which cuts a torn tail. So
/// each is refused on a directory an open log holds, and names it.
#[tokio::test]
async fn every_mode_refuses_a_directory_an_open_log_holds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let held = DiskLog::open(dir.path(), "broker", config.clone()).expect("open");
    let named = |outcome: Result<(), Failure>| {
        outcome.is_err_and(|err| err.contains(&dir.path().display().to_string()))
    };

    let written = write(WriteArgs {
        dir: dir.path().to_path_buf(),
        records: 1,
        payload_bytes: 32,
        batch: 1,
        config: config.clone(),
        report_acks: false,
        clean_shutdown: true,
    })
    .await;
    assert!(named(written), "write was let into a held directory");
    let verified = verify(VerifyArgs {
        dir: dir.path().to_path_buf(),
        config: config.clone(),
        expect_at_least: None,
        payload_bytes: None,
    })
    .await;
    assert!(named(verified), "verify was let into a held directory");
    let benched = bench(BenchArgs {
        dir: dir.path().to_path_buf(),
        records: 1,
        payload_bytes: 32,
        batch: 1,
        concurrency: 1,
        warmup_records: 0,
        config: config.clone(),
        label: "held".to_string(),
    })
    .await;
    assert!(named(benched), "bench was let into a held directory");

    held.close().await.expect("close");
    verify(VerifyArgs {
        dir: dir.path().to_path_buf(),
        config,
        expect_at_least: None,
        payload_bytes: None,
    })
    .await
    .expect("verify once the holder closed");
}
