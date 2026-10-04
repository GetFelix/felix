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
        };
        log.append(&[record(0)]).await.expect("append");
        log.append(&[AppendRecord {
            payload: Bytes::copy_from_slice(&3u64.to_be_bytes()),
            timestamp_micros: 0,
            mark: felix_storage::log::RecordMark::GenerationStart,
            publisher: None,
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
