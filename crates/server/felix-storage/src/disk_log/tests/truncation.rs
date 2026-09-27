use super::*;

#[tokio::test]
async fn truncate_drops_the_suffix_and_survives_reopen() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..20 {
            log.append(&records(&[&format!("v{i:03}")]))
                .await
                .expect("append");
        }
        log.truncate(6).await.expect("truncate");
        assert_eq!(log.tail_offset().await.expect("tail"), 6);
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 0).await.len(), 6);
    log.append(&records(&["resumed"])).await.expect("append");
    assert_eq!(read_all(&log, 6).await, vec!["resumed"]);
}

/// A read runs without the segment lock, so a truncation and new appends can
/// land while it is between planning and reading. It must neither hold them
/// up nor return a mix of the old records and the ones now at their bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_racing_a_truncation_sees_one_side_of_it() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: 64 * 1024,
            ..config(FsyncMode::None)
        },
    )
    .expect("open");
    for i in 0..5 {
        log.append(&records(&[&format!("a-long-original-record-{i}")]))
            .await
            .expect("append");
    }

    let pause = std::sync::Arc::new(std::sync::Barrier::new(2));
    *log.inner.pause_next_read.lock() = Some(std::sync::Arc::clone(&pause));
    let reader = {
        let log = log.clone();
        tokio::spawn(async move { read_all(&log, 0).await })
    };
    let wait = std::sync::Arc::clone(&pause);
    tokio::task::spawn_blocking(move || wait.wait())
        .await
        .expect("planned");

    // Would deadlock if the paused read still held the segment lock.
    tokio::time::timeout(Duration::from_secs(5), async {
        log.truncate(2).await.expect("truncate");
        for i in 2..5 {
            log.append(&records(&[&format!("new-{i}")]))
                .await
                .expect("append");
        }
    })
    .await
    .expect("the paused read held the segment lock");

    tokio::task::spawn_blocking(move || pause.wait())
        .await
        .expect("resumed");
    let values = reader.await.expect("read");
    assert_eq!(
        values,
        vec![
            "a-long-original-record-0",
            "a-long-original-record-1",
            "new-2",
            "new-3",
            "new-4",
        ],
    );
}

/// A rebuild is a reset, not a truncation: everything goes, the base moves
/// to wherever the leader's oldest record is, in either direction, and the
/// generation history goes with the records it described.
#[tokio::test]
async fn reset_to_discards_everything_and_rebases_in_either_direction() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..10 {
            log.append(&records(&[&format!("v{i:03}")]))
                .await
                .expect("append");
        }
        log.record_generation(3, 0).expect("generation");

        log.reset_to(100).await.expect("reset up");
        assert_eq!(log.base_offset(), 100);
        assert_eq!(log.tail_offset().await.expect("tail"), 100);
        assert_eq!(log.durable_offset(), 100);
        assert!(log.generations().is_empty(), "history outlived its records");
        assert!(read_all(&log, 100).await.is_empty());

        log.append(&records(&["fresh"])).await.expect("append");
        assert_eq!(read_all(&log, 100).await, vec!["fresh"]);

        log.reset_to(5).await.expect("reset down");
        assert_eq!(log.base_offset(), 5);
        assert_eq!(log.tail_offset().await.expect("tail"), 5);
        log.append(&records(&["after"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.base_offset(), 5);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 5).await, vec!["after"]);
}

#[tokio::test]
async fn on_commit_reflushes_offsets_reused_after_truncation() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);

    log.append(&records(&["zero", "one", "two"]))
        .await
        .expect("initial append");
    assert_eq!(log.durable_offset(), 3);

    log.truncate(1).await.expect("truncate");
    assert_eq!(log.durable_offset(), 1);

    let replacement = log
        .append(&records(&["replacement"]))
        .await
        .expect("replacement append");
    assert_eq!(replacement.first_offset, 1);
    assert_eq!(log.durable_offset(), 2);
    assert_eq!(log.unsynced_bytes(), 0);
}
