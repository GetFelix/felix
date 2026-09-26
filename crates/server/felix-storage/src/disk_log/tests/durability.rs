use super::*;

#[tokio::test]
async fn on_commit_acknowledges_only_durable_records() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);

    for i in 0..5 {
        let result = log
            .append(&records(&[&format!("v{i}")]))
            .await
            .expect("append");
        // The acknowledgement itself is the durability guarantee: by the time
        // `append` returns, the record is past the device.
        assert!(
            log.durable_offset() > result.last_offset,
            "offset {} not durable at ack",
            result.last_offset
        );
        // Zero, or one segment header. A background rollover installs its
        // replacement by writing the header into the page cache and leaving the
        // flush to the next sync, so an ack can land in the window where those
        // 32 bytes are the only thing outstanding. No *record* is ever
        // unsynced at an ack, which is what the assertion above checks
        // directly.
        assert!(
            log.unsynced_bytes() <= crate::segment::SEGMENT_HEADER_LEN,
            "unsynced record bytes at ack: {}",
            log.unsynced_bytes(),
        );
    }
}

#[tokio::test]
async fn none_mode_acknowledges_without_syncing() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");

    // Nothing has been pushed to the device, which is exactly what `None` buys.
    assert!(log.unsynced_bytes() > 0);
    assert_eq!(log.durable_offset(), 0);

    // An explicit sync still works, and is what shutdown uses.
    log.sync().await.expect("sync");
    assert_eq!(log.unsynced_bytes(), 0);
    assert_eq!(log.durable_offset(), 2);
}

#[tokio::test]
async fn periodic_mode_bounds_unsynced_data_by_its_interval() {
    let dir = tempdir().expect("dir");
    let log = open(
        &dir,
        FsyncMode::Periodic {
            interval: Duration::from_millis(20),
        },
    );
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");

    // The append itself did not wait for the device.
    assert!(log.unsynced_bytes() > 0);

    // Within a few intervals the background syncer catches up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while log.durable_offset() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(log.durable_offset(), 3);
    assert_eq!(log.unsynced_bytes(), 0);

    log.shutdown().await.expect("shutdown");
}

/// The periodic timer fires for every open log, and a broker holds hundreds of
/// them with most idle. A tick that finds nothing unsynced must not fsync.
#[tokio::test]
async fn periodic_mode_does_not_flush_an_idle_log() {
    let dir = tempdir().expect("dir");
    let log = open(
        &dir,
        FsyncMode::Periodic {
            interval: Duration::from_millis(5),
        },
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        log.flushes_performed(),
        0,
        "a never-written log was flushed"
    );

    // Enough to roll the tiny test segments more than once, so the tick after
    // a seal still has to catch the watermark up.
    let payloads: Vec<String> = (0..12).map(|i| format!("record-{i:02}")).collect();
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    log.append(&records(&refs)).await.expect("append");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while log.durable_offset() < 12 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(log.durable_offset(), 12);
    assert_eq!(log.unsynced_bytes(), 0);

    let settled = log.flushes_performed();
    assert!(settled > 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        log.flushes_performed(),
        settled,
        "an idle log kept flushing after everything was durable"
    );

    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_flushes_what_the_policy_had_not() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::None);
        log.append(&records(&["a", "b", "c"]))
            .await
            .expect("append");
        assert!(log.unsynced_bytes() > 0);
        log.shutdown().await.expect("shutdown");
    }
    let log = open(&dir, FsyncMode::None);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn concurrent_on_commit_appends_all_land_exactly_once() {
    let dir = tempdir().expect("dir");
    let log = std::sync::Arc::new(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                segment_size_bytes: 4 * 1024,
                ..config(FsyncMode::OnCommit)
            },
        )
        .expect("open"),
    );

    let mut tasks = Vec::new();
    for i in 0..32u32 {
        let log = std::sync::Arc::clone(&log);
        tasks.push(tokio::spawn(async move {
            log.append(&records(&[&format!("task-{i:02}")]))
                .await
                .expect("append")
        }));
    }
    let mut assigned: Vec<Offset> = Vec::new();
    for task in tasks {
        let result = task.await.expect("join");
        assert_eq!(result.first_offset, result.last_offset);
        assigned.push(result.first_offset);
    }

    // Every append got a distinct offset, and the log holds exactly them.
    assigned.sort_unstable();
    assert_eq!(assigned, (0..32).collect::<Vec<_>>());
    assert_eq!(log.durable_offset(), 32);

    let stored = read_all(&log, 0).await;
    assert_eq!(stored.len(), 32);
    let mut sorted = stored.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 32, "duplicate or lost records");
}

/// A durable append must not queue for the shared blocking pool. That pool
/// also serves reads, rollovers and every other shard's work, so a flush
/// dispatched through it waits behind all of that, and the wait grows with
/// the number of shards flushing at once.
#[test]
fn a_saturated_blocking_pool_does_not_hold_up_a_durable_append() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let dir = tempdir().expect("dir");
        let log = open(&dir, FsyncMode::OnCommit);
        let (release, held) = std::sync::mpsc::channel::<()>();
        let occupant = tokio::task::spawn_blocking(move || {
            let _ = held.recv();
        });

        let append =
            tokio::time::timeout(Duration::from_secs(5), log.append(&records(&["durable"]))).await;
        release.send(()).expect("release the pool");
        occupant.await.expect("occupant");

        let result = append
            .expect("the append waited on the blocking pool")
            .expect("append");
        assert!(log.durable_offset() > result.last_offset);
    });
}

/// Fsyncgate: after a failed fsync the kernel may have dropped the dirty pages
/// and cleared the error, so the next fsync succeeds having written nothing.
/// The log has to stop rather than let that second flush publish a durable
/// bound over the lost bytes.
#[tokio::test]
async fn a_failed_flush_poisons_the_log_and_never_advances_the_durable_bound() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");

    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    log.sync().await.expect_err("injected fsync failure");
    assert_eq!(log.durable_offset(), 0);

    // The injected failure is spent, so the device would now "succeed".
    let err = log
        .sync()
        .await
        .expect_err("a later flush must not succeed");
    assert!(err.to_string().contains("flush failed"), "{err}");
    assert_eq!(
        log.durable_offset(),
        0,
        "durable bound advanced past lost bytes"
    );
    assert!(log.append(&records(&["c"])).await.is_err());
    assert!(log.shutdown().await.is_err());
}

#[tokio::test]
async fn a_failed_flush_under_on_commit_rejects_every_later_append() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");
    assert_eq!(log.durable_offset(), 1);

    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    log.append(&records(&["b"]))
        .await
        .expect_err("flush failed");
    log.append(&records(&["c"]))
        .await
        .expect_err("log is poisoned");
    assert_eq!(log.durable_offset(), 1);
}

/// The writer's own sync (here, the one a seal starts with) failing has to
/// stop the log too: the flush path syncs through a cloned handle and would
/// otherwise never learn about it.
#[tokio::test]
async fn a_failed_writer_sync_stops_later_flushes() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b", "c"]))
        .await
        .expect("append");

    log.inner.segments.write().active_mut().fail_next_sync();
    log.seal().await.expect_err("injected sync failure");

    log.sync()
        .await
        .expect_err("flush after a failed writer sync");
    assert_eq!(log.durable_offset(), 0);
}
