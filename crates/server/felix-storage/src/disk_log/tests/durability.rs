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
    let run = tempdir().expect("run dir");
    let boot = Some(crate::run_state::BootId::from_bytes([1; 16]));
    let begun =
        crate::run_state::begin(run.path(), true, None, boot, || false, || [1; 16]).expect("begin");
    crate::run_state::watch_poison(run.path());
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
    // Closed or not, the process may not record a clean stop.
    assert!(crate::disk_log::any_log_poisoned());
    // Nor may a process crash now keep the storage id.
    assert_eq!(
        crate::run_state::read(run.path(), begun.id).expect("read"),
        crate::run_state::LastRun::Crashed {
            relaxed: true,
            boot: None
        }
    );
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

/// The names in `path` each time opening a log there flushed `path` itself.
fn dir_syncs_opening(path: &std::path::Path, base: Option<Offset>) -> Vec<Vec<String>> {
    crate::io::DIR_SYNCS.with(|syncs| syncs.borrow_mut().clear());
    let log = match base {
        Some(base) => DiskLog::open_at(path, "t/ns/s/0", config(FsyncMode::OnCommit), base),
        None => DiskLog::open(path, "t/ns/s/0", config(FsyncMode::OnCommit)),
    }
    .expect("open");
    drop(log);
    crate::io::DIR_SYNCS.with(|syncs| {
        syncs
            .borrow()
            .iter()
            .filter(|(dir, _)| dir == path)
            .map(|(_, names)| names.clone())
            .collect()
    })
}

/// **Opening a log flushes its directory once, after its files exist.** The
/// first segment, its index and the durable mark all get their entries from
/// that one flush. A bulk create opens hundreds of logs, and every extra flush
/// there waits on the filesystem's journal.
#[tokio::test]
async fn opening_a_log_flushes_its_directory_once() {
    let dir = tempdir().expect("dir");
    let covered = [
        crate::segment::segment_file_name(0),
        crate::segment::index_file_name(0),
        crate::disk_log::durable_mark::mark_file_name().to_string(),
    ];
    let fresh = dir.path().join("fresh");
    let placed = dir.path().join("placed");
    for (path, base, label) in [
        (&fresh, None, "a new log"),
        (&fresh, None, "an existing log"),
        (&placed, Some(40), "a log placed at a base"),
    ] {
        let syncs = dir_syncs_opening(path, base);
        assert_eq!(syncs.len(), 1, "{label}: {syncs:?}");
        for name in &covered {
            assert!(
                syncs[0].contains(name),
                "{label}: {name} missing from {syncs:?}"
            );
        }
    }
}

/// A restart in the boot that poisoned the log: the failed flush's bytes are
/// still in the page cache and read back as valid records. They were never
/// acknowledged under `OnCommit`, so the log opens without them and takes
/// the next append where they began.
#[tokio::test]
async fn a_restart_after_a_poisoned_flush_cuts_the_unsynced_bytes() {
    // Small segments, so the bytes of the failed flush span a rollover.
    for failing in 3..6 {
        let dir = tempdir().expect("dir");
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..failing {
            log.append(&records(&[&format!("v{i}")]))
                .await
                .expect("append");
        }
        log.inner
            .fail_next_flush
            .store(true, std::sync::atomic::Ordering::Release);
        log.append(&records(&["lost"]))
            .await
            .expect_err("flush failed");
        assert!(log.is_poisoned());
        drop(log);
        assert!(dir.path().join("poisoned").exists());

        let log = open(&dir, FsyncMode::OnCommit);
        let kept: Vec<String> = (0..failing).map(|i| format!("v{i}")).collect();
        assert_eq!(read_all(&log, 0).await, kept, "failing at {failing}");
        assert!(!dir.path().join("poisoned").exists());
        log.append(&records(&["next"])).await.expect("append");
        let mut expected = kept;
        expected.push("next".to_string());
        assert_eq!(read_all(&log, 0).await, expected);
    }
}

/// The cut stays off for a relaxed mode, whose records past the last sync
/// were acknowledged, and the marker is still cleared.
#[tokio::test]
async fn a_restart_after_a_poisoned_flush_keeps_a_relaxed_logs_records() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");
    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    log.sync().await.expect_err("flush failed");
    drop(log);
    assert!(dir.path().join("poisoned").exists());

    let log = open(&dir, FsyncMode::None);
    assert_eq!(read_all(&log, 0).await, ["a", "b"]);
    assert!(!dir.path().join("poisoned").exists());
}

/// A state file whose last write failed is read back from the page cache as
/// if it were durable, so a start after a poison writes it again.
#[tokio::test]
async fn a_restart_after_a_poisoned_flush_writes_the_state_files_again() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.accept_generation(3, Some("leader-a"))
        .await
        .expect("accept");
    let ballot = dir.path().join("ballot");
    let before = (
        std::fs::metadata(&ballot).expect("ballot").ino(),
        std::fs::read(&ballot).expect("read"),
    );
    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    log.append(&records(&["lost"]))
        .await
        .expect_err("flush failed");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    let after = (
        std::fs::metadata(&ballot).expect("ballot").ino(),
        std::fs::read(&ballot).expect("read"),
    );
    assert_ne!(before.0, after.0, "the ballot was not written again");
    assert_eq!(before.1, after.1);
    drop(log);
}

/// Poison a log by failing the flush of `lost`, after `kept` went in.
async fn poison_after(dir: &TempDir, mode: FsyncMode, kept: &[&str]) {
    let log = open(dir, mode);
    log.append(&records(kept)).await.expect("append");
    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    if matches!(mode, FsyncMode::OnCommit) {
        log.append(&records(&["lost"]))
            .await
            .expect_err("flush failed");
    } else {
        log.append(&records(&["late"])).await.expect("append");
        log.sync().await.expect_err("flush failed");
    }
    assert!(log.is_poisoned());
    drop(log);
    assert!(dir.path().join("poisoned").exists());
}

/// A relaxed run acknowledged the records past its last sync, so the next
/// start keeps them and writes them again: a sync alone would find their
/// pages clean and write nothing.
#[tokio::test]
async fn a_restart_after_a_relaxed_poison_writes_the_unsynced_bytes_again() {
    let dir = tempdir().expect("dir");
    poison_after(&dir, FsyncMode::None, &["a", "b"]).await;
    super::recovery::REWRITTEN_BYTES.with(|total| total.set(0));
    let log = open(&dir, FsyncMode::None);
    let segment = dir.path().join(crate::segment::segment_file_name(0));
    let len = std::fs::metadata(&segment).expect("segment").len();
    let rewritten = super::recovery::REWRITTEN_BYTES.with(std::cell::Cell::get);
    // Everything past the header: the poisoned run never synced a record.
    assert_eq!(
        rewritten,
        len - crate::segment::SEGMENT_HEADER_LEN,
        "the unsynced bytes were not written again"
    );
    assert_eq!(read_all(&log, 0).await, ["a", "b", "late"]);
    assert!(!dir.path().join("poisoned").exists());
}

/// The cut follows the mode of the run that was poisoned, not the one that
/// reopens the log.
#[tokio::test]
async fn a_poisoned_logs_mode_decides_the_cut_across_a_mode_change() {
    let dir = tempdir().expect("dir");
    poison_after(&dir, FsyncMode::None, &["a", "b"]).await;
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 0).await, ["a", "b", "late"]);
    drop(log);

    let dir = tempdir().expect("dir");
    poison_after(&dir, FsyncMode::OnCommit, &["a", "b"]).await;
    let log = open(&dir, FsyncMode::None);
    assert_eq!(read_all(&log, 0).await, ["a", "b"]);
}

/// A sync that succeeded whose mark could not be written still counts: the
/// poison records the synced point, so the cut keeps what that sync
/// acknowledged.
#[tokio::test]
async fn a_failed_mark_write_does_not_let_the_cut_remove_an_acknowledged_record() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");
    log.inner
        .mark
        .fail_next_write
        .store(true, std::sync::atomic::Ordering::Release);
    log.append(&records(&["b"])).await.expect("append");
    log.inner
        .fail_next_flush
        .store(true, std::sync::atomic::Ordering::Release);
    log.append(&records(&["lost"]))
        .await
        .expect_err("flush failed");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 0).await, ["a", "b"]);
}

/// A rollover that failed poisons the log too. Every record it acknowledged
/// was synced, so the start after it keeps them all.
#[tokio::test]
async fn a_restart_after_a_roll_failure_keeps_the_acknowledged_records() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    let kept: Vec<String> = (0..6).map(|i| format!("v{i}")).collect();
    for value in &kept {
        log.append(&records(&[value])).await.expect("append");
    }
    log.inner
        .record_roll_failure(&crate::StorageError::SyncFailed("injected".to_string()));
    assert!(log.is_poisoned());
    drop(log);
    assert!(dir.path().join("poisoned").exists());

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(read_all(&log, 0).await, kept);
    assert!(!dir.path().join("poisoned").exists());
}

/// A crash at any step of the cut leaves the marker, so the next start
/// finishes the cut.
#[tokio::test]
async fn a_crash_part_way_through_the_cut_is_finished_by_the_next_start() {
    let lost: Vec<String> = (0..8).map(|i| format!("lost-{i}")).collect();
    let lost: Vec<&str> = lost.iter().map(String::as_str).collect();
    for steps in 0..4 {
        let dir = tempdir().expect("dir");
        let log = open(&dir, FsyncMode::OnCommit);
        log.append(&records(&["a", "b"])).await.expect("append");
        log.inner
            .fail_next_flush
            .store(true, std::sync::atomic::Ordering::Release);
        log.append(&records(&lost)).await.expect_err("flush failed");
        drop(log);

        super::recovery::CUT_STEPS_LEFT.with(|left| left.set(Some(steps)));
        let crashed = DiskLog::open(dir.path(), "t/ns/s/0", config(FsyncMode::OnCommit));
        super::recovery::CUT_STEPS_LEFT.with(|left| left.set(None));
        if steps == 0 {
            assert!(crashed.is_err(), "the injected crash did not stop the open");
        }
        drop(crashed);

        let log = open(&dir, FsyncMode::OnCommit);
        assert_eq!(
            read_all(&log, 0).await,
            ["a", "b"],
            "crashed after {steps} steps"
        );
    }
}

/// A marker that cannot be read cuts nothing, and says so.
#[tokio::test]
async fn an_unreadable_poison_marker_is_logged() {
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a", "b"])).await.expect("append");
    drop(log);
    std::fs::write(dir.path().join("poisoned"), b"not a mark").expect("marker");

    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let log = tracing::subscriber::with_default(subscriber, || open(&dir, FsyncMode::OnCommit));
    let logged = String::from_utf8(captured.0.lock().expect("lock").clone()).expect("utf8");
    assert!(logged.contains("marker cannot be read"), "logged: {logged}");
    assert_eq!(read_all(&log, 0).await, ["a", "b"]);
    assert!(!dir.path().join("poisoned").exists());
}

/// After the cut, `durable.mark` says where the log was cut to, even when a
/// flush in flight at the poison had moved it further.
#[tokio::test]
async fn the_cut_moves_the_durable_mark_back_to_the_marker() {
    let dir = tempdir().expect("dir");
    poison_after(&dir, FsyncMode::OnCommit, &["a", "b"]).await;
    let poison = super::durable_mark::load_poison(dir.path())
        .expect("marker")
        .expect("readable");
    let ahead = super::durable_mark::DurableMark {
        segment: poison.mark.segment,
        synced_bytes: poison.mark.synced_bytes + 1_000,
    };
    std::fs::write(dir.path().join("durable.mark"), ahead.encode()).expect("mark");

    super::recovery::after_poison(dir.path(), "t/ns/s/0", Some(poison)).expect("cut");
    assert_eq!(super::durable_mark::load(dir.path()), Some(poison.mark));
}
