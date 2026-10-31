use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode, ReadRange, Retention};

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn shard(stream: &str) -> ShardKey {
    ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: stream.into(),
        shard: 0,
    }
}

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 1,
        mark: Default::default(),
        publisher: None,
    }
}

#[tokio::test]
async fn a_closed_shard_fences_old_handles_and_reopens_from_disk() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    let old = provider.open_shard(&shard("orders")).expect("open");
    old.append(&[record("a"), record("b")])
        .await
        .expect("append");

    provider.close_shard(&shard("orders")).await.expect("close");
    assert!(old.is_closed());
    assert!(provider.open_shards().is_empty());
    assert!(matches!(
        old.append(&[record("late")]).await,
        Err(StorageError::Closed(_))
    ));
    assert!(matches!(
        old.read_range(ReadRange {
            start: 0,
            max_bytes: 1024
        })
        .await,
        Err(StorageError::Closed(_))
    ));

    let fresh = provider.open_shard(&shard("orders")).expect("reopen");
    assert!(!fresh.is_closed());
    assert_eq!(fresh.tail_offset().await.expect("tail"), 2);
    let appended = fresh.append(&[record("c")]).await.expect("append");
    assert_eq!(appended.first_offset, 2);
}

#[tokio::test]
async fn closing_a_shard_that_is_not_open_is_a_no_op() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    provider.close_shard(&shard("never")).await.expect("close");
    let other = provider.open_shard(&shard("other")).expect("open");
    provider.close_shard(&shard("never")).await.expect("close");
    assert!(!other.is_closed());
}

/// Descriptors this process holds on files under `dir`.
#[cfg(target_os = "linux")]
fn open_files_under(dir: &std::path::Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("fds")
        .filter_map(|fd| std::fs::read_link(fd.ok()?.path()).ok())
        .filter(|target| target.starts_with(dir))
        .count()
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn closing_a_shard_releases_its_files() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    let log = provider.open_shard(&shard("orders")).expect("open");
    log.append(&[record("a")]).await.expect("append");
    drop(log);
    assert!(open_files_under(dir.path()) > 0, "an open log holds files");

    provider.close_shard(&shard("orders")).await.expect("close");
    assert_eq!(open_files_under(dir.path()), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opening_one_shard_does_not_wait_on_a_slow_open_of_another() {
    let dir = tempdir().expect("dir");
    let provider = Arc::new(DiskLogProvider::new(dir.path(), config()).expect("provider"));
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let entered_tx = parking_lot::Mutex::new(entered_tx);
    let release_rx = parking_lot::Mutex::new(release_rx);
    provider.set_open_hook(move |key| {
        if key.stream == "slow" {
            entered_tx.lock().send(()).expect("signal");
            // Bounded, so a regression fails the test below rather than
            // hanging it.
            let _ = release_rx.lock().recv_timeout(Duration::from_secs(10));
        }
    });

    let slow = tokio::task::spawn_blocking({
        let provider = Arc::clone(&provider);
        move || provider.open_shard(&shard("slow"))
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the slow open started");

    let fast = tokio::task::spawn_blocking({
        let provider = Arc::clone(&provider);
        move || provider.open_shard(&shard("fast"))
    });
    let fast = tokio::time::timeout(Duration::from_secs(5), fast)
        .await
        .expect("the open of another shard waited on the slow one")
        .expect("join");
    fast.expect("fast open");

    release_tx.send(()).expect("release");
    slow.await.expect("join").expect("slow open");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opens_of_one_shard_open_it_once() {
    let dir = tempdir().expect("dir");
    let provider = Arc::new(DiskLogProvider::new(dir.path(), config()).expect("provider"));
    let opens = Arc::new(AtomicUsize::new(0));
    provider.set_open_hook({
        let opens = Arc::clone(&opens);
        move |_| {
            opens.fetch_add(1, Ordering::SeqCst);
            // Long enough that every caller below arrives mid-open.
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    let callers: Vec<_> = (0..8)
        .map(|_| {
            let provider = Arc::clone(&provider);
            tokio::task::spawn_blocking(move || provider.open_shard(&shard("same")))
        })
        .collect();
    let mut logs = Vec::new();
    for caller in callers {
        logs.push(caller.await.expect("join").expect("open"));
    }
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    // One log, not eight over the same files: a write through one is the
    // tail of all.
    logs[0].append(&[record("a")]).await.expect("append");
    for log in &logs {
        assert_eq!(log.tail_offset().await.expect("tail"), 1);
    }
}

/// A fresh node's stream root, and the acknowledged record in it, survive a
/// power loss that lands before anything else flushes the storage directory.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_fresh_root_keeps_its_acknowledged_record_through_a_power_loss() {
    use crate::io::power_loss::{PowerLoss, Writeback};

    let dir = tempdir().expect("dir");
    let observer = PowerLoss::install(dir.path()).expect("install");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        ..config()
    };
    let provider =
        DiskLogProvider::new(dir.path().join("streams"), config.clone()).expect("provider");
    let log = provider.open_shard(&shard("orders")).expect("open");
    log.append(&[record("acked")]).await.expect("append");

    for seed in 0..32 {
        let image = tempdir().expect("image");
        observer
            .crash(seed, Writeback::AnySubset, image.path())
            .expect("crash");
        let root = image.path().join("streams");
        assert!(root.is_dir(), "seed {seed} lost the stream root");
        let recovered = DiskLogProvider::new(root, config.clone()).expect("reopen");
        let read = recovered
            .open_shard(&shard("orders"))
            .expect("open")
            .read_range(ReadRange {
                start: 0,
                max_bytes: 1024,
            })
            .await
            .expect("read")
            .records;
        assert_eq!(read.len(), 1, "seed {seed} lost the acknowledged record");
        recovered.shutdown().await.expect("shutdown");
    }
    provider.shutdown().await.expect("shutdown");
}

/// **A stream's own retention bounds its logs and no other stream's**, both
/// the shards already open when it is set and the ones opened after.
#[tokio::test]
async fn a_streams_retention_bounds_only_its_own_logs() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 80,
        index_spacing_bytes: 32,
        retention_check_interval: Duration::from_secs(3600),
        ..config()
    };
    let provider = DiskLogProvider::new(dir.path(), config).expect("provider");
    let orders = provider.open_shard(&shard("orders")).expect("open");
    let events = provider.open_shard(&shard("events")).expect("open");

    let bound = Retention {
        bytes: Some(crate::segment::SEGMENT_HEADER_LEN + 120),
        age: None,
    };
    provider
        .set_stream_retention("t", "ns", "orders", bound)
        .expect("set retention");
    let later = provider
        .open_shard(&ShardKey {
            shard: 1,
            ..shard("orders")
        })
        .expect("open");
    assert_eq!(later.retention(), bound);
    assert_eq!(events.retention(), Retention::default());

    for log in [&orders, &events] {
        for payload in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            log.append(&[record(payload)]).await.expect("append");
        }
    }
    assert!(
        orders
            .enforce_retention_now()
            .await
            .expect("sweep")
            .segments_deleted
            > 0
    );
    assert!(orders.base_offset() > 0);
    assert_eq!(
        events
            .enforce_retention_now()
            .await
            .expect("sweep")
            .segments_deleted,
        0
    );
    assert_eq!(events.base_offset(), 0);
}

/// A bound the stream leaves unset comes from the provider's configuration,
/// and a zero is refused rather than read as "keep nothing".
#[tokio::test]
async fn a_streams_retention_falls_back_per_bound_and_refuses_zero() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        retention_age: Some(Duration::from_secs(60)),
        retention_check_interval: Duration::from_secs(3600),
        ..config()
    };
    let provider = DiskLogProvider::new(dir.path(), config).expect("provider");
    let orders = provider.open_shard(&shard("orders")).expect("open");

    provider
        .set_stream_retention(
            "t",
            "ns",
            "orders",
            Retention {
                bytes: Some(1 << 20),
                age: None,
            },
        )
        .expect("set retention");
    assert_eq!(
        orders.retention(),
        Retention {
            bytes: Some(1 << 20),
            age: Some(Duration::from_secs(60)),
        }
    );

    let zero = Retention {
        bytes: Some(0),
        age: None,
    };
    assert!(matches!(
        provider.set_stream_retention("t", "ns", "orders", zero),
        Err(StorageError::InvalidConfig(_))
    ));
}

/// Looking up a log that is not open neither opens it nor creates its
/// directory, so a read-only caller leaves nothing behind.
#[tokio::test]
async fn looking_for_an_open_log_creates_nothing() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    let before: Vec<_> = std::fs::read_dir(dir.path()).expect("read").collect();

    assert!(provider.opened_shard(&shard("orders")).is_none());
    let after: Vec<_> = std::fs::read_dir(dir.path()).expect("read").collect();
    assert_eq!(before.len(), after.len());

    provider.open_shard(&shard("orders")).expect("open");
    assert!(provider.opened_shard(&shard("orders")).is_some());
    provider.close_shard(&shard("orders")).await.expect("close");
    assert!(provider.opened_shard(&shard("orders")).is_none());
}

/// A stream's hold reaches its open shards at once, and the ones opened
/// later before their first sweep. Other streams keep their own.
#[tokio::test]
async fn a_streams_retention_hold_reaches_open_and_later_shards() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    let open = provider.open_shard(&shard("s")).expect("open");
    let other = provider.open_shard(&shard("other")).expect("open");

    provider
        .set_stream_retention_hold("t", "ns", "s", true)
        .await
        .expect("hold");
    assert!(open.retention_held_at_commit());
    assert!(!other.retention_held_at_commit());
    let later = provider
        .open_shard(&ShardKey {
            shard: 1,
            ..shard("s")
        })
        .expect("open");
    assert!(later.retention_held_at_commit());

    provider
        .set_stream_retention_hold("t", "ns", "s", false)
        .await
        .expect("lift");
    assert!(!open.retention_held_at_commit());
    assert!(!later.retention_held_at_commit());
}

/// A provider configured to lift every hold, as a broker that does not
/// replicate is, ignores a stream asking for one.
#[tokio::test]
async fn a_lifted_provider_ignores_a_streams_hold() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(
        dir.path(),
        LogConfig {
            retention_hold: crate::log::RetentionHold::Lifted,
            ..config()
        },
    )
    .expect("provider");
    let open = provider.open_shard(&shard("s")).expect("open");
    provider
        .set_stream_retention_hold("t", "ns", "s", true)
        .await
        .expect("hold");
    assert!(!open.retention_held_at_commit());
    let later = provider
        .open_shard(&ShardKey {
            shard: 1,
            ..shard("s")
        })
        .expect("open");
    assert!(!later.retention_held_at_commit());
}

#[tokio::test]
async fn a_new_incarnation_never_sees_the_old_ones_records() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    provider.set_stream_incarnation("t", "ns", "orders", Some(7));
    let old = provider.open_shard(&shard("orders")).expect("open");
    old.append(&[record("a"), record("b")])
        .await
        .expect("append");

    // A broker that missed the delete: the new incarnation is set while the
    // old log is still open, and still gets a log of its own.
    provider.set_stream_incarnation("t", "ns", "orders", Some(8));
    let new = provider.open_shard(&shard("orders")).expect("open new");
    assert_eq!(new.tail_offset().await.expect("tail"), 0);
    assert!(!old.is_closed());

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire");
    assert!(old.is_closed());
    let reopened = provider
        .opened_shard(&shard("orders"))
        .expect("new still open");
    assert_eq!(reopened.tail_offset().await.expect("tail"), 0);
}

#[tokio::test]
async fn a_retired_stream_opens_nothing_and_comes_back_empty() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    let old = provider.open_shard(&shard("orders")).expect("open");
    old.append(&[record("a")]).await.expect("append");

    provider.set_stream_incarnation("t", "ns", "orders", None);
    provider
        .retire_stream("t", "ns", "orders", 0)
        .await
        .expect("retire");
    assert!(old.is_closed());
    assert!(matches!(
        provider.open_shard(&shard("orders")),
        Err(StorageError::Closed(_))
    ));
    assert!(!layout::shard_dir(dir.path(), &shard("orders")).exists());

    // Created again at the same incarnation, as an older control plane does.
    provider.set_stream_incarnation("t", "ns", "orders", Some(0));
    let fresh = provider.open_shard(&shard("orders")).expect("open again");
    assert_eq!(fresh.tail_offset().await.expect("tail"), 0);
}

#[tokio::test]
async fn with_registration_required_an_unregistered_stream_opens_nothing() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    provider.require_registration();
    assert!(matches!(
        provider.open_shard(&shard("orders")),
        Err(StorageError::Closed(_))
    ));
    assert!(!layout::shard_dir(dir.path(), &shard("orders")).exists());

    provider.set_stream_incarnation("t", "ns", "orders", Some(3));
    provider.open_shard(&shard("orders")).expect("open");
    assert!(layout::incarnation_dir(dir.path(), &shard("orders"), 3).is_dir());
}

/// The directory index is read from disk once; one created after that is
/// still found and retired.
#[tokio::test]
async fn a_directory_opened_after_the_index_was_read_is_still_retired() {
    let dir = tempdir().expect("dir");
    let provider = DiskLogProvider::new(dir.path(), config()).expect("provider");
    provider.set_stream_incarnation("t", "ns", "orders", Some(7));
    assert!(
        provider
            .stream_incarnations_on_disk("t", "ns", "orders")
            .expect("scan")
            .is_empty()
    );
    let log = provider.open_shard(&shard("orders")).expect("open");
    log.append(&[record("a")]).await.expect("append");
    assert_eq!(
        provider
            .stream_incarnations_on_disk("t", "ns", "orders")
            .expect("scan")
            .into_iter()
            .collect::<Vec<_>>(),
        vec![7]
    );
    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire");
    assert!(!layout::incarnation_dir(dir.path(), &shard("orders"), 7).exists());
}

#[tokio::test]
async fn retiring_a_stream_deletes_its_offloaded_copies() {
    let dir = tempdir().expect("dir");
    let archive = dir.path().join("archive");
    let config = LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 120,
        retention_check_interval: Duration::from_secs(3600),
        offload: Some(crate::log::OffloadTarget::LocalDir {
            dir: archive.clone(),
            node: "node-a".into(),
        }),
        ..config()
    };
    let provider = DiskLogProvider::new(dir.path().join("logs"), config).expect("provider");
    let mut copies = Vec::new();
    for stream in ["orders", "payments"] {
        provider.set_stream_incarnation("t", "ns", stream, Some(7));
        let log = provider.open_shard(&shard(stream)).expect("open");
        for i in 0..24 {
            log.append(&[record(&format!("record-{i:02}"))])
                .await
                .expect("append");
        }
        log.sync().await.expect("sync");
        log.enforce_retention_now().await.expect("offload pass");
        let name = layout::incarnation_dir(provider.root(), &shard(stream), 7)
            .file_name()
            .expect("name")
            .to_owned();
        let copied = archive.join("node-a").join(name);
        assert!(
            std::fs::read_dir(&copied).expect("copies").count() > 0,
            "{stream} offloaded nothing"
        );
        copies.push(copied);
    }

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire");
    assert!(!copies[0].exists(), "the deleted stream's copies stay");
    assert!(
        std::fs::read_dir(&copies[1]).expect("copies").count() > 0,
        "another stream's copies went too"
    );
    // Again, with nothing left to do.
    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire again");
}

/// A provider offloading to `archive`, with `stream` at `incarnation` holding
/// offloaded copies. Returns the stream's copy directory in the archive.
async fn offloaded(
    provider: &DiskLogProvider,
    archive: &std::path::Path,
    stream: &str,
    incarnation: u64,
) -> std::path::PathBuf {
    provider.set_stream_incarnation("t", "ns", stream, Some(incarnation));
    let log = provider.open_shard(&shard(stream)).expect("open");
    for i in 0..24 {
        log.append(&[record(&format!("record-{i:02}"))])
            .await
            .expect("append");
    }
    log.sync().await.expect("sync");
    log.enforce_retention_now().await.expect("offload pass");
    let name = layout::incarnation_dir_name(&shard(stream), incarnation);
    let copied = archive.join("node-a").join(name);
    assert!(
        std::fs::read_dir(&copied).expect("copies").count() > 0,
        "{stream}/{incarnation} offloaded nothing"
    );
    copied
}

fn offloading_config(archive: &std::path::Path) -> LogConfig {
    LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 120,
        retention_check_interval: Duration::from_secs(3600),
        offload: Some(crate::log::OffloadTarget::LocalDir {
            dir: archive.to_path_buf(),
            node: "node-a".into(),
        }),
        ..config()
    }
}

#[tokio::test]
async fn retiring_one_incarnation_keeps_the_copies_of_the_others() {
    let dir = tempdir().expect("dir");
    let archive = dir.path().join("archive");
    let provider = DiskLogProvider::new(dir.path().join("logs"), offloading_config(&archive))
        .expect("provider");
    let mut copies = Vec::new();
    for incarnation in [0, 7, 8] {
        copies.push(offloaded(&provider, &archive, "orders", incarnation).await);
    }

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retire");
    assert!(!copies[1].exists(), "incarnation 7's copies stay");
    for kept in [&copies[0], &copies[2]] {
        assert!(
            std::fs::read_dir(kept).expect("copies").count() > 0,
            "{} lost its copies",
            kept.display()
        );
    }
}

#[tokio::test]
async fn a_manifest_that_cannot_be_read_keeps_the_shard_for_the_retry() {
    let dir = tempdir().expect("dir");
    let archive = dir.path().join("archive");
    let provider = DiskLogProvider::new(dir.path().join("logs"), offloading_config(&archive))
        .expect("provider");
    let copied = offloaded(&provider, &archive, "orders", 7).await;
    let local = layout::incarnation_dir(provider.root(), &shard("orders"), 7);
    let manifest = super::super::offload::manifest::path_in(&local);
    let saved = std::fs::read(&manifest).expect("manifest");
    // A directory in its place reads as an I/O error, not as bad content.
    std::fs::remove_file(&manifest).expect("remove");
    std::fs::create_dir(&manifest).expect("dir in its place");

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect_err("an unreadable manifest must not retire the shard");
    assert!(local.exists(), "the local folder went");
    assert!(manifest.exists(), "the manifest went");
    assert!(copied.exists(), "the copies went");

    std::fs::remove_dir(&manifest).expect("clear");
    std::fs::write(&manifest, saved).expect("restore");
    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retry");
    assert!(!local.exists() && !copied.exists());
}

#[tokio::test]
async fn a_retire_that_fails_partway_completes_on_retry() {
    let dir = tempdir().expect("dir");
    let archive = dir.path().join("archive");
    let provider = DiskLogProvider::new(dir.path().join("logs"), offloading_config(&archive))
        .expect("provider");
    let copied = offloaded(&provider, &archive, "orders", 7).await;
    let local = layout::incarnation_dir(provider.root(), &shard("orders"), 7);
    let recorded = super::super::offload::manifest::load(&local).expect("manifest");
    assert!(recorded.entries().len() > 1, "need several copies");
    // The last recorded copy cannot be removed as a file, after the earlier
    // ones already were.
    let last = archive.join(&recorded.entries().last().expect("entry").key);
    std::fs::remove_file(&last).expect("remove");
    std::fs::create_dir(&last).expect("dir in its place");
    std::fs::write(last.join("pin"), b"x").expect("pin");

    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect_err("the copy that cannot be removed fails the retire");
    assert!(local.exists(), "the local folder went on a failed retire");
    assert!(
        super::super::offload::manifest::path_in(&local).exists(),
        "the manifest went on a failed retire"
    );

    std::fs::remove_dir_all(&last).expect("clear");
    provider
        .retire_stream("t", "ns", "orders", 7)
        .await
        .expect("retry completes, copies already gone count as done");
    assert!(!local.exists() && !copied.exists());
}
