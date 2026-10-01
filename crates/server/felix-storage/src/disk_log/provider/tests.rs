use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode, ReadRange};

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
            .expect("read");
        assert_eq!(read.len(), 1, "seed {seed} lost the acknowledged record");
        recovered.shutdown().await.expect("shutdown");
    }
    provider.shutdown().await.expect("shutdown");
}
