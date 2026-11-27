//! Opening a Raft shard directory as a copy flushes off the runtime's
//! workers. A bulk create hands each broker hundreds of new shards, and each
//! open syncs the directory, the copy file and the new log. On a worker a
//! slow disk would hold up every task queued behind it, the peer transport
//! and the lease cache's reads among them.
//!
//! The fsync delay is process-wide, so this test has a binary of its own.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::raft_copy::{CopyRoot, Evidence, Gate, check};
use felix_storage::storage_identity;

const FLUSH: Duration = Duration::from_millis(300);

fn copy_root(root: &Path) -> CopyRoot {
    storage_identity::load_or_create(root, || [1; 16]).expect("storage id");
    let evidence = Evidence {
        witness_dir: None,
        control_plane: None,
        restored: false,
    };
    let Gate::Passed(passed) = check(root, evidence).expect("gate") else {
        panic!("a new root did not pass the gate");
    };
    let next = std::sync::atomic::AtomicU64::new(1);
    CopyRoot::open(root, passed, move || {
        next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    })
}

/// The longest gap between ticks of a task ticking every 5 ms while
/// `work` runs, and how long `work` took. A single-threaded runtime shows
/// any flush made on it as a gap.
async fn longest_stall<F: Future>(work: F) -> (F::Output, Duration, Duration) {
    let ticks = Arc::new(Mutex::new(vec![Instant::now()]));
    let ticker = tokio::spawn({
        let ticks = Arc::clone(&ticks);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                ticks.lock().expect("ticks").push(Instant::now());
            }
        }
    });
    let started = Instant::now();
    let output = work.await;
    let end = Instant::now();
    ticker.abort();
    let mut ticks = ticks.lock().expect("ticks").clone();
    // The end counts as a tick, so a runtime that never got to tick shows
    // its whole stall.
    ticks.push(end);
    let longest = ticks
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .max()
        .unwrap_or_default();
    (output, longest, end - started)
}

/// **A slow disk does not stall the runtime while a new copy opens**, nor
/// while a copy it holds opens again.
#[tokio::test]
async fn a_copy_opens_off_the_runtime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = copy_root(dir.path());
    let shard_dir = dir.path().join("shards").join("0");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let open = || root.open_copy("t/ns/orders/0/Stream", &shard_dir, "orders", config.clone());

    felix_storage::fault::set_fsync_delay(FLUSH);
    let (fresh, fresh_stall, fresh_took) = longest_stall(open()).await;
    let fresh = fresh.expect("a new copy opens");
    assert!(fresh.rotation.is_some(), "a new directory gets a copy id");
    fresh.log.close().await.expect("close");
    let (again, again_stall, again_took) = longest_stall(open()).await;
    felix_storage::fault::set_fsync_delay(Duration::ZERO);
    let again = again.expect("the copy opens again");
    assert_eq!(again.copy, fresh.copy, "a copy it holds keeps its id");
    again.log.close().await.expect("close");

    assert!(
        fresh_took >= FLUSH,
        "the open flushed nothing: {fresh_took:?}"
    );
    for (what, stall, took) in [
        ("a new copy", fresh_stall, fresh_took),
        ("a copy held", again_stall, again_took),
    ] {
        assert!(
            stall < FLUSH / 2,
            "the runtime stalled {stall:?} while {what} opened in {took:?}"
        );
    }
}
