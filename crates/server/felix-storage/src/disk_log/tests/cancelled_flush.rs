//! A caller that stops waiting on a flush or a roll must not lose its
//! failure: later appends would be acknowledged on top of a failed sync.

use std::time::Duration;

use super::*;

/// The hook the io_uring thread runs on a failed completion stops the log,
/// on every platform.
#[tokio::test]
async fn a_failure_given_to_the_recorder_poisons_the_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a"])).await.expect("append");

    let record = log.inner.flush_failure_recorder(false);
    record(&std::io::Error::other("reaped fsync failure"));

    assert!(log.is_poisoned());
    let refused = log.append(&records(&["b"])).await;
    assert!(
        matches!(refused, Err(StorageError::Poisoned { .. })),
        "{refused:?}"
    );
}

/// The flush future is dropped while its fsync is in the ring, and the fsync
/// then fails. The log must stop anyway.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_dropped_uring_flush_that_fails_still_poisons_the_log() {
    use std::io::Write;
    use std::os::fd::OwnedFd;

    if !crate::io::uring_fsync::available() {
        eprintln!("io_uring is unavailable here; skipping");
        return;
    }
    let _ring = crate::io::uring_fsync::ForceForTests::hold();
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a"])).await.expect("append");

    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let active = log.inner.segments.read().active().sync_handle();
    crate::io::uring_fsync::hold_and_fail_next_fsync(active, gate, libc::EIO);

    let flush = tokio::spawn(Arc::clone(&log.inner).flush());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!flush.is_finished(), "the held flush completed early");
    flush.abort();
    let _ = flush.await;
    assert!(!log.is_poisoned(), "poisoned before the fsync failed");

    writer.write_all(b"x").expect("release the fsync");
    let mut poisoned = false;
    for _ in 0..500 {
        if log.is_poisoned() {
            poisoned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        poisoned,
        "a failed fsync nobody waited for left the log running"
    );
    let refused = log.append(&records(&["b"])).await;
    assert!(
        matches!(refused, Err(StorageError::Poisoned { .. })),
        "{refused:?}"
    );
}

/// A roll whose caller gave up still finishes and frees the roll slot, so the
/// next roll does not wait forever.
#[tokio::test]
async fn a_dropped_roll_still_frees_the_roll_slot() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    log.append(&records(&["a"])).await.expect("append");

    let (roll, release) = park_a_seal(&log).await;
    roll.abort();
    let _ = roll.await;
    release.send(()).expect("release the seal");

    tokio::time::timeout(Duration::from_secs(5), log.roll_now())
        .await
        .expect("the abandoned roll kept the roll slot")
        .expect("roll");
}
