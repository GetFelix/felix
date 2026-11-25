//! A caller that stops waiting on a flush or a roll must not lose its
//! failure: later appends would be acknowledged on top of a failed sync.

use std::time::Duration;

use tokio::sync::mpsc;

use super::*;
use crate::disk_log::flush::{FakeRing, RingFsync};

/// Routes the log's flushes through a ring the test answers by hand.
fn fake_ring(log: &DiskLog) -> mpsc::UnboundedReceiver<RingFsync> {
    let (tx, rx) = mpsc::unbounded_channel();
    *log.inner.fake_ring.lock() = Some(FakeRing(tx));
    rx
}

fn tail(log: &DiskLog) -> Offset {
    log.inner.segments.read().tail_offset()
}

/// Start a group-commit flush to the tail, wait until its fsync is in the
/// ring, then drop the caller.
async fn drop_a_flush_in_the_ring(
    log: &DiskLog,
    ring: &mut mpsc::UnboundedReceiver<RingFsync>,
) -> RingFsync {
    let inner = Arc::clone(&log.inner);
    let target = tail(log);
    let flush = tokio::spawn(async move { inner.ensure_durable(target).await });
    let fsync = tokio::time::timeout(Duration::from_secs(5), ring.recv())
        .await
        .expect("the flush never reached the ring")
        .expect("ring");
    flush.abort();
    let _ = flush.await;
    fsync
}

/// A log pinned to the flush thread keeps its flushes there with a ring
/// switched on, so tests that hold that thread do not race the tests that
/// force the ring for the whole process.
#[tokio::test]
async fn a_log_pinned_to_the_flush_thread_skips_the_ring() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let mut ring = fake_ring(&log);
    pin_to_flush_thread(&log);
    log.append(&records(&["a"])).await.expect("append");

    tokio::time::timeout(Duration::from_secs(5), log.sync())
        .await
        .expect("the flush went to the ring")
        .expect("sync");
    assert!(ring.try_recv().is_err(), "an fsync reached the ring");
    assert_eq!(log.durable_offset(), tail(&log));
}

/// The flush future is dropped while its fsync is in the ring, and the fsync
/// then fails. The log must stop anyway.
#[tokio::test]
async fn a_dropped_ring_flush_that_fails_still_poisons_the_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let mut ring = fake_ring(&log);
    log.append(&records(&["a"])).await.expect("append");

    let a = drop_a_flush_in_the_ring(&log, &mut ring).await;
    assert!(!log.is_poisoned(), "poisoned before the fsync failed");
    a.answer(Err(std::io::Error::from_raw_os_error(libc::EIO)));

    assert!(log.is_poisoned());
    assert_eq!(log.durable_offset(), 0);
    let refused = log.append(&records(&["b"])).await;
    assert!(
        matches!(refused, Err(StorageError::Poisoned { .. })),
        "{refused:?}"
    );
}

/// Flush A is dropped with its fsync still in the ring, and flush B starts.
/// The ring runs fsyncs concurrently and reports a writeback error to only
/// one of them, so B's could succeed and be reaped before A's fails. B must
/// not call anything durable on that success.
#[tokio::test]
async fn a_later_flush_is_not_made_durable_past_an_earlier_one_still_in_the_ring() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let mut ring = fake_ring(&log);
    log.append(&records(&["a"])).await.expect("append");
    let a = drop_a_flush_in_the_ring(&log, &mut ring).await;

    log.append(&records(&["b"])).await.expect("append");
    let inner = Arc::clone(&log.inner);
    let target = tail(&log);
    let b = tokio::spawn(async move { inner.ensure_durable(target).await });
    // B's fsync, if it reaches the ring, completes first and succeeds.
    if let Ok(Some(b_fsync)) = tokio::time::timeout(Duration::from_millis(200), ring.recv()).await {
        b_fsync.answer(Ok(()));
    }
    tokio::task::yield_now().await;
    a.answer(Err(std::io::Error::from_raw_os_error(libc::EIO)));

    let b = tokio::time::timeout(Duration::from_secs(5), b)
        .await
        .expect("flush B never finished")
        .expect("join");
    assert!(b.is_err(), "B acknowledged on top of A's failure: {b:?}");
    assert_eq!(log.durable_offset(), 0, "B's records were marked durable");
    assert!(log.is_poisoned());
}

/// The ring's thread panics with one of this log's fsyncs in flight and
/// nobody waiting on it. Unwinding drops that fsync unanswered, and the next
/// flush finds the ring gone and falls back to a plain fsync of the same
/// file. If the lost fsync had failed, the kernel would have used up the
/// error on it and the fallback would succeed, so it must never run.
#[tokio::test]
async fn a_ring_thread_that_panics_mid_flight_poisons_the_log() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let mut ring = fake_ring(&log);
    log.append(&records(&["a"])).await.expect("append");
    let a = drop_a_flush_in_the_ring(&log, &mut ring).await;

    let thread = std::thread::spawn(move || {
        let _in_flight = (a, ring);
        panic!("the ring thread panics holding an fsync");
    });
    assert!(thread.join().is_err());

    // Refused once poisoned; either way, nothing may become durable.
    let _ = log.append(&records(&["b"])).await;
    let flushed = tokio::time::timeout(Duration::from_secs(5), log.sync())
        .await
        .expect("the flush never finished");
    assert!(
        flushed.is_err(),
        "a flush fell back past a lost fsync: {flushed:?}"
    );
    assert_eq!(log.durable_offset(), 0, "records were marked durable");
    assert!(log.is_poisoned());
}

/// The same with the caller still waiting: it hears an error, never success.
#[tokio::test]
async fn a_ring_thread_that_panics_fails_the_waiting_flush() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    let mut ring = fake_ring(&log);
    log.append(&records(&["a"])).await.expect("append");
    let inner = Arc::clone(&log.inner);
    let target = tail(&log);
    let flush = tokio::spawn(async move { inner.ensure_durable(target).await });
    let a = tokio::time::timeout(Duration::from_secs(5), ring.recv())
        .await
        .expect("the flush never reached the ring")
        .expect("ring");

    let thread = std::thread::spawn(move || {
        let _in_flight = (a, ring);
        panic!("the ring thread panics holding an fsync");
    });
    assert!(thread.join().is_err());

    let flushed = tokio::time::timeout(Duration::from_secs(5), flush)
        .await
        .expect("the waiting flush was never answered")
        .expect("join");
    assert!(flushed.is_err(), "{flushed:?}");
    assert_eq!(log.durable_offset(), 0);
    assert!(log.is_poisoned());
}

/// The real ring, through the group-commit lock: flush A is dropped with its
/// fsync held in the ring, flush B's succeeds, then A's fails.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_dropped_uring_flush_that_fails_still_poisons_the_log() {
    use std::io::Write;
    use std::os::fd::OwnedFd;

    if !crate::io::uring_fsync::available_or_required() {
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

    let inner = Arc::clone(&log.inner);
    let target = tail(&log);
    let a = tokio::spawn(async move { inner.ensure_durable(target).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!a.is_finished(), "the held flush completed early");
    a.abort();
    let _ = a.await;
    assert!(!log.is_poisoned(), "poisoned before the fsync failed");

    log.append(&records(&["b"])).await.expect("append");
    let inner = Arc::clone(&log.inner);
    let target = tail(&log);
    let b = tokio::spawn(async move { inner.ensure_durable(target).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    writer.write_all(b"x").expect("release the fsync");
    let b = tokio::time::timeout(Duration::from_secs(5), b)
        .await
        .expect("flush B never finished")
        .expect("join");
    assert!(b.is_err(), "B acknowledged on top of A's failure: {b:?}");
    assert_eq!(log.durable_offset(), 0, "B's records were marked durable");
    assert!(
        log.is_poisoned(),
        "a failed fsync nobody waited for left the log running"
    );
    let refused = log.append(&records(&["c"])).await;
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
