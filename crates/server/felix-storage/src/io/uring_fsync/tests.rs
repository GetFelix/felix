use std::io::Write;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use super::*;

fn temp_file(dir: &std::path::Path, name: &str) -> Arc<File> {
    let mut file = std::fs::File::options()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(dir.join(name))
        .expect("open");
    file.write_all(b"record").expect("write");
    Arc::new(crate::fs::from_os(file))
}

/// A flush must reach the kernel while another operation is still in the
/// ring. One log's slow device sync must not hold back every other log. (A
/// log's own fsyncs do wait for each other, in `disk_log::flush`.)
#[tokio::test]
async fn a_new_flush_is_not_held_behind_an_outstanding_one() {
    if !available_or_required() {
        return;
    }
    // A poll on an empty pipe stands in for a slow fsync: it stays in the
    // ring until the test writes to the pipe.
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let held = tokio::spawn(poll_readable(gate));
    // Let the gate reach the ring and the service thread go back to waiting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let flushed =
        tokio::time::timeout(Duration::from_secs(2), fsync(file, OnFailure::new(|_| {}))).await;

    writer.write_all(b"x").expect("release gate");
    let released = held.await.expect("join");
    assert!(matches!(released, Some(Ok(()))), "gate: {released:?}");
    let flushed = flushed.expect("flush waited behind an unrelated outstanding operation");
    assert!(matches!(flushed, Some(Ok(()))), "flush: {flushed:?}");
}

/// More operations than the ring has entries, all held in flight at once.
/// The overflow has to wait for room, not come back as an I/O error: a failed
/// flush poisons its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_requests_than_ring_entries_wait_rather_than_fail() {
    if !available_or_required() {
        return;
    }
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let requests = RING_ENTRIES as usize * 4;
    let held: Vec<_> = (0..requests)
        .map(|_| tokio::spawn(poll_readable(Arc::clone(&gate))))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        held.iter().all(|task| !task.is_finished()),
        "an operation finished before the gate opened",
    );

    writer.write_all(b"x").expect("release gate");
    for task in held {
        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("an operation never completed")
            .expect("join");
        assert!(matches!(outcome, Some(Ok(()))), "{outcome:?}");
    }
}

/// A ring of the test's own, so an injected submit error cannot fail the
/// flushes of other tests sharing the process-wide one.
fn private_ring() -> Option<&'static Ring> {
    Ring::start().map(|ring| &*Box::leak(Box::new(ring)))
}

/// Wait until only the test holds `file`, i.e. the ring has let it go.
async fn released(file: &Arc<File>) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Arc::strong_count(file) > 1 {
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

/// A submit error with an operation in flight. Its waiter and the new one are
/// told the error, never success, and the in-flight file stays open until the
/// kernel's completion is reaped: closing it early would let the kernel act on
/// a reused descriptor.
#[tokio::test]
async fn a_submit_error_fails_waiters_but_keeps_in_flight_files_open() {
    let Some(ring) = private_ring() else {
        skip_without_ring();
        return;
    };
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let held = tokio::spawn(submit_to(ring, Arc::clone(&gate), Op::PollReadable, None));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !held.is_finished(),
        "the gate finished before it was opened"
    );

    ring.submit_fault.store(libc::EINVAL, Ordering::SeqCst);
    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let flushed = tokio::time::timeout(
        Duration::from_secs(2),
        submit_to(ring, Arc::clone(&file), Op::Fsync, None),
    )
    .await
    .expect("the flush waiter was never answered");
    assert!(matches!(flushed, Some(Err(_))), "flush: {flushed:?}");
    let gated = tokio::time::timeout(Duration::from_secs(2), held)
        .await
        .expect("the in-flight waiter was never answered")
        .expect("join");
    assert!(matches!(gated, Some(Err(_))), "gate: {gated:?}");

    assert_eq!(
        Arc::strong_count(&gate),
        2,
        "the in-flight file was dropped before its completion was reaped",
    );
    writer.write_all(b"x").expect("release gate");
    assert!(released(&gate).await, "the ring never let go of the gate");
    assert!(released(&file).await, "the ring never let go of the log");

    let after = tokio::time::timeout(
        Duration::from_secs(2),
        submit_to(ring, Arc::clone(&file), Op::Fsync, None),
    )
    .await
    .expect("a flush after the error was never answered");
    assert!(matches!(after, Some(Ok(()))), "after: {after:?}");
}

/// `EBUSY` asks for completions to be reaped before the kernel takes more.
/// Nothing failed, so nothing may be reported as failed: a failed flush
/// poisons its log.
#[tokio::test]
async fn a_busy_ring_delays_flushes_rather_than_failing_them() {
    let Some(ring) = private_ring() else {
        skip_without_ring();
        return;
    };
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let held = tokio::spawn(submit_to(ring, Arc::clone(&gate), Op::PollReadable, None));
    tokio::time::sleep(Duration::from_millis(50)).await;

    ring.submit_fault.store(libc::EBUSY, Ordering::SeqCst);
    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let flushed = tokio::time::timeout(
        Duration::from_secs(2),
        submit_to(ring, file, Op::Fsync, None),
    )
    .await
    .expect("the flush waiter was never answered");
    assert!(matches!(flushed, Some(Ok(()))), "flush: {flushed:?}");

    writer.write_all(b"x").expect("release gate");
    let gated = tokio::time::timeout(Duration::from_secs(2), held)
        .await
        .expect("the in-flight waiter was never answered")
        .expect("join");
    assert!(matches!(gated, Some(Ok(()))), "gate: {gated:?}");
}

/// N logs flushing concurrently through the ring. Prints the per-flush
/// latency so a change to the service loop can be compared before and after:
/// `cargo test -p felix-storage --lib uring_fsync::tests::concurrent_flush_latency -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement, not a check"]
async fn concurrent_flush_latency() {
    if !available_or_required() {
        return;
    }
    let dir = tempdir().expect("dir");
    for logs in [1usize, 4, 12] {
        let mut tasks = Vec::new();
        for i in 0..logs {
            let file = temp_file(dir.path(), &format!("log-{logs}-{i}"));
            tasks.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(500);
                for _ in 0..500 {
                    (&*file).write_all(&[0u8; 4096]).expect("write");
                    let start = Instant::now();
                    let outcome = fsync(Arc::clone(&file), OnFailure::new(|_| {})).await;
                    assert!(matches!(outcome, Some(Ok(()))), "{outcome:?}");
                    samples.push(start.elapsed());
                }
                samples
            }));
        }
        let mut all = Vec::new();
        for task in tasks {
            all.extend(task.await.expect("join"));
        }
        all.sort();
        let mean = all.iter().sum::<Duration>() / all.len() as u32;
        let p = |q: f64| all[((all.len() - 1) as f64 * q) as usize];
        eprintln!(
            "logs={logs:>2} flushes={} mean={mean:?} p50={:?} p99={:?}",
            all.len(),
            p(0.50),
            p(0.99)
        );
    }
}

/// A flush that fails after its caller stopped waiting still reports the
/// failure: the hook runs where the completion is reaped.
#[tokio::test]
async fn a_failure_reaped_after_the_caller_left_still_runs_the_hook() {
    if !available_or_required() {
        return;
    }
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    hold_and_fail_next_fsync(Arc::clone(&file), gate, libc::EIO);

    let (failed_tx, failed) = std::sync::mpsc::channel();
    let flush = tokio::spawn(fsync(
        file,
        OnFailure::new(move |err: &io::Error| {
            let _ = failed_tx.send(err.raw_os_error());
        }),
    ));
    // Let the flush reach the ring, then give up on it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!flush.is_finished(), "the held flush completed early");
    flush.abort();
    let _ = flush.await;

    writer.write_all(b"x").expect("release gate");
    let reported = failed
        .recv_timeout(Duration::from_secs(5))
        .expect("the failure was lost with the caller");
    assert_eq!(reported, Some(libc::EIO));
}

/// The service thread panics with a flush in flight. Unwinding must fail that
/// flush, running its hook before its waiter hears, and a request after the
/// thread is gone falls back without the hook running: it never reached a
/// ring, so there is nothing to answer for.
#[tokio::test]
async fn a_panicking_service_thread_fails_its_in_flight_flushes() {
    let Some(ring) = private_ring() else {
        skip_without_ring();
        return;
    };
    let (reader, _writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(crate::fs::from_os(std::fs::File::from(OwnedFd::from(
        reader,
    ))));
    let failed = Arc::new(AtomicBool::new(false));
    let hook = {
        let failed = Arc::clone(&failed);
        OnFailure::new(move |_| failed.store(true, Ordering::SeqCst))
    };
    let held = tokio::spawn(submit_to(
        ring,
        Arc::clone(&gate),
        Op::PollReadable,
        Some(hook),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!held.is_finished(), "the held operation finished early");

    ring.panic_fault.store(true, Ordering::SeqCst);
    ring.wake.signal();
    let answered = tokio::time::timeout(Duration::from_secs(5), held)
        .await
        .expect("the in-flight waiter was never answered")
        .expect("join");
    assert!(matches!(answered, Some(Err(_))), "{answered:?}");
    assert!(
        failed.load(Ordering::SeqCst),
        "the waiter heard before the hook ran"
    );

    let late = Arc::new(AtomicBool::new(false));
    let hook = {
        let late = Arc::clone(&late);
        OnFailure::new(move |_| late.store(true, Ordering::SeqCst))
    };
    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let fallback = submit_to(ring, file, Op::Fsync, Some(hook)).await;
    assert!(fallback.is_none(), "{fallback:?}");
    assert!(
        !late.load(Ordering::SeqCst),
        "an unsent request ran its hook"
    );
}

/// Every way an operation leaves the service thread unanswered runs its hook
/// once, before its reply.
#[test]
fn an_operation_dropped_unanswered_fails_once_before_its_reply() {
    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let ran = Arc::new(AtomicU64::new(0));
    let hook = |ran: &Arc<AtomicU64>| {
        let ran = Arc::clone(ran);
        let mut hook = OnFailure::new(move |_| {
            ran.fetch_add(1, Ordering::SeqCst);
        });
        hook.submitted();
        hook
    };

    // Registered in the ring.
    let (reply, mut wait) = oneshot::channel();
    let in_flight = InFlight {
        _file: Arc::clone(&file),
        reply: Some(reply),
        on_failure: Some(hook(&ran)),
        fail_with: 0,
    };
    drop(in_flight);
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    assert!(matches!(wait.try_recv(), Ok(Err(_))));

    // Queued but not yet pushed.
    let (reply, mut wait) = oneshot::channel();
    let submission = Submission {
        file: Arc::clone(&file),
        op: Op::Fsync,
        on_failure: Some(hook(&ran)),
        reply,
        fail_with: 0,
    };
    drop(submission);
    assert_eq!(ran.load(Ordering::SeqCst), 2);
    assert!(wait.try_recv().is_err());

    // Answered, then dropped: the hook does not run a second time.
    let (reply, _wait) = oneshot::channel();
    let mut in_flight = InFlight {
        _file: file,
        reply: Some(reply),
        on_failure: Some(hook(&ran)),
        fail_with: 0,
    };
    in_flight.complete(Err(io::Error::from_raw_os_error(libc::EIO)));
    drop(in_flight);
    assert_eq!(ran.load(Ordering::SeqCst), 3);
}
