use std::io;
use std::path::Path;
use std::time::Duration;

use super::*;

fn held(dir: &Path) -> bool {
    match RootLock::take(dir) {
        Ok(_) => false,
        Err(err) if err.kind() == io::ErrorKind::ResourceBusy => true,
        Err(err) => panic!("lock {}: {err}", dir.display()),
    }
}

/// A guard on `dir` with one writer that stops only once the run's token is
/// cancelled and `finish` is notified, like a task caught mid-write.
fn guard_with_writer(dir: &Path) -> (RunGuard, Arc<tokio::sync::Notify>) {
    let stop = CancellationToken::new();
    let mut guard = RunGuard::new(Some(RootLock::take(dir).expect("lock")));
    guard.stop_with(&stop);
    let finish = Arc::new(tokio::sync::Notify::new());
    let writer_finish = Arc::clone(&finish);
    guard.writers().spawn(async move {
        stop.cancelled().await;
        writer_finish.notified().await;
    });
    (guard, finish)
}

/// **A closing run keeps the lock until every writer has stopped**, so a
/// second broker that races it is refused instead of beginning a run under
/// a task still writing.
#[tokio::test]
async fn a_closing_run_holds_the_lock_until_its_writers_stop() {
    let dir = tempfile::tempdir().expect("dir");
    let (guard, finish) = guard_with_writer(dir.path());
    let closing = tokio::spawn(guard.close());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        held(dir.path()),
        "the lock went while a writer was still running"
    );
    finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .expect("the close never finished")
        .expect("close");
    assert!(!held(dir.path()), "the lock outlived the closed run");
}

/// **A guard dropped without a close, as when the start's future is
/// dropped, closes on a task of its own** and lets the lock go only after.
#[tokio::test]
async fn a_dropped_guard_lets_go_only_once_closed() {
    let dir = tempfile::tempdir().expect("dir");
    let (guard, finish) = guard_with_writer(dir.path());
    drop(guard);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        held(dir.path()),
        "a dropped guard let the lock go while a writer was still running"
    );
    finish.notify_one();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while held(dir.path()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a dropped guard never let the lock go"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **With no runtime to close on, a dropped guard keeps the lock** until the
/// process exits rather than let it go with storage still open.
#[test]
fn a_guard_dropped_without_a_runtime_keeps_the_lock() {
    let dir = tempfile::tempdir().expect("dir");
    let guard = RunGuard::new(Some(RootLock::take(dir.path()).expect("lock")));
    drop(guard);
    assert!(held(dir.path()), "the lock went with nothing closed");
}
