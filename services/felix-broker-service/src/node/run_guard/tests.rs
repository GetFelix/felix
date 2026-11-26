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
    let mut guard = RunGuard::new(
        Some(RootLock::take(dir).expect("lock")),
        Duration::from_secs(30),
    );
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
    let guard = RunGuard::new(
        Some(RootLock::take(dir.path()).expect("lock")),
        Duration::from_secs(30),
    );
    drop(guard);
    assert!(held(dir.path()), "the lock went with nothing closed");
}

/// A guard holding storage opened on `dir`, as a start does, with a
/// deadline of `deadline`.
fn guard_over_storage(dir: &Path, deadline: Duration) -> RunGuard {
    let lock = crate::node::storage::lock_root(dir).expect("lock");
    let mut guard = RunGuard::new(Some(lock), deadline);
    let durable = crate::config::DurableStorageConfig {
        root: dir.to_path_buf(),
        log: Default::default(),
        offload_dir: None,
    };
    let stored = crate::node::startup::open_storage(
        &crate::config::BrokerConfig::default(),
        Some((durable, guard.lock().expect("a lock"))),
        None,
    )
    .expect("open");
    guard.opened(&stored.broker, Some((dir.to_path_buf(), stored.storage_id)));
    guard
}

fn recorded_clean(dir: &Path) -> bool {
    let id = felix_storage::storage_identity::load(dir)
        .expect("read the id")
        .expect("an id");
    felix_storage::run_state::read(dir, id).expect("read the run record")
        == felix_storage::run_state::LastRun::Clean
}

/// **A writer that does not stop in time costs the clean stop, not the
/// exit**: the close returns at its deadline, records nothing, and keeps the
/// lock until the process exits.
#[tokio::test]
async fn a_writer_that_does_not_stop_in_time_keeps_the_lock_and_no_clean_stop() {
    let dir = tempfile::tempdir().expect("dir");
    let guard = guard_over_storage(dir.path(), Duration::from_millis(200));
    guard.writers().spawn(std::future::pending::<()>());
    tokio::time::timeout(Duration::from_secs(5), guard.close())
        .await
        .expect("the close waited past its deadline");
    assert!(
        held(dir.path()),
        "the lock went with a writer still running"
    );
    assert!(
        !recorded_clean(dir.path()),
        "a run that did not stop was recorded as clean"
    );
}

/// **A client that stays connected after a late failed start does not hold
/// the start's error back**: the close gives up at its deadline and keeps
/// the lock, as for a writer.
#[tokio::test]
async fn a_connected_client_does_not_hold_up_a_failed_start() {
    let dir = tempfile::tempdir().expect("dir");
    let mut guard = guard_over_storage(dir.path(), Duration::from_millis(200));
    let connections = TaskTracker::new();
    connections.spawn(std::future::pending::<()>());
    guard.connections(&connections);
    tokio::time::timeout(Duration::from_secs(5), guard.close())
        .await
        .expect("the close waited on a connection past its deadline");
    assert!(
        held(dir.path()),
        "the lock went with a connection still open"
    );
    assert!(!recorded_clean(dir.path()));
}

/// **After a drain that was cut short, the close does not wait again**: it
/// returns at once and keeps the lock, since a writer may still run.
#[tokio::test]
async fn after_a_drain_cut_short_the_close_returns_and_keeps_the_lock() {
    let dir = tempfile::tempdir().expect("dir");
    let guard = RunGuard::new(
        Some(RootLock::take(dir.path()).expect("lock")),
        Duration::from_secs(60),
    );
    guard.writers().spawn(std::future::pending::<()>());
    guard.verdict().decide(true);
    tokio::time::timeout(Duration::from_secs(2), guard.close())
        .await
        .expect("the close waited again after the drain");
    assert!(
        held(dir.path()),
        "the lock went with a writer still running"
    );
}

/// **A run dropped after the drain decided keeps the drain's verdict**: the
/// teardown does not flush and record a clean stop over a drain that found
/// the stop unclean.
#[tokio::test]
async fn a_dropped_run_never_turns_an_unclean_drain_clean() {
    let dir = tempfile::tempdir().expect("dir");
    let guard = guard_over_storage(dir.path(), Duration::from_secs(5));
    // As the drain does when a step that writes was cut short: it records
    // no clean stop, then is dropped at a later step.
    guard.verdict().decide(true);
    drop(guard);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !recorded_clean(dir.path()),
        "the teardown recorded a clean stop over the drain's verdict"
    );
    assert!(held(dir.path()), "the lock went after an unclean drain");
}
