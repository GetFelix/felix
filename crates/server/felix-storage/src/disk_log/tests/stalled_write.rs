//! A segment write the kernel stalls, as it does once it throttles a process
//! that dirties pages faster than the device takes them, must not stall the
//! runtime or the rest of the log. And an append whose caller gives up must
//! leave the log as it would have when the write ran in the caller's poll.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::*;
use crate::CommitSequencer;
use crate::disk_log::append::HoldAt;

fn hold_next(log: &DiskLog, at: HoldAt) -> std::sync::mpsc::Sender<()> {
    let (release, held) = std::sync::mpsc::channel();
    *log.inner.hold_next_append.lock() = Some((at, held));
    release
}

async fn wait_until_held(log: &DiskLog) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !log.inner.append_held.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the append never got there");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Start an append of `payload` and return once its write is held.
async fn stall_an_append(
    log: &Arc<DiskLog>,
    payload: &'static str,
) -> (
    tokio::task::JoinHandle<Result<AppendResult>>,
    std::sync::mpsc::Sender<()>,
) {
    let release = hold_next(log, HoldAt::Write);
    let append = tokio::spawn({
        let log = Arc::clone(log);
        async move { log.append(&records(&[payload])).await }
    });
    wait_until_held(log).await;
    (append, release)
}

/// With one runtime thread, an append that wrote on it would take the whole
/// runtime with it: not even a timer could fire until the write returned.
#[tokio::test(flavor = "current_thread")]
async fn a_stalled_write_leaves_the_runtime_running() {
    let dir = tempdir().expect("dir");
    let log = Arc::new(open(&dir, FsyncMode::None));
    log.append(&records(&["before"])).await.expect("append");

    let (append, release) = stall_an_append(&log, "stalled").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        log.inner.append_held.load(Ordering::Acquire),
        "the runtime only ran again once the write had returned"
    );

    release.send(()).expect("release");
    append.await.expect("task").expect("append");
    assert_eq!(read_all(&log, 0).await, ["before", "stalled"]);
}

/// The write runs without the segment lock, so reading the tail and reading
/// records go ahead while it is held. Appends queued behind it wait without
/// holding a worker, and so does a flush, which covers what was queued before
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_write_holds_up_only_the_appends_behind_it() {
    let dir = tempdir().expect("dir");
    let log = Arc::new(open(&dir, FsyncMode::OnCommit));
    log.append(&records(&["before"])).await.expect("append");

    let (append, release) = stall_an_append(&log, "stalled").await;
    // More than there are workers, so if waiting for the write parked a
    // worker, none would be left for the checks below.
    let queued: Vec<_> = (0..4)
        .map(|i| {
            let log = Arc::clone(&log);
            tokio::spawn(async move { log.append(&records(&[&format!("queued-{i}")])).await })
        })
        .collect();

    let sync = tokio::spawn({
        let log = Arc::clone(&log);
        async move { log.sync().await }
    });

    assert_eq!(log.tail_offset().await.expect("tail"), 1);
    assert_eq!(read_all(&log, 0).await, ["before"]);
    assert!(
        log.inner.append_held.load(Ordering::Acquire),
        "the checks only finished once the write had returned"
    );
    assert!(queued.iter().all(|task| !task.is_finished()));
    assert!(!sync.is_finished());

    release.send(()).expect("release");
    append.await.expect("task").expect("append");
    for task in queued {
        task.await.expect("task").expect("append");
    }
    sync.await.expect("task").expect("sync");
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
}

/// A caller that gives up during its write leaves nothing behind: no record,
/// no spent offset, no claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_abandoned_during_its_write_is_undone() {
    let dir = tempdir().expect("dir");
    let log = Arc::new(open(&dir, FsyncMode::None));
    let order = Arc::new(CommitSequencer::new(0));

    let release = hold_next(&log, HoldAt::Write);
    let abandoned = tokio::spawn({
        let log = Arc::clone(&log);
        let order = Arc::clone(&order);
        async move { log.append_claimed(&records(&["abandoned"]), &order).await }
    });
    wait_until_held(&log).await;
    abandoned.abort();
    let _ = abandoned.await;
    release.send(()).expect("release");

    let (pending, turn) = log
        .append_claimed(&records(&["next"]), &order)
        .await
        .expect("append");
    assert_eq!(
        pending.first_offset(),
        0,
        "the abandoned batch spent an offset"
    );
    turn.wait().await.expect("turn");
    drop(turn);
    assert_eq!(read_all(&log, 0).await, ["next"]);

    drop(log);
    let reopened = open(&dir, FsyncMode::None);
    assert_eq!(read_all(&reopened, 0).await, ["next"]);
}

/// A caller that gives up just after its batch is kept does not hear its
/// offsets, but the claim made for it is released with the reply it never
/// read, so the commit order moves on past the batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_whose_caller_gave_up_is_released() {
    let dir = tempdir().expect("dir");
    let log = Arc::new(open(&dir, FsyncMode::None));
    let order = Arc::new(CommitSequencer::new(0));

    let release = hold_next(&log, HoldAt::Reply);
    let abandoned = tokio::spawn({
        let log = Arc::clone(&log);
        let order = Arc::clone(&order);
        async move { log.append_claimed(&records(&["kept"]), &order).await }
    });
    wait_until_held(&log).await;
    abandoned.abort();
    let _ = abandoned.await;
    release.send(()).expect("release");

    let (pending, turn) = log
        .append_claimed(&records(&["next"]), &order)
        .await
        .expect("append");
    assert_eq!(pending.first_offset(), 1, "the kept batch holds offset 0");
    tokio::time::timeout(Duration::from_secs(5), turn.wait())
        .await
        .expect("the abandoned batch's range was never released")
        .expect("turn");
}
