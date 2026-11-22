//! The flush queue: appends that wait for a flush are answered by one
//! task, in order, and an append with nothing to wait for is answered
//! before it returns.

use std::time::Duration;

use futures_util::FutureExt;
use openraft::storage::RaftLogStorage;

use super::writes::append;
use super::*;
use crate::disk_log::PendingAppend;
use crate::log::{Epoch, WrittenBy};

/// Appends that arrive while a flush is held are all answered by the one
/// flush that follows, and not before it.
#[tokio::test]
async fn one_flush_answers_every_waiting_append() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();

    let held = log.hold_flushes().await;
    let mut waiting = Vec::new();
    for index in 0..5 {
        let entry = normal(1, index, "command");
        waiting.push(append(&mut raft, &[entry]).await.expect("append"));
    }
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    for flushed in &mut waiting {
        assert!(
            flushed.as_mut().now_or_never().is_none(),
            "a callback fired before a flush"
        );
    }
    let before = log.flushes_performed();

    drop(held);
    for flushed in waiting {
        flushed.await.expect("flush");
    }
    assert_eq!(log.durable_offset(), 5);
    assert_eq!(log.flushes_performed() - before, 1);
}

/// A flush that fails is reported to every append that was waiting on it,
/// and a truncation after it does not wait for ever.
#[tokio::test]
async fn a_failed_flush_fails_every_waiting_append() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();

    let held = log.hold_flushes().await;
    log.fail_next_flush();
    let mut waiting = Vec::new();
    for index in 0..3 {
        let entry = normal(1, index, "command");
        waiting.push(append(&mut raft, &[entry]).await.expect("append"));
    }
    drop(held);
    for flushed in waiting {
        flushed.await.expect_err("the flush failed");
    }

    let cut = raft.truncate_after(Some(log_id(1, 0)));
    tokio::time::timeout(std::time::Duration::from_secs(10), cut)
        .await
        .expect("the truncation waited on callbacks that had fired")
        .expect_err("the log is poisoned");
}

/// Once a flush has covered an append there is nothing left to wait for:
/// the queue is empty and the log answers the append at once, which is
/// what lets `append` fire a callback before it returns.
#[tokio::test]
async fn a_covered_append_is_answered_at_once() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();

    let first = write(&log, 0).await;
    append(&mut raft, &[normal(1, 1, "command")])
        .await
        .expect("append")
        .await
        .expect("flush");

    assert!(raft.shared.flushes.is_empty());
    log.commit_now(&first)
        .expect("a flush covered the append")
        .expect("flush");
}

/// A callback that panics takes the flush task with it. The appends still
/// queued are failed, and a truncation does not wait for them for ever.
#[tokio::test]
async fn a_stopped_flush_task_fails_what_it_held() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log().clone();
    let write = |offset| write(&log, offset);

    let held = log.hold_flushes().await;
    let first = write(0).await;
    raft.shared
        .flushes
        .push(&log, first, Box::new(|_| panic!("a callback")), None);
    let (answered, answer) = tokio::sync::oneshot::channel();
    let second = write(1).await;
    raft.shared.flushes.push(
        &log,
        second,
        Box::new(move |flushed| drop(answered.send(flushed))),
        None,
    );
    // Queued behind the first two, in a later batch.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let (answered_late, late) = tokio::sync::oneshot::channel();
    let third = write(2).await;
    raft.shared.flushes.push(
        &log,
        third,
        Box::new(move |flushed| drop(answered_late.send(flushed))),
        None,
    );
    drop(held);

    answer
        .await
        .expect("answered")
        .expect_err("the task stopped");
    late.await.expect("answered").expect_err("the task stopped");
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        raft.shared.flushes.drained(),
    )
    .await
    .expect("the queue never emptied");
}

/// A task that stopped is replaced by the next append that has to wait,
/// which is answered by a flush, not failed.
#[tokio::test]
async fn an_append_after_the_task_stopped_is_flushed() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log().clone();

    let first = write(&log, 0).await;
    raft.shared
        .flushes
        .push(&log, first, Box::new(|_| panic!("a callback")), None);
    tokio::time::timeout(Duration::from_secs(10), raft.shared.flushes.drained())
        .await
        .expect("the queue never emptied");

    let (answered, answer) = tokio::sync::oneshot::channel();
    let second = write(&log, 1).await;
    raft.shared.flushes.push(
        &log,
        second,
        Box::new(move |flushed| drop(answered.send(flushed))),
        None,
    );
    answer.await.expect("answered").expect("flushed");
    assert_eq!(log.durable_offset(), 2);
}

/// A task whose runtime goes before it ever runs still fails what was sent
/// to it, so a truncation does not wait for those callbacks for ever.
#[test]
fn a_task_dropped_before_it_ran_fails_what_was_sent() {
    let dir = tempdir().expect("dir");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let _entered = runtime.enter();
    let raft = open(&dir);
    let log = raft.disk_log().clone();
    let pending = runtime.block_on(write(&log, 0));
    // Held, so the append goes to the task rather than the flush thread.
    let held = runtime.block_on(log.hold_flushes());

    let (answered, mut answer) = tokio::sync::oneshot::channel();
    let unpolled = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    // Returns without yielding, so the spawned task never runs.
    unpolled.block_on(async {
        raft.shared.flushes.push(
            &log,
            pending,
            Box::new(move |flushed| drop(answered.send(flushed))),
            None,
        );
    });
    drop(unpolled);

    answer
        .try_recv()
        .expect("answered")
        .expect_err("the task stopped");
    assert!(raft.shared.flushes.is_empty());
    drop(held);
}

/// An append still queued when the last user of the log goes is answered
/// with the flush, not a failure.
#[tokio::test]
async fn a_queued_append_outlives_the_log() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();

    let held = log.hold_flushes().await;
    let flushed = append(&mut raft, &[normal(1, 0, "command")])
        .await
        .expect("append");
    drop(raft);
    drop(held);
    tokio::time::timeout(Duration::from_secs(10), flushed)
        .await
        .expect("answered")
        .expect("flushed");
}

/// What `push` answered, and the thread it answered on.
type Answered = tokio::sync::oneshot::Receiver<(io::Result<()>, Option<String>)>;

/// Queue `pending` with a callback that reports its thread.
fn push(raft: &RaftLog<Types, Codec>, log: &DiskLog, pending: PendingAppend) -> Answered {
    let (answered, answer) = tokio::sync::oneshot::channel();
    raft.shared.flushes.push(
        log,
        pending,
        Box::new(move |flushed| {
            let thread = std::thread::current().name().map(str::to_owned);
            drop(answered.send((flushed, thread)));
        }),
        None,
    );
    answer
}

/// An append with nothing queued ahead of it is answered by the flush
/// thread itself, with no task in between, and only once its fsync is
/// done: held before the fsync, the callback has not fired and nothing is
/// durable.
#[tokio::test]
async fn the_flush_thread_answers_an_append_only_after_its_fsync() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log().clone();
    let pending = write(&log, 0).await;
    let pause = std::sync::Arc::new(std::sync::Barrier::new(2));
    log.pause_next_sync(std::sync::Arc::clone(&pause));

    let mut answer = push(&raft, &log, pending);
    pause.wait();
    std::thread::sleep(Duration::from_millis(20));
    assert!(answer.try_recv().is_err(), "answered before the fsync");
    assert_eq!(log.durable_offset(), 0);
    assert!(!raft.shared.flushes.is_empty());

    pause.wait();
    let (flushed, thread) = answer.await.expect("answered");
    flushed.expect("flushed");
    assert_eq!(thread.as_deref(), Some("felix-flush"));
    assert_eq!(log.durable_offset(), 1);
    assert!(raft.shared.flushes.is_empty());
}

/// An append queued behind a flush the flush thread is answering goes to
/// the task, and is answered after the one ahead of it.
#[tokio::test]
async fn an_append_behind_a_flush_thread_answer_is_answered_after_it() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log().clone();
    let first = write(&log, 0).await;
    let pause = std::sync::Arc::new(std::sync::Barrier::new(2));
    log.pause_next_sync(std::sync::Arc::clone(&pause));
    let mut ahead = push(&raft, &log, first);
    pause.wait();

    let second = write(&log, 1).await;
    let behind = push(&raft, &log, second);
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(ahead.try_recv().is_err(), "answered before the fsync");

    pause.wait();
    let (flushed, thread) = behind.await.expect("answered");
    flushed.expect("flushed");
    assert_ne!(thread.as_deref(), Some("felix-flush"), "not from the task");
    let (flushed, _) = ahead
        .try_recv()
        .expect("the append ahead was answered first");
    flushed.expect("flushed");
    assert_eq!(log.durable_offset(), 2);
}

/// A truncation waits for an append the flush thread is still answering.
#[tokio::test]
async fn a_truncation_waits_for_a_flush_thread_answer() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();
    let pause = std::sync::Arc::new(std::sync::Barrier::new(2));
    log.pause_next_sync(std::sync::Arc::clone(&pause));
    let entries = [blank(1, 0), normal(1, 1, "a"), normal(1, 2, "b")];
    let flushed = append(&mut raft, &entries).await.expect("append");
    pause.wait();

    let mut cutter = raft.clone();
    let mut cut = std::pin::pin!(cutter.truncate_after(Some(log_id(1, 0))));
    for _ in 0..20 {
        assert!(
            cut.as_mut().now_or_never().is_none(),
            "cut under a flush in flight"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    pause.wait();
    cut.await.expect("truncate");
    flushed
        .now_or_never()
        .expect("the callback had not fired when the truncation returned")
        .expect("flush");
    assert_eq!(all(&raft).await.expect("read"), entries[..1]);
}

/// One record at `offset`, written but not yet waited for.
async fn write(log: &DiskLog, offset: u64) -> PendingAppend {
    let writers = WrittenBy::Writers(vec![Epoch {
        generation: 1,
        start_offset: offset,
    }]);
    log.append_pending_at(offset, vec![record(RecordMark::None, b"a")], writers)
        .await
        .expect("append")
        .expect("at the tail")
}

/// A Raft log syncs no more than the plain log for the same appends under
/// `OnCommit`: one flush per append waited on alone, and one for every
/// append that waited together. The vote, reservation and marks add no sync
/// per append.
#[tokio::test]
async fn a_raft_append_costs_one_shared_flush_like_a_plain_append() {
    const APPENDS: u64 = 20;
    let plain_dir = tempdir().expect("dir");
    let plain = DiskLog::open(
        plain_dir.path(),
        "t/ns/plain/0",
        LogConfig {
            raft_log: false,
            ..config()
        },
    )
    .expect("open");
    for index in 0..APPENDS {
        plain
            .append(&[record(
                RecordMark::None,
                format!("command-{index}").as_bytes(),
            )])
            .await
            .expect("append");
    }

    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let log = raft.disk_log().clone();
    for index in 0..APPENDS {
        let entry = normal(1, index, &format!("command-{index}"));
        let flushed = append(&mut raft, &[entry]).await.expect("append");
        flushed.await.expect("flush");
    }
    assert_eq!(
        plain.flushes(),
        APPENDS,
        "the plain log, one append at a time"
    );
    assert_eq!(
        log.flushes(),
        plain.flushes(),
        "the Raft log, one append at a time"
    );

    // Appends that wait together share one flush.
    let held = log.hold_flushes().await;
    let mut waiting = Vec::new();
    for index in APPENDS..APPENDS + 8 {
        let entry = normal(1, index, "command");
        waiting.push(append(&mut raft, &[entry]).await.expect("append"));
    }
    drop(held);
    for flushed in waiting {
        flushed.await.expect("flush");
    }
    assert_eq!(log.flushes(), APPENDS + 1, "eight waiting appends");
}
