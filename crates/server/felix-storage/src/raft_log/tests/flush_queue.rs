//! The flush queue: appends that wait for a flush are answered by one
//! task, in order, and an append with nothing to wait for is answered
//! before it returns.

use futures_util::FutureExt;
use openraft::storage::RaftLogStorage;

use super::writes::append;
use super::*;
use crate::log::{Epoch, WrittenBy};
use crate::raft_log::flush_queue::FlushQueue;

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

/// On a log that acknowledges before its sync there is nothing to wait
/// for, so the callback has fired when the append returns.
#[tokio::test]
async fn an_append_with_nothing_to_wait_for_is_answered_at_once() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        ..config()
    };
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config).expect("open");
    let mut raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");

    for index in 0..3 {
        let entry = normal(1, index, "command");
        let mut flushed = append(&mut raft, &[entry]).await.expect("append");
        flushed
            .as_mut()
            .now_or_never()
            .expect("the callback had not fired when the append returned")
            .expect("flush");
    }
}

/// A callback that panics takes the flush task with it. The appends still
/// queued are failed, and a truncation does not wait for them for ever.
#[tokio::test]
async fn a_stopped_flush_task_fails_what_it_held() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log().clone();
    let write = async |offset| {
        let writers = WrittenBy::Writers(vec![Epoch {
            generation: 1,
            start_offset: offset,
        }]);
        log.append_pending_at(offset, &[record(RecordMark::None, b"a")], writers)
            .await
            .expect("append")
            .expect("at the tail")
    };

    let held = log.hold_flushes().await;
    let first = write(0).await;
    FlushQueue::push(&raft.shared, first, Box::new(|_| panic!("a callback")));
    let (answered, answer) = tokio::sync::oneshot::channel();
    let second = write(1).await;
    FlushQueue::push(
        &raft.shared,
        second,
        Box::new(move |flushed| drop(answered.send(flushed))),
    );
    // Queued behind the first two, in a later batch.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let (answered_late, late) = tokio::sync::oneshot::channel();
    let third = write(2).await;
    FlushQueue::push(
        &raft.shared,
        third,
        Box::new(move |flushed| drop(answered_late.send(flushed))),
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
