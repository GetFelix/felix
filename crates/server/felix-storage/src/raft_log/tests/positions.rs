//! The waiter queues beside the positions' watch: each advance of the
//! written or flushed position wakes the waits whose key it reached.

use std::time::Duration;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;

use super::*;
use crate::raft_log::LogPoint;

type Raft = RaftLog<Types, Codec>;

/// Long enough for a loaded runner, short enough that a missing advance
/// fails the test instead of hanging it.
const BOUND: Duration = Duration::from_secs(10);

async fn append(raft: &mut Raft, entries: Vec<TestEntry>) {
    let (tx, rx) = Types::oneshot();
    raft.append(entries, IOFlushed::signal(tx))
        .await
        .expect("append");
    rx.await.expect("callback").expect("flush");
}

/// Completes once the watched position reaches `key`.
fn reached(at: Option<LogPoint>, key: (u64, u64)) -> Option<()> {
    at.filter(|at| (at.term, at.index) >= key).map(|_| ())
}

#[tokio::test]
async fn an_append_wakes_a_flushed_waiter() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let positions = raft.positions();
    let waiters = raft.flushed_waiters();
    let wait = waiters.wait_for(&positions, (1, 1), |p| reached(p.flushed, (1, 1)));
    let wait = tokio::time::timeout(BOUND, wait);
    // The join polls the wait first, so it is queued before the append.
    let (woken, ()) = tokio::join!(
        wait,
        append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
    );
    woken.expect("woken by the append").expect("open");
}

#[tokio::test]
async fn an_append_wakes_a_written_waiter() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let positions = raft.positions();
    let waiters = raft.written_waiters();
    let wait = waiters.wait_for(&positions, (1, 1), |p| reached(p.written, (1, 1)));
    let wait = tokio::time::timeout(BOUND, wait);
    // The join polls the wait first, so it is queued before the append.
    let (woken, ()) = tokio::join!(
        wait,
        append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
    );
    woken.expect("woken by the append").expect("open");
}

/// A cut lowers the written position, and the waiter's key is above it.
/// Entries appended behind the cut must still wake it.
#[tokio::test]
async fn a_reappend_after_a_truncate_wakes_a_written_waiter() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append(
        &mut raft,
        vec![blank(1, 0), normal(1, 1, "a"), normal(1, 2, "b")],
    )
    .await;
    raft.truncate_after(Some(log_id(1, 0))).await.expect("cut");

    let positions = raft.positions();
    let waiters = raft.written_waiters();
    let wait = waiters.wait_for(&positions, (2, 2), |p| reached(p.written, (2, 2)));
    let wait = tokio::time::timeout(BOUND, wait);
    // The join polls the wait first, so it is queued before the append.
    let (woken, ()) = tokio::join!(
        wait,
        append(&mut raft, vec![normal(2, 1, "c"), normal(2, 2, "d")])
    );
    woken.expect("woken by the re-append").expect("open");
}

/// A waiter that wants the written position to fall: only the truncate's
/// own advance can wake it, as nothing is appended afterwards.
#[tokio::test]
async fn a_truncate_wakes_a_written_waiter_the_cut_satisfies() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append(
        &mut raft,
        vec![blank(1, 0), normal(1, 1, "a"), normal(1, 2, "b")],
    )
    .await;

    let positions = raft.positions();
    let waiters = raft.written_waiters();
    let wait = waiters.wait_for(&positions, (1, 0), |p| {
        p.written.filter(|at| at.index <= 1).map(|_| ())
    });
    let wait = tokio::time::timeout(BOUND, wait);
    let (woken, cut) = tokio::join!(wait, raft.truncate_after(Some(log_id(1, 1))));
    cut.expect("cut");
    woken.expect("woken by the cut").expect("open");
}
