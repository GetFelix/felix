//! The Raft log keeps its marks: the vote mark before `save_vote` returns,
//! the exact log mark before Raft hears of a flush, the batched one once per
//! interval and never past the flush, and a truncation brings the log mark
//! down first.

use futures_util::FutureExt;
use openraft::storage::RaftLogStorage;
use openraft::type_config::alias::VoteOf;
use openraft::vote::{RaftLeaderId, RaftVote};

use super::writes::append;
use super::{Codec, Types, log_id, normal};
use crate::DiskLog;
use crate::log::{FsyncMode, LogConfig};
use crate::raft_copy::tests::{Root, SHARD, config, point, vote};
use crate::raft_copy::{CopyRoot, LogMarkMode, OpenedCopy};
use crate::raft_log::RaftLog;

type Log = RaftLog<Types, Codec>;

fn raft_vote(term: u64, node: u64, committed: bool) -> VoteOf<Types> {
    let leader = <Types as openraft::RaftTypeConfig>::LeaderId::new(term, node);
    VoteOf::<Types>::from_leader_id(leader, committed)
}

async fn opened(root: &Root, copies: &CopyRoot, mode: LogMarkMode) -> (OpenedCopy, Log) {
    let opened = copies
        .open_copy(SHARD, &root.shard_dir(), "t/ns/orders/0", config())
        .await
        .expect("open copy");
    let log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), mode).expect("marks");
    (opened, log)
}

/// The vote mark is on disk when `save_vote` returns: a crash right after
/// it, before the vote is answered, cannot leave a vote above its mark that
/// a restore could take back unseen.
#[tokio::test]
async fn save_vote_returns_after_the_vote_mark_is_synced() {
    let root = Root::new();
    let copies = root.open();
    let (opened, mut log) = opened(&root, &copies, LogMarkMode::Exact).await;
    for (term, node, committed) in [(1, 1, false), (2, 3, false), (2, 2, true)] {
        log.save_vote(&raft_vote(term, node, committed))
            .await
            .expect("vote");
        let marks = root.marks_on_disk().expect("marks");
        assert_eq!(marks.copy, opened.copy);
        assert_eq!(
            marks.vote,
            vote(term + 1, &node.to_string(), committed),
            "term {term}"
        );
    }
}

/// A vote the log refuses marks nothing.
#[tokio::test]
async fn a_refused_vote_marks_nothing() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(&root, &copies, LogMarkMode::Exact).await;
    log.save_vote(&raft_vote(3, 1, false)).await.expect("vote");
    log.save_vote(&raft_vote(2, 1, false))
        .await
        .expect_err("goes back");
    assert_eq!(
        root.marks_on_disk().expect("marks").vote,
        vote(4, "1", false)
    );
}

/// Raft hears an append is on disk only once its exact mark is too, and
/// the mark never names an entry the flush has not covered.
#[tokio::test]
async fn the_exact_log_mark_is_synced_before_the_callback() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(&root, &copies, LogMarkMode::Exact).await;
    let disk = log.disk_log().clone();
    let held = disk.hold_flushes().await;
    let mut flushed = append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
        .await
        .expect("append");
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        flushed.as_mut().now_or_never().is_none(),
        "fired before the flush"
    );
    assert_eq!(
        root.marks_on_disk().expect("marks").log,
        None,
        "marked before the flush"
    );
    // The flush lands, and the mark write after it is held: the callback
    // waits for the mark.
    let marking = copies.mark_log().hold_writer().await;
    drop(held);
    while copies.mark_log().queued() == 0 {
        tokio::task::yield_now().await;
    }
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        flushed.as_mut().now_or_never().is_none(),
        "fired before the mark was synced"
    );
    drop(marking);
    flushed.await.expect("flush");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 1)));
}

/// Concurrent appends' marks share one write per flush round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_in_one_flush_round_share_a_mark_write() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(&root, &copies, LogMarkMode::Exact).await;
    let before = copies.mark_log().sequence();
    let disk = log.disk_log().clone();
    let held = disk.hold_flushes().await;
    let mut callbacks = Vec::new();
    for index in 0..10 {
        callbacks.push(
            append(&mut log, &[normal(1, index, "x")])
                .await
                .expect("append"),
        );
    }
    drop(held);
    for flushed in callbacks {
        flushed.await.expect("flush");
    }
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 9)));
    let writes = copies.mark_log().sequence() - before;
    assert!(writes < 10, "{writes} mark writes for one flush round");
}

/// The mark comes down before the cut, so a crash after the cut leaves no
/// mark above the log to rotate it for nothing.
#[tokio::test]
async fn a_truncation_lowers_the_log_mark_first() {
    for mode in [
        LogMarkMode::Exact,
        LogMarkMode::Batched(std::time::Duration::from_secs(3600)),
    ] {
        let root = Root::new();
        let copies = root.open();
        let (_opened, mut log) = opened(&root, &copies, mode).await;
        let entries: Vec<_> = (0..5).map(|index| normal(1, index, "x")).collect();
        append(&mut log, &entries)
            .await
            .expect("append")
            .await
            .expect("flush");
        copies.mark_log().mark_batched().await.expect("batch");
        assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 4)));
        log.truncate_after(Some(log_id(1, 2))).await.expect("cut");
        assert_eq!(
            root.marks_on_disk().expect("marks").log,
            Some(point(1, 2)),
            "{mode:?}"
        );
        // The batch does not put it back up.
        copies.mark_log().mark_batched().await.expect("batch");
        assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 2)));
    }
}

/// The batched mark trails the log until the batch runs, then covers every
/// shard that moved in one write.
#[tokio::test]
async fn the_batched_mark_trails_the_log_until_the_batch() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(
        &root,
        &copies,
        LogMarkMode::Batched(std::time::Duration::from_secs(3600)),
    )
    .await;
    // The batcher's first run, at once.
    tokio::task::yield_now().await;
    append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
        .await
        .expect("append")
        .await
        .expect("flush");
    assert_eq!(root.marks_on_disk().expect("marks").log, None);
    let before = copies.mark_log().sequence();
    copies.mark_log().mark_batched().await.expect("batch");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 1)));
    assert_eq!(copies.mark_log().sequence(), before + 1);
    copies
        .mark_log()
        .mark_batched()
        .await
        .expect("nothing moved");
    assert_eq!(copies.mark_log().sequence(), before + 1);
}

/// The batch marks the flushed position, never the written one: an entry
/// that is not on disk yet is not marked, so a crash cannot leave a mark
/// above the log and rotate the copy for nothing.
#[tokio::test]
async fn the_batched_mark_never_passes_the_flushed_log() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(
        &root,
        &copies,
        LogMarkMode::Batched(std::time::Duration::from_secs(3600)),
    )
    .await;
    append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
        .await
        .expect("append")
        .await
        .expect("flush");
    let disk = log.disk_log().clone();
    let held = disk.hold_flushes().await;
    let mut unflushed = append(&mut log, &[normal(1, 2, "c"), normal(1, 3, "d")])
        .await
        .expect("append");
    assert_eq!(log.positions().borrow().written, Some(point(1, 3)));
    copies.mark_log().mark_batched().await.expect("batch");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 1)));
    assert!(unflushed.as_mut().now_or_never().is_none());
    drop(held);
    unflushed.await.expect("flush");
    copies.mark_log().mark_batched().await.expect("batch");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 3)));
}

/// Under a steady stream of flushes the batched mark costs one synced
/// write per interval, not one per flush round, and it still catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_batched_mark_writes_once_per_interval_under_load() {
    let interval = std::time::Duration::from_millis(100);
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(&root, &copies, LogMarkMode::Batched(interval)).await;
    let before = copies.mark_log().sequence();
    let started = std::time::Instant::now();
    let mut index = 0;
    while started.elapsed() < std::time::Duration::from_millis(600) {
        append(&mut log, &[normal(1, index, "x")])
            .await
            .expect("append")
            .await
            .expect("flush");
        index += 1;
    }
    let elapsed = started.elapsed();
    let writes = copies.mark_log().sequence() - before;
    let intervals = (elapsed.as_millis() / interval.as_millis()) as u64;
    assert!(index > intervals + 1, "only {index} flushes in {elapsed:?}");
    assert!(
        writes <= intervals + 1,
        "{writes} mark writes for {index} flushes in {elapsed:?}"
    );
    let last = Some(point(1, index - 1));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while root.marks_on_disk().expect("marks").log != last {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the mark never caught up"
        );
        tokio::time::sleep(interval / 4).await;
    }
}

/// A copy reopened before its batch ran is marked at what it recovered:
/// with no append after the reopen the batch has nothing to mark, and the
/// mark would otherwise trail the log for as long as the shard is idle.
#[tokio::test]
async fn a_reopened_log_marks_what_it_recovered() {
    let batched = LogMarkMode::Batched(std::time::Duration::from_secs(3600));
    let root = Root::new();
    let copies = root.open();
    let (opened, mut log) = opened(&root, &copies, batched).await;
    append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
        .await
        .expect("append")
        .await
        .expect("flush");
    assert_eq!(root.marks_on_disk().expect("marks").log, None);
    drop(log);
    let reopened = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    reopened
        .mark_with(opened.marks.clone(), batched)
        .expect("marks");
    reopened.mark_recovered().await.expect("recovered");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(1, 1)));
}

/// The ballot is synced whatever the log's fsync mode: a Raft log refuses
/// any mode but on-commit, but the vote does not rely on that.
#[tokio::test]
async fn a_vote_is_synced_in_every_fsync_mode() {
    for fsync_mode in [
        FsyncMode::None,
        FsyncMode::Periodic {
            interval: std::time::Duration::from_secs(3600),
        },
        FsyncMode::OnCommit,
    ] {
        let dir = tempfile::tempdir().expect("dir");
        let log = DiskLog::open(
            dir.path(),
            "t/ns/orders/0",
            LogConfig {
                fsync_mode,
                ..config()
            },
        )
        .expect("open");
        // Counts the steps of the ballot's write: the temporary, its fsync,
        // the rename and the directory's sync.
        crate::disk_log::crash_point::watch(dir.path(), None);
        log.save_vote(2, "1", false).await.expect("vote");
        let steps = crate::disk_log::crash_point::unwatch(dir.path());
        assert!(steps >= 4, "{fsync_mode:?}: {steps} steps");
        log.close().await.expect("close");
    }
}

/// A batched mark write that fails stops the mark log, so the batched
/// mark can no longer follow the log. From then on the log takes no
/// appends rather than run ahead of its mark without bound.
#[tokio::test]
async fn a_failed_batched_mark_fails_later_appends() {
    let root = Root::new();
    let copies = root.open();
    let (_opened, mut log) = opened(
        &root,
        &copies,
        LogMarkMode::Batched(std::time::Duration::from_millis(10)),
    )
    .await;
    append(&mut log, &[normal(1, 0, "a")])
        .await
        .expect("append")
        .await
        .expect("flush");
    copies.mark_log().fail_next_write();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut index = 1;
    loop {
        match append(&mut log, &[normal(1, index, "b")]).await {
            Ok(flushed) => {
                flushed.await.expect("flush");
                index += 1;
            }
            Err(_) => break,
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "appends went on after the batched mark failed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(copies.mark_log().failure().is_some());
}
