//! A Raft log on a relaxed root: the callback fires at the write, once the
//! reservation covers the entries, and `flushed` still waits for an fsync.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;

use super::*;
use crate::raft_log::{LogPoint, reserved, state_file};

type Raft = RaftLog<Types, Codec>;

const BOUND: Duration = Duration::from_secs(10);
/// For a whole run of appends racing syncs, so a slow disk is not read as a
/// hang.
const RACE_BOUND: Duration = Duration::from_secs(45);

fn relaxed_config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        fsync_mode,
        ..config()
    }
}

/// Begin a run on `root` the way a broker does before any store opens.
fn begin(root: &Path, relaxed: bool) {
    crate::run_state::begin(root, relaxed, None, None, || false, || [7; 16]).expect("begin");
}

fn open_under(root: &Path, mode: FsyncMode) -> io::Result<Raft> {
    let dir = root.join("shard");
    std::fs::create_dir_all(&dir).expect("shard dir");
    let log = DiskLog::open(&dir, "t/ns/raft/0", relaxed_config(mode)).expect("open");
    RaftLog::new(log, Codec)
}

/// Append and wait for the callback, which must not wait for an fsync.
async fn append(raft: &mut Raft, entries: Vec<TestEntry>) -> io::Result<()> {
    let (tx, rx) = Types::oneshot();
    raft.append(entries, IOFlushed::signal(tx)).await?;
    tokio::time::timeout(BOUND, rx)
        .await
        .expect("the callback fires without an fsync")
        .expect("callback")
}

#[tokio::test]
async fn a_relaxed_log_is_refused_on_a_root_that_does_not_rotate() {
    let root = tempdir().expect("dir");
    begin(root.path(), false);
    let err = open_under(root.path(), FsyncMode::None)
        .err()
        .expect("refused");
    assert!(err.to_string().contains("must fsync on commit"), "{err}");
}

#[tokio::test]
async fn a_written_append_is_counted_before_its_fsync_and_flushed_after() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mut raft = open_under(root.path(), FsyncMode::None).expect("opens on a relaxed root");
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append");
    // Under `None` nothing has synced, so `flushed` has not moved, even
    // once the flush task has had its turn.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(raft.positions().borrow().flushed, None);
    let dir = raft.disk_log().dir().to_path_buf();
    assert_eq!(
        reserved::load(&dir).expect("load"),
        Some(1 + reserved::STEP),
        "the reservation covers the entries before the callback"
    );
    raft.disk_log().sync().await.expect("sync");
    let mut positions = raft.positions();
    tokio::time::timeout(
        BOUND,
        positions.wait_for(|at| at.flushed.is_some_and(|at| at.index == 1)),
    )
    .await
    .expect("flushed follows the sync")
    .expect("open");
}

#[tokio::test]
async fn an_append_past_the_reservation_raises_it_and_one_below_does_not() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let raft = open_under(root.path(), FsyncMode::None).expect("open");
    let dir = raft.disk_log().dir().to_path_buf();
    reserved::store(&dir, 5).expect("store");
    drop(raft);
    let mut raft = open_under(root.path(), FsyncMode::None).expect("reopen");
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append below");
    assert_eq!(
        reserved::load(&dir).expect("load"),
        Some(5),
        "loaded on open"
    );
    let past: Vec<_> = (2..=7).map(|index| normal(1, index, "b")).collect();
    append(&mut raft, past).await.expect("append past");
    assert_eq!(
        reserved::load(&dir).expect("load"),
        Some(7 + reserved::STEP)
    );
}

#[tokio::test]
async fn a_failed_reservation_write_fails_the_append_and_fires_no_callback() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mut raft = open_under(root.path(), FsyncMode::None).expect("open");
    let dir = raft.disk_log().dir().to_path_buf();
    // The write goes through this temporary, which a directory blocks.
    std::fs::create_dir(state_file::temporary(&dir, reserved::FILE_NAME)).expect("block");
    let (tx, mut rx) = Types::oneshot();
    let appended = raft.append(vec![blank(1, 0)], IOFlushed::signal(tx)).await;
    assert!(appended.is_err(), "the append fails");
    assert!(
        rx.try_recv().is_err(),
        "no callback for an unreserved entry"
    );
    // The raise comes before the write, so nothing a restart would count
    // as flushed sits above the reservation.
    assert_eq!(raft.disk_log().tail_now(), 0, "the entry was not written");
    raft.disk_log().close().await.expect("close");
    drop(raft);
    std::fs::remove_dir(state_file::temporary(&dir, reserved::FILE_NAME)).expect("unblock");
    let raft = open_under(root.path(), FsyncMode::None).expect("reopen");
    let tail = raft.disk_log().tail_now();
    assert!(
        tail == 0 || reserved::load(&dir).expect("load") >= Some(tail - 1),
        "a restart finds every entry it holds reserved"
    );
}

/// **An fsynced append reserves before its callback, like a written one.**
/// A re-form can cut a copy to its saved commit, below an entry it fsynced
/// and counted, and pads only up to the reservations it reads.
#[tokio::test]
async fn an_fsynced_append_fires_no_callback_past_the_reservation() {
    let root = tempdir().expect("dir");
    begin(root.path(), false);
    let mut raft = open_under(root.path(), FsyncMode::OnCommit).expect("open");
    let dir = raft.disk_log().dir().to_path_buf();
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append");
    assert_eq!(
        reserved::load(&dir).expect("load"),
        Some(1 + reserved::STEP),
        "the reservation covers the entries before the callback"
    );

    raft.disk_log().close().await.expect("close");
    drop(raft);
    reserved::store(&dir, 2).expect("store");
    let mut raft = open_under(root.path(), FsyncMode::OnCommit).expect("reopen");

    std::fs::create_dir(state_file::temporary(&dir, reserved::FILE_NAME)).expect("block");
    append(&mut raft, vec![normal(1, 2, "b")])
        .await
        .expect("an append inside the reservation needs no raise");
    let (tx, mut rx) = Types::oneshot();
    let appended = raft
        .append(vec![normal(1, 3, "c")], IOFlushed::signal(tx))
        .await;
    assert!(appended.is_err(), "the append past it fails");
    assert!(
        rx.try_recv().is_err(),
        "no callback for an unreserved entry"
    );
    assert_eq!(raft.disk_log().tail_now(), 3, "the entry was not written");
}

/// **An open raises the reservation over the entries already on disk, in
/// every fsync mode.** Raft counts every entry it finds at start as
/// flushed, and a copy written by a build that reserved only under written
/// commit has no reservation at all. The open fails when the raise does.
#[tokio::test]
async fn an_open_reserves_the_entries_already_on_disk_in_every_mode() {
    for (relaxed, mode) in [
        (false, FsyncMode::OnCommit),
        (true, FsyncMode::OnCommit),
        (true, FsyncMode::None),
    ] {
        let root = tempdir().expect("dir");
        begin(root.path(), relaxed);
        let mut raft = open_under(root.path(), mode).expect("open");
        let entries: Vec<_> = (0..=2).map(|index| normal(1, index, "a")).collect();
        append(&mut raft, entries).await.expect("append");
        let dir = raft.disk_log().dir().to_path_buf();
        raft.disk_log().close().await.expect("close");
        drop(raft);
        // What a build that did not reserve here leaves behind.
        std::fs::remove_file(dir.join(reserved::FILE_NAME)).expect("unreserve");

        std::fs::create_dir(state_file::temporary(&dir, reserved::FILE_NAME)).expect("block");
        let err = open_under(root.path(), mode)
            .err()
            .expect("an open whose raise fails is refused");
        assert_eq!(reserved::load(&dir).expect("load"), None, "{mode:?}: {err}");
        std::fs::remove_dir(state_file::temporary(&dir, reserved::FILE_NAME)).expect("unblock");

        let _raft = open_under(root.path(), mode).expect("reopen");
        assert_eq!(
            reserved::load(&dir).expect("load"),
            Some(2 + reserved::STEP),
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn a_damaged_reservation_fails_every_open() {
    for (relaxed, mode) in [
        (false, FsyncMode::OnCommit),
        (true, FsyncMode::OnCommit),
        (true, FsyncMode::None),
        (
            true,
            FsyncMode::Periodic {
                interval: Duration::from_secs(60),
            },
        ),
    ] {
        let root = tempdir().expect("dir");
        begin(root.path(), relaxed);
        let raft = open_under(root.path(), mode).expect("open");
        let dir = raft.disk_log().dir().to_path_buf();
        drop(raft);
        std::fs::write(dir.join(reserved::FILE_NAME), b"not a reservation").expect("damage");
        let err = open_under(root.path(), mode).err().expect("refused");
        assert!(
            err.to_string().contains("does not decode"),
            "{mode:?}: {err}"
        );
    }
}

#[tokio::test]
async fn a_failed_periodic_flush_poisons_the_log() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mut raft = open_under(
        root.path(),
        FsyncMode::Periodic {
            interval: Duration::from_millis(10),
        },
    )
    .expect("open");
    raft.disk_log().fail_next_flush();
    append(&mut raft, vec![blank(1, 0)]).await.expect("append");
    let log = raft.disk_log().clone();
    tokio::time::timeout(BOUND, async {
        while !log.is_poisoned() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the tick's failure poisons the log, as a commit flush's does");
    assert_eq!(raft.positions().borrow().flushed, None);
    // The queued flush waits for a sync that will never come; the poison
    // must wake it, or the queue never drains.
    tokio::time::timeout(BOUND, raft.shared.flushes.drained())
        .await
        .expect("the poison wakes the queued flush");
}

/// **A purge on a written-commit log under `None` does not wait forever for
/// the queued flushes.** Nothing syncs such a log on its own, so a purge
/// that only waited would stall the group's log task.
#[tokio::test]
async fn a_purge_under_none_finishes_with_flushes_queued() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mut raft = open_under(root.path(), FsyncMode::None).expect("open");
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append");
    let purged = tokio::time::timeout(BOUND, raft.purge(log_id(1, 0))).await;
    purged.expect("the purge waits on no sync").expect("purge");
}

/// A written-commit log under a marked root, with two appends whose flushes
/// are still queued: under `None` nothing syncs them.
async fn queued_under_marks(
    root: &crate::raft_copy::tests::Root,
    mode: crate::raft_copy::LogMarkMode,
) -> Raft {
    use crate::raft_copy::tests::SHARD;
    begin(root.path(), true);
    let copies = root.open();
    let opened = copies
        .open_copy(
            SHARD,
            &root.shard_dir(),
            "t/ns/orders/0",
            relaxed_config(FsyncMode::None),
        )
        .await
        .expect("open copy");
    let mut raft = Raft::new(opened.log.clone(), Codec).expect("a written-commit log");
    raft.mark_with(opened.marks.clone(), mode).expect("marks");
    append(&mut raft, vec![blank(1, 0)]).await.expect("append");
    append(&mut raft, vec![normal(1, 1, "a")])
        .await
        .expect("append");
    // An exact mark queues them for the flush task; a batched one leaves
    // them for the sync to raise.
    assert!(
        !raft.shared.flushes.is_empty() || raft.shared.waits_for_sync(),
        "the flushes are queued"
    );
    raft
}

fn marks_modes() -> [crate::raft_copy::LogMarkMode; 2] {
    use crate::raft_copy::LogMarkMode;
    [
        LogMarkMode::Exact,
        LogMarkMode::Batched(Duration::from_secs(60)),
    ]
}

/// **A close that beats a queued flush to a written-commit log does not
/// poison it.** The close syncs everything itself, so the flush that then
/// finds the log closed lost nothing. Poisoning would make the next start
/// rotate a storage id that a clean stop keeps.
#[tokio::test]
async fn a_close_with_flushes_queued_leaves_the_log_unpoisoned() {
    for mode in marks_modes() {
        let root = crate::raft_copy::tests::Root::new();
        let raft = queued_under_marks(&root, mode).await;
        let log = raft.disk_log().clone();
        log.close().await.expect("close");
        tokio::time::timeout(BOUND, raft.shared.flushes.drained())
            .await
            .expect("the queued flushes are answered");
        assert!(
            !log.is_poisoned(),
            "{mode:?}: a clean close poisoned the log"
        );
        assert!(
            !crate::disk_log::durable_mark::left_poisoned(log.dir()).expect("look"),
            "{mode:?}: the next start would rotate the storage id"
        );
    }
}

/// **Draining before the close answers every queued flush with its real
/// result**, so `flushed` reaches the last entry and the log stops clean.
#[tokio::test]
async fn draining_before_the_close_flushes_every_queued_append() {
    for mode in marks_modes() {
        let root = crate::raft_copy::tests::Root::new();
        let raft = queued_under_marks(&root, mode).await;
        tokio::time::timeout(BOUND, raft.drain_flushes())
            .await
            .expect("the drain finishes")
            .expect("drain");
        assert_eq!(
            raft.positions().borrow().flushed.map(|at| at.index),
            Some(1),
            "{mode:?}"
        );
        let log = raft.disk_log().clone();
        log.close().await.expect("close");
        assert!(!log.is_poisoned(), "{mode:?}");
        assert!(!crate::disk_log::durable_mark::left_poisoned(log.dir()).expect("look"));
    }
}

const WRITTEN_MODES: [FsyncMode; 2] = [
    FsyncMode::None,
    // Long enough that only the explicit sync below covers the appends.
    FsyncMode::Periodic {
        interval: Duration::from_secs(3600),
    },
];

/// **A written-commit append hands nothing to the flush task.** The sync
/// that covers it raises `flushed` on its own thread, so an append costs
/// no wake of another task, and `flushed` has moved when the sync returns.
#[tokio::test]
async fn a_written_commit_append_wakes_no_flush_task() {
    for mode in WRITTEN_MODES {
        let root = tempdir().expect("dir");
        begin(root.path(), true);
        let mut raft = open_under(root.path(), mode).expect("open");
        for index in 0..5 {
            append(&mut raft, vec![normal(1, index, "a")])
                .await
                .expect("append");
        }
        assert_eq!(raft.shared.flushes.pushed(), 0, "{mode:?}: no task woken");
        assert_eq!(raft.positions().borrow().flushed, None, "{mode:?}");
        raft.disk_log().sync().await.expect("sync");
        assert_eq!(
            raft.positions().borrow().flushed,
            Some(LogPoint { term: 1, index: 4 }),
            "{mode:?}: the sync raised it"
        );
        append(&mut raft, vec![normal(1, 5, "b")])
            .await
            .expect("append");
        assert_eq!(
            raft.positions().borrow().flushed,
            Some(LogPoint { term: 1, index: 4 }),
            "{mode:?}: not before its sync"
        );
        raft.disk_log().sync().await.expect("sync");
        assert_eq!(
            raft.positions().borrow().flushed,
            Some(LogPoint { term: 1, index: 5 }),
            "{mode:?}"
        );
    }
}

/// **What the callback sees is the same in every mode.** Under `OnCommit`
/// it fires once `flushed` covers the entry; under written commit it fires
/// at the write, with `written` there and `flushed` behind until a sync.
#[tokio::test]
async fn the_callback_and_flushed_keep_their_order_in_every_mode() {
    let last = Some(LogPoint { term: 1, index: 1 });
    for (relaxed, mode) in [
        (false, FsyncMode::OnCommit),
        (true, FsyncMode::OnCommit),
        (true, WRITTEN_MODES[0]),
        (true, WRITTEN_MODES[1]),
    ] {
        let root = tempdir().expect("dir");
        begin(root.path(), relaxed);
        let mut raft = open_under(root.path(), mode).expect("open");
        append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
            .await
            .expect("append");
        let at = *raft.positions().borrow();
        assert_eq!(at.written, last, "{mode:?}");
        if mode == FsyncMode::OnCommit {
            assert_eq!(at.flushed, last, "{mode:?}: flushed before the callback");
        } else {
            assert_eq!(at.flushed, None, "{mode:?}: the callback did not wait");
            raft.disk_log().sync().await.expect("sync");
            let mut positions = raft.positions();
            tokio::time::timeout(BOUND, positions.wait_for(|at| at.flushed == last))
                .await
                .expect("flushed follows the sync")
                .expect("open");
        }
    }
}

/// **The periodic sync raises `flushed` with no call from Raft.**
#[tokio::test]
async fn a_periodic_sync_raises_flushed() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mode = FsyncMode::Periodic {
        interval: Duration::from_millis(20),
    };
    let mut raft = open_under(root.path(), mode).expect("open");
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append");
    let mut positions = raft.positions();
    tokio::time::timeout(
        BOUND,
        positions.wait_for(|at| at.flushed.is_some_and(|at| at.index == 1)),
    )
    .await
    .expect("flushed follows the periodic sync")
    .expect("open");
    assert_eq!(raft.shared.flushes.pushed(), 0);
}

/// **A stopped log never raises `flushed` past what a sync covered.**
#[tokio::test]
async fn a_poisoned_written_commit_log_leaves_flushed_where_it_was() {
    let root = tempdir().expect("dir");
    begin(root.path(), true);
    let mut raft = open_under(root.path(), FsyncMode::None).expect("open");
    append(&mut raft, vec![blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("append");
    raft.disk_log().poison("test".to_string());
    assert!(
        raft.disk_log().sync().await.is_err(),
        "a stopped log syncs nothing"
    );
    assert_eq!(raft.positions().borrow().flushed, None);
    assert!(raft.disk_log().is_poisoned());
}

/// Run `work` on its own thread and fail, rather than hang the suite, if it
/// does not return within `bound`: a lock taken twice inside the sync hook
/// blocks for ever. A panic inside `work` is passed on as it was.
fn within<T: Send + 'static>(
    what: &str,
    bound: Duration,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(work());
    });
    match rx.recv_timeout(bound) {
        Ok(done) => done,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => match worker.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => panic!("{what} returned nothing"),
        },
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
            "{what} did not finish in {} s with the sync hook registered",
            bound.as_secs()
        ),
    }
}

fn within_bound<T: Send + 'static>(what: &str, work: impl FnOnce() -> T + Send + 'static) -> T {
    within(what, BOUND, work)
}

/// Run `test` on a runtime of its own on another thread, so a deadlock in
/// the log fails the test instead of hanging the runtime's shutdown.
fn on_own_runtime<F: Future<Output = ()> + 'static>(
    what: &str,
    bound: Duration,
    test: impl FnOnce() -> F + Send + 'static,
) {
    within(what, bound, move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(test());
    });
}

/// A written-commit log under `None` with appends waiting in the hook.
async fn waiting_on_sync(root: &Path, entries: u64) -> Raft {
    begin(root, true);
    let mut raft = open_under(root, FsyncMode::None).expect("open");
    for index in 0..entries {
        append(&mut raft, vec![normal(1, index, "a")])
            .await
            .expect("append");
    }
    assert!(raft.shared.waits_for_sync(), "the appends wait for a sync");
    raft
}

/// **A poison with appends waiting does not block.** `poison` wakes the
/// durable watch after it lets go of the failure mutex, and the hook reads
/// that mutex, so a poison taken inside the wake would deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poison_with_appends_waiting_returns() {
    let root = tempdir().expect("dir");
    let raft = waiting_on_sync(root.path(), 3).await;
    let log = raft.disk_log().clone();
    within_bound("a poison", move || log.poison("test".to_string()));
    // Twice: the second finds the log stopped and must still return.
    let log = raft.disk_log().clone();
    within_bound("a second poison", move || log.poison("again".to_string()));
    assert!(!raft.shared.waits_for_sync(), "the poison answered them");
    assert_eq!(raft.positions().borrow().flushed, None, "nothing synced");
    assert!(raft.disk_log().is_poisoned());
}

/// **A failed flush with appends waiting stops the log without blocking.**
/// The flush fails inside the group-commit path, holding the flush lock,
/// and poisons from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_sync_with_appends_waiting_returns() {
    let root = tempdir().expect("dir");
    let raft = waiting_on_sync(root.path(), 3).await;
    let log = raft.disk_log().clone();
    log.fail_next_flush();
    let synced = tokio::time::timeout(BOUND, tokio::spawn(async move { log.sync().await }))
        .await
        .expect("the failed sync returns")
        .expect("join");
    assert!(synced.is_err());
    assert!(raft.disk_log().is_poisoned());
    assert_eq!(raft.positions().borrow().flushed, None);
}

/// **A sync under the segments write lock raises `flushed` without
/// blocking.** Opening a re-form gap seals the active segment and raises
/// the durable bound with the segments lock held; the hook must not take
/// it.
#[test]
fn a_gap_raised_under_the_segments_lock_raises_flushed() {
    on_own_runtime("a gap", BOUND, || async {
        let root = tempdir().expect("dir");
        let raft = waiting_on_sync(root.path(), 3).await;
        let log = raft.disk_log().clone();
        tokio::time::timeout(BOUND, tokio::spawn(async move { log.open_gap(10).await }))
            .await
            .expect("the gap opens")
            .expect("join")
            .expect("open gap");
        assert_eq!(
            raft.positions().borrow().flushed,
            Some(LogPoint { term: 1, index: 2 }),
            "the seal synced the waiting appends"
        );
    });
}

/// **A truncation lowers `flushed` and no sync raises it past the cut.**
/// The truncation syncs, takes the hook's queue and only then cuts, under
/// the flush lock and the segments lock; the cut's own bound reset calls
/// the hook too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_sync_raises_flushed_past_a_truncation() {
    let root = tempdir().expect("dir");
    let mut raft = waiting_on_sync(root.path(), 4).await;
    tokio::time::timeout(BOUND, raft.truncate_after(Some(log_id(1, 1))))
        .await
        .expect("the truncation finishes")
        .expect("truncate");
    let kept = Some(LogPoint { term: 1, index: 1 });
    assert_eq!(raft.positions().borrow().flushed, kept);
    assert!(!raft.shared.waits_for_sync());
    // The late sync, and an append after the cut that waits for one.
    tokio::time::timeout(BOUND, raft.disk_log().sync())
        .await
        .expect("sync returns")
        .expect("sync");
    assert_eq!(raft.positions().borrow().flushed, kept, "not past the cut");
    append(&mut raft, vec![normal(2, 2, "b")])
        .await
        .expect("append");
    assert_eq!(raft.positions().borrow().flushed, kept);
    raft.disk_log().sync().await.expect("sync");
    assert_eq!(
        raft.positions().borrow().flushed,
        Some(LogPoint { term: 2, index: 2 })
    );
}

/// **Appends racing syncs on other threads neither block nor lose a raise.**
/// An append holds the hook's queue while it reads the durable bound; a
/// sync raises the bound and then takes the queue. Neither holds what the
/// other waits for.
#[test]
fn appends_racing_syncs_raise_flushed_in_order() {
    // Set on every way out, a failure included, so a failed run does not
    // leave the syncers fsyncing for the rest of the test process.
    struct StopOnDrop(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _stop = StopOnDrop(Arc::clone(&stop));
    // Generous: the run takes well under a second, so only a stuck one
    // reaches it.
    on_own_runtime("appends racing syncs", RACE_BOUND, move || async move {
        let root = tempdir().expect("dir");
        begin(root.path(), true);
        let dir = root.path().join("shard");
        std::fs::create_dir_all(&dir).expect("shard dir");
        // Dozens of entries per segment, so a few rolls: the race is appends
        // against syncs, and a roll fsyncs several times, so a segment per
        // append would make the run as slow as the disk.
        let config = LogConfig {
            segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 2 * 1024,
            ..relaxed_config(FsyncMode::None)
        };
        let log = DiskLog::open(&dir, "t/ns/raft/0", config).expect("open");
        let mut raft = Raft::new(log, Codec).expect("open");
        let syncers: Vec<_> = (0..2)
            .map(|_| {
                let (log, stop) = (raft.disk_log().clone(), Arc::clone(&stop));
                tokio::spawn(async move {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        log.sync().await.expect("sync");
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
            })
            .collect();
        let mut seen = raft.positions();
        let watcher = tokio::spawn(async move {
            let mut last = None;
            while seen.changed().await.is_ok() {
                let now = seen.borrow_and_update().flushed.map(|at| at.index);
                assert!(now >= last, "flushed fell from {last:?} to {now:?}");
                last = now;
            }
        });
        for index in 0..200 {
            append(&mut raft, vec![normal(1, index, "a")])
                .await
                .expect("append");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for syncer in syncers {
            tokio::time::timeout(BOUND, syncer)
                .await
                .expect("the syncers stop")
                .expect("syncer");
        }
        raft.disk_log().sync().await.expect("sync");
        assert_eq!(
            raft.positions().borrow().flushed,
            Some(LogPoint {
                term: 1,
                index: 199
            })
        );
        assert_eq!(raft.shared.flushes.pushed(), 0);
        drop(raft);
        tokio::time::timeout(BOUND, watcher)
            .await
            .expect("the watcher ends with the log")
            .expect("flushed never fell");
    });
}
