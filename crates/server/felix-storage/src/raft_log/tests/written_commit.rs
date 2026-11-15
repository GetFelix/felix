//! A Raft log on a relaxed root: the callback fires at the write, once the
//! reservation covers the entries, and `flushed` still waits for an fsync.

use std::path::Path;
use std::time::Duration;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;

use super::*;
use crate::raft_log::{reserved, state_file};

type Raft = RaftLog<Types, Codec>;

const BOUND: Duration = Duration::from_secs(10);

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
}

#[tokio::test]
async fn a_damaged_reservation_fails_every_open() {
    let root = tempdir().expect("dir");
    begin(root.path(), false);
    let raft = open_under(root.path(), FsyncMode::OnCommit).expect("open");
    let dir = raft.disk_log().dir().to_path_buf();
    drop(raft);
    std::fs::write(dir.join(reserved::FILE_NAME), b"not a reservation").expect("damage");
    let err = open_under(root.path(), FsyncMode::OnCommit)
        .err()
        .expect("refused");
    assert!(err.to_string().contains("does not decode"), "{err}");
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
}
