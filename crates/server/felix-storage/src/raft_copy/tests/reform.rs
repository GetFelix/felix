//! What a re-form reads: kept copies' commits and the pad above every
//! reservation.

use openraft::storage::RaftLogStorage;
use tempfile::TempDir;

use super::reclaim::{kept_dir, rotated};
use super::*;
use crate::DiskLog;
use crate::raft_log::tests::writes::append_flushed;
use crate::raft_log::tests::{Codec, Types, log_id, normal};
use crate::raft_log::{RaftLog, reserved};

type Log = RaftLog<Types, Codec>;

const SPACING: u64 = 4096;

fn open(dir: &TempDir) -> Log {
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    Log::new(log, Codec).expect("a Raft log")
}

/// A log holding entries 0..=9 of term 1 whose saved commit names (term 5,
/// index 10), an entry its last sync never covered: the state a power cut
/// leaves on a relaxed copy.
async fn saved_commit_past_the_log() -> TempDir {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..10).map(|index| normal(1, index, "kept")).collect();
    entries.push(normal(5, 10, "lost"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(5, 10)))
        .await
        .expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &entries[..10]).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");
    dir
}

/// A kept copy is read without an open, so its saved commit is cut at its
/// last synced entry here, to what an open of the same bytes reads.
#[tokio::test]
async fn a_kept_saved_commit_past_its_last_entry_is_cut_at_the_entry() {
    let dir = saved_commit_past_the_log().await;
    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!(kept.last, Some(9));
    assert_eq!(kept.committed, Some(9));

    // Nothing was written, and an open agrees.
    let mut raft = open(&dir);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 9))
    );
    raft.disk_log().close().await.expect("close");
}

/// The cut lands on an earlier entry of the committed one's log, so a
/// later term there is another log's entry and vouches for nothing.
#[tokio::test]
async fn a_kept_commit_cut_onto_a_later_terms_entry_vouches_for_nothing() {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let entries: Vec<_> = (0..4).map(|index| normal(2, index, "x")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(2, 3))).await.expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let other: Vec<_> = (0..2).map(|index| normal(3, index, "y")).collect();
    append_flushed(&mut raft, &other).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!(kept.last, Some(1));
    assert_eq!(kept.committed, None);
}

/// A saved commit inside the log whose entry is another term's names some
/// other log's entry. An open does not take it, so neither does the read.
#[tokio::test]
async fn a_kept_commit_on_another_terms_entry_inside_the_log_vouches_for_nothing() {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..5).map(|index| normal(1, index, "x")).collect();
    entries.push(normal(2, 5, "other"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(2, 5))).await.expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let ours: Vec<_> = (0..6).map(|index| normal(1, index, "x")).collect();
    append_flushed(&mut raft, &ours).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(5), None));
}

/// A saved commit the log holds stands as saved.
#[tokio::test]
async fn a_kept_commit_inside_the_log_stands() {
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries: Vec<_> = (0..6).map(|index| normal(1, index, "x")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(1, 3))).await.expect("save");
    raft.disk_log().commit_offset_persisted().await;
    raft.disk_log().close().await.expect("close");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(5), Some(3)));
}

/// The pad reads the kept copies too: a kept reservation above the current
/// copy's still moves the first free index past it.
#[tokio::test]
async fn the_first_free_index_is_above_every_reservation_current_or_kept() {
    let root = Root::new();
    let dir = root.shard_dir();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    reserved::store(&kept, 100).expect("reserve");
    reserved::store(&dir, 40).expect("reserve");

    assert_eq!(first_free_index(&dir, Some(2), None).expect("pad"), 101);
    assert_eq!(
        first_free_index(&dir, Some(2), Some(500)).expect("pad"),
        501
    );
    assert_eq!(
        first_free_index(&dir, Some(200), Some(5)).expect("pad"),
        201
    );

    let logs = kept_logs(&dir, SPACING).expect("kept");
    assert_eq!(
        logs,
        vec![KeptLog {
            path: kept,
            copy: Some(old),
            last: Some(2),
            committed: None,
        }]
    );
    new.log.close().await.expect("close");
}

/// A copy that never reserved and an empty base pad from the floor alone.
#[tokio::test]
async fn with_no_reservation_the_floor_and_base_decide() {
    let root = Root::new();
    let dir = root.shard_dir();
    std::fs::create_dir_all(&dir).expect("dir");
    assert_eq!(first_free_index(&dir, None, None).expect("pad"), 0);
    assert_eq!(first_free_index(&dir, None, Some(7)).expect("pad"), 8);
}
