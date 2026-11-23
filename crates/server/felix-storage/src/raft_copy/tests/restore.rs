//! Cutting a Raft copy back to a backup point.

use openraft::storage::RaftLogStorage;
use tempfile::TempDir;

use super::*;
use crate::DiskLog;
use crate::raft_log::RaftLog;
use crate::raft_log::snapshot::{self, SnapshotId, StoredSnapshot};
use crate::raft_log::tests::writes::append_flushed;
use crate::raft_log::tests::{Codec, Types, log_id, normal};

const SPACING: u64 = 4096;

/// The broker's own config, which is not a Raft log's: the cut must make it
/// one, or the saved commit names no term and a re-form vouches for nothing.
fn broker_config() -> LogConfig {
    LogConfig {
        raft_log: false,
        ..config()
    }
}

/// A copy holding entries 0..=9 of term 1, all committed.
async fn committed_copy() -> TempDir {
    let dir = tempfile::tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let mut raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");
    let entries: Vec<_> = (0..10).map(|index| normal(1, index, "e")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(1, 9))).await.expect("save");
    raft.disk_log().close().await.expect("close");
    dir
}

/// **A copy cut to a point ends there and vouches for exactly the entries
/// below it**, which is what a re-form at that point reads; a second run
/// changes nothing.
#[tokio::test]
async fn a_cut_copy_vouches_for_exactly_the_entries_below_the_point() {
    let dir = committed_copy().await;
    for _ in 0..2 {
        restore_copy(dir.path(), "t/ns/raft/0", 6, broker_config())
            .await
            .expect("restore");
        let kept = read_kept(dir.path(), SPACING).expect("read");
        assert_eq!((kept.last, kept.committed), (Some(5), Some(5)));
    }
}

/// **A snapshot past the point refuses the cut, and nothing is written**:
/// the state as of the point is not in the copy any more.
#[tokio::test]
async fn a_snapshot_past_the_point_refuses_the_cut() {
    let dir = committed_copy().await;
    let stored = StoredSnapshot {
        last: Some(SnapshotId { term: 1, index: 7 }),
        meta: bytes::Bytes::new(),
        data: bytes::Bytes::new(),
    };
    snapshot::store_in(dir.path(), &stored).expect("store");
    let err = restore_copy(dir.path(), "t/ns/raft/0", 6, broker_config())
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("snapshot covers index 7"), "{err}");
    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(9), Some(9)));

    // One at or below the point's last entry is fine.
    let below = StoredSnapshot {
        last: Some(SnapshotId { term: 1, index: 5 }),
        ..stored
    };
    snapshot::store_in(dir.path(), &below).expect("store");
    restore_copy(dir.path(), "t/ns/raft/0", 6, broker_config())
        .await
        .expect("restore");
}

/// **A copy that re-formed after the point refuses the cut.**
#[tokio::test]
async fn a_copy_re_formed_past_the_point_refuses_the_cut() {
    let dir = committed_copy().await;
    let file = epoch_file::EpochFile {
        incarnation: 0,
        bases: vec![EpochBase {
            base_last: None,
            first_index: 6,
        }],
    };
    epoch_file::store(dir.path(), &file).expect("store");
    let err = restore_copy(dir.path(), "t/ns/raft/0", 6, broker_config())
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("re-formed at index 6"), "{err}");
}
