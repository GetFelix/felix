//! Copy identity: the mark log, the gate, the judge, and the Raft log's
//! hooks that keep the marks.

mod gate;
mod judge;
mod marks;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::TempDir;

use super::*;
use crate::StorageId;
use crate::log::{FsyncMode, LogConfig};
use crate::storage_identity;

pub(crate) const SHARD: &str = "t/ns/orders/0/Stream";

/// A start that knows nothing from outside the root.
pub(crate) const NO_EVIDENCE: Evidence<'static> = Evidence {
    witness_dir: None,
    control_plane: None,
    restored: false,
};

/// A data root with a storage id and a place for the run witness.
pub(crate) struct Root {
    dir: TempDir,
    shm: TempDir,
}

impl Root {
    pub(crate) fn new() -> Self {
        let root = Self {
            dir: tempfile::tempdir().expect("root"),
            shm: tempfile::tempdir().expect("shm"),
        };
        storage_identity::load_or_create(root.path(), || [1; 16]).expect("storage id");
        root
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn shard_dir(&self) -> PathBuf {
        self.path().join("shards").join("0")
    }

    pub(crate) fn gate(&self, control_plane: Option<u64>) -> Gate {
        check(
            self.path(),
            Evidence {
                witness_dir: Some(self.shm.path()),
                control_plane: control_plane.map(|sequence| self.copy(sequence)),
                restored: false,
            },
        )
        .expect("gate")
    }

    /// A control-plane copy of `sequence` under the root's current id.
    pub(crate) fn copy(&self, sequence: u64) -> ControlPlaneCopy {
        ControlPlaneCopy {
            storage_id: storage_identity::load(self.path())
                .expect("id")
                .expect("an id"),
            sequence,
        }
    }

    /// The root as a start opens it, through a gate that must pass.
    pub(crate) fn open(&self) -> CopyRoot {
        match self.gate(None) {
            Gate::Passed(passed) => CopyRoot::open(self.path(), passed, minter()),
            Gate::Behind(behind) => panic!("the root is behind: {behind:?}"),
            Gate::Unchecked(unchecked) => panic!("the root is unchecked: {unchecked:?}"),
        }
    }

    /// The witness records the root's sequence under its current storage
    /// id, as a rotation through the gate does.
    pub(crate) fn witness_current_id(&self) {
        let id = storage_identity::load(self.path())
            .expect("id")
            .expect("an id");
        MarkLog::open(self.path(), id, Some(self.shm.path().to_path_buf()))
            .expect("marks")
            .record_witness();
    }

    /// A machine restart: the run witness is gone.
    pub(crate) fn forget_witness(&self) {
        for entry in std::fs::read_dir(self.shm.path()).expect("shm") {
            std::fs::remove_file(entry.expect("entry").path()).expect("remove");
        }
    }

    /// The marks on disk, read by a fresh open.
    pub(crate) fn marks_on_disk(&self) -> Option<ShardMarks> {
        let id = storage_identity::load(self.path())
            .expect("id")
            .expect("an id");
        MarkLog::open(self.path(), id, None)
            .expect("marks")
            .marks(SHARD)
    }
}

/// Copy ids from 0x100 up, distinct across the whole test binary.
pub(crate) fn minter() -> impl Fn() -> u64 + Send + Sync + 'static {
    static NEXT: AtomicU64 = AtomicU64::new(0x100);
    || NEXT.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    }
}

pub(crate) fn vote(generation: u64, leader: &str, committed: bool) -> VoteMark {
    VoteMark {
        generation,
        leader: Some(Arc::from(leader)),
        committed,
    }
}

pub(crate) fn point(term: u64, index: u64) -> LogPoint {
    LogPoint { term, index }
}

/// Copy every file under `from` to `to`, replacing what `to` held: a
/// backup put back.
fn restore(from: &Path, to: &Path) {
    let _ = std::fs::remove_dir_all(to);
    copy_tree(from, to);
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}

#[test]
fn the_vote_order_is_openraft_s() {
    let none = VoteMark::default();
    let a1 = vote(1, "a", false);
    let b1 = vote(1, "b", false);
    let a1c = vote(1, "a", true);
    let a2 = vote(2, "a", false);
    assert!(none.at_or_above(&none));
    assert!(a1.at_or_above(&none));
    assert!(!none.at_or_above(&a1));
    assert!(a2.at_or_above(&a1c));
    assert!(!a1c.at_or_above(&a2));
    assert!(a1c.at_or_above(&a1));
    assert!(!a1.at_or_above(&a1c));
    // Two uncommitted votes in one term for two leaders: neither covers
    // the other.
    assert!(!a1.at_or_above(&b1));
    assert!(!b1.at_or_above(&a1));
    // A committed vote for another leader covers an uncommitted one.
    assert!(vote(1, "b", true).at_or_above(&a1));
    let unnamed = VoteMark {
        generation: 1,
        leader: None,
        committed: false,
    };
    assert!(a1.at_or_above(&unnamed));
}

#[test]
fn a_log_id_compares_by_term_then_index() {
    assert!(log_below(None, Some(point(0, 0))));
    assert!(!log_below(Some(point(0, 0)), None));
    assert!(log_below(Some(point(1, 9)), Some(point(2, 1))));
    assert!(!log_below(Some(point(2, 1)), Some(point(1, 9))));
    assert!(log_below(Some(point(2, 4)), Some(point(2, 5))));
    assert!(!log_below(Some(point(2, 5)), Some(point(2, 5))));
}
