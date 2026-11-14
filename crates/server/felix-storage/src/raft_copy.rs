//! Which copy of a shard a Raft member is, and when a copy has to become a
//! new one.
//!
//! A member id is a broker and a copy id. The copy id is minted in the shard
//! directory (`raft-copy`) together with the storage id it was minted
//! under. The data root keeps a mark log ([`MarkLog`]): for each shard, the
//! copy id the marks belong to, the highest vote it saved and its last log
//! id. A member opens its directory through [`CopyRoot::open_copy`], which
//! rotates the copy (new copy id, never voted, empty log) when the directory
//! is empty, was minted under another storage id, names another copy than
//! its marks, or holds a vote or a log below them.
//!
//! A root opens only through the gate ([`check`]): its sequence, which rises with
//! every synced mark write, is compared with the run witness's and the
//! control plane's copies, and a root that is behind either, or behind the
//! node's incarnation, gets a new storage id before any member opens. A root
//! holding marks with no copy to compare against opens nothing until one
//! arrives.
//!
//! The design is in `docs/per-shard-raft.md`, "Group and member identity",
//! and the rules are the ones `docs/formal/FelixRaftIdentity.tla` checks.

mod copy_file;
mod gate;
mod judge;
mod marks;
mod reclaim;
#[cfg(test)]
pub(crate) mod tests;

pub use copy_file::CopyFile;
pub use gate::{Behind, ControlPlaneCopy, Evidence, Gate, GatePassed, Unchecked, check};
pub use judge::{
    CopyRoot, KeptCopy, OpenedCopy, RotateReason, Rotation, holds_log_without_copy, kept_copies,
};
pub use marks::{CopyMarks, MarkLog};
pub use reclaim::{CurrentCopy, HeldCopy, HeldReason, KeptReport, reclaim_kept};

use std::sync::Arc;
use std::time::Duration;

use crate::raft_log::LogPoint;

/// A saved vote as the marks keep it: the generation (term plus one), the
/// leader it names and whether it is committed. Generation 0 with no leader
/// is no vote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VoteMark {
    pub generation: u64,
    pub leader: Option<Arc<str>>,
    pub committed: bool,
}

impl VoteMark {
    /// Whether this vote is at or above `mark` in openraft's order: a higher
    /// generation, or at the same one the same vote, a committed vote over
    /// an uncommitted one, or two uncommitted votes where the mark names no
    /// leader or this vote's leader.
    pub fn at_or_above(&self, mark: &VoteMark) -> bool {
        if self.generation != mark.generation {
            return self.generation > mark.generation;
        }
        self == mark
            || (self.committed && !mark.committed)
            || (!self.committed
                && !mark.committed
                && (mark.leader.is_none() || mark.leader == self.leader))
    }
}

/// What the root holds for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMarks {
    /// The copy id these marks belong to.
    pub copy: u64,
    pub vote: VoteMark,
    /// The last log id the directory had fsynced. `None` is no entry.
    pub log: Option<LogPoint>,
}

impl ShardMarks {
    /// Marks for a copy that never voted and holds no entry.
    pub fn fresh(copy: u64) -> Self {
        Self {
            copy,
            vote: VoteMark::default(),
            log: None,
        }
    }
}

/// Whether log id `found` is below `mark`: an older term, or the same term
/// and a lower index. No entry is below every entry.
pub(crate) fn log_below(found: Option<LogPoint>, mark: Option<LogPoint>) -> bool {
    let key = |point: Option<LogPoint>| point.map(|at| (at.term, at.index));
    key(found) < key(mark)
}

/// How a shard's last log id reaches its mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogMarkMode {
    /// Synced after each flush and before Raft hears the entries are on
    /// disk. Concurrent flushes share one mark write.
    Exact,
    /// One synced write per interval covering every shard whose flushed
    /// position moved. The mark trails the log by up to the interval.
    Batched(Duration),
}

/// A random copy id, never 0. Copy ids only need to differ from the ids a
/// shard had before, so a process-seeded hash of the time is enough.
pub fn random_copy_id() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    loop {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos()),
        );
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        let id = hasher.finish();
        if id != 0 {
            return id;
        }
    }
}
