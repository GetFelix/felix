//! What each part of a cache group's state takes in its snapshot, and the
//! bound that keeps every snapshot under the cap a member takes. See
//! `docs/per-shard-raft.md`, "How big a cache group gets".

use crate::raft::install::MAX_SNAPSHOT_BYTES;

/// A snapshot's bytes outside its maps: the version, the log's time, the
/// mark's end and the three counts.
pub(super) const HEADER: u64 = 1 + 8 + 8 + 8 + 8 + 8;
/// One producer's id, floor and answer count.
pub(super) const PRODUCER: u64 = 8 + 8 + 8;
/// One kept answer: its sequence, tag and value.
pub(super) const ANSWER: u64 = 8 + 1 + 8;

/// A value's bytes: its key, version, expiry and value.
pub(super) fn entry(key: &str, value: usize) -> u64 {
    (4 + key.len() + 8 + 8 + 4 + value) as u64
}

/// A counter's bytes: its key and sum.
pub(super) fn counter(key: &str) -> u64 {
    (4 + key.len() + 8) as u64
}

/// The most values and counters a group's state holds, in snapshot bytes.
/// A put or an add that would take it past this answers
/// [`super::Answer::Full`].
///
/// The same on every member: a constant, never a setting, since members
/// that judged a write by different bounds would hold different states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Bound(pub(super) u64);

impl Bound {
    /// The bound under a snapshot cap of `cap`. Kept answers and floors
    /// are not counted against it: the sixteenth of the cap left over
    /// holds them, and the header.
    pub(crate) const fn under(cap: u64) -> Self {
        Self(cap - HEADER - cap / 16)
    }
}

impl Default for Bound {
    fn default() -> Self {
        Self::under(MAX_SNAPSHOT_BYTES)
    }
}
