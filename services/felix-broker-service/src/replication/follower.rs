//! Where each follower is, as the leader understands it, and whether it has
//! caught up.

use std::net::SocketAddr;

/// How far a follower may be behind and still be fit to lead.
///
/// Zero: a follower is caught up when it holds every record the leader does.
///
/// A bound above zero is a bound on how much a promotion may silently lose, and
/// there is no honest value for it that is not a policy decision. Zero needs no
/// such decision, and a follower reaches it constantly on a healthy shard — the
/// leader only has to be momentarily idle. Loosening it is a change to make
/// deliberately, with a measurement behind it, rather than a default nobody
/// chose.
pub const CATCH_UP_BOUND: u64 = 0;

/// How far a follower has got, as this leader understands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerCursor {
    pub node_id: String,
    pub addr: SocketAddr,
    /// The offset to send next. Moved only by the follower's own answers.
    pub next_offset: u64,
    /// Set once this follower has answered something that does not resolve by
    /// retrying. Nothing more is shipped to it at this generation.
    pub halted: Option<Halt>,
    /// This follower discarded its copy at the leader's request and is being
    /// shipped from the leader's base. Holds one of the policy's slots until
    /// it reaches the tail.
    pub rebuilding: bool,
    /// This follower refused, or did not understand, a rebuild. Asked once per
    /// generation: nothing about a refusal changes with the next pass.
    pub rebuild_refused: bool,
    /// Bytes this follower has stored from this leader, for pacing a copy.
    pub shipped_bytes: u64,
    /// The last batch did not reach the follower, or it refused it: its
    /// position is not moving, however close it is.
    pub stalled: bool,
    /// The follower refused the frame kind that carries a commit offset, so
    /// it is shipped without one.
    pub legacy_frames: bool,
}

impl FollowerCursor {
    pub fn new(node_id: impl Into<String>, addr: SocketAddr, next_offset: u64) -> Self {
        Self {
            node_id: node_id.into(),
            addr,
            next_offset,
            halted: None,
            rebuilding: false,
            rebuild_refused: false,
            shipped_bytes: 0,
            stalled: false,
            legacy_frames: false,
        }
    }
}

/// Why replication to a follower stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Halt {
    /// The follower holds different bytes at an offset both sides have. Records
    /// are never rewritten, so there is no repair.
    #[error("the follower's log has diverged from this one")]
    Diverged,
    /// The follower knows a newer generation than this broker is shipping at.
    /// This broker is not the leader any more.
    #[error("this broker has been superseded as leader")]
    Fenced,
    /// The follower wants records retention has already removed from the
    /// leader, so shipping cannot reach it. It needs its history transferred
    /// before live replication can resume.
    #[error("the follower needs history this leader no longer holds")]
    NeedsBootstrap,
}

impl Halt {
    /// Whether discarding the follower's copy would resolve this.
    ///
    /// A fenced halt is this broker's problem, not the follower's: it is no
    /// longer the leader, and nothing it ships is authoritative.
    pub(super) fn rebuildable(self) -> bool {
        matches!(self, Halt::Diverged | Halt::NeedsBootstrap)
    }
}

/// Which followers hold enough of the log to lead it.
///
/// Reported to the control plane, which gates promotion on it. A halted
/// follower never qualifies however close its last position was: it has stopped
/// rather than fallen behind, and its position is no longer moving toward the
/// leader's.
// The bound is zero today, so "within it" is an equality and clippy says so.
// Written as a comparison because the bound is the thing meant to change: if it
// is ever raised, this reads correctly without being rediscovered.
#[allow(clippy::absurd_extreme_comparisons)]
pub fn caught_up(tail: u64, followers: &[FollowerCursor]) -> Vec<String> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .filter(|follower| tail.saturating_sub(follower.next_offset) <= CATCH_UP_BOUND)
        .map(|follower| follower.node_id.clone())
        .collect()
}

/// How far behind the slowest follower is, in records.
///
/// This is the `Leader` consistency level's loss window, and the design note is
/// explicit that it has to be observable: an operator choosing `Leader` is
/// choosing this window, and a bound nobody can see is not a bound.
///
/// A halted follower is excluded. It is not lagging, it has stopped, and
/// folding "stopped" into a lag figure hides it behind a number that merely
/// looks large.
pub fn lag_records(tail: u64, followers: &[FollowerCursor]) -> Option<u64> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .map(|follower| tail.saturating_sub(follower.next_offset))
        .max()
}
