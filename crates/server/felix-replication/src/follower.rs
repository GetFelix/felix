//! Where each follower is, as the leader understands it, and whether it has
//! caught up.

use std::net::SocketAddr;

use felix_storage::StorageId;

use crate::peer::{Incarnation, PeerIdentity};

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
pub(crate) const CATCH_UP_BOUND: u64 = 0;

/// How far a follower has got, as this leader understands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FollowerCursor {
    pub node_id: String,
    pub addr: SocketAddr,
    /// The offset to send next. Moved only by the follower's own answers.
    pub next_offset: u64,
    /// How far this follower has answered, at this generation, that its log
    /// is this leader's. Unlike `next_offset`, never taken from where a
    /// follower says it wants to resume, which nobody compared.
    pub confirmed: u64,
    /// Set once this follower has answered something that does not resolve by
    /// retrying. Nothing more is shipped to it at this generation.
    pub halted: Option<Halt>,
    /// The ballot this follower named when it fenced this leader
    /// ([`Halt::Fenced`]): a newer generation, or this one promised to
    /// another node. `None` when it named none, such as for a newer set.
    pub(crate) fenced_by: Option<u64>,
    /// This follower discarded its copy at the leader's request and is being
    /// shipped from the leader's base. Holds one of the policy's slots until
    /// it reaches the tail.
    pub rebuilding: bool,
    /// This follower refused, or did not understand, a rebuild, and is not
    /// asked again until the backoff runs out.
    pub rebuild_refused: Option<RebuildBackoff>,
    /// Bytes this follower has stored from this leader, for pacing a copy.
    pub shipped_bytes: u64,
    /// The last batch did not reach the follower, or it refused it, or it has
    /// not answered one yet: its position is not moving, however close it is.
    /// A new cursor starts stalled, since its offset is only where shipping
    /// will start and says nothing about what the follower holds.
    pub stalled: bool,
    /// The follower's last answer to a batch was that its routing view is
    /// behind, and it took the fence the pass sent it again, so the next
    /// pass resolves it. Stalled, but up and answering, unlike a follower
    /// the request never reached or one refusing for a reason a pass cannot
    /// fix.
    pub(crate) refused: bool,
    /// The follower refused the frame kind that carries a commit offset, so
    /// it is shipped without one.
    pub legacy_frames: bool,
    /// The follower refused a batch that named no writers: it stamps records
    /// though this log does not yet. It is sent them from then on.
    pub(crate) wants_writers: bool,
    /// The storage identity the follower gave when this cursor's positions
    /// were learned; `None` from one that gives none. A different one is a
    /// different copy of the log, so the cursor starts over.
    pub storage_id: Option<StorageId>,
    /// The node incarnation the follower gave beside `storage_id`. A
    /// different one is another process, which may not hold what this one
    /// did, so the cursor starts over on it too.
    pub incarnation: Incarnation,
    /// The follower's last answer was that its routing view is behind this
    /// generation. An elected leader sends it the candidate fence again, since
    /// no control-plane route will carry that generation to it.
    pub(crate) route_behind: bool,
    /// The follower's last stored answer said its copy is a learner
    /// ([`ReplicateOk::learner`]): it lost what it promised before a wipe, so
    /// it counts toward no mark until an answer without the flag arrives.
    ///
    /// [`ReplicateOk::learner`]: felix_wire::internal::ReplicateOk::learner
    pub learner: bool,
    /// What the follower's last checked answer said it holds, and the copy
    /// and generation that answer came from. `confirmed` counts toward a
    /// mark only up to this, and only while the cursor still names that copy
    /// (see [`Self::vouched_at`]).
    pub(crate) vouched: Option<Vouched>,
    /// The newer generation the follower's ballot holds, from the refusal
    /// that halted it. Only set beside a fenced halt, and only while the
    /// cursor names the copy that refused.
    pub(crate) superseded_by: Option<u64>,
    /// The fenced halt's refusal said the control plane assigned a newer
    /// generation. See [`crate::replica::assigned_newer`].
    pub(crate) assigned_newer: bool,
    /// The follower's last answer said its copy does not follow this leader
    /// at this generation. See [`crate::replica::refuses_leader`].
    pub(crate) refuses_leader: bool,
    /// This copy stored a batch that left it level through the batch's end,
    /// this leader's start and a commit offset above zero: what a follower
    /// back from an unclean restart needs to be trusted again, or answered
    /// the one empty batch a log that never held a record sends instead.
    /// Until then a level follower is sent its last record again with the
    /// mark, since on an idle shard nothing else would reach it.
    pub(crate) mark_carried: bool,
}

/// A follower's answer that came from the copy its cursor describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Vouched {
    pub(crate) identity: PeerIdentity,
    pub(crate) generation: u64,
    pub(crate) offset: u64,
}

impl FollowerCursor {
    pub(crate) fn new(node_id: impl Into<String>, addr: SocketAddr, next_offset: u64) -> Self {
        Self {
            node_id: node_id.into(),
            addr,
            next_offset,
            confirmed: 0,
            halted: None,
            fenced_by: None,
            rebuilding: false,
            rebuild_refused: None,
            shipped_bytes: 0,
            stalled: true,
            refused: false,
            legacy_frames: false,
            wants_writers: false,
            storage_id: None,
            incarnation: Incarnation::Unsent,
            route_behind: false,
            learner: false,
            vouched: None,
            superseded_by: None,
            assigned_newer: false,
            refuses_leader: false,
            mark_carried: false,
        }
    }

    /// What this follower's last checked answer vouched for at `generation`,
    /// while the cursor still describes the copy and generation that answer
    /// came from. A copy the leader has seen change since, even only its
    /// incarnation, has to answer again. A halted follower vouches for
    /// nothing: it is no longer shipped to or pinged, so it must not be named
    /// holding the start. A learner is still reported holding what it vouched
    /// for; the marks leave it out ([`Self::counts`]).
    pub(crate) fn vouched_at(&self, generation: u64) -> u64 {
        match self.vouched {
            Some(vouched)
                if self.halted.is_none()
                    && vouched.identity == self.identity()
                    && vouched.generation == generation =>
            {
                vouched.offset.min(self.confirmed)
            }
            _ => 0,
        }
    }

    /// Whether this follower's `confirmed` counts toward a mark: not while it
    /// is halted, nor while its copy is a learner.
    pub(crate) fn counts(&self) -> bool {
        self.halted.is_none() && !self.learner
    }

    /// The identity this cursor's positions were learned under.
    pub(crate) fn identity(&self) -> PeerIdentity {
        PeerIdentity {
            storage: self.storage_id,
            incarnation: self.incarnation,
        }
    }
}

/// When a follower that refused a rebuild is asked again.
///
/// A refusal can resolve: the follower may have been restarted onto a build
/// that accepts, or have dropped the records it would not discard. Asking again
/// is harmless, since a rebuild either keeps a follower's committed records or
/// refuses, so the only cost of asking is the round trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RebuildBackoff {
    /// Refusals in a row from this follower at this generation.
    pub refusals: u32,
    /// No rebuild is requested before this.
    pub retry_at: tokio::time::Instant,
}

impl RebuildBackoff {
    /// The first wait after a refusal. Doubles with each refusal in a row.
    pub(crate) const FIRST: std::time::Duration = std::time::Duration::from_secs(5);
    /// The longest wait between two requests.
    pub(crate) const MAX: std::time::Duration = std::time::Duration::from_secs(300);

    /// The backoff after one more refusal than `previous`.
    pub(crate) fn after(previous: Option<Self>, now: tokio::time::Instant) -> Self {
        let refusals = previous.map_or(0, |backoff| backoff.refusals) + 1;
        let wait = Self::FIRST
            .saturating_mul(1 << (refusals - 1).min(16))
            .min(Self::MAX);
        Self {
            refusals,
            retry_at: now + wait,
        }
    }
}

/// Why replication to a follower stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Halt {
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
///
/// A follower counts only as far as it has answered holding at this
/// generation (`confirmed`). Before it answers, `next_offset` is the leader's
/// guess, one record below where its own generation begins.
// The bound is zero today, so "within it" is an equality and clippy says so.
// Written as a comparison because the bound is the thing meant to change: if it
// is ever raised, this reads correctly without being rediscovered.
#[allow(clippy::absurd_extreme_comparisons)]
pub(crate) fn caught_up(tail: u64, followers: &[FollowerCursor]) -> Vec<String> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .filter(|follower| tail.saturating_sub(follower.confirmed) <= CATCH_UP_BOUND)
        .map(|follower| follower.node_id.clone())
        .collect()
}

/// The newest generation a follower refused this leader's with, when the
/// followers left cannot make a majority with the leader.
///
/// Such a refusal never goes back, and it came after the fence, so no fence
/// reports it. A follower counts as left unless it is halted, a learner, the
/// staged `learner`, or stalled: one whose exchanges hang to their timeout
/// must not hold the report off.
pub(crate) fn refused_without_majority(
    followers: &[FollowerCursor],
    learner: Option<&str>,
) -> Option<u64> {
    let set: Vec<&FollowerCursor> = followers
        .iter()
        .filter(|follower| learner != Some(follower.node_id.as_str()))
        .collect();
    let ballot = set
        .iter()
        .filter(|follower| follower.halted == Some(Halt::Fenced))
        .filter_map(|follower| follower.superseded_by)
        .max()?;
    let left = set
        .iter()
        .filter(|follower| follower.counts())
        .filter(|follower| !follower.stalled)
        .count();
    (left + 1 < crate::quorum::majority_of(set.len())).then_some(ballot)
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
pub(crate) fn lag_records(tail: u64, followers: &[FollowerCursor]) -> Option<u64> {
    followers
        .iter()
        .filter(|follower| follower.halted.is_none())
        .map(|follower| tail.saturating_sub(follower.next_offset))
        .max()
}
