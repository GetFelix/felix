//! What placement decided for a shard, and why it could not do more.
use crate::model::ShardAssignment;

/// What placement decided for one shard.
// One per shard per pass, so the size of the move variant costs nothing worth
// a box at every match.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The existing assignment is still valid. Nothing to write.
    Kept,
    /// This shard needs an assignment written: a leader, and the followers that
    /// will hold a copy of it.
    Place(String, Vec<String>),
    /// One step of a planned move, as the assignment to write for it.
    Move(MoveStep, ShardAssignment),
    /// A move is in progress and this pass can do nothing for it yet.
    Waiting(Blocked),
    Unplaceable(Unplaceable),
}

/// The steps of a planned move. Each is one assignment write at a new
/// generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveStep {
    /// The destination joins the replica set as `successor`.
    Stage { successor: String },
    /// The destination is caught up: the assignment goes `Draining` and the
    /// leader stops serving.
    Fence,
    /// The leader has stopped. `to` leads from the next generation.
    CutOver { from: String, to: String },
    /// A move's destination, or a follower being copied in, stopped being
    /// live before it finished; its staging is undone.
    Abandon { successor: String },
    /// A move's destination, or a follower being copied in, did not get
    /// close enough within `MovePolicy::timeout_millis` and is dropped, giving
    /// its slot to the next move.
    TimedOut { successor: String },
    /// A follower on a draining node starts being replaced: `to` joins the
    /// replica set beside it.
    Reseat { from: String, to: String },
    /// A follower's broker has been gone past
    /// `MovePolicy::restore_after_millis` (`replacing`), or the replica set
    /// is short of the replication factor: `to` joins the set.
    Restore {
        replacing: Option<String>,
        to: String,
    },
    /// The replacement has caught up, and the follower it replaces leaves.
    /// A restore that only tops the set up replaces nobody.
    Seat { from: Option<String>, to: String },
    /// An operator cancelled a move or replacement before the fence: its
    /// destination is dropped, as for `TimedOut`.
    Cancel { successor: String },
    /// An operator cancelled a fenced move: the leader that was stopped
    /// serves again, at a new generation. `successor` is the destination
    /// dropped, if the move still had one.
    Retake { successor: Option<String> },
    /// An operator gave up the log of a shard whose only copies are out of
    /// reach: `to` leads it afresh, and whatever `from` held is lost.
    Discard { from: String, to: String },
}

impl MoveStep {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Stage { .. } => "stage",
            Self::Fence => "fence",
            Self::CutOver { .. } => "cut_over",
            Self::Abandon { .. } => "abandon",
            Self::TimedOut { .. } => "timed_out",
            Self::Reseat { .. } => "reseat",
            Self::Restore { .. } => "restore",
            Self::Seat { .. } => "seat",
            Self::Cancel { .. } => "cancel",
            Self::Retake { .. } => "retake",
            Self::Discard { .. } => "discard",
        }
    }
}

/// Why a move could not advance this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocked {
    /// The successor, or a follower being copied in, is not yet within
    /// `MovePolicy::fence_max_lag_records` of the leader.
    DestinationCatchingUp { successor: String },
    /// The assignment is `Draining` and the leader has not reported drained:
    /// writes are still in flight there, or its group state or counters are
    /// not yet on every replica that could take over.
    LeaderStopping,
    /// A move is wanted, and `MovePolicy::max_concurrent` is reached.
    MoveLimit,
    /// A move is wanted, and `node` already has `MovePolicy::max_per_node`
    /// copies going in or out.
    NodeMoveLimit { node: String },
    /// The leader is draining and no live node can take the shard.
    NoDestination,
    /// A move is wanted, and placement is paused.
    Paused,
}

impl std::fmt::Display for Blocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DestinationCatchingUp { successor } => {
                write!(f, "waiting for {successor} to catch up")
            }
            Self::LeaderStopping => write!(
                f,
                "waiting for the leader to stop serving and hand over its logs"
            ),
            Self::MoveLimit => write!(f, "waiting for a move slot"),
            Self::NodeMoveLimit { node } => write!(f, "waiting for a move slot on {node}"),
            Self::NoDestination => write!(f, "no live node can take this shard"),
            Self::Paused => write!(f, "placement is paused"),
        }
    }
}

/// Why a shard could not be placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unplaceable {
    /// No node is live.
    NoEligibleNode,
    /// Every live node is at its `max_shards` cap.
    AllNodesAtCapacity,
    /// The shard was replicated, its leader is gone, and no replica that holds
    /// the log can take over.
    ///
    /// Deliberately unavailable rather than placed elsewhere. A node that has
    /// never seen the shard would serve an empty log at a new generation while
    /// the records sat on replicas that were not chosen — the failover would
    /// *be* the data loss, and nothing downstream would report it as one. This
    /// is visible, and it resolves on its own when a replica catches up or the
    /// old leader returns, or when an operator abandons the log.
    NoCaughtUpReplica,
    /// The shard has no replicas and its leader, which holds the only copy of
    /// the log, is not serving.
    ///
    /// Unavailable for the same reason as `NoCaughtUpReplica`. It resolves
    /// when `leader` returns, or when an operator abandons the log and has
    /// the shard placed afresh.
    OwnerUnavailable { leader: String },
    /// The stream belongs to `region`, and no live node is in it or in a
    /// region it has a bridge to. Not placed elsewhere: that is exactly the
    /// copy the region policy exists to refuse.
    NoNodeInRegion { region: String },
}

impl std::fmt::Display for Unplaceable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEligibleNode => write!(f, "no live node is available to lead this shard"),
            Self::AllNodesAtCapacity => {
                write!(f, "every live node is at its max_shards capacity")
            }
            Self::NoCaughtUpReplica => write!(
                f,
                "the leader is gone and no replica holding this shard's log can take over"
            ),
            Self::OwnerUnavailable { leader } => write!(
                f,
                "the only copy of this shard's log is on {leader}, which is not serving; \
                 waiting for it to return"
            ),
            Self::NoNodeInRegion { region } => write!(
                f,
                "no live node is in region {region} or in a region bridged from it"
            ),
        }
    }
}
