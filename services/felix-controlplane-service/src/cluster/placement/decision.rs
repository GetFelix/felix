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
    /// A new shard waits for more brokers to register before it is placed:
    /// `live` are, fewer than its replication factor, and the cluster is
    /// within `MovePolicy::settle_millis` of its first broker registering.
    /// Placed on what is live at `until_millis`, on the store's clock, if no
    /// more have come.
    Settling {
        live: u32,
        until_millis: u64,
    },
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
    /// live before it finished, or a staged balance move met a broker down;
    /// its staging is undone. After the fence, the leader serves again.
    Abandon { successor: String },
    /// A move's destination, or a follower being copied in, stopped
    /// following: its leader reported it halted for `reason`. Undone as for
    /// `Abandon`, and the next pass picks somewhere else.
    Halted { successor: String, reason: String },
    /// A move's destination, or a follower being copied in, did not get
    /// close enough within `MovePolicy::timeout_millis` and is dropped, giving
    /// its slot to the next move.
    TimedOut { successor: String },
    /// A restore's new copy went `MovePolicy::restore_stall_millis` without
    /// its reported position moving. Dropped as for `TimedOut`, and the shard
    /// waits behind every other one for its next slot.
    Stalled { successor: String },
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
    /// On a fenced stream, the leader stops serving before a seat that
    /// leaves an even set, so `to` can be seen holding its whole log first.
    SeatFence { to: String },
    /// On a fenced stream, the leader stops serving before a replacement is
    /// undone beside an even set, so the rest can be seen holding its whole
    /// log before `successor` leaves.
    UndoFence { successor: String },
    /// The leader of a set above the replication factor stops serving, so
    /// a follower can leave once the rest hold its whole log.
    TrimFence,
    /// A set above the replication factor drops `from`, and its leader
    /// serves again.
    Trim { from: String },
    /// An operator cancelled a move or replacement before the fence: its
    /// destination is dropped, as for `TimedOut`.
    Cancel { successor: String },
    /// An operator cancelled a fenced move: the leader that was stopped
    /// serves again, at a new generation. `successor` is the destination
    /// dropped, if the move still had one.
    Retake { successor: Option<String> },
    /// The leader stayed fenced past `MovePolicy::fence_timeout_millis`
    /// without reporting drained: it serves again at a new generation, as for
    /// `Retake`, and `successor` is dropped.
    FenceTimedOut { successor: Option<String> },
    /// The fenced leader reported, at the fence's generation, that it gave
    /// its copy up in a crash, so it will never report drained: it is named
    /// again at a new generation, as for `FenceTimedOut`, but `successor`
    /// stays in the set as a follower. Its broker leads only if no follower is
    /// ahead of its copy.
    LeaderGaveUp { successor: Option<String> },
    /// An operator gave up the log of a shard whose only copies are out of
    /// reach: `to` leads it afresh, and whatever `from` held is lost.
    Discard { from: String, to: String },
    /// The leader reported its generation's start record acknowledged on a
    /// majority of the old set and of the pending one: the pending set
    /// becomes the control plane's.
    TakePendingSet,
    /// The leader never reported the joint start within
    /// `MovePolicy::pending_set_timeout_millis`: the change is dropped and
    /// the control plane's own set stays, at a new generation.
    PendingSetTimedOut,
    /// The leader reported `node`, a member of the set being left, caught up
    /// under a new storage identity before the joint start: the change is
    /// dropped and the control plane's own set stays, at a new generation,
    /// which names `node` by its new storage and holds it as a learner.
    PendingSetMemberReplaced { node: String },
    /// The leader reported that it refused to open its generation, having
    /// promised it to a node whose election the control plane recorded
    /// without adopting. It is named again at a new generation, above the
    /// one it promised away, on the same set.
    ElectionRefused,
    /// The leader reported that it refused its generation without naming a
    /// node: its joint fence cannot learn the set that generation leaves, as
    /// for a shard it learned from a snapshot, or it or a replica of its set
    /// already accepted a newer generation no assignment named. It is named
    /// again one generation up, on the same set, whose set before is the one
    /// it is named on now, or at `at_least` when that is higher: past a newer
    /// ballot its own copy holds for another node, which a fence one up would
    /// find promised away and wait on. 0 when there is none.
    SetUnknown { at_least: u64 },
    /// The leader's broker is live, but the leader has shown no progress at
    /// its generation for `MovePolicy::stuck_leader_millis`: it is named
    /// again one generation up, on the same set, as for `SetUnknown`. A new
    /// fenced term ends whatever held the old one, and never loses a record.
    Stuck { reason: super::StuckReason },
    /// The shard no longer elects (its consistency changed), so a pending
    /// set can never be reported: the change is dropped.
    PendingSetDropped,
    /// An operator cancelled the pending change.
    PendingSetCancelled,
    /// A member whose storage was replaced is caught up under its new
    /// identity: the set is written again at a new generation, naming it by
    /// that identity and holding it as a learner until it shows the
    /// generation's start record.
    Readmit { node: String },
    /// A Raft shard's placement leader moves to another member of its set.
    /// Only `leader` changes: the group keeps its members, and once brokers
    /// follow placement its leader hands over to the one placement names.
    RaftLeader { from: String, to: String },
    /// The group reported `to`, the leader an operator moved a Raft shard
    /// to within its set, leading: the move ends.
    RaftLeaderLanded { to: String },
    /// An operator's move of a Raft shard's leader within its set did not
    /// land within `RAFT_OPERATOR_LEADER_MILLIS`: `to` is named, the member
    /// the group runs while a member is down, else `from` again, and the
    /// move ends.
    RaftLeaderTimedOut { from: String, to: String },
    /// A member of a Raft shard's group is swapped for `to`, a broker outside
    /// the set, in one write: a follower on an internal, draining or lost
    /// broker, or the leader's seat on an operator move outside the set, when
    /// `to` is also named leader. The group's leader adds `to` as a learner
    /// and then swaps the voters through its membership change, so the group
    /// is never below its replication factor.
    RaftReplace { from: String, to: String },
    /// A hybrid move left on a Raft shard is dropped. Its group never
    /// reports what the move waits for.
    RaftMoveDropped,
    /// An RF 1 shard's move is undone: `node`, the destination, leaves the
    /// set and the old broker keeps the shard, because `node` is down,
    /// draining or gone before it votes, or draining while it votes, or an
    /// operator moved the shard back.
    RaftMoveUndone { node: String },
    /// An operator moved an in-memory shard to `to`: one write names it the
    /// owner at the next generation. Nothing is copied, so it starts empty.
    OwnerReplaced { from: String, to: String },
}

impl MoveStep {
    /// The generation the step's write takes at least, above the next one.
    /// 0 for the next.
    pub fn at_least(&self) -> u64 {
        match self {
            Self::SetUnknown { at_least } => *at_least,
            _ => 0,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Stage { .. } => "stage",
            Self::Fence => "fence",
            Self::CutOver { .. } => "cut_over",
            Self::Abandon { .. } => "abandon",
            Self::Halted { .. } => "halted",
            Self::TimedOut { .. } => "timed_out",
            Self::Stalled { .. } => "stalled",
            Self::Reseat { .. } => "reseat",
            Self::Restore { .. } => "restore",
            Self::Seat { .. } => "seat",
            Self::SeatFence { .. } => "seat_fence",
            Self::UndoFence { .. } => "undo_fence",
            Self::TrimFence => "trim_fence",
            Self::Trim { .. } => "trim",
            Self::Cancel { .. } => "cancel",
            Self::Retake { .. } => "retake",
            Self::FenceTimedOut { .. } => "fence_timed_out",
            Self::LeaderGaveUp { .. } => "leader_gave_up",
            Self::Discard { .. } => "discard",
            Self::TakePendingSet => "take_pending_set",
            Self::PendingSetTimedOut => "pending_set_timed_out",
            Self::PendingSetMemberReplaced { .. } => "pending_set_member_replaced",
            Self::ElectionRefused => "election_refused",
            Self::SetUnknown { .. } => "set_unknown",
            Self::Stuck { .. } => "stuck",
            Self::PendingSetDropped => "pending_set_dropped",
            Self::PendingSetCancelled => "pending_set_cancelled",
            Self::Readmit { .. } => "readmit",
            Self::RaftLeader { .. } => "raft_leader",
            Self::RaftLeaderLanded { .. } => "raft_leader_landed",
            Self::RaftLeaderTimedOut { .. } => "raft_leader_timed_out",
            Self::RaftReplace { .. } => "raft_replace",
            Self::RaftMoveDropped => "raft_move_dropped",
            Self::RaftMoveUndone { .. } => "raft_move_undone",
            Self::OwnerReplaced { .. } => "owner_replaced",
        }
    }
}

/// Why a Raft shard's member must leave its set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leaving {
    /// Its broker is marked internal and holds no tenant shards.
    Internal,
    /// Its broker is draining, or outside the stream's allowed regions.
    Draining,
    /// Its broker has been down or gone for `restore_after_millis`.
    Lost,
}

impl std::fmt::Display for Leaving {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Internal => "an internal node",
            Self::Draining => "draining",
            Self::Lost => "lost",
        })
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
    /// The leader has deregistered, and the next step would name it again at
    /// a new generation (a fenced move giving up, or a refused generation).
    /// The shard waits for another copy to take it, or fails over once the
    /// leader's lease has run out.
    LeaderDeparted,
    /// A move is wanted, and `MovePolicy::max_concurrent` is reached.
    MoveLimit,
    /// A move is wanted, and `node` already has `MovePolicy::max_per_node`
    /// copies going in or out.
    NodeMoveLimit { node: String },
    /// The leader is draining and no live node can take the shard.
    NoDestination,
    /// The leader is draining, and the only live nodes that could take the
    /// shard hold a copy of it replication has stopped for. `node` is one.
    DestinationHalted { node: String, reason: String },
    /// A move is wanted, and placement is paused.
    Paused,
    /// A move for balance is wanted, and a broker is down or a copy is being
    /// restored. It starts once that is over.
    Recovering,
    /// The set is above the replication factor, and no follower can go yet:
    /// a report at this generation does not show the rest caught up without
    /// one of them.
    OverFactor,
    /// A move is wanted, and this shard's last `given_up` moves in a row
    /// were given up at the fence: the next starts at `until_millis`, on the
    /// store's clock.
    BackingOff { given_up: u32, until_millis: u64 },
    /// A cut-over's set change is pending: nothing else changes the set
    /// until the leader reports the joint start, or the change is dropped
    /// (a timeout, a cancel, a failover).
    SetChangePending,
    /// A Raft shard has a member that must leave (`why`), and no live node
    /// outside the set can take its place. The set stays as it is, below no
    /// quorum, until one registers.
    NoReplacement { node: String, why: Leaving },
    /// A Raft shard has a member that must leave (`why`), and another member,
    /// `node`, is not serving. The leaving member stays until it is, so the
    /// group can finish the change.
    MemberDown { node: String, why: Leaving },
    /// A Raft shard's member is replaced once the control plane has stored
    /// the group's target for its current set: one set change at a time.
    TargetUnsettled,
    /// A Raft shard's set stays until a leader of its group has reported:
    /// a second set with its own target could form beside the first (#2794).
    Unformed,
    /// A formed Raft group has not reported voters that keep a majority of
    /// the next set (#2905).
    Unapplied,
    /// An RF 1 shard's move reached two voters, and the destination `node`
    /// is not serving. Either set of one needs its vote, so the shard stays
    /// until it is back or an operator re-forms the group.
    MoveStranded { node: String },
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
            Self::LeaderDeparted => write!(
                f,
                "the leader has deregistered; waiting for a caught-up copy to take over, or for its lease to run out"
            ),
            Self::MoveLimit => write!(f, "waiting for a move slot"),
            Self::NodeMoveLimit { node } => write!(f, "waiting for a move slot on {node}"),
            Self::NoDestination => write!(f, "no live node can take this shard"),
            Self::DestinationHalted { node, reason } => write!(
                f,
                "no live node can take this shard: the copy on {node} is halted ({reason})"
            ),
            Self::Paused => write!(f, "placement is paused"),
            Self::Recovering => write!(
                f,
                "a broker is down or a copy is being restored; moves for balance wait until that is over"
            ),
            Self::OverFactor => write!(
                f,
                "waiting for the replicas to catch up before dropping one above the replication factor"
            ),
            Self::BackingOff {
                given_up,
                until_millis,
            } => write!(
                f,
                "the last {given_up} moves of this shard were given up at the fence; \
                 the next starts at {until_millis}"
            ),
            Self::SetChangePending => write!(
                f,
                "waiting for the leader to report its start acknowledged on both the old and the new replica set"
            ),
            Self::NoReplacement { node, why } => write!(
                f,
                "the copy on {node}, {why}, has no live node to move to; it stays until one can take it"
            ),
            Self::MemberDown { node, why } => write!(
                f,
                "a copy must move off a broker that is {why}; waiting for {node} to serve before replacing it"
            ),
            Self::TargetUnsettled => write!(
                f,
                "waiting for the group's membership target for its current set before changing it"
            ),
            Self::Unformed => write!(f, "waiting for the group to form before changing its set"),
            Self::Unapplied => write!(
                f,
                "waiting for the group to report voters that keep a majority of the next set"
            ),
            Self::MoveStranded { node } => write!(
                f,
                "{node} votes in this group of two and is not serving; the move finishes once it is back, or re-form the group without it"
            ),
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

#[cfg(test)]
mod tests;
