//! Moving a shard off a leader that is still serving: stage, fence, cut over.
//!
//! Each step is one assignment write, decided from the store and fresh
//! reports alone, so any instance resumes a half-done move where the last
//! pass left it.
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use felix_router::RegionRouter;

use super::leadership::{Leadership, Owner, Shares};
use super::rendezvous::{choose_replicas, promote, score};
use super::restore::{newcomer, restore, restore_step};
use super::zones::{Domain, domain, domain_of, keep_spread};
use super::{Blocked, CaughtUp, Decision, MoveStep};
use crate::model::{HaltedCopy, MoveReason, Node, ShardAssignment, ShardKey, ShardState};

/// One at a time, like the broker's rebuild limit: a move is a full copy of
/// a shard's log.
pub const DEFAULT_MAX_CONCURRENT_MOVES: usize = 1;

/// How far behind the leader's tail a destination may be when the leader is
/// fenced. The fence stops new writes, and the drained report waits for the
/// destination to hold the rest, so this bounds how long the switch-over
/// waits on the copy -- a few milliseconds of shipping -- not what is lost.
pub const DEFAULT_FENCE_MAX_LAG_RECORDS: u64 = 1_000;

/// How long a move may copy before it is given up: long enough for a large
/// shard under a bandwidth limit, short enough that a copy that is stuck
/// gives its slot back the same hour.
pub const DEFAULT_MOVE_TIMEOUT_MILLIS: u64 = 30 * 60 * 1_000;

/// How long a fenced leader may go without reporting drained before the move
/// is given up and the leader serves again. Draining takes milliseconds once
/// the destination is level; a leader still fenced after this is stuck, and
/// its shard takes no writes until something ends the fence.
pub const DEFAULT_FENCE_TIMEOUT_MILLIS: u64 = 2 * 60 * 1_000;

/// The longest a shard waits to start a move after moves given up at the
/// fence. Each give-up costs the shard its writes for the fence timeout, so
/// the wait doubles from the timeout up to this.
pub(super) const MAX_FENCE_BACKOFF_MILLIS: u64 = 60 * 60 * 1_000;

/// How long a follower's broker may be gone before placement copies the
/// shard somewhere else: longer than a broker restart takes, so a rolling
/// restart does not copy every shard once per broker.
pub const DEFAULT_RESTORE_AFTER_MILLIS: u64 = 5 * 60 * 1_000;

/// How long a restore's new copy may go without its reported position moving
/// before the restore is given up. A copy being shipped to moves every pass;
/// one that has not in two minutes is not going to, and it holds the move slot
/// every other short shard is waiting for.
pub const DEFAULT_RESTORE_STALL_MILLIS: u64 = 2 * 60 * 1_000;

/// How long after a cluster's first broker registered a new shard waits for
/// enough brokers to hold all its copies before it is placed on fewer.
/// Brokers started together register within a second or two of each other.
pub const DEFAULT_PLACEMENT_SETTLE_MILLIS: u64 = 10_000;

/// How moves are paced, and which regions a shard may be placed in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovePolicy {
    /// Copies in flight at once, cluster-wide: moves from staging to
    /// cut-over, and follower replacements until the new follower has caught
    /// up. `0` starts nothing: a drain waits and an imbalance stays, both
    /// visibly.
    pub max_concurrent: usize,
    /// Copies in flight into or out of any one node. `None` is no limit
    /// beyond `max_concurrent`.
    pub max_per_node: Option<usize>,
    /// How far behind the leader a destination may be for the leader to be
    /// fenced. Leaders that do not report their tail are waited on until the
    /// destination is exactly level.
    pub fence_max_lag_records: u64,
    /// How long a move may copy before the fence, or a replacement before it
    /// has caught up, before it is abandoned. `None` never gives up.
    pub timeout_millis: Option<u64>,
    /// How long a fenced leader may go without reporting drained, as this
    /// instance has watched it, before the move is given up and the leader
    /// takes the shard back at a new generation. `None` waits forever.
    pub fence_timeout_millis: Option<u64>,
    /// How long a follower's broker must have been down or gone, or its copy
    /// reported halted, before placement replaces the copy it held, restoring
    /// the replication factor.
    /// `None` never replaces one; a replica set short of the factor is still
    /// topped up.
    pub restore_after_millis: Option<u64>,
    /// How long a restore's new copy may go without its reported position
    /// moving, as this instance watched it, before the restore is given up and
    /// its slot goes to the next shard. `None` waits for `timeout_millis`.
    pub restore_stall_millis: Option<u64>,
    /// How long after the cluster's first broker registered a new shard
    /// waits for as many live brokers as its replication factor before it
    /// is placed on fewer. Only a young cluster waits: one whose oldest
    /// broker registered longer ago places on whatever is live. `None` never
    /// waits.
    pub settle_millis: Option<u64>,
    /// Placement starts no moves or replacements of its own; those in flight
    /// go on, and an operator may still start one. Not configuration: each
    /// pass reads it from the store (`POST /v1/placement/pause`).
    pub paused: bool,
    /// Where a stream with a home region may have copies: that region, and
    /// any region the allowlist bridges it to (`FELIX_REGION_BRIDGES`). A
    /// stream without one is placed anywhere.
    pub regions: Arc<RegionRouter<String>>,
}

impl Default for MovePolicy {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT_MOVES,
            max_per_node: None,
            fence_max_lag_records: DEFAULT_FENCE_MAX_LAG_RECORDS,
            timeout_millis: Some(DEFAULT_MOVE_TIMEOUT_MILLIS),
            fence_timeout_millis: Some(DEFAULT_FENCE_TIMEOUT_MILLIS),
            restore_after_millis: Some(DEFAULT_RESTORE_AFTER_MILLIS),
            restore_stall_millis: Some(DEFAULT_RESTORE_STALL_MILLIS),
            settle_millis: Some(DEFAULT_PLACEMENT_SETTLE_MILLIS),
            paused: false,
            regions: Arc::new(RegionRouter::new(String::new())),
        }
    }
}

/// The move slots one pass hands out, cluster-wide and per node.
pub(super) struct Moves {
    in_flight: usize,
    per_node: HashMap<String, usize>,
    policy: MovePolicy,
    /// Whether a move may start for the cluster's leader totals.
    pub(super) cluster_moves: bool,
    /// Whether some shard wanted a move for its stream's spread.
    pub(super) stream_wanted: bool,
}

impl Moves {
    /// The copies `existing` already has in flight, except a copy joining a
    /// shard that is `unpaced`.
    pub(super) fn counting(
        existing: &[ShardAssignment],
        policy: MovePolicy,
        unpaced: &dyn Fn(&ShardAssignment) -> bool,
    ) -> Self {
        let mut moves = Self {
            in_flight: 0,
            per_node: HashMap::new(),
            policy,
            cluster_moves: true,
            stream_wanted: false,
        };
        for assignment in existing {
            if assignment.successor.is_some() || assignment.state == ShardState::Draining {
                moves.take(&assignment.leader, assignment.successor.as_deref());
            }
            if let Some(joining) = assignment.joining.as_deref()
                && !unpaced(assignment)
            {
                moves.take(&assignment.leader, Some(joining));
            }
        }
        moves
    }

    /// The most records a shard's leader may hold for a copy into it to
    /// start without a slot.
    pub(super) fn unpaced_max_records(&self) -> u64 {
        self.policy.fence_max_lag_records
    }

    /// Start a copy that takes no slot, unless placement is paused.
    pub(super) fn begin_unpaced(&self) -> Result<(), Blocked> {
        if self.policy.paused {
            return Err(Blocked::Paused);
        }
        Ok(())
    }

    /// Take a slot for a copy placement wants from `from` to `to`, if
    /// placement is not paused and both nodes and the cluster have one free.
    pub(super) fn begin(&mut self, from: &str, to: &str) -> Result<(), Blocked> {
        if self.policy.paused {
            return Err(Blocked::Paused);
        }
        self.begin_requested(from, to)
    }

    /// Take a slot for a copy an operator asked for. Pausing placement does
    /// not stop these; the limits do.
    pub(super) fn begin_requested(&mut self, from: &str, to: &str) -> Result<(), Blocked> {
        if self.in_flight >= self.policy.max_concurrent {
            return Err(Blocked::MoveLimit);
        }
        if let Some(max) = self.policy.max_per_node {
            for node in [from, to] {
                if self.per_node.get(node).copied().unwrap_or(0) >= max {
                    return Err(Blocked::NodeMoveLimit {
                        node: node.to_string(),
                    });
                }
            }
        }
        self.take(from, Some(to));
        Ok(())
    }

    fn take(&mut self, from: &str, to: Option<&str>) {
        self.in_flight += 1;
        for node in std::iter::once(from).chain(to) {
            *self.per_node.entry(node.to_string()).or_default() += 1;
        }
    }

    /// Whether `destination` is close enough to the leader to fence it.
    pub(super) fn ready_to_fence(
        &self,
        caught_up: &dyn CaughtUp,
        key: &ShardKey,
        destination: &str,
    ) -> bool {
        caught_up.is_caught_up(key, destination)
            || caught_up
                .lag_records(key, destination)
                .is_some_and(|lag| lag <= self.policy.fence_max_lag_records)
    }

    /// Whether a copy started at `started` has run past the timeout, on the
    /// clock the reports were read at.
    pub(super) fn timed_out(&self, caught_up: &dyn CaughtUp, started: Option<u64>) -> bool {
        match (
            self.policy.timeout_millis,
            started,
            caught_up.as_of_millis(),
        ) {
            (Some(timeout), Some(started), Some(now)) => now.saturating_sub(started) > timeout,
            _ => false,
        }
    }

    /// Whether `key` has stayed fenced at `generation` past the fence
    /// timeout, as this instance watched it.
    pub(super) fn fence_timed_out(
        &self,
        caught_up: &dyn CaughtUp,
        key: &ShardKey,
        generation: u64,
    ) -> bool {
        match (
            self.policy.fence_timeout_millis,
            caught_up.fenced_at_millis(key, generation),
            caught_up.as_of_millis(),
        ) {
            (Some(timeout), Some(fenced), Some(now)) => now.saturating_sub(fenced) > timeout,
            _ => false,
        }
    }

    /// When `key` may next start a move, if its last moves were given up at
    /// the fence: the fence timeout after the first, doubling with each one
    /// after, up to [`MAX_FENCE_BACKOFF_MILLIS`].
    pub(super) fn backing_off(&self, caught_up: &dyn CaughtUp, key: &ShardKey) -> Option<Blocked> {
        let (given_up, at) = caught_up.fence_give_ups(key)?;
        let now = caught_up.as_of_millis()?;
        let base = self
            .policy
            .fence_timeout_millis
            .unwrap_or(DEFAULT_FENCE_TIMEOUT_MILLIS);
        let wait = base
            .saturating_mul(1u64 << given_up.saturating_sub(1).min(20))
            .min(MAX_FENCE_BACKOFF_MILLIS);
        let until = at.saturating_add(wait);
        (now < until).then_some(Blocked::BackingOff {
            given_up,
            until_millis: until,
        })
    }

    /// Whether a restore's copy into `joining` has gone the stall window
    /// without its position moving, as this instance watched it.
    pub(super) fn stalled(&self, caught_up: &dyn CaughtUp, key: &ShardKey, joining: &str) -> bool {
        match (
            self.policy.restore_stall_millis,
            caught_up.progressed_at_millis(key, joining),
            caught_up.as_of_millis(),
        ) {
            (Some(window), Some(progressed), Some(now)) => now.saturating_sub(progressed) > window,
            _ => false,
        }
    }
}

/// What to do about a shard whose leader is still serving.
///
/// Stage the destination as a replica; fence once it is within the lag
/// bound, which stops the leader; cut over once the leader reports drained,
/// which it does only once the destination holds everything. A leader that
/// dies mid-move is handled by the failover path, where the successor is a
/// candidate like any other replica unless the shard is fenced. Every input is
/// in the store or a fresh report, so any pass resumes where the last left off.
#[allow(clippy::too_many_arguments)]
pub(super) fn move_step<'a>(
    key: &ShardKey,
    existing: &'a ShardAssignment,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    is_draining: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    load: &mut HashMap<&'a str, u32>,
    leaders: &mut Leadership<'a>,
    shares: Shares,
    moves: &mut Moves,
    fenced: bool,
    is_lost: &dyn Fn(&str) -> bool,
) -> Decision {
    let leader = existing.leader.as_str();
    let leader_live = is_live(leader);
    let caught_up = &AtGeneration {
        inner: caught_up,
        generation: existing.generation,
    };
    // The maps are keyed by catalog-lifetime strings.
    let catalog_id = |id: &str| -> Option<&'a str> {
        eligible
            .iter()
            .find(|node| node.node_id == id)
            .map(|node| node.node_id.as_str())
    };
    let started = caught_up.as_of_millis();
    // A copy in this generation's set that its leader has stopped shipping to
    // will never catch up, so a move waiting on it is given up.
    let halted_now = |node: &str| {
        caught_up
            .halted_at(key, node, existing.generation)
            .map(|halt| halt.reason.clone())
    };
    // Kept off as a destination, as of any report still believed.
    let is_halted = |node: &str| caught_up.halted(key, node).is_some();

    if existing.state == ShardState::Draining {
        // The move timeout stops here. The leader has stopped serving, and
        // going back is a new generation and every client following the
        // shard twice; going on waits for at most the lag bound's worth of
        // copy. Only a leader that never says drained is given the shard back.
        if !caught_up.is_drained(key, existing.generation) {
            // The fence may land while the destination is still copying, and
            // the leader will not report drained until it is level. One that
            // died first never will be: drop it, still fenced, and the leader
            // reports drained against the followers it has. The cut-over then
            // picks one of them, or hands the shard back to the leader.
            // A halted one never will either.
            if let Some(successor) = existing
                .successor
                .as_deref()
                .filter(|s| !is_live(s) || halted_now(s).is_some())
            {
                let mut replicas = existing.replicas.clone();
                replicas.retain(|replica| replica != successor);
                return Decision::Move(
                    given_up(successor, halted_now(successor)),
                    ShardAssignment {
                        successor: None,
                        replicas,
                        generation: 0,
                        fence_storage_ids: Default::default(),
                        ..existing.clone()
                    },
                );
            }
            // Nobody else has led since the fence, so the leader still holds
            // every write it accepted: the same step as an operator's cancel,
            // and conditional on this generation, so a cut-over planned from
            // a drained report and this cannot both land.
            if moves.fence_timed_out(caught_up, key, existing.generation) {
                return Decision::Move(
                    MoveStep::FenceTimedOut {
                        successor: existing.successor.clone(),
                    },
                    retake(existing, replication_factor),
                );
            }
            return Decision::Waiting(Blocked::LeaderStopping);
        }
        // Whoever leads next must hold everything the leader held.
        let target = existing
            .successor
            .as_deref()
            .filter(|successor| is_live(successor) && caught_up.is_caught_up(key, successor))
            .or_else(|| promote(key, existing, eligible, caught_up))
            // Nothing else holds the log; the leader takes it back at a new
            // generation and the move is chosen again.
            .unwrap_or(leader);
        let replicas = replicas_after_cut_over(
            key,
            existing,
            target,
            replication_factor,
            eligible,
            is_live,
            load,
        );
        // The tally already credits this shard to its live successor, so
        // only a cut-over somewhere else moves the count.
        let counted = existing
            .successor
            .as_deref()
            .filter(|successor| is_live(successor))
            .unwrap_or(leader);
        if counted != target {
            if catalog_id(counted).is_some() {
                leaders.remove(Owner::of(&existing.key), counted);
            }
            if let Some(to) = catalog_id(target) {
                leaders.add(Owner::of(&existing.key), to);
            }
        }
        return Decision::Move(
            MoveStep::CutOver {
                from: leader.to_string(),
                to: target.to_string(),
            },
            ShardAssignment {
                key: key.clone(),
                leader: target.to_string(),
                replicas,
                generation: 0,
                state: ShardState::Assigning,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                incarnation: 0,
                fence_storage_ids: Default::default(),
            },
        );
    }

    if let Some(successor) = existing.successor.as_deref() {
        let halted = halted_now(successor);
        if !is_live(successor) || halted.is_some() {
            // Gone or halted before it led. Undone rather than waited out, or
            // the move holds its slot until it times out.
            let mut replicas = existing.replicas.clone();
            replicas.retain(|replica| replica != successor);
            return Decision::Move(
                given_up(successor, halted),
                ShardAssignment {
                    successor: None,
                    move_started_at_millis: None,
                    move_reason: None,
                    replicas,
                    generation: 0,
                    fence_storage_ids: Default::default(),
                    ..existing.clone()
                },
            );
        }
        if moves.ready_to_fence(caught_up, key, successor) {
            return Decision::Move(
                MoveStep::Fence,
                ShardAssignment {
                    state: ShardState::Draining,
                    generation: 0,
                    fence_storage_ids: Default::default(),
                    ..existing.clone()
                },
            );
        }
        if moves.timed_out(caught_up, existing.move_started_at_millis) {
            return Decision::Move(
                MoveStep::TimedOut {
                    successor: successor.to_string(),
                },
                undo_staged(existing, successor, replication_factor),
            );
        }
        return Decision::Waiting(Blocked::DestinationCatchingUp {
            successor: successor.to_string(),
        });
    }

    if let Some(joining) = existing.joining.as_deref() {
        if let Some(reason) = halted_now(joining) {
            return Decision::Move(
                MoveStep::Halted {
                    successor: joining.to_string(),
                    reason,
                },
                ShardAssignment {
                    move_started_at_millis: None,
                    ..undo_replacement(existing, joining)
                },
            );
        }
        if existing.move_reason == Some(MoveReason::Restore) {
            return restore_step(
                existing,
                joining,
                replication_factor,
                is_live,
                is_lost,
                caught_up,
                moves,
                fenced,
            );
        }
        return replacement_step(
            existing,
            joining,
            eligible,
            is_live,
            is_draining,
            caught_up,
            moves,
            fenced,
        );
    }

    // Nothing in progress. A shard missing a copy gets one before anything
    // moves for balance: one more failure could cost it its majority.
    if leader_live
        && let Some(decision) = restore(
            key,
            existing,
            replication_factor,
            eligible,
            is_lost,
            &is_halted,
            caught_up,
            load,
            moves,
        )
    {
        return decision;
    }

    // Should a move start?
    let destination = |goal: Goal| {
        choose_destination(
            key,
            existing,
            replication_factor,
            eligible,
            &is_halted,
            leaders,
            load,
            shares,
            goal,
        )
    };
    let wanted = if !leader_live {
        // Draining: a caught-up live replica under its share is cheapest, the
        // copy already exists; otherwise the bounded choice, or any live node
        // with room.
        promote_under_share(
            key, existing, eligible, &is_halted, caught_up, leaders, shares,
        )
        .or_else(|| destination(Goal::Drain))
    } else {
        // The stream's own spread first, then the cluster's; see `leadership`.
        // Each only from the node leading most to one leading at least two
        // fewer, so moves converge instead of trading shards back and forth,
        // and a node is never given shards and then asked for them back.
        let gap = |count: &dyn Fn(&str) -> u32| {
            let counts = eligible.iter().map(|node| count(&node.node_id));
            let (fewest, most) = counts.fold((u32::MAX, 0), |(lo, hi), c| (lo.min(c), hi.max(c)));
            let mine = count(leader);
            mine == most && mine >= fewest.saturating_add(2)
        };
        let stream_over = gap(&|node| leaders.of(key, node));
        let over_share = moves.cluster_moves && gap(&|node| leaders.total(node));
        let for_stream = stream_over.then(|| destination(Goal::Stream)).flatten();
        moves.stream_wanted |= for_stream.is_some();
        for_stream.or_else(|| over_share.then(|| destination(Goal::Cluster)).flatten())
    };

    match wanted {
        Some(destination) => {
            // A move given up at the fence cost the shard its writes for the
            // fence timeout; the same move again soon would likely do the same.
            if let Some(blocked) = moves.backing_off(caught_up, key) {
                return Decision::Waiting(blocked);
            }
            if let Err(blocked) = moves.begin(leader, destination) {
                return Decision::Waiting(blocked);
            }
            if catalog_id(leader).is_some() {
                leaders.remove(Owner::of(&existing.key), leader);
            }
            leaders.add(Owner::of(&existing.key), destination);
            if !existing.replicas.iter().any(|r| r == destination) {
                *load.entry(destination).or_default() += 1;
            }
            let reason = if leader_live {
                MoveReason::Balance
            } else {
                MoveReason::Drain
            };
            let (step, assignment) =
                start(existing, destination, reason, caught_up, started, moves);
            Decision::Move(step, assignment)
        }
        None if !leader_live => Decision::Waiting(
            // Say so when a halted copy is what stands in the way, since that
            // is something an operator can act on.
            eligible
                .iter()
                .filter(|node| node.node_id != leader)
                .find_map(|node| {
                    caught_up
                        .halted(key, &node.node_id)
                        .map(|halt| Blocked::DestinationHalted {
                            node: node.node_id.clone(),
                            reason: halt.reason.clone(),
                        })
                })
                .unwrap_or(Blocked::NoDestination),
        ),
        None => reseat(
            key,
            existing,
            eligible,
            is_draining,
            &is_halted,
            caught_up,
            load,
            moves,
        ),
    }
}

/// The step that drops a destination that stopped being live, or halted.
fn given_up(successor: &str, halted: Option<String>) -> MoveStep {
    match halted {
        Some(reason) => MoveStep::Halted {
            successor: successor.to_string(),
            reason,
        },
        None => MoveStep::Abandon {
            successor: successor.to_string(),
        },
    }
}

/// The first write of a move to `destination`, whose slot is already taken:
/// stage it, or fence at once when it already holds the copy.
pub(super) fn start(
    existing: &ShardAssignment,
    destination: &str,
    reason: MoveReason,
    caught_up: &dyn CaughtUp,
    started: Option<u64>,
    moves: &Moves,
) -> (MoveStep, ShardAssignment) {
    let already_a_replica = existing.replicas.iter().any(|r| r == destination);
    let mut assignment = ShardAssignment {
        key: existing.key.clone(),
        leader: existing.leader.clone(),
        replicas: existing.replicas.clone(),
        generation: 0,
        state: existing.state,
        successor: Some(destination.to_string()),
        joining: None,
        move_started_at_millis: started,
        move_reason: Some(reason),
        incarnation: 0,
        fence_storage_ids: Default::default(),
    };
    if already_a_replica && moves.ready_to_fence(caught_up, &existing.key, destination) {
        // The copy is already there: straight to the fence.
        assignment.state = ShardState::Draining;
        return (MoveStep::Fence, assignment);
    }
    if !already_a_replica {
        assignment.replicas.push(destination.to_string());
    }
    (
        MoveStep::Stage {
            successor: destination.to_string(),
        },
        assignment,
    )
}

/// A fenced move given up: the leader that stopped serves again at a new
/// generation, and the destination is dropped unless the shard had it anyway.
/// The start stays, as for [`undo_staged`].
pub(super) fn retake(existing: &ShardAssignment, replication_factor: u32) -> ShardAssignment {
    let replicas = match existing.successor.as_deref() {
        Some(successor) => undo_staged(existing, successor, replication_factor).replicas,
        None => existing.replicas.clone(),
    };
    ShardAssignment {
        replicas,
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_reason: None,
        fence_storage_ids: Default::default(),
        ..existing.clone()
    }
}

/// A staged move without its destination: the copy is dropped unless the
/// stream had it anyway. The start stays, so this shard waits behind the
/// others for its next slot rather than being chosen again at once.
pub(super) fn undo_staged(
    existing: &ShardAssignment,
    successor: &str,
    replication_factor: u32,
) -> ShardAssignment {
    let mut replicas = existing.replicas.clone();
    if replicas.len() >= replication_factor.max(1) as usize {
        replicas.retain(|replica| replica != successor);
    }
    ShardAssignment {
        successor: None,
        move_reason: None,
        replicas,
        generation: 0,
        fence_storage_ids: Default::default(),
        ..existing.clone()
    }
}

/// A replacement without the follower it was copying in. The start stays,
/// as for [`undo_staged`].
pub(super) fn undo_replacement(existing: &ShardAssignment, joining: &str) -> ShardAssignment {
    let mut replicas = existing.replicas.clone();
    replicas.retain(|replica| replica != joining);
    ShardAssignment {
        replicas,
        generation: 0,
        joining: None,
        move_reason: None,
        fence_storage_ids: Default::default(),
        ..existing.clone()
    }
}

/// Start replacing a follower on a draining node with one that is staying,
/// or, with none draining, a follower crowding a zone with one in a zone the
/// shard has no copy in.
///
/// Draining only, never merely down: a rolling restart takes every node down
/// in turn, and reseating each would copy every shard once per restart.
///
/// The replacement joins beside the follower it replaces, which leaves once
/// it has caught up (`replacement_step`), so the shard keeps every copy it
/// asked for while the new one fills.
#[allow(clippy::too_many_arguments)]
fn reseat<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    is_draining: &dyn Fn(&str) -> bool,
    is_halted: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Decision {
    let draining = existing
        .replicas
        .iter()
        .find(|replica| is_draining(replica));
    let Some(departing) = draining.or_else(|| crowded(existing, eligible, None)) else {
        return Decision::Kept;
    };
    // Swapped only when someone can take its place; dropping it costs a copy.
    let (widening, best) = newcomer(
        key,
        existing,
        eligible,
        is_halted,
        load,
        Some(departing.as_str()),
    );
    // A crowded follower is only worth a copy if the shard gains a zone.
    let replacement = match draining {
        Some(_) => widening.or(best),
        None => widening,
    };
    let Some(replacement) = replacement else {
        return Decision::Kept;
    };
    if let Err(blocked) = moves.begin(&existing.leader, &replacement.node_id) {
        return Decision::Waiting(blocked);
    }
    *load.entry(replacement.node_id.as_str()).or_default() += 1;
    let mut replicas = existing.replicas.clone();
    replicas.push(replacement.node_id.clone());
    Decision::Move(
        MoveStep::Reseat {
            from: departing.clone(),
            to: replacement.node_id.clone(),
        },
        ShardAssignment {
            replicas,
            generation: 0,
            joining: Some(replacement.node_id.clone()),
            move_started_at_millis: caught_up.as_of_millis(),
            move_reason: Some(MoveReason::Replace),
            fence_storage_ids: Default::default(),
            ..existing.clone()
        },
    )
}

/// A follower replacement in progress: seat it once it has caught up, or
/// undo it if it cannot finish.
#[allow(clippy::too_many_arguments)]
fn replacement_step(
    existing: &ShardAssignment,
    joining: &str,
    eligible: &[&Node],
    is_live: &dyn Fn(&str) -> bool,
    is_draining: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    moves: &Moves,
    fenced: bool,
) -> Decision {
    let departing = existing
        .replicas
        .iter()
        .find(|replica| replica.as_str() != joining && is_draining(replica))
        .or_else(|| crowded(existing, eligible, Some(joining)));
    let undo = |step: MoveStep, started| {
        Decision::Move(
            step,
            ShardAssignment {
                move_started_at_millis: started,
                ..undo_replacement(existing, joining)
            },
        )
    };
    // Nothing to replace any more (the node was undrained), or nowhere to
    // put the copy: the follower that was staying stays.
    let Some(departing) = departing.filter(|_| is_live(joining)) else {
        return undo(
            MoveStep::Abandon {
                successor: joining.to_string(),
            },
            None,
        );
    };
    if moves.ready_to_fence(caught_up, &existing.key, joining)
        && (!fenced || holds_what_the_set_held(existing, joining, caught_up))
    {
        let mut replicas = existing.replicas.clone();
        replicas.retain(|replica| replica != departing);
        return Decision::Move(
            MoveStep::Seat {
                from: Some(departing.clone()),
                to: joining.to_string(),
            },
            ShardAssignment {
                replicas,
                generation: 0,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                fence_storage_ids: Default::default(),
                ..existing.clone()
            },
        );
    }
    if moves.timed_out(caught_up, existing.move_started_at_millis) {
        return undo(
            MoveStep::TimedOut {
                successor: joining.to_string(),
            },
            existing.move_started_at_millis,
        );
    }
    Decision::Waiting(Blocked::DestinationCatchingUp {
        successor: joining.to_string(),
    })
}

/// Whether `joining` holds everything a majority of the set it joined holds,
/// by a report at the current generation.
///
/// Seating drops a follower, and on a fenced stream the next promotion counts
/// a majority of the smaller set. A record acknowledged before the joiner
/// came in may sit on the leader and the departing follower alone; seated
/// early, the joiner and the lagging follower are a majority without it. A
/// record acknowledged since needed three of the four, so it survives the
/// drop. `FelixShardFencedAckSeatEarly` in `docs/formal/FelixShard.tla`.
///
/// A member the report leaves out is taken to be level with the leader: its
/// position is unknown, and assuming less could pass a record it holds.
///
/// The joiner must also be one the leader offers as a successor. Offsets
/// cover only the shard's own log, and a `Quorum` cache's acknowledged
/// counter updates reach placement only through that offer: a copy seated
/// without them counts toward the majority but could not take over.
pub(super) fn holds_what_the_set_held(
    existing: &ShardAssignment,
    joining: &str,
    caught_up: &dyn CaughtUp,
) -> bool {
    let key = &existing.key;
    if !caught_up.is_caught_up(key, joining) {
        return false;
    }
    let (Some(tail), Some(held)) = (
        caught_up.leader_offset(key),
        caught_up.reported_offset(key, joining),
    ) else {
        return false;
    };
    let old: Vec<&str> = existing
        .replicas
        .iter()
        .map(String::as_str)
        .filter(|replica| *replica != joining)
        .collect();
    let mut positions: Vec<u64> = std::iter::once(tail)
        .chain(
            old.iter()
                .map(|replica| caught_up.reported_offset(key, replica).unwrap_or(tail)),
        )
        .collect();
    positions.sort_unstable_by(|a, b| b.cmp(a));
    // `majority_of` in felix-replication: the leader and half the followers.
    let needed = old.len().div_ceil(2) + 1;
    held >= positions[needed - 1]
}

/// The replica set once `target` leads: nodes that already hold a copy first
/// (the old leader if it is staying, then live followers), topped up by
/// score, and cut to the replication factor. The cut keeps copies in zones the
/// set would otherwise lose, so a move never narrows a shard's spread by
/// dropping the wrong copy.
fn replicas_after_cut_over<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    target: &str,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
) -> Vec<String> {
    let wanted = replication_factor.saturating_sub(1) as usize;
    let staying: Vec<&str> = existing
        .nodes()
        .map(String::as_str)
        .filter(|node| *node != target && is_live(node))
        .collect();
    let mut replicas: Vec<String> = keep_spread(eligible, &[target], &staying, wanted)
        .into_iter()
        .map(str::to_string)
        .collect();
    if replicas.len() < wanted {
        let holding: Vec<&str> = std::iter::once(target)
            .chain(replicas.iter().map(String::as_str))
            .collect();
        let chosen = choose_replicas(
            key,
            eligible,
            load,
            &holding,
            (wanted - replicas.len()) as u32,
        );
        replicas.extend(chosen);
    }
    replicas
}

/// How many zones the shard's copies would span if `target` led it: the copies
/// [`replicas_after_cut_over`] would keep, plus the zones a top-up could still
/// reach.
pub(super) fn spread_if_led_by(
    existing: &ShardAssignment,
    target: &str,
    replication_factor: u32,
    eligible: &[&Node],
) -> usize {
    let wanted = replication_factor.saturating_sub(1) as usize;
    let staying: Vec<&str> = existing
        .nodes()
        .map(String::as_str)
        .filter(|node| *node != target && eligible.iter().any(|e| e.node_id == *node))
        .collect();
    let kept = keep_spread(eligible, &[target], &staying, wanted);
    let used: BTreeSet<Domain<'_>> = std::iter::once(target)
        .chain(kept.iter().copied())
        .map(|id| domain(eligible, id))
        .collect();
    let free = eligible
        .iter()
        .filter(|node| node.node_id != target && !kept.contains(&node.node_id.as_str()))
        .map(|node| domain_of(node))
        .filter(|zone| !used.contains(zone))
        .collect::<BTreeSet<_>>()
        .len();
    used.len() + free.min(wanted - kept.len())
}

/// A caught-up live follower under its leadership share, furthest ahead first,
/// and under its share of the stream if one is.
fn promote_under_share<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    is_halted: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    leaders: &Leadership<'_>,
    shares: Shares,
) -> Option<&'a str> {
    let under_share: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| !is_halted(&node.node_id))
        .filter(|node| leaders.total(&node.node_id) < shares.cluster)
        .collect();
    let under_stream: Vec<&'a Node> = under_share
        .iter()
        .copied()
        .filter(|node| leaders.of(key, &node.node_id) < shares.ceil)
        .collect();
    promote(key, existing, &under_stream, caught_up)
        .or_else(|| promote(key, existing, &under_share, caught_up))
}

/// Why a move is wanted, which decides where it may go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Goal {
    /// The leader is leaving: anywhere live, balanced if possible.
    Drain,
    /// The leader's node leads two or more of the stream's shards more than
    /// some other node does: to a node that leads at least two fewer.
    Stream,
    /// The leader's node leads two or more shards more than some other node,
    /// cluster-wide: to a node that leads at least two fewer, and fewer of
    /// the stream, so the stream's spread does not widen.
    Cluster,
}

/// The live node to move a shard to for `goal`, by score, preferring nodes
/// under their leadership shares. A balancing goal answers `None` rather than
/// spill over.
///
/// Zones come before balance: a drain goes where the shard keeps the most
/// zones, and a rebalance, which is optional, only where it loses none.
///
/// `max_shards` caps roles, leaders and followers alike, so a node already
/// holding a copy of this shard gains no role by leading it.
#[allow(clippy::too_many_arguments)]
fn choose_destination<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_halted: &dyn Fn(&str) -> bool,
    leaders: &Leadership<'_>,
    load: &HashMap<&str, u32>,
    shares: Shares,
    goal: Goal,
) -> Option<&'a str> {
    let balanced_only = goal != Goal::Drain;
    let spread_of =
        |node: &Node| spread_if_led_by(existing, &node.node_id, replication_factor, eligible);
    let mut candidates: Vec<&'a Node> = eligible
        .iter()
        .copied()
        .filter(|node| node.node_id != existing.leader)
        .filter(|node| !is_halted(&node.node_id))
        .filter(|node| match node.spec.capacity.max_shards {
            Some(max) => {
                existing.replicas.contains(&node.node_id)
                    || load.get(node.node_id.as_str()).copied().unwrap_or(0) < max
            }
            None => true,
        })
        .collect();
    let floor = if balanced_only {
        spread_if_led_by(existing, &existing.leader, replication_factor, eligible)
    } else {
        candidates
            .iter()
            .map(|node| spread_of(node))
            .max()
            .unwrap_or(0)
    };
    candidates.retain(|node| spread_of(node) >= floor);
    let leads = leaders.of(key, &existing.leader);
    let under_cluster = |node: &&Node| leaders.total(&node.node_id) < shares.cluster;
    let tier = |keep: &dyn Fn(&&'a Node) -> bool| {
        candidates
            .iter()
            .copied()
            .filter(|node| keep(node))
            .collect::<Vec<_>>()
    };
    let pick = |from: &[&'a Node]| {
        from.iter()
            .copied()
            .max_by(|a, b| {
                spread_of(a)
                    .cmp(&spread_of(b))
                    .then_with(|| score(key, &a.node_id).cmp(&score(key, &b.node_id)))
                    .then_with(|| a.node_id.cmp(&b.node_id))
            })
            .map(|node| node.node_id.as_str())
    };
    match goal {
        Goal::Stream => {
            // To the node leading fewest of the stream, or a node between
            // gets the shard now and gives it on later.
            let fewest = candidates
                .iter()
                .map(|node| leaders.of(key, &node.node_id))
                .min()
                .filter(|fewest| fewest + 2 <= leads)?;
            let fewest = |node: &&Node| leaders.of(key, &node.node_id) == fewest;
            pick(&tier(&|node| fewest(node) && under_cluster(node)))
                .or_else(|| pick(&tier(&fewest)))
        }
        Goal::Cluster => {
            let total = leaders.total(&existing.leader);
            let eligible = tier(&|node| {
                leaders.total(&node.node_id) + 2 <= total && leaders.of(key, &node.node_id) < leads
            });
            let fewest = eligible
                .iter()
                .map(|node| leaders.total(&node.node_id))
                .min()?;
            pick(&tier(&|node| {
                leaders.total(&node.node_id) == fewest && leaders.of(key, &node.node_id) < leads
            }))
        }
        Goal::Drain => {
            let under_stream = |node: &&Node| leaders.of(key, &node.node_id) < shares.ceil;
            pick(&tier(&|node| under_cluster(node) && under_stream(node)))
                .or_else(|| pick(&tier(&under_cluster)))
                .or_else(|| pick(&tier(&under_stream)))
                .or_else(|| pick(&candidates))
        }
    }
}

/// A live follower whose zone another copy of the shard is already in, if
/// any: the one to give up for a copy in a zone the shard lacks. `joining` is
/// left out on both sides, being the copy that would replace it.
fn crowded<'a>(
    existing: &'a ShardAssignment,
    eligible: &[&'a Node],
    joining: Option<&str>,
) -> Option<&'a String> {
    existing
        .replicas
        .iter()
        .filter(|replica| Some(replica.as_str()) != joining)
        .filter(|replica| eligible.iter().any(|node| node.node_id == **replica))
        .filter(|replica| {
            let mine = domain(eligible, replica);
            existing
                .nodes()
                .filter(|other| other != replica && Some(other.as_str()) != joining)
                .any(|other| domain(eligible, other) == mine)
        })
        // The worst-scoring one, so the copy that goes is the one placement
        // would least have chosen.
        .min_by(|a, b| {
            score(&existing.key, a)
                .cmp(&score(&existing.key, b))
                .then_with(|| a.cmp(b))
        })
}

/// `inner`, believed only where its report is from `generation`.
///
/// A move is decided on the assignment's own generation. The report the store
/// holds may be the previous leader's, still fresh and still listing the
/// replicas it had level -- and a fence made on that would stop a leader for
/// a destination that may hold nothing of what it has written since.
pub(super) struct AtGeneration<'a> {
    inner: &'a dyn CaughtUp,
    generation: u64,
}

impl<'a> AtGeneration<'a> {
    pub(super) fn new(inner: &'a dyn CaughtUp, generation: u64) -> Self {
        Self { inner, generation }
    }
}

impl AtGeneration<'_> {
    fn current(&self, key: &ShardKey) -> bool {
        self.inner.reported_generation(key) == Some(self.generation)
    }
}

impl CaughtUp for AtGeneration<'_> {
    fn is_caught_up(&self, key: &ShardKey, node_id: &str) -> bool {
        self.current(key) && self.inner.is_caught_up(key, node_id)
    }

    fn reported_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.reported_offset(key, node_id))
            .flatten()
    }

    fn reported_counter_offset(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.reported_counter_offset(key, node_id))
            .flatten()
    }

    fn is_drained(&self, key: &ShardKey, generation: u64) -> bool {
        self.inner.is_drained(key, generation)
    }

    fn lag_records(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.lag_records(key, node_id))
            .flatten()
    }

    fn leader_offset(&self, key: &ShardKey) -> Option<u64> {
        self.current(key)
            .then(|| self.inner.leader_offset(key))
            .flatten()
    }

    fn reported_generation(&self, key: &ShardKey) -> Option<u64> {
        self.inner.reported_generation(key)
    }

    fn as_of_millis(&self) -> Option<u64> {
        self.inner.as_of_millis()
    }

    fn progressed_at_millis(&self, key: &ShardKey, node_id: &str) -> Option<u64> {
        self.inner.progressed_at_millis(key, node_id)
    }

    fn fenced_at_millis(&self, key: &ShardKey, generation: u64) -> Option<u64> {
        self.inner.fenced_at_millis(key, generation)
    }

    fn fence_give_ups(&self, key: &ShardKey) -> Option<(u32, u64)> {
        self.inner.fence_give_ups(key)
    }

    // Not held to the generation: a copy that halted under the previous one
    // is no better a destination for this one.
    fn halted(&self, key: &ShardKey, node_id: &str) -> Option<&HaltedCopy> {
        self.inner.halted(key, node_id)
    }
}
