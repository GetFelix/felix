//! Placement for a shard a Raft group runs.
//!
//! The group elects its own leader and keeps its own members, so none of the
//! hybrid's steps apply: there is no fence to wait on, no drained report, and
//! no failover to decide. Placement only names the member it wants leading,
//! and the group's leader hands its leadership there. Moving a leader is one
//! write that changes `leader` to another member of the set; the set itself never
//! changes here. `docs/control-plane.md`, "Raft shards".
use super::leadership::{Leadership, Owner};
use super::moves::Moves;
use super::rendezvous::score;
use super::{Blocked, Decision, MoveStep};
use crate::model::{ShardAssignment, ShardKind, ShardState};

/// Most Raft shards one pass names a new leader for. Once brokers follow
/// placement each write asks for a leadership transfer, which closes the
/// shard for an election, so one pass does not ask for thousands.
pub(super) const RAFT_LEADERS_PER_PASS: usize = 64;

/// Whether placement treats `existing` as a shard a Raft group runs: a
/// durable stream's shard on a cluster that runs those on Raft. A shard that
/// switched from the hybrid is left out, since the store takes no placement
/// write for it.
pub(super) fn on_raft(existing: &ShardAssignment, raft_streams: bool, durable: bool) -> bool {
    raft_streams && durable && existing.key.kind == ShardKind::Stream && !existing.switched()
}

/// What to do about a Raft shard that has an assignment.
///
/// - A hybrid move left on it is dropped: the group sends none of the
///   reports it waits for.
/// - A leader that is down, on a draining broker, or outside the stream's
///   region is moved to a live member. A down one is renamed even while
///   placement is paused, as the hybrid still replaces a failed leader:
///   brokers with no member forward to the leader placement names.
/// - A down leader with no live member stays named, with no alert: the
///   group cannot elect either until a member is back.
/// - A live leader leading two or more shards over a live member moves to
///   it, the stream's spread first and then the cluster's, while nothing is
///   recovering.
#[allow(clippy::too_many_arguments)]
pub(super) fn raft_step<'a>(
    existing: &'a ShardAssignment,
    replication_factor: u32,
    is_live: &dyn Fn(&str) -> bool,
    is_serving: &dyn Fn(&str) -> bool,
    counted: &dyn Fn(&str) -> bool,
    leaders: &mut Leadership<'a>,
    moves: &mut Moves,
) -> Decision {
    if let Some(dropped) = drop_hybrid_move(existing, replication_factor) {
        return Decision::Move(MoveStep::RaftMoveDropped, dropped);
    }
    let leader = existing.leader.as_str();
    let down = !is_serving(leader);
    let key = &existing.key;
    // Only a member can lead: a node outside the set holds no copy.
    let members: Vec<&'a str> = existing
        .replicas
        .iter()
        .map(String::as_str)
        .filter(|node| is_live(node) && !existing.learners.contains(*node))
        .collect();
    let fewest = |count: &dyn Fn(&str) -> u32| {
        members
            .iter()
            .copied()
            .min_by_key(|node| (count(node), u64::MAX - score(key, node), *node))
    };
    let leader_live = is_live(leader);
    let to = if leader_live {
        // Two or more apart, so moves converge instead of trading leaders.
        let over = |count: &dyn Fn(&str) -> u32| {
            fewest(count).filter(|to| count(to).saturating_add(2) <= count(leader))
        };
        let for_stream = over(&|node| leaders.of(key, node));
        moves.stream_wanted |= for_stream.is_some();
        // For the totals only where the stream's own spread does not get
        // worse, or the next pass's stream rule moves it straight back.
        let to = for_stream.or_else(|| {
            moves
                .cluster_moves
                .then(|| over(&|node| leaders.total(node)))
                .flatten()
                .filter(|to| leaders.of(key, to) < leaders.of(key, leader))
        });
        match to {
            None => return Decision::Kept,
            Some(_) if moves.recovering => return Decision::Waiting(Blocked::Recovering),
            Some(to) => to,
        }
    } else {
        match fewest(&|node| leaders.total(node)) {
            Some(to) => to,
            None if down => return Decision::Kept,
            None => return Decision::Waiting(Blocked::NoDestination),
        }
    };
    if moves.paused() && !down {
        return Decision::Waiting(Blocked::Paused);
    }
    if moves.raft_leaders >= RAFT_LEADERS_PER_PASS {
        return Decision::Waiting(Blocked::MoveLimit);
    }
    moves.raft_leaders += 1;
    // The tally counts a leader on any live broker, region aside.
    if counted(leader) {
        leaders.remove(Owner::of(key), leader);
    }
    leaders.add(Owner::of(key), to);
    Decision::Move(
        MoveStep::RaftLeader {
            from: leader.to_string(),
            to: to.to_string(),
        },
        led_by(existing, to),
    )
}

/// `existing` with `to`, one of its replicas, as leader. The set and every
/// other field stay.
pub(super) fn led_by(existing: &ShardAssignment, to: &str) -> ShardAssignment {
    let replicas = existing
        .replicas
        .iter()
        .map(|node| {
            if node == to {
                existing.leader.clone()
            } else {
                node.clone()
            }
        })
        .collect();
    ShardAssignment {
        leader: to.to_string(),
        replicas,
        ..existing.renamed()
    }
}

/// `existing` without the hybrid move or set change it has in progress, if
/// any. A staged destination or a joining copy that took the set over its
/// factor was added by the move, not formed into the group, so it leaves.
fn drop_hybrid_move(
    existing: &ShardAssignment,
    replication_factor: u32,
) -> Option<ShardAssignment> {
    let moving = existing.successor.is_some()
        || existing.joining.is_some()
        || existing.pending_set.is_some()
        || existing.state == ShardState::Draining;
    if !moving {
        return None;
    }
    let mut replicas = existing.replicas.clone();
    if existing.nodes().count() > replication_factor.max(1) as usize {
        replicas.retain(|node| {
            existing.successor.as_ref() != Some(node) && existing.joining.as_ref() != Some(node)
        });
    }
    Some(ShardAssignment {
        replicas,
        successor: None,
        joining: None,
        pending_set: None,
        move_reason: None,
        state: if existing.state == ShardState::Draining {
            ShardState::Assigning
        } else {
            existing.state
        },
        ..existing.renamed()
    })
}
