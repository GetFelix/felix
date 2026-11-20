//! Placement for a shard a Raft group runs.
//!
//! The group elects its own leader and keeps its own members, so none of the
//! hybrid's steps apply: there is no fence to wait on, no drained report, and
//! no failover to decide. Placement only names the member it wants leading,
//! and the group's leader hands its leadership there. Moving a leader is one
//! write that changes `leader` to another member of the set. The set changes
//! in one case: a follower on an internal node is swapped for a live node. `docs/control-plane.md`, "Raft shards".
use std::collections::BTreeSet;

use felix_common::random::HashMap;

use super::leadership::{Leadership, Owner};
use super::moves::Moves;
use super::rendezvous::score;
use super::zones::domain;
use super::{Blocked, Decision, MoveStep};
use crate::model::{Node, ShardAssignment, ShardKind, ShardState};

/// Most Raft shards one pass names a new leader for. Once brokers follow
/// placement each write asks for a leadership transfer, which closes the
/// shard for an election, so one pass does not ask for thousands.
pub(super) const RAFT_LEADERS_PER_PASS: usize = 64;

/// Most Raft shards one pass changes the set of. Each is a membership change
/// the leader runs, with a learner catching up, so one pass starts a few.
pub(super) const RAFT_REPLACEMENTS_PER_PASS: usize = 16;

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
/// - A follower on an internal node is replaced by a live node outside the
///   set, one at a time, through the group's membership target. With no such
///   node it stays and the shard waits, shown to the operator.
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
    is_internal: &dyn Fn(&str) -> bool,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
) -> Decision {
    if let Some(dropped) = drop_hybrid_move(existing, replication_factor) {
        return Decision::Move(MoveStep::RaftMoveDropped, dropped);
    }
    let leader = existing.leader.as_str();
    // A leader on an internal node moves first, below; its followers are
    // swapped on a later pass.
    if is_live(leader)
        && !is_internal(leader)
        && let Some(decision) =
            replace_internal(existing, is_live, is_internal, eligible, load, moves)
    {
        return decision;
    }
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

/// Swaps a follower on an internal node for a live node outside the set.
///
/// The set keeps its size, and the new target is stored with the new member
/// in it before the group's leader acts, so the leader adds it as a learner
/// and only then drops the old voter. A change is written only on a target
/// stored for the current set, so there is one change at a time.
fn replace_internal<'a>(
    existing: &'a ShardAssignment,
    is_live: &dyn Fn(&str) -> bool,
    is_internal: &dyn Fn(&str) -> bool,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Option<Decision> {
    let from = existing
        .replicas
        .iter()
        .find(|node| is_internal(node.as_str()))?;
    let key = &existing.key;
    // The swap replaces the stored target. With another member down the group
    // could not finish the change, so the internal voter would stay unseen.
    if let Some(down) = existing
        .nodes()
        .find(|node| node.as_str() != from && !is_internal(node) && !is_live(node))
    {
        return Some(Decision::Waiting(Blocked::MemberDown {
            node: down.clone(),
        }));
    }
    // A replacement in a zone the rest of the set already holds comes last.
    let staying: Vec<&str> = existing
        .nodes()
        .map(String::as_str)
        .filter(|node| *node != from.as_str())
        .collect();
    let to = eligible
        .iter()
        .map(|node| node.node_id.as_str())
        .filter(|id| !existing.nodes().any(|member| member == id))
        .min_by_key(|id| {
            (
                staying
                    .iter()
                    .any(|node| domain(eligible, node) == domain(eligible, id)),
                load.get(id).copied().unwrap_or(0),
                u64::MAX - score(key, id),
                *id,
            )
        });
    let Some(to) = to else {
        return Some(Decision::Waiting(Blocked::NoReplacement {
            node: from.clone(),
        }));
    };
    let settled = existing.raft_target.as_ref().is_some_and(|target| {
        target
            .members
            .keys()
            .eq(existing.nodes().collect::<BTreeSet<_>>())
    });
    if !settled {
        return Some(Decision::Waiting(Blocked::TargetUnsettled));
    }
    if moves.paused() {
        return Some(Decision::Waiting(Blocked::Paused));
    }
    if moves.raft_replacements >= RAFT_REPLACEMENTS_PER_PASS {
        return Some(Decision::Waiting(Blocked::MoveLimit));
    }
    moves.raft_replacements += 1;
    *load.entry(to).or_default() += 1;
    let replicas = existing
        .replicas
        .iter()
        .map(|node| {
            if node == from {
                to.to_string()
            } else {
                node.clone()
            }
        })
        .collect();
    Some(Decision::Move(
        MoveStep::RaftReplaceInternal {
            from: from.clone(),
            to: to.to_string(),
        },
        ShardAssignment {
            replicas,
            ..existing.renamed()
        },
    ))
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
