//! Bringing a shard back to its replication factor.
//!
//! A follower whose broker has been down or gone for
//! `MovePolicy::restore_after_millis`, one its leader has reported halted for
//! that long, or a replica set a failover left short of the factor, gets a new
//! copy on a live broker. The copy joins the way a
//! draining follower's replacement does (`joining`), and is seated only once
//! it holds what a majority of the set held, so a fenced stream keeps the old
//! set's majority rule. Each step is one assignment write, so a pass on any
//! instance picks a restore up from the store where the last one left it.
use std::collections::{BTreeSet, HashMap};

use super::moves::{Moves, holds_what_the_set_held, undo_replacement};
use super::plan::{owner_of, replication_factors};
use super::rendezvous::score;
use super::zones::{Domain, domain, domain_of};
use super::{Blocked, CaughtUp, Decision, MoveStep};
use crate::model::{Cache, HaltedCopy, MoveReason, Node, ShardAssignment, ShardKey, Stream};

/// How many copies a shard has against its replication factor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replication {
    pub key: ShardKey,
    pub leader: String,
    /// The replication factor.
    pub desired: u32,
    /// Seated copies on a serving broker that are following, the leader's
    /// included.
    pub current: u32,
    /// Seated copies whose broker is not serving.
    pub unavailable: Vec<String>,
    /// Seated copies on a serving broker that the leader has stopped shipping
    /// to, by node. Not counted in `current`: they are in no quorum.
    pub halted: Vec<(String, HaltedCopy)>,
    /// The copy being added to bring the shard back to `desired`.
    pub restoring: Option<String>,
}

impl Replication {
    pub fn under_replicated(&self) -> bool {
        self.current < self.desired
    }
}

/// Every assigned shard's copies against its replication factor, in key
/// order. A copy still being added is not counted until it is seated, and one
/// its leader reports halted is not counted at all.
pub fn replication(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: &[ShardAssignment],
    caught_up: &dyn CaughtUp,
) -> Vec<Replication> {
    let factors = replication_factors(streams, caches);
    let serving = |id: &str| {
        nodes
            .iter()
            .any(|node| node.node_id == id && node.status.lifecycle.is_serving())
    };
    let mut shards: Vec<Replication> = existing
        .iter()
        .filter_map(|assignment| {
            let desired = *factors.get(&owner_of(&assignment.key))?;
            let (up, down): (Vec<&String>, Vec<&String>) =
                seated(assignment).partition(|node| serving(node));
            let halted: Vec<(String, HaltedCopy)> = up
                .iter()
                .filter_map(|node| {
                    caught_up
                        .halted_member(&assignment.key, node, assignment.generation)
                        .map(|halt| ((*node).clone(), halt.clone()))
                })
                .collect();
            Some(Replication {
                key: assignment.key.clone(),
                leader: assignment.leader.clone(),
                desired,
                current: (up.len() - halted.len()) as u32,
                unavailable: down.into_iter().cloned().collect(),
                halted,
                restoring: assignment
                    .joining
                    .clone()
                    .filter(|_| assignment.move_reason == Some(MoveReason::Restore)),
            })
        })
        .collect();
    shards.sort_by(|a, b| super::plan::order(&a.key).cmp(&super::plan::order(&b.key)));
    shards
}

/// Start restoring `existing` if it is missing a copy, or `None` when it is
/// not, or when no live broker can take one.
#[allow(clippy::too_many_arguments)]
pub(super) fn restore<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    replication_factor: u32,
    eligible: &[&'a Node],
    is_lost: &dyn Fn(&str) -> bool,
    is_halted: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Option<Decision> {
    let replacing = replacing(existing, replication_factor, is_lost);
    if replacing.is_none() && !short(existing, replication_factor) {
        return None;
    }
    // Only once the leader has reported at this generation, which it does
    // only after its promotion fence. A set written while it is still fencing
    // is the set it fences, and a set of two grown to three would let it open
    // on itself and an empty newcomer. The model's `Regenerate` has the same
    // guard.
    if caught_up.reported_generation(key) != Some(existing.generation) {
        return None;
    }
    let (widening, best) = newcomer(key, existing, eligible, is_halted, load, replacing);
    let to = widening.or(best)?;
    if let Err(blocked) = moves.begin(&existing.leader, &to.node_id) {
        return Some(Decision::Waiting(blocked));
    }
    *load.entry(to.node_id.as_str()).or_default() += 1;
    let mut replicas = existing.replicas.clone();
    replicas.push(to.node_id.clone());
    Some(Decision::Move(
        MoveStep::Restore {
            replacing: replacing.map(str::to_string),
            to: to.node_id.clone(),
        },
        ShardAssignment {
            replicas,
            generation: 0,
            joining: Some(to.node_id.clone()),
            move_started_at_millis: caught_up.as_of_millis(),
            move_reason: Some(MoveReason::Restore),
            ..existing.clone()
        },
    ))
}

/// A restore in progress: seat the new copy once it holds what the set held,
/// or undo it if it cannot finish or is no longer needed.
#[allow(clippy::too_many_arguments)]
pub(super) fn restore_step(
    existing: &ShardAssignment,
    joining: &str,
    replication_factor: u32,
    is_live: &dyn Fn(&str) -> bool,
    is_lost: &dyn Fn(&str) -> bool,
    caught_up: &dyn CaughtUp,
    moves: &Moves,
    fenced: bool,
) -> Decision {
    let undo = |step: MoveStep, started| {
        Decision::Move(
            step,
            ShardAssignment {
                move_started_at_millis: started,
                ..undo_replacement(existing, joining)
            },
        )
    };
    let lost = replacing(existing, replication_factor, is_lost);
    // A destination that went down is dropped and the next pass picks another.
    // A lost broker that came back holds its copy again, and the new one is
    // not needed.
    if !is_live(joining) || (lost.is_none() && !short(existing, replication_factor)) {
        return undo(
            MoveStep::Abandon {
                successor: joining.to_string(),
            },
            None,
        );
    }
    if moves.ready_to_fence(caught_up, &existing.key, joining)
        && (!fenced || holds_what_the_set_held(existing, joining, caught_up))
    {
        let mut replicas = existing.replicas.clone();
        if let Some(lost) = lost {
            replicas.retain(|replica| replica != lost);
        }
        return Decision::Move(
            MoveStep::Seat {
                from: lost.map(str::to_string),
                to: joining.to_string(),
            },
            ShardAssignment {
                replicas,
                generation: 0,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                ..existing.clone()
            },
        );
    }
    // One that has stopped moving would otherwise hold the slot every other
    // short shard needs until the move timeout. Its start stays, which puts
    // it behind them for the next slot.
    if moves.stalled(caught_up, &existing.key, joining) {
        return undo(
            MoveStep::Stalled {
                successor: joining.to_string(),
            },
            existing.move_started_at_millis,
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

/// Whether a restore would start, or go on, for `existing`: the order a slot
/// goes in (`start_order`) puts these ahead of rebalancing.
pub(super) fn wanted(
    existing: &ShardAssignment,
    replication_factor: u32,
    is_lost: &dyn Fn(&str) -> bool,
) -> bool {
    lost_follower(existing, is_lost).is_some() || short(existing, replication_factor)
}

/// The live broker to copy a shard into beside its current copies: the best
/// by score in a zone the shard has no copy in, and the best by score at all.
/// `departing` is the copy it stands in for, whose zone does not count. A node
/// whose copy of the shard `is_halted` is never one.
pub(super) fn newcomer<'a>(
    key: &ShardKey,
    existing: &ShardAssignment,
    eligible: &[&'a Node],
    is_halted: &dyn Fn(&str) -> bool,
    load: &HashMap<&'a str, u32>,
    departing: Option<&str>,
) -> (Option<&'a Node>, Option<&'a Node>) {
    let taken: Vec<&str> = existing.nodes().map(String::as_str).collect();
    let held: BTreeSet<Domain<'_>> = existing
        .nodes()
        .filter(|node| Some(node.as_str()) != departing)
        .map(|node| domain(eligible, node))
        .collect();
    let candidates = || {
        eligible
            .iter()
            .copied()
            .filter(|node| !taken.contains(&node.node_id.as_str()))
            .filter(|node| !is_halted(&node.node_id))
            .filter(|node| match node.spec.capacity.max_shards {
                Some(max) => load.get(node.node_id.as_str()).copied().unwrap_or(0) < max,
                None => true,
            })
    };
    let best = |nodes: &mut dyn Iterator<Item = &'a Node>| {
        nodes.max_by(|a, b| {
            score(key, &a.node_id)
                .cmp(&score(key, &b.node_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        })
    };
    (
        best(&mut candidates().filter(|node| !held.contains(&domain_of(node)))),
        best(&mut candidates()),
    )
}

/// The follower a restore replaces: the first, in set order, whose broker is
/// lost. None while the set is short, which grows first: a newcomer beside an
/// even set can make a majority with the leader alone, and seating it in a
/// lost follower's place would leave a record acknowledged on the leader and
/// that follower on half the set. Beside the odd set a factor of three gives,
/// every majority keeps a majority of the old set, as for a drain.
fn replacing<'e>(
    existing: &'e ShardAssignment,
    replication_factor: u32,
    is_lost: &dyn Fn(&str) -> bool,
) -> Option<&'e str> {
    if short(existing, replication_factor) {
        return None;
    }
    lost_follower(existing, is_lost)
}

/// The first follower, in set order, whose broker is lost. A copy still being
/// added is not one.
fn lost_follower<'e>(
    existing: &'e ShardAssignment,
    is_lost: &dyn Fn(&str) -> bool,
) -> Option<&'e str> {
    existing
        .replicas
        .iter()
        .map(String::as_str)
        .filter(|replica| Some(*replica) != existing.joining.as_deref())
        .find(|replica| is_lost(replica))
}

/// Whether the seated set is smaller than the replication factor, as a
/// failover that had too few live brokers to choose from leaves it.
fn short(existing: &ShardAssignment, replication_factor: u32) -> bool {
    seated(existing).count() < replication_factor.max(1) as usize
}

/// The leader and followers, without a copy still being added.
fn seated(existing: &ShardAssignment) -> impl Iterator<Item = &String> {
    existing
        .nodes()
        .filter(move |node| Some(node.as_str()) != existing.joining.as_deref())
}
