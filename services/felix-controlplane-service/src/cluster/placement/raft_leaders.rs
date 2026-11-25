//! Placement for a shard a Raft group runs.
//!
//! The group elects its own leader and keeps its own members, so none of the
//! hybrid's steps apply: there is no fence to wait on, no drained report, and
//! no failover to decide. Placement only names the member it wants leading,
//! and the group's leader hands its leadership there. Moving a leader is one
//! write that changes `leader` to another member of the set. The set changes
//! when a member must leave (an internal, draining or lost broker) or an
//! operator or a balance pass moves a shard outside it: one member is swapped
//! for a live node and the group's leader runs the membership change.
//! `docs/control-plane.md`, "Raft shards".
use std::collections::BTreeSet;

use felix_common::random::HashMap;

use super::leadership::{Leadership, Owner};
use super::moves::Moves;
use super::rendezvous::score;
use super::zones::{domain, unspread};
use super::{Blocked, Decision, Leaving, MoveStep};
use crate::model::{Node, ShardAssignment, ShardKind, ShardState};

/// Most Raft shards one pass names a new leader for. Once brokers follow
/// placement each write asks for a leadership transfer, which closes the
/// shard for an election, so one pass does not ask for thousands.
pub(super) const RAFT_LEADERS_PER_PASS: usize = 64;

/// Most Raft shards one pass changes the set of. Each is a membership change
/// the leader runs, with a learner catching up, so one pass starts a few.
pub(super) const RAFT_REPLACEMENTS_PER_PASS: usize = 16;

/// Whether placement treats `existing` as a shard a Raft group runs: a
/// durable stream's shard or a cache's shard on a cluster that runs shards on
/// Raft. A cache counts as durable: a Raft broker always has a disk, so its
/// cache is a log and every cache shard runs as a group. A shard that
/// switched from the hybrid is left out, since the store takes no placement
/// write for it.
pub(super) fn on_raft(existing: &ShardAssignment, raft_streams: bool, durable: bool) -> bool {
    raft_streams
        && durable
        && matches!(existing.key.kind, ShardKind::Stream | ShardKind::Cache)
        && !existing.switched()
}

/// What to do about a Raft shard that has an assignment.
///
/// - A hybrid move left on it is dropped: the group sends none of the
///   reports it waits for.
/// - A leader that is down, on a draining broker, or outside the stream's
///   region is moved to a live member: the one the group reported electing
///   ([`elected`]) when there is one, else the member leading fewest. A
///   down one is renamed even while placement is paused, as the hybrid
///   still replaces a failed leader: brokers with no member forward to the
///   leader placement names.
/// - While a member of the set is down, a live leader is renamed to the
///   member the group reported electing. The group's leader does not hand
///   over while a voter is down, so naming any other member would leave
///   placement and the group disagreeing until the broker is back (#3041).
/// - A down leader with no live member stays named, with no alert: the
///   group cannot elect either until a member is back.
/// - A follower that must leave (`leaving`: an internal, draining or lost
///   broker) is replaced by a live node outside the set, one at a time,
///   through the group's membership target. With no such node it stays and
///   the shard waits, shown to the operator.
/// - A live leader leading two or more shards over a live member moves to
///   it, the stream's spread first and then the cluster's, while nothing is
///   recovering. With no such member, a live broker outside the set takes a
///   follower's seat and the lead, or an RF 1 shard starts its move there
///   ([`balanced_outside`]).
#[allow(clippy::too_many_arguments)]
pub(super) fn raft_step<'a>(
    existing: &'a ShardAssignment,
    replication_factor: u32,
    is_live: &dyn Fn(&str) -> bool,
    is_serving: &dyn Fn(&str) -> bool,
    counted: &dyn Fn(&str) -> bool,
    leaders: &mut Leadership<'a>,
    moves: &mut Moves,
    leaving: &dyn Fn(&str) -> Option<Leaving>,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
) -> Decision {
    if let Some(dropped) = drop_hybrid_move(existing, replication_factor) {
        return Decision::Move(MoveStep::RaftMoveDropped, dropped);
    }
    if let Some(decision) =
        finish_single_move(existing, replication_factor, is_live, is_serving, moves)
    {
        return decision;
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
    let elected = elected(existing, &members, is_serving, leaving);
    let degraded = existing.nodes().any(|node| !is_serving(node));
    // Before a swap: the swap waits on the down member anyway, and the
    // leader the group runs is the one that will make it.
    if degraded
        && is_live(leader)
        && let Some(to) = elected
    {
        return rename(existing, to, true, counted, leaders, moves);
    }
    // A leader that must leave is renamed first, below; it is swapped out
    // as a follower on a later pass.
    if is_live(leader)
        && leaving(leader).is_none()
        && let Some(decision) =
            replace_leaving(existing, is_serving, leaving, eligible, load, moves)
    {
        return decision;
    }
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
            None => {
                return balanced_outside(
                    existing,
                    replication_factor,
                    is_serving,
                    counted,
                    leaders,
                    moves,
                    eligible,
                    load,
                )
                .unwrap_or(Decision::Kept);
            }
            Some(_) if moves.recovering => return Decision::Waiting(Blocked::Recovering),
            Some(to) => to,
        }
    } else {
        match elected.or_else(|| fewest(&|node| leaders.total(node))) {
            Some(to) => to,
            None if down => return Decision::Kept,
            None => return Decision::Waiting(Blocked::NoDestination),
        }
    };
    rename(existing, to, down, counted, leaders, moves)
}

/// The member a Raft shard's group reported electing
/// ([`ShardAssignment::reported_leader`]), when placement may name it: a
/// live voter in the set, serving, not leaving, and not already named. A
/// report naming anyone else is ignored, and placement chooses as if there
/// were none.
///
/// A broker the leave rules rename a leader away from (draining, internal,
/// outside the stream's region) is never followed, even though the group
/// cannot hand over from it while the set is degraded: following it would
/// undo the leave rename on the next pass, and the drain must still name
/// where the shard goes. `is_live` already rules those brokers out; the
/// `leaving` check keeps it so if that changes.
fn elected<'a>(
    existing: &ShardAssignment,
    members: &[&'a str],
    is_serving: &dyn Fn(&str) -> bool,
    leaving: &dyn Fn(&str) -> Option<Leaving>,
) -> Option<&'a str> {
    let reported = existing.reported_leader()?;
    if reported == existing.leader {
        return None;
    }
    members
        .iter()
        .copied()
        .find(|node| *node == reported)
        .filter(|node| is_serving(node) && leaving(node).is_none() && existing.may_be_voting(node))
}

/// The write that names `to` leader, within this pass's budget.
///
/// `failover`: a pause does not hold the write. That is right for a down
/// leader, which brokers forward to, and for naming the member the group
/// elected: that write moves nothing and asks for no transfer, it records
/// what the group already did. A pause holds moves, not the record of one.
fn rename<'a>(
    existing: &'a ShardAssignment,
    to: &'a str,
    failover: bool,
    counted: &dyn Fn(&str) -> bool,
    leaders: &mut Leadership<'a>,
    moves: &mut Moves,
) -> Decision {
    let leader = existing.leader.as_str();
    let key = &existing.key;
    if moves.paused() && !failover {
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

/// Balance onto a live broker outside the set, when it leads two fewer than
/// the leader and no member does: one write gives it a follower's seat and
/// names it leader. A broker a drain swapped out of every set holds no
/// member anywhere, so renaming within sets alone would never give it a
/// shard again (#3113). The group's leader keeps its seat: it adds the new
/// copy, swaps the follower out, and hands over once the copy votes.
/// The swap takes a slot from the same per-pass budget as a replacement.
/// `None` when no such broker exists, or no follower can give its seat
/// without the set losing a zone.
///
/// An RF 1 shard has no follower, so it moves as an operator moves it
/// (#3118): this write grows the set to the leader and the broker
/// ([`grown`]), and [`finish_single_move`] shrinks it once the copy votes.
/// The tally counts the shard for the broker from this write on
/// ([`will_lead`]), so later passes do not start more moves than the gap.
#[allow(clippy::too_many_arguments)]
fn balanced_outside<'a>(
    existing: &'a ShardAssignment,
    replication_factor: u32,
    is_serving: &dyn Fn(&str) -> bool,
    counted: &dyn Fn(&str) -> bool,
    leaders: &mut Leadership<'a>,
    moves: &mut Moves,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
) -> Option<Decision> {
    let key = &existing.key;
    let leader = existing.leader.as_str();
    let roles = |id: &str| load.get(id).copied().unwrap_or(0);
    let outside: Vec<&'a str> = eligible
        .iter()
        .filter(|node| {
            node.spec
                .capacity
                .max_shards
                .is_none_or(|max| roles(&node.node_id) < max)
        })
        .map(|node| node.node_id.as_str())
        .filter(|id| !existing.nodes().any(|member| member == id))
        .collect();
    let single = replication_factor.max(1) == 1 && existing.replicas.is_empty();
    let current: Vec<&'a str> = existing.nodes().map(String::as_str).collect();
    let was_spread = !unspread(eligible, &current);
    // The follower that gives `to` its seat: one in `to`'s zone first, so
    // the set keeps its zones, then the busiest. None that would leave a
    // spread set short of a zone. A set of one gives the leader's own.
    let seat_for = |to: &'a str| {
        if single {
            return Some(&existing.leader);
        }
        existing
            .replicas
            .iter()
            .filter(|node| !existing.learners.contains(*node))
            .filter(|from| {
                let next: Vec<&'a str> = current
                    .iter()
                    .map(|node| if node == from { to } else { node })
                    .collect();
                !(was_spread && unspread(eligible, &next))
            })
            .min_by_key(|from| {
                (
                    domain(eligible, from) != domain(eligible, to),
                    u32::MAX - roles(from),
                    score(key, from),
                    from.as_str(),
                )
            })
    };
    let under = |count: &dyn Fn(&str) -> u32, keep: &dyn Fn(&str) -> bool| {
        let mut candidates: Vec<&'a str> = outside
            .iter()
            .copied()
            .filter(|to| count(to).saturating_add(2) <= count(leader) && keep(to))
            .collect();
        candidates.sort_by_key(|node| (count(node), u64::MAX - score(key, node), *node));
        candidates
            .into_iter()
            .find_map(|to| seat_for(to).map(|from| (to, from)))
    };
    // An RF 1 move copies the whole log, so it serves the stream's spread
    // only where the totals hold too. Otherwise a fresh cluster, placed
    // even on totals, trades one shard each way between two brokers.
    let for_stream = under(&|node| leaders.of(key, node), &|to| {
        !single || leaders.total(to) < leaders.total(leader)
    });
    moves.stream_wanted |= for_stream.is_some();
    // As for a member: the totals only where the stream's spread holds.
    let (to, from) = for_stream.or_else(|| {
        moves
            .cluster_moves
            .then(|| {
                under(&|node| leaders.total(node), &|to| {
                    leaders.of(key, to) < leaders.of(key, leader)
                })
            })
            .flatten()
    })?;
    if moves.recovering || existing.nodes().any(|node| !is_serving(node)) {
        return Some(Decision::Waiting(Blocked::Recovering));
    }
    if let Err(blocked) = seat_open(existing) {
        return Some(Decision::Waiting(blocked));
    }
    let next = if single {
        grown(existing, to)
    } else {
        ShardAssignment {
            leader: to.to_string(),
            replicas: existing
                .replicas
                .iter()
                .map(|node| if node == from { leader } else { node })
                .map(str::to_string)
                .collect(),
            ..existing.renamed()
        }
    };
    // The store refuses the write on the same check (#2905).
    if existing.holds_formed_set(&next.nodes().cloned().collect()) {
        return Some(Decision::Waiting(Blocked::Unapplied));
    }
    let step = MoveStep::RaftReplace {
        from: from.clone(),
        to: to.to_string(),
    };
    let decision = spend_replacement(moves, step, next);
    if matches!(decision, Decision::Move(..)) {
        *load.entry(to).or_default() += 1;
        if counted(leader) {
            leaders.remove(Owner::of(key), leader);
        }
        leaders.add(Owner::of(key), to);
    }
    Some(decision)
}

/// Ends the move of an RF 1 shard, which placement grew to its leader and
/// the destination ([`grown`]): once the group's leader reports the
/// destination's copy voting, the set shrinks to it and it leads. Until
/// then the shard waits, and nothing else changes its set or leader, so the
/// one broker the move is for stays the replica.
///
/// A destination that stops being live (down, draining, gone) is dropped
/// again ([`single_move_undone`]) while it has not voted or still serves,
/// so it can vote for its own removal; that is also how a drain of the old
/// broker gets past a move that cannot finish. One that votes and is down
/// cannot be: either set of one needs its vote, so the shard reports
/// [`Blocked::MoveStranded`] and waits for it or for a re-form.
fn finish_single_move(
    existing: &ShardAssignment,
    replication_factor: u32,
    is_live: &dyn Fn(&str) -> bool,
    is_serving: &dyn Fn(&str) -> bool,
    moves: &mut Moves,
) -> Option<Decision> {
    let to = single_move_destination(existing, replication_factor)?;
    if !is_live(to) {
        if existing.reports_voting(to) && !is_serving(to) {
            return Some(Decision::Waiting(Blocked::MoveStranded {
                node: to.to_string(),
            }));
        }
        let (node, undone) = single_move_undone(existing, replication_factor)?;
        let step = MoveStep::RaftMoveUndone {
            node: node.to_string(),
        };
        return Some(spend_replacement(moves, step, undone));
    }
    if let Err(blocked) = seat_open(existing) {
        return Some(Decision::Waiting(blocked));
    }
    // The store refuses the shrink on the same check: a set of the
    // destination alone, before its copy votes, would let that empty copy
    // form a second group (#2905).
    let next = BTreeSet::from([to.to_string()]);
    if existing.holds_formed_set(&next) {
        return Some(Decision::Waiting(Blocked::Unapplied));
    }
    Some(spend_replacement(
        moves,
        MoveStep::RaftReplace {
            from: existing.leader.clone(),
            to: to.to_string(),
        },
        ShardAssignment {
            leader: to.to_string(),
            replicas: Vec::new(),
            ..existing.renamed()
        },
    ))
}

/// The destination of an RF 1 move in progress: the one replica of a
/// shard whose factor is 1.
fn single_move_destination(existing: &ShardAssignment, replication_factor: u32) -> Option<&str> {
    match existing.replicas.as_slice() {
        [to] if replication_factor.max(1) == 1 => Some(to),
        _ => None,
    }
}

/// The broker the leader tally counts a Raft shard for: the destination of
/// an RF 1 move while it is live, since the move ends with it leading, and
/// the leader otherwise. A destination that is not live is dropped or
/// waited for, and the leader keeps the shard.
pub(super) fn will_lead<'a>(
    existing: &'a ShardAssignment,
    replication_factor: u32,
    is_live: &dyn Fn(&str) -> bool,
) -> &'a str {
    single_move_destination(existing, replication_factor)
        .filter(|to| is_live(to))
        .unwrap_or(existing.leader.as_str())
}

/// The write that drops an RF 1 move's destination, back to the leader
/// alone. The store takes it: the leader is a reported voter, so the set of
/// it alone keeps a majority. Once the destination votes, the group needs
/// its vote to remove it, so callers undo a voting one only while it
/// serves. Returns the destination and the write; `None` when the shard is
/// not mid-move.
pub(super) fn single_move_undone(
    existing: &ShardAssignment,
    replication_factor: u32,
) -> Option<(&str, ShardAssignment)> {
    let to = single_move_destination(existing, replication_factor)?;
    Some((
        to,
        ShardAssignment {
            replicas: Vec::new(),
            ..existing.renamed()
        },
    ))
}

/// Takes a set change's slot for this pass, or says why it waits.
fn spend_replacement(moves: &mut Moves, step: MoveStep, next: ShardAssignment) -> Decision {
    if moves.paused() {
        return Decision::Waiting(Blocked::Paused);
    }
    if moves.raft_replacements >= RAFT_REPLACEMENTS_PER_PASS {
        return Decision::Waiting(Blocked::MoveLimit);
    }
    moves.raft_replacements += 1;
    Decision::Move(step, next)
}

/// Swaps a follower that must leave for a live node outside the set.
///
/// The set keeps its size, and the new target is stored with the new member
/// in it before the group's leader acts, so the leader adds it as a learner
/// and only then drops the old voter. A change is written only on a target
/// stored for the current set, so there is one change at a time.
fn replace_leaving<'a>(
    existing: &'a ShardAssignment,
    is_serving: &dyn Fn(&str) -> bool,
    leaving: &dyn Fn(&str) -> Option<Leaving>,
    eligible: &[&'a Node],
    load: &mut HashMap<&'a str, u32>,
    moves: &mut Moves,
) -> Option<Decision> {
    // A member that is not serving first, lost ones before the rest: until
    // it is replaced every other swap waits on it below, so a draining or
    // internal member picked ahead of it would block forever.
    let (from, why) = existing
        .replicas
        .iter()
        .filter_map(|node| leaving(node).map(|why| (node, why)))
        .min_by_key(|(node, why)| (is_serving(node), *why != Leaving::Lost))?;
    let to = match replacement(existing, from, is_serving, eligible, load) {
        Ok(to) => to,
        Err(Blocked::NoReplacement { node, .. }) => {
            return Some(Decision::Waiting(Blocked::NoReplacement { node, why }));
        }
        Err(Blocked::MemberDown { node, .. }) => {
            return Some(Decision::Waiting(Blocked::MemberDown { node, why }));
        }
        Err(blocked) => return Some(Decision::Waiting(blocked)),
    };
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
        MoveStep::RaftReplace {
            from: from.clone(),
            to: to.to_string(),
        },
        ShardAssignment {
            replicas,
            ..existing.renamed()
        },
    ))
}

/// The live node outside the set that takes `from`'s seat, when the group
/// can make the change now.
///
/// Refused while another member is not serving, since the joint change needs
/// a majority of both sets, and while the target is not stored for the
/// current set or the group has not formed on it: one set change at a time.
/// The errors name no `why`; callers fill it in.
fn replacement<'a>(
    existing: &ShardAssignment,
    from: &str,
    is_serving: &dyn Fn(&str) -> bool,
    eligible: &[&'a Node],
    load: &HashMap<&'a str, u32>,
) -> Result<&'a str, Blocked> {
    let key = &existing.key;
    if let Some(down) = existing
        .nodes()
        .find(|node| node.as_str() != from && !is_serving(node))
    {
        return Err(Blocked::MemberDown {
            node: down.clone(),
            why: Leaving::Draining,
        });
    }
    // A replacement in a zone the rest of the set already holds comes last.
    let staying: Vec<&str> = existing
        .nodes()
        .map(String::as_str)
        .filter(|node| *node != from)
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
        })
        .ok_or_else(|| Blocked::NoReplacement {
            node: from.to_string(),
            why: Leaving::Draining,
        })?;
    seat_open(existing)?;
    Ok(to)
}

/// Whether `existing`'s set may change now: its target is stored for the
/// current set and the group has formed on it.
pub(super) fn seat_open(existing: &ShardAssignment) -> Result<(), Blocked> {
    let settled = existing.raft_target.as_ref().is_some_and(|target| {
        target
            .members
            .keys()
            .eq(existing.nodes().collect::<BTreeSet<_>>())
    });
    if !settled {
        return Err(Blocked::TargetUnsettled);
    }
    // The store refuses the write too; waiting here keeps it from being
    // proposed every pass.
    if existing.holds_unformed_set() {
        return Err(Blocked::Unformed);
    }
    Ok(())
}

/// `existing` with its leader's seat given to `to`, a broker outside the
/// set, which placement also names leader: an operator move outside the set.
/// The group's leader adds `to` as a learner, grows the voters to the target
/// plus itself, hands leadership to a voter both keep and that one shrinks
/// the voters, so no leader writes a membership without itself.
pub(super) fn seat_moved(existing: &ShardAssignment, to: &str) -> ShardAssignment {
    ShardAssignment {
        leader: to.to_string(),
        ..existing.renamed()
    }
}

/// An RF 1 shard's set `existing` grown by `to`: the first half of moving
/// it. Its leader stays, so the group adds `to` as a learner and then a
/// voter while the old copy still holds every record, and
/// [`finish_single_move`] drops the old copy once `to` votes. A direct
/// write of `{to}` would be held by the store: `to`'s empty copy would be
/// the whole group (#2905).
pub(super) fn grown(existing: &ShardAssignment, to: &str) -> ShardAssignment {
    ShardAssignment {
        replicas: vec![to.to_string()],
        ..existing.renamed()
    }
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
