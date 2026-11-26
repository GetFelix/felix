//! Placement for an in-memory stream's shard.
//!
//! There is no log, so nothing is copied, caught up or fenced, and no replica
//! report is read. The shard has one owner and no followers. The owner keeps
//! it while it is live and may hold it. Once it is down, gone, draining,
//! internal or outside the stream's region, the shard is placed again in the
//! same pass, chosen as a new shard is. Every placement is a new assignment,
//! which the store writes at the next generation: that is how the old owner
//! and its subscribers learn they are stale. The new owner starts empty.
//! `docs/control-plane.md`, "In-memory shards".
use felix_common::random::HashMap;

use super::leadership::{Leadership, Owner, Shares};
use super::rendezvous::choose;
use super::{Decision, Unplaceable};
use crate::model::{Node, ShardAssignment, ShardKey, ShardState};

/// What to do about an in-memory shard, `existing` its assignment if it has
/// one.
///
/// `is_live` is the shard's own: live, not internal, and in a region the
/// stream may use. `counted` is the cluster-wide one the leader tally was
/// built with. `unplaced` says why when no node can take the shard.
#[allow(clippy::too_many_arguments)]
pub(super) fn in_memory_step<'a>(
    key: &ShardKey,
    owner: Owner<'a>,
    existing: Option<&'a ShardAssignment>,
    eligible: &[&'a Node],
    is_live: &dyn Fn(&str) -> bool,
    counted: &dyn Fn(&str) -> bool,
    load: &mut HashMap<&'a str, u32>,
    leaders: &mut Leadership<'a>,
    cap: u32,
    shares: Shares,
    unplaced: &dyn Fn() -> Unplaceable,
) -> Decision {
    if let Some(existing) = existing
        && is_live(&existing.leader)
    {
        if !hybrid_leftovers(existing) {
            return Decision::Kept;
        }
        // A hybrid move or set change left on the shard would wait for
        // reports that never come, and a `Draining` owner refuses writes
        // until one does. The owner is named again alone.
        forget(existing, load);
        *load.entry(existing.leader.as_str()).or_default() += 1;
        return Decision::Place(existing.leader.clone(), Vec::new());
    }
    if let Some(existing) = existing {
        forget(existing, load);
        if counted(&existing.leader) {
            leaders.remove(owner, &existing.leader);
        }
    }
    match choose(key, eligible, load, cap, leaders, shares) {
        Some(chosen) => {
            *load.entry(chosen).or_default() += 1;
            leaders.add(owner, chosen);
            Decision::Place(chosen.to_string(), Vec::new())
        }
        None => Decision::Unplaceable(unplaced()),
    }
}

/// Whether `existing` carries state only the hybrid's move steps end: a
/// staged successor, a joining copy, a pending set or a fence.
fn hybrid_leftovers(existing: &ShardAssignment) -> bool {
    existing.successor.is_some()
        || existing.joining.is_some()
        || existing.pending_set.is_some()
        || existing.state == ShardState::Draining
}

/// Take `existing`'s roles off `load`: the write replacing it names none of
/// them but the one it adds back.
fn forget(existing: &ShardAssignment, load: &mut HashMap<&str, u32>) {
    for node in existing.nodes() {
        if let Some(count) = load.get_mut(node.as_str()) {
            *count = count.saturating_sub(1);
        }
    }
}
