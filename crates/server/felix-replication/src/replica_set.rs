//! The replica set an assignment names, in the forms the election checks
//! need: on the wire for a fence, and on disk for a log.
//!
//! The members are the route's leader and replicas. Storage ids come from
//! `Route::fence_storage_ids` when the assignment carries them.

use felix_router::Route;
use felix_storage::StorageId;
use felix_storage::disk_log::{Member, Membership, ReplicaSet};
use felix_wire::internal::{ReplicaSetView, SetMember};

/// The route's set at its generation, as a fence carries it.
///
/// Every set built from a route lists the same members: an equal-generation
/// set never replaces the stored one, so the leader's and a replica's must
/// agree, which is why `learner` is the one thing a caller adds: the staged
/// destination the leader leaves out of its quorum (`staged_learner`), and
/// `None` wherever that is not known. A successor that already replicated the
/// stream keeps voting, so marking it a learner would make a promotion fence
/// refuse its answer.
pub(crate) fn view(route: &Route, learner: Option<&str>) -> ReplicaSetView {
    ReplicaSetView {
        generation: route.generation,
        members: members(route, learner)
            .map(|(node, storage, learner)| SetMember {
                node_id: node.to_string(),
                storage_id: storage,
                learner,
            })
            .collect(),
    }
}

/// The route's set at its generation, as a log keeps it.
pub(crate) fn kept(route: &Route, learner: Option<&str>) -> ReplicaSet {
    ReplicaSet {
        current: membership(&view(route, learner)),
        leaving: None,
    }
}

/// A set as a log keeps it, from a fence's.
pub(crate) fn membership(view: &ReplicaSetView) -> Membership {
    Membership {
        generation: view.generation,
        members: view
            .members
            .iter()
            .map(|member| Member {
                node: member.node_id.clone(),
                storage: member.storage_id.map(StorageId::from_bytes),
                learner: member.learner,
            })
            .collect(),
    }
}

fn members<'a>(
    route: &'a Route,
    staged: Option<&'a str>,
) -> impl Iterator<Item = (&'a str, Option<[u8; 16]>, bool)> + 'a {
    let storage = |node: &str| {
        route
            .fence_storage_ids
            .as_deref()
            .and_then(|ids| ids.get(node))
            .and_then(|hex| from_hex(hex))
    };
    std::iter::once(&route.leader)
        .chain(&route.replicas)
        .map(|node| node.node_id.as_str())
        .map(move |node| {
            let learner = staged == Some(node) && node != route.leader.node_id;
            (node, storage(node), learner)
        })
}

/// A storage id as a route names it, 32 hex digits, or `None` for an empty
/// one: placement names a member it could not vouch for that way.
fn from_hex(hex: &str) -> Option<[u8; 16]> {
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests;
