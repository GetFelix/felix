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
pub(crate) fn view(route: &Route) -> ReplicaSetView {
    ReplicaSetView {
        generation: route.generation,
        members: members(route, None)
            .map(|(node, storage)| SetMember {
                node_id: node.to_string(),
                storage_id: storage,
                learner: false,
            })
            .collect(),
    }
}

/// The route's set at its generation, as a log keeps it, without `leaving_out`:
/// a staged copy still copying is not part of the set it is joining.
pub(crate) fn kept(route: &Route, leaving_out: Option<&str>) -> ReplicaSet {
    ReplicaSet {
        current: Membership {
            generation: route.generation,
            members: members(route, leaving_out)
                .map(|(node, storage)| Member {
                    node: node.to_string(),
                    storage: storage.map(StorageId::from_bytes),
                    learner: false,
                })
                .collect(),
        },
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
    leaving_out: Option<&'a str>,
) -> impl Iterator<Item = (&'a str, Option<[u8; 16]>)> + 'a {
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
        .filter(move |node| Some(*node) != leaving_out)
        .map(move |node| (node, storage(node)))
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
