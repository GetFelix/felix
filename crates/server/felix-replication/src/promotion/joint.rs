//! Joint fences: a leader whose set changed fences the set it leaves too.
//!
//! Rule 7 of "Electing without the control plane" in
//! `docs/replication-design.md`; `FenceMembers` and `OpenForWrites` under
//! `JointFence` in `docs/formal/FelixShard.tla`. Without it, a cut-over that
//! swaps two members leaves a majority of the old set that never heard of the
//! new leader, and a replica there can stand and acknowledge on it
//! (`FelixShardElectSwapNoJoint.cfg`).
//!
//! The set being left is the one the leader's own log last kept: the set its
//! previous leader shipped with, or the one it led itself. The control plane
//! is not asked, so this works from what the leader saw.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use felix_router::{NodeRef, Route};
use felix_storage::disk_log::Membership;
use felix_wire::internal::{PeerCapabilities, ReplicaSetView, SetMember};

use crate::peer::PeerRequester;

/// What a leader needs to fence jointly, given once the fleet has finalized
/// `joint_fence`: where to reach a member of the set it leaves, which its
/// route may no longer name.
pub struct Joint<'a> {
    addr_of: &'a (dyn Fn(&str) -> Option<SocketAddr> + Sync),
}

impl<'a> Joint<'a> {
    pub fn new(addr_of: &'a (dyn Fn(&str) -> Option<SocketAddr> + Sync)) -> Self {
        Self { addr_of }
    }
}

/// One joint fence: whom it asks, the set it leaves, and the voters of each
/// set it needs a majority of.
#[derive(Debug)]
pub(super) struct JointSets {
    /// Every member of either set but this broker that offered `ELECTION`.
    pub(super) asked: Vec<NodeRef>,
    /// The set being left, as the fence carries it.
    pub(super) leaving: ReplicaSetView,
    old_voters: BTreeSet<String>,
    new_voters: BTreeSet<String>,
    votes_in_old: bool,
    votes_in_new: bool,
}

impl JointSets {
    /// Whether `answered`, with this broker, is a majority of the voters of
    /// both sets. A learner is never a voter, so it never counts.
    pub(super) fn has_majorities<'n>(&self, answered: impl Iterator<Item = &'n str>) -> bool {
        let (mut old, mut new) = (
            usize::from(self.votes_in_old),
            usize::from(self.votes_in_new),
        );
        for node in answered {
            old += usize::from(self.old_voters.contains(node));
            new += usize::from(self.new_voters.contains(node));
        }
        old * 2 > self.old_voters.len() && new * 2 > self.new_voters.len()
    }

    /// How far the answers got, for a fence that stays pending.
    pub(super) fn shortfall<'n>(&self, answered: impl Iterator<Item = &'n str> + Clone) -> String {
        let count = |voters: &BTreeSet<String>, me: bool| {
            answered
                .clone()
                .filter(|node| voters.contains(*node))
                .count()
                + usize::from(me)
        };
        format!(
            "{} of {} voters of the set being left and {} of {} of the new set took the fence, \
             a majority of each needed",
            count(&self.old_voters, self.votes_in_old),
            self.old_voters.len(),
            count(&self.new_voters, self.votes_in_new),
            self.new_voters.len(),
        )
    }
}

/// The joint fence for `route`, when one applies: this broker's log keeps a
/// set older than the route's (or the one it is leaving, if the route's set
/// is already kept) with different voters, and no member of either set is
/// known to lack `ELECTION`. `None` is a fence on the route's set alone,
/// exactly as without `joint_fence`.
///
/// A member whose capabilities cannot be learned, or whose address is
/// unknown, is not asked: it is a member that has not answered, which keeps
/// the fence from opening on less than both majorities.
pub(super) async fn sets<R: PeerRequester>(
    requester: &R,
    log: &felix_broker::StreamLog,
    local_node_id: &str,
    route: &Route,
    set: &ReplicaSetView,
    joint: &Joint<'_>,
) -> Option<JointSets> {
    let stored = log.replica_set()?;
    // REVERT EVIDENCE: no joint fence.
    if stored.current.generation > 0 {
        return None;
    }
    let leaving = if stored.current.generation < route.generation {
        stored.current
    } else if stored.current.generation == route.generation {
        // A retry after this leader kept the new set beside the old one.
        stored.leaving?
    } else {
        return None;
    };
    let voters = |members: &mut dyn Iterator<Item = (&str, bool)>| {
        members
            .filter(|(_, learner)| !learner)
            .map(|(node, _)| node.to_string())
            .collect::<BTreeSet<_>>()
    };
    let old_voters = voters(
        &mut leaving
            .members
            .iter()
            .map(|member| (member.node.as_str(), member.learner)),
    );
    let new_voters = voters(
        &mut set
            .members
            .iter()
            .map(|member| (member.node_id.as_str(), member.learner)),
    );
    if old_voters == new_voters {
        return None;
    }

    let route_nodes = std::iter::once(&route.leader).chain(&route.replicas);
    let mut members: Vec<(String, Option<SocketAddr>)> = route_nodes
        .map(|node| (node.node_id.clone(), Some(node.advertise_addr)))
        .collect();
    for member in &leaving.members {
        if !members.iter().any(|(node, _)| *node == member.node) {
            members.push((member.node.clone(), (joint.addr_of)(&member.node)));
        }
    }
    let mut asked = Vec::new();
    for (node_id, addr) in members {
        if node_id == local_node_id {
            continue;
        }
        let offered = match (requester.recorded_capabilities(&node_id), addr) {
            (Some(offered), _) => Some(offered),
            (None, Some(addr)) => requester.capabilities(&node_id, addr).await.ok(),
            (None, None) => None,
        };
        match (offered, addr) {
            // A member known not to check sets: the fleet is not one that
            // fences jointly, and the shard is fenced as it always was.
            (Some(offered), _) if !offered.contains(PeerCapabilities::ELECTION) => return None,
            (Some(_), Some(advertise_addr)) => asked.push(NodeRef {
                node_id,
                advertise_addr,
                region: String::new(),
                live: true,
            }),
            _ => {}
        }
    }
    Some(JointSets {
        asked,
        votes_in_old: old_voters.contains(local_node_id),
        votes_in_new: new_voters.contains(local_node_id),
        leaving: view(&leaving),
        old_voters,
        new_voters,
    })
}

fn view(set: &Membership) -> ReplicaSetView {
    ReplicaSetView {
        generation: set.generation,
        members: set
            .members
            .iter()
            .map(|member| SetMember {
                node_id: member.node.clone(),
                storage_id: member.storage.map(|id| id.to_bytes()),
                learner: member.learner,
            })
            .collect(),
    }
}
