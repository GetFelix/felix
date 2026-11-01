//! Joint fences: a leader whose set changed fences the set it leaves too.
//!
//! Rule 7 of "Electing without the control plane" in
//! `docs/replication-design.md`; `FenceMembers` and `OpenForWrites` under
//! `JointFence` in `docs/formal/FelixShard.tla`. Without it, a cut-over that
//! swaps two members leaves a majority of the old set that never heard of the
//! new leader, and a replica there can stand and acknowledge on it
//! (`FelixShardElectSwapNoJoint.cfg`).
//!
//! The set being left is the control plane's set at the generation right
//! before the route's, as the model's `Swap` takes it (`oldSet' = CpSet`).
//! It comes from the assignment this broker's watch saw at that generation,
//! from the set the leader before shipped with at it, or from what this
//! leader kept beside its own set. When none of those knows it, the fence
//! stays pending: fencing the new set alone could leave a majority of the old
//! one that never heard of the change.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use felix_router::{NodeRef, PriorSet, Route};
use felix_storage::StorageId;
use felix_storage::disk_log::{Membership, ReplicaSet};
use felix_wire::internal::{PeerCapabilities, ReplicaSetView, SetMember};

use crate::peer::PeerRequester;

/// What a leader needs to fence jointly, given once the fleet has finalized
/// `joint_fence`: where to reach a member of the set it leaves, which its
/// route may no longer name.
///
/// `prior` is the control plane's set right before the route being fenced,
/// where this broker saw it ([`felix_router::ShardRouter::prior_set`]).
pub struct Joint<'a> {
    addr_of: &'a (dyn Fn(&str) -> Option<SocketAddr> + Sync),
    prior: Option<PriorSet>,
    local_storage: Option<StorageId>,
}

impl<'a> Joint<'a> {
    pub fn new(
        addr_of: &'a (dyn Fn(&str) -> Option<SocketAddr> + Sync),
        prior: Option<PriorSet>,
    ) -> Self {
        Self {
            addr_of,
            prior,
            local_storage: None,
        }
    }

    /// This broker's storage identity, so a promotion can tell a copy a set
    /// named another id for (a wipe) from one staged as a learner.
    pub fn with_local_storage(mut self, storage: Option<StorageId>) -> Self {
        self.local_storage = storage;
        self
    }

    /// Whether `log` keeps `local` as a learner of a set that names another
    /// copy for it: it lost what it held and has not caught up since.
    pub(super) fn is_wiped_learner(&self, log: &felix_broker::StreamLog, local: &str) -> bool {
        let Some(own) = self.local_storage else {
            return false;
        };
        log.replica_set().is_some_and(|kept| {
            kept.current.members.iter().any(|member| {
                member.node == local
                    && member.learner
                    && member.storage.is_some_and(|named| named != own)
            })
        })
    }
}

/// What a fence under `joint_fence` found: the set it leaves, to keep beside
/// this leader's own, and the joint fence when that set's voters differ.
#[derive(Debug)]
pub(super) struct Fenced {
    /// Kept even when nothing changed, so a fence tried again at this
    /// generation, after a restart too, knows it from the log.
    pub(super) before: Membership,
    pub(super) joint: Option<JointSets>,
}

/// One joint fence: whom it asks, the set it leaves, and the voters of each
/// set it needs a majority of.
#[derive(Debug)]
pub(super) struct JointSets {
    /// Every member of either set but this broker that offered `ELECTION`.
    pub(super) asked: Vec<NodeRef>,
    /// The set being left, as the fence carries it.
    pub(super) leaving: ReplicaSetView,
    /// The set being left counted with its learners and without: replicas
    /// keep it either way, and a majority of each is needed.
    old_voters: Vec<BTreeSet<String>>,
    new_voters: BTreeSet<String>,
    /// The copy the set being left names for a member, where it names one.
    copies: std::collections::BTreeMap<String, felix_storage::StorageId>,
    local: String,
}

impl JointSets {
    /// Whether an answer from `node_id`, whose handshake gave `identity`, is
    /// from the copy the set being left names. A node id given to a new,
    /// empty copy holds nothing the old set acknowledged.
    pub(super) fn same_copy(
        &self,
        node_id: &str,
        identity: Option<felix_storage::StorageId>,
    ) -> bool {
        self.copies
            .get(node_id)
            .is_none_or(|named| identity.as_ref() == Some(named))
    }

    /// Whether any member of the set being left is named by its copy, so
    /// answers must say which copy they come from.
    pub(super) fn names_copies(&self) -> bool {
        !self.copies.is_empty()
    }

    /// Whether `answered`, with this broker, is a majority of the voters of
    /// both sets. A learner is never a voter, so it never counts.
    pub(super) fn has_majorities<'n>(&self, answered: impl Iterator<Item = &'n str>) -> bool {
        let mut answered: BTreeSet<&str> = answered.collect();
        answered.insert(&self.local);
        let majority = |voters: &BTreeSet<String>| {
            voters
                .iter()
                .filter(|v| answered.contains(v.as_str()))
                .count()
                * 2
                > voters.len()
        };
        self.old_voters.iter().all(majority) && majority(&self.new_voters)
    }

    /// How far the answers got, for a fence that stays pending.
    pub(super) fn shortfall<'n>(&self, answered: impl Iterator<Item = &'n str>) -> String {
        let mut answered: BTreeSet<&str> = answered.collect();
        answered.insert(&self.local);
        let count = |voters: &BTreeSet<String>| {
            voters
                .iter()
                .filter(|v| answered.contains(v.as_str()))
                .count()
        };
        let old = self
            .old_voters
            .iter()
            .map(|voters| format!("{} of {}", count(voters), voters.len()))
            .collect::<Vec<_>>()
            .join(" and ");
        format!(
            "{old} voters of the set being left and {} of {} of the new set took the fence, \
             a majority of each needed",
            count(&self.new_voters),
            self.new_voters.len(),
        )
    }
}

/// The control plane's set right before `route`, which a set change at the
/// route's generation leaves. `Ok(None)`: the route is the shard's first
/// assignment and this log keeps no set, so nothing came before it. `Err`
/// says why it is unknown here.
pub(crate) fn set_before(
    stored: Option<&ReplicaSet>,
    route: &Route,
    prior: Option<&PriorSet>,
) -> Result<Option<Membership>, String> {
    let before = route.generation.checked_sub(1);
    if let Some(stored) = stored {
        if stored.current.generation > route.generation {
            return Err(format!(
                "this broker counts the set of generation {}, newer than this one",
                stored.current.generation
            ));
        }
        // What this leader kept beside its set under `joint_fence`. A set
        // kept at this generation without it may have been kept before the
        // feature was on, and says nothing about the set before.
        if stored.current.generation == route.generation
            && let Some(leaving) = &stored.leaving
            && Some(leaving.generation) == before
        {
            return Ok(Some(leaving.clone()));
        }
    }
    match prior.filter(|prior| prior.generation == route.generation) {
        Some(PriorSet {
            before: Some(set), ..
        }) => {
            if let Some(before) = before {
                return Ok(Some(crate::replica_set::placed(set, before)));
            }
        }
        // A first assignment has nothing before it, and a log that already
        // keeps a set is not one a first assignment would find.
        Some(PriorSet { before: None, .. }) if stored.is_none() => return Ok(None),
        _ => {}
    }
    // The leader at the generation before ships with its route's set.
    if let Some(stored) = stored
        && Some(stored.current.generation) == before
    {
        return Ok(Some(stored.current.clone()));
    }
    Err(format!(
        "the set before generation {} is not known here, so the set being left cannot be \
         fenced",
        route.generation
    ))
}

/// The fence for `route` under `joint_fence`: the set it leaves, and the
/// joint fence when that set's voters differ from the route's. `Err` is a
/// fence that cannot open yet, because the set being left is not known.
///
/// A member whose capabilities cannot be learned, whose address is unknown,
/// or that does not check sets, is not asked: it is a member that has not
/// answered, which keeps the fence from opening on less than both
/// majorities.
pub(super) async fn sets<R: PeerRequester>(
    requester: &R,
    log: &felix_broker::StreamLog,
    local_node_id: &str,
    route: &Route,
    set: &ReplicaSetView,
    joint: &Joint<'_>,
) -> Result<Fenced, String> {
    let before = match set_before(log.replica_set().as_ref(), route, joint.prior.as_ref())? {
        Some(before) => before,
        // Nothing before a first assignment: kept as an empty set, which
        // is no change to fence across, as in the model.
        None => Membership {
            generation: route.generation.saturating_sub(1),
            members: Vec::new(),
        },
    };
    let new_voters: BTreeSet<String> = set
        .members
        .iter()
        .filter(|member| !member.learner)
        .map(|member| member.node_id.clone())
        .collect();
    let all: BTreeSet<String> = before.members.iter().map(|m| m.node.clone()).collect();
    let counted: BTreeSet<String> = before
        .members
        .iter()
        .filter(|m| !m.learner)
        .map(|m| m.node.clone())
        .collect();
    let mut old_voters = vec![all];
    if counted != old_voters[0] {
        old_voters.push(counted);
    }
    // An empty set being left is no change to fence across, as in the model.
    if before.members.is_empty() || old_voters.iter().all(|old| *old == new_voters) {
        return Ok(Fenced {
            before,
            joint: None,
        });
    }

    let route_nodes = std::iter::once(&route.leader).chain(&route.replicas);
    let mut members: Vec<(String, Option<SocketAddr>)> = route_nodes
        .map(|node| (node.node_id.clone(), Some(node.advertise_addr)))
        .collect();
    for member in &before.members {
        if !members.iter().any(|(node, _)| *node == member.node) {
            members.push((member.node.clone(), (joint.addr_of)(&member.node)));
        }
    }
    let mut asked = Vec::new();
    for (node_id, addr) in members {
        if node_id == local_node_id {
            continue;
        }
        let Some(advertise_addr) = addr else {
            continue;
        };
        let offered = match requester.recorded_capabilities(&node_id) {
            Some(offered) => Some(offered),
            None => requester.capabilities(&node_id, advertise_addr).await.ok(),
        };
        // A member that does not check sets would take the fence without
        // the set being left; it cannot count toward either majority.
        if offered.is_some_and(|offered| offered.contains(PeerCapabilities::ELECTION)) {
            asked.push(NodeRef {
                node_id,
                advertise_addr,
                region: String::new(),
                live: true,
            });
        }
    }
    let joint = JointSets {
        asked,
        leaving: view(&before),
        copies: before
            .members
            .iter()
            .filter_map(|member| Some((member.node.clone(), member.storage?)))
            .collect(),
        old_voters,
        new_voters,
        local: local_node_id.to_string(),
    };
    Ok(Fenced {
        before,
        joint: Some(joint),
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
