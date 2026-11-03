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
//! A cut-over the control plane holds as pending names it on the route.
//! Otherwise it comes from the assignment this broker's watch saw at that
//! generation, from the set the leader before shipped with at it, or from
//! what this leader kept beside its own set. When none of those knows it, the fence
//! stays pending: fencing the new set alone could leave a majority of the old
//! one that never heard of the change.
//!
//! Outside a cut-over, the set this leader's log keeps from before the route
//! is fenced too when it names other members: after an election the control
//! plane recorded without taking, a failover leaves both the recorded set and
//! the elected leader's (#1806). A majority of each is needed.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use felix_router::{NodeRef, PendingChange, PriorSet, Route};
use felix_storage::StorageId;
use felix_storage::disk_log::{Member, Membership, ReplicaSet};
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
    replaced: bool,
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
            replaced: false,
        }
    }

    /// This broker's storage identity, so a promotion can tell a copy a set
    /// named another id for (a wipe) from one staged as a learner.
    pub fn with_local_storage(mut self, storage: Option<StorageId>) -> Self {
        self.local_storage = storage;
        self
    }

    /// Whether the control plane last registered this node under another
    /// storage id, so a copy here with no set may be one it lost.
    pub fn with_replaced_storage(mut self, replaced: bool) -> Self {
        self.replaced = replaced;
        self
    }

    /// Whether this copy is one a replica would refuse to count: its kept set
    /// marks it a learner it made itself on evidence of a wipe, or names
    /// another storage id for it, or it holds no set on a directory that
    /// replaced another, unless `route` is the shard's creation naming this
    /// copy's id. A learner placement staged, named under its own id and not
    /// marked wiped, may still lead. `PromoteNonVoter` in
    /// `docs/formal/FelixShard.tla`.
    pub(super) fn is_wiped_learner(
        &self,
        log: &felix_broker::StreamLog,
        local: &str,
        route: &Route,
    ) -> bool {
        let Some(kept) = log.replica_set() else {
            let created_here = route.generation == 1
                && crate::replica_set::view(route, None)
                    .members
                    .iter()
                    .any(|member| {
                        member.node_id == local
                            && member.storage_id.is_some_and(|named| {
                                self.local_storage == Some(StorageId::from_bytes(named))
                            })
                    });
            return self.replaced && !created_here;
        };
        let names_other = kept.current.members.iter().any(|member| {
            member.node == local
                && member
                    .storage
                    .is_some_and(|named| self.local_storage.is_some_and(|own| named != own))
        });
        log.member_is_learner(local) == Some(true) && (log.member_is_wiped(local) || names_other)
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
    /// The log's set from before the route, when it is a second set being
    /// left. A member voting only there is sent it instead of `leaving`, or
    /// it would refuse a fence naming no set it votes in.
    besides: Option<ReplicaSetView>,
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

    /// The set being left as the fence to `node_id` carries it: one it
    /// votes in.
    pub(super) fn leaving_for(&self, node_id: &str) -> &ReplicaSetView {
        let votes_in = |set: &ReplicaSetView| {
            set.members
                .iter()
                .any(|member| member.node_id == node_id && !member.learner)
        };
        match &self.besides {
            Some(besides) if !votes_in(&self.leaving) && votes_in(besides) => besides,
            _ => &self.leaving,
        }
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
    if let Some(stored) = stored
        && stored.current.generation > route.generation
    {
        return Err(format!(
            "this broker counts the set of generation {}, newer than this one",
            stored.current.generation
        ));
    }
    // A cut-over the control plane holds as pending names the set it leaves:
    // the control plane's own, as the model's `Swap` takes it, and the set
    // the leader acknowledges on (`held_by_both`). After an election the
    // control plane recorded without taking, this log can keep the elected
    // leader's set at the generation before, which is not the one being left.
    if let Some(pending) = route
        .pending
        .as_deref()
        .filter(|pending| pending.generation == route.generation)
        && let Some(before) = before
    {
        return Ok(Some(recorded_leaving(
            route, pending, before, stored, prior,
        )));
    }
    // What this leader kept beside its set under `joint_fence`. A set kept
    // at this generation without it may have been kept before the feature
    // was on, and says nothing about the set before.
    if let Some(stored) = stored
        && stored.current.generation == route.generation
        && let Some(leaving) = &stored.leaving
        && Some(leaving.generation) == before
    {
        return Ok(Some(leaving.clone()));
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

/// The set `stored` keeps from before `route`, when it names other members
/// than `before`, the set [`set_before`] found. After an election the control
/// plane recorded without taking its set, the recorded set and the elected
/// leader's, which this log took, can have disjoint majorities, and either
/// may hold one that acknowledged. A failover's fence needs a majority of
/// both, as `Promote` fences the promoted broker's own set. A pending
/// cut-over leaves only the recorded set.
pub(super) fn logged_besides(
    stored: Option<&ReplicaSet>,
    route: &Route,
    before: &Membership,
) -> Option<Membership> {
    if route
        .pending
        .as_deref()
        .is_some_and(|pending| pending.generation == route.generation)
        || before.members.is_empty()
    {
        return None;
    }
    let logged = &stored?.current;
    // Learner flags count: the same names voting differently are another
    // set, whose voter majorities the set before may miss.
    let nodes = |set: &Membership| -> BTreeSet<(String, bool)> {
        set.members
            .iter()
            .map(|m| (m.node.clone(), m.learner))
            .collect()
    };
    (logged.generation < route.generation && nodes(logged) != nodes(before)).then(|| logged.clone())
}

/// A pending change's set being left, at generation `before`. The other
/// sources only fill in what `PendingChange` does not carry, each member's
/// copy and learner mark, and only when they name exactly its members. The
/// watch comes first: a set this leader kept may have been kept without
/// copies. A member no source names a copy for takes the one the route
/// names, if any.
fn recorded_leaving(
    route: &Route,
    pending: &PendingChange,
    before: u64,
    stored: Option<&ReplicaSet>,
    prior: Option<&PriorSet>,
) -> Membership {
    let recorded: BTreeSet<&str> = pending.leaving.iter().map(String::as_str).collect();
    let same = |set: &Membership| {
        set.generation == before
            && set.members.len() == recorded.len()
            && set
                .members
                .iter()
                .all(|member| recorded.contains(member.node.as_str()))
    };
    let watched = prior
        .filter(|prior| prior.generation == route.generation)
        .and_then(|prior| prior.before.as_deref())
        .map(|set| crate::replica_set::placed(set, before));
    let kept_leaving = stored.and_then(|stored| stored.leaving.clone());
    let logged = stored.map(|stored| stored.current.clone());
    let mut set = [watched, kept_leaving, logged]
        .into_iter()
        .flatten()
        .find(same)
        .unwrap_or_else(|| Membership {
            generation: before,
            members: recorded
                .iter()
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .collect(),
        });
    for member in &mut set.members {
        if member.storage.is_none() {
            member.storage = crate::replica_set::named_storage(route, &member.node);
        }
    }
    set
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
    let stored = log.replica_set();
    let before = match set_before(stored.as_ref(), route, joint.prior.as_ref())? {
        Some(before) => before,
        // Nothing before a first assignment: kept as an empty set, which
        // is no change to fence across, as in the model.
        None => Membership {
            generation: route.generation.saturating_sub(1),
            members: Vec::new(),
        },
    };
    let besides = logged_besides(stored.as_ref(), route, &before);
    let olds = || std::iter::once(&before).chain(besides.as_ref());
    let new_voters: BTreeSet<String> = set
        .members
        .iter()
        .filter(|member| !member.learner)
        .map(|member| member.node_id.clone())
        .collect();
    // Each set being left counted with its learners and without.
    let mut old_voters: Vec<BTreeSet<String>> = Vec::new();
    for old in olds() {
        let all: BTreeSet<String> = old.members.iter().map(|m| m.node.clone()).collect();
        let counted: BTreeSet<String> = old
            .members
            .iter()
            .filter(|m| !m.learner)
            .map(|m| m.node.clone())
            .collect();
        for voters in [all, counted] {
            if !old_voters.contains(&voters) {
                old_voters.push(voters);
            }
        }
    }
    // A member kept under another storage id is another copy: the old set's
    // majority must count only the copy it named, so the same node list is
    // still a change to fence across (#1737).
    let replaced = set.members.iter().any(|member| {
        olds().flat_map(|old| &old.members).any(|old| {
            old.node == member.node_id
                && matches!(
                    (old.storage, member.storage_id),
                    (Some(was), Some(now)) if was != StorageId::from_bytes(now)
                )
        })
    });
    // An empty set being left is no change to fence across, as in the model.
    if before.members.is_empty() || (!replaced && old_voters.iter().all(|old| *old == new_voters)) {
        return Ok(Fenced {
            before,
            joint: None,
        });
    }

    let route_nodes = std::iter::once(&route.leader).chain(&route.replicas);
    let mut members: Vec<(String, Option<SocketAddr>)> = route_nodes
        .map(|node| (node.node_id.clone(), Some(node.advertise_addr)))
        .collect();
    for member in olds().flat_map(|old| &old.members) {
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
        besides: besides.as_ref().map(view),
        // The set `set_before` found names the copy where both name one.
        copies: olds()
            .rev()
            .flat_map(|old| &old.members)
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
