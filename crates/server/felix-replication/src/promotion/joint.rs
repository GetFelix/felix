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
//!
//! So are the sets kept beside the log's own when this log holds no record
//! of its own set's generation: that fence may never have opened, and then a
//! majority of any set it was leaving may still hold what was acknowledged
//! before it (#1878). The log keeps every such set across fences that never
//! opened, not only the newest (#1881), and the fence carries all of them to
//! every member, so whichever replica fails over next fences them too.

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
    /// The log's sets from before the route that are left besides
    /// `leaving`. A member voting only in one is sent it instead of
    /// `leaving`, or it would refuse a fence naming no set it votes in. Every
    /// member is sent the rest as the fence's earlier sets, to keep.
    besides: Vec<ReplicaSetView>,
    /// Each set being left counted with its learners and without: replicas
    /// keep it either way, and a majority of each is needed.
    old_voters: Vec<OldVoters>,
    new_voters: BTreeSet<String>,
    local: String,
}

/// The voters of one set being left, with the copy it names for each
/// member, where it names one. An answer counts toward this set only from
/// that copy: another copy under the same node id holds nothing this set
/// acknowledged on.
#[derive(Debug, PartialEq)]
struct OldVoters {
    voters: BTreeSet<String>,
    copies: std::collections::BTreeMap<String, StorageId>,
}

impl OldVoters {
    fn counts(&self, node_id: &str, identity: Option<StorageId>) -> bool {
        self.copies
            .get(node_id)
            .is_none_or(|named| identity.as_ref() == Some(named))
    }
}

impl JointSets {
    /// Whether an answer from `node_id`, whose handshake gave `identity`, is
    /// from a copy some set being left names. A node id given to a new,
    /// empty copy holds nothing an old set acknowledged.
    pub(super) fn same_copy(&self, node_id: &str, identity: Option<StorageId>) -> bool {
        self.old_voters
            .iter()
            .any(|old| old.counts(node_id, identity))
    }

    /// The set being left as the fence to `node_id` carries it: one it
    /// votes in.
    pub(super) fn leaving_for(&self, node_id: &str) -> &ReplicaSetView {
        let votes_in = |set: &ReplicaSetView| {
            set.members
                .iter()
                .any(|member| member.node_id == node_id && !member.learner)
        };
        if votes_in(&self.leaving) {
            return &self.leaving;
        }
        self.besides
            .iter()
            .find(|besides| votes_in(besides))
            .unwrap_or(&self.leaving)
    }

    /// The sets being left that the fence to `node_id` carries besides
    /// [`Self::leaving_for`], so a replica keeps every one of them.
    pub(super) fn earlier_for(&self, node_id: &str) -> Vec<ReplicaSetView> {
        let sent = self.leaving_for(node_id);
        std::iter::once(&self.leaving)
            .chain(&self.besides)
            .filter(|set| *set != sent)
            .cloned()
            .collect()
    }

    /// Whether any member of the set being left is named by its copy, so
    /// answers must say which copy they come from.
    pub(super) fn names_copies(&self) -> bool {
        self.old_voters.iter().any(|old| !old.copies.is_empty())
    }

    /// Whether `answered`, each with the copy its handshake named, is with
    /// this broker a majority of the voters of every set. A learner is never
    /// a voter, so it never counts.
    pub(super) fn has_majorities<'n>(
        &self,
        answered: impl Iterator<Item = (&'n str, Option<StorageId>)>,
    ) -> bool {
        let answered: Vec<_> = answered.collect();
        self.old_voters
            .iter()
            .all(|old| self.majority(&old.voters, |v| self.answered_for(old, &answered, v)))
            && self.majority(&self.new_voters, |v| {
                answered.iter().any(|(node, _)| *node == v)
            })
    }

    /// Whether `nodes` could still make every majority, whichever copies
    /// they answer from.
    pub(super) fn can_reach<'n>(&self, nodes: impl Iterator<Item = &'n str>) -> bool {
        let nodes: BTreeSet<&str> = nodes.collect();
        self.old_voters
            .iter()
            .map(|old| &old.voters)
            .chain(std::iter::once(&self.new_voters))
            .all(|voters| self.majority(voters, |v| nodes.contains(v)))
    }

    fn answered_for(
        &self,
        old: &OldVoters,
        answered: &[(&str, Option<StorageId>)],
        voter: &str,
    ) -> bool {
        answered
            .iter()
            .any(|(node, identity)| *node == voter && old.counts(node, *identity))
    }

    fn count(&self, voters: &BTreeSet<String>, answered: impl Fn(&str) -> bool) -> usize {
        voters
            .iter()
            .filter(|v| *v == &self.local || answered(v))
            .count()
    }

    fn majority(&self, voters: &BTreeSet<String>, answered: impl Fn(&str) -> bool) -> bool {
        self.count(voters, answered) * 2 > voters.len()
    }

    /// How far the answers got, for a fence that stays pending.
    pub(super) fn shortfall<'n>(
        &self,
        answered: impl Iterator<Item = (&'n str, Option<StorageId>)>,
    ) -> String {
        let answered: Vec<_> = answered.collect();
        let old = self
            .old_voters
            .iter()
            .map(|old| {
                let took = self.count(&old.voters, |v| self.answered_for(old, &answered, v));
                format!("{took} of {}", old.voters.len())
            })
            .collect::<Vec<_>>()
            .join(" and ");
        format!(
            "{old} voters of the set being left and {} of {} of the new set took the fence, \
             a majority of each needed",
            self.count(&self.new_voters, |v| answered
                .iter()
                .any(|(node, _)| *node == v)),
            self.new_voters.len(),
        )
    }
}

/// Why [`set_before`] has no set for a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NoSetBefore {
    /// This broker counts the set of this newer generation.
    Newer(u64),
    /// Nothing here names the set at the generation before this one, and
    /// this log keeps this generation's own set. Only a later generation can
    /// tell, and renaming to it leaves the set this log keeps. [`sets`]
    /// returns it only when this broker also opened the generation as its
    /// leader, before `joint_fence` was on: that open already fenced the
    /// set before, so the rename skips nothing. Also generation 0 with no
    /// kept set: no log keeps one there, and no generation precedes it.
    Unknown(u64),
    /// As [`Self::Unknown`], but this log keeps the set of an older
    /// generation than the one before: this broker may never have opened
    /// the generation in between, whose set a rename would leave unfenced.
    /// [`sets`] returns [`Self::Unknown`] instead when this broker's own
    /// ballot and start record at this generation show its election opened
    /// it.
    Skipped(u64),
    /// As [`Self::Unknown`], but this log keeps no set and nothing says the
    /// route is the shard's first assignment. An earlier set may hold
    /// acknowledged records that a rename would leave unfenced.
    Unseen(u64),
    /// As [`Self::Unknown`], but this broker's ballot is not its own at this
    /// generation: the set was kept from another node's fence or election
    /// request, which proves nothing was fenced from here.
    NotOpened(u64),
}

impl std::fmt::Display for NoSetBefore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Newer(generation) => write!(
                f,
                "this broker counts the set of generation {generation}, newer than this one"
            ),
            Self::Unknown(generation)
            | Self::Skipped(generation)
            | Self::Unseen(generation)
            | Self::NotOpened(generation) => {
                write!(
                    f,
                    "the set before generation {generation} is not known here, so the set being \
                     left cannot be fenced"
                )
            }
        }
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
) -> Result<Option<Membership>, NoSetBefore> {
    let before = route.generation.checked_sub(1);
    if let Some(stored) = stored
        && stored.current.generation > route.generation
    {
        return Err(NoSetBefore::Newer(stored.current.generation));
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
        // A first assignment, as the watch saw it or a snapshot marked it,
        // has nothing before it, and a log that already keeps a set is not
        // one a first assignment would find.
        Some(PriorSet { before: None, .. }) if stored.is_none() => return Ok(None),
        _ => {}
    }
    // The leader at the generation before ships with its route's set.
    if let Some(stored) = stored
        && Some(stored.current.generation) == before
    {
        return Ok(Some(stored.current.clone()));
    }
    // Only this generation's own kept set is evidence that no earlier set
    // still holds records this broker would leave behind. A log never keeps
    // a set at generation 0 (`accept_set` takes it as the one it has), but
    // no generation precedes it: only a deleted stream recreated at
    // incarnation 0 can have written before, as without `joint_fence`. The
    // rename's fence at 1 still needs a majority of the set at 0.
    match stored {
        Some(stored) if stored.current.generation == route.generation => {
            Err(NoSetBefore::Unknown(route.generation))
        }
        Some(_) => Err(NoSetBefore::Skipped(route.generation)),
        None if route.generation == 0 => Err(NoSetBefore::Unknown(0)),
        None => Err(NoSetBefore::Unseen(route.generation)),
    }
}

/// The set `stored` keeps from before `route`, when it names other members
/// than `before`, the set [`set_before`] found. After an election the control
/// plane recorded without taking its set, the recorded set and the elected
/// leader's, which this log took, can have disjoint majorities, and either
/// may hold one that acknowledged. A failover's fence needs a majority of
/// both, as `Promote` fences the promoted broker's own set. A pending
/// cut-over leaves only the recorded set.
///
/// Without a watch prior, `before` is this log's own set at the generation
/// before, which is the route's set and not the control plane's when a
/// change was pending at it: the control plane's is the one the leader kept
/// as leaving, and the shard fails over from it. So that kept set is fenced
/// too.
pub(super) fn logged_besides(
    stored: Option<&ReplicaSet>,
    route: &Route,
    before: &Membership,
    prior: Option<&PriorSet>,
) -> Option<Membership> {
    if route
        .pending
        .as_deref()
        .is_some_and(|pending| pending.generation == route.generation)
        || before.members.is_empty()
    {
        return None;
    }
    let stored = stored?;
    let logged = &stored.current;
    if logged.generation < route.generation && !same_set(logged, before) {
        return Some(logged.clone());
    }
    let watched = prior.is_some_and(|prior| prior.generation == route.generation);
    stored
        .leaving
        .as_ref()
        .filter(|leaving| {
            !watched
                && Some(logged.generation) == route.generation.checked_sub(1)
                && !leaving.members.is_empty()
                && !same_set(leaving, before)
        })
        .cloned()
}

/// The sets `stored` keeps from before its own set, when `opened` is false:
/// this log holds no record of its set's generation, so that fence may never
/// have opened, and a majority of a set it was leaving may still hold what
/// was acknowledged before it while no later set does. That is the set kept
/// as leaving (#1878) and every earlier set the log carried across fences
/// that never opened (#1881). A failover fences each that `fenced` does not
/// already hold.
pub(super) fn unopened_sets(
    stored: Option<&ReplicaSet>,
    route: &Route,
    opened: bool,
    fenced: &[&Membership],
) -> Vec<Membership> {
    let Some(stored) =
        stored.filter(|stored| !opened && stored.current.generation <= route.generation)
    else {
        return Vec::new();
    };
    let mut sets: Vec<Membership> = Vec::new();
    for set in stored.leaving.iter().chain(&stored.earlier) {
        if !set.members.is_empty()
            && !fenced
                .iter()
                .copied()
                .chain(&sets)
                .any(|kept| same_set(kept, set))
        {
            sets.push(set.clone());
        }
    }
    sets
}

/// Whether two sets name the same members. Learner flags and copies count:
/// the same names voting differently, or from other copies, are another set,
/// whose majorities the other may miss.
fn same_set(one: &Membership, other: &Membership) -> bool {
    one.same_members(other)
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
/// fence that cannot open at this generation, because the set being left is
/// not known.
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
) -> Result<Fenced, NoSetBefore> {
    let stored = log.replica_set();
    let before = match set_before(stored.as_ref(), route, joint.prior.as_ref()) {
        // A rename is safe only past a generation this broker opened as
        // leader: its own ballot there, as `keep_own_set` records it.
        Err(NoSetBefore::Unknown(generation))
            if log.accepted_generation() != generation
                || log.accepted_leader().as_deref() != Some(local_node_id) =>
        {
            return Err(NoSetBefore::NotOpened(generation));
        }
        // A leader elected at a generation the control plane then adopted
        // keeps the set it stood on, from an older generation, and after a
        // restart nothing here names the set at the one before. A candidate
        // stands only on the set the control plane records, and adoption
        // keeps it, so that kept set is the one before, and the rename fences
        // it beside the control plane's. The evidence: its own ballot and
        // start record here (a ballot alone may be a withdrawn standing), and
        // the kept set naming exactly the route's set on the same copies with
        // nothing pending. These do not prove the open was fenced; the
        // unfenced opens that also leave them are covered in
        // docs/replication-design.md, "Planned handoff".
        Err(NoSetBefore::Skipped(generation))
            if log.accepted_generation() == generation
                && log.accepted_leader().as_deref() == Some(local_node_id)
                && log
                    .generations()
                    .iter()
                    .any(|epoch| epoch.generation == generation)
                && route.pending.is_none()
                && stored.as_ref().is_some_and(|stored| {
                    stored.leaving.is_none()
                        && stored.earlier.is_empty()
                        && names_the_route_set(&stored.current, route)
                }) =>
        {
            return Err(NoSetBefore::Unknown(generation));
        }
        before => before?,
    };
    let before = match before {
        Some(before) => before,
        // Nothing before a first assignment: kept as an empty set, which
        // is no change to fence across, as in the model.
        None => Membership {
            generation: route.generation.saturating_sub(1),
            members: Vec::new(),
        },
    };
    let mut besides: Vec<Membership> =
        logged_besides(stored.as_ref(), route, &before, joint.prior.as_ref())
            .into_iter()
            .collect();
    if !before.members.is_empty()
        && let Some(stored) = stored.as_ref()
    {
        let opened = log.holds_records_of(stored.current.generation);
        let fenced: Vec<&Membership> = std::iter::once(&before).chain(&besides).collect();
        let unopened = unopened_sets(Some(stored), route, opened, &fenced);
        besides.extend(unopened);
    }
    Ok(across(requester, local_node_id, route, set, before, besides, joint).await)
}

/// Whether `kept` names exactly `route`'s set: the same voters, none a learner
/// or wiped, each on the copy the route names. A copy either side leaves
/// unnamed may be another one, so it does not match.
fn names_the_route_set(kept: &Membership, route: &Route) -> bool {
    let routed = crate::replica_set::kept(route, None).current;
    kept.members.len() == routed.members.len()
        && kept.members.iter().all(|member| {
            !member.learner
                && !member.wiped
                && member.storage.is_some()
                && routed
                    .members
                    .iter()
                    .any(|named| named.node == member.node && named.storage == member.storage)
        })
}

/// The fence for `route` leaving `before`, and each of `besides` when more
/// sets are being left: jointly when the voters differ.
pub(super) async fn across<R: PeerRequester>(
    requester: &R,
    local_node_id: &str,
    route: &Route,
    set: &ReplicaSetView,
    before: Membership,
    besides: Vec<Membership>,
    joint: &Joint<'_>,
) -> Fenced {
    let olds = || std::iter::once(&before).chain(&besides);
    let new_voters: BTreeSet<String> = set
        .members
        .iter()
        .filter(|member| !member.learner)
        .map(|member| member.node_id.clone())
        .collect();
    // Each set being left counted with its learners and without.
    let mut old_voters: Vec<OldVoters> = Vec::new();
    for old in olds() {
        let all: BTreeSet<String> = old.members.iter().map(|m| m.node.clone()).collect();
        let counted: BTreeSet<String> = old
            .members
            .iter()
            .filter(|m| !m.learner)
            .map(|m| m.node.clone())
            .collect();
        let copies: std::collections::BTreeMap<String, StorageId> = old
            .members
            .iter()
            .filter_map(|m| Some((m.node.clone(), m.storage?)))
            .collect();
        for voters in [all, counted] {
            let voters = OldVoters {
                voters,
                copies: copies.clone(),
            };
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
    if before.members.is_empty()
        || (!replaced && old_voters.iter().all(|old| old.voters == new_voters))
    {
        return Fenced {
            before,
            joint: None,
        };
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
        // the set being left, and one that keeps a single set being left
        // would let a failover from its log skip the others; neither counts
        // toward any majority.
        if offered.is_some_and(|offered| {
            offered.contains(PeerCapabilities::ELECTION.union(PeerCapabilities::KEPT_SETS))
        }) {
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
        besides: besides.iter().map(view).collect(),
        old_voters,
        new_voters,
        local: local_node_id.to_string(),
    };
    Fenced {
        before,
        joint: Some(joint),
    }
}

pub(crate) fn view(set: &Membership) -> ReplicaSetView {
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
