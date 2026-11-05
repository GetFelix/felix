//! Ownership resolution on the ingress path.
//!
//! Every publish asks one question before anything else happens: is this shard
//! mine? The answer is `Local`, `Forward`, or a typed refusal — never a
//! shrug. A broker that treats an unresolved shard as its own is a broker
//! writing data another node owns.
//!
//! **Single-node brokers are unaffected.** A broker with no cluster identity has
//! no router to consult and no assignments to honour, so dispatch is `Local` by
//! construction. Clustering is opt-in, and a broker that never joined one must
//! behave exactly as it did before this existed.

/// Map a request to a shard number.
///
/// Re-exported from `felix_wire::routing` rather than defined here: a client
/// that routes its publishes to the shard's owner has to reach the same answer
/// this broker does, and two copies of a hash are two things that can drift.
/// The wire crate is where both sides already meet.
pub use felix_wire::routing::shard_for;

pub mod hold;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use felix_router::{Resolution, ShardRouter, Unavailable};

use crate::shards::lifecycle::Phase;
use crate::shards::lifecycle::fence::{FenceGuard, Fenced, ShardFence};
use crate::shards::routing::hold::MoveHold;
use crate::shards::{ShardKey, ShardKind};

/// Bound on one node catalog read. The feed publishes routes only after the
/// read settles, so a control plane that accepts the request and never answers
/// would otherwise hold every later ownership change here with it.
const CATALOG_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// What ingress should do with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatch {
    /// Serve it here, at this generation.
    ///
    /// A write carries the generation to its claim, where the fence refuses it
    /// if the shard stopped serving in between. Zero on a single-node broker,
    /// which has no generations and no fence.
    Local { generation: u64 },
    /// Another node owns it. The publish is forwarded there over the internal
    /// protocol and its answer is relayed back to the client.
    Forward {
        node_id: String,
        advertise_addr: SocketAddr,
        /// The generation this broker resolved against.
        ///
        /// Carried to the owner, which compares it with its own. A mismatch in
        /// either direction is a typed answer and never a successful ownership
        /// claim — see `docs/internal-protocol.md`.
        generation: u64,
        /// The stream or cache incarnation the route is for, 0 for one from
        /// before incarnations. Carried to an owner that reads it, which
        /// refuses a forward routed by another incarnation's assignment.
        incarnation: u64,
    },
    /// Nobody can serve it right now, and this says why.
    Unavailable(Reason),
}

/// Why a request cannot be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// Placement has not assigned the shard.
    NotAssigned,
    /// The owner is known but unreachable or not live.
    OwnerUnavailable(String),
    /// This node owns the shard but has not finished opening it.
    ///
    /// Deliberately not `Local`: serving now would acknowledge a write against
    /// a log that is not yet recovered.
    NotReady,
    /// This node's routing view is behind the caller's.
    Stale { have: u64, wanted: u64 },
    /// The shard is moving to another broker and has not cut over yet.
    Moving,
    /// The leader is in a region this broker has no bridge to, so it will not
    /// forward there. A client that connects to the leader directly is not
    /// refused by this.
    RegionNotRoutable { region: String },
}

impl Reason {
    /// The `reason` a `shard_unavailable` error carries to the client.
    pub fn wire_name(&self) -> &'static str {
        use felix_wire::shard_unavailable_reason as wire;
        match self {
            Self::NotAssigned => wire::NOT_ASSIGNED,
            Self::OwnerUnavailable(_) => wire::OWNER_UNAVAILABLE,
            Self::NotReady => wire::NOT_READY,
            Self::Stale { .. } => wire::STALE,
            Self::Moving => wire::MOVING,
            Self::RegionNotRoutable { .. } => wire::REGION_NOT_ROUTABLE,
        }
    }
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAssigned => write!(f, "shard is not assigned to any broker"),
            Self::OwnerUnavailable(detail) => write!(f, "shard owner is unavailable: {detail}"),
            Self::NotReady => write!(f, "this broker is still opening the shard"),
            Self::Stale { have, wanted } => {
                write!(
                    f,
                    "routing view is behind: have generation {have}, caller has {wanted}"
                )
            }
            Self::Moving => write!(f, "shard is moving to another broker"),
            Self::RegionNotRoutable { region } => write!(
                f,
                "shard leader is in region {region}, which this broker has no bridge to"
            ),
        }
    }
}

/// Shards this broker has opened, and the generation each was opened at.
///
/// Published as an immutable snapshot for the same reason routes are: the
/// publish path reads this on every request, and it must not take a lock a
/// writer can hold -- least of all an async one, which would put an await into
/// the hot path for a lookup that is two loads.
pub type ServableShards = HashMap<ShardKey, u64>;

/// Resolves ingress requests against cluster ownership.
///
/// Absent on a single-node broker — see [`dispatch`].
///
/// One `ArcSwap` load per request, so `dispatch` is synchronous and allocation
/// free. That is deliberate: it sits in front of every publish, and a resolver
/// that cost a lock or an await would show up in p999 long before it showed up
/// in a correctness test.
pub struct IngressRouter {
    router: Arc<ShardRouter>,
    view: ArcSwap<View>,
    fence: Arc<ShardFence>,
    /// Woken on every new view, for writes held while their shard moves.
    changed: tokio::sync::Notify,
    hold: MoveHold,
    /// Runs inside [`IngressRouter::publish`] just before the view is stored,
    /// so a test can land a change between the read and the store.
    #[cfg(test)]
    before_publish: std::sync::Mutex<Option<PublishHook>>,
}

#[cfg(test)]
type PublishHook = Box<dyn FnMut(&IngressRouter) + Send>;

/// Routes and the servable set, published as one value.
///
/// Two swaps would leave a moment where one is updated and the other is not:
/// a new owner that has opened a shard but still routes it to the old leader,
/// or an old leader that has stopped serving but still routes it to itself.
struct View {
    routes: Arc<felix_router::Routes>,
    servable: ServableShards,
}

impl IngressRouter {
    /// `fence` is the one the shard lifecycle opens and closes; see
    /// [`crate::shards::lifecycle::ShardLifecycle::fence`].
    pub fn new(router: Arc<ShardRouter>, fence: Arc<ShardFence>) -> Self {
        let view = ArcSwap::from_pointee(View {
            routes: router.routes(),
            servable: ServableShards::new(),
        });
        Self {
            router,
            view,
            fence,
            changed: tokio::sync::Notify::new(),
            hold: MoveHold::disabled(),
            #[cfg(test)]
            before_publish: std::sync::Mutex::new(None),
        }
    }

    /// Hold writes to a moving shard under `hold` rather than refusing them.
    pub fn with_move_hold(mut self, hold: MoveHold) -> Self {
        self.hold = hold;
        self
    }

    /// The write fence every local write enters when it claims its place in
    /// the log. Here because every write path already holds this router.
    pub fn fence(&self) -> &Arc<ShardFence> {
        &self.fence
    }

    /// The limits writes to a moving shard are held under.
    pub fn move_hold(&self) -> &MoveHold {
        &self.hold
    }

    /// Publish new routes and the shards this broker can serve, together.
    ///
    /// The feed calls this after each lifecycle reconcile, which is the only
    /// thing that changes the servable set.
    pub fn publish(
        &self,
        table: felix_router::RoutingTable,
        nodes: &HashMap<String, felix_router::NodeRef>,
        servable: ServableShards,
    ) {
        let routes = self.router.publish(table, nodes);
        #[cfg(test)]
        if let Some(hook) = self.before_publish.lock().unwrap().as_mut() {
            hook(self);
        }
        self.store(View { routes, servable });
    }

    /// Replace the servable set, keeping the routes last published to the
    /// router.
    pub fn publish_servable(&self, servable: ServableShards) {
        self.store(View {
            routes: self.router.routes(),
            servable,
        });
    }

    /// The generation this broker currently leads `key` at, if it does.
    ///
    /// A publish waiting for a quorum needs it: the majority is over the
    /// replica set of *that* generation, and an acknowledgement from an older
    /// one does not count toward a newer one's quorum.
    pub fn generation(&self, key: &ShardKey) -> Option<u64> {
        match self.dispatch(key) {
            Dispatch::Local { generation } => Some(generation),
            _ => None,
        }
    }

    /// Whether `key` has a replica besides its leader. A shard with none has
    /// its majority in the leader alone, and nothing ships for it.
    pub fn replicated(&self, key: &ShardKey) -> bool {
        self.view
            .load()
            .routes
            .table()
            .get(&to_router_key(key))
            .is_some_and(|route| !route.replicas.is_empty())
    }

    /// The route last published for `key`: its leader and replicas.
    pub fn route(&self, key: &ShardKey) -> Option<felix_router::Route> {
        self.view
            .load()
            .routes
            .table()
            .get(&to_router_key(key))
            .cloned()
    }

    /// Every shard this broker leads and has opened, with the generation it
    /// leads each at.
    pub(crate) fn led_shards(&self) -> Vec<(ShardKey, u64)> {
        let view = self.view.load();
        view.servable
            .keys()
            .filter_map(|key| match self.dispatch_in(&view, key) {
                Dispatch::Local { generation } => Some((key.clone(), generation)),
                _ => None,
            })
            .collect()
    }

    /// How many shards this stream or cache was placed with.
    ///
    /// Read from the published routes, which is an `ArcSwap` load and no lock.
    pub fn shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> u32 {
        self.view.load().routes.table().shards_for(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
        )
    }

    /// This broker's node id, as the routes name it.
    pub(crate) fn local_node_id(&self) -> &str {
        self.router.local_node_id()
    }

    /// How the stream was placed, or `None` if the routing snapshot does not
    /// know it. See [`felix_router::RoutingTable::placement_for`].
    pub(crate) fn placement_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<felix_router::StreamPlacement> {
        self.view.load().routes.table().placement_for(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
        )
    }

    /// The shard `routing_key` belongs to, by the stream's own width and
    /// mapping. See [`felix_router::RoutingTable::shard_for_key`].
    pub(crate) fn shard_for_key(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        routing_key: Option<&[u8]>,
    ) -> u32 {
        self.view.load().routes.table().shard_for_key(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
            routing_key,
        )
    }

    /// The placed shard count, or `None` if the routing snapshot does not know
    /// this stream. See [`felix_router::RoutingTable::placed_shards_for`].
    pub fn placed_shards_for(
        &self,
        kind: ShardKind,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Option<u32> {
        self.view.load().routes.table().placed_shards_for(
            to_router_kind(kind),
            tenant_id,
            namespace,
            stream,
        )
    }

    /// Decide what to do with a request for `key`.
    ///
    /// Two sources have to agree. The routes say who the cluster believes owns
    /// the shard; the servable set says whether this broker has actually opened
    /// it. Trusting only the routes would serve writes during recovery;
    /// trusting only local state would keep serving a shard that has been
    /// reassigned.
    pub fn dispatch(&self, key: &ShardKey) -> Dispatch {
        self.dispatch_in(&self.view.load(), key)
    }

    /// [`Self::dispatch`] for a write, waiting out a planned move.
    ///
    /// A shard between its fence and its cut-over is held until the routes
    /// show where it went, then dispatched there; see [`hold`]. So is one this
    /// broker has just taken and is still fencing. A local answer
    /// comes with the write's place in the fence, entered here so a fence that
    /// closes before the write claims its offsets still counts it and the move
    /// waits for it.
    pub(crate) async fn dispatch_write(&self, key: &ShardKey) -> (Dispatch, Option<FenceGuard>) {
        let mut held = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            // Registered before the view is read, so a view published in
            // between still wakes this.
            changed.as_mut().enable();
            let view = self.view.load();
            let dispatch = self.dispatch_in(&view, key);
            let moving = match &dispatch {
                Dispatch::Local { generation } => match self.fence.admit(key, *generation) {
                    Ok(fenced) => return (settled(held, dispatch), Some(fenced)),
                    // No move to wait out: the write goes on without a place
                    // in the fence and is refused when it tries to enter it.
                    Err(Fenced::LeaseLapsed) => return (settled(held, dispatch), None),
                    // The lifecycle closed the fence and the view that says
                    // why is a moment behind it.
                    Err(Fenced::NotServing) => true,
                },
                Dispatch::Unavailable(Reason::Moving) => true,
                // The write waits for a takeover as it waits out a move,
                // rather than failing the cut-over it arrived at.
                Dispatch::Unavailable(Reason::NotReady) => self.taking_over(key),
                // The leader named here is fenced and will not take it,
                // whether this broker would forward to it or cannot reach it.
                _ => draining(&view, key),
            };
            if !moving {
                return (settled(held, dispatch), None);
            }
            if held.is_none() {
                match self.hold.begin() {
                    Ok(hold) => held = Some(hold),
                    Err(_) => return (gave_up(dispatch), None),
                }
            }
            let deadline = held.as_ref().map(hold::Held::deadline).expect("held");
            if tokio::time::timeout_at(deadline.into(), changed)
                .await
                .is_err()
            {
                if let Some(hold) = held.take() {
                    hold.timed_out();
                }
                return (gave_up(dispatch), None);
            }
        }
    }

    /// Wait until `key` is not moving, this broker's routes have reached
    /// `generation` (and `incarnation`, when the requester named one), and a
    /// shard this broker has just taken is serving, or the hold gives up.
    ///
    /// Routes for a newer incarnation than the requester named are settled
    /// at any generation: the ownership check refuses the forward, and no
    /// wait would change that.
    ///
    /// For a write forwarded here: the requester may have seen a cut-over this
    /// broker has not, may have sent it here in the gap before one, or may
    /// have sent it the moment the routes named this broker, while it still
    /// fences the replicas. Each settles in milliseconds, and the answer after
    /// waiting is a definite one, and usually a success.
    pub(crate) async fn settle(&self, key: &ShardKey, generation: u64, incarnation: Option<u64>) {
        let mut held = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let view = self.view.load();
            let unsettled = match view.routes.table().get(&to_router_key(key)) {
                Some(route) => {
                    route.draining
                        || match incarnation {
                            Some(sent) if sent != route.incarnation => route.incarnation < sent,
                            _ => route.generation < generation,
                        }
                }
                None => generation > 0 || incarnation.is_some(),
            } || (self.dispatch_in(&view, key)
                == Dispatch::Unavailable(Reason::NotReady)
                && self.taking_over(key));
            if !unsettled {
                if let Some(hold) = held {
                    hold::Held::settled(hold);
                }
                return;
            }
            if held.is_none() {
                match self.hold.begin() {
                    Ok(hold) => held = Some(hold),
                    Err(_) => return,
                }
            }
            let deadline = held.as_ref().map(hold::Held::deadline).expect("held");
            if tokio::time::timeout_at(deadline.into(), changed)
                .await
                .is_err()
            {
                if let Some(hold) = held.take() {
                    hold.timed_out();
                }
                return;
            }
        }
    }

    /// Whether this broker has just taken `key` and is opening or fencing it,
    /// which usually takes a round trip. `Active` too: the lifecycle settles
    /// the fence a moment before the servable set that says so is published.
    fn taking_over(&self, key: &ShardKey) -> bool {
        self.fence.awaiting_promotion(key).is_some()
            || self.fence.phase_of(key).is_some_and(|record| {
                matches!(
                    record.phase,
                    Phase::Opening | Phase::Fencing | Phase::Active
                )
            })
    }

    fn dispatch_in(&self, view: &View, key: &ShardKey) -> Dispatch {
        match self.router.resolve_with(&view.routes, &to_router_key(key)) {
            Resolution::Local { generation } => {
                // The cluster says ours. Local readiness has the deciding vote,
                // and only at this exact generation: an older one means we have
                // not caught up with a reassignment that already happened.
                if view.servable.get(key) == Some(&generation) {
                    Dispatch::Local { generation }
                } else if draining(view, key) {
                    Dispatch::Unavailable(Reason::Moving)
                } else {
                    Dispatch::Unavailable(Reason::NotReady)
                }
            }
            Resolution::Remote {
                node_id,
                advertise_addr,
                generation,
                incarnation,
            } => Dispatch::Forward {
                node_id,
                advertise_addr,
                generation,
                incarnation,
            },
            Resolution::Stale { have, wanted } => {
                Dispatch::Unavailable(Reason::Stale { have, wanted })
            }
            Resolution::Unavailable(Unavailable::NoAssignment) => {
                Dispatch::Unavailable(Reason::NotAssigned)
            }
            Resolution::Unavailable(Unavailable::RegionNotRoutable { region }) => {
                Dispatch::Unavailable(Reason::RegionNotRoutable { region })
            }
            Resolution::Unavailable(other) => {
                Dispatch::Unavailable(Reason::OwnerUnavailable(other.to_string()))
            }
        }
    }

    fn store(&self, view: View) {
        self.view.store(Arc::new(view));
        self.changed.notify_waiters();
    }
}

impl felix_replication::quorum::ShardServing for IngressRouter {
    fn replicated(&self, key: &ShardKey) -> bool {
        IngressRouter::replicated(self, key)
    }

    fn generation(&self, key: &ShardKey) -> Option<u64> {
        IngressRouter::generation(self, key)
    }

    fn lease_valid(&self) -> bool {
        self.fence.lease_valid()
    }

    fn record_ack_refusal(&self) {
        crate::cluster::lease::metrics::record_refusal(
            crate::cluster::lease::metrics::BOUNDARY_ACK,
        );
    }
}

/// Whether `key`'s leader in `view` has been fenced for a move.
fn draining(view: &View, key: &ShardKey) -> bool {
    view.routes
        .table()
        .get(&to_router_key(key))
        .is_some_and(|route| route.draining)
}

fn settled(held: Option<hold::Held<'_>>, dispatch: Dispatch) -> Dispatch {
    if let Some(hold) = held {
        hold.settled();
    }
    dispatch
}

/// What a write is told when the hold gives up on a moving shard. Anything
/// but a local answer is refused as moving: forwarding to a fenced leader
/// would only be refused there. A local answer whose fence closed is left for
/// the claim to refuse, as it always did.
fn gave_up(dispatch: Dispatch) -> Dispatch {
    match dispatch {
        Dispatch::Local { .. } => dispatch,
        _ => Dispatch::Unavailable(Reason::Moving),
    }
}

/// Decide what to do with a request, for a broker that may or may not be in a
/// cluster.
///
/// `None` is a single-node broker: no identity, no assignments, nothing to
/// resolve against. Everything is local, exactly as it was before clustering
/// existed, and it costs one branch.
#[inline]
pub fn dispatch(ingress: Option<&IngressRouter>, key: &ShardKey) -> Dispatch {
    match ingress {
        None => Dispatch::Local { generation: 0 },
        Some(ingress) => ingress.dispatch(key),
    }
}

/// [`dispatch`] for a write, held through a planned move; see
/// [`IngressRouter::dispatch_write`].
pub(crate) async fn dispatch_write(
    ingress: Option<&IngressRouter>,
    key: &ShardKey,
) -> (Dispatch, Option<FenceGuard>) {
    match ingress {
        None => (Dispatch::Local { generation: 0 }, None),
        Some(ingress) => ingress.dispatch_write(key).await,
    }
}

pub(crate) fn to_router_key(key: &ShardKey) -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: to_router_kind(key.kind),
    }
}

/// The watch's kind and the router's kind are separate types on purpose — the
/// router is a library that does not know about the control plane's wire
/// format — so the two are mapped here, in one place.
pub(crate) fn to_router_kind(kind: ShardKind) -> felix_router::ShardKind {
    match kind {
        ShardKind::Stream => felix_router::ShardKind::Stream,
        ShardKind::Cache => felix_router::ShardKind::Cache,
    }
}

/// Build a routing table from what the watch currently holds.
///
/// Lives here rather than in the watch because it is the point where two
/// separate views -- ownership and node addresses -- are joined into the one
/// the hot path reads.
pub fn routing_table_from(
    assignments: &std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    nodes: &std::collections::HashMap<String, felix_router::NodeRef>,
) -> felix_router::RoutingTable {
    felix_router::RoutingTable::build_with(
        assignments.values().map(|assignment| felix_router::Placed {
            key: to_router_key(&assignment.key),
            leader: assignment.leader.clone(),
            // While a change is pending the route names the new set, as it
            // did when a cut-over took the new set at once, and keeps the
            // control plane's set beside it for the acknowledgements.
            replicas: assignment.pending_set.as_ref().map_or_else(
                || assignment.replicas.clone(),
                |pending| pending.replicas.clone(),
            ),
            generation: assignment.generation,
            draining: assignment.is_draining(),
            successor: assignment.successor.clone(),
            routing: assignment.routing,
            incarnation: assignment.incarnation,
            fence_storage_ids: assignment.fence_storage_ids.clone(),
            member_storage_ids: assignment.member_storage_ids.clone(),
            pending: assignment
                .pending_set
                .as_ref()
                .map(|pending| felix_router::PendingChange {
                    generation: pending.generation,
                    leaving: std::iter::once(&assignment.leader)
                        .chain(&assignment.replicas)
                        .cloned()
                        .collect(),
                }),
        }),
        nodes,
    )
}

/// `assignments` with the leaderships the replicas elected laid over them
/// (`felix_replication::election`): the lifecycle opens one this broker
/// stood for as a promotion, and the routes send its followers' clients and
/// batches to it. Each stands until the control plane names its generation
/// or a newer one.
fn with_elected(
    mut assignments: std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    elections: &felix_replication::election::Elections,
) -> std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment> {
    if elections.is_empty() {
        return assignments;
    }
    let by_router_key: HashMap<felix_router::ShardKey, ShardKey> = assignments
        .keys()
        .map(|key| (to_router_key(key), key.clone()))
        .collect();
    let elected = elections.overlay(|key| {
        by_router_key
            .get(key)
            .and_then(|key| assignments.get(key))
            .map(|assignment| felix_replication::election::Named {
                generation: assignment.generation,
                incarnation: assignment.incarnation,
                leader: assignment.leader.clone(),
            })
    });
    for (key, elected) in elected {
        let Some(assignment) = by_router_key
            .get(&key)
            .and_then(|key| assignments.get_mut(key))
        else {
            continue;
        };
        // The route below hides the control plane's set, which a candidate
        // must still compare its own with.
        let ids = if assignment.member_storage_ids.is_empty() {
            &assignment.fence_storage_ids
        } else {
            &assignment.member_storage_ids
        };
        elections.note_recorded(
            key.clone(),
            felix_replication::election::Recorded {
                generation: assignment.generation,
                members: std::iter::once(&assignment.leader)
                    .chain(&assignment.replicas)
                    .map(|node| (node.clone(), ids.get(node).cloned()))
                    .collect(),
                pending: assignment.pending_set.is_some(),
            },
        );
        assignment.replicas = elected
            .set
            .members
            .iter()
            .map(|member| member.node.clone())
            .filter(|node| *node != elected.leader)
            .collect();
        assignment.member_storage_ids = elected
            .set
            .members
            .iter()
            .filter_map(|member| Some((member.node.clone(), member.storage?.to_hex())))
            .collect();
        assignment.leader = elected.leader;
        assignment.generation = elected.generation;
        assignment.state = "active".to_string();
        assignment.successor = None;
        assignment.fence_storage_ids.clear();
        assignment.pending_set = None;
    }
    assignments
}

/// The shards `after` names `local` a follower of, by key and incarnation,
/// that `before`, the assignments the feed last applied, did not. Nothing
/// while `before` is empty, as in the first view after a start, whose
/// copies mostly hold promises already and would each start a read for
/// nothing. A shard's creation naming `local` is left out too, since that
/// copy skips the read (`ReplicaHandler::created_here`).
fn newly_named<'a>(
    before: &'a std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    after: &'a std::collections::HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    local: &'a str,
) -> impl Iterator<Item = (felix_router::ShardKey, u64)> + 'a {
    let follows = move |assignment: &crate::shards::watch::ShardAssignment| {
        assignment.leader != local
            && (assignment.successor.as_deref() == Some(local)
                || assignment.replicas.iter().any(|node| node == local)
                || assignment
                    .pending_set
                    .as_ref()
                    .is_some_and(|pending| pending.replicas.iter().any(|node| node == local)))
    };
    after
        .values()
        .filter(move |assignment| {
            !before.is_empty()
                && follows(assignment)
                && !(assignment.generation == 1
                    && assignment.incarnation != 0
                    && assignment.member_storage_ids.contains_key(local))
                && !before
                    .get(&assignment.key)
                    .is_some_and(|was| was.incarnation == assignment.incarnation && follows(was))
        })
        .map(|assignment| (to_router_key(&assignment.key), assignment.incarnation))
}

/// The cluster state one task keeps in step. Grouped because they are only ever
/// used together, and only in the order this feed applies them.
pub struct FeedState {
    pub ownership: Arc<tokio::sync::RwLock<crate::shards::watch::ShardOwnership>>,
    pub lifecycle: Arc<tokio::sync::Mutex<crate::shards::lifecycle::ShardLifecycle>>,
    pub store: Arc<dyn crate::shards::lifecycle::ShardStore>,
    pub ingress: Arc<IngressRouter>,
    /// Refreshed on the same tick as the catalog it is derived from, so what a
    /// client is told and what this broker forwards to cannot come from
    /// different fetches.
    pub client_endpoints: Option<Arc<crate::cluster::client_endpoints::ClientEndpoints>>,
    /// Notified by the watch when ownership changed. The feed acts on it at
    /// once instead of at its next tick.
    pub assignments_changed: Arc<tokio::sync::Notify>,
    /// Notified by the feed after it acts on a change, so replication ships a
    /// newly fenced or newly led shard without waiting for its own tick.
    pub routes_changed: Arc<tokio::sync::Notify>,
    /// Priors are read only once `joint_fence` is finalized here, so they are
    /// built only then.
    pub joint_fence: Option<Arc<felix_common::fleet::FleetGate>>,
    /// Starts a copy's confirmed read as soon as the assignments name it a
    /// follower, before the routes are even published, so the read is
    /// usually done before the leader's first request.
    pub prefetch: Option<felix_replication::confirmed::Prefetch>,
    /// Leaderships the replicas elected, laid over the control plane's
    /// assignments. Notifies `assignments_changed` when it changes.
    pub elections: Option<Arc<felix_replication::election::Elections>>,
}

/// What the feed needs to read the node catalog.
pub struct CatalogSource {
    pub client: reqwest::Client,
    pub base_url: String,
    /// The same credential the assignment feed uses: `/v1/nodes` and
    /// `/v1/shard-assignments` both require `node.view:cluster:*`. Held, not
    /// copied, so a refresh reaches this feed too.
    pub token: Option<crate::cluster::credential::NodeCredential>,
}

/// Keep local shard state and the routing table in step with the watch.
///
/// One task owns the sequence, so the two never disagree: reconcile local state
/// first, then publish what is servable and the routes together.
///
/// Runs on a tick and whenever the watch reports a change, and both refresh the
/// node catalog: an assignment names an owner by id, and only the catalog turns
/// that into an address to forward to. A wake refreshes too because a broker
/// that restarted re-registers under the same id on new ports, and nothing in
/// the assignments says so. Wakes follow the watch's change batches, so this
/// is at most one catalog read per batch, and it runs alongside reconcile
/// rather than in front of it.
pub fn spawn_feed(
    state: FeedState,
    catalog_source: Option<CatalogSource>,
    interval: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let FeedState {
        ownership,
        lifecycle,
        store,
        ingress,
        client_endpoints,
        assignments_changed,
        routes_changed,
        joint_fence,
        prefetch,
        elections,
    } = state;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut catalog = HashMap::new();
        // The assignments the last pass applied, to tell which are new.
        let mut applied = HashMap::new();
        let mut published_priors = HashMap::new();
        loop {
            let woken = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => false,
                _ = assignments_changed.notified() => true,
            };

            let joint = joint_fence
                .as_ref()
                .is_some_and(|fleet| fleet.supports(felix_common::fleet::JOINT_FENCE));
            let (assignments, renewals, priors) = {
                let owned = ownership.read().await;
                (
                    owned.assignments().clone(),
                    owned.renewals().clone(),
                    if joint {
                        owned.priors()
                    } else {
                        HashMap::new()
                    },
                )
            };
            let assignments = match &elections {
                Some(elections) => with_elected(assignments, elections),
                None => assignments,
            };
            if let Some(prefetch) = &prefetch {
                prefetch.named(newly_named(&applied, &assignments, ingress.local_node_id()));
            }

            // Read on every refresh rather than once, so a refreshed token is
            // in use from the next poll.
            let refresh = async {
                let source = catalog_source.as_ref()?;
                let bearer = source.token.as_ref().map(|token| token.bearer());
                let fetch = crate::cluster::node_catalog::fetch(
                    &source.client,
                    &source.base_url,
                    bearer.as_deref().map(String::as_str),
                );
                Some(
                    tokio::time::timeout(CATALOG_FETCH_TIMEOUT, fetch)
                        .await
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out"))),
                )
            };
            // Shards new to this pass are routed as they open, not when the
            // whole pass is done: a bulk create opens hundreds one after
            // another. Only additions: every other route and its priors stay
            // as last published, and only a fence's own publish stops a shard
            // being served, until the pass has applied its changes. Not
            // before the first catalog, which
            // would publish every remote shard without an address.
            let mut early = EarlyRoutes {
                ingress: &ingress,
                catalog: &catalog,
                published_priors: &published_priors,
                assignments: (catalog_source.is_none() || !catalog.is_empty())
                    .then(|| early_assignments(&applied, &assignments, &priors))
                    .flatten(),
                last: None,
            };
            let ((), fetched) = tokio::join!(
                crate::shards::lifecycle::reconcile_reporting(
                    &lifecycle,
                    store.as_ref(),
                    &assignments,
                    &renewals,
                    &mut early,
                ),
                refresh,
            );
            match fetched {
                Some(Ok(fetched)) => {
                    if let Some(endpoints) = &client_endpoints {
                        endpoints.refresh(&fetched);
                    }
                    catalog = fetched.nodes;
                }
                // The previous catalog is kept: a control-plane blip must not
                // erase every address this broker can forward to and turn a
                // healthy cluster into one that refuses every remote publish.
                Some(Err(err)) => {
                    tracing::warn!(error = %err, "node catalog refresh failed; keeping the last one");
                    crate::shards::watch::metrics::record_catalog_refresh_failure();
                }
                None => {}
            }

            // Read after reconcile, so an open that just finished is included:
            // publishing the routes first would advertise this node as the
            // owner of a shard it has not opened. Held through the publish: a
            // fence publishes under this lock too, and one finishing between
            // the read and the publish would be undone until the next pass.
            let table = routing_table_from(&assignments, &catalog).with_priors(priors.clone());
            {
                let owned = lifecycle.lock().await;
                ingress.publish(table, &catalog, owned.servable());
            }
            published_priors = priors;
            crate::shards::watch::metrics::set_catalog_nodes(catalog.len());
            if woken {
                routes_changed.notify_one();
            }
            applied = assignments;
        }
    })
}

/// The last applied assignments with this pass's new shards added, and the
/// new shards' priors; `None` when the pass names none.
fn early_assignments(
    applied: &HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    assignments: &HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    priors: &HashMap<felix_router::ShardKey, felix_router::PriorSet>,
) -> Option<(
    HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
    HashMap<felix_router::ShardKey, felix_router::PriorSet>,
)> {
    let added: HashMap<_, _> = assignments
        .iter()
        .filter(|(key, _)| !applied.contains_key(*key))
        .map(|(key, assignment)| (key.clone(), assignment.clone()))
        .collect();
    if added.is_empty() {
        return None;
    }
    let added_priors = added
        .keys()
        .map(to_router_key)
        .filter_map(|key| priors.get(&key).map(|prior| (key, prior.clone())))
        .collect();
    let mut routes = applied.clone();
    routes.extend(added);
    Some((routes, added_priors))
}

/// The view a reconcile pass publishes while it is still opening shards: the
/// live one, plus each new shard's route and every shard that now serves at
/// the generation routed here.
struct EarlyRoutes<'a> {
    ingress: &'a IngressRouter,
    catalog: &'a HashMap<String, felix_router::NodeRef>,
    /// The priors of the last end-of-pass publish.
    published_priors: &'a HashMap<felix_router::ShardKey, felix_router::PriorSet>,
    /// The last applied assignments with the new shards added, and the new
    /// shards' priors. `None` when there is nothing to add.
    assignments: Option<(
        HashMap<ShardKey, crate::shards::watch::ShardAssignment>,
        HashMap<felix_router::ShardKey, felix_router::PriorSet>,
    )>,
    last: Option<std::time::Instant>,
}

/// How often a pass republishes while it opens shards. Each publish rebuilds
/// the whole table, so a pass opening hundreds does not do it per shard.
const EARLY_PUBLISH_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

impl crate::shards::lifecycle::PassProgress for EarlyRoutes<'_> {
    fn due(&self) -> bool {
        self.assignments.is_some()
            && self
                .last
                .is_none_or(|last| last.elapsed() >= EARLY_PUBLISH_EVERY)
    }

    /// The pass's own publish at the end follows regardless, so one skipped
    /// here is only late.
    fn opened(&mut self, servable: ServableShards) {
        let Some((routes, added_priors)) = &self.assignments else {
            return;
        };
        self.last = Some(std::time::Instant::now());
        // Start from what is live, not from the last end-of-pass publish: a
        // fence finishing mid-pass publishes the servable set on its own, and
        // that adds and removes shards. Add every shard now serving at the
        // generation routed here; other removals wait for the pass's end.
        let mut early_servable = self.ingress.view.load().servable.clone();
        early_servable.extend(servable.into_iter().filter(|(key, generation)| {
            routes
                .get(key)
                .is_some_and(|assignment| assignment.generation == *generation)
        }));
        let mut priors = self.published_priors.clone();
        priors.extend(
            added_priors
                .iter()
                .map(|(key, prior)| (key.clone(), prior.clone())),
        );
        self.ingress.publish(
            routing_table_from(routes, self.catalog).with_priors(priors),
            self.catalog,
            early_servable,
        );
    }
}

#[cfg(test)]
mod tests;
