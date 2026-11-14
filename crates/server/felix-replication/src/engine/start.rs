//! Starting the hybrid engine: the replica handler that answers peers, and
//! the tasks a cluster member runs for replication.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::ShardRouter;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{Host, Hybrid};
use crate::driver::{self, Published, Replication, WriteFence};
use crate::election::{self, Elections};
use crate::peer::PeerRequester;
use crate::promotion::PromotionGate;
use crate::quorum::{QuorumMarks, ShardServing};
use crate::reporter::{ReportTo, Reporter};
use crate::suspicion::{self, Suspects, SuspicionConfig};
use crate::{MoveThrottle, RebuildPolicy, ReplicaHandler};

/// What answers a peer's replication requests ([`Hybrid::handle`]).
///
/// `leader_retry` is how soon a leader retries a copy that refused it: the
/// driver's tick.
pub fn replica_handler(
    broker: &Arc<Broker>,
    router: &Arc<ShardRouter>,
    marks: &Arc<QuorumMarks>,
    storage_id: felix_storage::StorageId,
    unclean: Option<Arc<crate::unclean::UncleanStart>>,
    confirmed: Option<Arc<dyn crate::confirmed::ConfirmedRoutes>>,
    leader_retry: Duration,
) -> ReplicaHandler {
    // A replica refuses a candidate for as long as it would take to suspect
    // its leader, so the two windows agree.
    let handler = ReplicaHandler::new(Arc::clone(broker), Arc::clone(router))
        .with_stickiness(SuspicionConfig::from_env().suspect_after)
        .with_fleet(Arc::clone(marks))
        .with_local_storage(storage_id)
        .with_replaced_storage(marks.replaced_storage());
    let handler = match unclean {
        Some(unclean) => handler.with_unclean_start(unclean),
        None => handler,
    };
    let handler = match confirmed {
        Some(confirmed) => handler.with_confirmed_routes(confirmed),
        None => handler,
    };
    handler.with_leader_retry(leader_retry)
}

/// The leaderships the replicas elect, only where this broker checks sets
/// (`offers_election`). The routing feed lays them over the control plane's,
/// so build them before the feed starts. `changed` wakes that feed.
pub fn elections(
    offers_election: bool,
    router: &ShardRouter,
    changed: Arc<Notify>,
    marks: &QuorumMarks,
) -> Option<Arc<Elections>> {
    offers_election.then(|| {
        let elections = Arc::new(
            Elections::new(router.local_node_id(), changed)
                .with_defer_for(SuspicionConfig::from_env().defer_for),
        );
        marks.set_elections(Arc::clone(&elections));
        elections
    })
}

/// Everything [`Hybrid::start`] runs replication from.
pub struct StartDeps<S: ?Sized, F: ?Sized, R, L> {
    pub requester: Arc<R>,
    pub broker: Arc<Broker>,
    pub router: Arc<ShardRouter>,
    /// The shard routing the write fence is paired with.
    pub serving: Arc<S>,
    pub fence: Arc<F>,
    pub gate: Arc<dyn PromotionGate>,
    pub published: Published,
    /// Where replica reports go. `None` outside a cluster, where nobody
    /// assigned this broker a shard to report on.
    pub report: Option<ReportTo<L>>,
    pub interval: Duration,
    pub routes_changed: Arc<Notify>,
    pub rebuild: RebuildPolicy,
    pub move_throttle: MoveThrottle,
    pub suspects: Arc<Suspects>,
    /// Until cancelled this broker only votes in elections.
    pub leads_after: Option<CancellationToken>,
    pub unclean: Option<Arc<crate::unclean::UncleanStart>>,
    /// The seal orders on shards this broker leads. `None` where nothing
    /// orders a seal; with no `report` there is nobody to answer one.
    pub seals: Option<Arc<dyn crate::sealing::SealOrders>>,
    pub shutdown: CancellationToken,
}

#[cfg(test)]
thread_local! {
    /// The components `Hybrid::start` spawned on this thread, in order.
    pub(super) static SPAWNED: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Note a spawn for the order test; nothing in a release build.
fn spawned(_name: &'static str) {
    #[cfg(test)]
    SPAWNED.with(|spawned| spawned.borrow_mut().push(_name));
}

/// What a started engine hands back.
pub struct Started {
    /// The driver's handle: shipping to followers.
    pub replication: Replication,
}

impl<S: ShardServing + ?Sized + 'static> Hybrid<S> {
    /// Start replication for the shards this broker leads or follows.
    ///
    /// The order is part of the contract: the replica reporter first (the
    /// driver is handed its handle), then the driver, then the suspicion
    /// heartbeat, then the election driver that stands on those suspicions.
    /// The caller builds [`elections`] before the routing feed and the
    /// promotion gate before this.
    pub fn start<F, R, L>(deps: StartDeps<S, F, R, L>) -> Started
    where
        F: WriteFence + 'static,
        R: PeerRequester + Send + Sync + 'static,
        L: crate::controlplane_link::ControlPlaneLink + Clone,
    {
        let StartDeps {
            requester,
            broker,
            router,
            serving,
            fence,
            gate,
            published,
            report,
            interval,
            routes_changed,
            rebuild,
            move_throttle,
            suspects,
            leads_after,
            unclean,
            seals,
            shutdown,
        } = deps;
        let marks = Arc::clone(&published.marks);
        if let (Some(to), Some(orders)) = (&report, seals) {
            spawned("sealing");
            drop(crate::sealing::spawn(
                crate::sealing::Sealing::new(
                    Arc::clone(&requester),
                    Arc::clone(&broker),
                    Arc::clone(&router),
                    orders,
                    crate::sealing::SealTo {
                        link: to.link.clone(),
                        node_id: to.node_id.clone(),
                        token: to.token.clone(),
                        held: to.held.clone(),
                        storage_id: to.storage_id,
                    },
                ),
                interval,
                shutdown.clone(),
            ));
        }
        let reporter = report.map(|to| {
            spawned("reporter");
            let (reporter, _task) = Reporter::spawn(to, shutdown.clone());
            reporter
        });
        spawned("driver");
        let replication = driver::spawn(
            Arc::clone(&requester),
            Arc::clone(&broker),
            Arc::clone(&router),
            Arc::new(Host::new(serving, fence)) as Arc<dyn WriteFence>,
            gate,
            published,
            reporter,
            interval,
            routes_changed,
            rebuild,
            move_throttle,
            shutdown.clone(),
        );
        spawned("suspicion");
        drop(suspicion::spawn(
            Arc::clone(&requester),
            Arc::clone(&router),
            Arc::clone(&suspects),
            SuspicionConfig::from_env(),
            shutdown.clone(),
        ));
        spawned("election");
        drop(election::spawn(
            requester,
            broker,
            router,
            marks,
            suspects,
            SuspicionConfig::from_env(),
            leads_after,
            unclean,
            shutdown,
        ));
        Started { replication }
    }
}
