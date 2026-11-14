//! A real broker's cluster machinery, run in-process for the simulator.
//!
//! This is the code `node::run_with_shutdown` runs, minus everything a
//! simulated cluster has no use for: the client listeners, metrics, Kafka,
//! MQTT, the key store and auth. The control plane and the peer transport are
//! injected, so the simulator can fault both.
//!
//! A broker starts in two phases because the simulated network needs the peer
//! handler before it can hand back the requester the shard tasks send
//! through:
//!
//! 1. [`SimBroker::open`] does what the node does up to and including
//!    storage, and builds the peer handler.
//! 2. [`OpenedBroker::run`] takes that requester and starts membership, the
//!    catalog sync, the shard tasks, suspicion and election.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, LogKind};
use felix_common::fleet::FleetGate;
use felix_replication::controlplane_link::ControlPlaneLink;
use felix_replication::peer::PeerRequester;
use felix_replication::peer::server::PeerRequestHandler;
use felix_replication::quorum::QuorumMarks;
use felix_replication::{self as replication};
use felix_router::ShardRouter;
use felix_wire::internal::PeerCapabilities;
use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{cluster, membership, startup};
use crate::cluster::catalog_sync::{self, CatalogFetch};
use crate::cluster::credential::NodeCredential;
use crate::cluster::lease::LeaseState;
use crate::cluster::membership::MembershipTask;
use crate::config::{BrokerConfig, DurableStorageConfig, MembershipConfig};
use crate::serving::auth::BrokerAuth;
use crate::shards::lifecycle::ShardLifecycle;
use crate::shards::routing::IngressRouter;
use crate::shards::watch::ShardOwnership;
pub use crate::shards::{ShardKey, ShardKind};

/// Reads one path of the control plane's catalog, in process: the body of
/// `GET <path>` as the catalog sync would receive it.
///
/// The paths are the ones `cluster::catalog_sync` fetches: for each of
/// `tenants`, `namespaces`, `caches` and `streams`, `/v1/<kind>/snapshot` and
/// `/v1/<kind>/changes?since=<seq>`. An `Err` is a failed fetch, which the
/// sync retries on its next poll as it does over HTTP.
pub type CatalogFetcher = Arc<dyn Fn(String) -> BoxFuture<'static, Result<Bytes>> + Send + Sync>;

struct FetcherSource(CatalogFetcher);

impl CatalogFetch for FetcherSource {
    async fn get(&self, path: &str, _bearer: Option<&str>) -> Result<Bytes> {
        (self.0)(path.to_string()).await
    }
}

/// What a simulated broker is configured with. Everything else is the real
/// broker's default.
#[derive(Debug, Clone)]
pub struct SimBrokerConfig {
    pub node_id: String,
    /// Durable storage root.
    pub data_dir: PathBuf,
    pub region: String,
    /// The address peers are told to reach this broker at. Never bound.
    pub advertise_addr: SocketAddr,
    /// The fleet features this broker reports. The default is what a broker
    /// with the fence and election on reports.
    pub features: BTreeSet<String>,
    /// How often the control plane is polled (`FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`).
    pub sync_interval: Duration,
    /// How the logs sync. Under anything but `OnCommit` a restart that finds
    /// the last run did not stop cleanly takes a new storage id on a platform
    /// that cannot sync the data directory's filesystem after a crash (all but
    /// Linux), which makes peers copy the whole log again.
    pub fsync_mode: felix_storage::log::FsyncMode,
    /// A segment size to use instead of the default, so a short run rolls
    /// its logs onto new segments.
    pub segment_size_bytes: Option<u64>,
}

impl SimBrokerConfig {
    /// A broker with the real defaults, the fence and election on.
    pub fn new(node_id: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        let node_id = node_id.into();
        Self {
            advertise_addr: SocketAddr::from(([10, 0, 0, 1], 5001)),
            region: "local".to_string(),
            features: crate::config::cluster_features(true, false, true),
            sync_interval: Duration::from_millis(
                BrokerConfig::default().controlplane_sync_interval_ms,
            ),
            fsync_mode: felix_storage::log::FsyncMode::None,
            segment_size_bytes: None,
            node_id,
            data_dir: data_dir.into(),
        }
    }

    fn broker_config(&self) -> BrokerConfig {
        let peer = felix_replication::peer::PeerTransportConfig {
            bind: self.advertise_addr,
            fence: true,
            election: true,
            // So a seed can be replayed with the kill switch thrown.
            unified_fence: std::env::var("FELIX_UNIFIED_FENCE").map_or(true, |v| v.trim() != "0"),
            ..Default::default()
        };
        BrokerConfig {
            controlplane_url: Some("sim://control-plane".to_string()),
            controlplane_token: "sim".to_string(),
            controlplane_sync_interval_ms: u64::try_from(self.sync_interval.as_millis())
                .unwrap_or(u64::MAX),
            membership: Some(MembershipConfig {
                node_id: self.node_id.clone(),
                advertise_addr: self.advertise_addr.to_string(),
                client_advertise_addr: None,
                kafka_advertise_addr: None,
                region: self.region.clone(),
                zone: None,
                region_bridges: Vec::new(),
                features: self.features.clone(),
            }),
            peer_transport: Some(peer),
            ..BrokerConfig::default()
        }
    }

    fn durable_config(&self) -> DurableStorageConfig {
        DurableStorageConfig {
            root: self.data_dir.clone(),
            log: felix_storage::log::LogConfig {
                fsync_mode: self.fsync_mode,
                segment_size_bytes: self
                    .segment_size_bytes
                    .unwrap_or(felix_storage::log::LogConfig::default().segment_size_bytes),
                ..Default::default()
            },
            offload_dir: None,
        }
    }
}

/// A broker opened over its storage with its peer handler built, waiting for
/// a peer requester to run on. See [`SimBroker::open`].
pub struct OpenedBroker<L> {
    config: BrokerConfig,
    link: Option<L>,
    credential: Option<NodeCredential>,
    catalog: CatalogFetcher,
    cluster: Option<cluster::ShardState>,
    broker: Arc<Broker>,
    durable_storage: Option<DurableStorage>,
    storage_id: felix_storage::StorageId,
    unclean: Option<Arc<replication::unclean::UncleanStart>>,
    lease: Arc<LeaseState>,
    fleet: Arc<FleetGate>,
    quorum_marks: Arc<QuorumMarks>,
    engine: Arc<crate::shards::routing::StreamEngine>,
    handler: Arc<dyn PeerRequestHandler>,
    prefetch: Option<replication::confirmed::Prefetch>,
    capabilities: PeerCapabilities,
    outgoing: PeerCapabilities,
    registered: tokio::sync::watch::Sender<Option<u64>>,
    registered_watch: tokio::sync::watch::Receiver<Option<u64>>,
}

/// A running simulated broker. Dropping it leaves its tasks running until
/// [`SimBroker::crash`] or the tokens they hold are cancelled.
pub struct SimBroker {
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    ingress: Arc<IngressRouter>,
    lifecycle: Arc<tokio::sync::Mutex<ShardLifecycle>>,
    ownership: Arc<tokio::sync::RwLock<ShardOwnership>>,
    quorum_marks: Arc<QuorumMarks>,
    engine: Arc<crate::shards::routing::StreamEngine>,
    lease: Arc<LeaseState>,
    fleet: Arc<FleetGate>,
    seeded: CancellationToken,
    incarnation: Arc<AtomicU64>,
    registered_watch: tokio::sync::watch::Receiver<Option<u64>>,
    storage_id: felix_storage::StorageId,
    started_unclean: bool,
    node_id: String,
    membership: Option<MembershipTask>,
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

/// What a broker believes about one shard, read without waiting on anything,
/// so a sample across brokers is one instant on a single-threaded runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStand {
    /// The broker would take a write for the shard right now: it routes the
    /// shard to itself, its fence is open at that generation and its lease is
    /// valid.
    pub serving: bool,
    /// The generation this broker leads the shard at, when the router says it
    /// leads it.
    pub generation: Option<u64>,
    /// The leader and generation in the routing table, whoever it is.
    pub routed_leader: Option<(String, u64)>,
    /// The generation the shard's lifecycle has open for writes (phase
    /// `active`), if it has one. The fence and the routing table are checked
    /// against it.
    pub opened_generation: Option<u64>,
    /// Writes to the shard at this broker's generation get in without the
    /// lease, because a majority of followers at that generation acknowledges
    /// them. A deposed leader of such a shard may keep `serving` until it
    /// learns of the newer generation, but it cannot get a majority to
    /// acknowledge once its successor has fenced one.
    pub acks_by_followers: bool,
}

/// What a broker believes about one shard, for checkers.
#[derive(Debug, Clone)]
pub struct ShardSnapshot {
    /// See [`ShardStand::serving`].
    pub serving: bool,
    /// See [`ShardStand::generation`].
    pub generation: Option<u64>,
    /// See [`ShardStand::routed_leader`].
    pub routed_leader: Option<(String, u64)>,
    /// See [`ShardStand::opened_generation`].
    pub opened_generation: Option<u64>,
    /// See [`ShardStand::acks_by_followers`].
    pub acks_by_followers: bool,
    /// One past the last record in this broker's copy of the log, `None` when
    /// it holds no copy or the copy is not open.
    pub log_tail: Option<u64>,
    /// How far a majority of the replica set has the log, when this broker
    /// tracks it (a leader does).
    pub quorum_mark: Option<u64>,
    /// The lifecycle phase, `None` when the lifecycle was busy.
    pub phase: Option<&'static str>,
}

impl SimBroker {
    /// Phase one: everything the node does up to and including opening
    /// storage, and the peer request handler.
    ///
    /// Asks the control plane (through `link`) for this node's last
    /// registration first, and keeps asking until it answers, as the node
    /// does. `catalog` is how the stream catalog is read; see
    /// [`CatalogFetcher`].
    pub async fn open<L: ControlPlaneLink + Clone>(
        config: SimBrokerConfig,
        link: L,
        catalog: CatalogFetcher,
    ) -> Result<OpenedBroker<L>> {
        let broker_config = config.broker_config();
        let peer = broker_config
            .peer_transport
            .clone()
            .expect("the config above has a peer transport");
        let cluster = cluster::shard_state(&broker_config);
        let lease = Arc::new(membership::initial_lease());
        let fleet = Arc::new(FleetGate::new(config.features.clone()));
        if let Some((_, ingress, _, _)) = &cluster {
            ingress.fence().bind_lease(Arc::clone(&lease));
        }
        let quorum_marks = Arc::new(QuorumMarks::with_fleet(Arc::clone(&fleet)));
        let engine = Arc::new(crate::shards::routing::StreamEngine::new(
            Some(Arc::clone(&quorum_marks)),
            cluster
                .as_ref()
                .map(|(_, ingress, _, _)| Arc::clone(ingress)),
        ));
        let credential = Some(NodeCredential::new("sim"));
        let link = Some(link);
        let registered = membership::await_registered(&broker_config, &link, &credential).await?;
        let startup::Opened {
            broker,
            durable_storage,
            storage_id,
            unclean,
        } = startup::open_broker(
            &broker_config,
            Some(config.durable_config()),
            registered.as_ref(),
            &cluster,
            &fleet,
            &quorum_marks,
            &engine,
        )
        .await
        .context("open the broker")?;
        super::gate_record_generations(&broker_config, &broker, &fleet);
        let (router, _, _, _) = cluster.as_ref().expect("a cluster member has shard state");
        // Peers are not authenticated in the simulator; nothing here presents
        // a client credential.
        let auth = Arc::new(BrokerAuth::new("http://sim.invalid".to_string()));
        let (handler, prefetch) = cluster::peer_handler(
            &broker_config,
            router,
            &engine,
            &broker,
            &quorum_marks,
            &auth,
            storage_id,
            unclean.clone(),
            // Through the same link as every other control-plane call, so a
            // learner is backed here as in a broker.
            link.clone().map(|link| {
                Arc::new(crate::shards::ControlPlaneRoutes::new(
                    link,
                    credential.clone(),
                )) as Arc<dyn replication::confirmed::ConfirmedRoutes>
            }),
        );
        let handler = Arc::new(handler);
        let (registered, registered_watch) = tokio::sync::watch::channel(None);
        Ok(OpenedBroker {
            capabilities: peer.capabilities(),
            outgoing: peer.outgoing_capabilities(),
            config: broker_config,
            link,
            credential,
            catalog,
            cluster,
            broker,
            durable_storage,
            storage_id,
            unclean,
            lease,
            fleet,
            quorum_marks,
            engine,
            handler,
            prefetch,
            registered,
            registered_watch,
        })
    }
}

impl<L: ControlPlaneLink + Clone> OpenedBroker<L> {
    /// What answers a peer's requests to this broker.
    pub fn handler(&self) -> Arc<dyn PeerRequestHandler> {
        Arc::clone(&self.handler)
    }

    /// What this broker offers a peer in the handshake, the fence and
    /// election included.
    pub fn capabilities(&self) -> PeerCapabilities {
        self.capabilities
    }

    /// What this broker offers when it dials a peer.
    pub fn outgoing_capabilities(&self) -> PeerCapabilities {
        self.outgoing
    }

    /// The storage identity a peer handshake announces.
    pub fn storage_id(&self) -> felix_storage::StorageId {
        self.storage_id
    }

    /// The node incarnation a peer handshake announces: `None` until the
    /// broker registers.
    pub fn incarnation_watch(&self) -> tokio::sync::watch::Receiver<Option<u64>> {
        self.registered_watch.clone()
    }

    /// Phase two: start membership, the catalog sync, shard ownership,
    /// replication, suspicion and election over `peers`, which must report
    /// `offers_fence()` and `offers_election()`.
    ///
    /// Membership registers once the catalog has been applied, as in the node.
    pub fn run<R: PeerRequester + Send + Sync + 'static>(self, peers: Arc<R>) -> SimBroker {
        let Self {
            config,
            link,
            credential,
            catalog,
            cluster,
            broker,
            durable_storage,
            storage_id,
            unclean,
            lease,
            fleet,
            quorum_marks,
            engine,
            prefetch,
            registered,
            registered_watch,
            handler: _,
            capabilities: _,
            outgoing: _,
        } = self;
        let started_unclean = unclean.is_some();
        let node_id = config
            .membership
            .as_ref()
            .map(|membership| membership.node_id.clone())
            .unwrap_or_default();
        let peers = Some(peers);
        let shutdown = CancellationToken::new();
        let seeded = CancellationToken::new();
        let halted_replicas = Arc::new(replication::halted::HaltedReplicas::new());
        let shard_status = Arc::new(replication::status::ShardStatusBoard::new());
        let client_endpoints = Arc::new(crate::cluster::client_endpoints::ClientEndpoints::new());
        let suspects = Arc::new(replication::suspicion::Suspects::default());
        let incarnation = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();

        if let Err(err) =
            startup::bind_read_check(&config, &cluster, &peers, &broker, &quorum_marks, || {
                Ok(false)
            })
        {
            tracing::error!(error = %err, "could not bind the read check");
        }

        // The node's catalog sync, over the injected fetcher. Seeded is
        // released only after the first pass applied everything.
        let (applied_tx, applied_rx) = tokio::sync::oneshot::channel();
        tasks.push(tokio::spawn({
            let broker = Arc::clone(&broker);
            let shutdown = shutdown.clone();
            let interval = Duration::from_millis(config.controlplane_sync_interval_ms);
            async move {
                let source = FetcherSource(catalog);
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    result = catalog_sync::sync_loop(broker, &source, interval, Some(applied_tx), None, Default::default(), None) => {
                        if let Err(err) = result {
                            tracing::warn!(error = %err, "catalog sync exited");
                        }
                    }
                }
            }
        }));
        tasks.push(tokio::spawn({
            let seeded = seeded.clone();
            async move {
                if applied_rx.await.is_ok() {
                    seeded.cancel();
                }
            }
        }));
        tasks.push(tokio::spawn(super::cache_expiry::run(
            Arc::clone(&broker),
            cluster
                .as_ref()
                .map(|(_, ingress, _, _)| Arc::clone(ingress)),
            super::cache_expiry::INTERVAL,
            shutdown.clone(),
        )));

        let membership = membership::spawn(
            &config,
            link.clone(),
            false,
            true,
            &seeded,
            &Some(Arc::clone(&lease)),
            &fleet,
            &credential,
            &suspects,
            &incarnation,
            registered,
            storage_id,
            durable_storage.as_ref().map(DurableStorage::root),
            durable_storage.as_ref().map(|storage| {
                crate::cluster::controlplane_view::ViewFile::new(storage.root().to_path_buf())
            }),
            None,
            &shutdown,
        );
        let shard_tasks = cluster::spawn_shard_tasks(cluster::ShardTaskDeps {
            config: &config,
            cluster: &cluster,
            durable_storage: &durable_storage,
            link: &link,
            credential: &credential,
            client_endpoints: &client_endpoints,
            peers: &peers,
            broker: &broker,
            engine: &engine,
            quorum_marks: &quorum_marks,
            halted_replicas: &halted_replicas,
            shard_status: &shard_status,
            fleet: &fleet,
            suspects: &suspects,
            incarnation: &incarnation,
            storage_id: durable_storage.as_ref().map(|_| storage_id),
            unclean: &unclean,
            prefetch,
            engine_shards: None,
            sync_shutdown: &shutdown,
            nodes: Default::default(),
            reports_held: None,
            leads_after: None,
        });
        if let Some(shard_tasks) = shard_tasks {
            tasks.push(shard_tasks.watch);
            tasks.push(shard_tasks.feed);
            // The driver stops on `shutdown`, which it was given.
            drop(shard_tasks.replication);
        }
        let (router, ingress, lifecycle, ownership) =
            cluster.expect("a cluster member has shard state");
        SimBroker {
            broker,
            router,
            ingress,
            lifecycle,
            ownership,
            quorum_marks,
            engine,
            lease,
            fleet,
            seeded,
            incarnation,
            registered_watch,
            storage_id,
            started_unclean,
            node_id,
            membership,
            shutdown,
            tasks,
        }
    }
}

impl SimBroker {
    pub fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }

    pub fn router(&self) -> &Arc<ShardRouter> {
        &self.router
    }

    /// The router every write enters, with the shard fence.
    pub fn ingress(&self) -> &Arc<IngressRouter> {
        &self.ingress
    }

    pub fn lifecycle(&self) -> &Arc<tokio::sync::Mutex<ShardLifecycle>> {
        &self.lifecycle
    }

    pub fn ownership(&self) -> &Arc<tokio::sync::RwLock<ShardOwnership>> {
        &self.ownership
    }

    /// The engine every path of this broker shares.
    pub fn engine(&self) -> &Arc<crate::shards::routing::StreamEngine> {
        &self.engine
    }

    pub fn quorum_marks(&self) -> &Arc<QuorumMarks> {
        &self.quorum_marks
    }

    pub fn lease(&self) -> &Arc<LeaseState> {
        &self.lease
    }

    pub fn fleet(&self) -> &Arc<FleetGate> {
        &self.fleet
    }

    /// Cancelled once the catalog is applied and the logs recovered; the
    /// broker registers after it.
    pub fn seeded(&self) -> &CancellationToken {
        &self.seeded
    }

    /// The incarnation membership last registered under, 0 before it has.
    pub fn incarnation(&self) -> u64 {
        self.incarnation.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn incarnation_watch(&self) -> tokio::sync::watch::Receiver<Option<u64>> {
        self.registered_watch.clone()
    }

    pub fn storage_id(&self) -> felix_storage::StorageId {
        self.storage_id
    }

    /// Whether this start followed a stop that may have cost acknowledged
    /// records, so the broker runs with its unclean-start handling.
    pub fn started_unclean(&self) -> bool {
        self.started_unclean
    }

    /// What this broker attests of its copy of `key` to a unified fence, the
    /// answer a promotion counts. `None` when it holds no copy open; it never
    /// opens one, so asking cannot change what is being checked.
    pub async fn copy_attestation(
        &self,
        key: &ShardKey,
    ) -> Option<felix_wire::internal::FenceAttestation> {
        let kind = match key.kind {
            ShardKind::Stream => LogKind::Stream,
            ShardKind::Cache => LogKind::Cache,
        };
        let log = self
            .broker
            .opened_log(
                kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
            )
            .await?;
        let route = felix_router::ShardKey {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
            kind: match key.kind {
                ShardKind::Stream => felix_router::ShardKind::Stream,
                ShardKind::Cache => felix_router::ShardKind::Cache,
            },
        };
        let readmitted = self
            .router
            .snapshot()
            .get(&route)
            .and_then(|route| route.readmitted.clone())
            .unwrap_or_default();
        Some(
            self.quorum_marks
                .attest(&route, &log, &self.node_id, readmitted.as_slice())
                .await,
        )
    }

    /// Whether `key` has a copy on disk, open or not. A fence would open an
    /// unopened one to answer; asking here does not.
    pub fn holds_copy_on_disk(&self, key: &ShardKey) -> bool {
        let kind = match key.kind {
            ShardKind::Stream => LogKind::Stream,
            ShardKind::Cache => LogKind::Cache,
        };
        self.broker
            .copy_on_disk(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
    }

    /// Whether a fence would find the copy of `key` untrusted without reading
    /// its log. See [`QuorumMarks::surely_untrusted`].
    pub fn copy_surely_untrusted(&self, key: &ShardKey) -> bool {
        let route = felix_router::ShardKey {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            shard: key.shard,
            kind: match key.kind {
                ShardKind::Stream => felix_router::ShardKind::Stream,
                ShardKind::Cache => felix_router::ShardKind::Cache,
            },
        };
        self.quorum_marks.surely_untrusted(&route)
    }

    /// The membership task, `None` only if the config named no cluster.
    pub fn membership(&self) -> Option<&MembershipTask> {
        self.membership.as_ref()
    }

    /// The highest generation a leader of `key` was accepted at on this
    /// broker's open copy, read without awaiting. `None` when no copy is open.
    pub fn accepted_generation(&self, key: &ShardKey) -> Option<u64> {
        use std::future::Future;
        let kind = match key.kind {
            ShardKind::Stream => LogKind::Stream,
            ShardKind::Cache => LogKind::Cache,
        };
        let open = self.broker.opened_log(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            None,
        );
        // A stream shard's lookup never waits. A cache shard's may be pending,
        // which reads as no generation and leaves the old leader a chance, so
        // the checker errs toward failing.
        let mut open = std::pin::pin!(open);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match open.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(log) => log.map(|log| log.accepted_generation()),
            std::task::Poll::Pending => None,
        }
    }

    /// What this broker believes about `key` right now, without awaiting.
    pub fn shard_stand(&self, key: &ShardKey) -> ShardStand {
        use crate::shards::lifecycle::Phase;
        use crate::shards::routing::Dispatch;
        let generation = match self.ingress.dispatch(key) {
            Dispatch::Local { generation } => Some(generation),
            _ => None,
        };
        // The check a write makes on entry; the guard is dropped at once.
        let serving = generation
            .is_some_and(|generation| self.ingress.fence().admit(key, generation).is_ok());
        let routed_leader = self
            .ingress
            .route(key)
            .map(|route| (route.leader.node_id, route.generation));
        // The fence mirrors the lifecycle's phase, so this needs no lock.
        let opened_generation = self
            .ingress
            .fence()
            .phase_of(key)
            .filter(|record| record.phase == Phase::Active)
            .map(|record| record.generation);
        // The shard's own gate, not the fleet's features: a shard opened
        // before the fleet finalized election keeps the lease check.
        let acks_by_followers =
            generation.is_some_and(|generation| self.ingress.fence().lease_free(key, generation));
        ShardStand {
            serving,
            generation,
            routed_leader,
            opened_generation,
            acks_by_followers,
        }
    }

    /// What this broker believes about `key` right now.
    pub async fn shard_snapshot(&self, key: &ShardKey) -> ShardSnapshot {
        let ShardStand {
            serving,
            generation,
            routed_leader,
            opened_generation,
            acks_by_followers,
        } = self.shard_stand(key);
        let kind = match key.kind {
            ShardKind::Stream => LogKind::Stream,
            ShardKind::Cache => LogKind::Cache,
        };
        // Read-only: a census must not open or recover a log it is observing.
        let log_tail = match self
            .broker
            .opened_log(
                kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
            )
            .await
        {
            Some(log) => log.tail_offset().await.ok(),
            None => None,
        };
        let quorum_mark =
            generation.and_then(|generation| self.quorum_marks.offset(key, generation));
        let phase = self
            .lifecycle
            .try_lock()
            .ok()
            .map(|lifecycle| lifecycle.phase(key).label());
        ShardSnapshot {
            serving,
            generation,
            routed_leader,
            opened_generation,
            acks_by_followers,
            log_tail,
            quorum_mark,
            phase,
        }
    }

    /// Stop as a process crash does: cancel every token, abort every task and drop
    /// the broker with no drain, deregistration or handoff. The data directory
    /// can then be opened by a new [`SimBroker::open`].
    ///
    /// Tasks spawned deeper down (replication, the lease refresh) end when the
    /// token fires and may outlive this call by a poll; a requester that
    /// refuses their requests, as the simulated network does, is enough.
    pub fn crash(self) {
        self.halt();
    }

    /// Stop every task as [`SimBroker::crash`] does, without giving up the
    /// broker, for a caller that holds only a reference at the moment the
    /// process dies, such as a power cut inside a write. A task that is
    /// running finishes its current poll.
    pub fn halt(&self) {
        self.shutdown.cancel();
        if let Some(membership) = &self.membership {
            membership.handle.abort();
        }
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
