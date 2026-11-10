//! A cluster member's shard ownership, its peer transport, and the tasks that
//! keep ownership and replication in step with the control plane.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_broker::{Broker, DurableStorage};
use felix_router::ShardRouter;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::peer_dispatch;
use crate::cluster::client_endpoints::ClientEndpoints;
use crate::cluster::credential::NodeCredential;
use crate::config::BrokerConfig;
use crate::serving::auth::BrokerAuth;
use crate::shards::{lifecycle as shard_lifecycle, routing as shard_routing, watch as shard_watch};
use felix_replication::controlplane_link::ControlPlaneLink;
use felix_replication::peer::{self, PeerPool, PeerRequester, tls::PeerTls};
use felix_replication::{self as replication, halted::HaltedReplicas, quorum::QuorumMarks};

/// The router, ingress router, shard lifecycle and ownership of a cluster member.
pub(super) type ShardState = (
    Arc<ShardRouter>,
    Arc<shard_routing::IngressRouter>,
    Arc<tokio::sync::Mutex<shard_lifecycle::ShardLifecycle>>,
    Arc<tokio::sync::RwLock<shard_watch::ShardOwnership>>,
);

/// Cluster ownership, when this broker has an identity. Built before the
/// accept loop because the publish path consults it, and `None` on a
/// single-node broker so that path is a null check.
pub(super) fn shard_state(config: &BrokerConfig) -> Option<ShardState> {
    config.membership.as_ref().map(|membership| {
        let router = Arc::new(felix_router::ShardRouter::new(
            membership.node_id.clone(),
            membership.region.clone(),
            felix_router::RegionRouter::with_bridges(
                membership.region.clone(),
                membership.region_bridges.iter().cloned(),
            ),
        ));
        let lifecycle = shard_lifecycle::ShardLifecycle::new(membership.node_id.clone());
        // One fence, shared: the lifecycle opens and closes it, every write
        // path enters it through the ingress router.
        let ingress = Arc::new(
            shard_routing::IngressRouter::new(Arc::clone(&router), Arc::clone(lifecycle.fence()))
                .with_move_hold(shard_routing::hold::MoveHold::new(
                    Duration::from_millis(config.shard_move_hold_ms),
                    config.shard_move_hold_max,
                )),
        );
        let lifecycle = Arc::new(tokio::sync::Mutex::new(lifecycle));
        let ownership = Arc::new(tokio::sync::RwLock::new(
            shard_watch::ShardOwnership::default(),
        ));
        (router, ingress, lifecycle, ownership)
    })
}

/// Outbound peer connections, and the mTLS identity both ends of the peer
/// transport use.
///
/// Built before the accept loop because the publish path forwards through the
/// pool, for the same reason the router is: a publish must never arrive at a
/// broker that can resolve a remote owner and not reach it.
pub(super) fn connect_peers(
    config: &BrokerConfig,
    peer_shutdown: &CancellationToken,
) -> Result<Peers> {
    // One identity for both ends of the peer transport, so a rotation
    // reaches the listener and the dialler together. Loaded before either
    // binds: unreadable key material is a misconfiguration to refuse at
    // startup, not a handshake to fail later.
    let peer_tls = match config
        .peer_transport
        .as_ref()
        .and_then(|peer| peer.tls.as_ref())
    {
        Some(paths) => {
            let tls = Arc::new(peer::tls::PeerTls::load(paths).context("load peer mTLS material")?);
            drop(Arc::clone(&tls).spawn_reload(peer_shutdown.clone()));
            Some(tls)
        }
        None => None,
    };
    let peers = match (&config.peer_transport, &config.membership) {
        (Some(peer_config), Some(membership_config)) => Some(
            peer::PeerPool::new_with_tls(
                membership_config.node_id.clone(),
                peer::PeerTransportConfig {
                    forward_timeout: config.peer_forward_timeout(),
                    ..peer_config.clone()
                },
                peer_shutdown.clone(),
                peer_tls.clone(),
            )
            .context("bind peer transport")?,
        ),
        _ => None,
    };
    Ok(Peers {
        tls: peer_tls,
        pool: peers,
    })
}

/// The peer mTLS identity and the outbound pool, each absent when not configured.
pub(super) struct Peers {
    pub(super) tls: Option<Arc<PeerTls>>,
    pub(super) pool: Option<Arc<PeerPool>>,
}

/// The broker-internal listener's task, and what lets the routing feed start
/// a copy's confirmed read early. Both absent outside a cluster.
pub(super) struct PeerListener {
    pub(super) task: Option<JoinHandle<()>>,
    pub(super) prefetch: Option<replication::confirmed::Prefetch>,
}

/// Bind and serve the broker-internal listener, when this broker is in a
/// cluster. This is the address `NodeSpec.advertise_addr` names, so it must be
/// up for peers to reach this node at all.
#[allow(clippy::too_many_arguments)]
pub(super) fn bind_peer_listener(
    config: &BrokerConfig,
    cluster: &Option<ShardState>,
    broker: &Arc<Broker>,
    quorum_marks: &Arc<QuorumMarks>,
    auth: &Arc<BrokerAuth>,
    peer_tls: &Option<Arc<PeerTls>>,
    known_capabilities: Option<peer::KnownCapabilities>,
    storage_id: felix_storage::StorageId,
    incarnation: tokio::sync::watch::Receiver<Option<u64>>,
    unclean: Option<Arc<replication::unclean::UncleanStart>>,
    controlplane: Option<(&reqwest::Client, &str, &Option<NodeCredential>)>,
    clients: Option<CancellationToken>,
    serving: Option<&CancellationToken>,
    peer_shutdown: &CancellationToken,
) -> Result<PeerListener> {
    Ok(
        match (&config.peer_transport, &config.membership, cluster) {
            (Some(peer_config), Some(membership_config), Some((router, ingress, _, _))) => {
                let (handler, prefetch) = peer_handler(
                    config,
                    router,
                    ingress,
                    broker,
                    quorum_marks,
                    auth,
                    storage_id,
                    unclean,
                    controlplane.map(|(client, base_url, credential)| {
                        Arc::new(crate::shards::ControlPlaneRoutes::new(
                            replication::controlplane_link::HttpControlPlaneLink::new(
                                client.clone(),
                                base_url,
                            ),
                            credential.clone(),
                        ))
                            as Arc<dyn replication::confirmed::ConfirmedRoutes>
                    }),
                );
                let handler = match clients {
                    Some(clients) => handler.serving_clients_after(clients),
                    None => handler,
                };
                let server = peer::PeerServer::bind_with_tls(
                    membership_config.node_id.clone(),
                    peer_config,
                    Arc::new(handler),
                    peer_tls.clone(),
                )
                .context("bind broker-internal listener")?;
                // Shared with the pool, so a leader knows what a replica that
                // only ever dialled it can do.
                let server = match known_capabilities {
                    Some(known) => server.with_known_capabilities(known),
                    None => server,
                }
                .with_storage_identity(storage_id)
                .with_node_incarnation(incarnation);
                let server = match serving {
                    Some(serving) => server.with_serving(serving.clone()),
                    None => server,
                };
                match &peer_config.tls {
                    Some(tls) => tracing::info!(
                        addr = %server.local_addr()?,
                        ca = %tls.ca_path,
                        "broker-internal listener started; peers must present a certificate \
                         from this CA issued to their node id",
                    ),
                    None => tracing::warn!(
                        addr = %server.local_addr()?,
                        "broker-internal listener started WITHOUT peer authentication: \
                         anything that can reach it is a peer. Set FELIX_INTERNAL_TLS_CERT, \
                         FELIX_INTERNAL_TLS_KEY and FELIX_INTERNAL_TLS_CA, or keep the port \
                         reachable from brokers only; see docs/internal-protocol.md",
                    ),
                }
                PeerListener {
                    task: Some(tokio::spawn(server.serve(peer_shutdown.clone()))),
                    prefetch,
                }
            }
            _ => PeerListener {
                task: None,
                prefetch: None,
            },
        },
    )
}

/// What answers every request a peer sends this broker, and the handle that
/// starts a copy's confirmed read early. The listener and the simulator's
/// in-memory network both serve this.
#[allow(clippy::too_many_arguments)]
pub(super) fn peer_handler(
    config: &BrokerConfig,
    router: &Arc<ShardRouter>,
    ingress: &Arc<shard_routing::IngressRouter>,
    broker: &Arc<Broker>,
    quorum_marks: &Arc<QuorumMarks>,
    auth: &Arc<BrokerAuth>,
    storage_id: felix_storage::StorageId,
    unclean: Option<Arc<replication::unclean::UncleanStart>>,
    confirmed: Option<Arc<dyn replication::confirmed::ConfirmedRoutes>>,
) -> (
    peer_dispatch::BrokerPeerHandler<shard_routing::StreamEngine>,
    Option<replication::confirmed::Prefetch>,
) {
    let replicas = replication::engine::replica_handler(
        broker,
        router,
        quorum_marks,
        storage_id,
        unclean,
        confirmed,
        // A leader retries a copy that refused it on its driver tick.
        Duration::from_millis(config.controlplane_sync_interval_ms),
    );
    let engine =
        shard_routing::StreamEngine::new(Some(Arc::clone(quorum_marks)), Some(Arc::clone(ingress)))
            .with_replica(replicas);
    let prefetch = engine.confirmed_prefetch();
    let advertise_addr = config
        .membership
        .as_ref()
        .map(|membership| membership.advertise_addr.clone())
        .unwrap_or_default();
    let handler = peer_dispatch::BrokerPeerHandler::new(
        crate::serving::forward::ForwardingHandler::new(
            Arc::clone(broker),
            Arc::clone(ingress),
            Arc::clone(router),
            advertise_addr,
            Some(Arc::clone(quorum_marks)),
            Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
            Arc::clone(auth),
        ),
        engine,
    );
    (handler, prefetch)
}

/// The shard watch, the routing feed, and shipping to followers.
pub(super) struct ShardTasks {
    pub(super) watch: JoinHandle<()>,
    pub(super) feed: JoinHandle<()>,
    /// Absent without a peer transport to ship over.
    pub(super) replication: Option<replication::driver::Replication>,
}

/// What the shard tasks read and publish to. Generic over the peer transport
/// so a simulator can run these tasks over an in-memory network; a broker
/// passes its [`PeerPool`].
pub(super) struct ShardTaskDeps<'a, R, L> {
    pub(super) config: &'a BrokerConfig,
    pub(super) cluster: &'a Option<ShardState>,
    pub(super) durable_storage: &'a Option<DurableStorage>,
    /// The control plane, over HTTP in a broker and in-process in a simulator.
    pub(super) link: &'a Option<L>,
    pub(super) credential: &'a Option<NodeCredential>,
    pub(super) client_endpoints: &'a Arc<ClientEndpoints>,
    pub(super) peers: &'a Option<Arc<R>>,
    pub(super) broker: &'a Arc<Broker>,
    pub(super) quorum_marks: &'a Arc<QuorumMarks>,
    pub(super) halted_replicas: &'a Arc<HaltedReplicas>,
    pub(super) shard_status: &'a Arc<replication::status::ShardStatusBoard>,
    pub(super) fleet: &'a Arc<felix_common::fleet::FleetGate>,
    pub(super) suspects: &'a Arc<replication::suspicion::Suspects>,
    /// This process's node incarnation, which membership sets once it
    /// registers. Reports carry it.
    pub(super) incarnation: &'a Arc<std::sync::atomic::AtomicU64>,
    /// This broker's storage id, with durable storage. Reports carry it.
    pub(super) storage_id: Option<felix_storage::StorageId>,
    /// Set after a start that rotated the storage id; see
    /// `replication::unclean`.
    pub(super) unclean: &'a Option<Arc<replication::unclean::UncleanStart>>,
    /// Starts a copy's confirmed read when the routes first name it.
    pub(super) prefetch: Option<replication::confirmed::Prefetch>,
    /// Node addresses from the control-plane view, routed by until the
    /// first node catalog read.
    pub(super) nodes: felix_common::random::HashMap<String, felix_router::NodeRef>,
    /// After a start from the view, replica reports wait for the restore
    /// check: they announce this directory's storage id as current.
    pub(super) reports_held: Option<CancellationToken>,
    /// After a start from the view: this broker stands for election only once
    /// cancelled, since peers suspect it until it has registered.
    pub(super) leads_after: Option<CancellationToken>,
    pub(super) sync_shutdown: &'a CancellationToken,
}

/// Shard ownership: follow the control plane's assignments, and keep local
/// state and the routing table in step with them. Also starts shipping to
/// followers for the shards this broker leads.
pub(super) fn spawn_shard_tasks<R, L>(deps: ShardTaskDeps<'_, R, L>) -> Option<ShardTasks>
where
    R: PeerRequester + Send + Sync + 'static,
    L: ControlPlaneLink + Clone,
{
    let ShardTaskDeps {
        config,
        cluster,
        durable_storage,
        link,
        credential,
        client_endpoints,
        peers,
        broker,
        quorum_marks,
        halted_replicas,
        shard_status,
        fleet,
        suspects,
        incarnation,
        storage_id,
        unclean,
        prefetch,
        nodes,
        reports_held,
        leads_after,
        sync_shutdown,
    } = deps;
    match (cluster, link, durable_storage) {
        (Some((router, ingress, lifecycle, ownership)), Some(link), storage) => {
            let readers = shard_lifecycle::ShardReaders::new(Arc::clone(broker))
                .with_endpoints(Arc::clone(client_endpoints));
            // Ends readers on a lapsed lease, or, where they need none, once
            // this broker learns it was deposed.
            tokio::spawn(readers.clone().watch_leadership(
                Arc::clone(ingress.fence()),
                Arc::clone(quorum_marks),
                Arc::clone(lifecycle),
                sync_shutdown.clone(),
            ));
            // A promoted shard waits for replication to fence its replicas,
            // so only where replication runs: a peer transport to fence over,
            // and durable logs to fence.
            let gate: Arc<dyn replication::promotion::PromotionGate> = match (peers, storage) {
                // A broker with the fence turned off is an older broker to its
                // peers, and leads like one too.
                (Some(pool), Some(storage)) if pool.offers_fence() => match lifecycle.try_lock() {
                    Ok(mut guard) => {
                        guard.fence_promotions();
                        // A broker that checks sets fences the next
                        // generation too, once `joint_fence` is finalized.
                        if pool.offers_election() {
                            guard.fence_next_generations(Arc::clone(fleet));
                        }
                        Arc::new(shard_lifecycle::promotion::LifecycleGate::new(
                            Arc::clone(lifecycle),
                            Arc::clone(ingress),
                            Arc::new(storage.clone()),
                            Arc::clone(broker),
                            Arc::clone(fleet),
                        ))
                    }
                    Err(_) => {
                        tracing::warn!(
                            "shard lifecycle busy at startup; promotions open on the lease"
                        );
                        Arc::new(replication::promotion::NoGate)
                    }
                },
                _ => Arc::new(replication::promotion::NoGate),
            };
            let store: Arc<dyn shard_lifecycle::ShardStore> = match storage {
                Some(storage) => {
                    let store = shard_lifecycle::DurableShardStore::new(Arc::new(storage.clone()))
                        .with_readers(readers)
                        .with_generation_starts(shard_lifecycle::GenerationStarts {
                            broker: Arc::clone(broker),
                            fleet: Arc::clone(fleet),
                        })
                        .with_fence(Arc::clone(ingress.fence()));
                    let store = match unclean {
                        Some(unclean) => store.with_unclean_start(Arc::clone(unclean)),
                        None => store,
                    };
                    // Ballots ride with the fence, as its peers are told
                    // (`PeerCapabilities::BALLOTS`).
                    Arc::new(match (&config.peer_transport, &config.membership) {
                        (Some(peer), Some(membership)) if peer.fence => {
                            store.with_ballots(membership.node_id.clone())
                        }
                        _ => store,
                    })
                }
                // Without durable storage there is no log to open, so taking a
                // shard is bookkeeping only.
                None => Arc::new(shard_lifecycle::EphemeralShardStore::with_readers(readers)),
            };
            // The watch wakes the feed, and the feed wakes replication, so a
            // fence or a cut-over runs through all three as soon as it is
            // written rather than a tick apart at each hop.
            let assignments_changed = Arc::new(tokio::sync::Notify::new());
            let routes_changed = Arc::new(tokio::sync::Notify::new());
            // Leaderships the replicas elect, only where this broker checks
            // sets: the routing feed lays them over the control plane's.
            let elections = replication::engine::elections(
                peers.as_ref().is_some_and(|pool| pool.offers_election()),
                router,
                Arc::clone(&assignments_changed),
                quorum_marks,
            );
            let watch = tokio::spawn(shard_watch::run(
                link.clone(),
                // The assignment feed is cluster metadata, so it is read with
                // the same credential the rest of membership uses.
                credential.clone(),
                Arc::clone(ownership),
                Arc::clone(&assignments_changed),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                ingress.fence().lease().cloned(),
                sync_shutdown.clone(),
            ));
            let feed = shard_routing::spawn_feed(
                shard_routing::FeedState {
                    ownership: Arc::clone(ownership),
                    lifecycle: Arc::clone(lifecycle),
                    store,
                    ingress: Arc::clone(ingress),
                    client_endpoints: Some(Arc::clone(client_endpoints)),
                    assignments_changed,
                    routes_changed: Arc::clone(&routes_changed),
                    joint_fence: Some(Arc::clone(fleet)),
                    prefetch,
                    elections,
                    nodes,
                },
                Some(shard_routing::CatalogSource {
                    link: link.clone(),
                    token: credential.clone(),
                }),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                sync_shutdown.clone(),
            );
            // Shipping to followers, for the shards this broker leads, the
            // suspicion heartbeat over the same pool and the election driver
            // standing on it. Only when there is a peer transport: without
            // one the replica set is a plan nobody can act on.
            let replication = peers.as_ref().map(|pool| {
                replication::engine::Hybrid::<shard_routing::IngressRouter>::start(
                    replication::engine::StartDeps {
                        requester: Arc::clone(pool),
                        broker: Arc::clone(broker),
                        router: Arc::clone(router),
                        serving: Arc::clone(ingress),
                        fence: Arc::clone(ingress.fence()),
                        gate: Arc::clone(&gate),
                        published: replication::driver::Published {
                            marks: Arc::clone(quorum_marks),
                            halted: Arc::clone(halted_replicas),
                            status: Arc::clone(shard_status),
                        },
                        // Only a broker that is a cluster member reports: the
                        // report is about shards the control plane assigned,
                        // and a broker it has never registered leads none of
                        // them. Reports from every shard in a pass share one
                        // request; see `replication::reporter`.
                        report: config.membership.as_ref().map(|membership| {
                            replication::reporter::ReportTo {
                                link: link.clone(),
                                node_id: membership.node_id.clone(),
                                token: credential.clone().map(|credential| {
                                    Arc::new(credential)
                                        as Arc<dyn replication::reporter::Credential>
                                }),
                                incarnation: Arc::clone(incarnation),
                                storage_id,
                                held: reports_held.clone(),
                            }
                        }),
                        interval: Duration::from_millis(config.controlplane_sync_interval_ms),
                        routes_changed,
                        rebuild: replication::RebuildPolicy {
                            max_concurrent: config.replication_rebuild_max_concurrent,
                            bytes_per_sec: config.replication_rebuild_bytes_per_sec,
                        },
                        move_throttle: replication::MoveThrottle::new(
                            config.shard_move_bytes_per_sec,
                        ),
                        suspects: Arc::clone(suspects),
                        leads_after: leads_after.clone(),
                        unclean: unclean.clone(),
                        shutdown: sync_shutdown.clone(),
                    },
                )
                .replication
            });
            Some(ShardTasks {
                watch,
                feed,
                replication,
            })
        }
        _ => None,
    }
}
