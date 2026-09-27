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
use crate::peer::{self, PeerPool, tls::PeerTls};
use crate::replication::{self, halted::HaltedReplicas, quorum::QuorumMarks};
use crate::serving::auth::BrokerAuth;
use crate::shards::{lifecycle as shard_lifecycle, routing as shard_routing, watch as shard_watch};

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
                peer_config.clone(),
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

/// Bind and serve the broker-internal listener, when this broker is in a
/// cluster. This is the address `NodeSpec.advertise_addr` names, so it must be
/// up for peers to reach this node at all.
pub(super) fn bind_peer_listener(
    config: &BrokerConfig,
    cluster: &Option<ShardState>,
    broker: &Arc<Broker>,
    quorum_marks: &Arc<QuorumMarks>,
    auth: &Arc<BrokerAuth>,
    peer_tls: &Option<Arc<PeerTls>>,
    peer_shutdown: &CancellationToken,
) -> Result<Option<JoinHandle<()>>> {
    Ok(
        match (&config.peer_transport, &config.membership, cluster) {
            (Some(peer_config), Some(membership_config), Some((router, ingress, _, _))) => {
                let server = peer::PeerServer::bind_with_tls(
                    membership_config.node_id.clone(),
                    peer_config,
                    Arc::new(peer_dispatch::BrokerPeerHandler::new(
                        crate::serving::forward::ForwardingHandler::new(
                            Arc::clone(broker),
                            Arc::clone(ingress),
                            Arc::clone(router),
                            membership_config.advertise_addr.clone(),
                            Some(Arc::clone(quorum_marks)),
                            Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
                            Arc::clone(auth),
                        ),
                        replication::ReplicaHandler::new(Arc::clone(broker), Arc::clone(router)),
                    )),
                    peer_tls.clone(),
                )
                .context("bind broker-internal listener")?;
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
                Some(tokio::spawn(server.serve(peer_shutdown.clone())))
            }
            _ => None,
        },
    )
}

/// The shard watch, the routing feed, and shipping to followers.
pub(super) struct ShardTasks {
    pub(super) watch: JoinHandle<()>,
    pub(super) feed: JoinHandle<()>,
    /// Absent without a peer transport to ship over.
    pub(super) replication: Option<replication::driver::Replication>,
}

/// What the shard tasks read and publish to.
pub(super) struct ShardTaskDeps<'a> {
    pub(super) config: &'a BrokerConfig,
    pub(super) cluster: &'a Option<ShardState>,
    pub(super) durable_storage: &'a Option<DurableStorage>,
    pub(super) membership_client: &'a reqwest::Client,
    pub(super) credential: &'a Option<NodeCredential>,
    pub(super) client_endpoints: &'a Arc<ClientEndpoints>,
    pub(super) peers: &'a Option<Arc<PeerPool>>,
    pub(super) broker: &'a Arc<Broker>,
    pub(super) quorum_marks: &'a Arc<QuorumMarks>,
    pub(super) halted_replicas: &'a Arc<HaltedReplicas>,
    pub(super) sync_shutdown: &'a CancellationToken,
}

/// Shard ownership: follow the control plane's assignments, and keep local
/// state and the routing table in step with them. Also starts shipping to
/// followers for the shards this broker leads.
pub(super) fn spawn_shard_tasks(deps: ShardTaskDeps<'_>) -> Option<ShardTasks> {
    let ShardTaskDeps {
        config,
        cluster,
        durable_storage,
        membership_client,
        credential,
        client_endpoints,
        peers,
        broker,
        quorum_marks,
        halted_replicas,
        sync_shutdown,
    } = deps;
    match (cluster, &config.controlplane_url, durable_storage) {
        (Some((router, ingress, lifecycle, ownership)), Some(base_url), storage) => {
            let readers = shard_lifecycle::ShardReaders::new(Arc::clone(broker))
                .with_endpoints(Arc::clone(client_endpoints));
            // A lapsed lease ends the readers of every shard this broker led.
            if let Some(lease) = ingress.fence().lease() {
                tokio::spawn(readers.clone().end_on_lapse(
                    Arc::clone(lease),
                    Arc::clone(lifecycle),
                    sync_shutdown.clone(),
                ));
            }
            let store: Arc<dyn shard_lifecycle::ShardStore> = match storage {
                Some(storage) => Arc::new(
                    shard_lifecycle::DurableShardStore::new(Arc::new(storage.clone()))
                        .with_readers(readers),
                ),
                // Without durable storage there is no log to open, so taking a
                // shard is bookkeeping only.
                None => Arc::new(shard_lifecycle::EphemeralShardStore::with_readers(readers)),
            };
            // The watch wakes the feed, and the feed wakes replication, so a
            // fence or a cut-over runs through all three as soon as it is
            // written rather than a tick apart at each hop.
            let assignments_changed = Arc::new(tokio::sync::Notify::new());
            let routes_changed = Arc::new(tokio::sync::Notify::new());
            let watch = tokio::spawn(shard_watch::run(
                membership_client.clone(),
                base_url.clone(),
                // The assignment feed is cluster metadata, so it is read with
                // the same credential the rest of membership uses.
                credential.clone(),
                Arc::clone(ownership),
                Arc::clone(&assignments_changed),
                Duration::from_millis(config.controlplane_sync_interval_ms),
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
                },
                Some(shard_routing::CatalogSource {
                    client: membership_client.clone(),
                    base_url: base_url.clone(),
                    token: credential.clone(),
                }),
                Duration::from_millis(config.controlplane_sync_interval_ms),
                sync_shutdown.clone(),
            );
            // Shipping to followers, for the shards this broker leads. Only
            // when there is a peer transport to ship over: without one the
            // replica set is a plan nobody can act on.
            let replication = peers.as_ref().map(|pool| {
                replication::driver::spawn(
                    Arc::clone(pool),
                    Arc::clone(broker),
                    Arc::clone(router),
                    Arc::clone(ingress.fence()),
                    replication::driver::Published {
                        marks: Arc::clone(quorum_marks),
                        halted: Arc::clone(halted_replicas),
                    },
                    // Only a broker that is a cluster member reports: the
                    // report is about shards the control plane assigned, and a
                    // broker it has never registered leads none of them.
                    //
                    // Reports from every shard in a pass share one request —
                    // see `replication::reporter`.
                    config.membership.as_ref().map(|membership| {
                        let (reporter, _task) = replication::reporter::Reporter::spawn(
                            replication::reporter::ReportTo {
                                client: membership_client.clone(),
                                base_url: base_url.clone(),
                                node_id: membership.node_id.clone(),
                                token: credential.clone(),
                                incarnation: 0,
                            },
                            sync_shutdown.clone(),
                        );
                        reporter
                    }),
                    Duration::from_millis(config.controlplane_sync_interval_ms),
                    routes_changed,
                    replication::RebuildPolicy {
                        max_concurrent: config.replication_rebuild_max_concurrent,
                        bytes_per_sec: config.replication_rebuild_bytes_per_sec,
                    },
                    replication::MoveThrottle::new(config.shard_move_bytes_per_sec),
                    sync_shutdown.clone(),
                )
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
