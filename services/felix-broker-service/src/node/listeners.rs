//! The client-facing listeners and their accept loops: QUIC always, Kafka when
//! `FELIX_KAFKA_LISTEN` is set. Both serve the certificate in
//! [`ClientTls`](crate::serving::tls::ClientTls).

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_broker::Broker;
use felix_transport::{QuicServer, TransportConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::client_endpoints::ClientEndpoints;
use crate::cluster::lease::LeaseState;
use crate::config::{BrokerConfig, KafkaListenerConfig};
use crate::peer::PeerPool;
use crate::replication::quorum::QuorumMarks;
use crate::serving::kafka::{BrokerCluster, KafkaListener, STANDALONE_NODE_ID};
use crate::serving::tls::ClientTls;
use crate::serving::{auth::BrokerAuth, quic};
use crate::shards::routing::IngressRouter;

/// Bind one QUIC listener per configured address.
pub(super) fn bind(config: &BrokerConfig, tls: &ClientTls) -> Result<Vec<Arc<QuicServer>>> {
    let server_config = tls
        .quic_server_config()
        .context("build QUIC server config")?;

    // Apply transport-level configuration (flow control windows, pooling behavior, etc.)
    // derived from broker config.
    let transport =
        crate::serving::quic::cache_transport_config(config, TransportConfig::default());

    // One `QuicServer` per configured listener. Each owns its own UDP socket and
    // therefore its own `quinn` endpoint driver -- the single task that reads
    // every datagram for that socket and routes it by connection id. That task
    // is the per-broker throughput ceiling (#557): it cannot use more than one
    // core, and it saturates while the rest of the machine idles. N sockets are
    // N drivers. Default is one, so a deployment that has not asked for more
    // binds exactly what it always did.
    let mut quic_servers = Vec::with_capacity(config.quic_listeners);
    for bind_addr in config.quic_binds() {
        let server = QuicServer::bind(bind_addr, server_config.clone(), transport.clone())
            .with_context(|| format!("bind QUIC listener on {bind_addr}"))?;
        tracing::info!(addr = %server.local_addr()?, "quic listener started");
        quic_servers.push(Arc::new(server));
    }
    Ok(quic_servers)
}

/// What every accept loop shares.
pub(super) struct AcceptLoops<'a> {
    pub(super) broker: &'a Arc<Broker>,
    pub(super) config: &'a BrokerConfig,
    pub(super) auth: &'a Arc<BrokerAuth>,
    pub(super) accept_shutdown: &'a CancellationToken,
    pub(super) connections: &'a TaskTracker,
    pub(super) seeded: &'a CancellationToken,
    pub(super) gate_readiness_on_sync: bool,
    pub(super) ingress_router: &'a Option<Arc<IngressRouter>>,
    pub(super) peers: &'a Option<Arc<PeerPool>>,
    pub(super) lease: &'a Option<Arc<LeaseState>>,
    pub(super) quorum_marks: &'a Arc<QuorumMarks>,
    pub(super) client_endpoints: &'a Arc<ClientEndpoints>,
}

/// Start accepting QUIC connections, one background task per listener. If an
/// accept loop exits due to an error, it is logged and shutdown continues
/// normally.
///
/// The loops share everything behind them -- the same broker, the same
/// connection registry, the same shutdown token -- and differ only in the
/// socket they read from.
pub(super) fn spawn_accept_loops(
    quic_servers: &[Arc<QuicServer>],
    shared: AcceptLoops<'_>,
) -> Vec<JoinHandle<()>> {
    let AcceptLoops {
        broker,
        config,
        auth,
        accept_shutdown,
        connections,
        seeded,
        gate_readiness_on_sync,
        ingress_router,
        peers,
        lease,
        quorum_marks,
        client_endpoints,
    } = shared;
    let limit = quic::ConnectionLimit::new(config.max_client_connections);
    quic_servers
        .iter()
        .map(|server| {
            let limit = limit.clone();
            let quic_server = Arc::clone(server);
            let broker = Arc::clone(broker);
            let quic_config = config.clone();
            let auth = Arc::clone(auth);
            let accept_shutdown = accept_shutdown.clone();
            let connections = connections.clone();
            let seeded = seeded.clone();
            let ingress_router = ingress_router.clone();
            let peers_for_accept = peers.clone();
            let lease_for_accept = lease.clone();
            let marks_for_accept = Arc::clone(quorum_marks);
            let endpoints_for_accept = Arc::clone(client_endpoints);
            tokio::spawn(async move {
                // A durable broker does not accept until its streams exist.
                // Readiness alone only steers orchestrated traffic; a client with
                // the address in hand would otherwise connect during recovery and
                // be told its durable stream does not exist. Waiting is the honest
                // answer, and shutdown still wins the race so a broker told to stop
                // during recovery stops.
                if gate_readiness_on_sync {
                    tokio::select! {
                        biased;
                        _ = accept_shutdown.cancelled() => {
                            tracing::info!("shutdown before initial sync; not accepting");
                            return;
                        }
                        _ = seeded.cancelled() => {
                            tracing::info!("initial sync applied; accepting connections");
                        }
                    }
                }
                if let Err(err) = quic::serve_with_shutdown(
                    quic_server,
                    broker,
                    quic_config,
                    auth,
                    accept_shutdown,
                    connections,
                    quic::ClusterContext {
                        ingress: ingress_router,
                        peers: peers_for_accept,
                        lease: lease_for_accept,
                        marks: Some(Arc::clone(&marks_for_accept)),
                        client_endpoints: Some(Arc::clone(&endpoints_for_accept)),
                    },
                    limit,
                )
                .await
                {
                    tracing::warn!(error = %err, "quic accept loop exited");
                }
            })
        })
        .collect()
}

/// What the Kafka listener needs of the cluster: where shards are and who
/// may be sent where, and what a write checks and waits on.
pub(super) struct KafkaClusterView<'a> {
    pub(super) ingress: &'a Option<Arc<IngressRouter>>,
    pub(super) client_endpoints: &'a Arc<ClientEndpoints>,
    pub(super) lease: &'a Option<Arc<LeaseState>>,
    pub(super) quorum_marks: &'a Arc<QuorumMarks>,
}

/// Bind the Kafka listener, when one is configured.
///
/// Bound here, before anything is accepted, so a port conflict fails startup
/// rather than surfacing later as a listener that never came up.
pub(super) async fn bind_kafka(
    config: &BrokerConfig,
    tls: &ClientTls,
    broker: &Arc<Broker>,
    auth: &Arc<BrokerAuth>,
    view: KafkaClusterView<'_>,
) -> Result<Option<KafkaListener>> {
    let KafkaClusterView {
        ingress,
        client_endpoints,
        lease,
        quorum_marks,
    } = view;
    let Some(kafka) = KafkaListenerConfig::from_env()? else {
        return Ok(None);
    };
    let node_id = config
        .membership
        .as_ref()
        .map_or(STANDALONE_NODE_ID, |membership| membership.node_id.as_str());
    let cluster = BrokerCluster::new(
        Arc::clone(auth),
        ingress.clone(),
        config
            .membership
            .as_ref()
            .map(|_| Arc::clone(client_endpoints)),
        node_id,
        &kafka.advertise,
    )?
    .with_writes(
        lease.clone(),
        Some(Arc::clone(quorum_marks)),
        std::time::Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
    );
    let kafka_tls = kafka.tls.then(|| tls.kafka_server_config()).transpose()?;
    if kafka.tls {
        tracing::info!("kafka listener serves TLS: clients connect with SASL_SSL");
    } else {
        tracing::warn!(
            "kafka listener without TLS (FELIX_KAFKA_TLS=false): SASL/PLAIN sends tokens in clear text"
        );
    }
    if let Some(tenant) = &kafka.anonymous_tenant {
        tracing::warn!(
            tenant = %tenant,
            "FELIX_KAFKA_ANONYMOUS_TENANT is set: unauthenticated Kafka clients can read and write every stream of this tenant"
        );
    }
    let listener = KafkaListener::bind(
        &kafka,
        kafka_tls,
        Arc::clone(broker),
        Arc::new(cluster),
        format!("felix-{node_id}"),
    )
    .await?;
    tracing::info!(
        addr = %listener.local_addr()?,
        advertise = %kafka.advertise,
        "kafka listener started"
    );
    Ok(Some(listener))
}

/// Accept Kafka connections once the broker may serve, like the QUIC loops.
pub(super) fn spawn_kafka(
    listener: KafkaListener,
    accept_shutdown: &CancellationToken,
    connections: &TaskTracker,
    seeded: &CancellationToken,
    gate_readiness_on_sync: bool,
) -> JoinHandle<()> {
    let accept_shutdown = accept_shutdown.clone();
    let connections = connections.clone();
    let seeded = seeded.clone();
    tokio::spawn(async move {
        if gate_readiness_on_sync {
            tokio::select! {
                biased;
                _ = accept_shutdown.cancelled() => return,
                _ = seeded.cancelled() => {}
            }
        }
        listener.serve(accept_shutdown, connections).await;
    })
}
