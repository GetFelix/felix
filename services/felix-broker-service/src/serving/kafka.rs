//! The Kafka listener: accepting connections, TLS, and answering
//! `felix_kafka`'s questions about this cluster.
//!
//! The protocol work is in the `felix-kafka` crate. This module is what only
//! the broker service knows: how tokens are verified, who leads each shard,
//! and where every broker's Kafka listener is.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use felix_authz::AuthzError;
use felix_broker::{Broker, PublishOutcome, StreamHandle};
use felix_kafka::{
    Cluster, Endpoint, KafkaService, Placement, Principal, Settings, ShardRef, WriteError,
    WritePermit,
};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::client_endpoints::ClientEndpoints;
use crate::cluster::lease::LeaseState;
use crate::config::KafkaListenerConfig;
use crate::observability::tenants;
use crate::serving::auth::BrokerAuth;
use crate::serving::limits::{PerIpLimiter, TenantRates};
use crate::shards::lifecycle::fence;
use crate::shards::routing::{Dispatch, IngressRouter, Reason, dispatch, dispatch_write};
use crate::shards::{ShardKey, ShardKind};
use felix_replication::quorum::{QuorumError, QuorumMarks};

/// A TLS handshake that has not finished by now is abandoned, so a client
/// that connects and sends nothing does not hold a connection slot.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The node id a broker outside a cluster answers as.
pub const STANDALONE_NODE_ID: &str = "felix";

/// [`Cluster`] for this broker.
pub struct BrokerCluster {
    auth: Arc<BrokerAuth>,
    ingress: Option<Arc<IngressRouter>>,
    endpoints: Option<Arc<ClientEndpoints>>,
    local: Endpoint,
    lease: Option<Arc<LeaseState>>,
    marks: Option<Arc<QuorumMarks>>,
    quorum_timeout: Duration,
    quotas: Arc<TenantRates>,
}

impl BrokerCluster {
    /// `local_node_id` is the membership node id, or [`STANDALONE_NODE_ID`]
    /// outside a cluster; `advertise` is this broker's Kafka address.
    pub fn new(
        auth: Arc<BrokerAuth>,
        ingress: Option<Arc<IngressRouter>>,
        endpoints: Option<Arc<ClientEndpoints>>,
        local_node_id: &str,
        advertise: &str,
    ) -> Result<Self> {
        let local = Endpoint::parse(local_node_id, advertise)
            .with_context(|| format!("Kafka advertise address {advertise:?}"))?;
        Ok(Self {
            auth,
            ingress,
            endpoints,
            local,
            lease: None,
            marks: None,
            quorum_timeout: Duration::from_secs(5),
            quotas: Arc::new(TenantRates::unlimited()),
        })
    }

    /// The tenant publish quotas a produce is charged to: the broker's own,
    /// so a tenant's QUIC and Kafka publishes share one budget.
    pub(crate) fn with_quotas(mut self, quotas: Arc<TenantRates>) -> Self {
        self.quotas = quotas;
        self
    }

    /// What a write checks and waits on, as the QUIC publish path does: the
    /// lease that lets this broker lead, and the quorum marks a `Quorum`
    /// stream's write waits for. Both `None` outside a cluster.
    pub fn with_writes(
        mut self,
        lease: Option<Arc<LeaseState>>,
        marks: Option<Arc<QuorumMarks>>,
        quorum_timeout: Duration,
    ) -> Self {
        self.lease = lease;
        self.marks = marks;
        self.quorum_timeout = quorum_timeout;
        self
    }

    fn key(shard: &ShardRef<'_>) -> ShardKey {
        ShardKey {
            tenant_id: shard.tenant_id.to_string(),
            namespace: shard.namespace.to_string(),
            stream: shard.stream.to_string(),
            shard: shard.shard,
            kind: ShardKind::Stream,
        }
    }
}

#[async_trait]
impl Cluster for BrokerCluster {
    async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<Principal, String> {
        self.authenticate_peer(tenant_id, token, &[]).await
    }

    async fn authenticate_peer(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, String> {
        let certs: Vec<rustls::pki_types::CertificateDer<'_>> = peer_certs
            .iter()
            .map(|der| rustls::pki_types::CertificateDer::from(der.as_slice()))
            .collect();
        let peer = (!certs.is_empty()).then_some(certs.as_slice());
        match self.auth.authenticate_peer(tenant_id, token, peer).await {
            Ok(context) => Ok(Principal::with_permissions(tenant_id, context.matcher)),
            // The token's own fault is safe to say. Anything else is the
            // control plane being unreachable, whose error text can carry its
            // URL, so it is logged here and not handed to the client.
            Err(err) => match err.downcast_ref::<AuthzError>() {
                Some(authz) => Err(format!("token rejected: {authz}")),
                None => {
                    tracing::warn!(error = %err, "kafka authentication could not verify a token");
                    Err("the broker could not verify the token right now".to_string())
                }
            },
        }
    }

    fn brokers(&self) -> Vec<Endpoint> {
        let mut brokers = vec![self.local.clone()];
        let Some(endpoints) = &self.endpoints else {
            return brokers;
        };
        for endpoint in endpoints.kafka_snapshot().iter() {
            if endpoint.node_id == self.local.node_id {
                continue;
            }
            match Endpoint::parse(endpoint.node_id.clone(), &endpoint.addr) {
                Some(parsed) => brokers.push(parsed),
                None => tracing::warn!(
                    node_id = %endpoint.node_id,
                    addr = %endpoint.addr,
                    "skipping a broker whose Kafka address does not parse",
                ),
            }
        }
        brokers
    }

    fn local_node_id(&self) -> String {
        self.local.node_id.clone()
    }

    fn placement(&self, shard: &ShardRef<'_>) -> Placement {
        let key = Self::key(shard);
        let route = self
            .ingress
            .as_deref()
            .and_then(|ingress| ingress.route(&key));
        let replicas = route
            .as_ref()
            .map(|route| {
                route
                    .replicas
                    .iter()
                    .map(|replica| replica.node_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        match dispatch(self.ingress.as_deref(), &key) {
            Dispatch::Local { .. } => Placement::Local { replicas },
            Dispatch::Forward { node_id, .. } => Placement::Remote {
                leader: node_id,
                replicas,
            },
            // This broker will not forward across regions, but a Kafka client
            // connects to the leader itself.
            Dispatch::Unavailable(Reason::RegionNotRoutable { .. }) => match route {
                Some(route) => Placement::Remote {
                    leader: route.leader.node_id,
                    replicas,
                },
                None => Placement::Unavailable,
            },
            Dispatch::Unavailable(_) => Placement::Unavailable,
        }
    }

    async fn admit_write(&self, shard: &ShardRef<'_>) -> Result<WritePermit, WriteError> {
        // The same gates as a QUIC publish (`route.rs`, then the worker's
        // commit fence): a valid lease, the shard dispatched here, and a
        // place in its fence so a move waits for this write. Checked right
        // before the write, so the authoritative lease check is the only one.
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| !lease.is_valid_now())
        {
            crate::cluster::lease::metrics::record_refusal(
                crate::cluster::lease::metrics::BOUNDARY_COMMIT,
            );
            return Err(WriteError::NotLeader);
        }
        let key = Self::key(shard);
        let ingress = self.ingress.as_deref();
        match dispatch_write(ingress, &key).await {
            (Dispatch::Local { generation }, held) => {
                let mut held = held;
                match fence::enter_or_keep(&mut held, ingress, Some(&key), generation) {
                    Ok(Some(guard)) => Ok(WritePermit::holding(guard)),
                    Ok(None) => Ok(WritePermit::default()),
                    Err(_) => Err(WriteError::NotLeader),
                }
            }
            // A Kafka client goes to the leader itself, so nothing is
            // forwarded: it is sent back to Metadata.
            _ => Err(WriteError::NotLeader),
        }
    }

    async fn await_consistency(
        &self,
        shard: &ShardRef<'_>,
        handle: &StreamHandle,
        outcome: &PublishOutcome,
    ) -> Result<(), WriteError> {
        let key = self.ingress.as_ref().map(|_| Self::key(shard));
        felix_replication::quorum::await_quorum(
            handle,
            key.as_ref(),
            outcome,
            self.marks.as_deref(),
            self.ingress.as_deref(),
            self.quorum_timeout,
        )
        .await
        .map_err(|err| match err.downcast_ref::<QuorumError>() {
            Some(QuorumError::TimedOut { .. }) => WriteError::QuorumTimeout,
            _ => WriteError::LeadershipLost,
        })
    }

    fn committed_until(&self, shard: &ShardRef<'_>, handle: &StreamHandle) -> Option<u64> {
        let key = Self::key(shard);
        felix_replication::quorum::read_bound(
            Some(handle.consistency()),
            &key,
            self.marks.as_deref(),
            self.ingress.as_deref(),
        )
    }

    fn admit_produce(&self, tenant_id: &str, records: u64, bytes: u64) -> Duration {
        tenants::record_published(tenant_id, records, bytes);
        let hold = self.quotas.charge(tenant_id, records, bytes);
        if !hold.is_zero() {
            tenants::record_throttled(tenant_id, tenants::THROTTLE_DELAYED);
        }
        hold
    }
}

/// A bound Kafka listener, not yet accepting.
pub struct KafkaListener {
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    service: KafkaService,
    max_connections: usize,
    per_ip: Arc<PerIpLimiter>,
}

impl KafkaListener {
    /// Bind the listener. `tls` is required when the config asks for TLS.
    pub async fn bind(
        config: &KafkaListenerConfig,
        tls: Option<Arc<rustls::ServerConfig>>,
        broker: Arc<Broker>,
        cluster: Arc<dyn Cluster>,
        cluster_id: String,
    ) -> Result<Self> {
        let listener = TcpListener::bind(config.listen)
            .await
            .with_context(|| format!("bind the Kafka listener on {}", config.listen))?;
        let tls = match (config.tls, tls) {
            (true, Some(server)) => Some(TlsAcceptor::from(server)),
            (true, None) => anyhow::bail!("FELIX_KAFKA_TLS is on but no certificate was given"),
            (false, _) => None,
        };
        let service = KafkaService::new(
            broker,
            cluster,
            Settings {
                anonymous_tenant: config.anonymous_tenant.clone(),
                default_namespace: config.default_namespace.clone(),
                cluster_id,
                auth_timeout: Duration::from_millis(config.auth_timeout_ms),
            },
        );
        Ok(Self {
            listener,
            tls,
            service,
            max_connections: config.max_connections,
            per_ip: PerIpLimiter::new(config.max_connections_per_ip),
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept until `shutdown`, serving each connection on `connections` so a
    /// drain waits for it. A connection sees the same token and closes
    /// between requests, or ends a long-polling fetch early.
    pub async fn serve(self, shutdown: CancellationToken, connections: TaskTracker) {
        let slots = Arc::new(Semaphore::new(self.max_connections));
        loop {
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                accepted = self.listener.accept() => accepted,
            };
            let (socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tracing::warn!(error = %err, "kafka accept failed");
                    continue;
                }
            };
            // Per address first, so one host at its cap is refused without
            // taking a slot from the shared pool even briefly.
            let Some(per_ip) = self.per_ip.try_acquire(peer.ip()) else {
                tracing::debug!(%peer, max = self.per_ip.max(), "kafka per-address connection limit reached; closing");
                metrics::counter!("felix_kafka_refused_total", "reason" => "per_ip_limit")
                    .increment(1);
                continue;
            };
            let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                tracing::warn!(%peer, max = self.max_connections, "kafka connection limit reached; closing");
                metrics::counter!("felix_kafka_refused_total", "reason" => "connection_limit")
                    .increment(1);
                continue;
            };
            let _ = socket.set_nodelay(true);
            let tls = self.tls.clone();
            let service = self.service.clone();
            let shutdown = shutdown.clone();
            connections.spawn(async move {
                let _slot = slot;
                let _per_ip = per_ip;
                match tls {
                    None => service.serve_connection(socket, shutdown).await,
                    Some(tls) => {
                        match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, tls.accept(socket)).await
                        {
                            Ok(Ok(stream)) => {
                                // Kept for SASL, which binds the token to the
                                // certificate when the broker is set to.
                                let peer_certs = stream
                                    .get_ref()
                                    .1
                                    .peer_certificates()
                                    .map(|chain| chain.iter().map(|der| der.to_vec()).collect())
                                    .unwrap_or_default();
                                service
                                    .serve_connection_with_peer(stream, peer_certs, shutdown)
                                    .await;
                            }
                            Ok(Err(err)) => {
                                tracing::debug!(%peer, error = %err, "kafka TLS handshake failed");
                            }
                            Err(_) => tracing::debug!(%peer, "kafka TLS handshake timed out"),
                        }
                    }
                }
            });
        }
    }
}

/// The TLS config the Kafka listener serves, from the broker's certificate.
pub fn tls_config(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<Arc<rustls::ServerConfig>> {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .context("kafka TLS protocol versions")?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .context("kafka TLS certificate")?;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests;
