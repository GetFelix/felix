//! The outbound half of the internal transport: connections to peer brokers.
//!
//! # Shape
//!
//! One [`PeerPool`] per broker, holding at most `conns_per_peer` connections to
//! each peer it has been asked to reach. Each connection carries a control
//! stream plus `streams_per_conn` streams for forwards and as many again for
//! replication; requests are spread round-robin within their class and matched
//! to responses by correlation id, which the protocol scopes to the connection.
//!
//! Several streams rather than one because a QUIC stream is ordered: a large
//! forwarded batch on a single stream would hold up every smaller request queued
//! behind it. Separate classes because a forward waits on a quorum that only
//! replication can reach (#1458).
//!
//! # What bounds it
//!
//! An unhealthy peer must not be able to consume this broker, so three limits
//! apply and each fails differently:
//!
//! - **In-flight requests per peer.** A peer that accepts frames and never
//!   answers otherwise accumulates one waiter per forwarded publish. At the
//!   limit, requests are shed immediately rather than queued. Forwards and
//!   replication each have their own allowance.
//! - **Reconnect backoff.** A peer that refuses connections is dialled on a
//!   jittered exponential schedule, and requests arriving inside the backoff
//!   window fail without a dial.
//! - **Request timeout.** Every request has one, so no waiter is unbounded even
//!   if the connection stays open.
//!
//! # Every request ends
//!
//! `docs/internal-protocol.md` requires that a forwarded publish is never left
//! pending. A connection loss fails every waiter on it at the moment it drops,
//! which is what makes "the requester treats it as failed" true rather than
//! merely eventual.

mod connection;
mod state;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::peer::identity::PeerIdentity;
use anyhow::{Context, Result};
use felix_transport::QuicClient;
use felix_wire::internal::{InternalMessage, PeerCapabilities, ReplicaLog};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::KnownCapabilities;
use super::config::PeerTransportConfig;
use super::metrics;
use super::tls;
use connection::{LaneClass, PeerConnection};
use state::Peer;

/// Connections to peer brokers.
pub struct PeerPool {
    config: PeerTransportConfig,
    /// Test-only peer severing. `None` in any deployment that did not ask for
    /// it by setting `FELIX_PEER_PARTITION_FILE`.
    partition: Option<super::PartitionInjector>,
    local_node_id: String,
    client: QuicClient,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
    shutdown: CancellationToken,
    /// Whether this broker presents a certificate and verifies each peer's
    /// against the node id it dials.
    authenticated: bool,
    /// What each peer said it can do, shared with this broker's listener.
    known: KnownCapabilities,
}

impl PeerPool {
    /// Bind the outbound endpoint without peer authentication. See
    /// [`Self::new_with_tls`].
    pub fn new(
        local_node_id: String,
        config: PeerTransportConfig,
        shutdown: CancellationToken,
    ) -> Result<Arc<Self>> {
        Self::new_with_tls(local_node_id, config, shutdown, None)
    }

    /// Bind the outbound endpoint. No peer is dialled until one is requested.
    ///
    /// With `tls`, every dial verifies the peer's certificate against the
    /// node id being dialled, so a catalog entry pointing at the wrong broker
    /// fails in the handshake rather than after a `HelloOk`.
    pub fn new_with_tls(
        local_node_id: String,
        config: PeerTransportConfig,
        shutdown: CancellationToken,
        tls: Option<Arc<tls::PeerTls>>,
    ) -> Result<Arc<Self>> {
        let client = QuicClient::bind(
            "0.0.0.0:0".parse().expect("literal address"),
            tls::client_config(tls.as_deref())?,
            config.quic_transport(),
        )
        .context("bind peer QUIC endpoint")?;
        let authenticated = tls.is_some();

        let pool = Arc::new(Self {
            partition: config
                .partition_file
                .clone()
                .map(super::PartitionInjector::new),
            config,
            local_node_id,
            client,
            peers: Mutex::new(HashMap::new()),
            shutdown,
            authenticated,
            known: KnownCapabilities::default(),
        });
        pool.clone().spawn_reaper();
        Ok(pool)
    }

    /// Send `message` to `node_id` at `addr` and wait for its response.
    ///
    /// Establishes the connection if there is not one already. The address is
    /// passed per call rather than cached because the catalog owns it: a peer
    /// that re-registers at a new address must be reached there on the next
    /// request, not after this pool notices.
    pub async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        self.request_identified(node_id, addr, message)
            .await
            .map(|(answer, _)| answer)
    }

    /// [`Self::request`], with the identity of the connection the answer
    /// came over. Unlike [`Self::identity`] this says who answered: a pooled
    /// connection can outlive the process it was opened to.
    pub async fn request_identified(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<(InternalMessage, PeerIdentity), PeerError> {
        let started = Instant::now();
        let result = self.request_inner(node_id, addr, message).await;
        match &result {
            Ok(_) => metrics::record_request(metrics::OUTCOME_OK, started.elapsed()),
            Err(err) => metrics::record_request(err.outcome(), started.elapsed()),
        }
        result
    }

    /// What `node_id` can do, dialling it if there is no connection yet.
    pub async fn capabilities(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<PeerCapabilities, PeerError> {
        Ok(self.connection(node_id, addr).await?.capabilities())
    }

    /// The storage identity and incarnation `node_id` gave in its
    /// handshake, dialling it if there is no connection yet.
    pub async fn identity(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        Ok(self.connection(node_id, addr).await?.identity())
    }

    /// Whether this broker offers the promotion fence to its peers.
    pub fn offers_fence(&self) -> bool {
        self.config
            .capabilities()
            .contains(crate::promotion::REQUIRED)
    }

    /// What each peer said in its latest handshake with this broker, in
    /// either direction. Hand it to the listener so inbound handshakes count.
    pub fn known_capabilities(&self) -> &KnownCapabilities {
        &self.known
    }

    /// Live connections across all peers. Test and metrics surface.
    pub async fn connection_count(&self) -> usize {
        let peers: Vec<_> = self.peers.lock().values().cloned().collect();
        let mut total = 0;
        for peer in peers {
            total += peer.state.lock().await.live_connections();
        }
        total
    }

    /// Close every connection and stop the reaper.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let peers: Vec<_> = self.peers.lock().drain().map(|(_, peer)| peer).collect();
        for peer in peers {
            peer.state.lock().await.close_all("broker shutting down");
        }
        metrics::set_connections(0);
        metrics::set_streams(0);
    }

    async fn connection(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<Arc<PeerConnection>, PeerError> {
        if self.shutdown.is_cancelled() {
            return Err(PeerError::ShuttingDown);
        }
        if let Some(partition) = &self.partition
            && partition.blocks(node_id)
        {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "partitioned from this broker (fault injection)".to_string(),
            });
        }
        self.peer(node_id).connection(self, addr).await
    }

    async fn request_inner(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<(InternalMessage, PeerIdentity), PeerError> {
        if self.shutdown.is_cancelled() {
            return Err(PeerError::ShuttingDown);
        }
        // Test-only, and `None` unless `FELIX_PEER_PARTITION_FILE` is set.
        //
        // Refused rather than dropped: a silently discarded packet would make
        // every partition test wait out a timeout, and what is under test is how
        // the broker behaves when a peer is unreachable, not how long the
        // transport takes to notice.
        if let Some(partition) = &self.partition
            && partition.blocks(node_id)
        {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "partitioned from this broker (fault injection)".to_string(),
            });
        }
        let peer = self.peer(node_id);

        // Taken before the connection is touched, so a peer at its limit is shed
        // without being dialled. Dropping the permit is what releases it, which
        // covers every exit below including a timeout.
        let class = LaneClass::of(&message);
        let budget = match class {
            LaneClass::Forward => &peer.inflight,
            _ => &peer.replication_inflight,
        };
        let Ok(_permit) = Arc::clone(budget).try_acquire_owned() else {
            metrics::record_request_shed();
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: format!(
                    "{} requests already in flight",
                    self.config.max_inflight_per_peer
                ),
            });
        };

        let connection = peer.connection(self, addr).await?;
        // Checked here, where the handshake's answer is, so no caller can
        // send a peer a request it never said it understands.
        if let Some(needed) = required_capability(&message)
            && !connection.capabilities().contains(needed)
        {
            return Err(PeerError::Unsupported {
                node_id: node_id.to_string(),
                kind: message.kind(),
            });
        }
        // A forward's answer comes after the owner's quorum wait, so it gets
        // longer than that wait; anything shorter turns a slow commit into an
        // indeterminate one.
        let timeout = match class {
            LaneClass::Forward => self.config.forward_timeout.max(self.config.request_timeout),
            _ => self.config.request_timeout,
        };
        connection
            .request(message, timeout, &self.shutdown)
            .await
            .map(|answer| (answer, connection.identity()))
    }

    fn peer(&self, node_id: &str) -> Arc<Peer> {
        let mut peers = self.peers.lock();
        Arc::clone(peers.entry(node_id.to_string()).or_insert_with(|| {
            Arc::new(Peer::new(
                node_id.to_string(),
                self.config.max_inflight_per_peer,
            ))
        }))
    }

    /// Close connections that have gone unused, and republish the gauges.
    ///
    /// Rebalancing changes which peers a broker forwards to, so a pool that only
    /// grew would keep a connection and its keep-alives alive for every peer it
    /// had ever talked to.
    fn spawn_reaper(self: Arc<Self>) {
        let interval = (self.config.idle_timeout / 2).max(Duration::from_secs(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = self.shutdown.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                self.reap().await;
            }
        });
    }

    async fn reap(&self) {
        let peers: Vec<_> = self
            .peers
            .lock()
            .iter()
            .map(|(id, peer)| (id.clone(), Arc::clone(peer)))
            .collect();

        let mut connections = 0;
        let mut streams = 0;
        let mut idle_peers = Vec::new();
        for (id, peer) in peers {
            let mut state = peer.state.lock().await;
            state.evict_idle(self.config.idle_timeout);
            let live = state.live_connections();
            connections += live;
            // The control stream, and a forward and a replication class.
            streams += live * (1 + 2 * self.config.streams_per_conn.max(1));
            if live == 0 && !state.in_backoff() {
                idle_peers.push(id);
            }
        }

        if !idle_peers.is_empty() {
            let mut peers = self.peers.lock();
            for id in idle_peers {
                // Only if nothing took a reference since: a request between the
                // scan and here would otherwise lose its peer entry, and with it
                // the in-flight limit that request is already counted against.
                if peers
                    .get(&id)
                    .is_some_and(|peer| Arc::strong_count(peer) == 1)
                {
                    peers.remove(&id);
                }
            }
        }

        metrics::set_connections(connections);
        metrics::set_streams(streams);
    }

    /// Establish one connection and complete the handshake.
    async fn dial(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<Arc<PeerConnection>, PeerError> {
        // Under mTLS the certificate is verified for the node id being
        // dialled; without it the name is a fixed one both ends agree on.
        let server_name = if self.authenticated {
            node_id
        } else {
            tls::INTERNAL_SERVER_NAME
        };
        let connect = tokio::time::timeout(
            self.config.handshake_timeout,
            self.client.connect(addr, server_name),
        );
        let connection = match connect.await {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => {
                metrics::record_connect_attempt(metrics::OUTCOME_UNREACHABLE);
                return Err(PeerError::Unavailable {
                    node_id: node_id.to_string(),
                    detail: err.to_string(),
                });
            }
            Err(_) => {
                metrics::record_connect_attempt(metrics::OUTCOME_UNREACHABLE);
                return Err(PeerError::Unavailable {
                    node_id: node_id.to_string(),
                    detail: format!("no answer within {:?}", self.config.handshake_timeout),
                });
            }
        };

        let established = PeerConnection::establish(
            connection,
            node_id.to_string(),
            self.local_node_id.clone(),
            // This side reads an identity whatever it offers itself.
            self.config
                .capabilities()
                .union(PeerCapabilities::STORAGE_IDENTITY)
                .union(PeerCapabilities::NODE_INCARNATION),
            self.config.streams_per_conn,
            self.config.handshake_timeout,
        )
        .await;

        match established {
            Ok(connection) => {
                metrics::record_connect_attempt(metrics::OUTCOME_CONNECTED);
                self.known.record(node_id, connection.capabilities());
                tracing::debug!(peer = %node_id, %addr, "peer connection established");
                Ok(connection)
            }
            Err(err) => {
                metrics::record_connect_attempt(metrics::OUTCOME_HANDSHAKE);
                Err(PeerError::Handshake {
                    node_id: node_id.to_string(),
                    detail: format!("{err:#}"),
                })
            }
        }
    }
}

/// Why a request to a peer did not produce a response.
///
/// Typed so a caller can tell "this broker refused to try" from "the peer was
/// asked and did not answer" — the first is retryable elsewhere immediately,
/// the second is not necessarily safe to retry at all.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    /// The peer is in a reconnect backoff window, or at its in-flight limit.
    /// Nothing was sent.
    #[error("peer {node_id} is unavailable: {detail}")]
    Unavailable { node_id: String, detail: String },
    /// The connection could not be established or the handshake was refused.
    #[error("peer {node_id} handshake failed: {detail}")]
    Handshake { node_id: String, detail: String },
    /// The request was sent and the connection dropped before an answer.
    #[error("connection to peer {node_id} was lost")]
    Disconnected { node_id: String },
    /// The request was sent and no answer arrived in time.
    #[error("peer {node_id} did not respond within {timeout:?}")]
    Timeout { node_id: String, timeout: Duration },
    /// The pool is shutting down.
    #[error("peer transport is shutting down")]
    ShuttingDown,
    /// The peer did not say it understands this request, so it was not sent.
    #[error("peer {node_id} does not support {kind:?}")]
    Unsupported {
        node_id: String,
        kind: felix_wire::internal::Kind,
    },
}

impl PeerError {
    /// Whether the same request may be sent to the same peer again.
    ///
    /// `Disconnected` and `Timeout` are absent on purpose: the peer may have
    /// applied the write before the answer was lost, so retrying is a duplicate
    /// rather than a repair. Deciding that is the caller's, with the ack mode in
    /// hand.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }

    fn outcome(&self) -> &'static str {
        match self {
            Self::Unavailable { .. } | Self::ShuttingDown => metrics::OUTCOME_UNREACHABLE,
            Self::Handshake { .. } => metrics::OUTCOME_HANDSHAKE,
            Self::Disconnected { .. } => metrics::OUTCOME_DISCONNECTED,
            Self::Timeout { .. } => metrics::OUTCOME_TIMEOUT,
            Self::Unsupported { .. } => metrics::OUTCOME_UNSUPPORTED,
        }
    }
}

/// The one thing forwarding asks of the connection pool.
///
/// A trait rather than the pool itself so the retry rules above — which decide
/// whether a batch may be sent a second time — can be tested against an owner
/// that answers on command, including with the answers a healthy cluster
/// almost never produces.
pub trait PeerRequester: Sync {
    fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<Output = std::result::Result<InternalMessage, PeerError>> + Send;

    /// What `node_id` said it can do, reaching it if need be. A requester
    /// that cannot tell says it can do nothing, which keeps every request
    /// that needs a capability from being sent.
    fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> impl std::future::Future<Output = std::result::Result<PeerCapabilities, PeerError>> + Send
    {
        async { Ok(PeerCapabilities::NONE) }
    }

    /// What `node_id` said in its latest handshake with this broker, in
    /// either direction, without reaching it.
    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        None
    }

    /// The storage identity and incarnation `node_id` gave in its
    /// handshake, reaching it if need be. A peer that gave neither cannot be
    /// told apart from a wiped copy of itself.
    fn identity(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> impl std::future::Future<Output = std::result::Result<PeerIdentity, PeerError>> + Send
    {
        async { Ok(PeerIdentity::default()) }
    }

    /// [`Self::request`], with the identity of the connection the answer
    /// came over. A requester that cannot tell answers with
    /// [`Self::identity`].
    fn request_identified(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<
        Output = std::result::Result<(InternalMessage, PeerIdentity), PeerError>,
    > + Send {
        async move {
            let answer = self.request(node_id, addr, message).await?;
            let identity = self.identity(node_id, addr).await?;
            Ok((answer, identity))
        }
    }
}

impl<T: PeerRequester + Send> PeerRequester for std::sync::Arc<T> {
    fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<Output = std::result::Result<InternalMessage, PeerError>> + Send
    {
        T::request(self, node_id, addr, message)
    }

    fn capabilities(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> impl std::future::Future<Output = std::result::Result<PeerCapabilities, PeerError>> + Send
    {
        T::capabilities(self, node_id, addr)
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        T::recorded_capabilities(self, node_id)
    }

    fn identity(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> impl std::future::Future<Output = std::result::Result<PeerIdentity, PeerError>> + Send
    {
        T::identity(self, node_id, addr)
    }

    fn request_identified(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> impl std::future::Future<
        Output = std::result::Result<(InternalMessage, PeerIdentity), PeerError>,
    > + Send {
        T::request_identified(self, node_id, addr, message)
    }
}

impl PeerRequester for PeerPool {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        PeerPool::request(self, node_id, addr, message).await
    }

    async fn capabilities(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<PeerCapabilities, PeerError> {
        PeerPool::capabilities(self, node_id, addr).await
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        self.known.get(node_id)
    }

    async fn identity(
        &self,
        node_id: &str,
        addr: SocketAddr,
    ) -> std::result::Result<PeerIdentity, PeerError> {
        PeerPool::identity(self, node_id, addr).await
    }

    async fn request_identified(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<(InternalMessage, PeerIdentity), PeerError> {
        PeerPool::request_identified(self, node_id, addr, message).await
    }
}

/// The capability a peer must have said it has before `message` goes to it.
pub(crate) fn required_capability(message: &InternalMessage) -> Option<PeerCapabilities> {
    let (base, log) = match message {
        InternalMessage::Fence(fence) => (PeerCapabilities::FENCE, fence.log),
        InternalMessage::ReplicateFetch(fetch) => (PeerCapabilities::TAIL_FETCH, fetch.log),
        _ => return None,
    };
    Some(match log {
        ReplicaLog::Counters => base.union(PeerCapabilities::CACHE_FENCE),
        ReplicaLog::GroupCursors | ReplicaLog::GroupDeadLetters => {
            base.union(PeerCapabilities::GROUP_FENCE)
        }
        ReplicaLog::Stream | ReplicaLog::Cache => base,
    })
}

#[cfg(test)]
mod tests;
