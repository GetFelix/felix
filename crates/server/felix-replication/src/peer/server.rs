//! The inbound half of the internal transport: the listener peers connect to.
//!
//! Separate from the client-facing listener, and that separation is the point.
//! Three things keep the two roles apart, in the order a connection meets them:
//!
//! 1. **A separate port.** Startup refuses a configuration where the two share
//!    one (see [`super::config::PeerTransportConfig`]).
//! 2. **ALPN.** Both ends negotiate `felix-internal/1`; the client-facing
//!    endpoint negotiates none, so there is no protocol in common and TLS
//!    refuses the handshake. A client pointed at this port never reaches frame
//!    decoding, and never touches broker state.
//! 3. **A distinct magic.** A client frame fails to decode here rather than
//!    parsing into something plausible.
//!
//! None of that is peer *authentication*. That is mTLS, configured in `tls.rs`.
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_storage::StorageId;
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::internal::{
    ErrorCode, ForwardCacheError, ForwardPublishError, HelloOk, InternalMessage, PeerCapabilities,
    Pong, RaftAnswer, ReplicateError,
};
use futures::FutureExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::KnownCapabilities;
use super::codec::{Incoming, read_frame, write_frame};
use super::config::{INTERNAL_ALPN, PeerTransportConfig};
use super::metrics;
use super::tls;

/// Forwards one stream may have running at once. A requester's own in-flight
/// limit spread over its forward streams comes to about this, and it bounds
/// what one stream can make this broker hold.
const MAX_CONCURRENT_FORWARDS_PER_STREAM: usize = 256;

/// Fences, tail fetches and pre-votes one stream may hold at once. The same
/// as the requester's default in-flight limit per peer, so a requester at its
/// defaults reaches it only with requests it timed out on still running here.
pub(super) const MAX_CONTROLS_PER_STREAM: usize = 1024;

/// Raft messages one stream may have being answered at once. Past it a
/// message is refused, and Raft sends it again.
const MAX_RAFT_PER_STREAM: usize = 1024;

/// Of those, tail fetches that may run at once. Each answer holds up to 4 MiB
/// of records until it is written; the rest wait their turn off the stream.
const MAX_CONCURRENT_FETCHES_PER_STREAM: usize = 16;

/// Record batches one stream may have being stored at once. Stored one at a
/// time, a batch waited behind every other batch on its stream, so a leader's
/// round trip grew with how many shards it was shipping while the store itself
/// stayed fast (#1968). Past this the stream stops reading until one finishes,
/// so a stream holds at most this many batches plus the one it read.
pub(super) const MAX_CONCURRENT_BATCHES_PER_STREAM: usize = 16;

/// Record batches being stored at once across every connection. Without it
/// the batches a follower holds grow with peers and streams; with it, at most
/// this many, each about the leader's 1 MiB batch budget plus one record.
pub(super) const MAX_SERVED_BATCHES: usize = 256;

/// Record batches one peer node may have being stored at once, on any of its
/// connections. A peer whose stores stall would otherwise take every
/// broker-wide permit and stop all other peers' streams reading. 64 is four
/// streams at their own limit, and about three times the depth a leader
/// reached on one lane at failover scale (mean 16 to 23).
pub(super) const MAX_SERVED_BATCHES_PER_PEER: usize = 64;

/// Per-peer batch limits by the peer's node id, so a reconnect does not
/// reset one. One entry per node that ever greeted this broker.
pub(super) type PeerBatches =
    Arc<parking_lot::Mutex<felix_common::random::HashMap<Arc<str>, Arc<Semaphore>>>>;

/// What answers a peer's request.
///
/// The broker implements it with `BrokerPeerHandler` (#106). A listener built
/// without one answers every forward with `Unavailable`, which is the honest
/// answer: this broker can be reached, and cannot apply a forwarded write.
#[async_trait::async_trait]
pub trait PeerRequestHandler: Send + Sync + 'static {
    /// Answer one request. Must always produce a terminal response — the
    /// protocol has no "no answer" outcome, and a requester that gets none has
    /// to wait out its timeout.
    async fn handle(&self, request: InternalMessage) -> InternalMessage;

    /// Answer one request from `peer`, the node id it gave in its `Hello`,
    /// which offered `caller`. `peer` is `None` when this broker does not
    /// keep ballots, or the peer has not said who it is; `caller` is empty
    /// before the `Hello` and for a peer that predates the bits. Handlers
    /// that do not care who asked need not implement it.
    async fn handle_from(
        &self,
        peer: Option<Arc<str>>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> InternalMessage {
        let _ = (peer, caller);
        self.handle(request).await
    }
}

/// What answers a peer's Raft messages: the broker's group runtime.
///
/// Given to [`PeerServer::with_raft`]. A listener without one does not offer
/// [`PeerCapabilities::RAFT`], so no peer sends it a Raft message.
#[async_trait::async_trait]
pub trait RaftHandler: Send + Sync + 'static {
    /// Answer one `Raft` frame's body with the body of its `RaftAnswer`.
    async fn answer(&self, body: bytes::Bytes) -> bytes::Bytes;
}

/// Answers every forwarded request with `Unavailable`.
///
/// The listener is useful before forwarding is wired: peers can establish
/// connections, negotiate, and observe a live broker, and they are told plainly
/// that it cannot serve rather than being left to time out.
pub struct UnavailableHandler;

#[async_trait::async_trait]
impl PeerRequestHandler for UnavailableHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: request.correlation_id(),
            code: ErrorCode::Unavailable,
            detail: "this broker does not apply forwarded publishes yet".to_string(),
        })
    }
}

/// How many inbound connections are open, in total and per source address.
///
/// The accept loop had no bound at all: QUIC caps streams per connection and
/// the decoder caps a frame, but nothing capped connections, so one caller
/// could make this broker hold 64 MiB × 1024 streams × as many connections as
/// it opened (#504).
///
/// Per-source as well as total, because the total alone does not stop one peer
/// consuming the whole allowance — and a peer looping on a reconnect bug is
/// likelier than a hostile one, and starves the rest of the cluster just the
/// same.
#[derive(Debug, Default)]
struct Admitted {
    total: usize,
    per_source: felix_common::random::HashMap<std::net::IpAddr, usize>,
}

/// Holds a connection's place in the count, and gives it back on drop.
///
/// A guard rather than a decrement at the end of `serve_connection`: that
/// function has several exits and a task can be cancelled between any of them,
/// and a count that leaks is a broker that stops accepting peers after an
/// uptime nobody can correlate with anything.
struct Admission {
    counts: Arc<parking_lot::Mutex<Admitted>>,
    source: std::net::IpAddr,
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut counts = self.counts.lock();
        counts.total = counts.total.saturating_sub(1);
        if let Some(count) = counts.per_source.get_mut(&self.source) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                // Or the map grows by one entry per address ever seen, which is
                // its own slow exhaustion.
                counts.per_source.remove(&self.source);
            }
        }
    }
}

impl Admitted {
    /// Take a place for `source`, or say which limit refused it.
    fn admit(
        counts: &Arc<parking_lot::Mutex<Self>>,
        source: std::net::IpAddr,
        max_total: usize,
        max_per_source: usize,
    ) -> Result<Admission, &'static str> {
        let mut guard = counts.lock();
        if guard.total >= max_total {
            return Err("total");
        }
        let for_source = guard.per_source.entry(source).or_insert(0);
        if *for_source >= max_per_source {
            return Err("per_source");
        }
        *for_source += 1;
        guard.total += 1;
        Ok(Admission {
            counts: Arc::clone(counts),
            source,
        })
    }
}

/// The broker-internal listener.
pub struct PeerServer {
    server: QuicServer,
    node_id: String,
    handler: Arc<dyn PeerRequestHandler>,
    max_inbound_connections: usize,
    max_inbound_per_source: usize,
    handler_timeout: std::time::Duration,
    served_forwards: Arc<Semaphore>,
    served_batches: Arc<Semaphore>,
    peer_batches: PeerBatches,
    /// `Some` when peers must present a certificate, and the name a peer
    /// claims in `Hello` is held to it.
    tls: Option<Arc<tls::PeerTls>>,
    /// The runtime handlers run on. Stream pumps sit on the QUIC I/O runtime
    /// when one is enabled, which is a single thread shared with the
    /// endpoint's driver; an auth check or quorum wait there stalls every
    /// peer's socket I/O.
    app: tokio::runtime::Handle,
    /// What this broker offers peers, and where it notes what they offer.
    capabilities: PeerCapabilities,
    known: KnownCapabilities,
    /// This broker's storage identity, given to a caller that asks for it.
    identity: Option<StorageId>,
    /// This process's node incarnation, `None` until it registers.
    incarnation: Option<tokio::sync::watch::Receiver<Option<u64>>>,
    /// Cancelled once this broker serves clients. `None` serves from the start.
    serving: Option<CancellationToken>,
    raft: Option<Arc<dyn RaftHandler>>,
}

impl PeerServer {
    /// Bind the internal listener without peer authentication: encrypted, and
    /// anything that can reach the port is a peer. See [`Self::bind_with_tls`],
    /// including which runtime it must be called from.
    pub fn bind(
        node_id: String,
        config: &PeerTransportConfig,
        handler: Arc<dyn PeerRequestHandler>,
    ) -> Result<Self> {
        Self::bind_with_tls(node_id, config, handler, None)
    }

    /// Bind the internal listener, requiring every peer to present a
    /// certificate from the configured CA when `tls` is set.
    ///
    /// Must be called from the runtime request handlers should run on.
    pub fn bind_with_tls(
        node_id: String,
        config: &PeerTransportConfig,
        handler: Arc<dyn PeerRequestHandler>,
        tls: Option<Arc<tls::PeerTls>>,
    ) -> Result<Self> {
        let app = tokio::runtime::Handle::try_current()
            .context("the internal listener must be bound from inside a tokio runtime")?;
        let server = QuicServer::bind(
            config.bind,
            tls::server_config(tls.as_deref())?,
            config.quic_transport(),
        )
        .context("bind internal QUIC listener")?;
        Ok(Self {
            server,
            node_id,
            handler,
            max_inbound_connections: config.max_inbound_connections,
            max_inbound_per_source: config.max_inbound_per_source,
            handler_timeout: config.request_timeout,
            served_forwards: Arc::new(Semaphore::new(config.max_served_forwards.max(1))),
            served_batches: Arc::new(Semaphore::new(MAX_SERVED_BATCHES)),
            peer_batches: PeerBatches::default(),
            tls,
            app,
            capabilities: config.capabilities(),
            known: KnownCapabilities::default(),
            identity: None,
            incarnation: None,
            serving: None,
            raft: None,
        })
    }

    /// Note what each inbound peer offers in `known`, which the broker's
    /// [`super::PeerPool`] also fills from the handshakes it makes.
    pub fn with_known_capabilities(mut self, known: KnownCapabilities) -> Self {
        self.known = known;
        self
    }

    /// Answer a handshake that offers [`PeerCapabilities::STORAGE_IDENTITY`]
    /// with `identity`. Without it the bit is never answered.
    pub fn with_storage_identity(mut self, identity: StorageId) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Answer a handshake that offers [`PeerCapabilities::NODE_INCARNATION`]
    /// with the incarnation `incarnation` holds, and close each connection so
    /// answered once it changes. Without it the bit is never answered.
    ///
    /// The close is what keeps a caller's view current: it learned the
    /// incarnation once, at the handshake, and dials again to learn the new
    /// one.
    pub fn with_node_incarnation(
        mut self,
        incarnation: tokio::sync::watch::Receiver<Option<u64>>,
    ) -> Self {
        self.incarnation = Some(incarnation);
        self
    }

    /// Refuse pings until `serving` is cancelled, so a broker that cannot serve
    /// clients yet reads as down to the followers that watch it. A follower
    /// stands for a shard only while it suspects the leader, and a restarted
    /// leader that answered pings before it could serve would hold its shards
    /// with nobody serving them.
    pub fn with_serving(mut self, serving: CancellationToken) -> Self {
        self.serving = Some(serving);
        self
    }

    /// Answer peers' Raft messages with `raft`, and offer
    /// [`PeerCapabilities::RAFT`] so they send them.
    pub fn with_raft(mut self, raft: Arc<dyn RaftHandler>) -> Self {
        self.capabilities = self.capabilities.union(PeerCapabilities::RAFT);
        self.raft = Some(raft);
        self
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.server.local_addr()
    }

    /// Accept peer connections until cancelled.
    pub async fn serve(self, shutdown: CancellationToken) {
        let counts: Arc<parking_lot::Mutex<Admitted>> = Arc::default();
        loop {
            let connection = tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = self.server.accept() => match accepted {
                    Ok(connection) => connection,
                    Err(err) => {
                        // An accept failure is per-connection; the endpoint is
                        // still bound, so stopping the loop would silently take
                        // this broker out of the cluster.
                        tracing::debug!(error = %err, "internal accept failed");
                        metrics::record_inbound_rejected("handshake");
                        continue;
                    }
                },
            };

            // Admission before anything else is spawned for it. A connection
            // refused here has cost one handshake; one accepted can cost a
            // thousand streams of buffered frames, and the point is to decide
            // before that.
            //
            // Refused rather than queued: a peer told no can back off, where
            // one left waiting cannot tell a busy broker from a stuck one.
            let source = connection.info().peer_addr.ip();
            let admission = match Admitted::admit(
                &counts,
                source,
                self.max_inbound_connections,
                self.max_inbound_per_source,
            ) {
                Ok(admission) => admission,
                Err(limit) => {
                    tracing::warn!(
                        peer = %connection.info().peer_addr,
                        limit,
                        "refusing an internal connection: at the inbound limit",
                    );
                    metrics::record_inbound_rejected(limit);
                    connection.close(2u32.into(), b"internal connection limit reached");
                    continue;
                }
            };

            // A backstop, not the enforcement: TLS has already refused any
            // connection that did not negotiate the internal ALPN, so this only
            // fires if that ever stops being true.
            if connection.negotiated_protocol().as_deref() != Some(INTERNAL_ALPN) {
                tracing::warn!(
                    peer = %connection.info().peer_addr,
                    "refusing connection on the internal listener: not an internal peer",
                );
                metrics::record_inbound_rejected("alpn");
                connection.close(1u32.into(), b"internal listener requires felix-internal/1");
                continue;
            }

            // Close this connection when the broker shuts down, from a task of
            // its own. Cancellation alone is not enough: a handler already
            // running is not interruptible, so a peer would otherwise wait out
            // its request timeout to learn this broker had gone.
            let closer = connection.clone();
            let closing = shutdown.clone();
            connection.spawn_pump(async move {
                closing.cancelled().await;
                closer.close(0u32.into(), b"broker shutting down");
            });

            let node_id = self.node_id.clone();
            let handler = Arc::clone(&self.handler);
            let shutdown = shutdown.clone();
            let served = connection.clone();
            let tls = self.tls.clone();
            let app = self.app.clone();
            let offered = Offered {
                capabilities: self.capabilities,
                known: self.known.clone(),
                identity: self.identity,
                incarnation: self.incarnation.clone(),
                serving: self.serving.clone(),
                handler_timeout: self.handler_timeout,
                served_forwards: Arc::clone(&self.served_forwards),
                served_batches: Arc::clone(&self.served_batches),
                peer_batches: Arc::clone(&self.peer_batches),
                raft: self.raft.clone(),
            };
            connection.spawn_pump(async move {
                // The guard lives as long as the connection is served, and
                // gives its place back however that ends.
                let _admission = admission;
                serve_connection(served, node_id, handler, tls, offered, app, shutdown).await;
            });
        }
    }
}

/// What this broker offers in the handshake, and where it notes what the
/// peer offered.
#[derive(Clone)]
struct Offered {
    capabilities: PeerCapabilities,
    known: KnownCapabilities,
    identity: Option<StorageId>,
    incarnation: Option<tokio::sync::watch::Receiver<Option<u64>>>,
    serving: Option<CancellationToken>,
    /// How long a request answered in turn may hold its stream: the
    /// requester's own timeout, after which no one is waiting for the answer.
    handler_timeout: std::time::Duration,
    /// Forwards being served across every connection.
    served_forwards: Arc<Semaphore>,
    /// Record batches being stored across every connection.
    served_batches: Arc<Semaphore>,
    /// Record batches being stored, by the node that sent them.
    peer_batches: PeerBatches,
    raft: Option<Arc<dyn RaftHandler>>,
}

/// Serve every stream a peer opens on one connection.
async fn serve_connection(
    connection: QuicConnection,
    node_id: String,
    handler: Arc<dyn PeerRequestHandler>,
    tls: Option<Arc<tls::PeerTls>>,
    offered: Offered,
    app: tokio::runtime::Handle,
    shutdown: CancellationToken,
) {
    // Read once: the chain does not change for the life of a connection.
    let peer_certs = tls.as_ref().and_then(|_| connection.peer_certificates());
    // Who the peer said it is, for every stream it opens after its `Hello`.
    // The pool waits for `HelloOk` before it sends anything else, so no
    // request on this connection arrives before this is set.
    let identity: Arc<parking_lot::Mutex<Option<Arc<str>>>> = Arc::default();
    let connection_batches = Arc::new(Semaphore::new(MAX_SERVED_BATCHES_PER_PEER));
    // Whether the peer replaces a stream it gets no answer on, from its
    // `Hello`. Until it says so, a stream is never dropped under it.
    let renews = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // What the peer offered in its `Hello`, which shapes its answers.
    let caller = Arc::new(std::sync::atomic::AtomicU64::new(0));
    loop {
        let stream = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = connection.accept_bi() => match accepted {
                Ok(stream) => stream,
                // The peer closed, or the connection dropped. Either way this
                // connection is finished; the accept loop keeps running.
                Err(_) => break,
            },
        };

        let node_id = node_id.clone();
        let handler = Arc::clone(&handler);
        let shutdown = shutdown.clone();
        let tls = tls.clone();
        let peer_certs = peer_certs.clone();
        let stream_connection = connection.clone();
        let app = app.clone();
        let offered = offered.clone();
        let identity = Arc::clone(&identity);
        let connection_batches = Arc::clone(&connection_batches);
        let renews = Arc::clone(&renews);
        let caller = Arc::clone(&caller);
        connection.spawn_pump(async move {
            let connection = stream_connection;
            let (send, mut recv) = stream;
            // Shared with the forwards answered out of order below. One
            // `write_frame` per lock, so frames never interleave.
            let send = Arc::new(tokio::sync::Mutex::new(send));
            let forwards = Arc::new(Semaphore::new(MAX_CONCURRENT_FORWARDS_PER_STREAM));
            let controls = Arc::new(Semaphore::new(MAX_CONTROLS_PER_STREAM));
            let fetches = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES_PER_STREAM));
            let batches = Arc::new(Semaphore::new(MAX_CONCURRENT_BATCHES_PER_STREAM));
            let rafts = Arc::new(Semaphore::new(MAX_RAFT_PER_STREAM));
            // Cancelled when a batch's handler overran or panicked: the
            // stream is given up as a request answered in turn would be.
            let given_up = CancellationToken::new();
            loop {
                let request = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = given_up.cancelled() => break,
                    frame = read_frame(&mut recv) => match frame {
                        Ok(Incoming::Message(request)) => request,
                        // A kind from a later build. The body has been read, so
                        // the stream is still on a frame boundary — refuse this
                        // one and carry on, rather than dropping a lane every
                        // other in-flight request is sharing. This is what makes
                        // "add a kind" an additive change rather than a cutover.
                        Ok(Incoming::UnknownKind { kind, correlation_id }) => {
                            tracing::debug!(kind, "refusing a frame kind this broker does not know");
                            metrics::record_served(metrics::OUTCOME_UNSUPPORTED);
                            let refusal = InternalMessage::ForwardPublishError(ForwardPublishError {
                                correlation_id,
                                code: ErrorCode::UnsupportedKind,
                                detail: format!("this broker does not know frame kind {kind}"),
                            });
                            if write_frame(&mut *send.lock().await, &refusal).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        Ok(Incoming::Eof) => break,
                        Err(err) => {
                            // Not a frame of ours at all — wrong magic, a
                            // version this build does not speak, or a body that
                            // stopped short. The bytes are not laid out the way
                            // this assumes, so there is no boundary to skip to.
                            tracing::debug!(error = %err, "internal stream ended");
                            metrics::record_served(metrics::OUTCOME_ERROR);
                            break;
                        }
                    },
                };

                // Read per request: the `Hello` that sets it comes first.
                let offered_by = PeerCapabilities::from_bits(
                    caller.load(std::sync::atomic::Ordering::Relaxed),
                );
                let response = match request {
                    // Answered here rather than by the handler: identity is the
                    // transport's to assert, and it must work before the broker
                    // is able to serve anything.
                    InternalMessage::Hello(hello) => {
                        // The handshake proved the peer holds a certificate
                        // from the CA; this proves it is the broker it says it
                        // is. A refusal closes the connection: a peer that
                        // claims a name its certificate does not carry is not
                        // a peer that gets to try again on the next stream.
                        if let Some(tls) = &tls
                            && let Err(detail) =
                                tls.verify_identity(peer_certs.as_deref(), &hello.node_id)
                        {
                            tracing::warn!(
                                claimed = %hello.node_id,
                                peer = %connection.info().peer_addr,
                                %detail,
                                "refusing an internal peer: its certificate does not match \
                                 the identity it claims",
                            );
                            metrics::record_inbound_rejected("identity");
                            let refusal =
                                InternalMessage::ForwardPublishError(ForwardPublishError {
                                    correlation_id: hello.correlation_id,
                                    code: ErrorCode::Unauthorized,
                                    detail,
                                });
                            let mut send = send.lock().await;
                            let _ = write_frame(&mut send, &refusal).await;
                            let _ = send.finish();
                            connection.close(
                                3u32.into(),
                                b"peer identity does not match its certificate",
                            );
                            return;
                        }
                        tracing::debug!(peer = %hello.node_id, "internal peer connected");
                        *identity.lock() = Some(Arc::from(hello.node_id.as_str()));
                        renews.store(
                            hello
                                .capabilities
                                .is_some_and(|c| c.contains(PeerCapabilities::LANE_RENEWAL)),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        caller.store(
                            hello.capabilities.unwrap_or(PeerCapabilities::NONE).bits(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        // A plain `Hello` is a peer that predates the bits, so
                        // it can do none of it, and is answered as it expects.
                        offered.known.record(
                            &hello.node_id,
                            hello.capabilities.unwrap_or(PeerCapabilities::NONE),
                        );
                        // Only to a caller that offered the bit, so an older
                        // one reads the answer it always has.
                        let asked = |bit| hello.capabilities.is_some_and(|c| c.contains(bit));
                        let identity = offered
                            .identity
                            .filter(|_| asked(PeerCapabilities::STORAGE_IDENTITY));
                        let incarnation = offered
                            .incarnation
                            .clone()
                            .filter(|_| asked(PeerCapabilities::NODE_INCARNATION));
                        let mut answered = offered.capabilities;
                        if identity.is_some() {
                            answered = answered.union(PeerCapabilities::STORAGE_IDENTITY);
                        }
                        let mut at = None;
                        if let Some(mut watch) = incarnation {
                            answered = answered.union(PeerCapabilities::NODE_INCARNATION);
                            let current = *watch.borrow_and_update();
                            at = current;
                            // The caller keeps what it was told for the life
                            // of the connection, so a new registration has to
                            // end the connection for the caller to learn it in
                            // bounded time. Forwarded publishes on it go too;
                            // a broker registers only at startup or once it
                            // was marked down, when it is not leading.
                            let closer = connection.clone();
                            connection.spawn_pump(async move {
                                tokio::select! {
                                    _ = closer.closed() => {}
                                    changed = watch.wait_for(|now| *now != current) => {
                                        if changed.is_ok() {
                                            closer.close(4u32.into(), b"node incarnation changed");
                                        }
                                    }
                                }
                            });
                        }
                        InternalMessage::HelloOk(HelloOk {
                            correlation_id: hello.correlation_id,
                            node_id: node_id.clone(),
                            capabilities: hello.capabilities.map(|_| answered),
                            identity: identity.map(|id| id.to_bytes()),
                            incarnation: at,
                        })
                    }
                    // A kind that needs a capability this broker never offered
                    // (ELECTION, today) is refused as a build that predates it
                    // would, not decoded and acted on.
                    request
                        if request
                            .required_capability()
                            .is_some_and(|needed| !offered.capabilities.contains(needed)) =>
                    {
                        metrics::record_served(metrics::OUTCOME_UNSUPPORTED);
                        let refusal = InternalMessage::ForwardPublishError(ForwardPublishError {
                            correlation_id: request.correlation_id(),
                            code: ErrorCode::UnsupportedKind,
                            detail: "this broker does not offer the capability this kind needs"
                                .to_string(),
                        });
                        if write_frame(&mut *send.lock().await, &refusal).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    // Refused as a build without the fence would refuse it, so
                    // a broker with the fence turned off is exactly an older one
                    // to whoever sends it.
                    request @ (InternalMessage::Fence(_)
                    | InternalMessage::SealFence(_)
                    | InternalMessage::ReplicateFetch(_))
                        if !offered.capabilities.contains(PeerCapabilities::FENCE) =>
                    {
                        metrics::record_served(metrics::OUTCOME_UNSUPPORTED);
                        let refusal = InternalMessage::ForwardPublishError(ForwardPublishError {
                            correlation_id: request.correlation_id(),
                            code: ErrorCode::UnsupportedKind,
                            detail: "this broker does not answer the fence".to_string(),
                        });
                        if write_frame(&mut *send.lock().await, &refusal).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    // Answered by the serving runtime, not the pump, so a
                    // broker whose runtime is stuck reads as unreachable. Out
                    // of order and without a permit, so a ping never waits
                    // behind a slow fence: that would read as a dead leader.
                    InternalMessage::Ping(ping) => {
                        let send = Arc::clone(&send);
                        // Anything but a `Pong` is silence to a pinger, an
                        // older one included.
                        let serving = offered
                            .serving
                            .as_ref()
                            .is_none_or(CancellationToken::is_cancelled);
                        app.spawn(async move {
                            let pong = if serving {
                                InternalMessage::Pong(Pong {
                                    correlation_id: ping.correlation_id,
                                })
                            } else {
                                InternalMessage::ForwardPublishError(ForwardPublishError {
                                    correlation_id: ping.correlation_id,
                                    code: ErrorCode::Unavailable,
                                    detail: "this broker is not serving yet".to_string(),
                                })
                            };
                            answer_out_of_order(&send, offered_by, async { pong }).await;
                        });
                        continue;
                    }
                    // Fences, tail fetches and pre-votes run side by side and
                    // are answered as they finish, so a promotion fence slow
                    // to persist does not hold up a read round's fence or a
                    // ping behind it (#1796). None relies on its place on the
                    // stream: a requester waits for the answer before anything
                    // that depends on it. Past the limit a request is
                    // refused rather than read later, so a ping is never
                    // left unread behind them.
                    request @ (InternalMessage::Fence(_)
                    | InternalMessage::SealFence(_)
                    | InternalMessage::SealAck(_)
                    | InternalMessage::PreVote(_)
                    | InternalMessage::ReplicateFetch(_)) => {
                        let Ok(queued) = Arc::clone(&controls).try_acquire_owned() else {
                            metrics::record_served(metrics::OUTCOME_REFUSED);
                            let refusal = InternalMessage::ReplicateError(ReplicateError {
                                correlation_id: request.correlation_id(),
                                code: ErrorCode::Overload,
                                expected_offset: 0,
                                detail: "this broker is serving as many control requests as it allows"
                                    .to_string(),
                                reason: None,
                            });
                            // Off the loop, so a refusal never waits behind
                            // a large fetch answer and the pings after it.
                            let send = Arc::clone(&send);
                            app.spawn(async move {
                                answer_out_of_order(&send, offered_by, async { refusal }).await;
                            });
                            continue;
                        };
                        let fetching = matches!(request, InternalMessage::ReplicateFetch(_))
                            .then(|| Arc::clone(&fetches));
                        let handler = Arc::clone(&handler);
                        let peer = ballot_identity(&offered, &identity);
                        let send = Arc::clone(&send);
                        app.spawn(async move {
                            let _queued = queued;
                            // The semaphore is never closed.
                            let _running = match fetching {
                                Some(fetches) => fetches.acquire_owned().await.ok(),
                                None => None,
                            };
                            answer_out_of_order(&send, offered_by, handler.handle_from(peer, offered_by, request)).await;
                        });
                        continue;
                    }
                    // Raft messages are answered as they finish. openraft
                    // matches each answer to its own call and never relies
                    // on a stream's order, and an append can wait out an
                    // fsync that a vote or heartbeat behind it should not.
                    InternalMessage::Raft(call) => {
                        let (Some(raft), Ok(running)) =
                            (offered.raft.clone(), Arc::clone(&rafts).try_acquire_owned())
                        else {
                            metrics::record_served(metrics::OUTCOME_REFUSED);
                            let refusal = InternalMessage::ReplicateError(ReplicateError {
                                correlation_id: call.correlation_id,
                                code: ErrorCode::Overload,
                                expected_offset: 0,
                                detail: "this broker is answering as many Raft messages as it allows"
                                    .to_string(),
                                reason: None,
                            });
                            let send = Arc::clone(&send);
                            app.spawn(async move {
                                answer_out_of_order(&send, offered_by, async { refusal }).await;
                            });
                            continue;
                        };
                        // Bounded by the sender's own deadline: past it the
                        // sender has given up, and a member stuck on storage
                        // would otherwise hold the permit for good. The
                        // refusal clears the sender's waiter.
                        let deadline = super::raft_deadline(&call.body);
                        let send = Arc::clone(&send);
                        app.spawn(async move {
                            let _running = running;
                            let correlation_id = call.correlation_id;
                            answer_out_of_order(&send, offered_by, async move {
                                match tokio::time::timeout(deadline, raft.answer(call.body)).await {
                                    Ok(body) => InternalMessage::RaftAnswer(RaftAnswer {
                                        correlation_id,
                                        body,
                                    }),
                                    Err(_) => InternalMessage::ReplicateError(ReplicateError {
                                        correlation_id,
                                        code: ErrorCode::Unavailable,
                                        expected_offset: 0,
                                        detail: "the Raft message was not answered in time"
                                            .to_string(),
                                        reason: None,
                                    }),
                                }
                            })
                            .await;
                        });
                        continue;
                    }
                    // A forward can sit out a whole quorum wait, and that wait
                    // may need replication a peer has queued behind it on this
                    // stream (#1458). So forwards are answered as they finish,
                    // matched by correlation id, and the stream reads on.
                    // Requesters never relied on a stream's order: the ingress
                    // holds a shard's next forward until this one is answered.
                    request @ (InternalMessage::ForwardPublish(_)
                    | InternalMessage::ForwardCacheOp(_)
                    | InternalMessage::LeaseForwardCacheOp(_)) => {
                        // Refused rather than queued: waiting would stall the
                        // stream, and an older requester puts replication on
                        // it too. Nothing was applied, so the forwarder may
                        // retry.
                        let permits = Arc::clone(&forwards)
                            .try_acquire_owned()
                            .ok()
                            .zip(Arc::clone(&offered.served_forwards).try_acquire_owned().ok());
                        let Some(permit) = permits else {
                            metrics::record_served(metrics::OUTCOME_REFUSED);
                            let refusal = overloaded(&request);
                            if write_frame(&mut *send.lock().await, &refusal).await.is_err() {
                                break;
                            }
                            continue;
                        };
                        let handler = Arc::clone(&handler);
                        let peer = ballot_identity(&offered, &identity);
                        let send = Arc::clone(&send);
                        app.spawn(async move {
                            let _permit = permit;
                            answer_out_of_order(&send, offered_by, handler.handle_from(peer, offered_by, request)).await;
                        });
                        continue;
                    }
                    // Batches for different logs have nothing to wait on each
                    // other for, so they are stored side by side and answered
                    // as they finish. Stream order never kept a log's batches
                    // in turn: a requester spreads its requests over several
                    // streams, and a leader has one batch in flight per
                    // follower and log. Bootstraps and rebuilds stay in turn
                    // below, as before.
                    request if request.is_records() => {
                        // The stream's permit first, so no stream takes more
                        // of the broker-wide limit than its own.
                        let permit = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = given_up.cancelled() => break,
                            permit = Arc::clone(&batches).acquire_owned() => {
                                permit.expect("the batch semaphore is never closed")
                            }
                        };
                        // Then the peer's, so a peer past its share waits here
                        // without holding a broker-wide permit.
                        let of_peer = peer_batch_limit(&offered, &identity, &connection_batches);
                        let of_peer = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = given_up.cancelled() => break,
                            permit = of_peer.acquire_owned() => {
                                permit.expect("the batch semaphore is never closed")
                            }
                        };
                        let served = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = given_up.cancelled() => break,
                            permit = Arc::clone(&offered.served_batches).acquire_owned() => {
                                permit.expect("the batch semaphore is never closed")
                            }
                        };
                        let handler = Arc::clone(&handler);
                        let peer = ballot_identity(&offered, &identity);
                        let send = Arc::clone(&send);
                        let caller = Arc::clone(&caller);
                        let given_up = given_up.clone();
                        let bound = handler_bound(&renews, &offered);
                        // The peer and broker-wide permits go with the handler,
                        // which runs on after an overrun and still holds its batch.
                        let handling = app.spawn(async move {
                            let _served = (of_peer, served);
                            handler.handle_from(peer, offered_by, request).await
                        });
                        app.spawn(async move {
                            let _permit = permit;
                            let handled = tokio::time::timeout(bound, handling).await;
                            let Ok(Ok(response)) = handled else {
                                // As for a request answered in turn: the
                                // requester has given up, and a panicked
                                // handler may have stored the batch. The
                                // handler finishes on its own.
                                tracing::warn!(
                                    timeout = ?bound,
                                    "replication batch handler failed or overran; dropping its stream",
                                );
                                metrics::record_served(metrics::OUTCOME_ERROR);
                                given_up.cancel();
                                return;
                            };
                            let response = for_caller(
                                response,
                                PeerCapabilities::from_bits(
                                    caller.load(std::sync::atomic::Ordering::Relaxed),
                                ),
                            );
                            match write_frame(&mut *send.lock().await, &response).await {
                                Ok(()) => metrics::record_served(metrics::OUTCOME_OK),
                                Err(_) => metrics::record_served(metrics::OUTCOME_ERROR),
                            }
                        });
                        continue;
                    }
                    // Off the pump's runtime; the pump only waits. Everything
                    // else on a stream is answered one request at a time.
                    request => {
                        let handler = Arc::clone(&handler);
                        let peer = ballot_identity(&offered, &identity);
                        let handling =
                            app.spawn(async move { handler.handle_from(peer, offered_by, request).await });
                        let bound = handler_bound(&renews, &offered);
                        let handled = tokio::time::timeout(bound, handling).await.ok();
                        match handled.and_then(|joined| joined.ok()).ok_or(()) {
                            Ok(response) => for_caller(
                                response,
                                PeerCapabilities::from_bits(
                                    caller.load(std::sync::atomic::Ordering::Relaxed),
                                ),
                            ),
                            // A handler that panicked may have applied the
                            // write, so no answer here is safe to retry on.
                            // Drop the stream, as the panic did when it ran
                            // inline.
                            Err(()) => {
                                // Past the requester's own timeout it has given
                                // up, and every request behind this one on the
                                // stream would wait too. Drop the stream and let
                                // the handler finish on its own; the requester
                                // opens another.
                                tracing::warn!(
                                    timeout = ?offered.handler_timeout,
                                    "internal request handler failed or overran; dropping its stream",
                                );
                                metrics::record_served(metrics::OUTCOME_ERROR);
                                break;
                            }
                        }
                    }
                };

                if write_frame(&mut *send.lock().await, &response).await.is_err() {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    break;
                }
                metrics::record_served(metrics::OUTCOME_OK);
            }
            // Let the requests still running answer before the stream ends.
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = async {
                    tokio::join!(
                        forwards.acquire_many(MAX_CONCURRENT_FORWARDS_PER_STREAM as u32),
                        controls.acquire_many(MAX_CONTROLS_PER_STREAM as u32),
                        batches.acquire_many(MAX_CONCURRENT_BATCHES_PER_STREAM as u32),
                        rafts.acquire_many(MAX_RAFT_PER_STREAM as u32),
                    )
                } => {}
            }
            let _ = send.lock().await.finish();
        });
    }
}

/// `response` as a caller that offered `caller` reads it: a learner's answer
/// to one that did not offer [`PeerCapabilities::LEARNER_ACKS`], or a
/// kept-ballot fence answer to one that did not offer
/// [`PeerCapabilities::KEPT_BALLOTS`], or an attested one to one that did not
/// offer [`PeerCapabilities::UNIFIED_FENCE`], goes as the original kind, which such
/// a caller reads as it always has; and a refusal to one that did not offer
/// [`PeerCapabilities::REFUSAL_REASONS`] goes without its reason, a body such
/// a caller would not decode.
pub fn for_caller(response: InternalMessage, caller: PeerCapabilities) -> InternalMessage {
    match response {
        InternalMessage::ReplicateOk(ok)
            if ok.learner && !caller.contains(PeerCapabilities::LEARNER_ACKS) =>
        {
            InternalMessage::ReplicateOk(felix_wire::internal::ReplicateOk {
                learner: false,
                ..ok
            })
        }
        InternalMessage::FenceOk(mut ok) => {
            if !caller.contains(PeerCapabilities::UNIFIED_FENCE) {
                ok.attestation = None;
            }
            if !caller.contains(PeerCapabilities::KEPT_BALLOTS) {
                ok.kept_ballot = false;
            }
            InternalMessage::FenceOk(ok)
        }
        InternalMessage::ReplicateError(err)
            if err.reason.is_some() && !caller.contains(PeerCapabilities::REFUSAL_REASONS) =>
        {
            InternalMessage::ReplicateError(felix_wire::internal::ReplicateError {
                reason: None,
                ..err
            })
        }
        response => response,
    }
}

/// Write the answer to a request handed off so its stream could read on,
/// shaped for a caller that offered `caller` ([`for_caller`]).
///
/// A handler that panicked may have applied the write, so the stream is ended
/// rather than answered, as a panic in a request answered in turn ends it.
async fn answer_out_of_order(
    send: &tokio::sync::Mutex<quinn::SendStream>,
    caller: PeerCapabilities,
    answering: impl std::future::Future<Output = InternalMessage>,
) {
    let answered = AssertUnwindSafe(answering).catch_unwind().await;
    let mut send = send.lock().await;
    let Ok(response) = answered else {
        tracing::warn!("internal request handler panicked");
        metrics::record_served(metrics::OUTCOME_ERROR);
        let _ = send.finish();
        return;
    };
    match write_frame(&mut send, &for_caller(response, caller)).await {
        Ok(()) => metrics::record_served(metrics::OUTCOME_OK),
        Err(_) => metrics::record_served(metrics::OUTCOME_ERROR),
    }
}

/// How long a handler answered on a stream may run before the stream is given
/// up. Only a requester that replaces streams may have one dropped under it;
/// an older one would lose the lane for the life of the connection, so it
/// waits.
fn handler_bound(renews: &std::sync::atomic::AtomicBool, offered: &Offered) -> std::time::Duration {
    if renews.load(std::sync::atomic::Ordering::Relaxed) {
        offered.handler_timeout
    } else {
        std::time::Duration::MAX
    }
}

/// A forward refused at a limit, in the shape of the request.
fn overloaded(request: &InternalMessage) -> InternalMessage {
    let correlation_id = request.correlation_id();
    let detail = "this broker is serving as many forwards as it allows".to_string();
    match request {
        InternalMessage::ForwardCacheOp(_) | InternalMessage::LeaseForwardCacheOp(_) => {
            InternalMessage::ForwardCacheError(ForwardCacheError {
                correlation_id,
                code: ErrorCode::Overload,
                detail,
            })
        }
        _ => InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id,
            code: ErrorCode::Overload,
            detail,
        }),
    }
}

/// The limit for the peer a stream belongs to. A stream that has not said
/// hello has no node id yet, so its connection gets a limit of its own.
fn peer_batch_limit(
    offered: &Offered,
    identity: &parking_lot::Mutex<Option<Arc<str>>>,
    connection_limit: &Arc<Semaphore>,
) -> Arc<Semaphore> {
    let Some(node) = identity.lock().clone() else {
        return Arc::clone(connection_limit);
    };
    peer_limit(&offered.peer_batches, node)
}

/// The node's limit, made on first use. Entries nothing holds and that have
/// every permit back are dropped here, so changing node ids do not grow the map.
pub(super) fn peer_limit(peers: &PeerBatches, node: Arc<str>) -> Arc<Semaphore> {
    let mut peers = peers.lock();
    peers.retain(|_, limit| {
        Arc::strong_count(limit) > 1 || limit.available_permits() < MAX_SERVED_BATCHES_PER_PEER
    });
    Arc::clone(
        peers
            .entry(node)
            .or_insert_with(|| Arc::new(Semaphore::new(MAX_SERVED_BATCHES_PER_PEER))),
    )
}

/// Who the peer said it is, for a handler that keeps ballots. Ballots name a
/// leader by this identity, so it goes to the handler only when this broker
/// keeps them.
fn ballot_identity(
    offered: &Offered,
    identity: &parking_lot::Mutex<Option<Arc<str>>>,
) -> Option<Arc<str>> {
    offered
        .capabilities
        .contains(PeerCapabilities::BALLOTS)
        .then(|| identity.lock().clone())
        .flatten()
}
