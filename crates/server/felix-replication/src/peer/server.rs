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
//! None of that is peer *authentication*, which is mTLS, #125.
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::internal::{
    ErrorCode, ForwardPublishError, HelloOk, InternalMessage, PeerCapabilities,
};
use tokio_util::sync::CancellationToken;

use super::KnownCapabilities;
use super::codec::{Incoming, read_frame, write_frame};
use super::config::{INTERNAL_ALPN, PeerTransportConfig};
use super::metrics;
use super::tls;

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
    per_source: std::collections::HashMap<std::net::IpAddr, usize>,
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
            tls,
            app,
            capabilities: config.capabilities(),
            known: KnownCapabilities::default(),
        })
    }

    /// Note what each inbound peer offers in `known`, which the broker's
    /// [`super::PeerPool`] also fills from the handshakes it makes.
    pub fn with_known_capabilities(mut self, known: KnownCapabilities) -> Self {
        self.known = known;
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
        connection.spawn_pump(async move {
            let connection = stream_connection;
            let (mut send, mut recv) = stream;
            loop {
                let request = tokio::select! {
                    _ = shutdown.cancelled() => break,
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
                            if write_frame(&mut send, &refusal).await.is_err() {
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
                            let _ = write_frame(&mut send, &refusal).await;
                            let _ = send.finish();
                            connection.close(
                                3u32.into(),
                                b"peer identity does not match its certificate",
                            );
                            return;
                        }
                        tracing::debug!(peer = %hello.node_id, "internal peer connected");
                        // A plain `Hello` is a peer that predates the bits, so
                        // it can do none of it, and is answered as it expects.
                        offered.known.record(
                            &hello.node_id,
                            hello.capabilities.unwrap_or(PeerCapabilities::NONE),
                        );
                        InternalMessage::HelloOk(HelloOk {
                            correlation_id: hello.correlation_id,
                            node_id: node_id.clone(),
                            capabilities: hello.capabilities.map(|_| offered.capabilities),
                        })
                    }
                    // Refused as a build without the fence would refuse it, so
                    // a broker with the fence turned off is exactly an older one
                    // to whoever sends it.
                    InternalMessage::Fence(fence)
                        if !offered.capabilities.contains(PeerCapabilities::FENCE) =>
                    {
                        metrics::record_served(metrics::OUTCOME_UNSUPPORTED);
                        let refusal = InternalMessage::ForwardPublishError(ForwardPublishError {
                            correlation_id: fence.correlation_id,
                            code: ErrorCode::UnsupportedKind,
                            detail: "this broker does not answer the fence".to_string(),
                        });
                        if write_frame(&mut send, &refusal).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    // Off the pump's runtime; the pump only waits. The stream
                    // stays one request at a time, as the requester expects.
                    request => {
                        let handler = Arc::clone(&handler);
                        match app.spawn(async move { handler.handle(request).await }).await {
                            Ok(response) => response,
                            // A handler that panicked may have applied the
                            // write, so no answer here is safe to retry on.
                            // Drop the stream, as the panic did when it ran
                            // inline.
                            Err(err) => {
                                tracing::warn!(error = %err, "internal request handler failed");
                                metrics::record_served(metrics::OUTCOME_ERROR);
                                break;
                            }
                        }
                    }
                };

                if write_frame(&mut send, &response).await.is_err() {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    break;
                }
                metrics::record_served(metrics::OUTCOME_OK);
            }
            let _ = send.finish();
        });
    }
}
