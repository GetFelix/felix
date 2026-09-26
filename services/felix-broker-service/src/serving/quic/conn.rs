//! The broker's QUIC entrypoint: the accept loop, per-connection setup, and
//! dispatch of bi/uni streams to the publish/subscribe/cache handlers.
//! Authentication happens per stream via `BrokerAuth`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::Broker;
use felix_broker::timings as broker_publish_timings;
use felix_transport::{QuicConnection, QuicServer};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::client_error::ClientError;
use super::handlers::publish::{PublishContext, build_tracked_publish_context};
use super::preauth::{AUTH_TIMEOUT_CLOSE_CODE, ConnectionLimit, auth_timeout};
use super::streams::{handle_stream, handle_uni_stream};
use crate::config::BrokerConfig;
use crate::observability::timings;
use crate::serving::auth::BrokerAuth;
use crate::shards::routing::IngressRouter;

/// Serve incoming QUIC connections: accept loop, one task per connection.
///
/// # Errors
/// Propagates QUIC accept errors from the server.
pub async fn serve(
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
) -> Result<()> {
    // Runs until the listener errors. Callers that need to stop accepting without
    // killing in-flight connections should use `serve_with_shutdown`.
    let limit = ConnectionLimit::new(config.max_client_connections);
    serve_with_shutdown(
        server,
        broker,
        config,
        auth,
        CancellationToken::new(),
        TaskTracker::new(),
        // The simple entry point is single-node; a cluster member goes through
        // `serve_with_shutdown` so it can pass its ownership view and its peers.
        ClusterContext::default(),
        limit,
    )
    .await
}

/// Accept loop with cooperative shutdown.
///
/// Same as [`serve`], but stops accepting when `shutdown` is cancelled and registers
/// every per-connection task, and the publish workers behind them, with
/// `connections` so a drain can wait for in-flight work to finish.
///
/// Aborting the accept task kills the loop but says nothing about the connections it
/// already spawned — those are detached and die with the process. Separating "stop
/// admitting" from "wait for in-flight" is what makes a bounded drain possible.
///
/// - Cancelling `shutdown` stops admission only; accepted connections keep running.
/// - The caller owns `connections`, and must `close()` it before `wait()`ing or the
///   wait never resolves.
/// - `limit` is shared by every client listener, so the cap is broker-wide.
#[allow(clippy::too_many_arguments)]
pub async fn serve_with_shutdown(
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    shutdown: CancellationToken,
    connections: TaskTracker,
    cluster: ClusterContext,
    limit: ConnectionLimit,
) -> Result<()> {
    // The publish workers are tracked with the connections, so a drain waiting
    // on `connections` also waits for every queued publish to be written. They
    // exit once the connections and this loop have dropped their senders.
    let publish_ctx =
        build_tracked_publish_context(Arc::clone(&broker), &config, cluster, &connections);
    // Main accept loop: spawn a task per incoming QUIC connection.
    if config.disable_timings {
        timings::set_enabled(false);
        broker_publish_timings::set_enabled(false);
    }
    loop {
        // Accept the next QUIC connection, unless we have been asked to stop
        // admitting. Selecting here rather than checking between accepts means a
        // loop parked on an idle listener still exits promptly.
        let incoming = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("quic accept loop stopping; no longer admitting connections");
                return Ok(());
            }
            incoming = server.accept_incoming() => match incoming {
                Some(incoming) => incoming,
                None => return Err(anyhow::anyhow!("quic endpoint closed")),
            },
        };
        // Decided before the handshake, so a refusal costs one packet.
        let Some(permit) = limit.try_admit() else {
            metrics::counter!("felix_quic_connections_refused_total", "reason" => "limit")
                .increment(1);
            tracing::debug!(
                peer = %incoming.remote_address(),
                "refusing client connection: connection limit reached"
            );
            incoming.refuse();
            continue;
        };
        let broker = Arc::clone(&broker);
        let config = config.clone();
        let auth = Arc::clone(&auth);
        let publish_ctx = publish_ctx.clone();
        // Each connection observes the same shutdown signal, so a drain winds
        // down accepted connections cooperatively rather than waiting for peers
        // that may never disconnect.
        let conn_shutdown = shutdown.clone();
        connections.spawn(async move {
            // The permit is the connection's slot; it goes when this task does.
            let _permit = permit;
            // Handshaken here, not in the accept loop, so a peer that stalls
            // its handshake holds up nobody else. A failed one is that peer's
            // problem and must not stop the listener.
            let handshake = tokio::select! {
                // A drain does not wait on a handshake that has not finished.
                _ = conn_shutdown.cancelled() => return,
                handshake = incoming.accept() => handshake,
            };
            let connection = match handshake {
                Ok(connection) => connection,
                Err(err) => {
                    metrics::counter!("felix_quic_handshake_failures_total").increment(1);
                    tracing::debug!(error = %err, "client handshake failed");
                    return;
                }
            };
            if let Err(err) = handle_connection_with_shutdown(
                broker,
                connection,
                config,
                auth,
                publish_ctx,
                conn_shutdown,
            )
            .await
            {
                tracing::warn!(error = %err, "quic connection handler failed");
            }
        });
    }
}

/// What a broker needs to serve traffic as part of a cluster.
///
/// Both halves or neither: a broker that can resolve a remote owner and has no
/// way to reach it would refuse publishes it should forward. `Default` is a
/// single-node broker, which has neither.
#[derive(Clone, Default)]
pub struct ClusterContext {
    pub ingress: Option<Arc<IngressRouter>>,
    pub peers: Option<Arc<crate::peer::PeerPool>>,
    /// This broker's authority to serve the shards it leads.
    pub lease: Option<Arc<crate::cluster::lease::LeaseState>>,
    /// How far a majority of each shard's replica set has got. Read by a
    /// publish to a `Quorum` stream, which cannot acknowledge until the
    /// majority holds its records.
    pub marks: Option<Arc<crate::replication::quorum::QuorumMarks>>,
    /// Where a client may connect, for answering `Topology`. `None` on a broker
    /// with no cluster behind it, which then advertises no such feature.
    pub client_endpoints: Option<Arc<crate::cluster::client_endpoints::ClientEndpoints>>,
}

/// Handle one QUIC connection, winding down when `shutdown` is cancelled.
///
/// The shutdown token exists because a connection task otherwise ends only
/// when the *peer* disconnects — subscribers hold connections open
/// indefinitely by design, so a drain that just waited for connection tasks
/// would burn its whole deadline and then force-abort exactly the in-flight
/// work it meant to protect. Cancellation stops this connection accepting
/// *new* streams, gives streams already in flight a bounded grace period,
/// then closes the QUIC connection so the peer sees a clean shutdown rather
/// than a disappearance.
pub(crate) async fn handle_connection_with_shutdown(
    broker: Arc<Broker>,
    connection: QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
    publish_ctx: PublishContext,
    shutdown: CancellationToken,
) -> Result<()> {
    // Give this connection its own slice of the shared publish byte budget, its own
    // subscription-count limiter, and its own writer-lane manager, so one connection can't
    // exhaust the process-wide publish budget (`publish_ctx.admission`), open unbounded
    // subscriptions, or (via a stale/colliding cache) share subscription delivery state with
    // an unrelated connection. `workers`/`admission`/`depth` stay the shared, process-wide
    // instances from `build_publish_context`. What is and is not carried through
    // is `PublishContext::for_connection`'s to decide, in one place, because
    // dropping the cluster view here silently disabled shard ownership for every
    // client connection once already.
    let publish_ctx = publish_ctx.for_connection(&config);
    // Tracks the per-stream handler tasks so shutdown can wait for in-flight
    // work instead of dropping it.
    let streams = TaskTracker::new();

    // Periodic path stats for a *healthy* connection. The close-path logs below
    // only fire once the connection is already going away, which is too late to
    // see how path MTU, loss and congestion evolved under load — the numbers that
    // decide whether a throughput problem is transport-side or above it.
    // Off unless `FELIX_CONN_STATS_MS` is set, so it costs nothing in production.
    // Driven from the accept loop rather than a spawned task so it cannot outlive
    // the connection it reports on.
    let stats_interval_ms = std::env::var("FELIX_CONN_STATS_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0);
    let mut stats_ticker =
        stats_interval_ms.map(|ms| tokio::time::interval(Duration::from_millis(ms)));
    // A connection that never authenticates is closed, so an idle or
    // slow-dripping peer cannot hold one of the broker's connection slots.
    let preauth = Arc::clone(&publish_ctx.preauth);
    let auth_deadline = auth_timeout(&config).map(|timeout| tokio::time::Instant::now() + timeout);
    let mut awaiting_auth = auth_deadline.is_some();
    loop {
        // Accept both bidirectional control streams and uni-directional publish streams.
        tokio::select! {
            biased;
            // `udp_tx.ios` vs `udp_tx.datagrams` separates syscall count from
            // datagram count (they diverge only where GSO applies), and
            // `udp_tx.bytes / udp_tx.datagrams` is the effective datagram size —
            // which is what tells you whether path MTU is actually being used.
            _ = async { stats_ticker.as_mut().expect("ticker present").tick().await },
                if stats_ticker.is_some() =>
            {
                let stats = connection.stats();
                tracing::info!(
                    conn = connection.info().id.0,
                    mtu = stats.path.current_mtu,
                    // A small cwnd here is a send-rate cap no queue or
                    // topology change can lift.
                    cwnd = stats.path.cwnd,
                    rtt_us = stats.path.rtt.as_micros() as u64,
                    sent_packets = stats.path.sent_packets,
                    lost_packets = stats.path.lost_packets,
                    congestion_events = stats.path.congestion_events,
                    black_holes = stats.path.black_holes_detected,
                    udp_tx_datagrams = stats.udp_tx.datagrams,
                    udp_tx_bytes = stats.udp_tx.bytes,
                    udp_tx_ios = stats.udp_tx.ios,
                    udp_rx_datagrams = stats.udp_rx.datagrams,
                    udp_rx_bytes = stats.udp_rx.bytes,
                    udp_rx_ios = stats.udp_rx.ios,
                    // Non-zero *_blocked means this endpoint had bytes to send and
                    // was denied credit by the peer -- i.e. the peer is not
                    // consuming fast enough. Zero means flow control is not the
                    // constraint and the sender simply had nothing more to send.
                    tx_data_blocked = stats.frame_tx.data_blocked,
                    tx_stream_data_blocked = stats.frame_tx.stream_data_blocked,
                    rx_data_blocked = stats.frame_rx.data_blocked,
                    rx_stream_data_blocked = stats.frame_rx.stream_data_blocked,
                    tx_max_data = stats.frame_tx.max_data,
                    rx_max_data = stats.frame_rx.max_data,
                    "quic connection path stats"
                );
            }
            authenticated = async {
                tokio::time::timeout_at(
                    auth_deadline.expect("armed only with a deadline"),
                    preauth.authenticated(),
                )
                .await
            }, if awaiting_auth => {
                if authenticated.is_ok() {
                    awaiting_auth = false;
                    continue;
                }
                metrics::counter!("felix_quic_auth_timeout_total").increment(1);
                tracing::debug!(
                    conn = connection.info().id.0,
                    peer = %connection.info().peer_addr,
                    "closing client connection: not authenticated in time"
                );
                connection.close(AUTH_TIMEOUT_CLOSE_CODE.into(), b"authentication timeout");
                streams.close();
                return Ok(());
            }
            _ = shutdown.cancelled() => {
                tracing::info!(
                    stats = ?connection.stats(),
                    "connection draining; no longer accepting streams"
                );
                break;
            }
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, stats = ?connection.stats(), "quic connection closed");
                        break;
                    }
                };
                let broker = Arc::clone(&broker);
                let connection = connection.clone();
                let config = config.clone();
                let auth = Arc::clone(&auth);
                let publish_ctx = publish_ctx.clone();
                streams.spawn(async move {
                    // Dispatch the bidirectional control stream handler.
                    if let Err(err) = handle_stream(
                        broker,
                        connection,
                        config,
                        auth,
                        publish_ctx,
                        send,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic stream handler failed");
                    }
                });
            }
            result = connection.accept_uni() => {
                let recv = match result {
                    Ok(recv) => recv,
                    Err(err) => {
                        tracing::info!(error = %err, stats = ?connection.stats(), "quic connection closed");
                        break;
                    }
                };
                let broker = Arc::clone(&broker);
                let config = config.clone();
                let auth = Arc::clone(&auth);
                let publish_ctx = publish_ctx.clone();
                streams.spawn(async move {
                    // Dispatch the unidirectional publish stream handler.
                    if let Err(err) = handle_uni_stream(
                        broker,
                        config,
                        auth,
                        publish_ctx,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic uni stream handler failed");
                    }
                });
            }
        }
    }

    // Give the streams already accepted a bounded window to finish.
    //
    // The bound is essential, not defensive. Control and subscription streams are
    // long-lived by design: a publisher holds one open and streams requests down
    // it, a subscriber holds one open to receive events. Neither ends until the
    // *client* closes it, so waiting unconditionally would hang exactly as long
    // as waiting for the connection itself did. In-flight requests get the grace
    // window; anything still open after it is ended by closing the connection.
    //
    // Half the process drain budget, so a connection cannot consume the whole
    // allowance and starve the accept loop and metrics server that drain after it.
    let grace =
        Duration::from_millis(config.shutdown_drain_timeout_ms / 2).max(Duration::from_secs(1));
    streams.close();
    let drained = tokio::select! {
        drained = tokio::time::timeout(grace, streams.wait()) => drained,
        never = refuse_new_streams(&connection, preauth.frame_cap(false, config.max_frame_bytes)) => match never {},
    };
    if drained.is_err() {
        tracing::info!(
            ?grace,
            stats = ?connection.stats(),
            "connection drain grace expired with streams still open; closing"
        );
    }

    // Tell the peer this was a deliberate close rather than a vanished server, so
    // it can reconnect elsewhere instead of waiting out an idle timeout.
    connection.close(0u32.into(), b"broker shutting down");
    Ok(())
}

/// Answer control streams opened while this connection drains.
///
/// A client that offered `FEATURE_ERROR_CODES` is told `draining` in answer to
/// its `Auth`, so it can go to another broker now rather than wait out the
/// grace window. Any other client gets what it always got: no answer until the
/// connection closes. Never returns; the caller stops polling it once the grace
/// window ends.
async fn refuse_new_streams(
    connection: &QuicConnection,
    max_frame_bytes: usize,
) -> std::convert::Infallible {
    while let Ok((send, recv)) = connection.accept_bi().await {
        // Not tracked: these hold no work, and waiting on them would stretch
        // the drain to its full grace window every time.
        tokio::spawn(answer_draining(
            connection.clone(),
            send,
            recv,
            max_frame_bytes,
        ));
    }
    std::future::pending().await
}

async fn answer_draining(
    connection: QuicConnection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    max_frame_bytes: usize,
) {
    let mut scratch = bytes::BytesMut::new();
    let first = super::codec::read_message_limited(&mut recv, max_frame_bytes, &mut scratch).await;
    let asked = matches!(
        first,
        Ok(Some(felix_wire::Message::Auth {
            client_features: Some(features),
            ..
        })) if felix_wire::supports_feature(features, felix_wire::FEATURE_ERROR_CODES)
    );
    if asked {
        let refusal = ClientError::draining("broker is shutting down; connect to another broker");
        if super::codec::write_message(&mut send, refusal.into_message())
            .await
            .is_ok()
        {
            let _ = send.finish();
            // Hold the stream until the client has read the answer or the
            // connection goes; dropping it early could reset it first.
            let _ = tokio::time::timeout(Duration::from_secs(5), send.stopped()).await;
        }
        return;
    }
    connection.closed().await;
}

#[cfg(test)]
mod tests;
