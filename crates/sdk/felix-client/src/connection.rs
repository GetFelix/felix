//! The connections a client holds to a broker, and how streams on them open.
//!
//! Every stream authenticates on open (`handshake`). A `node` is the set of
//! connections to one broker that streams are placed on: several streams
//! share a connection, and a connection is added only when the ones there are
//! saturated. Each connection runs a router that hands the broker's event
//! streams to the subscriptions waiting for them (`event_router`).

mod event_router;
mod handshake;
mod node;

pub(crate) use event_router::{EventRouterCommand, spawn_event_router_with_config};
pub(crate) use handshake::{Credentials, Negotiated};
pub(crate) use node::{NodeConnections, NodeLimits, OpenedStream, StreamLease};

use std::net::SocketAddr;

use felix_transport::QuicConnection;

/// Where a client's connections to a broker should go, given what the broker
/// said about its listeners.
///
/// `dialled` always comes first and is always present, even if the broker did
/// not name its port: it is the address that demonstrably works, and a pool
/// that abandoned it on the strength of an advertisement would be trusting a
/// claim it has not tested.
///
/// Only the *port* is taken from the advertisement. The host stays the one
/// already connected to, so an `AuthOk` cannot move a client to a different
/// machine -- that would be a redirect, which is a much larger claim than "I
/// also listen here" and belongs to `NotLeader`.
pub(crate) fn listener_targets(dialled: SocketAddr, ports: &[u16]) -> Vec<SocketAddr> {
    let mut targets = vec![dialled];
    for port in ports {
        let mut candidate = dialled;
        candidate.set_port(*port);
        if !targets.contains(&candidate) {
            targets.push(candidate);
        }
    }
    targets
}

/// Periodic path stats for client-side connections, mirroring the broker's
/// `FELIX_CONN_STATS_MS` logging. The client is the sender on the publish path,
/// so its cwnd/rtt is invisible from broker-side stats. Off unless set.
pub(crate) fn spawn_conn_stats_logger(connection: &QuicConnection, role: &'static str) {
    let Some(interval_ms) = std::env::var("FELIX_CONN_STATS_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    else {
        return;
    };
    let connection = connection.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            ticker.tick().await;
            if connection.close_reason().is_some() {
                break;
            }
            let stats = connection.stats();
            tracing::info!(
                role,
                conn = connection.info().id.0,
                mtu = stats.path.current_mtu,
                cwnd = stats.path.cwnd,
                rtt_us = stats.path.rtt.as_micros() as u64,
                congestion_events = stats.path.congestion_events,
                lost_packets = stats.path.lost_packets,
                udp_tx_bytes = stats.udp_tx.bytes,
                udp_tx_datagrams = stats.udp_tx.datagrams,
                tx_data_blocked = stats.frame_tx.data_blocked,
                tx_stream_data_blocked = stats.frame_tx.stream_data_blocked,
                "client quic connection path stats"
            );
        }
    });
}

#[cfg(test)]
mod tests;
