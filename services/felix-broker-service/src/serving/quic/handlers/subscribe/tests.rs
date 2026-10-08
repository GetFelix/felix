//! Unit and integration tests for the subscribe path: lane routing, writer loops,
//! connection accounting, batching, and drop behavior.

mod conn_counts;
mod connection_writer;
mod feeder;
mod frame_writer;
mod handle_subscribe;
mod lanes;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::Context;
use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::AsyncReadExt;

use super::conn_counts::{
    ACTIVE_SUB_CONN_COUNTS, connection_subscriber_register, connection_subscriber_unregister,
};
use super::lane::ConnectionCommand;
use super::writer::{run_connection_writer, write_parts_many, write_parts_to};
use super::*;
use crate::serving::quic::handlers::publish::AckTimeoutState;

fn make_server_config() -> anyhow::Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])
        .context("generate self-signed cert")?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config = quinn::ServerConfig::with_single_cert(
        vec![cert_der.clone()],
        PrivateKeyDer::Pkcs8(key_der),
    )?;
    Ok((server_config, cert_der))
}

fn make_client_config(cert: CertificateDer<'static>) -> anyhow::Result<quinn::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert).context("add root cert")?;
    Ok(quinn::ClientConfig::with_root_certificates(
        std::sync::Arc::new(roots),
    )?)
}

fn test_config() -> crate::config::BrokerConfig {
    crate::config::BrokerConfig {
        quic_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        metrics_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        controlplane_url: None,
        controlplane_sync_interval_ms: 2000,
        membership: None,
        peer_transport: None,
        ack_on_commit: false,
        max_frame_bytes: 16 * 1024 * 1024,
        publish_queue_wait_timeout_ms: 2000,
        ack_wait_timeout_ms: 2000,
        disable_timings: false,
        control_stream_drain_timeout_ms: 50,
        shutdown_drain_timeout_ms: 25_000,
        cache_send_window: 256 * 1024 * 1024,
        event_batch_max_events: 1,
        event_batch_max_bytes: 64 * 1024,
        event_batch_max_delay_us: 250,
        fanout_batch_size: 1,
        pub_workers_per_conn: 1,
        pub_queue_depth: 8,
        pub_inflight_bytes: 64 * 1024 * 1024,
        pub_conn_inflight_bytes: 16 * 1024 * 1024,
        pub_ingress_wait: false,
        core_shards: 0,
        subscriber_queue_capacity: 8,
        subscriber_queue_capacity_max: 4096,
        max_subscriptions_per_conn: 4096,
        subscriber_queue_policy: felix_broker::SubQueuePolicy::DropNew,
        subscriber_writer_lanes: 4,
        subscriber_lane_queue_depth: 8192,
        subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
        max_subscriber_writer_lanes: 8,
        subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
        subscriber_single_writer_per_conn: true,
        subscriber_flush_max_items: 64,
        subscriber_flush_max_delay_us: 200,
        subscriber_max_bytes_per_write: 256 * 1024,
        sub_streams_per_conn: 4,
        sub_stream_mode: crate::config::SubStreamMode::PerSubscriber,
        ..Default::default()
    }
}

fn make_payload(payload: &[u8]) -> Bytes {
    Bytes::from(payload.to_vec())
}

fn decode_delivery_payloads(frame: &felix_wire::Frame) -> Result<Vec<Bytes>> {
    if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
        return Ok(felix_wire::binary::decode_shared_event_batch(frame)?.payloads);
    }
    Ok(felix_wire::binary::decode_event_batch(frame)?.payloads)
}

// Unique per call, which `SystemTime::now()` is not: consecutive reads can
// return an identical value (observed on macOS, 0 ns between reads). These
// tests key into the process-global `ACTIVE_SUB_CONN_COUNTS`, so colliding
// ids meant tests running in parallel clobbered each other's entries — a
// ~1-in-30 flake that never reproduced when a test ran alone.
//
// The high bit is set so these can never collide with a real quinn
// `stable_id()` in the same process either.
fn unique_test_connection_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    0x8000_0000_0000_0000 | NEXT.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn a_requested_queue_capacity_is_clamped_at_both_ends() {
    assert_eq!(super::grant_queue_capacity(0, 4096), 1);
    assert_eq!(super::grant_queue_capacity(1, 4096), 1);
    assert_eq!(super::grant_queue_capacity(300, 4096), 300);
    assert_eq!(super::grant_queue_capacity(4096, 4096), 4096);
    assert_eq!(super::grant_queue_capacity(u32::MAX, 4096), 4096);
    // A maximum past u32 does not wrap.
    assert_eq!(super::grant_queue_capacity(u32::MAX, usize::MAX), u32::MAX);
}
