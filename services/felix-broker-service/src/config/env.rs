//! Reading [`BrokerConfig`] from `FELIX_*` environment variables.

use std::io::ErrorKind;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use felix_broker::SubQueuePolicy;

use super::defaults::*;
use super::membership::{membership_from_env, warn_on_unreachable_advertise};
use super::{BrokerConfig, SubStreamMode, SubscriberLaneShard};

impl BrokerConfig {
    /// Read every setting from its `FELIX_*` variable, with a default for
    /// anything unset. Not validated; [`BrokerConfig::from_env_or_yaml`] is
    /// what startup calls. An unset `FELIX_QUIC_LISTENERS` is 1 here; startup
    /// derives it from the cores.
    pub fn from_env() -> Result<Self> {
        // Environment variables provide defaults for local development.
        let metrics_bind = std::env::var("FELIX_BROKER_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_BROKER_METRICS_BIND")?;
        let quic_bind: SocketAddr = std::env::var("FELIX_QUIC_BIND")
            .unwrap_or_else(|_| "0.0.0.0:5000".to_string())
            .parse()
            .with_context(|| "parse FELIX_QUIC_BIND")?;
        // Unset is filled in by `from_env_or_yaml`, which knows the final
        // ports; see `derive_quic_listeners`.
        let quic_listeners = quic_listeners_from_env(quic_bind)?.unwrap_or(1);
        let io_runtime_threads = std::env::var("FELIX_IO_RUNTIME_THREADS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .trim()
                    .parse::<usize>()
                    .with_context(|| format!("parse FELIX_IO_RUNTIME_THREADS: {value}"))
            })
            .transpose()?;
        let controlplane_url = std::env::var("FELIX_CONTROLPLANE_URL").ok();
        // Poll every 2s by default.
        let controlplane_sync_interval_ms = std::env::var("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2000);
        let controlplane_token = controlplane_token_from_env()?;
        // A value, not a path — which cannot work, so say why rather than accept
        // it and lock the broker out at its first restart.
        if std::env::var("FELIX_NODE_REFRESH_TOKEN")
            .ok()
            .is_some_and(|value| !value.trim().is_empty())
        {
            anyhow::bail!(
                "FELIX_NODE_REFRESH_TOKEN is set, but a refresh token cannot be \
                 passed by value: refreshing spends it and mints a replacement, so \
                 the broker has to write that replacement back somewhere. Use \
                 FELIX_NODE_REFRESH_TOKEN_FILE and make the path writable."
            );
        }
        let node_refresh_token_file = std::env::var("FELIX_NODE_REFRESH_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from);

        let node_token_file = std::env::var("FELIX_NODE_TOKEN_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from);
        let membership = membership_from_env(&controlplane_url, &controlplane_token)?;
        let peer_transport = match &membership {
            Some(membership) => {
                let peer = felix_replication::peer::PeerTransportConfig::from_env(
                    quic_bind,
                    quic_listeners,
                )?;
                warn_on_unreachable_advertise(membership, &peer);
                // Under mTLS the node id is the name on the certificate, and a
                // dialler verifies a peer's certificate against the node id it
                // means to reach -- so it has to be a name a certificate can
                // carry. Refused here rather than at the first dial.
                if peer.tls.is_some()
                    && rustls::pki_types::DnsName::try_from(membership.node_id.as_str()).is_err()
                {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "FELIX_NODE_ID {:?} is not a valid DNS name; with peer mTLS the node \
                             id is the certificate's DNS name, so no label may start or end \
                             with '-' or be empty",
                            membership.node_id
                        ),
                    )
                    .into());
                }
                Some(peer)
            }
            None => None,
        };
        let ack_on_commit = std::env::var("FELIX_ACK_ON_COMMIT")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let record_publishers = std::env::var("FELIX_RECORD_PUBLISHERS")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let max_frame_bytes = std::env::var("FELIX_MAX_FRAME_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);
        let preauth_max_frame_bytes = std::env::var("FELIX_PREAUTH_MAX_FRAME_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PREAUTH_MAX_FRAME_BYTES);
        let preauth_max_streams_per_conn = std::env::var("FELIX_PREAUTH_MAX_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PREAUTH_MAX_STREAMS_PER_CONN);
        // Zero is meaningful: no deadline.
        let auth_timeout_ms = std::env::var("FELIX_AUTH_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_AUTH_TIMEOUT_MS);
        let max_client_connections = std::env::var("FELIX_MAX_CLIENT_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_CLIENT_CONNECTIONS);
        let publish_queue_wait_timeout_ms = std::env::var("FELIX_PUBLISH_QUEUE_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS);
        let ack_wait_timeout_ms = std::env::var("FELIX_ACK_WAIT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_ACK_WAIT_TIMEOUT_MS);
        let group_visibility_timeout_ms = std::env::var("FELIX_GROUP_VISIBILITY_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS);
        let group_max_attempts = std::env::var("FELIX_GROUP_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_MAX_ATTEMPTS);
        let group_max_wait_ms = std::env::var("FELIX_GROUP_MAX_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_GROUP_MAX_WAIT_MS);
        let group_max_visibility_ms = std::env::var("FELIX_GROUP_MAX_VISIBILITY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_MAX_VISIBILITY_MS)
            .max(group_visibility_timeout_ms);
        let group_max_in_flight = std::env::var("FELIX_GROUP_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_MAX_IN_FLIGHT);
        let disable_timings = std::env::var("FELIX_DISABLE_TIMINGS")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_DISABLE_TIMINGS);
        let control_stream_drain_timeout_ms =
            std::env::var("FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS);
        let shutdown_drain_timeout_ms = std::env::var("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS);
        // Zero is meaningful here: no hold-off.
        let shutdown_predrain_ms = std::env::var("FELIX_SHUTDOWN_PREDRAIN_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_PREDRAIN_MS);
        // Zero is meaningful here too: no handoff.
        let shutdown_handoff_timeout_ms = std::env::var("FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_HANDOFF_TIMEOUT_MS);
        let pub_conn_recv_window = std::env::var("FELIX_BROKER_PUB_CONN_RECV_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0);
        let pub_stream_recv_window = std::env::var("FELIX_BROKER_PUB_STREAM_RECV_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0);
        let cache_send_window = std::env::var("FELIX_CACHE_SEND_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_SEND_WINDOW);
        let event_batch_max_events = std::env::var("FELIX_EVENT_BATCH_MAX_EVENTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let event_batch_max_bytes = std::env::var("FELIX_EVENT_BATCH_MAX_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64 * 1024);
        let event_batch_max_delay_us = std::env::var("FELIX_EVENT_BATCH_MAX_DELAY_US")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_EVENT_BATCH_MAX_DELAY_US);
        let fanout_batch_size = std::env::var("FELIX_FANOUT_BATCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let pub_workers_per_conn = std::env::var("FELIX_BROKER_PUB_WORKERS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_WORKERS_PER_CONN);
        let pub_flush_concurrency = std::env::var("FELIX_BROKER_PUB_FLUSH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PUB_FLUSH_CONCURRENCY);
        let pub_queue_depth = std::env::var("FELIX_BROKER_PUB_QUEUE_DEPTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_QUEUE_DEPTH);
        let pub_inflight_bytes = std::env::var("FELIX_BROKER_PUBLISH_INFLIGHT_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_INFLIGHT_BYTES);
        let pub_conn_inflight_bytes = std::env::var("FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_CONN_INFLIGHT_BYTES);
        let publish_window = std::env::var("FELIX_BROKER_PUBLISH_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(DEFAULT_PUBLISH_WINDOW);
        let pub_ingress_wait = std::env::var("FELIX_PUB_INGRESS_WAIT")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let core_shards = std::env::var("FELIX_CORE_SHARDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let subscriber_queue_capacity = std::env::var("FELIX_SUBSCRIBER_QUEUE_CAPACITY")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_QUEUE_CAPACITY").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_CAPACITY);
        let subscriber_queue_capacity_max = std::env::var("FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_CAPACITY_MAX);
        let max_subscriptions_per_conn = std::env::var("FELIX_MAX_SUBSCRIPTIONS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN);
        let subscriber_queue_policy = std::env::var("FELIX_SUB_QUEUE_POLICY")
            .ok()
            .and_then(|value| parse_sub_queue_policy(&value))
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_POLICY);
        let subscriber_writer_lanes = std::env::var("FELIX_SUB_EGRESS_LANES")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_WRITER_LANES").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_WRITER_LANES);
        let subscriber_lane_queue_depth = std::env::var("FELIX_SUB_QUEUE_BOUND")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_DEPTH").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH);
        let subscriber_lane_queue_policy = std::env::var("FELIX_SUB_QUEUE_MODE")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_POLICY").ok())
            .and_then(|value| parse_sub_queue_policy(&value))
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY);
        let max_subscriber_writer_lanes = std::env::var("FELIX_MAX_SUB_WRITER_LANES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_SUBSCRIBER_WRITER_LANES);
        let subscriber_lane_shard = std::env::var("FELIX_SUB_LANE_SHARD")
            .ok()
            .and_then(|value| SubscriberLaneShard::parse_env(value.as_str()))
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_SHARD);
        let subscriber_single_writer_per_conn = std::env::var("FELIX_SUB_SINGLE_WRITER_PER_CONN")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let subscriber_flush_max_items = std::env::var("FELIX_SUB_FLUSH_MAX_ITEMS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS);
        let subscriber_flush_max_delay_us = std::env::var("FELIX_SUB_FLUSH_MAX_DELAY_US")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US);
        let subscriber_max_bytes_per_write = std::env::var("FELIX_SUB_MAX_BYTES_PER_WRITE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE);
        let sub_streams_per_conn = std::env::var("FELIX_SUB_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUB_STREAMS_PER_CONN);
        let publish_quorum_timeout_ms = std::env::var("FELIX_PUBLISH_QUORUM_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS);
        let shard_move_hold_ms = std::env::var("FELIX_SHARD_MOVE_HOLD_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHARD_MOVE_HOLD_MS);
        let shard_move_hold_max = std::env::var("FELIX_SHARD_MOVE_HOLD_MAX")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_SHARD_MOVE_HOLD_MAX);
        let sub_stream_mode = std::env::var("FELIX_SUB_STREAM_MODE")
            .ok()
            .and_then(|value| SubStreamMode::parse_env(value.as_str()))
            .unwrap_or(DEFAULT_SUB_STREAM_MODE);
        let replication_rebuild_max_concurrent =
            std::env::var("FELIX_REPLICATION_REBUILD_MAX_CONCURRENT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT);
        let replication_rebuild_bytes_per_sec =
            std::env::var("FELIX_REPLICATION_REBUILD_BYTES_PER_SEC")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC);
        let shard_move_bytes_per_sec = std::env::var("FELIX_SHARD_MOVE_BYTES_PER_SEC")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHARD_MOVE_BYTES_PER_SEC);
        Ok(Self {
            quic_bind,
            quic_listeners,
            io_runtime_threads,
            metrics_bind,
            controlplane_url,
            controlplane_token,
            node_refresh_token_file,
            node_token_file,
            controlplane_sync_interval_ms,
            controlplane_ca: super::tls::controlplane_ca_from_env(),
            client_tls: super::ClientTlsConfig::from_env()?,
            membership,
            peer_transport,
            ack_on_commit,
            record_publishers,
            max_frame_bytes,
            preauth_max_frame_bytes,
            preauth_max_streams_per_conn,
            auth_timeout_ms,
            max_client_connections,
            publish_queue_wait_timeout_ms,
            ack_wait_timeout_ms,
            group_visibility_timeout_ms,
            group_max_attempts,
            group_max_wait_ms,
            group_max_visibility_ms,
            group_max_in_flight,
            disable_timings,
            control_stream_drain_timeout_ms,
            shutdown_drain_timeout_ms,
            shutdown_predrain_ms,
            shutdown_handoff_timeout_ms,
            pub_conn_recv_window,
            pub_stream_recv_window,
            cache_send_window,
            event_batch_max_events,
            event_batch_max_bytes,
            event_batch_max_delay_us,
            fanout_batch_size,
            pub_workers_per_conn,
            pub_flush_concurrency,
            pub_queue_depth,
            pub_inflight_bytes,
            pub_conn_inflight_bytes,
            publish_window,
            pub_ingress_wait,
            core_shards,
            subscriber_queue_capacity,
            subscriber_queue_capacity_max,
            max_subscriptions_per_conn,
            subscriber_queue_policy,
            subscriber_writer_lanes,
            subscriber_lane_queue_depth,
            subscriber_lane_queue_policy,
            max_subscriber_writer_lanes,
            subscriber_lane_shard,
            subscriber_single_writer_per_conn,
            subscriber_flush_max_items,
            subscriber_flush_max_delay_us,
            subscriber_max_bytes_per_write,
            sub_streams_per_conn,
            sub_stream_mode,
            publish_quorum_timeout_ms,
            replication_rebuild_max_concurrent,
            replication_rebuild_bytes_per_sec,
            shard_move_hold_ms,
            shard_move_hold_max,
            shard_move_bytes_per_sec,
            limits: super::LimitsConfig::from_env()?,
        })
    }
}

pub(super) fn parse_sub_queue_policy(value: &str) -> Option<SubQueuePolicy> {
    match value {
        "block" => Some(SubQueuePolicy::Block),
        "drop_new" => Some(SubQueuePolicy::DropNew),
        "drop_old" => Some(SubQueuePolicy::DropOld),
        _ => None,
    }
}

/// An explicit `FELIX_QUIC_LISTENERS`, or `None` when unset.
///
/// Refused rather than clamped when the range would wrap past port 65535: a
/// broker that silently bound fewer listeners than asked would read as the
/// feature not working, and the operator has a port number to fix.
fn quic_listeners_from_env(quic_bind: SocketAddr) -> Result<Option<usize>> {
    let Some(raw) = std::env::var("FELIX_QUIC_LISTENERS")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let count: usize = raw
        .trim()
        .parse()
        .with_context(|| format!("parse FELIX_QUIC_LISTENERS: {raw}"))?;
    if count == 0 {
        anyhow::bail!("FELIX_QUIC_LISTENERS is 0; a broker with no listener serves nothing");
    }
    let last = u32::from(quic_bind.port()) + count as u32 - 1;
    if last > u32::from(u16::MAX) {
        anyhow::bail!(
            "FELIX_QUIC_LISTENERS ({count}) from FELIX_QUIC_BIND port {} runs past 65535",
            quic_bind.port(),
        );
    }
    Ok(Some(count))
}

impl BrokerConfig {
    /// With `FELIX_QUIC_LISTENERS` unset, set the listener count from the
    /// cores this process may use, shortened to fit the free ports.
    ///
    /// Only the startup path calls this. A config from [`BrokerConfig::from_env`]
    /// keeps one listener, because code that binds its own server and passes
    /// the config to `serve` would otherwise advertise ports nobody bound.
    pub(super) fn derive_quic_listeners(&mut self) {
        let unset =
            std::env::var("FELIX_QUIC_LISTENERS").map_or(true, |value| value.trim().is_empty());
        if !unset {
            return;
        }
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let wanted = default_quic_listeners(cores);
        let internal = self.peer_transport.as_ref().map(|peer| peer.bind.port());
        self.quic_listeners = fit_listener_range(wanted, self.quic_bind.port(), internal);
        if self.quic_listeners < wanted {
            tracing::info!(
                wanted,
                listeners = self.quic_listeners,
                "FELIX_QUIC_LISTENERS is unset and the port range from FELIX_QUIC_BIND runs \
                 into FELIX_INTERNAL_BIND or port 65535, or is port 0; binding fewer listeners",
            );
        }
    }
}

/// `max(1, min(cores / 2, 4))`. Past half the cores, listeners contend with
/// the broker's own threads and throughput drops.
pub(super) fn default_quic_listeners(cores: usize) -> usize {
    (cores / 2).clamp(1, 4)
}

/// Shorten a derived listener count so its ports stop before the internal
/// listener and port 65535. Never below 1; a clash on the first port is left
/// for the peer transport's own check to report. Port 0 asks the OS for any
/// port, which has no consecutive range after it, so it gets one listener.
pub(super) fn fit_listener_range(wanted: usize, first: u16, internal: Option<u16>) -> usize {
    if first == 0 {
        return 1;
    }
    let mut room = usize::from(u16::MAX - first) + 1;
    if let Some(internal) = internal.filter(|port| *port >= first) {
        room = room.min(usize::from(internal - first));
    }
    wanted.min(room).max(1)
}

/// The control-plane credential, from `FELIX_NODE_TOKEN_FILE` or
/// `FELIX_NODE_TOKEN`; empty when neither is set.
///
/// The file form exists so a token can arrive as a mounted secret rather than
/// an environment variable visible in a process listing. Whitespace is trimmed,
/// and a blank value is treated as no credential rather than as an empty one.
fn controlplane_token_from_env() -> std::io::Result<String> {
    match std::env::var("FELIX_NODE_TOKEN_FILE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(path) => Ok(std::fs::read_to_string(&path)
            .map_err(|err| {
                std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("read FELIX_NODE_TOKEN_FILE {path}: {err}"),
                )
            })?
            .trim()
            .to_string()),
        None => Ok(std::env::var("FELIX_NODE_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .unwrap_or_default()),
    }
}

/// `FELIX_QUORUM_READS`: whether a `Quorum` read confirms this broker still
/// leads by a round of fences to its replicas (`majority`, the default), or
/// trusts the lease (`lease`), the faster path that is only as safe as the
/// clocks. The round applies once the fleet has finalized `lease_free_reads`;
/// before that every read is on the lease.
pub(crate) fn quorum_reads_by_lease() -> Result<bool> {
    match std::env::var("FELIX_QUORUM_READS")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("" | "majority") => Ok(false),
        Some("lease") => Ok(true),
        Some(other) => {
            anyhow::bail!("FELIX_QUORUM_READS must be majority or lease, not {other:?}")
        }
    }
}
