//! Folding a YAML config file over the environment.

use std::fs;
use std::io::ErrorKind;

use anyhow::{Context, Result};
use serde::Deserialize;

use super::env::parse_sub_queue_policy;
use super::{BrokerConfig, SubStreamMode, SubscriberLaneShard};

const DEFAULT_BROKER_CONFIG_PATH: &str = "/usr/local/felix/config.yml";

impl BrokerConfig {
    /// The configuration this broker runs with: the environment, then the
    /// file at `FELIX_BROKER_CONFIG` (or the default path, when it exists)
    /// folded over it, then validated as a whole.
    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        let override_path = std::env::var("FELIX_BROKER_CONFIG").ok();
        let config_path = override_path
            .clone()
            .unwrap_or_else(|| DEFAULT_BROKER_CONFIG_PATH.to_string());
        let contents = match fs::read_to_string(&config_path) {
            Ok(contents) => Some(contents),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if override_path.is_some() {
                    return Err(err)
                        .with_context(|| format!("read FELIX_BROKER_CONFIG: {config_path}"));
                }
                None
            }
            Err(err) => {
                if override_path.is_some() {
                    return Err(err)
                        .with_context(|| format!("read FELIX_BROKER_CONFIG: {config_path}"));
                }
                return Err(err).with_context(|| format!("read broker config: {config_path}"));
            }
        };
        if let Some(contents) = contents {
            // YAML overrides allow ops-friendly config files.
            let override_cfg: BrokerConfigOverride =
                serde_yaml_ng::from_str(&contents).with_context(|| "parse broker config yaml")?;
            config.apply(override_cfg)?;
        }
        // After the file, which may move `quic_bind`.
        config.derive_quic_listeners();
        // After both sources, because a combination is only wrong once it is
        // whole: an override may fix what the environment set, or break what it
        // had right.
        config.validate()?;
        Ok(config)
    }

    /// Fold a parsed config file over the values already taken from the
    /// environment.
    ///
    /// Separate from the read so the precedence rules are testable without a
    /// file on disk and the process environment standing in for one.
    pub(super) fn apply(&mut self, override_cfg: BrokerConfigOverride) -> Result<()> {
        let config = self;
        if let Some(value) = override_cfg.quic_bind {
            config.quic_bind = value.parse().with_context(|| "parse quic_bind")?;
        }
        if let Some(value) = override_cfg.metrics_bind {
            config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
        }
        if let Some(value) = override_cfg.controlplane_url {
            config.controlplane_url = Some(value);
        }
        if let Some(value) = override_cfg.controlplane_sync_interval_ms {
            config.controlplane_sync_interval_ms = value;
        }
        if let Some(value) = override_cfg.ack_on_commit {
            config.ack_on_commit = value;
        }
        if let Some(value) = override_cfg.max_frame_bytes {
            config.max_frame_bytes = value;
        }
        if let Some(value) = override_cfg.preauth_max_frame_bytes
            && value > 0
        {
            config.preauth_max_frame_bytes = value;
        }
        if let Some(value) = override_cfg.preauth_max_streams_per_conn
            && value > 0
        {
            config.preauth_max_streams_per_conn = value;
        }
        if let Some(value) = override_cfg.auth_timeout_ms {
            config.auth_timeout_ms = value;
        }
        if let Some(value) = override_cfg.max_client_connections
            && value > 0
        {
            config.max_client_connections = value;
        }
        if let Some(value) = override_cfg.publish_queue_wait_timeout_ms {
            config.publish_queue_wait_timeout_ms = value;
        }
        if let Some(value) = override_cfg.ack_wait_timeout_ms {
            config.ack_wait_timeout_ms = value;
        }
        if let Some(value) = override_cfg.disable_timings {
            config.disable_timings = value;
        }
        if let Some(value) = override_cfg.control_stream_drain_timeout_ms {
            config.control_stream_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_drain_timeout_ms
            && value > 0
        {
            config.shutdown_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_predrain_ms {
            config.shutdown_predrain_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_handoff_timeout_ms {
            config.shutdown_handoff_timeout_ms = value;
        }
        if let Some(value) = override_cfg.cache_conn_recv_window
            && value > 0
        {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = override_cfg.cache_stream_recv_window
            && value > 0
        {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = override_cfg.cache_send_window
            && value > 0
        {
            config.cache_send_window = value;
        }
        if let Some(value) = override_cfg.event_batch_max_events
            && value > 0
        {
            config.event_batch_max_events = value;
        }
        if let Some(value) = override_cfg.event_batch_max_bytes
            && value > 0
        {
            config.event_batch_max_bytes = value;
        }
        if let Some(value) = override_cfg.event_batch_max_delay_us {
            config.event_batch_max_delay_us = value;
        }
        if let Some(value) = override_cfg.fanout_batch_size
            && value > 0
        {
            config.fanout_batch_size = value;
        }
        if let Some(value) = override_cfg.pub_workers_per_conn
            && value > 0
        {
            config.pub_workers_per_conn = value;
        }
        if let Some(value) = override_cfg.pub_queue_depth
            && value > 0
        {
            config.pub_queue_depth = value;
        }
        if let Some(value) = override_cfg.pub_inflight_bytes
            && value > 0
        {
            config.pub_inflight_bytes = value;
        }
        if let Some(value) = override_cfg.pub_conn_inflight_bytes
            && value > 0
        {
            config.pub_conn_inflight_bytes = value;
        }
        if let Some(value) = override_cfg.publish_window {
            config.publish_window = value;
        }
        if let Some(value) = override_cfg.pub_ingress_wait {
            config.pub_ingress_wait = value;
        }
        if let Some(value) = override_cfg.core_shards {
            config.core_shards = value;
        }
        if let Some(value) = override_cfg.subscriber_queue_capacity
            && value > 0
        {
            config.subscriber_queue_capacity = value;
        }
        if let Some(value) = override_cfg.max_subscriptions_per_conn
            && value > 0
        {
            config.max_subscriptions_per_conn = value;
        }
        if let Some(value) = override_cfg.subscriber_queue_policy
            && let Some(parsed) = parse_sub_queue_policy(&value)
        {
            config.subscriber_queue_policy = parsed;
        }
        if let Some(value) = override_cfg.subscriber_writer_lanes
            && value > 0
        {
            config.subscriber_writer_lanes = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_queue_depth
            && value > 0
        {
            config.subscriber_lane_queue_depth = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_queue_policy
            && let Some(parsed) = parse_sub_queue_policy(&value)
        {
            config.subscriber_lane_queue_policy = parsed;
        }
        if let Some(value) = override_cfg.max_subscriber_writer_lanes
            && value > 0
        {
            config.max_subscriber_writer_lanes = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_shard {
            config.subscriber_lane_shard = value;
        }
        if let Some(value) = override_cfg.subscriber_single_writer_per_conn {
            config.subscriber_single_writer_per_conn = value;
        }
        if let Some(value) = override_cfg.subscriber_flush_max_items
            && value > 0
        {
            config.subscriber_flush_max_items = value;
        }
        if let Some(value) = override_cfg.subscriber_flush_max_delay_us {
            config.subscriber_flush_max_delay_us = value;
        }
        if let Some(value) = override_cfg.subscriber_max_bytes_per_write
            && value > 0
        {
            config.subscriber_max_bytes_per_write = value;
        }
        if let Some(value) = override_cfg.sub_streams_per_conn
            && value > 0
        {
            config.sub_streams_per_conn = value;
        }
        if let Some(value) = override_cfg.sub_stream_mode {
            config.sub_stream_mode = value;
        }
        Ok(())
    }
}

/// The settings a config file may override.
///
/// `deny_unknown_fields` because a key nobody reads is a lie: an operator who
/// writes `metrics_bnid` gets the default, no error, and a broker listening
/// somewhere they did not ask for. The same reasoning as the wire protocol's
/// unknown flag bits — a thing not understood is refused, never ignored.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BrokerConfigOverride {
    quic_bind: Option<String>,
    metrics_bind: Option<String>,
    controlplane_url: Option<String>,
    controlplane_sync_interval_ms: Option<u64>,
    ack_on_commit: Option<bool>,
    max_frame_bytes: Option<usize>,
    preauth_max_frame_bytes: Option<usize>,
    preauth_max_streams_per_conn: Option<usize>,
    auth_timeout_ms: Option<u64>,
    max_client_connections: Option<usize>,
    publish_queue_wait_timeout_ms: Option<u64>,
    ack_wait_timeout_ms: Option<u64>,
    disable_timings: Option<bool>,
    control_stream_drain_timeout_ms: Option<u64>,
    shutdown_drain_timeout_ms: Option<u64>,
    shutdown_predrain_ms: Option<u64>,
    shutdown_handoff_timeout_ms: Option<u64>,
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    event_batch_max_events: Option<usize>,
    event_batch_max_bytes: Option<usize>,
    event_batch_max_delay_us: Option<u64>,
    fanout_batch_size: Option<usize>,
    pub_workers_per_conn: Option<usize>,
    pub_queue_depth: Option<usize>,
    pub_inflight_bytes: Option<usize>,
    pub_conn_inflight_bytes: Option<usize>,
    publish_window: Option<u32>,
    pub_ingress_wait: Option<bool>,
    core_shards: Option<usize>,
    subscriber_queue_capacity: Option<usize>,
    max_subscriptions_per_conn: Option<usize>,
    subscriber_queue_policy: Option<String>,
    subscriber_writer_lanes: Option<usize>,
    subscriber_lane_queue_depth: Option<usize>,
    subscriber_lane_queue_policy: Option<String>,
    max_subscriber_writer_lanes: Option<usize>,
    subscriber_lane_shard: Option<SubscriberLaneShard>,
    subscriber_single_writer_per_conn: Option<bool>,
    subscriber_flush_max_items: Option<usize>,
    subscriber_flush_max_delay_us: Option<u64>,
    subscriber_max_bytes_per_write: Option<usize>,
    sub_streams_per_conn: Option<usize>,
    sub_stream_mode: Option<SubStreamMode>,
}
