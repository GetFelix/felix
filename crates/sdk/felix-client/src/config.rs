//! Client configuration: pool sizes, stream windows, queue limits and
//! credentials.
//!
//! [`ClientConfig::optimized_defaults`] is the starting point;
//! [`ClientConfig::from_env_or_yaml`] layers `FELIX_*` environment variables
//! and then a YAML file over it. Defaults favor throughput for publish and low
//! latency for cache and event streams, with hard caps against oversized
//! frames.

mod defaults;
mod env;
mod yaml;

pub(crate) use defaults::*;

use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_transport::TransportConfig;

use crate::auth::{StaticToken, TokenProvider};
use crate::publish::PublishSharding;
use yaml::ClientConfigOverride;

/// Everything a [`crate::Client`] is built from.
///
/// Build one with [`ClientConfig::optimized_defaults`] or
/// [`ClientConfig::from_env_or_yaml`] and adjust fields from there.
#[derive(Clone)]
pub struct ClientConfig {
    /// TLS and QUIC settings shared by every connection.
    pub quinn: quinn::ClientConfig,
    /// Publish connections per client.
    pub publish_conn_pool: usize,
    /// Publish streams per publish connection, each with its own writer task.
    pub publish_streams_per_conn: usize,
    /// Not used: publish frames are written whole.
    pub publish_chunk_bytes: usize,
    /// Publishes queued per publish stream before the next one waits for room.
    pub publish_queue_depth: usize,
    /// Bytes of publishes sent but not yet answered, across the whole client.
    /// A publish waits for room in this budget before it is queued.
    pub publish_inflight_bytes: usize,
    /// How publishes are spread across the publish streams.
    pub publish_sharding: PublishSharding,
    /// Most publish streams the client opens for one shard each, beside the
    /// pool. A publish whose shard the client knows goes on that shard's own
    /// stream, so a shard stalled on the broker holds up only itself. Shards
    /// past this share the pool. `0` puts every publish on the pool.
    pub publish_shard_streams: usize,
    /// The tenant every stream authenticates as. Required to connect.
    pub auth_tenant_id: Option<String>,
    /// A fixed token for every stream. Clients that run longer than the
    /// token lasts should use `token_provider`.
    pub auth_token: Option<String>,
    /// Supplies the token each time a stream authenticates. Overrides
    /// `auth_token`. See [`crate::RefreshingToken`].
    pub token_provider: Option<Arc<dyn TokenProvider>>,
    /// Cache connections per client.
    pub cache_conn_pool: usize,
    /// Cache streams per cache connection, each with its own worker.
    pub cache_streams_per_conn: usize,
    /// Connections that carry subscriptions and cache watches.
    pub event_conn_pool: usize,
    /// Most connections a [`crate::ClusterClient`] opens to one broker.
    ///
    /// It starts with one, shared by every kind of traffic and every role
    /// that broker plays, and opens another only when the streams on the
    /// existing ones are saturated. The `*_conn_pool` sizes above apply to a
    /// [`crate::Client`] built with [`crate::Client::connect`].
    pub cluster_conn_pool: usize,
    /// Streams one of those connections carries before another is opened
    /// beside it. The broker's stream credit (QUIC `MAX_STREAMS`) is the other
    /// trigger, whichever comes first.
    pub cluster_streams_per_conn: usize,
    /// QUIC receive window per event connection, in bytes.
    pub event_conn_recv_window: u64,
    /// QUIC receive window per event stream, in bytes.
    pub event_stream_recv_window: u64,
    /// QUIC send window per event connection, in bytes.
    pub event_send_window: u64,
    /// QUIC receive window per cache connection, in bytes.
    pub cache_conn_recv_window: u64,
    /// QUIC receive window per cache stream, in bytes.
    pub cache_stream_recv_window: u64,
    /// QUIC send window per cache connection, in bytes.
    pub cache_send_window: u64,
    /// How many registrations and unclaimed event streams one event
    /// connection holds before refusing more.
    pub event_router_max_pending: usize,
    /// Capacity of each subscription's internal queues.
    pub client_sub_queue_capacity: usize,
    /// What a subscription does when one of those queues is full.
    pub client_sub_queue_policy: ClientSubQueuePolicy,
    /// Largest frame the client will read. A bigger one fails the stream
    /// rather than being allocated.
    pub max_frame_bytes: usize,
    /// Append a send timestamp to every publish payload, for end-to-end
    /// latency measurement. Benchmarks only: it changes what subscribers
    /// receive. Has no effect without the `telemetry` feature.
    pub bench_embed_ts: bool,
}

impl ClientConfig {
    /// The defaults, with no credentials set.
    pub fn optimized_defaults(quinn: quinn::ClientConfig) -> Self {
        Self {
            quinn,
            publish_conn_pool: DEFAULT_PUB_CONN_POOL,
            publish_streams_per_conn: DEFAULT_PUB_STREAMS_PER_CONN,
            publish_chunk_bytes: DEFAULT_PUBLISH_CHUNK_BYTES,
            publish_queue_depth: DEFAULT_PUBLISH_QUEUE_DEPTH,
            publish_inflight_bytes: DEFAULT_PUBLISH_INFLIGHT_BYTES,
            // Hash, and measurement says leave it there. Round-robin was
            // re-tested on a generator that actually reads the setting --
            // verified by the binary reporting `sharding=RoundRobin` rather
            // than assumed -- and landed at 912.6 MB/s, inside the band every
            // other valid run occupied. The earlier run that appeared to favour
            // it never applied the override at all (#553).
            publish_sharding: PublishSharding::HashStream,
            publish_shard_streams: DEFAULT_PUB_SHARD_STREAMS,
            auth_tenant_id: None,
            auth_token: None,
            token_provider: None,
            cache_conn_pool: DEFAULT_CACHE_CONN_POOL,
            cache_streams_per_conn: DEFAULT_CACHE_STREAMS_PER_CONN,
            event_conn_pool: DEFAULT_EVENT_CONN_POOL,
            cluster_conn_pool: DEFAULT_CLUSTER_CONN_POOL,
            cluster_streams_per_conn: DEFAULT_CLUSTER_STREAMS_PER_CONN,
            event_conn_recv_window: DEFAULT_EVENT_CONN_RECV_WINDOW,
            event_stream_recv_window: DEFAULT_EVENT_STREAM_RECV_WINDOW,
            event_send_window: DEFAULT_EVENT_SEND_WINDOW,
            cache_conn_recv_window: DEFAULT_CACHE_CONN_RECV_WINDOW,
            cache_stream_recv_window: DEFAULT_CACHE_STREAM_RECV_WINDOW,
            cache_send_window: DEFAULT_CACHE_SEND_WINDOW,
            event_router_max_pending: DEFAULT_EVENT_ROUTER_MAX_PENDING,
            client_sub_queue_capacity: DEFAULT_CLIENT_SUB_QUEUE_CAPACITY,
            client_sub_queue_policy: ClientSubQueuePolicy::DropNew,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            bench_embed_ts: false,
        }
    }

    /// The defaults, then the `FELIX_*` environment overrides, then the YAML
    /// file at `config_path` (or `FELIX_CLIENT_CONFIG`) when one is named.
    ///
    /// Fails if a named file cannot be read or parsed.
    pub fn from_env_or_yaml(quinn: quinn::ClientConfig, config_path: Option<&str>) -> Result<Self> {
        let mut config = Self::from_env(quinn);
        let override_path = config_path
            .map(|value| value.to_string())
            .or_else(|| std::env::var("FELIX_CLIENT_CONFIG").ok());
        let contents = match override_path.as_deref() {
            Some(path) => match fs::read_to_string(path) {
                Ok(contents) => Some(contents),
                Err(err) => {
                    return Err(err).with_context(|| format!("read client config: {path}"));
                }
            },
            None => None,
        };
        if let Some(contents) = contents {
            let override_cfg: ClientConfigOverride =
                serde_yaml_ng::from_str(&contents).context("parse client config yaml")?;
            override_cfg.apply(&mut config);
        }
        Ok(config)
    }

    /// The token source streams authenticate with.
    pub(crate) fn tokens(&self) -> Result<Arc<dyn TokenProvider>> {
        if let Some(provider) = &self.token_provider {
            return Ok(Arc::clone(provider));
        }
        let token = self
            .auth_token
            .clone()
            .context("FELIX_AUTH_TOKEN must be set, or a token provider configured")?;
        Ok(Arc::new(StaticToken(token)))
    }

    pub(crate) fn runtime_config(&self) -> ClientRuntimeConfig {
        ClientRuntimeConfig {
            event_router_max_pending: self.event_router_max_pending,
            max_frame_bytes: self.max_frame_bytes,
            client_sub_queue_capacity: self.client_sub_queue_capacity,
            client_sub_queue_policy: self.client_sub_queue_policy,
            bench_embed_ts: self.bench_embed_ts,
        }
    }
}

/// What a subscription does when its bounded queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientSubQueuePolicy {
    /// Wait for room, which stops reading the event stream until the
    /// application catches up.
    Block,
    /// Drop the arriving item. The default.
    DropNew,
    /// Meant to drop the oldest queued item. It currently drops the arriving
    /// one, exactly as `DropNew` does, and counts it under
    /// `felix_client_sub_queue_drop_old_emulated_total`.
    DropOld,
}

impl ClientSubQueuePolicy {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "block" => Some(Self::Block),
            "drop_new" => Some(Self::DropNew),
            "drop_old" => Some(Self::DropOld),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ClientRuntimeConfig {
    pub(crate) event_router_max_pending: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) client_sub_queue_capacity: usize,
    pub(crate) client_sub_queue_policy: ClientSubQueuePolicy,
    pub(crate) bench_embed_ts: bool,
}

pub(crate) fn event_transport_config(
    mut base: TransportConfig,
    config: &ClientConfig,
) -> TransportConfig {
    base.receive_window = config.event_conn_recv_window;
    base.stream_receive_window = config.event_stream_recv_window;
    base.send_window = config.event_send_window;
    base
}

/// One connection carrying every kind of traffic gets the largest of each
/// window, so none of them is starved by sharing it.
pub(crate) fn shared_transport_config(
    mut base: TransportConfig,
    config: &ClientConfig,
) -> TransportConfig {
    base.receive_window = base
        .receive_window
        .max(config.event_conn_recv_window)
        .max(config.cache_conn_recv_window);
    base.stream_receive_window = base
        .stream_receive_window
        .max(config.event_stream_recv_window)
        .max(config.cache_stream_recv_window);
    base.send_window = base
        .send_window
        .max(config.event_send_window)
        .max(config.cache_send_window);
    base
}

pub(crate) fn cache_transport_config(
    mut base: TransportConfig,
    config: &ClientConfig,
) -> TransportConfig {
    base.receive_window = config.cache_conn_recv_window;
    base.stream_receive_window = config.cache_stream_recv_window;
    base.send_window = config.cache_send_window;
    base
}

#[cfg(test)]
mod tests;
