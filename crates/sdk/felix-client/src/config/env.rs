//! The `FELIX_*` environment overrides.

use super::{ClientConfig, ClientSubQueuePolicy};
use crate::publish::PublishSharding;

impl ClientConfig {
    pub(super) fn from_env(quinn: quinn::ClientConfig) -> Self {
        let mut config = Self::optimized_defaults(quinn);
        if let Some(value) = read_usize_env("FELIX_PUB_CONN_POOL") {
            config.publish_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUB_STREAMS_PER_CONN") {
            config.publish_streams_per_conn = value;
        }
        // `0` is meaningful here: it turns shard streams off.
        if let Some(value) = std::env::var("FELIX_PUB_SHARD_STREAMS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
        {
            config.publish_shard_streams = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUBLISH_CHUNK_BYTES") {
            config.publish_chunk_bytes = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUBLISH_QUEUE_DEPTH") {
            config.publish_queue_depth = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUBLISH_INFLIGHT_BYTES") {
            config.publish_inflight_bytes = value;
        }
        if let Some(value) = PublishSharding::from_env() {
            config.publish_sharding = value;
        }
        if let Some(value) = read_usize_env("FELIX_CACHE_CONN_POOL") {
            config.cache_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_CACHE_STREAMS_PER_CONN") {
            config.cache_streams_per_conn = value;
        }
        if let Some(value) =
            read_usize_env("FELIX_EVENT_CONN_POOL").or_else(|| read_usize_env("FELIX_SUB_CONNS"))
        {
            config.event_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_CLUSTER_CONN_POOL") {
            config.cluster_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_CLUSTER_STREAMS_PER_CONN") {
            config.cluster_streams_per_conn = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_CONN_RECV_WINDOW") {
            config.event_conn_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_STREAM_RECV_WINDOW") {
            config.event_stream_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_SEND_WINDOW") {
            config.event_send_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_CONN_RECV_WINDOW") {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_STREAM_RECV_WINDOW") {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_SEND_WINDOW") {
            config.cache_send_window = value;
        }
        if let Some(value) = read_usize_env("FELIX_EVENT_ROUTER_MAX_PENDING") {
            config.event_router_max_pending = value;
        }
        if let Some(value) = read_usize_env("FELIX_CLIENT_SUB_QUEUE_CAPACITY") {
            config.client_sub_queue_capacity = value;
        }
        if let Ok(value) = std::env::var("FELIX_CLIENT_SUB_QUEUE_POLICY")
            && let Some(policy) = ClientSubQueuePolicy::parse(value.as_str())
        {
            config.client_sub_queue_policy = policy;
        }
        if let Some(value) = read_usize_env("FELIX_MAX_FRAME_BYTES") {
            config.max_frame_bytes = value;
        }
        if let Some(value) = read_bool_env("FELIX_BENCH_EMBED_TS") {
            config.bench_embed_ts = value;
        }
        if let Ok(value) = std::env::var("FELIX_AUTH_TENANT")
            && !value.trim().is_empty()
        {
            config.auth_tenant_id = Some(value);
        }
        if let Ok(value) = std::env::var("FELIX_AUTH_TOKEN")
            && !value.trim().is_empty()
        {
            config.auth_token = Some(value);
        }
        config
    }
}

fn read_u64_env(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn read_usize_env(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn read_bool_env(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}
