//! The YAML file override. A field that is absent, or zero where zero would
//! be meaningless, leaves the configured value alone.

use serde::Deserialize;

use super::{ClientConfig, ClientSubQueuePolicy};
use crate::publish::PublishSharding;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(super) struct ClientConfigOverride {
    publish_conn_pool: Option<usize>,
    publish_streams_per_conn: Option<usize>,
    publish_chunk_bytes: Option<usize>,
    publish_queue_depth: Option<usize>,
    publish_inflight_bytes: Option<usize>,
    publish_sharding: Option<String>,
    auth_tenant_id: Option<String>,
    auth_token: Option<String>,
    cache_conn_pool: Option<usize>,
    cache_streams_per_conn: Option<usize>,
    event_conn_pool: Option<usize>,
    cluster_conn_pool: Option<usize>,
    cluster_streams_per_conn: Option<usize>,
    event_conn_recv_window: Option<u64>,
    event_stream_recv_window: Option<u64>,
    event_send_window: Option<u64>,
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    event_router_max_pending: Option<usize>,
    client_sub_queue_capacity: Option<usize>,
    client_sub_queue_policy: Option<String>,
    max_frame_bytes: Option<usize>,
    bench_embed_ts: Option<bool>,
}

impl ClientConfigOverride {
    pub(super) fn apply(&self, config: &mut ClientConfig) {
        if let Some(value) = self.publish_conn_pool
            && value > 0
        {
            config.publish_conn_pool = value;
        }
        if let Some(value) = self.publish_streams_per_conn
            && value > 0
        {
            config.publish_streams_per_conn = value;
        }
        if let Some(value) = self.publish_chunk_bytes
            && value > 0
        {
            config.publish_chunk_bytes = value;
        }
        if let Some(value) = self.publish_queue_depth
            && value > 0
        {
            config.publish_queue_depth = value;
        }
        if let Some(value) = self.publish_inflight_bytes
            && value > 0
        {
            config.publish_inflight_bytes = value;
        }
        if let Some(value) = &self.publish_sharding
            && let Some(parsed) = parse_sharding(value)
        {
            config.publish_sharding = parsed;
        }
        if let Some(value) = &self.auth_tenant_id {
            config.auth_tenant_id = Some(value.clone());
        }
        if let Some(value) = &self.auth_token {
            config.auth_token = Some(value.clone());
        }
        if let Some(value) = self.cache_conn_pool
            && value > 0
        {
            config.cache_conn_pool = value;
        }
        if let Some(value) = self.cache_streams_per_conn
            && value > 0
        {
            config.cache_streams_per_conn = value;
        }
        if let Some(value) = self.event_conn_pool
            && value > 0
        {
            config.event_conn_pool = value;
        }
        if let Some(value) = self.cluster_conn_pool
            && value > 0
        {
            config.cluster_conn_pool = value;
        }
        if let Some(value) = self.cluster_streams_per_conn
            && value > 0
        {
            config.cluster_streams_per_conn = value;
        }
        if let Some(value) = self.event_conn_recv_window
            && value > 0
        {
            config.event_conn_recv_window = value;
        }
        if let Some(value) = self.event_stream_recv_window
            && value > 0
        {
            config.event_stream_recv_window = value;
        }
        if let Some(value) = self.event_send_window
            && value > 0
        {
            config.event_send_window = value;
        }
        if let Some(value) = self.cache_conn_recv_window
            && value > 0
        {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = self.cache_stream_recv_window
            && value > 0
        {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = self.cache_send_window
            && value > 0
        {
            config.cache_send_window = value;
        }
        if let Some(value) = self.event_router_max_pending
            && value > 0
        {
            config.event_router_max_pending = value;
        }
        if let Some(value) = self.client_sub_queue_capacity
            && value > 0
        {
            config.client_sub_queue_capacity = value;
        }
        if let Some(value) = &self.client_sub_queue_policy
            && let Some(policy) = ClientSubQueuePolicy::parse(value.as_str())
        {
            config.client_sub_queue_policy = policy;
        }
        if let Some(value) = self.max_frame_bytes
            && value > 0
        {
            config.max_frame_bytes = value;
        }
        if let Some(value) = self.bench_embed_ts {
            config.bench_embed_ts = value;
        }
    }
}

fn parse_sharding(value: &str) -> Option<PublishSharding> {
    match value {
        "rr" => Some(PublishSharding::RoundRobin),
        "hash_stream" => Some(PublishSharding::HashStream),
        _ => None,
    }
}
