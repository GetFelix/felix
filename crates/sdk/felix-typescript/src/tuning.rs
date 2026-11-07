//! The client's tuning knobs: what a subscription asks of the broker, how
//! queues behave, and the transport's pool sizes, windows and timeouts. They
//! are set once at `connect`, so they travel as one object and not as
//! positional arguments.

use felix_client::{ClientConfig, ClientSubQueuePolicy, PublishSharding};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::args::{duration_ms, whole};
use crate::errors::invalid;

/// Everything `connect` can tune beyond credentials and reconnecting. A field
/// left out keeps the client's default.
#[napi(object)]
pub struct ClientTuning {
    pub publishers: Option<bool>,
    pub timestamps: Option<bool>,
    pub record_headers: Option<bool>,
    pub client_sub_queue_policy: Option<String>,
    pub client_sub_queue_capacity: Option<f64>,
    pub broker_sub_queue_capacity: Option<f64>,
    pub publish_conn_pool: Option<f64>,
    pub publish_streams_per_conn: Option<f64>,
    pub publish_queue_depth: Option<f64>,
    pub publish_inflight_bytes: Option<f64>,
    pub publish_sharding: Option<String>,
    pub publish_shard_streams: Option<f64>,
    pub cache_conn_pool: Option<f64>,
    pub cache_streams_per_conn: Option<f64>,
    pub event_conn_pool: Option<f64>,
    pub cluster_conn_pool: Option<f64>,
    pub cluster_streams_per_conn: Option<f64>,
    pub event_conn_recv_window: Option<f64>,
    pub event_stream_recv_window: Option<f64>,
    pub event_send_window: Option<f64>,
    pub cache_conn_recv_window: Option<f64>,
    pub cache_stream_recv_window: Option<f64>,
    pub cache_send_window: Option<f64>,
    pub event_router_max_pending: Option<f64>,
    pub max_frame_bytes: Option<f64>,
    pub ack_on_commit: Option<bool>,
    pub connect_timeout: Option<f64>,
    pub finish_timeout: Option<f64>,
}

impl ClientTuning {
    /// Write the fields the caller set into `config`, refusing a value that
    /// does not fit before anything connects.
    pub(crate) fn apply(self, config: &mut ClientConfig) -> Result<()> {
        if let Some(on) = self.publishers {
            config.publishers = on;
        }
        if let Some(on) = self.timestamps {
            config.timestamps = on;
        }
        if let Some(on) = self.record_headers {
            config.record_headers = on;
        }
        if let Some(on) = self.ack_on_commit {
            config.ack_on_commit = on;
        }
        if let Some(policy) = self.client_sub_queue_policy {
            config.client_sub_queue_policy = match policy.as_str() {
                "block" => ClientSubQueuePolicy::Block,
                "drop_new" => ClientSubQueuePolicy::DropNew,
                "drop_old" => ClientSubQueuePolicy::DropOld,
                other => {
                    return Err(invalid(format!(
                        r#"clientSubQueuePolicy must be "block", "drop_new" or "drop_old", got {other:?}"#
                    )));
                }
            };
        }
        if let Some(sharding) = self.publish_sharding {
            config.publish_sharding = match sharding.as_str() {
                "round_robin" => PublishSharding::RoundRobin,
                "hash_stream" => PublishSharding::HashStream,
                other => {
                    return Err(invalid(format!(
                        r#"publishSharding must be "round_robin" or "hash_stream", got {other:?}"#
                    )));
                }
            };
        }
        if let Some(n) = self.broker_sub_queue_capacity {
            let n = whole(n, 1, "brokerSubQueueCapacity")?;
            config.broker_sub_queue_capacity = Some(
                u32::try_from(n)
                    .map_err(|_| invalid("brokerSubQueueCapacity does not fit in 32 bits"))?,
            );
        }
        let counts: [(Option<f64>, u64, &str, &mut usize); 12] = [
            (
                self.client_sub_queue_capacity,
                1,
                "clientSubQueueCapacity",
                &mut config.client_sub_queue_capacity,
            ),
            (
                self.publish_conn_pool,
                1,
                "publishConnPool",
                &mut config.publish_conn_pool,
            ),
            (
                self.publish_streams_per_conn,
                1,
                "publishStreamsPerConn",
                &mut config.publish_streams_per_conn,
            ),
            (
                self.publish_queue_depth,
                1,
                "publishQueueDepth",
                &mut config.publish_queue_depth,
            ),
            (
                self.publish_inflight_bytes,
                1,
                "publishInflightBytes",
                &mut config.publish_inflight_bytes,
            ),
            (
                self.publish_shard_streams,
                0,
                "publishShardStreams",
                &mut config.publish_shard_streams,
            ),
            (
                self.cache_conn_pool,
                1,
                "cacheConnPool",
                &mut config.cache_conn_pool,
            ),
            (
                self.cache_streams_per_conn,
                1,
                "cacheStreamsPerConn",
                &mut config.cache_streams_per_conn,
            ),
            (
                self.event_conn_pool,
                1,
                "eventConnPool",
                &mut config.event_conn_pool,
            ),
            (
                self.cluster_conn_pool,
                1,
                "clusterConnPool",
                &mut config.cluster_conn_pool,
            ),
            (
                self.cluster_streams_per_conn,
                1,
                "clusterStreamsPerConn",
                &mut config.cluster_streams_per_conn,
            ),
            (
                self.event_router_max_pending,
                1,
                "eventRouterMaxPending",
                &mut config.event_router_max_pending,
            ),
        ];
        for (wanted, min, name, slot) in counts {
            if let Some(n) = wanted {
                *slot = usize::try_from(whole(n, min, name)?)
                    .map_err(|_| invalid(format!("{name} is too large")))?;
            }
        }
        if let Some(n) = self.max_frame_bytes {
            config.max_frame_bytes = usize::try_from(whole(n, 1, "maxFrameBytes")?)
                .map_err(|_| invalid("maxFrameBytes is too large"))?;
        }
        let windows: [(Option<f64>, &str, &mut u64); 6] = [
            (
                self.event_conn_recv_window,
                "eventConnRecvWindow",
                &mut config.event_conn_recv_window,
            ),
            (
                self.event_stream_recv_window,
                "eventStreamRecvWindow",
                &mut config.event_stream_recv_window,
            ),
            (
                self.event_send_window,
                "eventSendWindow",
                &mut config.event_send_window,
            ),
            (
                self.cache_conn_recv_window,
                "cacheConnRecvWindow",
                &mut config.cache_conn_recv_window,
            ),
            (
                self.cache_stream_recv_window,
                "cacheStreamRecvWindow",
                &mut config.cache_stream_recv_window,
            ),
            (
                self.cache_send_window,
                "cacheSendWindow",
                &mut config.cache_send_window,
            ),
        ];
        for (wanted, name, slot) in windows {
            if let Some(n) = wanted {
                *slot = whole(n, 1, name)?;
            }
        }
        if let Some(ms) = self.connect_timeout {
            config.connect_timeout = duration_ms(ms, 1.0, "connectTimeout")?;
        }
        if let Some(ms) = self.finish_timeout {
            config.finish_timeout = duration_ms(ms, 1.0, "finishTimeout")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
