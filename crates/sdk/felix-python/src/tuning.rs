//! Transport and queue settings beyond the connection arguments, for
//! `Client(..., tuning=ClientTuning(...))`. Each field left out keeps the Rust
//! client's default.

use felix_client::{ClientConfig, ClientSubQueuePolicy, PublishSharding};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::args::positive_duration;

/// Settings for the client's connections, windows and queues.
///
/// Every argument is optional and defaults to what the Rust client uses, which
/// suits most applications. Counts and sizes must be at least 1
/// (`publish_shard_streams` may be 0). Durations are in seconds.
///
/// `client_sub_queue_policy` is what a subscription does when its queue is
/// full: `"drop_new"` (the default) drops the arriving item, `"block"` stops
/// reading the stream until the application catches up, and `"drop_old"`
/// currently behaves as `"drop_new"`. `publish_sharding` is `"hash_stream"`
/// (the default, which keeps a stream's publishes in order) or
/// `"round_robin"`. `broker_sub_queue_capacity` asks the broker for that many
/// queued batches per subscription; read what it granted from
/// `Subscription.queue_capacity`.
#[pyclass(module = "felix", frozen)]
#[derive(Clone, Default)]
pub struct ClientTuning {
    publish_conn_pool: Option<usize>,
    publish_streams_per_conn: Option<usize>,
    publish_queue_depth: Option<usize>,
    publish_inflight_bytes: Option<usize>,
    publish_sharding: Option<PublishSharding>,
    publish_shard_streams: Option<usize>,
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
    client_sub_queue_policy: Option<ClientSubQueuePolicy>,
    broker_sub_queue_capacity: Option<u32>,
    max_frame_bytes: Option<usize>,
    ack_on_commit: Option<bool>,
    connect_timeout: Option<std::time::Duration>,
    finish_timeout: Option<std::time::Duration>,
}

fn at_least_one<T: PartialOrd + From<u8> + std::fmt::Display + Copy>(
    name: &str,
    value: Option<T>,
) -> PyResult<Option<T>> {
    match value {
        Some(n) if n < T::from(1) => Err(PyValueError::new_err(format!(
            "{name} must be at least 1, not {n}"
        ))),
        other => Ok(other),
    }
}

pub(crate) fn parse_sharding(name: &str) -> PyResult<PublishSharding> {
    match name {
        "hash_stream" => Ok(PublishSharding::HashStream),
        "round_robin" => Ok(PublishSharding::RoundRobin),
        other => Err(PyValueError::new_err(format!(
            "publish_sharding must be 'hash_stream' or 'round_robin', not {other:?}"
        ))),
    }
}

pub(crate) fn parse_queue_policy(name: &str) -> PyResult<ClientSubQueuePolicy> {
    match name {
        "block" => Ok(ClientSubQueuePolicy::Block),
        "drop_new" => Ok(ClientSubQueuePolicy::DropNew),
        "drop_old" => Ok(ClientSubQueuePolicy::DropOld),
        other => Err(PyValueError::new_err(format!(
            "client_sub_queue_policy must be 'block', 'drop_new' or 'drop_old', not {other:?}"
        ))),
    }
}

#[pymethods]
impl ClientTuning {
    #[new]
    #[pyo3(signature = (
        *,
        publish_conn_pool=None,
        publish_streams_per_conn=None,
        publish_queue_depth=None,
        publish_inflight_bytes=None,
        publish_sharding=None,
        publish_shard_streams=None,
        cache_conn_pool=None,
        cache_streams_per_conn=None,
        event_conn_pool=None,
        cluster_conn_pool=None,
        cluster_streams_per_conn=None,
        event_conn_recv_window=None,
        event_stream_recv_window=None,
        event_send_window=None,
        cache_conn_recv_window=None,
        cache_stream_recv_window=None,
        cache_send_window=None,
        event_router_max_pending=None,
        client_sub_queue_capacity=None,
        client_sub_queue_policy=None,
        broker_sub_queue_capacity=None,
        max_frame_bytes=None,
        ack_on_commit=None,
        connect_timeout=None,
        finish_timeout=None,
    ))]
    fn new(
        publish_conn_pool: Option<usize>,
        publish_streams_per_conn: Option<usize>,
        publish_queue_depth: Option<usize>,
        publish_inflight_bytes: Option<usize>,
        publish_sharding: Option<&str>,
        publish_shard_streams: Option<usize>,
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
        client_sub_queue_policy: Option<&str>,
        broker_sub_queue_capacity: Option<u32>,
        max_frame_bytes: Option<usize>,
        ack_on_commit: Option<bool>,
        connect_timeout: Option<f64>,
        finish_timeout: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            publish_conn_pool: at_least_one("publish_conn_pool", publish_conn_pool)?,
            publish_streams_per_conn: at_least_one(
                "publish_streams_per_conn",
                publish_streams_per_conn,
            )?,
            publish_queue_depth: at_least_one("publish_queue_depth", publish_queue_depth)?,
            publish_inflight_bytes: at_least_one("publish_inflight_bytes", publish_inflight_bytes)?,
            publish_sharding: publish_sharding.map(parse_sharding).transpose()?,
            publish_shard_streams,
            cache_conn_pool: at_least_one("cache_conn_pool", cache_conn_pool)?,
            cache_streams_per_conn: at_least_one("cache_streams_per_conn", cache_streams_per_conn)?,
            event_conn_pool: at_least_one("event_conn_pool", event_conn_pool)?,
            cluster_conn_pool: at_least_one("cluster_conn_pool", cluster_conn_pool)?,
            cluster_streams_per_conn: at_least_one(
                "cluster_streams_per_conn",
                cluster_streams_per_conn,
            )?,
            event_conn_recv_window: at_least_one("event_conn_recv_window", event_conn_recv_window)?,
            event_stream_recv_window: at_least_one(
                "event_stream_recv_window",
                event_stream_recv_window,
            )?,
            event_send_window: at_least_one("event_send_window", event_send_window)?,
            cache_conn_recv_window: at_least_one("cache_conn_recv_window", cache_conn_recv_window)?,
            cache_stream_recv_window: at_least_one(
                "cache_stream_recv_window",
                cache_stream_recv_window,
            )?,
            cache_send_window: at_least_one("cache_send_window", cache_send_window)?,
            event_router_max_pending: at_least_one(
                "event_router_max_pending",
                event_router_max_pending,
            )?,
            client_sub_queue_capacity: at_least_one(
                "client_sub_queue_capacity",
                client_sub_queue_capacity,
            )?,
            client_sub_queue_policy: client_sub_queue_policy
                .map(parse_queue_policy)
                .transpose()?,
            broker_sub_queue_capacity: at_least_one(
                "broker_sub_queue_capacity",
                broker_sub_queue_capacity,
            )?,
            max_frame_bytes: at_least_one("max_frame_bytes", max_frame_bytes)?,
            ack_on_commit,
            connect_timeout: connect_timeout
                .map(|s| positive_duration(s, "connect_timeout"))
                .transpose()?,
            finish_timeout: finish_timeout
                .map(|s| positive_duration(s, "finish_timeout"))
                .transpose()?,
        })
    }

    fn __repr__(&self) -> String {
        "ClientTuning(...)".to_string()
    }
}

impl ClientTuning {
    /// Overwrite the fields this tuning names.
    pub(crate) fn apply(&self, config: &mut ClientConfig) {
        macro_rules! set {
            ($($field:ident),* $(,)?) => {
                $(if let Some(value) = self.$field { config.$field = value; })*
            };
        }
        set!(
            publish_conn_pool,
            publish_streams_per_conn,
            publish_queue_depth,
            publish_inflight_bytes,
            publish_sharding,
            publish_shard_streams,
            cache_conn_pool,
            cache_streams_per_conn,
            event_conn_pool,
            cluster_conn_pool,
            cluster_streams_per_conn,
            event_conn_recv_window,
            event_stream_recv_window,
            event_send_window,
            cache_conn_recv_window,
            cache_stream_recv_window,
            cache_send_window,
            event_router_max_pending,
            client_sub_queue_capacity,
            client_sub_queue_policy,
            max_frame_bytes,
            ack_on_commit,
            connect_timeout,
            finish_timeout,
        );
        if let Some(capacity) = self.broker_sub_queue_capacity {
            config.broker_sub_queue_capacity = Some(capacity);
        }
    }
}

/// The connection arguments that switch protocol features on, and the tuning,
/// applied to a config.
pub(crate) fn apply_options(
    config: &mut ClientConfig,
    publishers: Option<bool>,
    timestamps: Option<bool>,
    record_headers: Option<bool>,
    tuning: Option<&ClientTuning>,
) {
    if let Some(tuning) = tuning {
        tuning.apply(config);
    }
    if let Some(on) = publishers {
        config.publishers = on;
    }
    if let Some(on) = timestamps {
        config.timestamps = on;
    }
    if let Some(on) = record_headers {
        config.record_headers = on;
    }
}

#[cfg(test)]
mod tests;
