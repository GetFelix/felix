//! The queue and transport settings at the end of `FelixConnectOptions`.
//!
//! Every one is size-gated like the fields before it, and zero (or the
//! `*_DEFAULT` value of an enum) leaves the Rust client's default in place.

use std::mem::offset_of;
use std::time::Duration;

use felix_client::{ClientConfig, ClientSubQueuePolicy, PublishSharding};

use crate::boundary::{Failure, read_field, read_flag};
use crate::client::FelixConnectOptions;

/// What a subscription's client-side queue does when it is full.
pub type FelixSubQueuePolicy = i32;

/// The client's default, which drops the arriving item.
pub const FELIX_QUEUE_DEFAULT: FelixSubQueuePolicy = 0;
/// Stop reading the event stream until the application catches up.
pub const FELIX_QUEUE_BLOCK: FelixSubQueuePolicy = 1;
/// Drop the arriving item.
pub const FELIX_QUEUE_DROP_NEW: FelixSubQueuePolicy = 2;
/// Meant to drop the oldest queued item. It currently drops the arriving one,
/// exactly as `FELIX_QUEUE_DROP_NEW` does.
pub const FELIX_QUEUE_DROP_OLD: FelixSubQueuePolicy = 3;

/// How publishes are spread across the publish streams.
pub type FelixPublishSharding = i32;

/// The client's default, `FELIX_SHARDING_HASH_STREAM`.
pub const FELIX_SHARDING_DEFAULT: FelixPublishSharding = 0;
/// Pick the stream by hashing the stream's name, so each stream's publishes
/// share one writer and stay in order.
pub const FELIX_SHARDING_HASH_STREAM: FelixPublishSharding = 1;
/// Rotate through the streams. Publishes to one stream can reach the broker
/// out of order.
pub const FELIX_SHARDING_ROUND_ROBIN: FelixPublishSharding = 2;

/// QUIC windows are variable-length integers, which top out below 2^62.
const MAX_WINDOW: u64 = (1 << 62) - 1;

/// Waits are capped at a year so a deadline cannot overflow the clock.
const A_YEAR: Duration = Duration::from_secs(365 * 24 * 3600);

/// The tuning fields a caller's options reach, each `None` when left at zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tuning {
    queue_policy: Option<ClientSubQueuePolicy>,
    sharding: Option<PublishSharding>,
    client_sub_queue_capacity: Option<usize>,
    broker_sub_queue_capacity: Option<u32>,
    publish_conn_pool: Option<usize>,
    publish_streams_per_conn: Option<usize>,
    publish_queue_depth: Option<usize>,
    /// `Some(0)` puts every publish on the pool.
    publish_shard_streams: Option<usize>,
    cache_conn_pool: Option<usize>,
    cache_streams_per_conn: Option<usize>,
    event_conn_pool: Option<usize>,
    cluster_conn_pool: Option<usize>,
    cluster_streams_per_conn: Option<usize>,
    event_router_max_pending: Option<usize>,
    publish_inflight_bytes: Option<usize>,
    max_frame_bytes: Option<usize>,
    event_conn_recv_window: Option<u64>,
    event_stream_recv_window: Option<u64>,
    event_send_window: Option<u64>,
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    connect_timeout: Option<Duration>,
    finish_timeout: Option<Duration>,
    ack_on_commit: bool,
}

impl Tuning {
    /// # Safety
    ///
    /// `options` is valid for reads of `size` bytes.
    pub(crate) unsafe fn read(
        options: *const FelixConnectOptions,
        size: usize,
    ) -> Result<Self, Failure> {
        let base = options.cast::<u8>();
        macro_rules! field {
            ($name:ident, $ty:ty) => {
                // SAFETY: the caller promised `size` bytes; the read is gated
                // by it.
                unsafe { read_field::<$ty>(base, size, offset_of!(FelixConnectOptions, $name)) }
                    .filter(|&n| n != 0)
            };
        }
        let count = |value: Option<u32>| value.map(|n| n as usize);
        let bytes = |value: Option<u64>, name: &str| {
            value
                .map(|n| {
                    usize::try_from(n).map_err(|_| Failure::invalid(format!("{name} is too large")))
                })
                .transpose()
        };
        let window = |value: Option<u64>, name: &str| {
            value
                .map(|n| {
                    if n > MAX_WINDOW {
                        Err(Failure::invalid(format!("{name} must be below 2^62")))
                    } else {
                        Ok(n)
                    }
                })
                .transpose()
        };
        let duration = |ms: Option<u64>| ms.map(|ms| Duration::from_millis(ms).min(A_YEAR));
        // SAFETY: as above.
        let no_shard_streams = unsafe {
            read_flag(
                base,
                size,
                offset_of!(FelixConnectOptions, no_publish_shard_streams),
            )
        };
        let shard_streams = count(field!(publish_shard_streams, u32));
        Ok(Self {
            queue_policy: queue_policy(
                field!(client_sub_queue_policy, FelixSubQueuePolicy).unwrap_or(0),
            )?,
            sharding: sharding(field!(publish_sharding, FelixPublishSharding).unwrap_or(0))?,
            client_sub_queue_capacity: count(field!(client_sub_queue_capacity, u32)),
            broker_sub_queue_capacity: field!(broker_sub_queue_capacity, u32),
            publish_conn_pool: count(field!(publish_conn_pool, u32)),
            publish_streams_per_conn: count(field!(publish_streams_per_conn, u32)),
            publish_queue_depth: count(field!(publish_queue_depth, u32)),
            publish_shard_streams: if no_shard_streams {
                Some(0)
            } else {
                shard_streams
            },
            cache_conn_pool: count(field!(cache_conn_pool, u32)),
            cache_streams_per_conn: count(field!(cache_streams_per_conn, u32)),
            event_conn_pool: count(field!(event_conn_pool, u32)),
            cluster_conn_pool: count(field!(cluster_conn_pool, u32)),
            cluster_streams_per_conn: count(field!(cluster_streams_per_conn, u32)),
            event_router_max_pending: count(field!(event_router_max_pending, u32)),
            publish_inflight_bytes: bytes(
                field!(publish_inflight_bytes, u64),
                "publish_inflight_bytes",
            )?,
            max_frame_bytes: bytes(field!(max_frame_bytes, u64), "max_frame_bytes")?,
            event_conn_recv_window: window(
                field!(event_conn_recv_window, u64),
                "event_conn_recv_window",
            )?,
            event_stream_recv_window: window(
                field!(event_stream_recv_window, u64),
                "event_stream_recv_window",
            )?,
            event_send_window: window(field!(event_send_window, u64), "event_send_window")?,
            cache_conn_recv_window: window(
                field!(cache_conn_recv_window, u64),
                "cache_conn_recv_window",
            )?,
            cache_stream_recv_window: window(
                field!(cache_stream_recv_window, u64),
                "cache_stream_recv_window",
            )?,
            cache_send_window: window(field!(cache_send_window, u64), "cache_send_window")?,
            connect_timeout: duration(field!(connect_timeout_ms, u64)),
            finish_timeout: duration(field!(finish_timeout_ms, u64)),
            // SAFETY: as above.
            ack_on_commit: unsafe {
                read_flag(base, size, offset_of!(FelixConnectOptions, ack_on_commit))
            },
        })
    }

    /// Overwrite the config's fields this tuning names.
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
            max_frame_bytes,
            connect_timeout,
            finish_timeout,
        );
        if let Some(policy) = self.queue_policy {
            config.client_sub_queue_policy = policy;
        }
        if let Some(sharding) = self.sharding {
            config.publish_sharding = sharding;
        }
        if let Some(capacity) = self.broker_sub_queue_capacity {
            config.broker_sub_queue_capacity = Some(capacity);
        }
        if self.ack_on_commit {
            config.ack_on_commit = true;
        }
    }
}

fn queue_policy(value: FelixSubQueuePolicy) -> Result<Option<ClientSubQueuePolicy>, Failure> {
    match value {
        FELIX_QUEUE_DEFAULT => Ok(None),
        FELIX_QUEUE_BLOCK => Ok(Some(ClientSubQueuePolicy::Block)),
        FELIX_QUEUE_DROP_NEW => Ok(Some(ClientSubQueuePolicy::DropNew)),
        FELIX_QUEUE_DROP_OLD => Ok(Some(ClientSubQueuePolicy::DropOld)),
        other => Err(Failure::invalid(format!(
            "client_sub_queue_policy {other} is not a FELIX_QUEUE_* value"
        ))),
    }
}

fn sharding(value: FelixPublishSharding) -> Result<Option<PublishSharding>, Failure> {
    match value {
        FELIX_SHARDING_DEFAULT => Ok(None),
        FELIX_SHARDING_HASH_STREAM => Ok(Some(PublishSharding::HashStream)),
        FELIX_SHARDING_ROUND_ROBIN => Ok(Some(PublishSharding::RoundRobin)),
        other => Err(Failure::invalid(format!(
            "publish_sharding {other} is not a FELIX_SHARDING_* value"
        ))),
    }
}

#[cfg(test)]
mod tests;
