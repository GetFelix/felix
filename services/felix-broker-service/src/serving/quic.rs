//! QUIC transport adapter for the broker.
//!
//! Decodes felix-wire frames, enforces stream scope, and fans out events to subscribers.
//!
//! # Protocol overview
//!
//! - Bidirectional streams are the control plane: clients send Publish/PublishBatch, Subscribe,
//!   and Cache requests, and the broker returns acks/responses on the same stream.
//! - Subscribe on the control stream causes the broker to open a uni stream for event delivery;
//!   that uni stream starts with EventStreamHello and then carries subscriber-independent
//!   Event/EventBatch frames.
//! - Client-initiated uni streams are publish-only (no acks); they accept Publish/PublishBatch
//!   and are treated as fire-and-forget ingress.
//! - Frames use felix-wire; high-throughput paths may carry binary batch frames to avoid JSON
//!   encode/decode overhead.
//! - Acked publishes require request_id; PublishOk/PublishError echo it and may arrive out of order.
//!
//! # Design notes
//!
//! ## High level goals
//!
//! - Keep QUIC SendStream writes single-threaded: Quinn's SendStream is not safe/efficient under many
//!   concurrent writers (it can serialize internally and/or create heavy contention). We therefore funnel
//!   all outbound control-plane responses/acks through a single writer task per control stream.
//! - Keep broker mutation serialized per connection: a single publish worker per QUIC connection drains
//!   a bounded ingress queue. This avoids many tasks mutating broker state concurrently and reduces
//!   lock contention inside the broker.
//! - Make overload behavior explicit and observable: bounded queues + metrics + throttling.
//!
//! ## Key queues
//!
//! - Ingress publish queue (PUBLISH_QUEUE_DEPTH): work items sent to a per-connection publish worker.
//! - Outbound ack/response queue (ACK_QUEUE_DEPTH): Outgoing messages drained by the single writer.
//! - Ack waiter queue (ACK_WAITERS_MAX): only used when ack_on_commit is enabled; tracks acks that must
//!   wait until broker commit completes.
//!
//! ## Ack modes & policies
//!
//! - Wire-level ack mode is per-message/per-batch/none; server policy `ack_on_commit` optionally delays
//!   acks until the publish worker finishes.
//! - When ack_on_commit=true, the enqueue policy is Wait for acked publishes so we preserve the
//!   semantic that an ack implies the broker accepted work and (eventually) committed.
//! - When ack_on_commit=false, we can respond immediately after enqueue.
//!
//! ## Potential issues / edge cases to be aware of
//!
//! - Task lifecycle: writer + ack-waiter tasks must be joined or aborted on stream close. If one task
//!   finishes and the other is dropped without abort/join, it can continue running detached.
//!   (This is easy to accidentally introduce when using `tokio::select!` during shutdown.)
//! - Overload + acked publishes: if we accept a request that expects an ack, but later drop/skip the
//!   error ack because the outbound queue is full, clients may hang until their own timeout.
//!   Preferred policy is either "always respond" (critical enqueue / close on failure) or "hard close".
//! - Backpressure interactions: waiting to enqueue ingress (Wait policy) can propagate latency back to
//!   the control stream read loop; this is intentional for commit-acked publishes but must be bounded.
//! - Queue depth gauges are best-effort; under races they can drift. We track drift counters and reset
//!   local depths on teardown.
//! - Ordering: acks may be out-of-order relative to requests because publish jobs complete out-of-order
//!   (and ack waiters emit as they complete). This is allowed by protocol, but clients must treat
//!   request_id as the correlator.
//! - Cancellation: cancel signals are delivered via watch channels and are cooperative; code must check
//!   them in all long waits to avoid hanging tasks.

pub(crate) mod client_error;
mod codec;
mod conn;
mod errors;
pub(crate) mod preauth;
mod streams;
mod telemetry;

pub(crate) mod handlers;

pub use codec::{read_frame_limited_into, read_message_limited, write_message};
pub use conn::{ClusterContext, serve, serve_with_shutdown};
pub use preauth::ConnectionLimit;
pub use telemetry::{FrameCountersSnapshot, frame_counters_snapshot, reset_frame_counters};

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use felix_transport::TransportConfig;

use crate::config::BrokerConfig;

pub(crate) const ACK_QUEUE_DEPTH: usize = 2048;
pub(crate) const ACK_WAITERS_MAX: usize = 1024;
pub(crate) const ACK_HI_WATER: usize = ACK_QUEUE_DEPTH * 3 / 4;
pub(crate) const ACK_LO_WATER: usize = ACK_QUEUE_DEPTH / 2;
pub(crate) const ACK_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
pub(crate) const ACK_TIMEOUT_WINDOW: Duration = Duration::from_millis(200);
pub(crate) const ACK_TIMEOUT_THRESHOLD: u32 = 3;
pub(crate) const STREAM_CACHE_TTL: Duration = Duration::from_secs(2);
pub(crate) static SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) static GLOBAL_INGRESS_DEPTH: AtomicUsize = AtomicUsize::new(0);
pub(crate) static GLOBAL_ACK_DEPTH: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
pub(crate) static DECODE_ERROR_LOGS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "telemetry")]
pub(crate) const DECODE_ERROR_LOG_LIMIT: usize = 20;

/// The QUIC transport settings for client listeners: `base` with the cache
/// flow-control windows from `config` applied.
pub fn cache_transport_config(config: &BrokerConfig, mut base: TransportConfig) -> TransportConfig {
    base.receive_window = config.cache_conn_recv_window;
    base.stream_receive_window = config.cache_stream_recv_window;
    base.send_window = config.cache_send_window;
    base
}

#[cfg(test)]
mod tests;
