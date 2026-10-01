//! The workloads, each against a remote cluster.
//!
//! Every scenario runs a warmup it discards, measures a fixed count, and
//! reports percentiles with spread — the local matrix discipline, at
//! distance. Publish payloads carry a 16-byte header (sequence, then this
//! process's monotonic nanos), so delivery latency is measured against one
//! clock: the load generator holds both ends of the pipe, which is the only
//! arrangement in which "publish to delivery" is a subtraction rather than a
//! clock-synchronisation problem.

mod cache;
mod connect;
mod framing;
mod ingest;
mod pubsub;
mod queue;
mod retained;
mod round_trips;
mod watch;

pub(crate) use cache::{cache, counter};
pub(crate) use ingest::{IngestOptions, ingest};
pub(crate) use pubsub::pubsub;
pub(crate) use queue::queue;
pub(crate) use retained::retained;
pub(crate) use watch::watch;

use std::net::SocketAddr;
use std::time::Duration;

/// The settings every scenario shares.
pub(crate) struct Common {
    pub brokers: Vec<SocketAddr>,
    pub tenant: String,
    pub namespace: String,
    pub token: String,
    pub warmup: usize,
    pub total: usize,
    pub payload_bytes: usize,
    pub fanout: usize,
    pub batch: usize,
    pub concurrency: usize,
    pub environment: String,
    // Isolation probe: make the last `slow_subscribers` of the fanout dawdle
    // `slow_delay` per delivery, so a healthy subscriber (index 0, the sampled
    // one) and the publisher can be measured while others fall behind and drop.
    pub slow_subscribers: usize,
    pub slow_delay: Duration,
}

/// A momentary "the pipe was not ready" the instrument retries rather than
/// dies on, and *counts* rather than hides — a nonzero `publish_retries` in the
/// JSON is data about the cluster's readiness, not noise to bury:
///
/// - **routing convergence** — while the routing snapshot is unsettled a
///   broker answers `shard_unavailable` (or "stream not found", before the
///   stream reaches it), so a publish fails for a window;
/// - **client backpressure** — the publisher's bounded queue is momentarily
///   full because acks have not drained, which is the client telling the
///   caller to slow down, not a failure to deliver.
///
/// Neither is a completed round trip, so the retry is excluded from the
/// latency sample the way a warmup message is.
fn is_retriable_transient(err: &anyhow::Error) -> bool {
    if err
        .downcast_ref::<felix_client::BrokerError>()
        .is_some_and(|broker| broker.code == felix_wire::ErrorCode::ShardUnavailable)
    {
        return true;
    }
    let text = format!("{err:#}");
    text.contains("stream not found")
        || text.contains("cannot be subscribed")
        || text.contains("queue full")
}

fn scope(common: &Common, cache: &str) -> (String, String, String) {
    (
        common.tenant.clone(),
        common.namespace.clone(),
        cache.to_string(),
    )
}
