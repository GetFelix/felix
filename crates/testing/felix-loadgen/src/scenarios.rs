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

pub use ingest::IngestOptions;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use felix_client::ClientConfig;

/// The settings every scenario shares.
pub struct Common {
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
    /// How to reach the brokers. `None` is the instrument's own setup, which
    /// accepts any broker certificate; see `tls`.
    pub client_config: Option<ClientConfig>,
    /// The TLS server name sent to every broker.
    pub server_name: String,
    /// Print the human-readable result lines `scripts/perf` parses.
    pub prose: bool,
}

impl Common {
    /// The instrument's defaults for everything but where and who.
    pub fn new(brokers: Vec<SocketAddr>, tenant: String, namespace: String, token: String) -> Self {
        Self {
            brokers,
            tenant,
            namespace,
            token,
            warmup: 2000,
            total: 20000,
            payload_bytes: 256,
            fanout: 1,
            batch: 1,
            concurrency: 8,
            environment: "unknown".to_string(),
            slow_subscribers: 0,
            slow_delay: Duration::ZERO,
            client_config: None,
            server_name: "localhost".to_string(),
            prose: true,
        }
    }
}

/// A workload and the scenario-specific settings it takes.
pub enum Scenario {
    /// Publish/subscribe latency. `binary` publishes without per-message acks;
    /// `via_entry` publishes through the first broker instead of the owner.
    Pubsub {
        stream: String,
        binary: bool,
        via_entry: bool,
    },
    Cache {
        cache: String,
    },
    Counter {
        cache: String,
    },
    Watch {
        cache: String,
    },
    Queue {
        stream: String,
    },
    Retained {
        cache: String,
    },
    Ingest {
        stream: String,
        options: IngestOptions,
    },
}

/// Run one scenario and return its `LOADGEN_JSON` object. The prose lines are
/// printed as the run goes when `common.prose` is set; the JSON is left to the
/// caller.
pub async fn run(common: &Common, scenario: &Scenario) -> Result<serde_json::Value> {
    match scenario {
        Scenario::Pubsub {
            stream,
            binary,
            via_entry,
        } => pubsub::pubsub(common, stream, *binary, *via_entry).await,
        Scenario::Cache { cache } => cache::cache(common, cache).await,
        Scenario::Counter { cache } => cache::counter(common, cache).await,
        Scenario::Watch { cache } => watch::watch(common, cache).await,
        Scenario::Queue { stream } => queue::queue(common, stream).await,
        Scenario::Retained { cache } => retained::retained(common, cache).await,
        Scenario::Ingest { stream, options } => ingest::ingest(common, stream, options).await,
    }
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
