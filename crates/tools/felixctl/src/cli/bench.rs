//! Benchmark commands.

use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub(crate) enum BenchCommand {
    /// Aggregate publish throughput from several publishers
    #[command(
        long_about = "Measure aggregate publish throughput: --concurrency publishers, each \
                      on its own connection, send batches with --in-flight acknowledged \
                      batches outstanding. No subscribers. Reports msg/s, MB/s and batch \
                      acknowledgement latency.",
        after_long_help = "Examples:
  felixctl bench ingest orders
  felixctl bench ingest orders --concurrency 8 --batch 256 --duration-secs 30
  felixctl bench ingest orders --keys 64 --in-flight 0"
    )]
    Ingest(IngestBenchArgs),
    /// Publish acknowledgement and delivery latency
    #[command(
        long_about = "Measure latency with one publisher and one subscriber: the publish \
                      acknowledgement round trip and the publish-to-delivery time.",
        after_long_help = "Examples:
  felixctl bench latency orders
  felixctl bench latency orders --total 20000 --payload-bytes 1024"
    )]
    Latency(PubsubBenchArgs),
    /// Delivery to many subscribers at once
    #[command(
        long_about = "Measure fanout: one publisher and --fanout subscribers. Reports the \
                      publish rate, the delivered rate across all subscribers, and \
                      delivery latency.",
        after_long_help = "Examples:
  felixctl bench fanout orders
  felixctl bench fanout orders --fanout 50"
    )]
    Fanout(FanoutBenchArgs),
    /// Cache put and get round trips
    #[command(
        long_about = "Measure cache round trips: puts, then gets of the same keys, from \
                      --concurrency workers spread across the brokers.",
        after_long_help = "Examples:
  felixctl bench cache sessions
  felixctl bench cache sessions --concurrency 32 --payload-bytes 64"
    )]
    Cache(CacheBenchArgs),
}

/// Flags every benchmark takes.
#[derive(Debug, Clone, Args)]
pub(crate) struct BenchSize {
    /// Operations measured
    #[arg(long, value_name = "N", default_value_t = 5000)]
    pub(crate) total: usize,
    /// Operations run first and not measured
    #[arg(long, value_name = "N", default_value_t = 500)]
    pub(crate) warmup: usize,
    /// Payload size in bytes
    #[arg(long, value_name = "BYTES", default_value_t = 256)]
    pub(crate) payload_bytes: usize,
}

#[derive(Debug, Args)]
pub(crate) struct IngestBenchArgs {
    /// Stream to publish to
    pub(crate) stream: String,
    /// Records to publish in total
    #[arg(long, value_name = "N", default_value_t = 100_000)]
    pub(crate) total: usize,
    /// Payload size in bytes
    #[arg(long, value_name = "BYTES", default_value_t = 256)]
    pub(crate) payload_bytes: usize,
    /// Publishers, each on its own connection
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub(crate) concurrency: usize,
    /// Records per batch
    #[arg(long, value_name = "N", default_value_t = 64)]
    pub(crate) batch: usize,
    /// Acknowledged batches each publisher keeps outstanding; 0 sends without
    /// acknowledgements
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub(crate) in_flight: usize,
    /// Spread batches over this many routing keys; 0 publishes unkeyed, which
    /// puts everything on shard 0
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub(crate) keys: usize,
    /// Publish for this many seconds instead of --total records
    #[arg(long, value_name = "SECS")]
    pub(crate) duration_secs: Option<f64>,
}

#[derive(Debug, Args)]
pub(crate) struct PubsubBenchArgs {
    /// Stream to publish to and subscribe to
    pub(crate) stream: String,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}

#[derive(Debug, Args)]
pub(crate) struct FanoutBenchArgs {
    /// Stream to publish to and subscribe to
    pub(crate) stream: String,
    /// Subscribers
    #[arg(long, value_name = "N", default_value_t = 10)]
    pub(crate) fanout: usize,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}

#[derive(Debug, Args)]
pub(crate) struct CacheBenchArgs {
    /// Cache to write and read
    pub(crate) cache: String,
    /// Concurrent workers
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub(crate) concurrency: usize,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}
