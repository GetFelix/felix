//! Latency demo binary for QUIC publish/subscribe.
//!
//! Runs a controlled pub/sub workload against an in-process broker and reports
//! latency percentiles and throughput for different payload sizes and fanout.
//!
//! Intended for benchmarking and tuning; not part of production runtime.
//!
//! Throughput fairness experiments:
//! - `--pub-yield-every-batches N` yields publisher task every N batches.
//! - `--sub-dedicated-thread` runs the primary subscriber on a dedicated
//!   current-thread Tokio runtime to isolate scheduler contention.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::timings as broker_publish_timings;
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::observability::timings as broker_timings;
use felix_client::timings as client_timings;
use felix_client::{Client, ClientConfig, PublishSharding, Publisher, Subscription};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

type DemoAuthResult = Result<(
    Arc<felix_broker_service::serving::auth::BrokerAuth>,
    Option<(String, String)>,
)>;

/// Counts measured deliveries and remembers when the last one landed.
///
/// Delivery duration must end at the moment the final event arrived, not when
/// drain tasks finish: trailing bookkeeping would otherwise inflate the
/// measured delivery window and deflate reported throughput.
#[derive(Clone)]
struct DeliveryTracker {
    delivered: Arc<AtomicUsize>,
    epoch: Instant,
    last_delivery_ns: Arc<AtomicU64>,
}

impl DeliveryTracker {
    fn new() -> Self {
        Self {
            delivered: Arc::new(AtomicUsize::new(0)),
            epoch: Instant::now(),
            last_delivery_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
        let elapsed_ns = self.epoch.elapsed().as_nanos() as u64;
        // fetch_max keeps the latest timestamp under concurrent recorders.
        self.last_delivery_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn total(&self) -> usize {
        self.delivered.load(Ordering::Relaxed)
    }

    fn last_delivery_at(&self) -> Option<Instant> {
        let ns = self.last_delivery_ns.load(Ordering::Relaxed);
        (ns > 0).then(|| self.epoch + Duration::from_nanos(ns))
    }
}

#[derive(Clone, Debug)]
struct DemoConfig {
    warmup: usize,
    total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    idle_ms: u64,
    pub_conns: usize,
    pub_streams_per_conn: usize,
    pub_sharding: Option<String>,
    pub_stream_count: usize,
    sub_conns: usize,
    sub_writer_lanes: Option<usize>,
    sub_lane_shard: Option<String>,
    sub_delivery_shaping: bool,
    pub_yield_every_batches: usize,
    sub_dedicated_thread: bool,
    log_capacity: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct DemoResult {
    publish_total: usize,
    sample_total: usize,
    payload_bytes: usize,
    fanout: usize,
    batch_size: usize,
    binary: bool,
    received: usize,
    /// Events expected but never observed (`expected - delivered`). This is a
    /// *shortfall*, not a queue-drop counter: the broker's and client's real
    /// drop counters are `metrics` counters and are not collected here. Because
    /// a subscriber that times out now fails the run, this must be 0 on any
    /// successful run -- a non-zero value means the accounting is wrong, not
    /// that queues shed.
    unaccounted: usize,
    delivered_total: usize,
    p50: Duration,
    p99: Duration,
    p999: Duration,
    publish_duration: Duration,
    drain_duration: Duration,
    publish_submit_throughput: f64,
    throughput: f64,
    effective_throughput: f64,
    delivered_throughput: f64,
    delivered_per_sub_throughput: f64,
}

#[cfg(feature = "telemetry")]
#[derive(Clone, Copy, Debug)]
struct TimingSummary {
    client_pub_enqueue_wait_p50: Option<Duration>,
    client_pub_enqueue_wait_p99: Option<Duration>,
    client_pub_enqueue_wait_p999: Option<Duration>,
    client_encode_p50: Option<Duration>,
    client_encode_p99: Option<Duration>,
    client_binary_encode_p50: Option<Duration>,
    client_binary_encode_p99: Option<Duration>,
    client_binary_encode_p999: Option<Duration>,
    client_text_encode_p50: Option<Duration>,
    client_text_encode_p99: Option<Duration>,
    client_text_encode_p999: Option<Duration>,
    client_text_batch_build_p50: Option<Duration>,
    client_text_batch_build_p99: Option<Duration>,
    client_text_batch_build_p999: Option<Duration>,
    client_write_p50: Option<Duration>,
    client_write_p99: Option<Duration>,
    client_send_await_p50: Option<Duration>,
    client_send_await_p99: Option<Duration>,
    client_send_await_p999: Option<Duration>,
    client_sub_read_p50: Option<Duration>,
    client_sub_read_p99: Option<Duration>,
    client_sub_read_await_p50: Option<Duration>,
    client_sub_read_await_p99: Option<Duration>,
    client_sub_poll_gap_p50: Option<Duration>,
    client_sub_poll_gap_p99: Option<Duration>,
    client_sub_poll_gap_p999: Option<Duration>,
    client_sub_queue_wait_p50: Option<Duration>,
    client_sub_queue_wait_p99: Option<Duration>,
    client_sub_time_in_queue_p50: Option<Duration>,
    client_sub_time_in_queue_p99: Option<Duration>,
    client_sub_time_in_queue_p999: Option<Duration>,
    client_sub_runtime_gap_p50: Option<Duration>,
    client_sub_runtime_gap_p99: Option<Duration>,
    client_sub_runtime_gap_p999: Option<Duration>,
    client_sub_delivery_chan_wait_p50: Option<Duration>,
    client_sub_delivery_chan_wait_p99: Option<Duration>,
    client_sub_delivery_chan_wait_p999: Option<Duration>,
    client_sub_decode_p50: Option<Duration>,
    client_sub_decode_p99: Option<Duration>,
    client_sub_dispatch_p50: Option<Duration>,
    client_sub_dispatch_p99: Option<Duration>,
    client_sub_consumer_gap_p50: Option<Duration>,
    client_sub_consumer_gap_p99: Option<Duration>,
    client_sub_consumer_gap_p999: Option<Duration>,
    client_e2e_latency_p50: Option<Duration>,
    client_e2e_latency_p99: Option<Duration>,
    client_e2e_latency_p999: Option<Duration>,
    client_ack_read_p50: Option<Duration>,
    client_ack_read_p99: Option<Duration>,
    client_ack_decode_p50: Option<Duration>,
    client_ack_decode_p99: Option<Duration>,
    broker_decode_p50: Option<Duration>,
    broker_decode_p99: Option<Duration>,
    broker_fanout_p50: Option<Duration>,
    broker_fanout_p99: Option<Duration>,
    broker_ack_write_p50: Option<Duration>,
    broker_ack_write_p99: Option<Duration>,
    broker_quic_write_p50: Option<Duration>,
    broker_quic_write_p99: Option<Duration>,
    broker_sub_queue_wait_p50: Option<Duration>,
    broker_sub_queue_wait_p99: Option<Duration>,
    broker_sub_prefix_build_p50: Option<Duration>,
    broker_sub_prefix_build_p99: Option<Duration>,
    broker_sub_write_p50: Option<Duration>,
    broker_sub_write_p99: Option<Duration>,
    broker_sub_write_await_p50: Option<Duration>,
    broker_sub_write_await_p99: Option<Duration>,
    broker_sub_delivery_p50: Option<Duration>,
    broker_sub_delivery_p99: Option<Duration>,
    broker_sub_delivery_p999: Option<Duration>,
    broker_publish_lookup_p50: Option<Duration>,
    broker_publish_lookup_p99: Option<Duration>,
    broker_publish_append_p50: Option<Duration>,
    broker_publish_append_p99: Option<Duration>,
    broker_publish_fanout_p50: Option<Duration>,
    broker_publish_fanout_p99: Option<Duration>,
    broker_publish_enqueue_p50: Option<Duration>,
    broker_publish_enqueue_p99: Option<Duration>,
    broker_publish_send_p50: Option<Duration>,
    broker_publish_send_p99: Option<Duration>,
}

#[cfg(not(feature = "telemetry"))]
#[derive(Clone, Copy, Debug, Default)]
struct TimingSummary;

async fn run_demo(
    config: DemoConfig,
    matrix: bool,
    payloads: Vec<usize>,
    fanouts: Vec<usize>,
    all: bool,
) -> Result<()> {
    println!("== Felix QUIC Pub/Sub Latency Demo ==");
    println!("Mode: QUIC broker protocol over felix-transport.");
    println!(
        "Note: subscriber/event delivery is always binary; publish_fastpath toggles publish encoding path."
    );

    if all {
        run_all(config).await?;
    } else if matrix {
        for payload in payloads {
            for fanout in &fanouts {
                let mut case = config.clone();
                case.payload_bytes = payload;
                case.fanout = *fanout;
                let (result, summary) = run_case(case).await?;
                print_result(&result);
                print_timing_summary(summary);
            }
        }
    } else {
        let (result, summary) = run_case(config).await?;
        print_result(&result);
        print_timing_summary(summary);
    }
    Ok(())
}

#[cfg(feature = "telemetry")]
async fn run_smoke(mut base: DemoConfig) -> Result<()> {
    base.warmup = 200;
    base.total = 2000;
    base.payload_bytes = 1024;
    base.fanout = 10;
    base.batch_size = 64;
    base.binary = false;
    base.pub_conns = 4;
    base.pub_streams_per_conn = 2;
    base.pub_stream_count = 1;

    let mut baseline = base.clone();
    baseline.sub_dedicated_thread = false;
    let (baseline_result, baseline_summary) = run_case(baseline).await?;
    let baseline_summary = baseline_summary.ok_or_else(|| {
        anyhow::anyhow!("smoke mode requires telemetry timing summary for baseline run")
    })?;

    let mut dedicated = base;
    dedicated.sub_dedicated_thread = true;
    let (dedicated_result, dedicated_summary) = run_case(dedicated).await?;
    let dedicated_summary = dedicated_summary.ok_or_else(|| {
        anyhow::anyhow!("smoke mode requires telemetry timing summary for dedicated run")
    })?;

    println!(
        "Smoke summary: baseline p99={} dedicated p99={} | baseline sub_read_await_p99={} dedicated sub_read_await_p99={} | baseline sub_dispatch_p99={} dedicated sub_dispatch_p99={}",
        format_duration(baseline_result.p99),
        format_duration(dedicated_result.p99),
        baseline_summary
            .client_sub_read_await_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        dedicated_summary
            .client_sub_read_await_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        baseline_summary
            .client_sub_dispatch_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
        dedicated_summary
            .client_sub_dispatch_p99
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string()),
    );

    let baseline_read = baseline_summary
        .client_sub_read_await_p99
        .unwrap_or(Duration::MAX);
    let dedicated_read = dedicated_summary
        .client_sub_read_await_p99
        .unwrap_or(Duration::MAX);
    let dedicated_dispatch = dedicated_summary
        .client_sub_dispatch_p99
        .unwrap_or(Duration::MAX);

    let threshold = Duration::from_millis(50);
    if dedicated_read > threshold || dedicated_dispatch > threshold {
        anyhow::bail!(
            "smoke failed: dedicated subscriber tails too high (read_await_p99={}, dispatch_p99={})",
            format_duration(dedicated_read),
            format_duration(dedicated_dispatch),
        );
    }
    if dedicated_read > baseline_read.mul_f64(1.10) {
        anyhow::bail!(
            "smoke failed: dedicated subscriber did not improve read_await_p99 enough (baseline={}, dedicated={})",
            format_duration(baseline_read),
            format_duration(dedicated_read),
        );
    }
    Ok(())
}

#[cfg(not(feature = "telemetry"))]
async fn run_smoke(_base: DemoConfig) -> Result<()> {
    anyhow::bail!("--smoke requires telemetry feature");
}

fn main() -> Result<()> {
    // Runtime width is a first-class perf variable here: this harness runs the
    // broker and its clients in one process, so every extra worker thread is
    // another candidate for a cross-core wakeup on the publish/deliver chain.
    // Defaults to tokio's own choice (one worker per CPU).
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = std::env::var("FELIX_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
    {
        builder.worker_threads(threads);
    }
    builder.build()?.block_on(async_main())
}

async fn async_main() -> Result<()> {
    // Off unless RUST_LOG asks for it, so benchmark output stays clean. Exists so
    // broker-side diagnostics (notably FELIX_CONN_STATS_MS path stats) are
    // reachable from this harness at all.
    if std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    let (config, matrix, payloads, fanouts, all, smoke, compare_sub_delivery_shaping) =
        parse_args();
    if compare_sub_delivery_shaping {
        run_compare_sub_delivery_shaping(config).await
    } else if smoke {
        run_smoke(config).await
    } else {
        run_demo(config, matrix, payloads, fanouts, all).await
    }
}

fn parse_args_from<I>(mut args: I) -> (DemoConfig, bool, Vec<usize>, Vec<usize>, bool, bool, bool)
where
    I: Iterator<Item = String>,
{
    let mut warmup = 1000;
    let mut total = 10000;
    let mut payload_bytes = 0;
    let mut fanout = 1;
    let mut batch_size = 1;
    let mut binary = false;
    let mut idle_ms = 2000;
    let mut pub_conns = 4;
    let mut pub_streams_per_conn = 2;
    let mut pub_sharding = None;
    let mut pub_stream_count = 1;
    let mut sub_conns = std::env::var("FELIX_SUB_EGRESS_CONNS")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_CONNS").ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let mut sub_writer_lanes = None;
    let mut sub_lane_shard = None;
    let mut log_capacity: Option<usize> = std::env::var("FELIX_DEMO_LOG_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok());
    let mut pub_yield_every_batches = 0usize;
    let mut sub_dedicated_thread = std::env::var("FELIX_SUB_DEDICATED_THREAD")
        .ok()
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let mut sub_delivery_shaping = std::env::var("FELIX_SUB_DELIVERY_SHAPING")
        .ok()
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let mut smoke = false;
    let mut compare_sub_delivery_shaping = false;
    let mut matrix = false;
    let mut all = true;
    let mut all_set = false;
    let mut payloads = None;
    let mut fanouts = None;
    let mut warmup_set = false;
    let mut total_set = false;
    let mut payload_set = false;
    let mut fanout_set = false;
    let mut batch_set = false;
    let mut idle_set = false;
    let mut binary_set = false;
    let mut pub_conns_set = false;
    let mut pub_streams_set = false;
    let mut pub_sharding_set = false;
    let mut pub_stream_count_set = false;
    let mut sub_conns_set = false;
    let mut sub_writer_lanes_set = false;
    let mut sub_lane_shard_set = false;
    let mut pub_yield_every_batches_set = false;
    let mut sub_dedicated_thread_set = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--matrix" => {
                matrix = true;
                all = false;
            }
            "--all" => {
                all = true;
                all_set = true;
            }
            "--binary" => {
                binary = true;
                binary_set = true;
            }
            "--warmup" => {
                if let Some(value) = args.next() {
                    warmup = value.parse().unwrap_or(warmup);
                    warmup_set = true;
                }
            }
            "--total" => {
                if let Some(value) = args.next() {
                    total = value.parse().unwrap_or(total);
                    total_set = true;
                }
            }
            "--payload" => {
                if let Some(value) = args.next() {
                    payload_bytes = value.parse().unwrap_or(payload_bytes);
                    payload_set = true;
                }
            }
            "--fanout" => {
                if let Some(value) = args.next() {
                    fanout = value.parse().unwrap_or(fanout);
                    fanout_set = true;
                }
            }
            "--batch" => {
                if let Some(value) = args.next() {
                    batch_size = value.parse().unwrap_or(batch_size);
                    batch_set = true;
                }
            }
            "--idle-ms" => {
                if let Some(value) = args.next() {
                    idle_ms = value.parse().unwrap_or(idle_ms);
                    idle_set = true;
                }
            }
            "--log-capacity" => {
                if let Some(value) = args.next() {
                    log_capacity = value.parse().ok();
                }
            }
            "--pub-conns" => {
                if let Some(value) = args.next() {
                    pub_conns = value.parse().unwrap_or(pub_conns);
                    pub_conns_set = true;
                }
            }
            "--pub-streams-per-conn" => {
                if let Some(value) = args.next() {
                    pub_streams_per_conn = value.parse().unwrap_or(pub_streams_per_conn);
                    pub_streams_set = true;
                }
            }
            "--pub-sharding" => {
                if let Some(value) = args.next() {
                    pub_sharding = Some(value);
                    pub_sharding_set = true;
                }
            }
            "--pub-stream-count" => {
                if let Some(value) = args.next() {
                    pub_stream_count = value.parse().unwrap_or(pub_stream_count);
                    pub_stream_count_set = true;
                }
            }
            "--sub-writer-lanes" => {
                if let Some(value) = args.next() {
                    sub_writer_lanes = value.parse().ok();
                    sub_writer_lanes_set = true;
                }
            }
            "--sub-conns" => {
                if let Some(value) = args.next() {
                    sub_conns = value.parse().unwrap_or(sub_conns);
                    sub_conns_set = true;
                }
            }
            "--sub-lane-shard" => {
                if let Some(value) = args.next() {
                    sub_lane_shard = Some(value);
                    sub_lane_shard_set = true;
                }
            }
            "--pub-yield-every-batches" => {
                if let Some(value) = args.next() {
                    pub_yield_every_batches = value.parse().unwrap_or(pub_yield_every_batches);
                    pub_yield_every_batches_set = true;
                }
            }
            "--sub-dedicated-thread" => {
                sub_dedicated_thread = true;
                sub_dedicated_thread_set = true;
            }
            "--sub-shared-thread" => {
                sub_dedicated_thread = false;
                sub_dedicated_thread_set = true;
            }
            "--smoke" => {
                smoke = true;
                all = false;
            }
            "--sub-delivery-shaping-on" => {
                sub_delivery_shaping = true;
            }
            "--sub-delivery-shaping-off" => {
                sub_delivery_shaping = false;
            }
            "--compare-sub-delivery-shaping" => {
                compare_sub_delivery_shaping = true;
                all = false;
            }
            "--payloads" => {
                payloads = args.next();
            }
            "--fanouts" => {
                fanouts = args.next();
            }
            _ => {}
        }
    }

    if matrix {
        if !warmup_set {
            warmup = 200;
        }
        if !total_set {
            total = 2000;
        }
    } else if !all_set
        && (warmup_set
            || total_set
            || payload_set
            || fanout_set
            || batch_set
            || idle_set
            || binary_set
            || pub_conns_set
            || pub_streams_set
            || pub_sharding_set
            || pub_stream_count_set
            || sub_conns_set
            || sub_writer_lanes_set
            || sub_lane_shard_set
            || pub_yield_every_batches_set
            || sub_dedicated_thread_set)
    {
        all = false;
    }

    let payloads = payloads
        .as_deref()
        .map(parse_csv_usize)
        .unwrap_or_else(|| vec![0, 256, 1024, 4096]);
    let fanouts = fanouts
        .as_deref()
        .map(parse_csv_usize)
        .unwrap_or_else(|| vec![1, 10, 100]);

    (
        DemoConfig {
            warmup,
            total,
            payload_bytes,
            fanout,
            batch_size,
            binary,
            idle_ms,
            pub_conns,
            pub_streams_per_conn,
            pub_sharding,
            pub_stream_count,
            sub_conns,
            sub_writer_lanes,
            sub_lane_shard,
            sub_delivery_shaping,
            pub_yield_every_batches,
            sub_dedicated_thread,
            log_capacity,
        },
        matrix,
        payloads,
        fanouts,
        all,
        smoke,
        compare_sub_delivery_shaping,
    )
}

fn parse_args() -> (DemoConfig, bool, Vec<usize>, Vec<usize>, bool, bool, bool) {
    parse_args_from(std::env::args().skip(1))
}

fn print_usage() {
    println!("Felix latency-demo");
    println!("  --warmup <N> --total <N> --payload <bytes> --fanout <N> --batch <N>");
    println!("  --pub-conns <N> --pub-streams-per-conn <N> --pub-stream-count <N>");
    println!("  --pub-yield-every-batches <N>  Yield publisher loop every N batches (0=off)");
    println!("  --sub-dedicated-thread         Force dedicated subscriber thread (default ON)");
    println!("  --sub-shared-thread            Disable dedicated subscriber thread");
    println!("  --smoke                        Run throughput A/B sanity check");
    println!("  --sub-delivery-shaping-on      Enable broker sub delivery shaping (default ON)");
    println!("  --sub-delivery-shaping-off     Disable broker sub delivery shaping");
    println!("  --compare-sub-delivery-shaping Run current config twice (OFF then ON) and compare");
    println!("  --sub-writer-lanes <N> --sub-conns <N> --sub-lane-shard <mode>");
    println!("  --matrix --payloads <csv> --fanouts <csv> --all --binary");
    println!();
    println!(
        "Throughput starvation repro:\n  cargo run --release -p felix-broker-service --bin latency-demo --all-features -- --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1"
    );
    println!("Fairness A/B:\n  ... --pub-yield-every-batches 1\n  ... --sub-dedicated-thread");
}

fn effective_sub_lanes_hint(config: &DemoConfig) -> usize {
    if let Some(lanes) = config.sub_writer_lanes {
        return lanes.max(1);
    }
    std::env::var("FELIX_SUB_EGRESS_LANES")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_WRITER_LANES").ok())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

async fn run_case(config: DemoConfig) -> Result<(DemoResult, Option<TimingSummary>)> {
    let sub_lanes_hint = effective_sub_lanes_hint(&config);
    let sub_stream_mode = std::env::var("FELIX_SUB_STREAM_MODE")
        .ok()
        .unwrap_or_else(|| "per_subscriber".to_string());
    let sub_queue_bound = std::env::var("FELIX_SUB_QUEUE_BOUND")
        .ok()
        .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_DEPTH").ok())
        .unwrap_or_else(|| "64".to_string());
    println!(
        "Warmup: {} messages | Measure: {} messages | payload {} bytes | fanout {} | batch {} | publish_fastpath {} | pub conns {} | pub streams/conn {} | pub stream count {} | sub lanes {} | sub shard {} | sub conns {} | sub stream mode {} | sub queue bound {} | pub yield every {} batches | sub dedicated thread {} | sub delivery shaping {}",
        config.warmup,
        config.total,
        config.payload_bytes,
        config.fanout,
        config.batch_size,
        config.binary,
        config.pub_conns,
        config.pub_streams_per_conn,
        config.pub_stream_count,
        sub_lanes_hint,
        config.sub_lane_shard.as_deref().unwrap_or("default"),
        config.sub_conns.max(1),
        sub_stream_mode,
        sub_queue_bound,
        config.pub_yield_every_batches,
        config.sub_dedicated_thread,
        config.sub_delivery_shaping
    );
    println!("{}", expectation_note(&config));
    // A throughput run absorbed by the QUIC send window (64 MiB) reports
    // buffer-fill rate, not sustained throughput. Warn instead of refusing so
    // deliberate short runs stay possible, but the artifact can't pass silently.
    // Empty payloads move no bytes, so the send window cannot absorb them and
    // the check is meaningless there -- those cases are message-rate bound.
    let run_bytes = config.payload_bytes.saturating_mul(config.total);
    if config.payload_bytes > 0 && config.batch_size > 1 && run_bytes < 128 * 1024 * 1024 {
        println!(
            "WARNING: run volume {} MB is within ~2x the 64 MiB QUIC send window; \
             reported throughput may be buffer-fill rate, not sustained rate. \
             Raise --total so payload x total comfortably exceeds 128 MiB.",
            run_bytes / (1024 * 1024)
        );
    }
    let use_ack = config.batch_size <= 1 && !config.binary;
    let disable_timings = std::env::var("FELIX_DISABLE_TIMINGS")
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let collect_timings = (config.batch_size > 1 || use_ack) && !disable_timings;
    if collect_timings {
        let sample_every = std::env::var("FELIX_TIMING_SAMPLE_EVERY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1);
        client_timings::enable_collection(sample_every);
        broker_timings::enable_collection(sample_every);
        broker_publish_timings::enable_collection(sample_every);
    }
    client_timings::set_enabled(collect_timings);
    broker_timings::set_enabled(collect_timings);
    broker_publish_timings::set_enabled(collect_timings);

    let total_events = config.warmup + config.total;
    let log_capacity = config
        .log_capacity
        .unwrap_or_else(|| total_events.saturating_add(1));
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(total_events.saturating_add(1))?
            .with_log_capacity(log_capacity)?
            .with_subscriber_queue_policy(felix_broker::SubQueuePolicy::Block),
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    for stream_index in 0..config.pub_stream_count.max(1) {
        broker
            .register_stream(
                "t1",
                "default",
                stream_name(stream_index, config.pub_stream_count).as_str(),
                StreamMetadata::default(),
            )
            .await?;
    }
    #[cfg(feature = "telemetry")]
    {
        felix_broker_service::serving::quic::reset_frame_counters();
        felix_client::reset_frame_counters();
    }
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let mut broker_config = felix_broker_service::config::BrokerConfig::from_env()?;
    broker_config.fanout_batch_size = config.batch_size.max(1);
    // Tune the pipeline by delivery semantics. Only the single-message JSON path
    // waits for per-message acknowledgements; binary publishes are fire-and-forget
    // even with batch size 1 and therefore need lossless ingress backpressure.
    if use_ack {
        // Block the core per-subscriber queue too, not just the lane queue below it:
        // leaving it at production defaults (DropNew, capacity 512) let a publish burst
        // or scheduling delay drop warmup messages, which made the dedicated
        // subscriber's exact-count warmup detection hang until idle_timeout and error
        // out ("dedicated subscriber warmup channel closed") instead of completing.
        broker_config.subscriber_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_queue_capacity = 4096;
        broker_config.subscriber_lane_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_flush_max_items = 1;
        broker_config.subscriber_flush_max_delay_us = 0;
        broker_config.subscriber_max_bytes_per_write = 64 * 1024;
        broker_config.subscriber_single_writer_per_conn = true;
    } else {
        // Throughput profile measures *sustainable* rate: lossless end-to-end
        // backpressure (Block queues + ingress wait) paces the publisher to the
        // pipeline's real capacity instead of measuring what survived shedding.
        // Production keeps DropNew defaults for overload visibility.
        broker_config.subscriber_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_queue_capacity = 4096;
        broker_config.subscriber_lane_queue_policy = felix_broker::SubQueuePolicy::Block;
        broker_config.subscriber_lane_queue_depth = 1024;
        broker_config.pub_ingress_wait = true;
        broker_config.publish_queue_wait_timeout_ms = 60_000;
        broker_config.subscriber_flush_max_items = 16;
        broker_config.subscriber_flush_max_delay_us = 50;
        broker_config.subscriber_max_bytes_per_write = 64 * 1024;
        broker_config.subscriber_single_writer_per_conn = false;
    }
    if config.sub_delivery_shaping {
        broker_config.event_batch_max_events = broker_config.event_batch_max_events.max(64);
        broker_config.event_batch_max_bytes = 64 * 1024;
        broker_config.event_batch_max_delay_us = 250;
        broker_config.subscriber_max_bytes_per_write = 64 * 1024;
    } else {
        broker_config.event_batch_max_events = 1;
        broker_config.event_batch_max_bytes = 1024;
        broker_config.event_batch_max_delay_us = 0;
    }
    // The profiles above pin egress sizing, so explicit env overrides would be
    // silently ignored without re-applying them here.
    if let Ok(value) = std::env::var("FELIX_EVENT_BATCH_MAX_BYTES")
        && let Ok(bytes) = value.parse::<usize>()
        && bytes > 0
    {
        broker_config.event_batch_max_bytes = bytes;
    }
    if let Ok(value) = std::env::var("FELIX_SUB_MAX_BYTES_PER_WRITE")
        && let Ok(bytes) = value.parse::<usize>()
        && bytes > 0
    {
        broker_config.subscriber_max_bytes_per_write = bytes;
    }
    if let Some(lanes) = config.sub_writer_lanes {
        broker_config.subscriber_writer_lanes = lanes.max(1);
    }
    if let Ok(raw) = std::env::var("FELIX_SUB_EGRESS_LANES")
        && let Ok(expected) = raw.parse::<usize>()
    {
        let active = broker_config.subscriber_writer_lanes.max(1);
        if expected > 0 && active != expected {
            return Err(anyhow::anyhow!(
                "FELIX_SUB_EGRESS_LANES set to {} but active lanes is {}",
                expected,
                active
            ));
        }
    }
    if let Some(shard) = config.sub_lane_shard.as_deref() {
        broker_config.subscriber_lane_shard = match shard {
            "auto" => felix_broker_service::config::SubscriberLaneShard::Auto,
            "subscriber_id_hash" => {
                felix_broker_service::config::SubscriberLaneShard::SubscriberIdHash
            }
            "connection_id_hash" => {
                felix_broker_service::config::SubscriberLaneShard::ConnectionIdHash
            }
            "round_robin_pin" => felix_broker_service::config::SubscriberLaneShard::RoundRobinPin,
            _ => broker_config.subscriber_lane_shard,
        };
    }
    let (auth, auth_override) = resolve_demo_auth(&broker_config)?;
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
        auth,
    ));

    let mut client_config = apply_demo_auth(build_client_config(cert)?, auth_override);
    // The broker-side subscriber queues above are set to Block so a publish burst can't
    // get dropped before it reaches the client. The client's own subscriber channel
    // (between the QUIC receive task and the code consuming events) defaults to
    // DropNew independently of any of that, and was silently eating events under load
    // even after the broker-side fix — same failure mode, different layer.
    client_config.client_sub_queue_policy = felix_client::ClientSubQueuePolicy::Block;
    client_config.publish_conn_pool = config.pub_conns.max(1);
    client_config.publish_streams_per_conn = config.pub_streams_per_conn.max(1);
    if let Some(sharding) = config.pub_sharding.as_deref().and_then(parse_sharding_mode) {
        client_config.publish_sharding = sharding;
    }
    let client = Client::connect(addr, "localhost", client_config.clone()).await?;
    let mut sub_clients = Vec::with_capacity(config.sub_conns.max(1));
    for _ in 0..config.sub_conns.max(1) {
        sub_clients.push(Client::connect(addr, "localhost", client_config.clone()).await?);
    }

    let delivery_tracker = DeliveryTracker::new();
    let idle_timeout = Duration::from_millis(config.idle_ms);
    let (primary_sub, drain_tasks) = setup_subscribers(
        &sub_clients,
        config.fanout,
        total_events,
        config.warmup,
        config.pub_stream_count,
        delivery_tracker.clone(),
        config.sub_dedicated_thread,
        idle_timeout,
    )
    .await?;

    let primary_total = per_stream_events(config.total, config.pub_stream_count.max(1), 0);
    let primary_warmup = per_stream_events(config.warmup, config.pub_stream_count.max(1), 0);
    let mut dedicated_handle = if config.sub_dedicated_thread {
        Some(spawn_dedicated_primary_subscriber(
            addr,
            client_config.clone(),
            idle_timeout,
            primary_warmup,
            primary_total,
            config.pub_stream_count.max(1),
            delivery_tracker.clone(),
        ))
    } else {
        None
    };
    let mut warmup_rx = None;
    let recv_task = if let Some(mut sub) = primary_sub {
        let (warmup_tx, local_warmup_rx) = oneshot::channel();
        warmup_rx = Some(local_warmup_rx);
        let tracker_for_primary = delivery_tracker.clone();
        Some(tokio::spawn(async move {
            let mut results = Vec::with_capacity(primary_total);
            let mut seen = 0usize;
            let mut warmup_tx = Some(warmup_tx);
            if primary_warmup == 0
                && let Some(tx) = warmup_tx.take()
            {
                let _ = tx.send(());
            }
            while results.len() < primary_total {
                let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
                let event = match next {
                    Ok(result) => result,
                    Err(_) => {
                        anyhow::bail!(
                            "primary subscriber timed out after {} of {} measured events",
                            results.len(),
                            primary_total
                        );
                    }
                };
                match event {
                    Ok(Some(event)) => {
                        let sample = client_timings::should_sample();
                        let dispatch_start = sample.then(Instant::now);
                        if seen < primary_warmup {
                            seen += 1;
                            if seen == primary_warmup
                                && let Some(tx) = warmup_tx.take()
                            {
                                let _ = tx.send(());
                            }
                            continue;
                        }
                        let elapsed = decode_latency(event.payload.as_ref());
                        results.push(elapsed);
                        tracker_for_primary.record();
                        if let Some(start) = dispatch_start {
                            let dispatch_ns = start.elapsed().as_nanos() as u64;
                            client_timings::record_sub_dispatch_ns(dispatch_ns);
                            metrics::histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!(
                            "primary subscriber closed after {} of {} measured events",
                            results.len(),
                            primary_total
                        );
                    }
                    Err(err) => {
                        return Err(anyhow::anyhow!("primary subscriber failed: {err}"));
                    }
                }
            }
            Ok::<Vec<Duration>, anyhow::Error>(results)
        }))
    } else {
        None
    };
    if let Some(handle) = &mut dedicated_handle {
        handle.await_ready().await?;
    }
    let dedicated_recv_task = if let Some(handle) = &mut dedicated_handle {
        let mut rx = handle
            .delivery_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated delivery receiver missing"))?;
        Some(tokio::spawn(async move {
            let mut results = Vec::with_capacity(primary_total);
            while results.len() < primary_total {
                let next = tokio::time::timeout(idle_timeout, rx.recv()).await;
                match next {
                    Ok(Some(latency)) => results.push(latency),
                    Ok(None) => {
                        return Err(format!(
                            "dedicated delivery channel closed after {} of {} measured events",
                            results.len(),
                            primary_total
                        ));
                    }
                    Err(_) => {
                        return Err(format!(
                            "dedicated delivery channel timed out after {} of {} measured events",
                            results.len(),
                            primary_total
                        ));
                    }
                }
            }
            Ok(results)
        }))
    } else {
        None
    };

    let publisher = client.publisher().await?;
    publish_batches(
        &publisher,
        &PublishBatchConfig {
            payload_bytes: config.payload_bytes,
            total: config.warmup,
            batch_size: config.batch_size,
            binary: config.binary,
            use_ack,
            stream_count: config.pub_stream_count,
            pub_yield_every_batches: config.pub_yield_every_batches,
        },
    )
    .await?;
    if let Some(handle) = &mut dedicated_handle {
        handle.await_warmup().await
    } else if let Some(local_warmup_rx) = warmup_rx {
        local_warmup_rx
            .await
            .map_err(|_| anyhow::anyhow!("warmup channel closed"))
    } else {
        Ok(())
    }?;

    let start = Instant::now();
    publish_batches(
        &publisher,
        &PublishBatchConfig {
            payload_bytes: config.payload_bytes,
            total: config.total,
            batch_size: config.batch_size,
            binary: config.binary,
            use_ack,
            stream_count: config.pub_stream_count,
            pub_yield_every_batches: config.pub_yield_every_batches,
        },
    )
    .await?;
    let publish_elapsed = start.elapsed();

    let mut latencies = if let Some(task) = recv_task {
        task.await.expect("join")?
    } else if let Some(task) = dedicated_recv_task {
        task.await.expect("join").map_err(anyhow::Error::msg)?
    } else if let Some(handle) = dedicated_handle.take() {
        handle.finish().await?;
        Vec::new()
    } else {
        Vec::new()
    };
    if let Some(handle) = dedicated_handle.take() {
        handle.finish().await?;
    }
    let expected_delivered_total = config.total * config.fanout;
    publisher.finish().await?;
    join_drain_tasks(drain_tasks, idle_timeout).await?;
    // End the delivery window at the last event that actually arrived, not when
    // the drain tasks were joined.
    let delivery_elapsed = delivery_tracker
        .last_delivery_at()
        .and_then(|at| at.checked_duration_since(start))
        .map(|elapsed| elapsed.max(publish_elapsed))
        .unwrap_or(publish_elapsed);
    server_task.abort();

    latencies.sort_unstable();
    let received = latencies.len();
    let summary = if collect_timings {
        let client_samples = client_timings::take_samples();
        let broker_samples = broker_timings::take_samples();
        let broker_publish_samples = broker_publish_timings::take_samples();
        Some(build_timing_summary(
            client_samples,
            broker_samples,
            broker_publish_samples,
        ))
    } else {
        None
    };
    let delivered_total = delivery_tracker.total();
    let drain_duration = delivery_elapsed.saturating_sub(publish_elapsed);
    let delivered_throughput = delivered_total as f64 / delivery_elapsed.as_secs_f64();
    let delivered_per_sub_throughput =
        (delivered_total as f64 / config.fanout.max(1) as f64) / delivery_elapsed.as_secs_f64();
    Ok((
        DemoResult {
            publish_total: config.total,
            sample_total: primary_total,
            payload_bytes: config.payload_bytes,
            fanout: config.fanout,
            batch_size: config.batch_size,
            binary: config.binary,
            received,
            unaccounted: expected_delivered_total.saturating_sub(delivered_total),
            delivered_total,
            p50: percentile(&latencies, 0.50),
            p99: percentile(&latencies, 0.99),
            p999: percentile(&latencies, 0.999),
            publish_duration: publish_elapsed,
            drain_duration,
            publish_submit_throughput: config.total as f64 / publish_elapsed.as_secs_f64(),
            throughput: config.total as f64 / delivery_elapsed.as_secs_f64(),
            effective_throughput: received as f64 / delivery_elapsed.as_secs_f64(),
            delivered_throughput,
            delivered_per_sub_throughput,
        },
        summary,
    ))
}

async fn run_compare_sub_delivery_shaping(mut base: DemoConfig) -> Result<()> {
    base.sub_delivery_shaping = false;
    let (off_result, off_summary) = run_case(base.clone()).await?;
    base.sub_delivery_shaping = true;
    let (on_result, on_summary) = run_case(base).await?;

    #[cfg(feature = "telemetry")]
    {
        let off = off_summary.ok_or_else(|| anyhow::anyhow!("missing timing summary (OFF run)"))?;
        let on = on_summary.ok_or_else(|| anyhow::anyhow!("missing timing summary (ON run)"))?;
        println!(
            "Shaping compare: OFF p99={} p999={} read_await_p99={} write_await_p99={} | ON p99={} p999={} read_await_p99={} write_await_p99={}",
            format_duration(off_result.p99),
            format_duration(off_result.p999),
            format_optional_duration(off.client_sub_read_await_p99),
            format_optional_duration(off.broker_sub_write_await_p99),
            format_duration(on_result.p99),
            format_duration(on_result.p999),
            format_optional_duration(on.client_sub_read_await_p99),
            format_optional_duration(on.broker_sub_write_await_p99),
        );
    }
    #[cfg(not(feature = "telemetry"))]
    {
        let _ = (off_summary, on_summary);
        println!(
            "Shaping compare: OFF p99={} p999={} | ON p99={} p999={}",
            format_duration(off_result.p99),
            format_duration(off_result.p999),
            format_duration(on_result.p99),
            format_duration(on_result.p999),
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn setup_subscribers(
    clients: &[Client],
    fanout: usize,
    total_events: usize,
    warmup: usize,
    stream_count: usize,
    delivery_tracker: DeliveryTracker,
    reserve_primary_slot: bool,
    idle_timeout: Duration,
) -> Result<(Option<Subscription>, Vec<DrainTask>)> {
    let mut drain_tasks = Vec::new();
    let mut primary_sub = None;
    let mut primary_slot_reserved = reserve_primary_slot;
    let assign_primary = !reserve_primary_slot;
    let stream_count = stream_count.max(1);
    if clients.is_empty() {
        anyhow::bail!("no subscriber clients configured");
    }
    let mut next_client = 0usize;
    for stream_index in 0..stream_count {
        let per_stream_total = per_stream_events(total_events, stream_count, stream_index);
        let per_stream_warmup = per_stream_events(warmup, stream_count, stream_index);
        for _ in 0..fanout {
            let stream = stream_name(stream_index, stream_count);
            let client = &clients[next_client % clients.len()];
            next_client = next_client.wrapping_add(1);
            if stream_index == 0 && primary_slot_reserved {
                primary_slot_reserved = false;
                continue;
            }
            let sub = client.subscribe("t1", "default", stream.as_str()).await?;
            if assign_primary && primary_sub.is_none() {
                primary_sub = Some(sub);
            } else {
                let mut sub = sub;
                // Use the same configurable liveness bound as the primary
                // subscriber, but fail the run if it expires. Silently abandoning
                // this subscription makes `expected - observed` look like queue
                // shedding even though every delivery queue is configured Block.
                let tracker = delivery_tracker.clone();
                let received = Arc::new(AtomicUsize::new(0));
                let task_received = received.clone();
                let label = format!("{stream}#{}", drain_tasks.len());
                let handle = tokio::spawn(async move {
                    let mut remaining = per_stream_total;
                    let mut seen = 0usize;
                    while remaining > 0 {
                        let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
                        match next {
                            Ok(Ok(Some(_))) => {
                                remaining -= 1;
                                task_received.fetch_add(1, Ordering::Relaxed);
                                if seen >= per_stream_warmup {
                                    tracker.record();
                                }
                                seen += 1;
                            }
                            Ok(Ok(None)) => {
                                anyhow::bail!(
                                    "subscriber stream {stream} closed with {remaining} events remaining"
                                );
                            }
                            Ok(Err(err)) => {
                                return Err(err)
                                    .with_context(|| format!("subscriber stream {stream} failed"));
                            }
                            Err(_) => {
                                anyhow::bail!(
                                    "subscriber stream {stream} timed out with {remaining} events remaining"
                                );
                            }
                        }
                    }
                    Ok(())
                });
                drain_tasks.push(DrainTask {
                    label,
                    expected: per_stream_total,
                    received,
                    handle,
                });
            }
        }
    }
    Ok((primary_sub, drain_tasks))
}

/// A non-primary subscriber draining its share of the run.
struct DrainTask {
    label: String,
    expected: usize,
    received: Arc<AtomicUsize>,
    handle: tokio::task::JoinHandle<Result<()>>,
}

/// Waits for every drain task, failing only when none has made progress for
/// `idle_timeout` or one fails on its own.
///
/// Waiting on them together matters: a per-task deadline measures how long a
/// subscriber takes to finish, not whether it stalled, so a healthy subscriber
/// that is simply behind the others trips it. On failure the error lists every
/// subscriber still draining and how far it got.
async fn join_drain_tasks(tasks: Vec<DrainTask>, idle_timeout: Duration) -> Result<()> {
    use futures::stream::{FuturesUnordered, StreamExt};

    let progress: Vec<_> = tasks
        .iter()
        .map(|task| (task.label.clone(), task.expected, task.received.clone()))
        .collect();
    let mut done = vec![false; tasks.len()];
    let mut pending: FuturesUnordered<_> = tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| async move { (index, task.handle.await) })
        .collect();
    let received_total = || {
        progress
            .iter()
            .map(|(_, _, received)| received.load(Ordering::Relaxed))
            .sum::<usize>()
    };
    let unfinished = |done: &[bool]| {
        progress
            .iter()
            .zip(done)
            .filter(|(_, done)| !**done)
            .map(|((label, expected, received), _)| {
                format!("{label} {}/{expected}", received.load(Ordering::Relaxed))
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut last_total = received_total();
    let mut deadline = tokio::time::Instant::now() + idle_timeout;
    loop {
        tokio::select! {
            next = pending.next() => {
                let Some((index, joined)) = next else {
                    return Ok(());
                };
                done[index] = true;
                let result = joined.context("subscriber drain task panicked")?;
                if let Err(err) = result {
                    return Err(err.context(format!(
                        "subscriber drain failed; still draining: [{}]",
                        unfinished(&done)
                    )));
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let total = received_total();
                if total == last_total {
                    anyhow::bail!(
                        "subscriber drain stalled: no event in {idle_timeout:?}; still draining: [{}]",
                        unfinished(&done)
                    );
                }
                last_total = total;
                deadline = tokio::time::Instant::now() + idle_timeout;
            }
        }
    }
}

async fn publish_batches(publisher: &Publisher, config: &PublishBatchConfig) -> Result<()> {
    // Send each stream exactly `per_stream_events(total, ...)` events — the
    // same split subscriber accounting assumes. Batch-granular round-robin
    // over a single global counter would skew per-stream totals (whole batches
    // land on early streams), misclassifying warmup boundaries and leaving
    // drain tasks waiting on events that never arrive.
    let stream_count = config.stream_count.max(1);
    let mut remaining: Vec<usize> = (0..stream_count)
        .map(|index| per_stream_events(config.total, stream_count, index))
        .collect();
    let mut stream_index = 0usize;
    let mut sent_batches = 0usize;
    while remaining.iter().any(|&count| count > 0) {
        // Round-robin across streams that still have events to send.
        let mut probes = 0usize;
        while remaining[stream_index % stream_count] == 0 && probes < stream_count {
            stream_index = stream_index.wrapping_add(1);
            probes += 1;
        }
        let index = stream_index % stream_count;
        let count = remaining[index].min(config.batch_size);
        publish_batch(
            publisher,
            config.payload_bytes,
            count,
            config.binary,
            config.use_ack,
            index,
            config.stream_count,
        )
        .await?;
        remaining[index] -= count;
        stream_index = stream_index.wrapping_add(1);
        sent_batches = sent_batches.wrapping_add(1);
        if config.pub_yield_every_batches > 0
            && sent_batches.is_multiple_of(config.pub_yield_every_batches)
        {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

struct PublishBatchConfig {
    payload_bytes: usize,
    total: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
    stream_count: usize,
    pub_yield_every_batches: usize,
}

struct DedicatedPrimaryHandle {
    ready_rx: Option<oneshot::Receiver<Result<(), String>>>,
    warmup_rx: Option<oneshot::Receiver<()>>,
    error_rx: oneshot::Receiver<Result<(), String>>,
    delivery_rx: Option<tokio::sync::mpsc::Receiver<Duration>>,
    join: Option<thread::JoinHandle<()>>,
}

impl DedicatedPrimaryHandle {
    async fn await_ready(&mut self) -> Result<()> {
        let ready_rx = self
            .ready_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated subscriber ready already awaited"))?;
        let ready = ready_rx
            .await
            .map_err(|_| anyhow::anyhow!("dedicated subscriber ready channel closed"))?;
        ready.map_err(anyhow::Error::msg)
    }

    async fn await_warmup(&mut self) -> Result<()> {
        let warmup_rx = self
            .warmup_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("dedicated subscriber warmup already awaited"))?;
        tokio::select! {
            result = warmup_rx => {
                if result.is_ok() {
                    Ok(())
                } else {
                    match (&mut self.error_rx).await {
                        Ok(Ok(())) => Err(anyhow::anyhow!(
                            "dedicated subscriber completed before receiving all warmup events"
                        )),
                        Ok(Err(err)) => Err(anyhow::Error::msg(err)),
                        Err(_) => Err(anyhow::anyhow!(
                            "dedicated subscriber warmup and completion channels closed"
                        )),
                    }
                }
            }
            result = &mut self.error_rx => {
                match result {
                    Ok(Ok(())) => Err(anyhow::anyhow!(
                        "dedicated subscriber completed before receiving all warmup events"
                    )),
                    Ok(Err(err)) => Err(anyhow::Error::msg(err)),
                    Err(_) => Err(anyhow::anyhow!("dedicated subscriber completion channel closed")),
                }
            }
        }
    }

    async fn finish(mut self) -> Result<()> {
        let recv = self
            .error_rx
            .await
            .map_err(|_| anyhow::anyhow!("dedicated subscriber completion channel closed"))?;
        if let Some(join) = self.join.take() {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = join.join();
            })
            .await;
        }
        recv.map_err(anyhow::Error::msg)
    }
}

fn spawn_dedicated_primary_subscriber(
    addr: std::net::SocketAddr,
    client_config: ClientConfig,
    idle_timeout: Duration,
    primary_warmup: usize,
    primary_total: usize,
    stream_count: usize,
    delivery_tracker: DeliveryTracker,
) -> DedicatedPrimaryHandle {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (warmup_ready_tx, warmup_ready_rx) = oneshot::channel();
    let (error_tx, error_rx) = oneshot::channel();
    let chan_capacity = std::env::var("FELIX_SUB_DEDICATED_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4096)
        .max(1);
    let (delivery_tx, delivery_rx) = tokio::sync::mpsc::channel(chan_capacity);
    let join = thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                let _ = error_tx.send(Err(format!("build dedicated runtime failed: {err}")));
                return;
            }
        };
        let result = runtime.block_on(async move {
            let mut warmup_ready_tx = Some(warmup_ready_tx);
            let delivery_tx = delivery_tx;
            let client = match Client::connect(addr, "localhost", client_config).await {
                Ok(client) => client,
                Err(err) => {
                    let _ =
                        ready_tx.send(Err(format!("dedicated subscriber connect failed: {err}")));
                    return Err(format!("dedicated subscriber connect failed: {err}"));
                }
            };
            let mut sub = match client
                .subscribe("t1", "default", stream_name(0, stream_count).as_str())
                .await
            {
                Ok(sub) => sub,
                Err(err) => {
                    let _ =
                        ready_tx.send(Err(format!("dedicated subscriber subscribe failed: {err}")));
                    return Err(format!("dedicated subscriber subscribe failed: {err}"));
                }
            };
            let _ = ready_tx.send(Ok(()));
            let mut seen = 0usize;
            let mut last_runtime_tick = Instant::now();
            if primary_warmup == 0
                && let Some(tx) = warmup_ready_tx.take()
            {
                let _ = tx.send(());
            }
            while seen < primary_warmup + primary_total {
                let runtime_gap_ns = last_runtime_tick.elapsed().as_nanos() as u64;
                last_runtime_tick = Instant::now();
                client_timings::record_sub_runtime_gap_ns(runtime_gap_ns);
                metrics::histogram!("client_sub_runtime_gap_ns").record(runtime_gap_ns as f64);
                let next = tokio::time::timeout(idle_timeout, sub.next_event()).await;
                let event = match next {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(format!(
                            "dedicated subscriber timed out after {seen} of {} events",
                            primary_warmup + primary_total
                        ));
                    }
                };
                match event {
                    Ok(Some(event)) => {
                        let sample = client_timings::should_sample();
                        let dispatch_start = sample.then(Instant::now);
                        if seen < primary_warmup {
                            seen += 1;
                            if seen == primary_warmup
                                && let Some(tx) = warmup_ready_tx.take()
                            {
                                let _ = tx.send(());
                            }
                            continue;
                        }
                        let latency = decode_latency(event.payload.as_ref());
                        let enqueue_start = Instant::now();
                        if delivery_tx.send(latency).await.is_err() {
                            return Err(
                                "dedicated delivery receiver closed before completion".to_string()
                            );
                        }
                        seen += 1;
                        let wait_ns = enqueue_start.elapsed().as_nanos() as u64;
                        client_timings::record_sub_delivery_chan_wait_ns(wait_ns);
                        metrics::histogram!("client_sub_delivery_chan_wait_ns")
                            .record(wait_ns as f64);
                        if wait_ns > 0 {
                            metrics::counter!(
                                "felix_client_sub_delivery_channel_backpressure_total"
                            )
                            .increment(1);
                        }
                        metrics::gauge!("felix_client_sub_delivery_channel_depth")
                            .set((chan_capacity.saturating_sub(delivery_tx.capacity())) as f64);
                        delivery_tracker.record();
                        if let Some(start) = dispatch_start {
                            let dispatch_ns = start.elapsed().as_nanos() as u64;
                            client_timings::record_sub_dispatch_ns(dispatch_ns);
                            metrics::histogram!("sub_dispatch_ns").record(dispatch_ns as f64);
                        }
                    }
                    Ok(None) => {
                        return Err(format!(
                            "dedicated subscriber closed after {seen} of {} events",
                            primary_warmup + primary_total
                        ));
                    }
                    Err(err) => {
                        return Err(format!("dedicated subscriber receive failed: {err}"));
                    }
                }
            }
            Ok(())
        });
        let _ = error_tx.send(result);
    });
    DedicatedPrimaryHandle {
        ready_rx: Some(ready_rx),
        warmup_rx: Some(warmup_ready_rx),
        error_rx,
        delivery_rx: Some(delivery_rx),
        join: Some(join),
    }
}

async fn publish_batch(
    publisher: &Publisher,
    payload_bytes: usize,
    batch_size: usize,
    binary: bool,
    use_ack: bool,
    stream_index: usize,
    stream_count: usize,
) -> Result<()> {
    let stream = stream_name(stream_index, stream_count);
    if binary {
        let payloads = (0..batch_size)
            .map(|_| Bytes::from(encode_payload(payload_bytes)))
            .collect::<Vec<_>>();
        return publisher
            .publish_batch_binary_bytes("t1", "default", stream.as_str(), &payloads)
            .await;
    }
    let payloads = (0..batch_size)
        .map(|_| encode_payload(payload_bytes))
        .collect::<Vec<_>>();
    // The JSON encoding on purpose: the demo's whole point is the comparison
    // against the binary path above, so it has to keep reaching the deprecated
    // call rather than routing around it.
    #[allow(deprecated)]
    if batch_size == 1 {
        publisher
            .publish_json(
                "t1",
                "default",
                stream.as_str(),
                payloads[0].clone(),
                if use_ack {
                    AckMode::PerMessage
                } else {
                    AckMode::None
                },
            )
            .await?;
    } else {
        publisher
            .publish_batch_json(
                "t1",
                "default",
                stream.as_str(),
                payloads,
                if use_ack {
                    AckMode::PerBatch
                } else {
                    AckMode::None
                },
            )
            .await?;
    }
    Ok(())
}

fn stream_name(index: usize, stream_count: usize) -> String {
    if stream_count <= 1 {
        return "latency".to_string();
    }
    let index = index % stream_count;
    format!("latency-{index}")
}

fn per_stream_events(total: usize, stream_count: usize, stream_index: usize) -> usize {
    let base = total / stream_count;
    let extra = total % stream_count;
    base + usize::from(stream_index < extra)
}

fn print_result(result: &DemoResult) {
    println!(
        "Results (publish n = {}, sampled {}, received {}, unaccounted {}) payload={}B fanout={} batch={} publish_fastpath={}:",
        result.publish_total,
        result.sample_total,
        result.received,
        result.unaccounted,
        result.payload_bytes,
        result.fanout,
        result.batch_size,
        result.binary
    );
    println!("  delivered total = {}", result.delivered_total);
    if result.received == 0 {
        println!("  p50  = n/a (no received samples)");
        println!("  p99  = n/a (no received samples)");
        println!("  p999 = n/a (no received samples)");
    } else if result.batch_size > 1 {
        // A batched run is paced by lossless backpressure, so a message waits
        // for its batch to fill and then behind everything already queued.
        // These percentiles therefore scale with run length and say nothing
        // about per-message latency -- label them so they are not read as a
        // regression when a longer run reports a bigger number. The batch=1
        // profile below is the one that measures request latency.
        println!(
            "  queueing delay (NOT per-message latency; batch={} run, grows with --total):",
            result.batch_size
        );
        println!("    p50  = {}", format_duration(result.p50));
        println!("    p99  = {}", format_duration(result.p99));
        println!("    p999 = {}", format_duration(result.p999));
    } else {
        println!("  p50  = {}", format_duration(result.p50));
        println!("  p99  = {}", format_duration(result.p99));
        println!("  p999 = {}", format_duration(result.p999));
    }
    println!(
        "  publish submit throughput = {:.2} msg/s",
        result.publish_submit_throughput
    );
    println!(
        "  publish duration = {}",
        format_duration(result.publish_duration)
    );
    println!(
        "  delivery drain duration = {}",
        format_duration(result.drain_duration)
    );
    println!("  throughput = {:.2} msg/s", result.throughput);
    println!(
        "  effective throughput = {:.2} msg/s",
        result.effective_throughput
    );
    println!(
        "  delivered throughput = {:.2} msg/s",
        result.delivered_throughput
    );
    println!(
        "  delivered per-sub throughput = {:.2} msg/s",
        result.delivered_per_sub_throughput
    );
}

#[cfg(feature = "telemetry")]
fn print_timing_summary(summary: Option<TimingSummary>) {
    let Some(summary) = summary else {
        return;
    };
    println!(
        "  timings: client_pub_enqueue_wait p50={} p99={} p999={} client_encode p50={} p99={} client_binary_encode p50={} p99={} p999={} client_text_encode p50={} p99={} p999={} client_text_batch_build p50={} p99={} p999={} client_write p50={} p99={} client_send_await p50={} p99={} p999={} client_sub_read p50={} p99={} client_sub_read_await p50={} p99={} client_sub_poll_gap p50={} p99={} p999={} client_sub_queue_wait p50={} p99={} client_sub_time_in_queue p50={} p99={} p999={} client_sub_runtime_gap p50={} p99={} p999={} client_sub_delivery_chan_wait p50={} p99={} p999={} client_sub_decode p50={} p99={} client_sub_dispatch p50={} p99={} client_sub_consumer_gap p50={} p99={} p999={} client_e2e_latency p50={} p99={} p999={} client_ack_read p50={} p99={} client_ack_decode p50={} p99={} broker_decode p50={} p99={} broker_ingress_enqueue p50={} p99={} broker_ack_write p50={} p99={} broker_quic_write p50={} p99={} broker_sub_queue_wait p50={} p99={} broker_sub_prefix_build p50={} p99={} broker_sub_write p50={} p99={} broker_sub_write_await p50={} p99={} broker_sub_delivery p50={} p99={} p999={} broker_publish_lookup p50={} p99={} broker_publish_append p50={} p99={} broker_publish_fanout p50={} p99={} broker_publish_enqueue p50={} p99={} broker_publish_send p50={} p99={}",
        format_optional_duration(summary.client_pub_enqueue_wait_p50),
        format_optional_duration(summary.client_pub_enqueue_wait_p99),
        format_optional_duration(summary.client_pub_enqueue_wait_p999),
        format_optional_duration(summary.client_encode_p50),
        format_optional_duration(summary.client_encode_p99),
        format_optional_duration(summary.client_binary_encode_p50),
        format_optional_duration(summary.client_binary_encode_p99),
        format_optional_duration(summary.client_binary_encode_p999),
        format_optional_duration(summary.client_text_encode_p50),
        format_optional_duration(summary.client_text_encode_p99),
        format_optional_duration(summary.client_text_encode_p999),
        format_optional_duration(summary.client_text_batch_build_p50),
        format_optional_duration(summary.client_text_batch_build_p99),
        format_optional_duration(summary.client_text_batch_build_p999),
        format_optional_duration(summary.client_write_p50),
        format_optional_duration(summary.client_write_p99),
        format_optional_duration(summary.client_send_await_p50),
        format_optional_duration(summary.client_send_await_p99),
        format_optional_duration(summary.client_send_await_p999),
        format_optional_duration(summary.client_sub_read_p50),
        format_optional_duration(summary.client_sub_read_p99),
        format_optional_duration(summary.client_sub_read_await_p50),
        format_optional_duration(summary.client_sub_read_await_p99),
        format_optional_duration(summary.client_sub_poll_gap_p50),
        format_optional_duration(summary.client_sub_poll_gap_p99),
        format_optional_duration(summary.client_sub_poll_gap_p999),
        format_optional_duration(summary.client_sub_queue_wait_p50),
        format_optional_duration(summary.client_sub_queue_wait_p99),
        format_optional_duration(summary.client_sub_time_in_queue_p50),
        format_optional_duration(summary.client_sub_time_in_queue_p99),
        format_optional_duration(summary.client_sub_time_in_queue_p999),
        format_optional_duration(summary.client_sub_runtime_gap_p50),
        format_optional_duration(summary.client_sub_runtime_gap_p99),
        format_optional_duration(summary.client_sub_runtime_gap_p999),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p50),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p99),
        format_optional_duration(summary.client_sub_delivery_chan_wait_p999),
        format_optional_duration(summary.client_sub_decode_p50),
        format_optional_duration(summary.client_sub_decode_p99),
        format_optional_duration(summary.client_sub_dispatch_p50),
        format_optional_duration(summary.client_sub_dispatch_p99),
        format_optional_duration(summary.client_sub_consumer_gap_p50),
        format_optional_duration(summary.client_sub_consumer_gap_p99),
        format_optional_duration(summary.client_sub_consumer_gap_p999),
        format_optional_duration(summary.client_e2e_latency_p50),
        format_optional_duration(summary.client_e2e_latency_p99),
        format_optional_duration(summary.client_e2e_latency_p999),
        format_optional_duration(summary.client_ack_read_p50),
        format_optional_duration(summary.client_ack_read_p99),
        format_optional_duration(summary.client_ack_decode_p50),
        format_optional_duration(summary.client_ack_decode_p99),
        format_optional_duration(summary.broker_decode_p50),
        format_optional_duration(summary.broker_decode_p99),
        format_optional_duration(summary.broker_fanout_p50),
        format_optional_duration(summary.broker_fanout_p99),
        format_optional_duration(summary.broker_ack_write_p50),
        format_optional_duration(summary.broker_ack_write_p99),
        format_optional_duration(summary.broker_quic_write_p50),
        format_optional_duration(summary.broker_quic_write_p99),
        format_optional_duration(summary.broker_sub_queue_wait_p50),
        format_optional_duration(summary.broker_sub_queue_wait_p99),
        format_optional_duration(summary.broker_sub_prefix_build_p50),
        format_optional_duration(summary.broker_sub_prefix_build_p99),
        format_optional_duration(summary.broker_sub_write_p50),
        format_optional_duration(summary.broker_sub_write_p99),
        format_optional_duration(summary.broker_sub_write_await_p50),
        format_optional_duration(summary.broker_sub_write_await_p99),
        format_optional_duration(summary.broker_sub_delivery_p50),
        format_optional_duration(summary.broker_sub_delivery_p99),
        format_optional_duration(summary.broker_sub_delivery_p999),
        format_optional_duration(summary.broker_publish_lookup_p50),
        format_optional_duration(summary.broker_publish_lookup_p99),
        format_optional_duration(summary.broker_publish_append_p50),
        format_optional_duration(summary.broker_publish_append_p99),
        format_optional_duration(summary.broker_publish_fanout_p50),
        format_optional_duration(summary.broker_publish_fanout_p99),
        format_optional_duration(summary.broker_publish_enqueue_p50),
        format_optional_duration(summary.broker_publish_enqueue_p99),
        format_optional_duration(summary.broker_publish_send_p50),
        format_optional_duration(summary.broker_publish_send_p99),
    );
}

#[cfg(not(feature = "telemetry"))]
fn print_timing_summary(_summary: Option<TimingSummary>) {}

#[cfg(feature = "telemetry")]
fn print_frame_counters() -> (
    felix_client::FrameCountersSnapshot,
    felix_broker_service::serving::quic::FrameCountersSnapshot,
) {
    let broker = felix_broker_service::serving::quic::frame_counters_snapshot();
    let client = felix_client::frame_counters_snapshot();
    println!(
        "  frame_counters: client frames_in_ok={} frames_in_err={} frames_out_ok={} bytes_in={} bytes_out={} pub_out_ok={} pub_out_err={} pub_items_out_ok={} pub_items_out_err={} pub_batches_out_ok={} pub_batches_out_err={} sub_in_ok={} sub_items_in_ok={} sub_batches_in_ok={} ack_in_ok={} ack_items_in_ok={} binary_encode_reallocs={} text_encode_reallocs={}",
        client.frames_in_ok,
        client.frames_in_err,
        client.frames_out_ok,
        client.bytes_in,
        client.bytes_out,
        client.pub_frames_out_ok,
        client.pub_frames_out_err,
        client.pub_items_out_ok,
        client.pub_items_out_err,
        client.pub_batches_out_ok,
        client.pub_batches_out_err,
        client.sub_frames_in_ok,
        client.sub_items_in_ok,
        client.sub_batches_in_ok,
        client.ack_frames_in_ok,
        client.ack_items_in_ok,
        client.binary_encode_reallocs,
        client.text_encode_reallocs
    );
    println!(
        "  frame_counters: broker frames_in_ok={} frames_in_err={} frames_out_ok={} bytes_in={} bytes_out={} pub_in_ok={} pub_in_err={} pub_items_in_ok={} pub_items_in_err={} pub_batches_in_ok={} pub_batches_in_err={} ack_out_ok={} ack_items_out_ok={} sub_out_ok={} sub_items_out_ok={} sub_batches_out_ok={}",
        broker.frames_in_ok,
        broker.frames_in_err,
        broker.frames_out_ok,
        broker.bytes_in,
        broker.bytes_out,
        broker.pub_frames_in_ok,
        broker.pub_frames_in_err,
        broker.pub_items_in_ok,
        broker.pub_items_in_err,
        broker.pub_batches_in_ok,
        broker.pub_batches_in_err,
        broker.ack_frames_out_ok,
        broker.ack_items_out_ok,
        broker.sub_frames_out_ok,
        broker.sub_items_out_ok,
        broker.sub_batches_out_ok
    );
    println!(
        "  sub_delivery_counters: broker_batches_out={} broker_items_out={} client_batches_in={} client_items_in={}",
        broker.sub_batches_out_ok,
        broker.sub_items_out_ok,
        client.sub_batches_in_ok,
        client.sub_items_in_ok
    );
    (client, broker)
}

#[cfg(not(feature = "telemetry"))]
fn print_frame_counters() -> (
    felix_client::FrameCountersSnapshot,
    felix_broker_service::serving::quic::FrameCountersSnapshot,
) {
    println!("  frame_counters: telemetry disabled");
    (
        felix_client::frame_counters_snapshot(),
        felix_broker_service::serving::quic::frame_counters_snapshot(),
    )
}

#[cfg(feature = "telemetry")]
fn print_sanity_checks(
    config: &DemoConfig,
    result: &DemoResult,
    client: &felix_client::FrameCountersSnapshot,
    broker: &felix_broker_service::serving::quic::FrameCountersSnapshot,
) {
    let expected_items = config.warmup + config.total;
    let expected_batches = if config.batch_size == 0 {
        0
    } else {
        let warmup_batches = config.warmup.div_ceil(config.batch_size);
        let measure_batches = config.total.div_ceil(config.batch_size);
        warmup_batches + measure_batches
    };
    let expected_delivered = config.total * config.fanout;
    let use_ack = config.batch_size <= 1 && !config.binary;
    if client.pub_batches_out_ok > 0 && client.pub_frames_out_ok > 0 {
        let writes_per_batch = client.pub_frames_out_ok as f64 / client.pub_batches_out_ok as f64;
        let avg_bytes_per_write = client.bytes_out as f64 / client.pub_frames_out_ok as f64;
        println!(
            "  publish_write_shape: writes_per_batch={:.2} avg_bytes_per_write={:.1}",
            writes_per_batch, avg_bytes_per_write
        );
    }
    if client.pub_items_out_ok != expected_items as u64 {
        println!(
            "  sanity: pub_items_out_ok mismatch expected={} actual={}",
            expected_items, client.pub_items_out_ok
        );
    }
    if client.pub_batches_out_ok != expected_batches as u64 {
        println!(
            "  sanity: pub_batches_out_ok mismatch expected={} actual={}",
            expected_batches, client.pub_batches_out_ok
        );
    }
    if broker.pub_items_in_ok != client.pub_items_out_ok {
        println!(
            "  sanity: broker pub_items_in_ok mismatch client={} broker={}",
            client.pub_items_out_ok, broker.pub_items_in_ok
        );
    }
    if broker.pub_batches_in_ok != client.pub_batches_out_ok {
        println!(
            "  sanity: broker pub_batches_in_ok mismatch client={} broker={}",
            client.pub_batches_out_ok, broker.pub_batches_in_ok
        );
    }
    if result.delivered_total != expected_delivered {
        println!(
            "  sanity: delivered_total mismatch expected={} actual={}",
            expected_delivered, result.delivered_total
        );
    }
    // Subscribers now fail the run on timeout rather than abandoning their
    // remaining events, so a shortfall can no longer be explained away as
    // "the queues shed". Either the queue policy really is lossy or the
    // accounting is wrong; both invalidate the numbers above.
    if result.unaccounted > 0 {
        println!(
            "  sanity: INVALID RUN -- {} of {} expected deliveries never observed. \
             This is a shortfall, not a measured queue drop; throughput above is not meaningful.",
            result.unaccounted, expected_delivered
        );
    }
    if use_ack {
        if client.ack_items_in_ok == 0 {
            println!("  sanity: ack_items_in_ok expected > 0 (acks enabled)");
        }
    } else if client.ack_items_in_ok > 0 {
        println!(
            "  sanity: ack_items_in_ok expected 0 (acks disabled) actual={}",
            client.ack_items_in_ok
        );
    }
}

#[cfg(not(feature = "telemetry"))]
fn print_sanity_checks(
    _config: &DemoConfig,
    _result: &DemoResult,
    _client: &felix_client::FrameCountersSnapshot,
    _broker: &felix_broker_service::serving::quic::FrameCountersSnapshot,
) {
}

#[cfg(feature = "telemetry")]
fn build_timing_summary(
    client_samples: Option<client_timings::ClientTimingSamples>,
    broker_samples: Option<broker_timings::BrokerTimingSamples>,
    broker_publish_samples: Option<broker_publish_timings::BrokerPublishSamples>,
) -> TimingSummary {
    let (
        mut client_pub_enqueue_wait,
        mut client_encode,
        mut client_binary_encode,
        mut client_text_encode,
        mut client_text_batch_build,
        mut client_write,
        mut client_send_await,
        mut client_sub_read,
        mut client_sub_read_await,
        mut client_sub_queue_wait,
        mut client_sub_decode,
        mut client_sub_dispatch,
        mut client_sub_consumer_gap,
        mut client_sub_poll_gap,
        mut client_sub_time_in_queue,
        mut client_sub_runtime_gap,
        mut client_sub_delivery_chan_wait,
        mut client_e2e_latency,
        mut client_ack_read,
        mut client_ack_decode,
    ) = client_samples.unwrap_or_else(|| {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    });
    let (
        mut broker_decode,
        mut broker_fanout,
        mut broker_ack,
        mut broker_quic_write,
        mut broker_sub_queue,
        mut broker_sub_prefix,
        mut broker_sub_write,
        mut broker_sub_write_await,
        mut broker_sub_delivery,
    ) = broker_samples.unwrap_or_else(|| {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    });
    let (
        mut broker_publish_lookup,
        mut broker_publish_append,
        mut broker_publish_fanout,
        mut broker_publish_enqueue,
        mut broker_publish_send,
    ) = broker_publish_samples
        .unwrap_or_else(|| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    TimingSummary {
        client_pub_enqueue_wait_p50: percentile_ns(&mut client_pub_enqueue_wait, 0.50),
        client_pub_enqueue_wait_p99: percentile_ns(&mut client_pub_enqueue_wait, 0.99),
        client_pub_enqueue_wait_p999: percentile_ns(&mut client_pub_enqueue_wait, 0.999),
        client_encode_p50: percentile_ns(&mut client_encode, 0.50),
        client_encode_p99: percentile_ns(&mut client_encode, 0.99),
        client_binary_encode_p50: percentile_ns(&mut client_binary_encode, 0.50),
        client_binary_encode_p99: percentile_ns(&mut client_binary_encode, 0.99),
        client_binary_encode_p999: percentile_ns(&mut client_binary_encode, 0.999),
        client_text_encode_p50: percentile_ns(&mut client_text_encode, 0.50),
        client_text_encode_p99: percentile_ns(&mut client_text_encode, 0.99),
        client_text_encode_p999: percentile_ns(&mut client_text_encode, 0.999),
        client_text_batch_build_p50: percentile_ns(&mut client_text_batch_build, 0.50),
        client_text_batch_build_p99: percentile_ns(&mut client_text_batch_build, 0.99),
        client_text_batch_build_p999: percentile_ns(&mut client_text_batch_build, 0.999),
        client_write_p50: percentile_ns(&mut client_write, 0.50),
        client_write_p99: percentile_ns(&mut client_write, 0.99),
        client_send_await_p50: percentile_ns(&mut client_send_await, 0.50),
        client_send_await_p99: percentile_ns(&mut client_send_await, 0.99),
        client_send_await_p999: percentile_ns(&mut client_send_await, 0.999),
        client_sub_read_p50: percentile_ns(&mut client_sub_read, 0.50),
        client_sub_read_p99: percentile_ns(&mut client_sub_read, 0.99),
        client_sub_read_await_p50: percentile_ns(&mut client_sub_read_await, 0.50),
        client_sub_read_await_p99: percentile_ns(&mut client_sub_read_await, 0.99),
        client_sub_poll_gap_p50: percentile_ns(&mut client_sub_poll_gap, 0.50),
        client_sub_poll_gap_p99: percentile_ns(&mut client_sub_poll_gap, 0.99),
        client_sub_poll_gap_p999: percentile_ns(&mut client_sub_poll_gap, 0.999),
        client_sub_queue_wait_p50: percentile_ns(&mut client_sub_queue_wait, 0.50),
        client_sub_queue_wait_p99: percentile_ns(&mut client_sub_queue_wait, 0.99),
        client_sub_time_in_queue_p50: percentile_ns(&mut client_sub_time_in_queue, 0.50),
        client_sub_time_in_queue_p99: percentile_ns(&mut client_sub_time_in_queue, 0.99),
        client_sub_time_in_queue_p999: percentile_ns(&mut client_sub_time_in_queue, 0.999),
        client_sub_runtime_gap_p50: percentile_ns(&mut client_sub_runtime_gap, 0.50),
        client_sub_runtime_gap_p99: percentile_ns(&mut client_sub_runtime_gap, 0.99),
        client_sub_runtime_gap_p999: percentile_ns(&mut client_sub_runtime_gap, 0.999),
        client_sub_delivery_chan_wait_p50: percentile_ns(&mut client_sub_delivery_chan_wait, 0.50),
        client_sub_delivery_chan_wait_p99: percentile_ns(&mut client_sub_delivery_chan_wait, 0.99),
        client_sub_delivery_chan_wait_p999: percentile_ns(
            &mut client_sub_delivery_chan_wait,
            0.999,
        ),
        client_sub_decode_p50: percentile_ns(&mut client_sub_decode, 0.50),
        client_sub_decode_p99: percentile_ns(&mut client_sub_decode, 0.99),
        client_sub_dispatch_p50: percentile_ns(&mut client_sub_dispatch, 0.50),
        client_sub_dispatch_p99: percentile_ns(&mut client_sub_dispatch, 0.99),
        client_sub_consumer_gap_p50: percentile_ns(&mut client_sub_consumer_gap, 0.50),
        client_sub_consumer_gap_p99: percentile_ns(&mut client_sub_consumer_gap, 0.99),
        client_sub_consumer_gap_p999: percentile_ns(&mut client_sub_consumer_gap, 0.999),
        client_e2e_latency_p50: percentile_ns(&mut client_e2e_latency, 0.50),
        client_e2e_latency_p99: percentile_ns(&mut client_e2e_latency, 0.99),
        client_e2e_latency_p999: percentile_ns(&mut client_e2e_latency, 0.999),
        client_ack_read_p50: percentile_ns(&mut client_ack_read, 0.50),
        client_ack_read_p99: percentile_ns(&mut client_ack_read, 0.99),
        client_ack_decode_p50: percentile_ns(&mut client_ack_decode, 0.50),
        client_ack_decode_p99: percentile_ns(&mut client_ack_decode, 0.99),
        broker_decode_p50: percentile_ns(&mut broker_decode, 0.50),
        broker_decode_p99: percentile_ns(&mut broker_decode, 0.99),
        broker_fanout_p50: percentile_ns(&mut broker_fanout, 0.50),
        broker_fanout_p99: percentile_ns(&mut broker_fanout, 0.99),
        broker_ack_write_p50: percentile_ns(&mut broker_ack, 0.50),
        broker_ack_write_p99: percentile_ns(&mut broker_ack, 0.99),
        broker_quic_write_p50: percentile_ns(&mut broker_quic_write, 0.50),
        broker_quic_write_p99: percentile_ns(&mut broker_quic_write, 0.99),
        broker_sub_queue_wait_p50: percentile_ns(&mut broker_sub_queue, 0.50),
        broker_sub_queue_wait_p99: percentile_ns(&mut broker_sub_queue, 0.99),
        broker_sub_prefix_build_p50: percentile_ns(&mut broker_sub_prefix, 0.50),
        broker_sub_prefix_build_p99: percentile_ns(&mut broker_sub_prefix, 0.99),
        broker_sub_write_p50: percentile_ns(&mut broker_sub_write, 0.50),
        broker_sub_write_p99: percentile_ns(&mut broker_sub_write, 0.99),
        broker_sub_write_await_p50: percentile_ns(&mut broker_sub_write_await, 0.50),
        broker_sub_write_await_p99: percentile_ns(&mut broker_sub_write_await, 0.99),
        broker_sub_delivery_p50: percentile_ns(&mut broker_sub_delivery, 0.50),
        broker_sub_delivery_p99: percentile_ns(&mut broker_sub_delivery, 0.99),
        broker_sub_delivery_p999: percentile_ns(&mut broker_sub_delivery, 0.999),
        broker_publish_lookup_p50: percentile_ns(&mut broker_publish_lookup, 0.50),
        broker_publish_lookup_p99: percentile_ns(&mut broker_publish_lookup, 0.99),
        broker_publish_append_p50: percentile_ns(&mut broker_publish_append, 0.50),
        broker_publish_append_p99: percentile_ns(&mut broker_publish_append, 0.99),
        broker_publish_fanout_p50: percentile_ns(&mut broker_publish_fanout, 0.50),
        broker_publish_fanout_p99: percentile_ns(&mut broker_publish_fanout, 0.99),
        broker_publish_enqueue_p50: percentile_ns(&mut broker_publish_enqueue, 0.50),
        broker_publish_enqueue_p99: percentile_ns(&mut broker_publish_enqueue, 0.99),
        broker_publish_send_p50: percentile_ns(&mut broker_publish_send, 0.50),
        broker_publish_send_p99: percentile_ns(&mut broker_publish_send, 0.99),
    }
}

#[cfg(not(feature = "telemetry"))]
fn build_timing_summary(
    _client_samples: Option<client_timings::ClientTimingSamples>,
    _broker_samples: Option<broker_timings::BrokerTimingSamples>,
    _broker_publish_samples: Option<broker_publish_timings::BrokerPublishSamples>,
) -> TimingSummary {
    TimingSummary
}

fn expectation_note(config: &DemoConfig) -> String {
    if config.batch_size <= 1 && !config.binary {
        "Expectation: latency-focused run; p999 <= 1 ms on localhost if not saturated.".to_string()
    } else {
        "Expectation: throughput-focused run; higher p999 is normal due to queueing.".to_string()
    }
}

/// Message count for a batched throughput case in the `--all` sweep.
///
/// Balances two things. A run that fits inside the 64 MiB QUIC send window is
/// absorbed by buffers and reports buffer-fill rate rather than sustained
/// throughput, so every case wants volume. But fanout multiplies delivered
/// work, and this is a 20-case demo sweep, so every case also wants to stay
/// about a second.
///
/// The window floor wins whenever it is affordable, because a case that warns
/// about being undersized is a case that measured the wrong thing. Only the
/// combinations where clearing the window would cost millions of deliveries
/// (small payloads at high fanout) fall back to the time cap and warn. Use
/// `task perf:latency-matrix` for throughput numbers worth quoting -- it sizes
/// every case past the window with no wall-clock cap and medians five trials.
fn throughput_total(payload_bytes: usize, fanout: usize) -> usize {
    const TARGET_RUN_BYTES: usize = 192 * 1024 * 1024;
    // Comfortably past the 128 MiB the undersized-run warning checks for.
    const WINDOW_FLOOR_BYTES: usize = 136 * 1024 * 1024;
    const MAX_DELIVERIES: usize = 1_500_000;
    if payload_bytes == 0 {
        // No payload bytes, so the send window is irrelevant; bound by work.
        return (MAX_DELIVERIES / fanout.max(1)).clamp(5_000, 200_000);
    }
    let fanout = fanout.max(1);
    let by_volume = TARGET_RUN_BYTES.div_ceil(payload_bytes);
    let by_time = MAX_DELIVERIES / fanout;
    let window_floor = WINDOW_FLOOR_BYTES.div_ceil(payload_bytes);
    if window_floor * fanout <= MAX_DELIVERIES {
        by_volume.min(by_time).max(window_floor)
    } else {
        by_volume.min(by_time).max(5_000)
    }
}

async fn run_all(base: DemoConfig) -> Result<()> {
    let fast = std::env::var("FELIX_LATENCY_DEMO_FAST")
        .ok()
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    run_all_with_mode(base, fast).await
}

async fn run_all_with_mode(base: DemoConfig, fast: bool) -> Result<()> {
    println!("Running --all (default): latency-focused then throughput-focused.");
    let latency_payloads: &[usize] = if fast { &[0] } else { &[0, 256, 1024] };
    let latency_fanouts: &[usize] = if fast { &[1] } else { &[1, 10] };
    for &payload in latency_payloads {
        for &fanout in latency_fanouts {
            let mut case = base.clone();
            case.payload_bytes = payload;
            case.fanout = fanout;
            case.batch_size = 1;
            case.binary = base.binary;
            case.warmup = if fast { 1 } else { 200 };
            case.total = if fast { 5 } else { 2000 };
            let (result, summary) = run_case(case.clone()).await?;
            print_result(&result);
            print_timing_summary(summary);
            let (client_counters, broker_counters) = print_frame_counters();
            print_sanity_checks(&case, &result, &client_counters, &broker_counters);
        }
    }

    let throughput_payloads: &[usize] = if fast { &[0] } else { &[0, 1024, 4096] };
    let throughput_fanouts: &[usize] = if fast { &[1] } else { &[1, 10] };
    let binaries: &[bool] = if base.binary { &[true] } else { &[false, true] };
    for &payload in throughput_payloads {
        for &fanout in throughput_fanouts {
            for &binary in binaries {
                let mut case = base.clone();
                case.payload_bytes = payload;
                case.fanout = fanout;
                case.batch_size = 64;
                case.binary = binary;
                case.warmup = if fast { 1 } else { 200 };
                case.total = if fast {
                    5
                } else {
                    throughput_total(payload, fanout)
                };
                let (result, summary) = run_case(case.clone()).await?;
                print_result(&result);
                print_timing_summary(summary);
                let (client_counters, broker_counters) = print_frame_counters();
                print_sanity_checks(&case, &result, &client_counters, &broker_counters);
            }
        }
    }
    let mut baseline = base.clone();
    baseline.payload_bytes = 256;
    baseline.fanout = 1;
    baseline.batch_size = 64;
    baseline.binary = base.binary;
    baseline.warmup = if fast { 1 } else { 200 };
    baseline.total = if fast {
        5
    } else {
        throughput_total(baseline.payload_bytes, baseline.fanout)
    };
    baseline.pub_conns = 1;
    baseline.pub_streams_per_conn = 1;
    let (result, summary) = run_case(baseline.clone()).await?;
    print_result(&result);
    print_timing_summary(summary);

    let mut scaled = baseline;
    scaled.pub_conns = 4;
    scaled.pub_streams_per_conn = 2;
    let (result, summary) = run_case(scaled).await?;
    print_result(&result);
    print_timing_summary(summary);
    Ok(())
}

fn parse_csv_usize(input: &str) -> Vec<usize> {
    input
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .collect()
}

fn parse_sharding_mode(value: &str) -> Option<PublishSharding> {
    match value {
        "rr" => Some(PublishSharding::RoundRobin),
        "hash_stream" => Some(PublishSharding::HashStream),
        _ => None,
    }
}

fn encode_payload(payload_bytes: usize) -> Vec<u8> {
    let timestamp = current_time_nanos();
    let mut buf = Vec::with_capacity(16 + payload_bytes);
    buf.extend_from_slice(&timestamp.to_be_bytes());
    if payload_bytes > 0 {
        buf.extend(std::iter::repeat_n(0u8, payload_bytes));
    }
    buf
}

fn decode_latency(bytes: &[u8]) -> Duration {
    let mut raw = [0u8; 16];
    raw.copy_from_slice(&bytes[..16]);
    let sent = u128::from_be_bytes(raw);
    let now = current_time_nanos();
    let delta = now.saturating_sub(sent);
    Duration::from_nanos(delta as u64)
}

fn current_time_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos()
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    // Lossless measurement: block the subscription reader instead of shedding
    // client-side so reported drops reflect broker-side saturation only.
    config.client_sub_queue_policy = felix_client::ClientSubQueuePolicy::Block;
    config.client_sub_queue_capacity = config.client_sub_queue_capacity.max(4096);
    Ok(config)
}

fn resolve_demo_auth(config: &felix_broker_service::config::BrokerConfig) -> DemoAuthResult {
    if let Some(controlplane_url) = config.controlplane_url.clone() {
        return Ok((
            Arc::new(felix_broker_service::serving::auth::BrokerAuth::new(
                controlplane_url,
            )),
            None,
        ));
    }

    let demo = felix_broker_service::serving::auth::demo::demo_auth_for_tenant("t1")?;
    Ok((demo.auth, Some((demo.tenant_id, demo.token))))
}

fn apply_demo_auth(
    mut config: ClientConfig,
    auth_override: Option<(String, String)>,
) -> ClientConfig {
    if let Some((tenant_id, token)) = auth_override {
        config.auth_tenant_id = Some(tenant_id);
        config.auth_token = Some(token);
    }
    config
}

fn percentile(values: &[Duration], p: f64) -> Duration {
    let len = values.len();
    if len == 0 {
        return Duration::from_nanos(0);
    }
    let idx = ((len - 1) as f64 * p).round() as usize;
    values[idx]
}

#[cfg(feature = "telemetry")]
fn percentile_ns(values: &mut [u64], p: f64) -> Option<Duration> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    Some(Duration::from_nanos(values[idx]))
}

#[cfg(feature = "telemetry")]
fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(format_duration)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_duration(duration: Duration) -> String {
    let micros = duration.as_micros();
    if micros >= 1000 {
        format!("{:.3} ms", micros as f64 / 1000.0)
    } else {
        format!("{micros} us")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use serial_test::serial;
    use std::time::Duration;

    fn base_config() -> DemoConfig {
        DemoConfig {
            warmup: 1,
            total: 5,
            payload_bytes: 0,
            fanout: 1,
            batch_size: 1,
            binary: false,
            // Generous on purpose: this bounds how long a subscriber waits
            // before declaring a stall, and it has to cover a *cold* first
            // event -- connection setup, subscribe registration and the first
            // publish -- not just a steady-state delayed-ACK cycle (25 ms).
            // Under a coverage-instrumented build the cold path alone exceeds
            // 100 ms, which failed these runs spuriously. Matches the CLI
            // default; a genuine stall still fails the run, just not a slow
            // start.
            idle_ms: 2000,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
            log_capacity: None,
        }
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_end_to_end() -> Result<()> {
        let config = DemoConfig {
            warmup: 1,
            total: 5,
            payload_bytes: 8,
            fanout: 1,
            batch_size: 1,
            binary: false,
            // Generous on purpose: this bounds how long a subscriber waits
            // before declaring a stall, and it has to cover a *cold* first
            // event -- connection setup, subscribe registration and the first
            // publish -- not just a steady-state delayed-ACK cycle (25 ms).
            // Under a coverage-instrumented build the cold path alone exceeds
            // 100 ms, which failed these runs spuriously. Matches the CLI
            // default; a genuine stall still fails the run, just not a slow
            // start.
            idle_ms: 2000,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
            log_capacity: None,
        };
        tokio::time::timeout(
            Duration::from_secs(15),
            run_demo(config, false, vec![8], vec![1], false),
        )
        .await
        .context("latency demo timeout")?
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_matrix_small() -> Result<()> {
        let config = DemoConfig {
            warmup: 0,
            total: 2,
            payload_bytes: 0,
            fanout: 1,
            batch_size: 1,
            binary: false,
            // Generous on purpose: this bounds how long a subscriber waits
            // before declaring a stall, and it has to cover a *cold* first
            // event -- connection setup, subscribe registration and the first
            // publish -- not just a steady-state delayed-ACK cycle (25 ms).
            // Under a coverage-instrumented build the cold path alone exceeds
            // 100 ms, which failed these runs spuriously. Matches the CLI
            // default; a genuine stall still fails the run, just not a slow
            // start.
            idle_ms: 2000,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: Some("rr".to_string()),
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: false,
            log_capacity: None,
        };
        tokio::time::timeout(
            Duration::from_secs(15),
            run_demo(config, true, vec![0], vec![1], false),
        )
        .await
        .context("latency demo matrix timeout")?
    }

    #[test]
    fn latency_demo_parse_args_and_helpers() {
        let args = vec![
            "--matrix".to_string(),
            "--warmup".to_string(),
            "2".to_string(),
            "--total".to_string(),
            "4".to_string(),
            "--payload".to_string(),
            "8".to_string(),
            "--fanout".to_string(),
            "2".to_string(),
            "--batch".to_string(),
            "2".to_string(),
            "--idle-ms".to_string(),
            "5".to_string(),
            "--pub-conns".to_string(),
            "1".to_string(),
            "--pub-streams-per-conn".to_string(),
            "1".to_string(),
            "--pub-sharding".to_string(),
            "hash".to_string(),
            "--pub-stream-count".to_string(),
            "2".to_string(),
            "--sub-writer-lanes".to_string(),
            "8".to_string(),
            "--sub-lane-shard".to_string(),
            "connection_id_hash".to_string(),
            "--pub-yield-every-batches".to_string(),
            "3".to_string(),
            "--sub-dedicated-thread".to_string(),
            "--payloads".to_string(),
            "1,2".to_string(),
            "--fanouts".to_string(),
            "1,3".to_string(),
        ];
        let (config, matrix, payloads, fanouts, all, smoke, compare) =
            parse_args_from(args.into_iter());
        assert!(matrix);
        assert_eq!(payloads, vec![1, 2]);
        assert_eq!(fanouts, vec![1, 3]);
        assert!(!all);
        assert!(!smoke);
        assert!(!compare);
        assert_eq!(config.sub_writer_lanes, Some(8));
        assert_eq!(config.sub_lane_shard.as_deref(), Some("connection_id_hash"));
        assert_eq!(config.pub_yield_every_batches, 3);
        assert!(config.sub_dedicated_thread);

        assert_eq!(parse_csv_usize("1,2,3"), vec![1, 2, 3]);
        let payload = encode_payload(4);
        let encoded = current_time_nanos();
        assert!(encoded > 0);
        let _decoded = decode_latency(&payload);
        assert_eq!(format_duration(Duration::from_micros(5)), "5 us");
        let _ = percentile(&[Duration::from_micros(1)], 0.5);
        assert_eq!(stream_name(0, 1), "latency");
        assert_eq!(stream_name(3, 2), "latency-1");
        assert_eq!(per_stream_events(5, 2, 0), 3);
        assert_eq!(per_stream_events(5, 2, 1), 2);
        assert!(
            expectation_note(&DemoConfig {
                warmup: 1,
                total: 1,
                payload_bytes: 0,
                fanout: 1,
                batch_size: 1,
                binary: false,
                idle_ms: 1,
                pub_conns: 1,
                pub_streams_per_conn: 1,
                pub_sharding: None,
                pub_stream_count: 1,
                sub_conns: 1,
                sub_writer_lanes: None,
                sub_lane_shard: None,
                sub_delivery_shaping: true,
                pub_yield_every_batches: 0,
                sub_dedicated_thread: false,
                log_capacity: None,
            })
            .contains("latency-focused")
        );
        assert!(
            expectation_note(&DemoConfig {
                warmup: 1,
                total: 1,
                payload_bytes: 0,
                fanout: 1,
                batch_size: 2,
                binary: false,
                idle_ms: 1,
                pub_conns: 1,
                pub_streams_per_conn: 1,
                pub_sharding: None,
                pub_stream_count: 1,
                sub_conns: 1,
                sub_writer_lanes: None,
                sub_lane_shard: None,
                sub_delivery_shaping: true,
                pub_yield_every_batches: 0,
                sub_dedicated_thread: false,
                log_capacity: None,
            })
            .contains("throughput-focused")
        );
        assert_eq!(parse_sharding_mode("rr"), Some(PublishSharding::RoundRobin));
        assert_eq!(
            parse_sharding_mode("hash_stream"),
            Some(PublishSharding::HashStream)
        );
        assert_eq!(parse_sharding_mode("nope"), None);
        #[cfg(feature = "telemetry")]
        {
            let mut values = vec![100u64, 200, 300];
            let _ = percentile_ns(&mut values, 0.5);
        }
        #[cfg(feature = "telemetry")]
        {
            let _ = format_optional_duration(None);
        }
    }

    /// A drain task that receives `steps` events `every` apart, then either
    /// finishes or, if `stall` is set, hangs.
    fn paced_drain(label: &str, steps: usize, every: Duration, stall: bool) -> DrainTask {
        let received = Arc::new(AtomicUsize::new(0));
        let task_received = received.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..steps {
                tokio::time::sleep(every).await;
                task_received.fetch_add(1, Ordering::Relaxed);
            }
            if stall {
                std::future::pending::<()>().await;
            }
            Ok(())
        });
        DrainTask {
            label: label.to_string(),
            expected: 10,
            received,
            handle,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn drain_join_waits_out_a_subscriber_that_is_slow_but_live() -> Result<()> {
        // Each event lands well inside the idle window, but the subscriber as a
        // whole takes 2.5x the window to finish.
        let tasks = vec![
            paced_drain("fast", 10, Duration::from_millis(1), false),
            paced_drain("slow", 10, Duration::from_millis(50), false),
        ];
        join_drain_tasks(tasks, Duration::from_millis(200)).await
    }

    #[tokio::test(start_paused = true)]
    async fn drain_join_reports_every_stalled_subscriber() {
        let tasks = vec![
            paced_drain("done", 10, Duration::from_millis(1), false),
            paced_drain("stuck-a", 3, Duration::from_millis(10), true),
            paced_drain("stuck-b", 7, Duration::from_millis(10), true),
        ];
        let err = join_drain_tasks(tasks, Duration::from_millis(200))
            .await
            .expect_err("stalled subscribers must fail the run");
        let message = format!("{err:#}");
        assert!(message.contains("stalled"), "{message}");
        assert!(message.contains("stuck-a 3/10"), "{message}");
        assert!(message.contains("stuck-b 7/10"), "{message}");
        assert!(!message.contains("done"), "{message}");
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_run_all_fast() -> Result<()> {
        let base = base_config();
        tokio::time::timeout(Duration::from_secs(20), run_all_with_mode(base, true))
            .await
            .context("latency demo fast run_all timeout")?
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_smoke_flags() -> Result<()> {
        let config = DemoConfig {
            warmup: 1,
            total: 4,
            payload_bytes: 8,
            fanout: 1,
            batch_size: 2,
            binary: false,
            // Generous on purpose: this bounds how long a subscriber waits
            // before declaring a stall, and it has to cover a *cold* first
            // event -- connection setup, subscribe registration and the first
            // publish -- not just a steady-state delayed-ACK cycle (25 ms).
            // Under a coverage-instrumented build the cold path alone exceeds
            // 100 ms, which failed these runs spuriously. Matches the CLI
            // default; a genuine stall still fails the run, just not a slow
            // start.
            idle_ms: 2000,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: None,
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: Some(2),
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 1,
            sub_dedicated_thread: true,
            log_capacity: None,
        };
        tokio::time::timeout(
            Duration::from_secs(20),
            run_demo(config, false, vec![8], vec![1], false),
        )
        .await
        .context("latency demo smoke flags timeout")?
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_dedicated_binary_warmup_exceeds_default_queue() -> Result<()> {
        let config = DemoConfig {
            warmup: 500,
            total: 5000,
            payload_bytes: 256,
            fanout: 1,
            batch_size: 1,
            binary: true,
            idle_ms: 10_000,
            pub_conns: 1,
            pub_streams_per_conn: 1,
            pub_sharding: Some("hash_stream".to_string()),
            pub_stream_count: 1,
            sub_conns: 1,
            sub_writer_lanes: None,
            sub_lane_shard: None,
            sub_delivery_shaping: true,
            pub_yield_every_batches: 0,
            sub_dedicated_thread: true,
            log_capacity: None,
        };
        tokio::time::timeout(
            Duration::from_secs(15),
            run_demo(config, false, vec![256], vec![1], false),
        )
        .await
        .context("dedicated binary warmup timeout")?
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_binary_fanout_is_lossless() -> Result<()> {
        let mut config = base_config();
        config.warmup = 200;
        config.total = 1000;
        config.payload_bytes = 256;
        config.fanout = 10;
        config.binary = true;
        config.idle_ms = 10_000;
        config.pub_sharding = Some("hash_stream".to_string());
        config.sub_dedicated_thread = true;

        let (result, _) = tokio::time::timeout(Duration::from_secs(15), run_case(config))
            .await
            .context("binary fanout timeout")??;
        assert_eq!(result.received, 1000);
        assert_eq!(result.delivered_total, 10_000);
        assert_eq!(result.unaccounted, 0);
        Ok(())
    }

    #[serial]
    #[test]
    fn latency_demo_parse_env_and_error_inputs() {
        unsafe {
            std::env::set_var("FELIX_SUB_CONNS", "not-a-number");
            std::env::set_var("FELIX_SUB_EGRESS_CONNS", "7");
            std::env::set_var("FELIX_SUB_DEDICATED_THREAD", "false");
            std::env::set_var("FELIX_SUB_DELIVERY_SHAPING", "0");
        }

        let args = vec![
            "--warmup".to_string(),
            "oops".to_string(),
            "--total".to_string(),
            "bad".to_string(),
            "--payload".to_string(),
            "16".to_string(),
            "--fanout".to_string(),
            "5".to_string(),
            "--batch".to_string(),
            "0".to_string(),
            "--smoke".to_string(),
            "--compare-sub-delivery-shaping".to_string(),
            "--sub-shared-thread".to_string(),
            "--sub-writer-lanes".to_string(),
            "nope".to_string(),
            "--payloads".to_string(),
            "1,x,2".to_string(),
            "--fanouts".to_string(),
            "3, bad,4".to_string(),
        ];
        let (config, matrix, payloads, fanouts, all, smoke, compare) =
            parse_args_from(args.into_iter());
        assert!(!matrix);
        assert!(!all);
        assert!(smoke);
        assert!(compare);
        assert_eq!(payloads, vec![1, 2]);
        assert_eq!(fanouts, vec![3, 4]);
        assert_eq!(config.sub_conns, 7);
        assert_eq!(config.sub_writer_lanes, None);
        assert!(!config.sub_dedicated_thread);
        assert!(!config.sub_delivery_shaping);
        assert_eq!(config.warmup, 1000);
        assert_eq!(config.total, 10000);
        assert_eq!(config.batch_size, 0);

        unsafe {
            std::env::remove_var("FELIX_SUB_CONNS");
            std::env::remove_var("FELIX_SUB_EGRESS_CONNS");
            std::env::remove_var("FELIX_SUB_DEDICATED_THREAD");
            std::env::remove_var("FELIX_SUB_DELIVERY_SHAPING");
        }
    }

    #[serial]
    #[test]
    fn latency_demo_effective_lanes_hint_respects_env_and_floor() {
        let mut config = base_config();
        config.sub_writer_lanes = Some(0);
        assert_eq!(effective_sub_lanes_hint(&config), 1);
        config.sub_writer_lanes = Some(9);
        assert_eq!(effective_sub_lanes_hint(&config), 9);

        config.sub_writer_lanes = None;
        unsafe {
            std::env::set_var("FELIX_SUB_EGRESS_LANES", "0");
        }
        assert_eq!(effective_sub_lanes_hint(&config), 4);
        unsafe {
            std::env::set_var("FELIX_SUB_EGRESS_LANES", "6");
        }
        assert_eq!(effective_sub_lanes_hint(&config), 6);
        unsafe {
            std::env::remove_var("FELIX_SUB_EGRESS_LANES");
        }
    }

    #[tokio::test]
    #[serial]
    async fn latency_demo_compare_sub_delivery_shaping_path() -> Result<()> {
        let mut config = base_config();
        config.total = 3;
        config.warmup = 0;
        tokio::time::timeout(
            Duration::from_secs(25),
            run_compare_sub_delivery_shaping(config),
        )
        .await
        .context("latency demo compare shaping timeout")?
    }

    #[test]
    fn latency_demo_print_and_sanity_helpers_cover_branches() {
        let empty = DemoResult {
            publish_total: 10,
            sample_total: 5,
            payload_bytes: 16,
            fanout: 2,
            batch_size: 1,
            binary: false,
            received: 0,
            unaccounted: 1,
            delivered_total: 3,
            p50: Duration::from_micros(1),
            p99: Duration::from_micros(2),
            p999: Duration::from_micros(3),
            publish_duration: Duration::from_millis(10),
            drain_duration: Duration::from_millis(5),
            publish_submit_throughput: 2.0,
            throughput: 1.0,
            effective_throughput: 0.5,
            delivered_throughput: 0.6,
            delivered_per_sub_throughput: 0.3,
        };
        print_result(&empty);

        let mut received = empty;
        received.received = 4;
        received.delivered_total = 9;
        print_result(&received);

        #[cfg(feature = "telemetry")]
        {
            let summary = build_timing_summary(None, None, None);
            print_timing_summary(Some(summary));
            print_timing_summary(None);

            let client = felix_client::FrameCountersSnapshot {
                frames_in_ok: 0,
                frames_in_err: 0,
                frames_out_ok: 10,
                bytes_in: 0,
                bytes_out: 1000,
                pub_frames_out_ok: 10,
                pub_frames_out_err: 1,
                sub_frames_in_ok: 0,
                ack_frames_in_ok: 0,
                pub_items_out_ok: 99,
                pub_items_out_err: 1,
                pub_batches_out_ok: 1,
                pub_batches_out_err: 1,
                sub_items_in_ok: 0,
                sub_batches_in_ok: 0,
                ack_items_in_ok: 0,
                binary_encode_reallocs: 0,
                text_encode_reallocs: 0,
            };
            let broker = felix_broker_service::serving::quic::FrameCountersSnapshot {
                frames_in_ok: 0,
                frames_in_err: 0,
                frames_out_ok: 0,
                bytes_in: 0,
                bytes_out: 0,
                pub_frames_in_ok: 1,
                pub_frames_in_err: 0,
                pub_items_in_ok: 1,
                pub_items_in_err: 0,
                pub_batches_in_ok: 1,
                pub_batches_in_err: 0,
                ack_frames_out_ok: 0,
                ack_items_out_ok: 0,
                sub_frames_out_ok: 0,
                sub_items_out_ok: 0,
                sub_batches_out_ok: 0,
            };
            print_sanity_checks(&base_config(), &received, &client, &broker);
            let mut ack_config = base_config();
            ack_config.batch_size = 64;
            print_sanity_checks(&ack_config, &received, &client, &broker);

            let _ = print_frame_counters();
        }
    }
}
