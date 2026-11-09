//! Replicated publish latency, steady and through a promotion burst.
//!
//! `#[ignore]`: a measurement, not an assertion. Run explicitly:
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker
//! cargo test --release -p felix-cluster --test replicated_bench -- --ignored --nocapture
//! ```
//!
//! Steady: closed-loop publishers on one RF3 quorum stream, each publish
//! acknowledged by a majority. Burst: the same publishing while RF3 quorum
//! streams are created in bulk, each promoted (fenced) on its leader, and a
//! quorum cache is read with confirmed reads (a fence round per read). Each
//! phase prints one `BENCH_JSON` line.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_client::Client;
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serde_json::{Value, json};
use serial_test::serial;
use tokio::task::JoinSet;

const STREAM: &str = "steady";
const CACHE: &str = "confirmed";
const PAYLOAD: usize = 1024;
const CREATES_IN_FLIGHT: usize = 16;
const FEATURES: [&str; 4] = [
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    "fenced_caches",
];

const PHASE_WARMUP: u8 = 0;
const PHASE_STEADY: u8 = 1;
const PHASE_BURST: u8 = 2;
const PHASE_DONE: u8 = 3;
/// After the burst window: still publishing, not counted.
const PHASE_TAIL: u8 = 4;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Nearest rank, microseconds in, milliseconds out.
fn pct_ms(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((sorted.len() as f64) * p / 100.0).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1] as f64 / 1000.0
}

fn round3(v: f64) -> Value {
    if v.is_finite() {
        json!((v * 1000.0).round() / 1000.0)
    } else {
        Value::Null
    }
}

/// `(phase, latency in microseconds, ok)`.
type Sample = (u8, u64, bool);

fn phase_latencies(samples: &[Sample], phase: u8) -> (Vec<u64>, usize) {
    let mut ok: Vec<u64> = samples
        .iter()
        .filter(|s| s.0 == phase && s.2)
        .map(|s| s.1)
        .collect();
    ok.sort_unstable();
    let failed = samples.iter().filter(|s| s.0 == phase && !s.2).count();
    (ok, failed)
}

async fn publisher(
    client: Client,
    tenant: String,
    namespace: String,
    index: usize,
    phase: Arc<AtomicU8>,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    let payload = vec![7u8; env_usize("BENCH_PAYLOAD", PAYLOAD)];
    let mut seq = index as u64;
    loop {
        let now = phase.load(Ordering::Acquire);
        if now == PHASE_DONE {
            break;
        }
        // Keys spread the publishes over every shard, and so every leader.
        let key = Bytes::from(format!("k{}", seq % 64));
        seq += 17;
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            client.publish_keyed(
                &tenant,
                &namespace,
                STREAM,
                payload.clone(),
                key,
                AckMode::PerMessage,
            ),
        )
        .await;
        let ok = matches!(outcome, Ok(Ok(_)));
        samples.push((now, started.elapsed().as_micros() as u64, ok));
        if !ok {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    samples
}

/// Confirmed reads of one key, back to back with a short pause.
async fn reader(
    client: Client,
    tenant: String,
    namespace: String,
    phase: Arc<AtomicU8>,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    loop {
        let now = phase.load(Ordering::Acquire);
        if now == PHASE_DONE {
            break;
        }
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            client.cache_get_versioned(&tenant, &namespace, CACHE, "key"),
        )
        .await;
        let ok = matches!(outcome, Ok(Ok(Some(_))));
        samples.push((now, started.elapsed().as_micros() as u64, ok));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    samples
}

/// Every broker's `/metrics` text, by node id.
async fn scrape(cluster: &Cluster) -> Vec<(String, String)> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("http");
    let mut out = Vec::new();
    for node in &cluster.nodes {
        let body = match http
            .get(format!("http://{}/metrics", node.metrics_addr))
            .send()
            .await
        {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        out.push((node.node_id.clone(), body));
    }
    out
}

/// One summary metric as the Prometheus exporter writes it: quantiles over its
/// rolling window, and the running sum and count. Only label sets containing
/// `filter` count.
#[derive(Default, Clone)]
struct Summary {
    quantiles: Vec<(String, f64)>,
    sum: f64,
    count: f64,
}

fn parse_summary(body: &str, name: &str, filter: &str) -> Summary {
    let mut summary = Summary::default();
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let (head, value) = match line.rsplit_once(' ') {
            Some((h, v)) => (h, v.parse::<f64>().unwrap_or(f64::NAN)),
            None => continue,
        };
        let (base, labels) = match head.find('{') {
            Some(i) => (&head[..i], &head[i..]),
            None => (head, ""),
        };
        if !filter.is_empty() && !labels.contains(filter) {
            continue;
        }
        if base == name {
            if let Some(q) = labels
                .split("quantile=\"")
                .nth(1)
                .and_then(|r| r.split('"').next())
            {
                summary.quantiles.push((q.to_string(), value));
            }
        } else if base == format!("{name}_sum") {
            summary.sum += value;
        } else if base == format!("{name}_count") {
            summary.count += value;
        }
    }
    summary
}

const METRICS: [(&str, &str, &str); 4] = [
    (
        "replica_state_write",
        "felix_storage_replica_state_write_duration_seconds",
        "",
    ),
    ("storage_sync", "felix_storage_sync_duration_seconds", ""),
    (
        "peer_request_ok",
        "felix_broker_peer_request_seconds",
        "outcome=\"ok\"",
    ),
    ("append", "felix_storage_append_duration_seconds", ""),
];

/// Per broker and metric: what the phase added (count, mean) and the
/// exporter's quantiles at its end (a rolling window, roughly the last minute).
fn metric_report(before: &[(String, String)], after: &[(String, String)]) -> Value {
    let mut report = serde_json::Map::new();
    for (label, name, filter) in METRICS {
        let mut per_node = serde_json::Map::new();
        for ((node, b), (_, a)) in before.iter().zip(after) {
            let s0 = parse_summary(b, name, filter);
            let s1 = parse_summary(a, name, filter);
            let count = s1.count - s0.count;
            let mean_ms = if count > 0.0 {
                (s1.sum - s0.sum) / count * 1000.0
            } else {
                f64::NAN
            };
            let mut q = serde_json::Map::new();
            for (quantile, value) in &s1.quantiles {
                q.insert(format!("q{quantile}_ms"), round3(value * 1000.0));
            }
            per_node.insert(
                node.clone(),
                json!({ "count": count, "mean_ms": round3(mean_ms), "window": q }),
            );
        }
        report.insert(label.to_string(), Value::Object(per_node));
    }
    Value::Object(report)
}

async fn create_stream(http: &reqwest::Client, url: &str, token: &str, name: &str) -> bool {
    let body = json!({
        "stream": name,
        "kind": "Stream",
        "shards": 1,
        "replication_factor": 3,
        "retention": { "max_age_seconds": null, "max_size_bytes": null },
        "consistency": "Quorum",
        "delivery": "AtLeastOnce",
        "durable": true,
    });
    let mut waits_ms = [100, 250, 500, 1_000, 2_000, 4_000, 8_000].into_iter();
    loop {
        let Ok(response) = http.post(url).bearer_auth(token).json(&body).send().await else {
            return false;
        };
        let status = response.status();
        if status.is_success() || status == reqwest::StatusCode::CONFLICT {
            return true;
        }
        let text = response.text().await.unwrap_or_default();
        let busy = status == reqwest::StatusCode::SERVICE_UNAVAILABLE && text.contains("busy");
        if !busy {
            println!("create {name}: {status}: {text}");
            return false;
        }
        let Some(wait_ms) = waits_ms.next() else {
            return false;
        };
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
}

/// Publish to a new stream until a publish is acknowledged. Microseconds from
/// `created_at` (when its create was sent), or `None` past the deadline.
async fn first_ack(
    client: Arc<Client>,
    tenant: String,
    namespace: String,
    stream: String,
    created_at: Instant,
) -> Option<u64> {
    let deadline = created_at + Duration::from_secs(env_usize("BENCH_FIRST_ACK_SECS", 60) as u64);
    let mut last = String::new();
    while Instant::now() < deadline {
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            client.publish(
                &tenant,
                &namespace,
                &stream,
                b"first".to_vec(),
                AckMode::PerMessage,
            ),
        )
        .await;
        match outcome {
            Ok(Ok(_)) => return Some(created_at.elapsed().as_micros() as u64),
            Ok(Err(err)) => last = format!("{err:?}"),
            Err(_) => last = "timed out after 10 s".to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if FIRST_ERRORS.fetch_add(1, Ordering::Relaxed) < 5 {
        println!("first ack {stream}: gave up; last: {last}");
    }
    None
}

static FIRST_ERRORS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[serial]
#[ignore = "measurement, not an assertion; see the module docs"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_publish_steady_and_through_promotion_burst() {
    let publishers = env_usize("BENCH_PUBLISHERS", 16);
    let shards = env_usize("BENCH_SHARDS", 6) as u32;
    let streams = env_usize("BENCH_STREAMS", 1500);
    let warmup = Duration::from_secs(env_usize("BENCH_WARMUP_SECS", 5) as u64);
    let steady = Duration::from_secs(env_usize("BENCH_STEADY_SECS", 20) as u64);
    let side = std::env::var("BENCH_SIDE").unwrap_or_else(|_| "?".into());
    let round = std::env::var("BENCH_ROUND").unwrap_or_else(|_| "?".into());

    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        caches: vec![CacheSpec::quorum(CACHE, 1, 3)],
        streams: vec![StreamSpec::quorum(STREAM, shards, 3)],
        sync_interval_ms: 1_000,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &FEATURES,
            felix_cluster::wait::budget(Duration::from_secs(30)),
        )
        .await
        .expect("fleet features");

    let addrs = cluster.broker_addrs();
    let token = cluster.client_token();
    let tenant = cluster.tenant_id.clone();
    let namespace = cluster.namespace.clone();
    let connect = || felix_cluster::client::connect_cluster(&addrs, &tenant, &token);

    let reader_client = connect().await.expect("reader client");
    let seeded = Instant::now();
    loop {
        if reader_client
            .cache_put(
                &tenant,
                &namespace,
                CACHE,
                "key",
                Bytes::from_static(b"v"),
                None,
            )
            .await
            .is_ok()
        {
            break;
        }
        assert!(seeded.elapsed() < Duration::from_secs(60), "seed the cache");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let phase = Arc::new(AtomicU8::new(PHASE_WARMUP));
    let mut pubs = JoinSet::new();
    for index in 0..publishers {
        let client = connect().await.expect("publisher client");
        pubs.spawn(publisher(
            client,
            tenant.clone(),
            namespace.clone(),
            index,
            Arc::clone(&phase),
        ));
    }
    let read_task = tokio::spawn(reader(
        reader_client,
        tenant.clone(),
        namespace.clone(),
        Arc::clone(&phase),
    ));

    tokio::time::sleep(warmup).await;
    let m0 = scrape(&cluster).await;
    phase.store(PHASE_STEADY, Ordering::Release);
    let steady_started = Instant::now();
    tokio::time::sleep(steady).await;
    let steady_elapsed = steady_started.elapsed();
    phase.store(PHASE_BURST, Ordering::Release);
    let burst_started = Instant::now();
    // The promotions follow the creates within seconds; a longer window would
    // dilute their effect on the tail with quiet traffic.
    let window = Duration::from_secs(env_usize("BENCH_BURST_WINDOW_SECS", 10) as u64);
    {
        let phase = Arc::clone(&phase);
        tokio::spawn(async move {
            tokio::time::sleep(window).await;
            let _ = phase.compare_exchange(
                PHASE_BURST,
                PHASE_TAIL,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
    }
    let m1 = scrape(&cluster).await;

    // Burst: bulk creates, each followed by publishing until one is acked.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let url = format!(
        "{}/v1/tenants/{}/namespaces/{}/streams",
        cluster.control_plane_url(),
        tenant,
        namespace
    );
    let admin = cluster.admin_token();
    let first_client = Arc::new(connect().await.expect("first-ack client"));
    // The harness's control plane places shards only when asked; without
    // this no new stream is ever assigned, promoted or publishable.
    let placed = std::sync::atomic::AtomicUsize::new(0);
    let placing = async {
        loop {
            let outcome = cluster.place_shards().await;
            placed.fetch_add(outcome.placed, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let collecting = async {
        let mut creates = JoinSet::new();
        let mut firsts = JoinSet::new();
        let mut create_us = Vec::new();
        let mut create_failed = 0usize;
        let mut next = 0;
        loop {
            while next < streams && creates.len() < CREATES_IN_FLIGHT {
                let (http, url, admin) = (http.clone(), url.clone(), admin.clone());
                let name = format!("bulk-{next:05}");
                creates.spawn(async move {
                    let started = Instant::now();
                    let ok = create_stream(&http, &url, &admin, &name).await;
                    (name, started, ok)
                });
                next += 1;
            }
            match creates.join_next().await {
                Some(done) => {
                    let (name, started, ok) = done.expect("create task");
                    create_us.push(started.elapsed().as_micros() as u64);
                    // A sample, so the probes do not become the load.
                    let sampled = name.ends_with('0') || name.ends_with('5');
                    if ok && sampled {
                        firsts.spawn(first_ack(
                            Arc::clone(&first_client),
                            tenant.clone(),
                            namespace.clone(),
                            name,
                            started,
                        ));
                    } else if !ok {
                        create_failed += 1;
                    }
                }
                None => break,
            }
        }
        let created_in = burst_started.elapsed();
        let mut first_us = Vec::new();
        let mut first_failed = 0usize;
        while let Some(done) = firsts.join_next().await {
            match done.expect("first-ack task") {
                Some(us) => first_us.push(us),
                None => first_failed += 1,
            }
        }
        (create_us, create_failed, created_in, first_us, first_failed)
    };
    let (mut create_us, create_failed, created_in, mut first_us, first_failed) = tokio::select! {
        collected = collecting => collected,
        () = placing => unreachable!("placement never stops"),
    };
    let placed = placed.load(Ordering::Relaxed);
    let burst_elapsed = burst_started.elapsed();
    let m2 = scrape(&cluster).await;
    phase.store(PHASE_DONE, Ordering::Release);

    let mut samples = Vec::new();
    while let Some(done) = pubs.join_next().await {
        samples.extend(done.expect("publisher"));
    }
    let reads = read_task.await.expect("reader");

    let (steady_lat, steady_failed) = phase_latencies(&samples, PHASE_STEADY);
    let (burst_lat, burst_failed) = phase_latencies(&samples, PHASE_BURST);
    let (steady_reads, steady_reads_failed) = phase_latencies(&reads, PHASE_STEADY);
    let (burst_reads, burst_reads_failed) = phase_latencies(&reads, PHASE_BURST);
    first_us.sort_unstable();
    create_us.sort_unstable();

    let steady_line = json!({
        "bench": "replicated", "phase": "steady", "side": side, "round": round,
        "publishers": publishers, "shards": shards,
        "msgs_per_s": round3(steady_lat.len() as f64 / steady_elapsed.as_secs_f64()),
        "p50_ms": round3(pct_ms(&steady_lat, 50.0)),
        "p99_ms": round3(pct_ms(&steady_lat, 99.0)),
        "p999_ms": round3(pct_ms(&steady_lat, 99.9)),
        "failed": steady_failed,
        "read_p50_ms": round3(pct_ms(&steady_reads, 50.0)),
        "read_p99_ms": round3(pct_ms(&steady_reads, 99.0)),
        "reads": steady_reads.len(), "reads_failed": steady_reads_failed,
        "metrics": metric_report(&m0, &m1),
    });
    println!("BENCH_JSON {steady_line}");
    let burst_line = json!({
        "bench": "replicated", "phase": "burst", "side": side, "round": round,
        "streams": streams, "created_in_s": round3(created_in.as_secs_f64()),
        "burst_s": round3(burst_elapsed.as_secs_f64()),
        "msgs_per_s": round3(burst_lat.len() as f64 / burst_elapsed.min(window).as_secs_f64()),
        "p50_ms": round3(pct_ms(&burst_lat, 50.0)),
        "p99_ms": round3(pct_ms(&burst_lat, 99.0)),
        "p999_ms": round3(pct_ms(&burst_lat, 99.9)),
        "failed": burst_failed,
        "create_p50_ms": round3(pct_ms(&create_us, 50.0)),
        "create_p99_ms": round3(pct_ms(&create_us, 99.0)),
        "create_failed": create_failed,
        "first_ack_p50_ms": round3(pct_ms(&first_us, 50.0)),
        "first_ack_p99_ms": round3(pct_ms(&first_us, 99.0)),
        "first_ack_max_ms": round3(first_us.last().map_or(f64::NAN, |v| *v as f64 / 1000.0)),
        "first_ack_failed": first_failed, "first_acked": first_us.len(), "placed": placed,
        "read_p50_ms": round3(pct_ms(&burst_reads, 50.0)),
        "read_p99_ms": round3(pct_ms(&burst_reads, 99.0)),
        "reads": burst_reads.len(), "reads_failed": burst_reads_failed,
        "metrics": metric_report(&m1, &m2),
    });
    println!("BENCH_JSON {burst_line}");
    cluster.shutdown().await;
}
