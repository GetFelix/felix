//! Does aggregate throughput on ONE broker move with listener count?
//!
//! `#[ignore]` -- a measurement, not an assertion. Run explicitly:
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker
//! FELIX_INITIAL_MTU=1350 cargo test --release -p felix-cluster \
//!   --test listener_throughput -- --ignored --nocapture
//! ```
//!
//! `FELIX_INITIAL_MTU=1350` matters: macOS loopback otherwise pins a
//! 16336-byte MTU (~11x a real path), which understates the endpoint driver's
//! per-byte work and measures the wrong thing.
//!
//! On macOS this cannot answer the question: every client endpoint in one
//! process shares one I/O thread (`io_runtime_index`), so the generator
//! saturates before the broker does and every sweep reads flat. On Linux the
//! I/O pool is off and client drivers spread across the test's runtime, so a
//! run there can show a trend -- but the generator still shares the machine's
//! cores with the broker. Treat any run here as directional, never as the
//! per-broker ceiling, which needs generators on separate machines
//! (`scripts/perf/azure`); see #597.
//!
//! Full method, prior results, and what this test's own local run found (a
//! flat 1.00x-0.98x across 1-8 listeners, and why) are in
//! `docs/perf-investigation-sharding-ceiling.md`, section 10.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "bench";
const PAYLOAD: usize = 4096;
const BATCH: usize = 32;
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
/// Shards, and therefore independent downstream commit paths.
///
/// One shard makes this measure #535 -- the publish ingress serialising a shard
/// to one worker -- rather than the receive path. The first run of this
/// benchmark did exactly that: flat throughput with the broker at 1.87 cores
/// and 13 of 16 idle, which is a downstream funnel, not a feeder.
const SHARDS: u32 = 12;
const RUN: Duration = Duration::from_secs(12);
const WARMUP: Duration = Duration::from_secs(3);

/// Accumulated CPU seconds for a process, from `ps`.
///
/// Sampled at both ends of the measured window and differenced, so what comes
/// out is cores-used over that window. A single-sample `%cpu` would be an
/// average since process start, which is the same mistake #537 documents in the
/// Azure harness.
fn cpu_seconds(pid: u32) -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if raw.is_empty() {
        return None;
    }
    // Formats: "MM:SS.ss" or "HH:MM:SS".
    let parts: Vec<&str> = raw.split(':').collect();
    let mut seconds = 0.0;
    for part in &parts {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(seconds)
}

struct Measured {
    mib_per_sec: f64,
    records: u64,
    listeners: usize,
    broker_cores: f64,
    client_cores: f64,
}

async fn measure(listeners: usize) -> Measured {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        quic_listeners: listeners,
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    let spread = cluster.client_on(&owner).await.expect("probe client");
    let ports: Vec<u16> = spread.listeners_in_use().iter().map(|a| a.port()).collect();
    drop(spread);

    let bytes = Arc::new(AtomicU64::new(0));
    let records = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicU64::new(0));

    let publishers = env_usize("BENCH_PUBLISHERS", 8);
    let mut tasks = Vec::new();
    for publisher_index in 0..publishers {
        let client = cluster.client_on(&owner).await.expect("client");
        let publisher = client.publisher().await.expect("publisher");
        let tenant = cluster.tenant_id.clone();
        let namespace = cluster.namespace.clone();
        let bytes = Arc::clone(&bytes);
        let records = Arc::clone(&records);
        let stop = Arc::clone(&stop);
        tasks.push(tokio::spawn(async move {
            // Held so the connections stay open for the run.
            let _client = client;
            let payloads: Vec<Vec<u8>> = (0..BATCH).map(|_| vec![7u8; PAYLOAD]).collect();
            let mut seq: u64 = publisher_index as u64;
            let mut reported = false;
            while stop.load(Ordering::Relaxed) == 0 {
                // `publish_batch` with `AckMode::None` is the path felix-loadgen's
                // ingest scenario uses, and so the one every published
                // throughput figure describes. The receive path under test does
                // the same per-datagram work either way; waiting on acks would
                // measure the round trip instead.
                // Keyed, because the key picks the shard: unkeyed batches all
                // resolve to shard 0 and put the funnel back.
                let key = format!("k{}", seq % u64::from(SHARDS));
                seq += 1;
                match publisher
                    .publish_batch_keyed(
                        &tenant,
                        &namespace,
                        STREAM,
                        bytes::Bytes::from(key.into_bytes()),
                        payloads.clone(),
                        AckMode::None,
                    )
                    .await
                {
                    Ok(_) => {
                        bytes.fetch_add((PAYLOAD * BATCH) as u64, Ordering::Relaxed);
                        records.fetch_add(BATCH as u64, Ordering::Relaxed);
                    }
                    // "queue full" is the client's own bounded queue applying
                    // backpressure, not a failure -- the same thing felix-loadgen
                    // counts as a retriable transient. Yield and try again rather
                    // than sleeping, or the measurement becomes a sleep study.
                    Err(err) if format!("{err:#}").contains("queue full") => {
                        tokio::task::yield_now().await;
                    }
                    Err(err) => {
                        if !reported {
                            println!("   publish failed: {err:#}");
                            reported = true;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }));
    }

    // Warm up, then zero the counters so connection setup and the first
    // segment's allocation are not in the sample.
    tokio::time::sleep(WARMUP).await;
    bytes.store(0, Ordering::Relaxed);
    records.store(0, Ordering::Relaxed);
    let broker_pid = cluster.node(&owner).and_then(|n| n.pid());
    let self_pid = std::process::id();
    let broker_cpu0 = broker_pid.and_then(cpu_seconds);
    // What the BROKER says it stored, against what the client thinks it sent.
    // A client-side counter measures enqueue rate; only this says the bytes
    // arrived.
    let appended0 = cluster
        .metric(&owner, "felix_storage_append_bytes_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);
    let client_cpu0 = cpu_seconds(self_pid);
    let started = Instant::now();
    tokio::time::sleep(RUN).await;
    let elapsed = started.elapsed();
    let appended1 = cluster
        .metric(&owner, "felix_storage_append_bytes_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);
    let broker_mib = (appended1 - appended0) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let broker_cores = match (broker_cpu0, broker_pid.and_then(cpu_seconds)) {
        (Some(a), Some(b)) => (b - a) / elapsed.as_secs_f64(),
        _ => f64::NAN,
    };
    let client_cores = match (client_cpu0, cpu_seconds(self_pid)) {
        (Some(a), Some(b)) => (b - a) / elapsed.as_secs_f64(),
        _ => f64::NAN,
    };
    let moved = bytes.load(Ordering::Relaxed);
    let count = records.load(Ordering::Relaxed);
    stop.store(1, Ordering::Relaxed);
    for task in tasks {
        let _ = task.await;
    }

    let mib_per_sec = moved as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "listeners={listeners:<2} client-sent {mib_per_sec:7.1} MiB/s  broker-stored {broker_mib:7.1} MiB/s  \
         broker {broker_cores:5.2} cores  client {client_cores:5.2} cores  ports={}",
        ports.len(),
    );
    cluster.shutdown().await;
    Measured {
        mib_per_sec,
        records: count,
        listeners,
        broker_cores,
        client_cores,
    }
}

#[serial]
#[ignore = "measurement, not an assertion; see the module docs"]
#[tokio::test(flavor = "multi_thread")]
async fn throughput_against_listener_count() {
    let mtu = std::env::var("FELIX_INITIAL_MTU").unwrap_or_else(|_| "<unset>".into());
    println!("\n=== publish throughput vs listener count ===");
    println!(
        "payload={PAYLOAD}B batch={BATCH} shards={SHARDS} publishers={} run={}s FELIX_INITIAL_MTU={mtu}",
        env_usize("BENCH_PUBLISHERS", 8),
        RUN.as_secs(),
    );
    if mtu == "<unset>" {
        println!(
            "!! FELIX_INITIAL_MTU is unset: macOS loopback pins a 16336-byte MTU, so the \n\
             !! endpoint driver does ~11x less per-byte work than on a real path and this \n\
             !! measures the wrong bottleneck. Re-run with FELIX_INITIAL_MTU=1350."
        );
    }

    let mut results = Vec::new();
    let sweep: Vec<usize> = std::env::var("BENCH_LISTENERS")
        .ok()
        .map(|v| v.split(',').filter_map(|p| p.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 2, 4, 8]);
    for listeners in sweep {
        results.push(measure(listeners).await);
    }

    println!("\n--- summary ---");
    let base = results[0].mib_per_sec;
    for r in &results {
        println!(
            "listeners={:<2} {:8.1} MiB/s  {:.2}x   broker {:5.2} cores   client {:5.2} cores   ({} records)",
            r.listeners,
            r.mib_per_sec,
            r.mib_per_sec / base,
            r.broker_cores,
            r.client_cores,
            r.records,
        );
    }
    println!();
}
