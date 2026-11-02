//! Do concurrent ballot raises starve batch commits of their fsyncs?
//!
//! `#[ignore]`: a measurement, not an assertion. Run explicitly:
//!
//! ```text
//! cargo test --release -p felix-storage --test ballot_fsync_contention -- --ignored --nocapture
//! ```
//!
//! One log appends and commits under `FsyncMode::OnCommit` while K tasks raise
//! the accepted generation on K other logs in the same directory, each raise
//! writing and flushing `ballot` and `replica`. Reports commit latency and
//! throughput, and raise latency, per K.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use felix_storage::log::{AppendRecord, FsyncMode, LogConfig, ShardKey};
use felix_storage::{DiskLog, DiskLogProvider};

const PAYLOAD: usize = 1024;

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|v| v.split(',').filter_map(|p| p.trim().parse().ok()).collect())
        .unwrap_or_else(|| default.to_vec())
}

fn env_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

fn shard(stream: &str) -> ShardKey {
    ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: stream.into(),
        shard: 0,
    }
}

fn record() -> Vec<AppendRecord> {
    vec![AppendRecord {
        payload: vec![b'x'; PAYLOAD].into(),
        timestamp_micros: 0,
        mark: Default::default(),
        publisher: None,
        headers: None,
    }]
}

/// Nearest rank, in microseconds.
fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64) * p / 100.0).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

struct Cell {
    commits: Vec<u64>,
    raises: Vec<u64>,
    elapsed: Duration,
}

async fn run_cell(provider: &DiskLogProvider, label: &str, appenders: usize, k: usize) -> Cell {
    let warmup = env_secs("BENCH_WARMUP_SECS", 1);
    let measure = env_secs("BENCH_CELL_SECS", 6);
    let log: Arc<DiskLog> = Arc::new(
        provider
            .open_shard(&shard(&format!("{label}-commit")))
            .expect("open commit log"),
    );
    let pending = log.append_pending(&record()).await.expect("warm append");
    log.commit(&pending).await.expect("warm commit");

    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));

    let mut raisers = Vec::new();
    for i in 0..k {
        let ballot_log = provider
            .open_shard(&shard(&format!("{label}-ballot-{i}")))
            .expect("open ballot log");
        let stop = Arc::clone(&stop);
        let measuring = Arc::clone(&measuring);
        raisers.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut generation = 0u64;
            while !stop.load(Ordering::Relaxed) {
                generation += 1;
                let started = Instant::now();
                ballot_log
                    .accept_generation(generation, Some("leader-a"))
                    .await
                    .expect("raise");
                if measuring.load(Ordering::Relaxed) {
                    samples.push(started.elapsed().as_micros() as u64);
                }
            }
            samples
        }));
    }

    let mut writers = Vec::new();
    for _ in 0..appenders {
        let log = Arc::clone(&log);
        let stop = Arc::clone(&stop);
        let measuring = Arc::clone(&measuring);
        writers.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let records = record();
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let pending = log.append_pending(&records).await.expect("append");
                log.commit(&pending).await.expect("commit");
                if measuring.load(Ordering::Relaxed) {
                    samples.push(started.elapsed().as_micros() as u64);
                }
            }
            samples
        }));
    }

    tokio::time::sleep(warmup).await;
    measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    tokio::time::sleep(measure).await;
    measuring.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut commits = Vec::new();
    for w in writers {
        commits.extend(w.await.expect("writer"));
    }
    let mut raises = Vec::new();
    for r in raisers {
        raises.extend(r.await.expect("raiser"));
    }
    commits.sort_unstable();
    raises.sort_unstable();
    Cell {
        commits,
        raises,
        elapsed,
    }
}

#[ignore = "measurement, not an assertion; see the module docs"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ballot_raises_against_batch_commits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = DiskLogProvider::new(
        dir.path().to_path_buf(),
        LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            ..LogConfig::default()
        },
    )
    .expect("provider");

    let appender_counts = env_list("BENCH_APPENDERS", &[1, 16]);
    let ks = env_list("BENCH_K", &[0, 1, 2, 4, 16, 32, 48]);
    println!(
        "\n=== ballot raises vs batch commits (dir {}) ===",
        dir.path().display()
    );
    println!(
        "{:>9} {:>3} {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}",
        "appenders",
        "K",
        "commits/s",
        "c_p50us",
        "c_p99us",
        "c_p999us",
        "r_p50us",
        "r_p99us",
        "r_max",
        "raises/s"
    );
    for &appenders in &appender_counts {
        for &k in &ks {
            let label = format!("a{appenders}-k{k}");
            let cell = run_cell(&provider, &label, appenders, k).await;
            let secs = cell.elapsed.as_secs_f64();
            let commits_per_s = cell.commits.len() as f64 / secs;
            let raises_per_s = cell.raises.len() as f64 / secs;
            println!(
                "{:>9} {:>3} {:>10.0} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10.0}",
                appenders,
                k,
                commits_per_s,
                pct(&cell.commits, 50.0),
                pct(&cell.commits, 99.0),
                pct(&cell.commits, 99.9),
                pct(&cell.raises, 50.0),
                pct(&cell.raises, 99.0),
                cell.raises.last().copied().unwrap_or(0),
                raises_per_s,
            );
            println!(
                "BENCH_JSON {}",
                serde_json_line(&[
                    ("bench", "\"ballot_fsync\"".to_string()),
                    ("appenders", appenders.to_string()),
                    ("k", k.to_string()),
                    ("commits_per_s", format!("{commits_per_s:.1}")),
                    ("commit_p50_us", pct(&cell.commits, 50.0).to_string()),
                    ("commit_p99_us", pct(&cell.commits, 99.0).to_string()),
                    ("commit_p999_us", pct(&cell.commits, 99.9).to_string()),
                    ("raise_p50_us", pct(&cell.raises, 50.0).to_string()),
                    ("raise_p99_us", pct(&cell.raises, 99.0).to_string()),
                    ("raises_per_s", format!("{raises_per_s:.1}")),
                ])
            );
        }
    }
}

/// felix-storage has no serde_json dev-dependency; the values are numbers or
/// pre-quoted strings.
fn serde_json_line(fields: &[(&str, String)]) -> String {
    let body: Vec<String> = fields.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect();
    format!("{{{}}}", body.join(","))
}
