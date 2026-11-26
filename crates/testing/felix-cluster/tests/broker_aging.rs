//! Diagnostic: does a native broker slow down or grow over a long run of
//! one-record publishes? (#3203)
//!
//! `#[ignore]`: it runs for `FELIX_AGING_MINUTES` (default 40) and is started by
//! the `Broker aging` workflow. Each minute it runs the load generator for 60 s
//! and then records every broker's RSS and CPU, the minute's acked msg/s, and
//! a full `/metrics` scrape into `FELIX_AGING_DIR`, so whatever grows can be
//! read off afterwards.
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker -p felix-loadgen
//! FELIX_AGING_DIR=/tmp/aging FELIX_AGING_RF=1 cargo test --release -p felix-cluster \
//!   --test broker_aging -- --ignored --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;
use tokio::process::Command;

const STREAM: &str = "aging";
const SHARDS: u32 = 48;
const PAYLOAD_BYTES: usize = 256;
const IN_FLIGHT: usize = 64;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// CPU seconds and RSS of `pid`, from /proc.
fn proc_sample(pid: u32) -> (f64, u64) {
    let mut cpu_s = 0.0;
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        let rest = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let ticks = |i: usize| {
            fields
                .get(i)
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        // SAFETY: sysconf has no preconditions.
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64;
        cpu_s = (ticks(11) + ticks(12)) / hz;
    }
    let rss_kib = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0);
    (cpu_s, rss_kib)
}

/// One loadgen run of `secs` seconds, at `rate` msg/s when set; returns
/// (acked, acked msg/s, row).
async fn run_ingest(
    cluster: &Cluster,
    secs: u64,
    rate: Option<u64>,
) -> Result<(u64, f64, serde_json::Value)> {
    let loadgen = felix_cluster::built_binary("felix-loadgen")
        .map_err(|err| anyhow!("{err}; build it first"))?;
    let brokers: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.client_addr.to_string())
        .collect();
    let mut command = Command::new(loadgen);
    if let Some(rate) = rate {
        command.args(["--rate", &rate.to_string()]);
    }
    let output = command
        .args(["--brokers", &brokers.join(",")])
        .args(["--tenant", &cluster.tenant_id])
        .args(["--namespace", &cluster.namespace])
        .args(["--token", &cluster.client_token()])
        .args(["--environment", "aging"])
        .args(["--scenario", "ingest", "--stream", STREAM])
        .args([
            "--keys",
            &SHARDS.to_string(),
            "--shards",
            &SHARDS.to_string(),
        ])
        .args(["--in-flight", &IN_FLIGHT.to_string()])
        .args(["--concurrency", "1"])
        .args(["--batch", "1"])
        .args(["--payload-bytes", &PAYLOAD_BYTES.to_string()])
        .args(["--duration-secs", &secs.to_string()])
        .output()
        .await
        .context("run felix-loadgen")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        bail!(
            "felix-loadgen failed ({}):\n{stdout}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("LOADGEN_JSON "))
        .context("no LOADGEN_JSON line")?;
    let row: serde_json::Value = serde_json::from_str(line)?;
    Ok((
        row["acked_records"].as_u64().unwrap_or(0),
        row["acked_throughput_msg_s"].as_f64().unwrap_or(0.0),
        row,
    ))
}

async fn scrape(http: &reqwest::Client, addr: std::net::SocketAddr) -> String {
    match http.get(format!("http://{addr}/metrics")).send().await {
        Ok(response) => response.text().await.unwrap_or_default(),
        Err(err) => format!("# scrape failed: {err}\n"),
    }
}

/// Guest instructions since the previous dump, when the broker runs under
/// callgrind with `--callgrind-out-file=<cg_dir>/cg.%p`.
fn callgrind_ir(cg_dir: &Path, pid: u32) -> Option<u64> {
    // Callgrind numbers each requested dump `cg.<pid>.<n>`.
    let newest = || {
        let prefix = format!("cg.{pid}.");
        std::fs::read_dir(cg_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_prefix(&prefix)?
                    .parse::<u32>()
                    .ok()
            })
            .max()
    };
    let before = newest();
    std::process::Command::new("callgrind_control")
        .args(["-d", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .status()
        .ok()?;
    for _ in 0..300 {
        if let Some(part) = newest().filter(|&part| Some(part) > before)
            && let Ok(text) = std::fs::read_to_string(cg_dir.join(format!("cg.{pid}.{part}")))
            && let Some(ir) = text
                .lines()
                .find_map(|line| line.strip_prefix("summary: "))
                .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        {
            return Some(ir);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn write(dir: &Path, name: &str, body: &str) {
    if let Err(err) = std::fs::write(dir.join(name), body) {
        eprintln!("aging: write {name}: {err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "runs for tens of minutes; run by the Broker aging workflow"]
async fn broker_aging_under_one_record_publishes() {
    let dir = PathBuf::from(std::env::var("FELIX_AGING_DIR").expect("set FELIX_AGING_DIR"));
    std::fs::create_dir_all(&dir).expect("create FELIX_AGING_DIR");
    let minutes = env_usize("FELIX_AGING_MINUTES", 40);
    let rf = env_usize("FELIX_AGING_RF", 1) as u32;
    // Unset: flat out. 0: no publishes at all (idle). n: paced at n msg/s.
    let rate = std::env::var("FELIX_AGING_RATE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    // Set when the broker runs under callgrind: guest instructions per minute.
    let cg_dir = std::env::var("FELIX_AGING_CALLGRIND_DIR")
        .ok()
        .map(PathBuf::from);
    let stream = if rf > 1 {
        StreamSpec::quorum(STREAM, SHARDS, rf)
    } else {
        StreamSpec::new(STREAM, SHARDS)
    };
    let cluster = Cluster::start(ClusterConfig {
        nodes: rf as usize,
        streams: vec![stream],
        // As the instructions-per-message gate runs it.
        broker_env: vec![(
            "FELIX_DURABLE_FSYNC_MODE".to_string(),
            "periodic".to_string(),
        )],
        sync_interval_ms: 2000,
        node_expiry: Duration::from_secs(15),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .await_raft_leaders_placed(Duration::from_secs(120))
        .await
        .expect("leaders placed");

    let http = reqwest::Client::new();
    let pids: Vec<u32> = cluster
        .nodes
        .iter()
        .map(|node| node.pid().expect("broker pid"))
        .collect();
    let mut last: Vec<(f64, u64)> = pids.iter().map(|&pid| proc_sample(pid)).collect();
    if let Some(d) = cg_dir.as_deref() {
        for &pid in &pids {
            callgrind_ir(d, pid);
        }
    }
    let mut total_acked = 0u64;
    let mut series = Vec::new();
    for (index, node) in cluster.nodes.iter().enumerate() {
        write(
            &dir,
            &format!("metrics-n{index}-m000.txt"),
            &scrape(&http, node.metrics_addr).await,
        );
    }
    for minute in 1..=minutes {
        let started = Instant::now();
        let (acked, msg_s, row) = if rate == Some(0) {
            tokio::time::sleep(Duration::from_secs(60)).await;
            (0, 0.0, serde_json::Value::Null)
        } else {
            run_ingest(&cluster, 60, rate).await.expect("loadgen")
        };
        let wall = started.elapsed().as_secs_f64();
        total_acked += acked;
        let mut nodes = Vec::new();
        for (index, node) in cluster.nodes.iter().enumerate() {
            let now = proc_sample(pids[index]);
            let cpu_s = now.0 - last[index].0;
            let cpu_pct = cpu_s / wall * 100.0;
            let ir = cg_dir.as_deref().and_then(|d| callgrind_ir(d, pids[index]));
            last[index] = now;
            write(
                &dir,
                &format!("metrics-n{index}-m{minute:03}.txt"),
                &scrape(&http, node.metrics_addr).await,
            );
            nodes.push(serde_json::json!({
                "node": index,
                "rss_mib": now.1 as f64 / 1024.0,
                "cpu_pct": cpu_pct,
                "callgrind_ir": ir,
                "callgrind_m_ir_per_cpu_s": ir.map(|ir| ir as f64 / cpu_s.max(1e-9) / 1e6),
            }));
        }
        let entry = serde_json::json!({
            "minute": minute,
            "acked": acked,
            "total_acked": total_acked,
            "msg_s": msg_s,
            "failed_sends": row["failed_sends"],
            "publish_retries": row["publish_retries"],
            "nodes": nodes,
        });
        eprintln!("AGING {entry}");
        series.push(entry);
        write(
            &dir,
            "series.json",
            &serde_json::to_string_pretty(&series).unwrap(),
        );
    }
}

/// `Anonymous` and `Rss` from /proc/<pid>/smaps_rollup, the thread count and
/// open descriptors, in KiB and counts.
fn proc_detail(pid: u32) -> serde_json::Value {
    let field = |text: &str, name: &str| -> u64 {
        text.lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    };
    let rollup = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).unwrap_or_default();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |dir| dir.count());
    serde_json::json!({
        "anon_mib": field(&rollup, "Anonymous:") as f64 / 1024.0,
        "threads": field(&status, "Threads:"),
        "fds": fds,
    })
}

/// The newest jemalloc totals line the broker printed (`stats_interval`),
/// reading `log` from `offset` on. Returns allocated, active, resident,
/// retained in MiB.
fn jemalloc_totals(log: &Path, offset: &mut u64) -> Option<serde_json::Value> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(log).ok()?;
    file.seek(SeekFrom::Start(*offset)).ok()?;
    let mut text = String::new();
    let read = file.read_to_string(&mut text).ok()?;
    *offset += read as u64;
    let line = text
        .lines()
        .rev()
        .find(|line| line.starts_with("Allocated:"))?;
    let mut out = serde_json::Map::new();
    for part in line.split(',') {
        let mut words = part.split(':');
        let (Some(key), Some(value)) = (words.next(), words.next()) else {
            continue;
        };
        if let Ok(bytes) = value.trim().parse::<f64>() {
            out.insert(
                format!("{}_mib", key.trim().to_lowercase()),
                (bytes / 1048576.0).into(),
            );
        }
    }
    Some(out.into())
}

fn disk_free_gib(path: &Path) -> f64 {
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
        return 0.0;
    };
    // SAFETY: statvfs writes into the zeroed struct we own.
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return 0.0;
        }
        stat.f_bavail as f64 * stat.f_frsize as f64 / (1u64 << 30) as f64
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => dir_bytes(&entry.path()),
            Ok(_) => entry.metadata().map_or(0, |meta| meta.len()),
            Err(_) => 0,
        })
        .sum()
}

/// A long soak: one RF 1 broker, two publishers at `FELIX_SOAK_RATE` msg/s
/// each. One keeps its connection for the whole soak; the other reconnects
/// every minute, so a per-connection leak shows as growth that the long-lived
/// half alone would not cause. Stops early, cleanly, when the disk runs low.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "runs for hours; run by the Broker aging workflow"]
async fn broker_soak_one_record_publishes() {
    let dir = PathBuf::from(std::env::var("FELIX_AGING_DIR").expect("set FELIX_AGING_DIR"));
    std::fs::create_dir_all(&dir).expect("create FELIX_AGING_DIR");
    let minutes = env_usize("FELIX_AGING_MINUTES", 240);
    let rate = env_usize("FELIX_SOAK_RATE", 2500) as u64;
    let mut broker_env = vec![(
        "FELIX_DURABLE_FSYNC_MODE".to_string(),
        "periodic".to_string(),
    )];
    // The broker alone gets these, so only its allocator is swapped.
    for (from, to) in [
        ("FELIX_SOAK_BROKER_LD_PRELOAD", "LD_PRELOAD"),
        ("FELIX_SOAK_BROKER_MALLOC_CONF", "MALLOC_CONF"),
    ] {
        if let Ok(value) = std::env::var(from) {
            broker_env.push((to.to_string(), value));
        }
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        broker_env,
        sync_interval_ms: 2000,
        node_expiry: Duration::from_secs(15),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .await_raft_leaders_placed(Duration::from_secs(120))
        .await
        .expect("leaders placed");
    let cluster = std::sync::Arc::new(cluster);
    let node = &cluster.nodes[0];
    let pid = node.pid().expect("broker pid");
    let data_dir = node.data_dir.clone();
    let log = data_dir.join("broker.log");
    let http = reqwest::Client::new();

    // The long-lived publisher: one loadgen run for the whole soak.
    let long_secs = minutes as u64 * 60 + 30;
    let long_cluster = std::sync::Arc::clone(&cluster);
    let long = tokio::spawn(async move { run_ingest(&long_cluster, long_secs, Some(rate)).await });

    let mut last_cpu = proc_sample(pid).0;
    let mut last_messages = cluster
        .metric(&node.node_id, "felix_tenant_published_messages_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);
    let mut log_offset = 0u64;
    let mut short_acked = 0u64;
    let mut series = Vec::new();
    for minute in 1..=minutes {
        let started = Instant::now();
        let (acked, _, row) = run_ingest(&cluster, 60, Some(rate))
            .await
            .expect("reconnecting loadgen");
        short_acked += acked;
        let wall = started.elapsed().as_secs_f64();
        let (cpu, rss_kib) = proc_sample(pid);
        let messages = cluster
            .metric(&node.node_id, "felix_tenant_published_messages_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(last_messages);
        let entry = serde_json::json!({
            "minute": minute,
            "msg_s": (messages - last_messages) / wall,
            "total_messages": messages,
            "short_acked": short_acked,
            "short_failed": row["failed_sends"],
            "rss_mib": rss_kib as f64 / 1024.0,
            "cpu_pct": (cpu - last_cpu) / wall * 100.0,
            "proc": proc_detail(pid),
            "jemalloc": jemalloc_totals(&log, &mut log_offset),
            "data_gib": dir_bytes(&data_dir) as f64 / (1u64 << 30) as f64,
            "disk_free_gib": disk_free_gib(&data_dir),
            "long_publisher_running": !long.is_finished(),
        });
        last_cpu = cpu;
        last_messages = messages;
        write(
            &dir,
            &format!("metrics-n0-m{minute:03}.txt"),
            &scrape(&http, node.metrics_addr).await,
        );
        eprintln!("SOAK {entry}");
        let low_disk = entry["disk_free_gib"].as_f64().unwrap_or(0.0) < 4.0;
        series.push(entry);
        write(
            &dir,
            "series.json",
            &serde_json::to_string_pretty(&series).unwrap(),
        );
        if low_disk {
            eprintln!("SOAK stopping early: disk low");
            break;
        }
    }
    long.abort();
}
