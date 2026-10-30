//! CPU instructions the broker retires per acknowledged message.
//!
//! `#[ignore]`: it needs Linux `perf` with hardware counters, and CI's
//! `Instructions per message` workflow runs it. Wall-clock throughput moves with
//! whatever else shares the runner; the instruction count for a fixed number
//! of messages barely does, so it can pin a small per-message cost to the PR
//! that added it. `scripts/perf/ipm_gate.py` compares the result with
//! `scripts/perf/ipm-baseline.toml`.
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker -p felix-loadgen
//! FELIX_IPM_OUT=ipm.json cargo test --release -p felix-cluster \
//!   --test instructions_per_message -- --ignored --nocapture
//! ```
//!
//! Only the broker process is counted (`perf stat -p`), so a load generator
//! change cannot hide or fake a broker regression. The stream is in-memory, so
//! the disk's cost stays out of the number.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "ipm";
const SHARDS: u32 = 48;
const PAYLOAD_BYTES: usize = 256;
const IN_FLIGHT: usize = 64;

/// Messages discarded per cell before counting starts, so connection setup,
/// lazy allocation and first-touch costs are not in the measurement.
const WARMUP_MESSAGES: usize = 20_000;

/// (cell name, publish batch size, measured messages).
const CELLS: [(&str, usize, usize); 2] = [("b1", 1, 100_000), ("b64", 64, 400_000)];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn loadgen_binary() -> Result<PathBuf> {
    let mut dir = std::env::current_exe().context("locate the running executable")?;
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    let candidate = dir.join("felix-loadgen");
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(anyhow!(
        "felix-loadgen not found at {}; build it first",
        candidate.display()
    ))
}

/// Run the ingest scenario and return the records the broker acknowledged.
fn run_ingest(cluster: &Cluster, batch: usize, total: usize) -> Result<u64> {
    let node = &cluster.nodes[0];
    let output = Command::new(loadgen_binary()?)
        .args(["--brokers", &node.client_addr.to_string()])
        .args(["--tenant", &cluster.tenant_id])
        .args(["--namespace", &cluster.namespace])
        .args(["--token", &cluster.client_token()])
        .args(["--environment", "ipm"])
        .args(["--scenario", "ingest", "--stream", STREAM])
        .args([
            "--keys",
            &SHARDS.to_string(),
            "--shards",
            &SHARDS.to_string(),
        ])
        .args(["--in-flight", &IN_FLIGHT.to_string()])
        .args(["--concurrency", "1"])
        .args(["--batch", &batch.to_string()])
        .args(["--payload-bytes", &PAYLOAD_BYTES.to_string()])
        .args(["--total", &total.to_string()])
        .output()
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
    let acked = row["acked_records"].as_u64().unwrap_or(0);
    let failed = row["failed_sends"].as_u64().unwrap_or(0);
    if failed > 0 || row["publish_retries"].as_u64().unwrap_or(0) > 0 {
        bail!("the run was not clean, so its count is not comparable: {row}");
    }
    Ok(acked)
}

/// Broker instructions (user, kernel) spent while `work` runs.
fn count_instructions(pid: u32, work: impl FnOnce() -> Result<u64>) -> Result<(u64, u64, u64)> {
    let out = std::env::temp_dir().join(format!("ipm-perf-{pid}.csv"));
    let mut perf = Command::new("perf")
        .args(["stat", "-x,", "-e", "instructions:u,instructions:k"])
        .args(["-p", &pid.to_string(), "-o"])
        .arg(&out)
        .stdout(Stdio::null())
        .spawn()
        .context("start perf (is it installed, and perf_event_paranoid low enough?)")?;
    // perf has to be attached before the load starts.
    std::thread::sleep(Duration::from_millis(500));
    let acked = work();
    let _ = Command::new("kill")
        .args(["-INT", &perf.id().to_string()])
        .status();
    perf.wait()?;
    let acked = acked?;

    let text = std::fs::read_to_string(&out).context("read perf output")?;
    let (mut user, mut kernel) = (None, None);
    for line in text.lines() {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 3 {
            continue;
        }
        let value = fields[0].parse::<u64>().ok();
        match fields[2] {
            event if event.starts_with("instructions") && event.ends_with(":u") => user = value,
            event if event.starts_with("instructions") && event.ends_with(":k") => kernel = value,
            _ => {}
        }
    }
    match (user, kernel) {
        (Some(user), Some(kernel)) => Ok((user, kernel, acked)),
        _ => bail!("perf reported no instruction counts (hardware counters unavailable?):\n{text}"),
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    values[values.len() / 2]
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "needs perf with hardware counters; run by the Instructions per message workflow"]
async fn broker_instructions_per_message() {
    let out = std::env::var("FELIX_IPM_OUT").expect("set FELIX_IPM_OUT to the result file");
    let reps = env_usize("FELIX_IPM_REPS", 5);
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        // A blank directory is how the broker is told to keep nothing on disk.
        broker_env: vec![("FELIX_DURABLE_STORAGE_DIR".into(), " ".into())],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let pid = cluster.nodes[0].pid().expect("broker pid");

    // The loadgen and perf calls block, and the in-process control plane has
    // to keep serving heartbeats meanwhile.
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value> {
        let mut cells = serde_json::Map::new();
        for (name, batch, total) in CELLS {
            run_ingest(&cluster, batch, WARMUP_MESSAGES)?;
            let (mut user, mut kernel) = (Vec::new(), Vec::new());
            for rep in 0..reps {
                let (u, k, acked) = count_instructions(pid, || run_ingest(&cluster, batch, total))?;
                if acked == 0 {
                    bail!("cell {name} rep {rep}: nothing was acknowledged");
                }
                user.push(u as f64 / acked as f64);
                kernel.push(k as f64 / acked as f64);
                eprintln!(
                    "ipm {name} rep {rep}: user {:.1} kernel {:.1} per message",
                    user[rep], kernel[rep]
                );
            }
            let spread = |v: &[f64]| {
                let (lo, hi) = v
                    .iter()
                    .fold((f64::MAX, 0f64), |(lo, hi), x| (lo.min(*x), hi.max(*x)));
                (hi - lo) / lo * 100.0
            };
            cells.insert(
                name.to_string(),
                serde_json::json!({
                    "batch": batch,
                    "messages": total,
                    "user_per_message": median(&mut user.clone()),
                    "kernel_per_message": median(&mut kernel.clone()),
                    "user_spread_pct": spread(&user),
                    "user_reps": user,
                }),
            );
        }
        Ok(serde_json::json!({
            "arch": std::env::consts::ARCH,
            "cells": cells,
        }))
    })
    .await
    .expect("join")
    .expect("measurement");
    std::fs::write(&out, serde_json::to_string_pretty(&result).unwrap()).expect("write result");
}
