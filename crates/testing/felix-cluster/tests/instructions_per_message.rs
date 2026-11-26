//! CPU instructions the broker retires per acknowledged message.
//!
//! `#[ignore]`: it needs Linux with valgrind, and CI's
//! `Instructions per message` workflow runs it. GitHub-hosted runners expose no
//! hardware performance counters, so the broker runs under callgrind, which
//! counts the instructions it executes and is repeatable on any machine. Wall-clock throughput moves with
//! whatever else shares the runner; the instruction count for a fixed number
//! of messages barely does, so it can pin a small per-message cost to the PR
//! that added it. `scripts/perf/ipm_gate.py` compares the result with
//! `scripts/perf/ipm-baseline.toml`.
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker -p felix-loadgen
//! FELIX_BROKER_WRAPPER="valgrind --tool=callgrind --callgrind-out-file=$DIR/cg.%p" \
//! FELIX_IPM_DIR=$DIR FELIX_IPM_OUT=ipm.json cargo test --release -p felix-cluster \
//!   --test instructions_per_message -- --ignored --nocapture
//! ```
//!
//! Only the broker process is counted, so a load generator
//! change cannot hide or fake a broker regression. The stream is durable on the
//! suite's default engine (Raft), with periodic fsync so the disk's wait stays
//! out of the number. Time-driven work is fitted out, not subtracted at the idle
//! rate; see `fit_time_cost`.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "ipm";
const SHARDS: u32 = 48;
const PAYLOAD_BYTES: usize = 256;
const IN_FLIGHT: usize = 64;

/// How long the idle broker is counted to find its per-second background cost.
const IDLE_WINDOW: Duration = Duration::from_secs(10);

/// A paced repetition publishes at this fraction of the previous unpaced one's
/// rate, so it runs about twice as long for the same messages. See
/// [`fit_time_cost`] for why the reps need durations that differ by design.
const PACED_FRACTION: f64 = 0.5;

/// A cell: name, publish batch size, measured messages per repetition, and
/// warm-up messages. The count keeps falling for the first few tens of
/// thousands of messages (queues and buffers growing to their working size),
/// so the warm-up is long. Each cell gets a broker of its own: under callgrind
/// on aarch64 a broker runs slower the longer it has run, and a cell measured
/// on an aged broker can queue acks past the client's timeout.
struct CellSpec {
    name: &'static str,
    batch: usize,
    messages: usize,
    warmup: usize,
}

static CELLS: [CellSpec; 2] = [
    CellSpec {
        name: "b1",
        batch: 1,
        messages: 10_000,
        warmup: 80_000,
    },
    CellSpec {
        name: "b64",
        batch: 64,
        messages: 80_000,
        warmup: 320_000,
    },
];

/// The same load as `b64` on a broker whose MQTT listener is on and idle. The
/// gate holds it to `b64` from the same run: a listener nobody uses must cost
/// the QUIC path nothing.
static MQTT_IDLE_CELLS: [CellSpec; 1] = [CellSpec {
    name: "b64_mqtt_idle",
    batch: 64,
    messages: 80_000,
    warmup: 320_000,
}];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn loadgen_binary() -> Result<PathBuf> {
    felix_cluster::built_binary("felix-loadgen").map_err(|err| anyhow!("{err}; build it first"))
}

/// What one loadgen run reported.
struct Ingest {
    acked: u64,
    msg_s: f64,
}

/// Run the ingest scenario, at `rate` messages a second or as fast as acks
/// allow. A measured run must be clean: retries and failed sends add work the
/// baseline did not have. A warm-up only has to finish.
fn run_ingest(
    cluster: &Cluster,
    batch: usize,
    total: usize,
    rate: Option<u64>,
    must_be_clean: bool,
) -> Result<Ingest> {
    run_ingest_for(cluster, batch, total, None, rate, must_be_clean)
}

/// [`run_ingest`], for `duration` seconds instead of `total` records when set.
fn run_ingest_for(
    cluster: &Cluster,
    batch: usize,
    total: usize,
    duration: Option<u64>,
    rate: Option<u64>,
    must_be_clean: bool,
) -> Result<Ingest> {
    let brokers = cluster
        .nodes
        .iter()
        .map(|node| node.client_addr.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let mut command = Command::new(loadgen_binary()?);
    command
        .args(["--brokers", &brokers])
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
        .args(["--payload-bytes", &PAYLOAD_BYTES.to_string()]);
    match duration {
        Some(secs) => command.args(["--duration-secs", &secs.to_string()]),
        None => command.args(["--total", &total.to_string()]),
    };
    if let Some(rate) = rate {
        command.args(["--rate", &rate.to_string()]);
    }
    let output = command.output().context("run felix-loadgen")?;
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
    if must_be_clean && (failed > 0 || row["publish_retries"].as_u64().unwrap_or(0) > 0) {
        bail!(
            "the run was not clean, so its count is not comparable ({}): {row}",
            lease_counters(cluster)
        );
    }
    Ok(Ingest {
        acked,
        msg_s: row["acked_throughput_msg_s"].as_f64().unwrap_or(0.0),
    })
}

/// The broker's lease lapses and lease refusals so far: a retry the broker
/// asked for is most often one of these.
fn lease_counters(cluster: &Cluster) -> String {
    let node = &cluster.nodes[0].node_id;
    let handle = tokio::runtime::Handle::current();
    [
        "felix_broker_lease_expiries_total",
        "felix_broker_lease_refusals_total",
    ]
    .iter()
    .map(|name| match handle.block_on(cluster.metric(node, name)) {
        Ok(value) => format!("{name} = {}", value.unwrap_or(0.0)),
        Err(err) => format!("{name} unreadable: {err:#}"),
    })
    .collect::<Vec<_>>()
    .join(", ")
}

/// The Ir recorded in the newest callgrind dump for `pid`, after asking the
/// running broker for one. Callgrind starts a new part at each dump, so the
/// `summary:` line is the cost since the previous dump.
fn dump_instructions(dir: &Path, pid: u32) -> Result<u64> {
    let before = dumps(dir, pid).len();
    let status = Command::new("callgrind_control")
        .args(["-d", &pid.to_string()])
        .stdout(Stdio::null())
        .status()
        .context("run callgrind_control (is valgrind installed?)")?;
    if !status.success() {
        bail!("callgrind_control -d {pid} failed: {status}");
    }
    for _ in 0..100 {
        let found = dumps(dir, pid);
        if found.len() > before {
            let text = std::fs::read_to_string(found.last().unwrap())?;
            if let Some(count) = text
                .lines()
                .find_map(|line| line.strip_prefix("summary: "))
                .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
            {
                return Ok(count);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!(
        "no callgrind dump appeared for pid {pid} in {}",
        dir.display()
    )
}

/// Dump files for `pid`, oldest first.
fn dumps(dir: &Path, pid: u32) -> Vec<PathBuf> {
    // The first part has no suffix; later ones are numbered.
    let prefix = format!("cg.{pid}");
    let mut found: Vec<(u32, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let rest = name.strip_prefix(&prefix)?;
            let part = match rest.strip_prefix('.') {
                Some(number) => number.parse().ok()?,
                None if rest.is_empty() => 0,
                None => return None,
            };
            Some((part, entry.path()))
        })
        .collect();
    found.sort();
    found.into_iter().map(|(_, path)| path).collect()
}

/// Broker instructions spent while `work` runs, and what `work` returned.
fn count_instructions<T>(
    dir: &Path,
    pid: u32,
    work: impl FnOnce() -> Result<T>,
) -> Result<(u64, T)> {
    // Starts a fresh part, so the second dump covers `work` alone.
    dump_instructions(dir, pid)?;
    let out = work()?;
    Ok((dump_instructions(dir, pid)?, out))
}

/// Least-squares fit of `instructions = a * messages + c * seconds` over reps
/// that all carry the same messages, returned as `(a, c, worst residual)`.
///
/// Under callgrind part of the broker's work runs per second, not per
/// message: timers, and more of them under load than when idle (an active
/// Raft group heartbeats every 250 ms, an idle one every 2 s). Taking off the
/// idle rate leaves the difference in every message, scaled by how long the
/// rep took, so a slow runner reads as a regression. Fitting both terms
/// removes the time-driven work at the rate it runs under load. The reps
/// alternate between unpaced and paced at half speed so their durations
/// differ by design; without that the two terms cannot be told apart.
fn fit_time_cost(reps: &[(f64, f64)], messages: f64) -> Option<(f64, f64, f64)> {
    let n = reps.len() as f64;
    let (xs, ys): (Vec<f64>, Vec<f64>) = reps
        .iter()
        .map(|&(secs, ir)| (secs / messages, ir / messages))
        .unzip();
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let sxx: f64 = xs.iter().map(|x| (x - mx).powi(2)).sum();
    if sxx.is_nan() || sxx <= 0.0 {
        return None;
    }
    let c = xs
        .iter()
        .zip(&ys)
        .map(|(x, y)| (x - mx) * (y - my))
        .sum::<f64>()
        / sxx;
    let a = my - c * mx;
    let worst = xs
        .iter()
        .zip(&ys)
        .map(|(x, y)| ((y - (a + c * x)) / a).abs() * 100.0)
        .fold(0.0, f64::max);
    Some((a, c, worst))
}

/// What the broker process and the runner did, read from /proc. Kept with
/// each rep so a slowdown can be put down to CPU, waiting, memory or disk.
#[derive(Default, Clone, Copy)]
struct ProcSample {
    cpu_s: f64,
    rss_kib: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
    io_stall_us: u64,
}

impl ProcSample {
    fn read(pid: u32) -> Self {
        let mut sample = Self::default();
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            // Fields after the parenthesised command name; utime and stime are
            // the 14th and 15th fields overall.
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
            sample.cpu_s = (ticks(11) + ticks(12)) / hz;
        }
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
            for line in status.lines() {
                let value = |line: &str| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0)
                };
                if line.starts_with("VmRSS:") {
                    sample.rss_kib = value(line);
                } else if line.starts_with("voluntary_ctxt_switches:") {
                    sample.voluntary_switches = value(line);
                } else if line.starts_with("nonvoluntary_ctxt_switches:") {
                    sample.involuntary_switches = value(line);
                }
            }
        }
        if let Ok(pressure) = std::fs::read_to_string("/proc/pressure/io") {
            sample.io_stall_us = pressure
                .lines()
                .find(|line| line.starts_with("some"))
                .and_then(|line| line.split("total=").nth(1))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
        }
        sample
    }
}

/// Bytes and files under `dir`.
fn tree_size(dir: &Path) -> (u64, u64) {
    let mut total = (0, 0);
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => {
                let (bytes, files) = tree_size(&entry.path());
                total = (total.0 + bytes, total.1 + files);
            }
            Ok(_) => {
                let bytes = entry.metadata().map_or(0, |meta| meta.len());
                total = (total.0 + bytes, total.1 + 1);
            }
            Err(_) => {}
        }
    }
    total
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    values[values.len() / 2]
}

/// One measured rep, with what the process and runner did meanwhile.
struct Rep {
    paced: bool,
    secs: f64,
    ir: u64,
    msg_s: f64,
    before: ProcSample,
    after: ProcSample,
    data: (u64, u64),
}

impl Rep {
    fn json(&self, messages: usize) -> serde_json::Value {
        serde_json::json!({
            "paced": self.paced,
            "secs": self.secs,
            "ir": self.ir,
            "ir_per_message": self.ir as f64 / messages as f64,
            "msg_s": self.msg_s,
            "cpu_s": self.after.cpu_s - self.before.cpu_s,
            "rss_kib": self.after.rss_kib,
            "voluntary_switches": self.after.voluntary_switches.saturating_sub(self.before.voluntary_switches),
            "involuntary_switches": self.after.involuntary_switches.saturating_sub(self.before.involuntary_switches),
            "io_stall_s": self.after.io_stall_us.saturating_sub(self.before.io_stall_us) as f64 / 1e6,
            "data_bytes": self.data.0,
            "data_files": self.data.1,
        })
    }

    fn log(&self, name: &str, rep: usize, messages: usize) {
        let cpu = self.after.cpu_s - self.before.cpu_s;
        eprintln!(
            "ipm {name} rep {rep} ({}): {:.1} instructions per message before the time fit, {:.1}s, {:.1} msg/s, broker cpu {cpu:.1}s ({:.0}% of wall), {:.2} M instructions per cpu second, rss {} MiB, switches {}/{}, io stall {:.1}s, data {} MiB in {} files",
            if self.paced { "paced" } else { "unpaced" },
            self.ir as f64 / messages as f64,
            self.secs,
            self.msg_s,
            cpu / self.secs * 100.0,
            self.ir as f64 / cpu.max(1e-9) / 1e6,
            self.after.rss_kib / 1024,
            self.after
                .voluntary_switches
                .saturating_sub(self.before.voluntary_switches),
            self.after
                .involuntary_switches
                .saturating_sub(self.before.involuntary_switches),
            self.after
                .io_stall_us
                .saturating_sub(self.before.io_stall_us) as f64
                / 1e6,
            self.data.0 >> 20,
            self.data.1,
        );
    }
}

/// Run one counted rep of `spec` on the broker `pid`.
fn measure_rep(
    cluster: &Cluster,
    dir: &Path,
    pid: u32,
    spec: &CellSpec,
    rate: Option<u64>,
    must_be_clean: bool,
) -> Result<Rep> {
    let before = ProcSample::read(pid);
    let started = Instant::now();
    let (ir, ingest) = count_instructions(dir, pid, || {
        run_ingest(cluster, spec.batch, spec.messages, rate, must_be_clean)
    })?;
    let secs = started.elapsed().as_secs_f64();
    let after = ProcSample::read(pid);
    if ingest.acked != spec.messages as u64 {
        bail!(
            "cell {}: acked {} of {}",
            spec.name,
            ingest.acked,
            spec.messages
        );
    }
    Ok(Rep {
        paced: rate.is_some(),
        secs,
        ir,
        msg_s: ingest.msg_s,
        before,
        after,
        data: tree_size(&cluster.nodes[0].data_dir),
    })
}

/// Start a one-node cluster with `env` added to the broker's, measure `cells`
/// on it, and stop it.
async fn measure_cells(
    dir: &Path,
    reps: usize,
    restart_probe: bool,
    env: Vec<(String, String)>,
    cells: &'static [CellSpec],
) -> serde_json::Map<String, serde_json::Value> {
    // The broker keeps its logs under the node's data directory, as Raft
    // requires. Periodic fsync writes every append but leaves the flush to a
    // timer, so the count is the broker's CPU and not time spent waiting on
    // the disk; the timer's cost is taken out with the other time-driven work.
    let mut broker_env = vec![
        (
            "FELIX_DURABLE_FSYNC_MODE".to_string(),
            "periodic".to_string(),
        ),
        // Callgrind on aarch64 runs b1 at 20 to 60 msg/s with 64 acks
        // outstanding, so an ack can take seconds. The default 5 s commit
        // wait then answers a few publishes as timed out and the rep is not
        // clean. A timeout is not per-message work, so wait longer.
        (
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "60000".to_string(),
        ),
    ];
    broker_env.extend(env);
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        broker_env,
        // The broker polls the control plane on this timer. Under callgrind
        // the work it does scales with wall time, not messages, so a slow
        // tick keeps it out of the per-message figure.
        sync_interval_ms: 2000,
        // The harness's 1 s expiry leaves the broker a 750 ms lease. Callgrind
        // runs it tens of times slower, and under load a heartbeat can slip
        // past that: the lease lapses and publishes are refused as fenced and
        // retried. A deployment's expiry keeps that out of the measured run.
        node_expiry: Duration::from_secs(15),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let dir = dir.to_path_buf();

    // The loadgen and callgrind calls block, and the in-process control plane has
    // to keep serving heartbeats meanwhile.
    tokio::task::spawn_blocking(move || -> Result<serde_json::Map<String, serde_json::Value>> {
        let mut cluster = cluster;
        let dir = dir.as_path();
        let mut result = serde_json::Map::new();
        for spec in cells {
            let name = spec.name;
            let pid = cluster.nodes[0].pid().context("broker pid")?;
            let warm = run_ingest(&cluster, spec.batch, spec.warmup, None, false)?;
            if (warm.acked as usize) < spec.warmup * 99 / 100 {
                bail!("cell {name}: warm-up acked {} of {}", warm.acked, spec.warmup);
            }
            // Kept for comparison: what the idle broker spends per second.
            let idle_start = Instant::now();
            let (idle_ir, ()) = count_instructions(dir, pid, || {
                std::thread::sleep(IDLE_WINDOW);
                Ok(())
            })?;
            let idle_rate = idle_ir as f64
                / idle_start.elapsed().as_secs_f64().max(IDLE_WINDOW.as_secs_f64());
            let mut measured: Vec<Rep> = Vec::new();
            let mut last_rate = warm.msg_s;
            for rep in 0..reps {
                // Even reps run unpaced, odd ones at half the rate of the
                // unpaced rep before them.
                let rate = (rep % 2 == 1)
                    .then(|| ((last_rate * PACED_FRACTION).round() as u64).max(1));
                let measured_rep = measure_rep(&cluster, dir, pid, spec, rate, true)
                    .with_context(|| format!("cell {name} rep {rep}"))?;
                measured_rep.log(name, rep, spec.messages);
                if rate.is_none() {
                    last_rate = measured_rep.msg_s;
                }
                measured.push(measured_rep);
            }
            let messages = spec.messages as f64;
            let points: Vec<(f64, f64)> = measured.iter().map(|r| (r.secs, r.ir as f64)).collect();
            let (per_message, time_rate, worst_residual) = fit_time_cost(&points, messages)
                .with_context(|| format!("cell {name}: every rep took the same time, so the time cost cannot be fitted"))?;
            let unpaced: Vec<&Rep> = measured.iter().filter(|r| !r.paced).collect();
            // How much slower the last unpaced rep ran than the first: the
            // same work on the same broker should take the same time.
            let slowdown_pct = (unpaced.last().unwrap().secs / unpaced[0].secs - 1.0) * 100.0;
            let idle_subtracted: Vec<f64> = unpaced
                .iter()
                .map(|r| (r.ir as f64 - idle_rate * r.secs) / messages)
                .collect();
            eprintln!(
                "ipm {name}: {per_message:.1} instructions per message after the time fit ({:.2} M instructions/s under load, {:.2} M/s idle; worst residual {worst_residual:.2}%); unpaced reps slowed {slowdown_pct:+.1}% first to last",
                time_rate / 1e6,
                idle_rate / 1e6
            );
            let mut probe = serde_json::Value::Null;
            if restart_probe {
                probe = restart_probe_rep(&mut cluster, dir, spec, &measured)
                    .unwrap_or_else(|err| serde_json::json!({ "error": format!("{err:#}") }));
            }
            result.insert(
                name.to_string(),
                serde_json::json!({
                    "batch": spec.batch,
                    "messages": spec.messages,
                    "method": "time-fit",
                    "user_per_message": per_message,
                    "time_ir_per_second": time_rate,
                    "idle_ir_per_second": idle_rate,
                    "fit_worst_residual_pct": worst_residual,
                    "slowdown_pct": slowdown_pct,
                    "idle_subtracted_min_per_message": idle_subtracted.iter().cloned().fold(f64::MAX, f64::min),
                    "idle_subtracted_median_per_message": median(&mut idle_subtracted.clone()),
                    "reps": measured.iter().map(|r| r.json(spec.messages)).collect::<Vec<_>>(),
                    "restart_probe": probe,
                }),
            );
        }
        Ok(result)
    })
    .await
    .expect("join")
    .expect("measurement")
}

/// Diagnostic only: restart the broker on the data it has, then run one
/// unpaced rep. Fast again on the same data means the slowdown lives in the
/// process (its memory or callgrind's), not in the logs on disk or the runner.
fn restart_probe_rep(
    cluster: &mut Cluster,
    dir: &Path,
    spec: &CellSpec,
    measured: &[Rep],
) -> Result<serde_json::Value> {
    let handle = tokio::runtime::Handle::current();
    let node = cluster.nodes[0].node_id.clone();
    handle.block_on(cluster.stop_node(&node))?;
    handle.block_on(cluster.restart_node(&node))?;
    handle.block_on(cluster.await_raft_leaders_placed(Duration::from_secs(600)))?;
    let pid = cluster.nodes[0].pid().context("restarted broker pid")?;
    let rep = measure_rep(cluster, dir, pid, spec, None, false)?;
    rep.log(&format!("{}_restarted", spec.name), 0, spec.messages);
    let first = measured.iter().find(|r| !r.paced).map_or(0.0, |r| r.secs);
    let last = measured
        .iter()
        .rev()
        .find(|r| !r.paced)
        .map_or(0.0, |r| r.secs);
    eprintln!(
        "ipm {} restart probe: {:.1}s on the restarted broker against {first:.1}s first and {last:.1}s last before it",
        spec.name, rep.secs
    );
    Ok(rep.json(spec.messages))
}

/// A TCP port nothing is listening on right now.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "needs valgrind; run by the Instructions per message workflow"]
async fn broker_instructions_per_message() {
    let out = std::env::var("FELIX_IPM_OUT").expect("set FELIX_IPM_OUT to the result file");
    let reps = env_usize("FELIX_IPM_REPS", 6);
    assert!(
        reps >= 4 && reps.is_multiple_of(2),
        "FELIX_IPM_REPS must be even and at least 4: reps alternate unpaced and paced"
    );
    // Diagnostic: after each cell, restart the broker on its data and time
    // one more rep.
    let restart_probe = std::env::var("FELIX_IPM_RESTART_PROBE").is_ok_and(|v| v == "1");
    let commit =
        std::env::var("FELIX_IPM_COMMIT").expect("set FELIX_IPM_COMMIT to the measured commit");
    let toolchain =
        std::env::var("FELIX_IPM_TOOLCHAIN").expect("set FELIX_IPM_TOOLCHAIN to `rustc --version`");
    let dir = PathBuf::from(
        std::env::var("FELIX_IPM_DIR")
            .expect("set FELIX_IPM_DIR to the callgrind output directory"),
    );
    let mut cells = serde_json::Map::new();
    // Diagnostic: FELIX_IPM_CELLS=b1 measures only the named cells.
    let only = std::env::var("FELIX_IPM_CELLS").ok();
    let wanted = |name: &str| {
        only.as_deref()
            .is_none_or(|o| o.split(',').any(|c| c == name))
    };
    for cell in CELLS.chunks(1) {
        if !wanted(cell[0].name) {
            continue;
        }
        cells.extend(measure_cells(&dir, reps, restart_probe, Vec::new(), cell).await);
    }
    if !wanted("b64_mqtt_idle") {
        let result = serde_json::json!({
            "arch": std::env::consts::ARCH, "commit": commit, "toolchain": toolchain, "cells": cells,
        });
        std::fs::write(&out, serde_json::to_string_pretty(&result).unwrap()).expect("write result");
        return;
    }
    let mqtt_env = vec![
        (
            "FELIX_MQTT_LISTEN".to_string(),
            format!("127.0.0.1:{}", free_port()),
        ),
        ("FELIX_MQTT_TLS".to_string(), "false".to_string()),
    ];
    cells.extend(measure_cells(&dir, reps, false, mqtt_env, &MQTT_IDLE_CELLS).await);
    let result = serde_json::json!({
        "arch": std::env::consts::ARCH,
        "commit": commit,
        "toolchain": toolchain,
        "cells": cells,
    });
    std::fs::write(&out, serde_json::to_string_pretty(&result).unwrap()).expect("write result");
}

/// The diagnostic ShardSleep counters a broker exports.
const SLEEP_COUNTERS: [&str; 4] = [
    "felix_diag_shard_sleep_calls_total",
    "felix_diag_shard_sleep_soon_total",
    "felix_diag_shard_sleep_yields_total",
    "felix_diag_shard_sleep_timer_total",
];

fn node_counters(cluster: &Cluster) -> Vec<Vec<f64>> {
    let handle = tokio::runtime::Handle::current();
    cluster
        .nodes
        .iter()
        .map(|node| {
            SLEEP_COUNTERS
                .iter()
                .map(|name| {
                    handle
                        .block_on(cluster.metric(&node.node_id, name))
                        .ok()
                        .flatten()
                        .unwrap_or(-1.0)
                })
                .collect()
        })
        .collect()
}

/// `perf stat` over `pids` for `secs`, if perf runs here. Its raw CSV output.
fn spawn_perf(pids: &[u32], secs: f64) -> Option<std::process::Child> {
    let pids = pids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    Command::new("perf")
        .args([
            "stat",
            "-x",
            ",",
            "-e",
            "instructions,task-clock",
            "-p",
            &pids,
            "--",
        ])
        .args(["sleep", &format!("{secs:.1}")])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .ok()
}

fn perf_output(child: Option<std::process::Child>) -> String {
    child
        .and_then(|child| child.wait_with_output().ok())
        .map(|out| String::from_utf8_lossy(&out.stderr).into_owned())
        .unwrap_or_else(|| "perf unavailable".to_string())
}

/// One native window: CPU of each broker, perf, and the sleep counters.
fn native_window(
    cluster: &Cluster,
    label: &str,
    secs: f64,
    work: impl FnOnce() -> Result<u64>,
) -> Result<serde_json::Value> {
    let pids: Vec<u32> = cluster.nodes.iter().filter_map(|n| n.pid()).collect();
    let before: Vec<ProcSample> = pids.iter().map(|&p| ProcSample::read(p)).collect();
    let counters_before = node_counters(cluster);
    let perf = spawn_perf(&pids, secs);
    let started = Instant::now();
    let acked = work()?;
    let wall = started.elapsed().as_secs_f64();
    let perf = perf_output(perf);
    let after: Vec<ProcSample> = pids.iter().map(|&p| ProcSample::read(p)).collect();
    let counters_after = node_counters(cluster);
    let cpu_pct: Vec<f64> = before
        .iter()
        .zip(&after)
        .map(|(b, a)| (a.cpu_s - b.cpu_s) / wall * 100.0)
        .collect();
    let deltas: Vec<Vec<f64>> = counters_before
        .iter()
        .zip(&counters_after)
        .map(|(b, a)| b.iter().zip(a).map(|(b, a)| a - b).collect())
        .collect();
    eprintln!(
        "native {label}: wall {wall:.1}s acked {acked} cpu% per broker {cpu_pct:.2?} sleep calls/soon/yields/timer per broker {deltas:?} totals (-1 absent) {counters_after:?}\nperf: {}",
        perf.replace('\n', " | ")
    );
    Ok(serde_json::json!({
        "label": label, "wall_s": wall, "acked": acked, "cpu_pct": cpu_pct,
        "sleep_counters": deltas, "perf": perf,
    }))
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "native light-load CPU; run by the Instructions per message workflow"]
async fn broker_light_load_cpu() {
    let out = std::env::var("FELIX_IPM_OUT").expect("set FELIX_IPM_OUT");
    let window = Duration::from_secs(env_usize("FELIX_NATIVE_WINDOW_SECS", 20) as u64);
    let mut results = Vec::new();
    for rf in [1u32, 3] {
        let stream = if rf == 1 {
            StreamSpec::new(STREAM, SHARDS)
        } else {
            StreamSpec::quorum(STREAM, SHARDS, rf)
        };
        let cluster = Cluster::start(ClusterConfig {
            nodes: rf as usize,
            streams: vec![stream],
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
        let label = format!("rf{rf}");
        let rows = tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>> {
            let cluster = cluster;
            run_ingest(&cluster, 1, 5_000, None, false)?;
            let mut rows = Vec::new();
            std::thread::sleep(Duration::from_secs(10));
            rows.push(native_window(
                &cluster,
                &format!("{label} idle"),
                window.as_secs_f64(),
                || {
                    std::thread::sleep(window);
                    Ok(0)
                },
            )?);
            for rate in [100u64, 1000] {
                let total = (rate * window.as_secs()) as usize;
                rows.push(native_window(
                    &cluster,
                    &format!("{label} paced {rate}/s"),
                    window.as_secs_f64(),
                    || Ok(run_ingest(&cluster, 1, total, Some(rate), false)?.acked),
                )?);
                std::thread::sleep(Duration::from_secs(6));
                rows.push(native_window(
                    &cluster,
                    &format!("{label} after {rate}/s, idle"),
                    window.as_secs_f64(),
                    || {
                        std::thread::sleep(window);
                        Ok(0)
                    },
                )?);
            }
            let started = Instant::now();
            let unpaced = run_ingest(&cluster, 1, 20_000, None, false)?;
            eprintln!(
                "native {label} unpaced: {:.0} msg/s in {:.1}s",
                unpaced.msg_s,
                started.elapsed().as_secs_f64()
            );
            Ok(rows)
        })
        .await
        .expect("join")
        .expect("native measurement");
        results.extend(rows);
    }
    std::fs::write(&out, serde_json::to_string_pretty(&results).unwrap()).expect("write result");
}

/// Every counter a broker exports, summed over label sets.
fn all_counters(cluster: &Cluster, node: usize) -> std::collections::BTreeMap<String, f64> {
    let addr = cluster.nodes[node].metrics_addr;
    let handle = tokio::runtime::Handle::current();
    let body = handle
        .block_on(async move {
            reqwest::get(format!("http://{addr}/metrics"))
                .await?
                .text()
                .await
        })
        .unwrap_or_default();
    let mut out = std::collections::BTreeMap::new();
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let name = line.split(['{', ' ']).next().unwrap_or("");
        if !name.ends_with("_total") && !name.ends_with("_count") && !name.ends_with("_sum") {
            continue;
        }
        if let Some(v) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            *out.entry(name.to_string()).or_insert(0.0) += v;
        }
    }
    out
}

/// What one profiled window found on one broker.
fn perf_stat_csv(text: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() > 2 {
            let value = fields[0]
                .parse::<f64>()
                .map_or(serde_json::Value::Null, serde_json::Value::from);
            out.insert(fields[2].to_string(), value);
        }
    }
    out
}

/// One cell of the native profile: load runs in the background while each
/// broker gets `perf stat` (stats window) or `perf record` (record window).
fn profile_cell(
    cluster: &Cluster,
    label: &str,
    rate: Option<u64>,
    record: bool,
    prof_dir: &Path,
) -> Result<serde_json::Value> {
    const RAMP: u64 = 3;
    const WINDOW: u64 = 10;
    let pids: Vec<u32> = cluster.nodes.iter().filter_map(|n| n.pid()).collect();
    let syscalls = std::env::var("FELIX_PERF_SYSCALLS").unwrap_or_default();
    std::thread::scope(|scope| -> Result<serde_json::Value> {
        let load =
            scope.spawn(|| run_ingest_for(cluster, 1, 0, Some(RAMP + WINDOW + 2), rate, false));
        std::thread::sleep(Duration::from_secs(RAMP));
        let counters_before: Vec<_> = (0..pids.len()).map(|i| all_counters(cluster, i)).collect();
        let procs_before: Vec<ProcSample> = pids.iter().map(|&p| ProcSample::read(p)).collect();
        let started = Instant::now();
        let children: Vec<std::process::Child> = pids
            .iter()
            .enumerate()
            .map(|(i, pid)| {
                let mut cmd = Command::new("sudo");
                cmd.args(["-n", "perf"]);
                if record {
                    let file = prof_dir.join(format!("{label}.b{i}.data").replace(['/', ' '], "_"));
                    cmd.args(["record", "-q", "-e", "cpu-clock", "-F", "999", "-g", "-o"])
                        .arg(file);
                } else {
                    let mut events =
                        "instructions,cycles,context-switches,cpu-migrations,task-clock"
                            .to_string();
                    if !syscalls.is_empty() {
                        events.push(',');
                        events.push_str(&syscalls);
                    }
                    cmd.args(["stat", "-x", ",", "-e", &events]);
                }
                cmd.args(["-p", &pid.to_string(), "--", "sleep", &WINDOW.to_string()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .context("spawn perf")
            })
            .collect::<Result<_>>()?;
        let outputs: Vec<String> = children
            .into_iter()
            .map(|c| {
                c.wait_with_output()
                    .map(|o| String::from_utf8_lossy(&o.stderr).into_owned())
                    .unwrap_or_default()
            })
            .collect();
        let wall = started.elapsed().as_secs_f64();
        let procs_after: Vec<ProcSample> = pids.iter().map(|&p| ProcSample::read(p)).collect();
        let counters_after: Vec<_> = (0..pids.len()).map(|i| all_counters(cluster, i)).collect();
        let ingest = load.join().map_err(|_| anyhow!("load thread panicked"))??;
        // Messages acked in the window: the paced rate, or the run's average.
        let messages = rate.map_or(ingest.msg_s, |r| r as f64) * wall;
        let mut brokers = Vec::new();
        for i in 0..pids.len() {
            let counters: serde_json::Map<String, serde_json::Value> = counters_after[i]
                .iter()
                .filter_map(|(k, a)| {
                    let d = a - counters_before[i].get(k).copied().unwrap_or(0.0);
                    (d != 0.0).then(|| (k.clone(), serde_json::Value::from(d / messages)))
                })
                .collect();
            let cpu = (procs_after[i].cpu_s - procs_before[i].cpu_s) / wall * 100.0;
            let vol = procs_after[i]
                .voluntary_switches
                .saturating_sub(procs_before[i].voluntary_switches) as f64;
            let invol = procs_after[i]
                .involuntary_switches
                .saturating_sub(procs_before[i].involuntary_switches)
                as f64;
            let stat = perf_stat_csv(&outputs[i]);
            let per_message: serde_json::Map<String, serde_json::Value> = stat
                .iter()
                .filter_map(|(k, v)| {
                    Some((k.clone(), serde_json::Value::from(v.as_f64()? / messages)))
                })
                .collect();
            eprintln!(
                "profile {label} broker {i}{}: {messages:.0} msgs in {wall:.1}s ({:.0} msg/s run avg), cpu {cpu:.1}%, ctx vol/invol per msg {:.2}/{:.2}\n  perf per msg {per_message:?}\n  counters per msg {counters:?}",
                if record { " (record)" } else { "" },
                ingest.msg_s,
                vol / messages,
                invol / messages,
            );
            brokers.push(serde_json::json!({
                "cpu_pct": cpu, "vol_per_msg": vol / messages, "invol_per_msg": invol / messages,
                "perf_per_msg": per_message, "perf_raw": outputs[i], "counters_per_msg": counters,
            }));
        }
        Ok(
            serde_json::json!({ "label": label, "record": record, "messages": messages, "wall_s": wall, "msg_s": ingest.msg_s, "brokers": brokers }),
        )
    })
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "native profile; run by the Instructions per message workflow"]
async fn broker_light_load_profile() {
    let out = std::env::var("FELIX_IPM_OUT").expect("set FELIX_IPM_OUT");
    let prof_dir = PathBuf::from(std::env::var("FELIX_PROF_DIR").expect("set FELIX_PROF_DIR"));
    let record = std::env::var("FELIX_PROF_RECORD").is_ok_and(|v| v == "1");
    let mut results = Vec::new();
    for rf in [1u32, 3] {
        let stream = if rf == 1 {
            StreamSpec::new(STREAM, SHARDS)
        } else {
            StreamSpec::quorum(STREAM, SHARDS, rf)
        };
        let cluster = Cluster::start(ClusterConfig {
            nodes: rf as usize,
            streams: vec![stream],
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
        let prof_dir = prof_dir.clone();
        let rows = tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>> {
            let cluster = cluster;
            run_ingest(&cluster, 1, 5_000, None, false)?;
            let mut rows = Vec::new();
            for (name, rate) in [("100", Some(100u64)), ("1000", Some(1000)), ("flat", None)] {
                let label = format!("rf{rf}_{name}");
                rows.push(profile_cell(&cluster, &label, rate, false, &prof_dir)?);
                std::thread::sleep(Duration::from_secs(3));
                if record {
                    rows.push(profile_cell(&cluster, &label, rate, true, &prof_dir)?);
                    std::thread::sleep(Duration::from_secs(3));
                }
            }
            Ok(rows)
        })
        .await
        .expect("join")
        .expect("profile");
        results.extend(rows);
    }
    std::fs::write(&out, serde_json::to_string_pretty(&results).unwrap()).expect("write result");
}

/// Reps on a runner that slows down: the same per-message and per-second
/// cost, rep durations growing as on the aarch64 runner.
fn synthetic_reps(
    per_message: f64,
    per_second: f64,
    messages: f64,
    secs: &[f64],
) -> Vec<(f64, f64)> {
    secs.iter()
        .map(|&s| (s, per_message * messages + per_second * s))
        .collect()
}

#[test]
fn the_time_fit_does_not_depend_on_how_long_the_reps_took() {
    let messages = 10_000.0;
    let fast = synthetic_reps(
        190_000.0,
        8.4e6,
        messages,
        &[24.0, 48.0, 24.5, 49.0, 25.0, 50.0],
    );
    let slow = synthetic_reps(
        190_000.0,
        8.4e6,
        messages,
        &[175.0, 350.0, 215.0, 430.0, 270.0, 540.0],
    );
    let (fast_a, fast_c, fast_residual) = fit_time_cost(&fast, messages).unwrap();
    let (slow_a, slow_c, slow_residual) = fit_time_cost(&slow, messages).unwrap();
    for (a, c, residual) in [
        (fast_a, fast_c, fast_residual),
        (slow_a, slow_c, slow_residual),
    ] {
        assert!((a - 190_000.0).abs() < 1.0, "per message {a}");
        assert!((c - 8.4e6).abs() < 1.0, "per second {c}");
        assert!(residual < 1e-6, "residual {residual}");
    }
}

#[test]
fn reps_of_one_duration_cannot_be_fitted() {
    let reps = synthetic_reps(190_000.0, 8.4e6, 10_000.0, &[50.0, 50.0, 50.0, 50.0]);
    assert!(fit_time_cost(&reps, 10_000.0).is_none());
}
