//! CPU instructions the broker retires per acknowledged message.
//!
//! `#[ignore]`: it needs Linux with valgrind, and CI's
//! `Instructions per message` workflow runs it. GitHub-hosted runners expose no
//! hardware performance counters, so the broker runs under callgrind, which
//! counts the instructions it executes and is repeatable on any machine.
//! Wall-clock throughput moves with whatever else shares the runner; the
//! instruction count for a fixed amount of work barely does, so it can pin a
//! small per-message cost to the PR that added it. `scripts/perf/ipm_gate.py`
//! compares the result with `scripts/perf/ipm-baseline.toml`.
//!
//! ```text
//! cargo build --release -p felix-broker-service --bin felix-broker -p felix-loadgen
//! FELIX_BROKER_WRAPPER="valgrind --tool=callgrind --callgrind-out-file=$DIR/cg.%p" \
//! FELIX_IPM_DIR=$DIR FELIX_IPM_OUT=ipm.json cargo test --release -p felix-cluster \
//!   --test instructions_per_message -- --ignored --nocapture
//! ```
//!
//! Only the broker process is counted, so a load generator change cannot hide
//! or fake a broker regression. The stream is durable on the suite's default
//! engine (Raft), with periodic fsync so the disk's wait stays out of the number.
//!
//! Under callgrind the broker runs hundreds of times slower than native while
//! its timers fire in real time, so work done per second is a large share of
//! what it executes. Two rules keep that share the same from rep to rep and
//! run to run:
//!
//! - Every rep gets a fresh broker (and so a fresh valgrind) on a fresh data
//!   directory. A long-lived instrumented broker gets slower rep after rep, and
//!   a slower rep picks up more per-second work.
//! - `b1` is paced at a fixed rate for a fixed time, so its per-second work is
//!   a constant that is part of the baseline. A rep that could not hold the
//!   pace is a failed measurement, not a regression.

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
/// On a paced rep that cost is about half the raw count, so the window is long
/// enough that its own noise stays small.
const IDLE_WINDOW: Duration = Duration::from_secs(20);

/// How long a paced rep publishes for.
const PACED_REP: Duration = Duration::from_secs(300);

/// How far a paced rep's issue rate and wall time may stray from the target
/// before the rep is not comparable with the baseline.
const PACE_TOLERANCE_PCT: f64 = 3.0;

/// Unpaced reps that each take longer than the previous one, by this much
/// first to last, mean state is leaking from one rep into the next.
const CLIMB_LIMIT_PCT: f64 = 10.0;

/// The `b1` pace in messages a second. The instrumented broker's capacity is
/// what bounds it: on aarch64 it executes 14 to 19M guest instructions a
/// second, and about 8.4M of those go to per-second work under load. 20 msg/s
/// at about 190k instructions each asks for about 12M a second, under the
/// slowest runner seen, and is 26% under the worst rate an unpaced rep ever
/// sustained there (27 msg/s, on an aged broker). x86_64 executes 105 to 137M
/// a second and sustained 250 to 420 msg/s; 100 msg/s asks for about 27M.
/// Changing a pace changes the figure, so the baseline records it.
fn b1_pace() -> u64 {
    if let Some(rate) = std::env::var("FELIX_IPM_B1_RATE")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        return rate;
    }
    match std::env::consts::ARCH {
        "aarch64" => 20,
        _ => 100,
    }
}

/// How a cell drives its load.
#[derive(Clone, Copy)]
enum Load {
    /// This many messages a second for [`PACED_REP`].
    Paced(u64),
    /// This many messages as fast as acks allow.
    Unpaced(usize),
}

/// A cell: name, publish batch size, load, and the warm-up messages each fresh
/// broker gets first. Warm-up is unpaced; it only has to finish.
#[derive(Clone, Copy)]
struct Cell {
    name: &'static str,
    batch: usize,
    load: Load,
    warmup: usize,
}

impl Cell {
    fn messages(&self) -> usize {
        match self.load {
            Load::Paced(rate) => (rate * PACED_REP.as_secs()) as usize,
            Load::Unpaced(messages) => messages,
        }
    }

    fn rate(&self) -> Option<u64> {
        match self.load {
            Load::Paced(rate) => Some(rate),
            Load::Unpaced(_) => None,
        }
    }
}

/// `b64` stays unpaced: its reps take about 10 s, per-second work is about a
/// tenth of its count, and its spread was already 2 to 3%. Pacing it under the
/// slowest runner would make that share several times larger.
fn cells() -> Vec<Cell> {
    vec![
        Cell {
            name: "b1",
            batch: 1,
            load: Load::Paced(b1_pace()),
            warmup: 4_000,
        },
        Cell {
            name: "b64",
            batch: 64,
            load: Load::Unpaced(80_000),
            warmup: 320_000,
        },
    ]
}

/// The same load as `b64` on a broker whose MQTT listener is on and idle. The
/// gate holds it to `b64` from the same run: a listener nobody uses must cost
/// the QUIC path nothing.
const MQTT_IDLE_CELL: Cell = Cell {
    name: "b64_mqtt_idle",
    batch: 64,
    load: Load::Unpaced(80_000),
    warmup: 320_000,
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn loadgen_binary() -> Result<PathBuf> {
    felix_cluster::built_binary("felix-loadgen").map_err(|err| anyhow!("{err}; build it first"))
}

/// Run the ingest scenario and return the loadgen's JSON row. A measured run
/// must be clean: retries and failed sends add work the baseline did not have.
/// A warm-up only has to finish.
fn run_ingest(
    cluster: &Cluster,
    batch: usize,
    total: usize,
    rate: Option<u64>,
    must_be_clean: bool,
) -> Result<serde_json::Value> {
    let node = &cluster.nodes[0];
    let mut command = Command::new(loadgen_binary()?);
    command
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
        .args(["--total", &total.to_string()]);
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
    let failed = row["failed_sends"].as_u64().unwrap_or(0);
    if must_be_clean && (failed > 0 || row["publish_retries"].as_u64().unwrap_or(0) > 0) {
        bail!(
            "the run was not clean, so its count is not comparable ({}): {row}",
            lease_counters(cluster)
        );
    }
    Ok(row)
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

/// CPU time and resident memory of the broker process (valgrind runs the
/// broker in its own process), read from /proc. Zero where /proc is missing.
#[derive(Default, Clone, Copy)]
struct ProcSample {
    cpu_s: f64,
    rss_kib: u64,
}

impl ProcSample {
    fn read(pid: u32) -> Self {
        let mut sample = Self::default();
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            // utime and stime are fields 14 and 15, the 12th and 13th after
            // the parenthesised command name.
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
            sample.rss_kib = status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
                .unwrap_or(0);
        }
        sample
    }
}

/// One measured rep.
#[derive(Clone, Debug, PartialEq)]
struct Rep {
    /// Instructions per message less the idle rate over the rep's wall time.
    user: f64,
    raw: f64,
    idle_ir_per_second: f64,
    /// Wall time of the measured window, loadgen start to exit.
    secs: f64,
    /// The rate the loadgen issued at, and for how long.
    issued_msg_s: f64,
    issue_secs: f64,
    warmup_secs: f64,
    broker_cpu_s: f64,
    rss_kib_start: u64,
    rss_kib_end: u64,
}

impl Rep {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "user_per_message": self.user,
            "raw_per_message": self.raw,
            "idle_ir_per_second": self.idle_ir_per_second,
            "seconds": self.secs,
            "issued_msg_s": self.issued_msg_s,
            "issue_seconds": self.issue_secs,
            "warmup_seconds": self.warmup_secs,
            "broker_cpu_s": self.broker_cpu_s,
            "broker_cpu_pct_of_wall": self.broker_cpu_s / self.secs * 100.0,
            "rss_kib_start": self.rss_kib_start,
            "rss_kib_end": self.rss_kib_end,
        })
    }
}

/// Why a paced rep cannot stand in for the baseline's, if it cannot: the
/// loadgen fell behind the pace, or the rep ran long. Either way its
/// per-second work is not the constant the baseline holds.
fn pace_problem(rate: u64, messages: usize, rep: &Rep) -> Option<String> {
    let want_secs = messages as f64 / rate as f64;
    let rate_off = (rep.issued_msg_s - rate as f64) / rate as f64 * 100.0;
    let secs_off = (rep.secs - want_secs) / want_secs * 100.0;
    let held = rate_off.abs() <= PACE_TOLERANCE_PCT && secs_off <= PACE_TOLERANCE_PCT;
    (!held).then(|| {
        format!(
            "issued {:.2} msg/s against {rate} ({rate_off:+.1}%) and took {:.1} s against {want_secs:.0} ({secs_off:+.1}%); tolerance {PACE_TOLERANCE_PCT}%",
            rep.issued_msg_s, rep.secs
        )
    })
}

/// Whether unpaced reps each ran longer than the one before, by more than
/// [`CLIMB_LIMIT_PCT`] first to last: the sign of a rep inheriting state.
fn climb_problem(secs: &[f64]) -> Option<String> {
    let climbing = secs.len() >= 3 && secs.windows(2).all(|w| w[1] > w[0]);
    let climb = (secs.last()? / secs.first()? - 1.0) * 100.0;
    (climbing && climb > CLIMB_LIMIT_PCT).then(|| {
        format!(
            "every rep ran longer than the last ({climb:+.1}% first to last): a rep is inheriting the previous one's state"
        )
    })
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// Highest over lowest, as a percentage over the lowest.
fn spread_pct(values: &[f64]) -> f64 {
    let lo = values.iter().cloned().fold(f64::MAX, f64::min);
    let hi = values.iter().cloned().fold(f64::MIN, f64::max);
    (hi - lo) / lo * 100.0
}

/// A cell's JSON entry and its measurement failures, from its reps.
fn summarize(cell: &Cell, reps: &[Rep]) -> (serde_json::Value, Vec<String>) {
    let user: Vec<f64> = reps.iter().map(|r| r.user).collect();
    let raw: Vec<f64> = reps.iter().map(|r| r.raw).collect();
    let secs: Vec<f64> = reps.iter().map(|r| r.secs).collect();
    let mut problems = Vec::new();
    match cell.rate() {
        Some(rate) => {
            for (i, rep) in reps.iter().enumerate() {
                if let Some(problem) = pace_problem(rate, cell.messages(), rep) {
                    problems.push(format!("{} rep {i}: {problem}", cell.name));
                }
            }
        }
        None => {
            if let Some(problem) = climb_problem(&secs) {
                problems.push(format!("{}: {problem}", cell.name));
            }
        }
    }
    let entry = serde_json::json!({
        "batch": cell.batch,
        "messages": cell.messages(),
        "pace_msg_s": cell.rate(),
        "warmup_messages": cell.warmup,
        // Each rep runs on a fresh broker, so the reps are samples of one
        // thing and the median is the steadiest figure.
        "user_per_message": median(&user),
        "user_min_per_message": user.iter().cloned().fold(f64::MAX, f64::min),
        "user_spread_pct": spread_pct(&user),
        "raw_median_per_message": median(&raw),
        "raw_spread_pct": spread_pct(&raw),
        "median_seconds": median(&secs),
        "reps": reps.iter().map(Rep::json).collect::<Vec<_>>(),
    });
    (entry, problems)
}

/// Measure one rep of `cell` on a broker of its own, started with `env` added
/// to its environment and stopped after.
async fn measure_rep(dir: &Path, env: &[(String, String)], cell: Cell, rep: usize) -> Result<Rep> {
    // The broker keeps its logs under the node's data directory, as Raft
    // requires. Periodic fsync writes every append but leaves the flush to a
    // timer, so the count is the broker's CPU and not time spent waiting on
    // the disk; the timer's idle cost comes off like any other background work.
    let mut broker_env = vec![
        (
            "FELIX_DURABLE_FSYNC_MODE".to_string(),
            "periodic".to_string(),
        ),
        // Under callgrind an unpaced publish can wait past the default 5 s for
        // its commit and fail, which makes the run unclean. Nothing is lost;
        // the default is sized for native speed.
        (
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "60000".to_string(),
        ),
    ];
    broker_env.extend(env.iter().cloned());
    let started = Instant::now();
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
    .context("start cluster")?;
    let start_secs = started.elapsed().as_secs_f64();
    let pid = cluster.nodes[0].pid().context("broker pid")?;
    let dir = dir.to_path_buf();
    let name = cell.name;

    // The loadgen and callgrind calls block, and the in-process control plane
    // has to keep serving heartbeats meanwhile. The cluster is dropped in
    // there too, which stops the broker before the next rep starts.
    tokio::task::spawn_blocking(move || -> Result<Rep> {
        let dir = dir.as_path();
        let warm_started = Instant::now();
        let warm = run_ingest(&cluster, cell.batch, cell.warmup, None, false)?;
        let warm_acked = warm["acked_records"].as_u64().unwrap_or(0) as usize;
        if warm_acked < cell.warmup * 99 / 100 {
            bail!(
                "cell {name} rep {rep}: warm-up acked {warm_acked} of {}",
                cell.warmup
            );
        }
        let warmup_secs = warm_started.elapsed().as_secs_f64();
        // What the idle broker spends per second (timers, polling), so it can
        // be taken off the rep in proportion to the time it ran.
        let idle_start = Instant::now();
        let (idle_ir, ()) = count_instructions(dir, pid, || {
            std::thread::sleep(IDLE_WINDOW);
            Ok(())
        })?;
        let idle_rate = idle_ir as f64 / idle_start.elapsed().as_secs_f64();

        let total = cell.messages();
        let before = ProcSample::read(pid);
        let rep_started = Instant::now();
        let (ir, row) = count_instructions(dir, pid, || {
            run_ingest(&cluster, cell.batch, total, cell.rate(), true)
        })?;
        let secs = rep_started.elapsed().as_secs_f64();
        let after = ProcSample::read(pid);
        let acked = row["acked_records"].as_u64().unwrap_or(0);
        if acked != total as u64 {
            bail!("cell {name} rep {rep}: acked {acked} of {total}");
        }
        let net = ir as f64 - idle_rate * secs;
        if net.is_nan() || net <= 0.0 {
            bail!(
                "cell {name} rep {rep}: {ir} instructions less idle {:.0} is not positive",
                idle_rate * secs
            );
        }
        let measured = Rep {
            user: net / acked as f64,
            raw: ir as f64 / acked as f64,
            idle_ir_per_second: idle_rate,
            secs,
            issued_msg_s: row["throughput_msg_s"].as_f64().unwrap_or(0.0),
            issue_secs: row["duration_s"].as_f64().unwrap_or(0.0),
            warmup_secs,
            broker_cpu_s: after.cpu_s - before.cpu_s,
            rss_kib_start: before.rss_kib,
            rss_kib_end: after.rss_kib,
        };
        eprintln!(
            "ipm {name} rep {rep}: {:.1} instructions per message ({:.1} before taking off {:.0}/s idle over {secs:.1} s); issued {:.2} msg/s; broker cpu {:.1} s ({:.0}% of wall, {:.2}M instructions per cpu second); rss {} -> {} MiB; start {start_secs:.0} s, warm-up {warmup_secs:.0} s",
            measured.user,
            measured.raw,
            idle_rate,
            measured.issued_msg_s,
            measured.broker_cpu_s,
            measured.broker_cpu_s / secs * 100.0,
            ir as f64 / measured.broker_cpu_s.max(1e-9) / 1e6,
            before.rss_kib / 1024,
            after.rss_kib / 1024,
        );
        drop(cluster);
        Ok(measured)
    })
    .await
    .context("join")?
}

/// Measure `cell` over `reps` fresh brokers.
async fn measure_cell(
    dir: &Path,
    reps: usize,
    env: &[(String, String)],
    cell: Cell,
) -> Result<(serde_json::Value, Vec<String>)> {
    let mut measured = Vec::with_capacity(reps);
    for rep in 0..reps {
        measured.push(measure_rep(dir, env, cell, rep).await?);
    }
    Ok(summarize(&cell, &measured))
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
    let reps = env_usize("FELIX_IPM_REPS", 5);
    assert!(reps > 0, "FELIX_IPM_REPS must be at least 1");
    let commit =
        std::env::var("FELIX_IPM_COMMIT").expect("set FELIX_IPM_COMMIT to the measured commit");
    let toolchain =
        std::env::var("FELIX_IPM_TOOLCHAIN").expect("set FELIX_IPM_TOOLCHAIN to `rustc --version`");
    let dir = PathBuf::from(
        std::env::var("FELIX_IPM_DIR")
            .expect("set FELIX_IPM_DIR to the callgrind output directory"),
    );
    let mqtt_env = vec![
        (
            "FELIX_MQTT_LISTEN".to_string(),
            format!("127.0.0.1:{}", free_port()),
        ),
        ("FELIX_MQTT_TLS".to_string(), "false".to_string()),
    ];
    let mut plan: Vec<(Cell, Vec<(String, String)>)> =
        cells().into_iter().map(|cell| (cell, Vec::new())).collect();
    plan.push((MQTT_IDLE_CELL, mqtt_env));

    let mut cells = serde_json::Map::new();
    let mut problems = Vec::new();
    for (cell, env) in &plan {
        let (entry, cell_problems) = measure_cell(&dir, reps, env, *cell)
            .await
            .expect("measurement");
        cells.insert(cell.name.to_string(), entry);
        problems.extend(cell_problems);
    }
    let result = serde_json::json!({
        "arch": std::env::consts::ARCH,
        "commit": commit,
        "toolchain": toolchain,
        "cells": cells,
        "measurement_failures": problems,
    });
    std::fs::write(&out, serde_json::to_string_pretty(&result).unwrap()).expect("write result");
    // Written first, so the reps that did not hold are in the artifact.
    assert!(
        problems.is_empty(),
        "the measurement failed, so it says nothing about a regression:\n{}",
        problems.join("\n")
    );
}

fn rep(user: f64, secs: f64, issued_msg_s: f64) -> Rep {
    Rep {
        user,
        raw: user,
        idle_ir_per_second: 0.0,
        secs,
        issued_msg_s,
        issue_secs: secs,
        warmup_secs: 0.0,
        broker_cpu_s: 0.0,
        rss_kib_start: 0,
        rss_kib_end: 0,
    }
}

#[test]
fn a_held_pace_is_comparable() {
    assert_eq!(pace_problem(20, 6000, &rep(1.0, 301.5, 19.9)), None);
}

#[test]
fn a_paced_rep_that_fell_behind_is_a_failed_measurement() {
    // Issued slower than asked.
    assert!(pace_problem(20, 6000, &rep(1.0, 301.0, 18.0)).is_some());
    // Issued on pace, but the acks drained long after.
    assert!(pace_problem(20, 6000, &rep(1.0, 330.0, 20.0)).is_some());
}

#[test]
fn a_cell_is_reported_by_its_median_rep() {
    let cell = Cell {
        name: "b1",
        batch: 1,
        load: Load::Paced(20),
        warmup: 0,
    };
    let reps: Vec<Rep> = [5.0, 1.0, 3.0, 4.0, 2.0]
        .into_iter()
        .map(|user| rep(user, 300.0, 20.0))
        .collect();
    let (entry, problems) = summarize(&cell, &reps);
    assert!(problems.is_empty(), "{problems:?}");
    assert_eq!(entry["user_per_message"], 3.0);
    assert_eq!(entry["user_min_per_message"], 1.0);
    assert_eq!(entry["user_spread_pct"], 400.0);
}

#[test]
fn unpaced_reps_that_climb_are_a_failed_measurement() {
    assert!(climb_problem(&[10.0, 11.0, 12.0, 13.0]).is_some());
    // Slower overall but not one after another, or climbing by little.
    assert_eq!(climb_problem(&[12.0, 10.0, 13.0, 11.0]), None);
    assert_eq!(climb_problem(&[10.0, 10.1, 10.2, 10.3]), None);
}
