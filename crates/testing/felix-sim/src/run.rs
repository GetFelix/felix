//! One seed, start to finish.
//!
//! [`run_seed`] builds a cluster, runs clients and a nemesis against it on
//! virtual time, heals everything, takes the final reads and checks.
//!
//! ```text
//!   0 s            startup: catalog, brokers, election features, every shard served
//!   0 ......... 90 s   faults (SimConfig::fault_window) and clients;
//!                      Quiet checkers sample once a second throughout
//!  90 s              everything heals; Healed checkers start
//!  90 ........ 120 s  quiet period; clients keep going (SimConfig::run_for)
//! 120 s              clients stop; wait for in-flight work
//!                    final reads of every shard and every cache key
//!                    End checkers, then the history checker
//! ```
//!
//! # Determinism
//!
//! The seed fixes the cluster's shape, the fault schedule, every client's
//! choices and the network's drops. Time is tokio's paused clock, so sleeps
//! cost nothing. Blocking work runs on this runtime rather than on threads
//! (`felix_common::blocking`), and election jitter is drawn from the seed
//! (`felix_common::jitter`). Built with `--cfg tokio_unstable`, `select!` is
//! seeded too; without it, it picks among ready branches at random, so the
//! interleaving beneath the same faults can differ and a replay may pass.
//! See `docs/simulation.md`.

use std::sync::Arc;
use std::time::Duration;

use felix_cluster::history::checker::{self, Report};
use felix_cluster::history::model::History;
use felix_cluster::history::rng::Rng;
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::check::{CheckContext, Checker, Finding, Point, Severity, default_checkers};
use crate::client::{Recorder, SimClient};
use crate::cluster::{SimCluster, SimConfig};
use crate::nemesis::{Nemesis, Plan};

/// How often the checkers are called at [`Point::Quiet`].
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
/// How long to wait after the clients stop for work the clients gave up on to
/// finish, longer than any quorum wait.
const DRAIN: Duration = Duration::from_secs(20);
/// Readers that run beside the writers.
const READERS: usize = 2;

/// What a run found.
pub struct SimOutcome {
    pub seed: u64,
    pub config: SimConfig,
    /// What the history checker found in what the clients saw.
    pub report: Report,
    /// What the other checkers found in the cluster.
    pub findings: Vec<Finding>,
    pub history: History,
    /// One line of counts, from [`History::summary`].
    pub summary: String,
    /// Virtual time the whole run took.
    pub elapsed: Duration,
}

impl SimOutcome {
    /// Whether nothing failed. A cache key the search could not decide is not
    /// a pass: nothing was proven for it.
    pub fn passed(&self) -> bool {
        self.report.violations.is_empty()
            && self.report.undecided.is_empty()
            && self.failures().next().is_none()
    }

    /// The findings that fail the run, without the warnings.
    pub fn failures(&self) -> impl Iterator<Item = &Finding> {
        self.findings
            .iter()
            .filter(|finding| finding.severity == Severity::Failure)
    }

    /// The command that runs this seed again.
    pub fn replay_command(&self) -> String {
        format!(
            "FELIX_SIM_SEED={} cargo test -p felix-sim --test seeds replay -- --nocapture",
            self.seed
        )
    }
}

impl std::fmt::Display for SimOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "seed {}: {} brokers, {} shards, {:?} virtual",
            self.seed, self.config.brokers, self.config.shards, self.elapsed
        )?;
        writeln!(f, "{}", self.summary)?;
        for finding in &self.findings {
            write!(f, "{finding}")?;
        }
        if !self.passed() {
            writeln!(f, "faults, in virtual time from the start of the run:")?;
            for event in &self.history.faults {
                writeln!(f, "  {:>7.1}s {}", event.at as f64 / 1e9, event.what)?;
            }
        }
        if self.report.violations.is_empty() {
            writeln!(f, "history checker: no violations")?;
            for note in &self.report.undecided {
                writeln!(
                    f,
                    "history checker: undecided, which fails the seed: {note}"
                )?;
            }
            Ok(())
        } else {
            write!(f, "{}", self.report)
        }
    }
}

/// Run `seed` with the checkers that run by default.
pub fn run_seed(seed: u64, config: SimConfig) -> SimOutcome {
    run_seed_with(seed, config, default_checkers())
}

/// Run `seed` with `checkers`, which see the cluster at the points named in
/// [`crate::check`]. The history checker always runs.
pub fn run_seed_with(seed: u64, config: SimConfig, checkers: Vec<Box<dyn Checker>>) -> SimOutcome {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    // `select!` picks among ready branches at random. Seeded, it picks the
    // same way every run; tokio offers the seed only under `tokio_unstable`.
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    let runtime = builder.build().expect("build the simulation runtime");
    runtime.block_on(run(seed, config, checkers))
}

async fn run(seed: u64, config: SimConfig, checkers: Vec<Box<dyn Checker>>) -> SimOutcome {
    let begun = Instant::now();
    let cluster = match SimCluster::start(seed, config.clone()).await {
        Ok(cluster) => cluster,
        Err(err) => {
            return failed_start(seed, config, format!("{err:#}"), begun.elapsed());
        }
    };
    let stalled = cluster.stalled().clone();
    let recorder = Recorder::new(config.shards);
    tokio::select! {
        outcome = drive(seed, config.clone(), cluster.clone(), recorder.clone(), checkers) => outcome,
        () = stalled.cancelled() => {
            // Whatever the clients saw is checked as far as it can be without
            // final reads: the fault timeline and what was acknowledged.
            let mut history = recorder.history();
            drop_unread_lists(&mut history);
            let mut outcome = finished(seed, config.clone(), history, Vec::new(), begun.elapsed());
            let finding = Finding::new(
                    "stalled",
                    format!(
                        "the run stalled at {:?} of virtual time, within its {:?} of real time: \
                         some task keeps the runtime busy without waiting on a timer, so the \
                         clock cannot move. No census is taken, \
                         since the busy task would hold it up too. `sample <pid>` on macOS or \
                         `perf top` shows what spins",
                        cluster.elapsed(),
                        config.real_time_limit,
                    ),
                );
            outcome.findings = vec![finding];
            outcome
        }
    }
}

async fn drive(
    seed: u64,
    config: SimConfig,
    cluster: Arc<SimCluster>,
    recorder: Arc<Recorder>,
    checkers: Vec<Box<dyn Checker>>,
) -> SimOutcome {
    let begun = Instant::now();
    let stop = CancellationToken::new();
    tokio::spawn({
        let cluster = cluster.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                tracing::info!(target: "felix_sim::run", at = ?cluster.elapsed(), "virtual time");
            }
        }
    });
    let mut rng = Rng::new(seed);

    let mut clients = Vec::new();
    for id in 0..config.clients {
        let client = SimClient::new(id, cluster.clone(), recorder.clone(), rng.fork());
        clients.push(tokio::spawn(client.run(stop.clone())));
    }
    for id in 0..READERS {
        let client = SimClient::new(
            config.clients + id,
            cluster.clone(),
            recorder.clone(),
            rng.fork(),
        );
        clients.push(tokio::spawn(client.run_reader(stop.clone())));
    }

    let run_start = Instant::now();
    let nemesis = Nemesis::new(cluster.clone(), recorder.clone());
    let plan = if config.faults {
        Plan::generate(seed, config.brokers, config.fault_window)
    } else {
        Plan::default()
    };
    if config.faults {
        for step in &plan.steps {
            tracing::debug!(at = ?step.at, fault = %step.fault, "planned");
        }
        let nemesis = nemesis.clone();
        let plan = plan.clone();
        tokio::spawn(async move { nemesis.run(&plan).await });
    }
    let context = {
        let (cluster, plan) = (cluster.clone(), plan.clone());
        move |point, healed_at, history| CheckContext {
            cluster: cluster.clone(),
            point,
            healed_at,
            history,
            plan: plan.clone(),
        }
    };

    // Each checker is behind its own lock, so one that waits at the heal does
    // not hold the others back, and a sample finds a busy one and skips it.
    let checkers: Vec<_> = checkers
        .into_iter()
        .map(|checker| Arc::new(tokio::sync::Mutex::new(checker)))
        .collect();
    let sampled = Arc::new(Mutex::new(Vec::new()));
    let sampler = tokio::spawn({
        let (checkers, sampled, stop) = (checkers.clone(), sampled.clone(), stop.clone());
        let (context, recorder) = (context.clone(), recorder.clone());
        async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(SAMPLE_EVERY) => {}
                }
                let ctx = context(Point::Quiet, Duration::ZERO, recorder.history());
                for checker in &checkers {
                    if let Ok(mut checker) = checker.try_lock() {
                        let found = checker.check(&ctx).await;
                        sampled.lock().extend(found);
                    }
                }
            }
        }
    });

    tokio::time::sleep_until(run_start + config.fault_window).await;
    nemesis.heal().await;
    let healed_at = cluster.elapsed();

    let mut waiting = Vec::new();
    for checker in &checkers {
        let checker = checker.clone();
        let ctx = context(Point::Healed, healed_at, recorder.history());
        waiting.push(tokio::spawn(async move {
            checker.lock().await.check(&ctx).await
        }));
    }

    tokio::time::sleep_until(run_start + config.run_for).await;
    stop.cancel();
    for client in clients {
        let _ = client.await;
    }
    let _ = sampler.await;
    let mut findings = std::mem::take(&mut *sampled.lock());
    for task in waiting {
        if let Ok(found) = task.await {
            findings.extend(found);
        }
    }
    tokio::time::sleep(DRAIN).await;

    let finisher_id = config.clients + READERS;
    let mut finisher = SimClient::new(finisher_id, cluster.clone(), recorder.clone(), rng.fork());
    for shard in 0..config.shards {
        // A shard nobody serves cannot be read; the election check has
        // already said so.
        if cluster
            .servers_of(&crate::cluster::stream_key(shard))
            .await
            .is_empty()
        {
            continue;
        }
        for _ in 0..2 {
            if let Some(read) = finisher.read_shard(shard).await {
                recorder.set_final_read(shard, read);
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    finisher.get_every_key().await;

    let mut history = recorder.history();
    let unread = drop_unread_lists(&mut history);
    let ctx = context(Point::End, healed_at, history.clone());
    for checker in &checkers {
        findings.extend(checker.lock().await.check(&ctx).await);
    }
    if !unread.is_empty() {
        let mut finding = Finding::new(
            "final-reads",
            format!(
                "no final read for {} shards, left out of the history check: {}",
                unread.len(),
                unread.join(", ")
            ),
        );
        for list in unread.iter().take(3) {
            let shard = list.rsplit('/').next().and_then(|n| n.parse().ok());
            if let Some(shard) = shard {
                finding = finding.with(cluster.census(&crate::cluster::stream_key(shard)).await);
            }
        }
        findings.push(finding);
    }
    finished(seed, config, history, findings, begun.elapsed())
}

fn finished(
    seed: u64,
    config: SimConfig,
    history: History,
    findings: Vec<Finding>,
    elapsed: Duration,
) -> SimOutcome {
    SimOutcome {
        seed,
        config,
        report: checker::check(&history),
        findings,
        summary: history.summary(),
        history,
        elapsed,
    }
}

/// The checker's lost-write rule needs a final read of each list. A shard
/// that could not be read leaves the history; its name is returned.
fn drop_unread_lists(history: &mut History) -> Vec<String> {
    let unread: Vec<String> = history
        .lists
        .keys()
        .filter(|list| !history.final_reads.contains_key(*list))
        .cloned()
        .collect();
    for list in &unread {
        history.lists.remove(list);
    }
    history
        .ops
        .retain(|op| !unread.iter().any(|list| list == op.list()));
    unread
}

fn failed_start(seed: u64, config: SimConfig, error: String, elapsed: Duration) -> SimOutcome {
    let history = History::default();
    SimOutcome {
        seed,
        config,
        report: checker::check(&history),
        findings: vec![Finding::new("startup", "the cluster did not come up").with(error)],
        summary: history.summary(),
        history,
        elapsed,
    }
}

#[cfg(test)]
mod tests;
