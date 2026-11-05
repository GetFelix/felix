//! One seed, start to finish.
//!
//! [`run_seed`] builds a cluster, runs clients and a nemesis against it on
//! virtual time, heals everything, takes the final reads and checks.
//!
//! ```text
//!   0 s            startup: catalog, brokers, election features, every shard served
//!   0 ......... 90 s   faults (SimConfig::fault_window) and clients
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
//! cost nothing. The run is not bit-for-bit repeatable: the brokers iterate
//! hash maps whose order varies by process, real file I/O finishes in real
//! time, and tokio's `select!` picks among ready branches at random. A seed
//! that fails usually fails again, and the same faults land at the same
//! virtual times, but the interleaving beneath them differs, so a replay may
//! pass. Run it a few times before concluding a failure was a fluke.

use std::time::Duration;

use felix_cluster::history::checker::{self, Report};
use felix_cluster::history::model::History;
use felix_cluster::history::rng::Rng;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::check::{CheckContext, Checker, ElectionSettles, Finding, Point};
use crate::client::{Recorder, SimClient};
use crate::cluster::{SimCluster, SimConfig};
use crate::nemesis::{Nemesis, Plan};

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
    pub fn passed(&self) -> bool {
        self.report.violations.is_empty() && self.findings.is_empty()
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
        if self.report.violations.is_empty() {
            writeln!(f, "history checker: no violations")
        } else {
            write!(f, "{}", self.report)
        }
    }
}

/// Run `seed` with the checkers that run by default.
pub fn run_seed(seed: u64, config: SimConfig) -> SimOutcome {
    run_seed_with(seed, config, vec![Box::new(ElectionSettles::default())])
}

/// Run `seed` with `checkers`, which see the cluster at the points named in
/// [`crate::check`]. The history checker always runs.
pub fn run_seed_with(seed: u64, config: SimConfig, checkers: Vec<Box<dyn Checker>>) -> SimOutcome {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("build the simulation runtime");
    runtime.block_on(run(seed, config, checkers))
}

async fn run(seed: u64, config: SimConfig, mut checkers: Vec<Box<dyn Checker>>) -> SimOutcome {
    let begun = Instant::now();
    let cluster = match SimCluster::start(seed, config.clone()).await {
        Ok(cluster) => cluster,
        Err(err) => {
            return failed_start(seed, config, format!("{err:#}"), begun.elapsed());
        }
    };
    let recorder = Recorder::new(config.shards);
    let stop = CancellationToken::new();
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
    if config.faults {
        let plan = Plan::generate(seed, config.brokers, config.fault_window);
        for step in &plan.steps {
            tracing::debug!(at = ?step.at, fault = %step.fault, "planned");
        }
        let nemesis = nemesis.clone();
        tokio::spawn(async move { nemesis.run(&plan).await });
    }
    tokio::time::sleep_until(run_start + config.fault_window).await;
    nemesis.heal().await;
    let healed_at = cluster.elapsed();

    let mut waiting = Vec::new();
    for mut checker in std::mem::take(&mut checkers) {
        let ctx = CheckContext {
            cluster: cluster.clone(),
            point: Point::Healed,
            healed_at,
            history: History::default(),
        };
        waiting.push(tokio::spawn(async move {
            let found = checker.check(&ctx).await;
            (checker, found)
        }));
    }

    tokio::time::sleep_until(run_start + config.run_for).await;
    stop.cancel();
    for client in clients {
        let _ = client.await;
    }
    let mut findings = Vec::new();
    for task in waiting {
        if let Ok((checker, found)) = task.await {
            findings.extend(found);
            checkers.push(checker);
        }
    }
    tokio::time::sleep(DRAIN).await;

    let finisher_id = config.clients + READERS;
    let mut finisher = SimClient::new(finisher_id, cluster.clone(), recorder.clone(), rng.fork());
    for shard in 0..config.shards {
        for _ in 0..3 {
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
    let ctx = CheckContext {
        cluster: cluster.clone(),
        point: Point::End,
        healed_at,
        history: history.clone(),
    };
    for checker in &mut checkers {
        findings.extend(checker.check(&ctx).await);
    }
    if !unread.is_empty() {
        findings.push(Finding::new(
            "final-reads",
            format!(
                "no final read for {} shards, left out of the history check: {}",
                unread.len(),
                unread.join(", ")
            ),
        ));
    }
    let report = checker::check(&history);
    let summary = history.summary();
    SimOutcome {
        seed,
        config,
        report,
        findings,
        history,
        summary,
        elapsed: begun.elapsed(),
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
