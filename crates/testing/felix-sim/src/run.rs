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
//! (`felix_common::blocking`). Retry jitter, ids and the hash keys of every
//! map the brokers and the control plane keep are drawn from the seed
//! (`felix_common::jitter`, `felix_common::random`), wall time is virtual
//! (`felix_common::clock::system_time`), and data directories get the same
//! path every run. Built with `--cfg tokio_unstable`, `select!` is seeded too,
//! and then a seed replays exactly: `each_seed_runs_the_same_twice` in
//! `tests/seeds.rs` checks it.
//! See `docs/simulation.md`.

use std::collections::BTreeSet;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use felix_cluster::history::checker::{self, Report};
use felix_cluster::history::model::History;
use felix_cluster::history::rng::Rng;
use felix_common::fleet;
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::check::{CheckContext, Checker, Finding, Point, Severity, default_checkers};
use crate::client::{Recorder, SimClient};
use crate::cluster::{SimCluster, SimConfig};
use crate::nemesis::clocks::ClockSkew;
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

    /// The run as one line per event, for comparing two runs of a seed: the
    /// faults, every operation in completion order, the final reads, what
    /// the checkers found and the virtual time the run took.
    pub fn trace(&self) -> Vec<String> {
        let history = &self.history;
        let mut lines = Vec::new();
        lines.extend(history.faults.iter().map(|e| format!("fault {e:?}")));
        lines.extend(history.ops.iter().map(|op| format!("op {op:?}")));
        lines.extend(
            history
                .registers
                .iter()
                .map(|op| format!("register {op:?}")),
        );
        lines.extend(
            history
                .commit_reads
                .iter()
                .map(|r| format!("commit read {r:?}")),
        );
        lines.extend(
            history
                .subscriptions
                .iter()
                .map(|s| format!("subscription {s:?}")),
        );
        lines.extend(
            history
                .final_reads
                .iter()
                .map(|r| format!("final read {r:?}")),
        );
        lines.extend(history.opens.iter().map(|o| format!("opened {o:?}")));
        lines.extend(self.findings.iter().map(|f| format!("finding {f:?}")));
        lines.push(format!("elapsed {:?}", self.elapsed));
        lines
    }

    /// The command that runs this seed again, event for event. It goes
    /// through `task sim:replay`, which builds with `--cfg tokio_unstable`:
    /// a plain `cargo test` build leaves `select!` unseeded.
    pub fn replay_command(&self) -> String {
        let placement = if self.config.placement_faults {
            "FELIX_SIM_PLACEMENT=1 "
        } else {
            ""
        };
        let relaxed = self.config.fsync_mode != felix_storage::log::FsyncMode::OnCommit;
        let power = match (self.config.power_loss, relaxed) {
            (false, _) => "",
            (true, false) => "FELIX_SIM_POWER_LOSS=1 ",
            (true, true) => "FELIX_SIM_POWER_LOSS=relaxed ",
        };
        let power = format!(
            "{power}{}{}",
            if self.config.majority_cut {
                "FELIX_SIM_MAJORITY_CUT=1 "
            } else {
                ""
            },
            if self.config.wipes {
                "FELIX_SIM_WIPE=1 "
            } else {
                ""
            }
        );
        match self.config.clocks {
            ClockSkew::Off => format!(
                "{placement}{power}FELIX_SIM_SEED={} task sim:replay",
                self.seed
            ),
            skew => format!(
                "{placement}{power}FELIX_SIM_SEED={} FELIX_SIM_CLOCKS={} task sim:replay",
                self.seed,
                skew.name()
            ),
        }
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

/// Where two traces from [`SimOutcome::trace`] first differ, or `None` when
/// they are the same.
pub fn first_difference(a: &[String], b: &[String]) -> Option<String> {
    let at = a
        .iter()
        .zip(b)
        .position(|(a, b)| a != b)
        .or((a.len() != b.len()).then(|| a.len().min(b.len())))?;
    let missing = "<the trace ends here>".to_string();
    Some(format!(
        "the traces first differ at event {at} of {} and {}:\n  first:  {}\n  second: {}",
        a.len(),
        b.len(),
        a.get(at).unwrap_or(&missing),
        b.get(at).unwrap_or(&missing),
    ))
}

/// Run `seed` with the checkers that run by default.
pub fn run_seed(seed: u64, config: SimConfig) -> SimOutcome {
    run_seed_with(seed, config, default_checkers())
}

/// Run `seed` with `checkers`, which see the cluster at the points named in
/// [`crate::check`]. The history checker always runs.
///
/// # Panics
///
/// If `config` skews clocks in a build without `--cfg tokio_unstable`: only
/// the runtime's task hooks can tell whose clock a spawned task reads.
pub fn run_seed_with(seed: u64, config: SimConfig, checkers: Vec<Box<dyn Checker>>) -> SimOutcome {
    assert!(
        !config.skews_clocks() || cfg!(tokio_unstable),
        "clock skew needs a build with RUSTFLAGS=\"--cfg tokio_unstable\"; run it through \
         task sim:replay"
    );
    // The stall watch ends a run only if the runtime polls again. Work that
    // holds the thread and never returns to the runtime would hang the test
    // binary instead, so past a hard bound the process exits naming the
    // seed. Declared before the runtime, so it also covers the runtime's drop.
    let limit = config.real_time_limit + config.real_time_limit / 4;
    let _bound = Deadline::start(limit, move || {
        // Output that nobody drains blocks every writer, this message too,
        // so the exit does not wait on it.
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(10));
            std::process::exit(1);
        });
        eprintln!(
            "seed {seed} ran past {limit:?} of real time: something holds the simulation \
             thread and never returns to the runtime, so the stall watch cannot end the run. \
             To run it again:\n    FELIX_SIM_SEED={seed} task sim:replay"
        );
        std::process::exit(1);
    });
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    // `select!` picks among ready branches at random. Seeded, it picks the
    // same way every run; tokio offers the seed only under `tokio_unstable`.
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    #[cfg(tokio_unstable)]
    crate::clock::attribute_tasks(&mut builder);
    let runtime = builder.build().expect("build the simulation runtime");
    runtime.block_on(run(seed, config, checkers))
}

/// Calls `expired` on a thread of its own unless it is dropped within `limit`
/// of real time.
struct Deadline {
    _finished: mpsc::Sender<()>,
}

impl Deadline {
    fn start(limit: Duration, expired: impl FnOnce() + Send + 'static) -> Self {
        let (finished, wait) = mpsc::channel::<()>();
        std::thread::Builder::new()
            .name("sim-deadline".into())
            .spawn(move || {
                // A dropped sender disconnects the channel and ends the wait.
                if let Err(mpsc::RecvTimeoutError::Timeout) = wait.recv_timeout(limit) {
                    expired();
                }
            })
            .expect("start the run's deadline");
        Self {
            _finished: finished,
        }
    }
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
            drop_unread_lists(&mut history, &BTreeSet::new());
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
    if let Some(at) = config
        .finalize_record_generations
        .filter(|at| !at.is_zero())
    {
        let (cluster, recorder) = (cluster.clone(), recorder.clone());
        tokio::spawn(async move {
            tokio::time::sleep(at).await;
            match cluster
                .cp()
                .finalize_feature(fleet::RECORD_GENERATIONS.name())
                .await
            {
                Ok(_) => recorder.fault("finalize record_generations".to_string()),
                Err(err) => recorder.fault(format!("finalize record_generations failed: {err:#}")),
            }
        });
    }
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
    let plan = match (config.faults, config.placement_faults) {
        (false, _) => Plan::default(),
        (true, false) => {
            Plan::generate(seed, config.brokers, config.fault_window, config.power_loss)
        }
        (true, true) => {
            Plan::generate(seed, config.brokers, config.fault_window, config.power_loss)
                .with_placement(seed, config.brokers, config.fault_window)
        }
    };
    let plan = if config.majority_cut || config.wipes {
        plan.with_disk_faults(
            seed,
            config.brokers,
            config.fault_window,
            config.majority_cut,
            config.wipes,
        )
    } else {
        plan
    };
    if config.faults {
        for step in &plan.steps {
            tracing::debug!(at = ?step.at, fault = %step.fault, "planned");
        }
        let nemesis = nemesis.clone();
        let plan = plan.clone();
        tokio::spawn(async move { nemesis.run(&plan).await });
    }
    if config.skews_clocks() {
        let clocks = cluster.clock_plan.clone();
        for line in clocks.describe_initial() {
            recorder.fault(line);
        }
        let nemesis = nemesis.clone();
        tokio::spawn(async move { nemesis.run_clocks(&clocks).await });
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
        let (context, recorder, cluster) = (context.clone(), recorder.clone(), cluster.clone());
        async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(SAMPLE_EVERY) => {}
                }
                // Which copies a leader could vouch, for the hold rule.
                cluster.note_state().await;
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

    let held = cluster.held_shards().await;
    let mut history = recorder.history();
    let abandoned: Vec<String> = recorder.abandoned_lists().into_keys().collect();
    if !abandoned.is_empty() {
        findings.push(Finding::warning(
            "abandoned",
            format!(
                "an operator abandoned the logs of {} shards, which loses what they acknowledged by design; operations started before each abandon are not held to it: {}",
                abandoned.len(),
                abandoned.join(", ")
            ),
        ));
    }
    let unread = drop_unread_lists(&mut history, &held);
    let ctx = context(Point::End, healed_at, history.clone());
    for checker in &checkers {
        findings.extend(checker.lock().await.check(&ctx).await);
    }
    // A shard held on purpose cannot be read, so it is reported, not failed.
    let (exempt_lists, failing): (Vec<String>, Vec<String>) = unread
        .into_iter()
        .partition(|list| history.unread_lists.contains(list));
    for (lists, is_exempt) in [(&failing, false), (&exempt_lists, true)] {
        if lists.is_empty() {
            continue;
        }
        let summary = if is_exempt {
            format!(
                "no final read for {} exempt shards, held on purpose; their lost-write check is skipped, the rest of the history check ran: {}",
                lists.len(),
                lists.join(", ")
            )
        } else {
            format!(
                "no final read for {} shards, left out of the history check: {}",
                lists.len(),
                lists.join(", ")
            )
        };
        let mut finding = if is_exempt {
            Finding::warning("final-reads", summary)
        } else {
            Finding::new("final-reads", summary)
        };
        for list in lists.iter().take(3) {
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
/// that could not be read leaves the history, unless it is in `held`: that one
/// stays, marked unread, so only the rules that need the final log skip it.
/// Returns the names of all unread lists.
fn drop_unread_lists(history: &mut History, held: &BTreeSet<String>) -> Vec<String> {
    let unread: Vec<String> = history
        .lists
        .keys()
        .filter(|list| !history.final_reads.contains_key(*list))
        .cloned()
        .collect();
    let is_held = |list: &String| {
        let shard = list.rsplit('/').next().and_then(|n| n.parse().ok());
        shard.is_some_and(|shard| {
            held.contains(&crate::cluster::describe(&crate::cluster::stream_key(
                shard,
            )))
        })
    };
    let (kept, dropped): (Vec<&String>, Vec<&String>) = unread.iter().partition(|l| is_held(l));
    history.unread_lists.extend(kept.into_iter().cloned());
    for list in &dropped {
        history.lists.remove(*list);
    }
    history
        .ops
        .retain(|op| !dropped.iter().any(|list| *list == op.list()));
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
