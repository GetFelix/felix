//! Runs whole simulated clusters from a seed.
//!
//! Each run builds three to five real brokers and a control plane in this
//! process, drives clients and a nemesis against them for 120 seconds of
//! virtual time and checks what happened. A failure prints its seed. To run
//! one seed again, with the log the failure printed:
//!
//! ```text
//! FELIX_SIM_SEED=<n> task sim:replay
//! ```
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `FELIX_SIM_SEED` | the seed `replay` runs | unset: `replay` is skipped |
//! | `FELIX_SIM_SEEDS` | how many seeds `seeds` and `each_seed_runs_the_same_twice` run | 4 |
//! | `FELIX_SIM_SEED_START` | the first seed they run | 1 |
//! | `FELIX_SIM_HISTORY` | a file `replay` writes the run's trace to, for diffing two runs | unset |
//! | `FELIX_SIM_CLOCKS` | clock skew for `replay`, `seeds` and `each_seed_runs_the_same_twice`: `off`, `bounded` or `unbounded`; anything but `off` needs `--cfg tokio_unstable` | `off` |
//! | `FELIX_SIM_PLACEMENT` | `1` turns placement faults on for `replay`, `seeds` and `each_seed_runs_the_same_twice` | unset: off |
//! | `FELIX_SIM_POWER_LOSS` | `1` cuts the power in place of kills for `replay`, `seeds` and `each_seed_runs_the_same_twice` | unset: off |
//! | `RUST_LOG` | broker and harness logs | none |
//!
//! Built with `RUSTFLAGS="--cfg tokio_unstable"`, as `task sim:replay` does,
//! a replay is the same run, event for event; `task sim:determinism` checks
//! that within one process and across two. Without the flag `select!` is
//! unseeded: the faults and operations repeat, but the interleaving under
//! them can differ, so `replay` refuses to write `FELIX_SIM_HISTORY` from
//! such a build.
//!
//! A seed that exposed a bug is pinned below as a test that asserts only the
//! finding its issue is about. Some pins are skipped without `--cfg
//! tokio_unstable`, so they check the one run the seed gives rather than
//! whichever interleaving a build happens to take; `task sim:determinism`
//! runs them.
//! A seed whose bug is still open is pinned with `#[ignore = "#<issue>"]` and
//! runs with `cargo test -p felix-sim --test seeds -- --ignored`.

use felix_sim::clock::{Clocks, NodeClock, PPM};
use felix_sim::cluster::SimConfig;
use felix_sim::nemesis::clocks::{BOUNDED_RATE_PPM, ClockPlan, ClockSkew};
use felix_sim::run::{SimOutcome, first_difference, run_seed};

fn log() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(VirtualTime)
        .with_test_writer()
        .try_init();
}

/// Stamps each log line with the run's virtual time, so the logs of two runs
/// of a seed can be diffed.
struct VirtualTime;

impl tracing_subscriber::fmt::time::FormatTime for VirtualTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(
            w,
            "{:>10.3}s",
            felix_common::clock::boottime().as_secs_f64()
        )
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number, got {value:?}")),
    )
}

/// `seed`'s config, with the clock skew `FELIX_SIM_CLOCKS` asks for,
/// placement faults when `FELIX_SIM_PLACEMENT=1` and power cuts when
/// `FELIX_SIM_POWER_LOSS=1`.
fn config(seed: u64) -> SimConfig {
    let clocks = match std::env::var("FELIX_SIM_CLOCKS") {
        Ok(value) => ClockSkew::parse(&value).unwrap_or_else(|| {
            panic!("FELIX_SIM_CLOCKS must be off, bounded or unbounded, got {value:?}")
        }),
        Err(_) => ClockSkew::Off,
    };
    let config = SimConfig {
        clocks,
        ..SimConfig::for_seed(seed)
    };
    let config = match std::env::var("FELIX_SIM_PLACEMENT").as_deref() {
        Ok("1") => config.with_placement_faults(),
        _ => config,
    };
    match std::env::var("FELIX_SIM_POWER_LOSS").as_deref() {
        Ok("1") => config.with_power_loss(),
        _ => config,
    }
}

/// Run `seed` and panic with its outcome and replay command if it failed.
fn check(seed: u64) -> SimOutcome {
    let history = std::env::var("FELIX_SIM_HISTORY").ok();
    // Two histories from an unseeded build differ wherever two ready
    // `select!` branches raced, which reads as a determinism bug.
    if let Some(path) = &history
        && !cfg!(tokio_unstable)
    {
        panic!(
            "FELIX_SIM_HISTORY needs a build with --cfg tokio_unstable to repeat; \
             run FELIX_SIM_SEED={seed} FELIX_SIM_HISTORY={path} task sim:replay"
        );
    }
    log();
    let outcome = run_seed(seed, config(seed));
    if let Some(path) = history {
        std::fs::write(&path, outcome.trace().join("\n") + "\n")
            .unwrap_or_else(|err| panic!("write the history to {path}: {err}"));
    }
    println!("{outcome}");
    assert!(
        outcome.passed(),
        "seed {seed} failed. To run it again:\n    {}\n\n{outcome}",
        outcome.replay_command()
    );
    outcome
}

#[test]
fn replay() {
    let Some(seed) = env_u64("FELIX_SIM_SEED") else {
        eprintln!("FELIX_SIM_SEED is not set; nothing to replay");
        return;
    };
    check(seed);
}

/// Each seed `seeds` would run, run twice in one process, must be the same
/// run event for event. Tokio seeds `select!` only under `--cfg
/// tokio_unstable`, so without it this is skipped; `task sim:determinism`
/// runs it with the flag, and CI runs that.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn each_seed_runs_the_same_twice() {
    let count = env_u64("FELIX_SIM_SEEDS").unwrap_or(4);
    let start = env_u64("FELIX_SIM_SEED_START").unwrap_or(1);
    let mut differed = Vec::new();
    for seed in start..start + count {
        log();
        let first = run_seed(seed, config(seed));
        let second = run_seed(seed, config(seed));
        match first_difference(&first.trace(), &second.trace()) {
            Some(difference) => differed.push(format!("seed {seed}: {difference}")),
            None => println!("seed {seed}: the same twice, passed: {}", first.passed()),
        }
    }
    assert!(
        differed.is_empty(),
        "{} of {count} seeds ran differently the second time:\n{}",
        differed.len(),
        differed.join("\n")
    );
}

/// Skewed clocks are drawn from the seed and stepped at fixed virtual
/// times, so a skewed seed is as repeatable as any other.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select! and the task hooks"
)]
fn a_skewed_seed_runs_the_same_twice() {
    log();
    let skewed = || SimConfig {
        clocks: ClockSkew::Bounded,
        ..SimConfig::for_seed(2)
    };
    let first = run_seed(2, skewed());
    let second = run_seed(2, skewed());
    assert!(
        first.trace().iter().any(|line| line.contains("clock:")),
        "the trace names no clock change:\n{first}"
    );
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed 2 with skewed clocks ran differently the second time: {difference}");
    }
}

/// The pair the lease margins guard against, at the edge of the bounded
/// drift: every broker's lease clock runs slow while the control plane's runs
/// fast, so a broker counts its lease as longest while the control plane
/// counts silence fastest. Only the safety checks are asserted; liveness
/// findings are the known stalls.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for the task hooks"
)]
fn seed_8_slow_brokers_and_a_fast_control_plane_stay_safe() {
    log();
    let base = SimConfig::for_seed(8);
    let plan = ClockPlan {
        initial: Clocks {
            controlplane: NodeClock::new(0, PPM + BOUNDED_RATE_PPM),
            brokers: vec![NodeClock::new(0, PPM - BOUNDED_RATE_PPM); base.brokers],
        },
        steps: Vec::new(),
    };
    let outcome = run_seed(
        8,
        SimConfig {
            clock_plan: Some(plan),
            ..base
        },
    );
    println!("{outcome}");
    assert!(outcome.report.violations.is_empty(), "{outcome}");
    for unsafe_finding in [
        "at-most-one-serving",
        "one-leader-per-generation",
        "acked-held-by-leader",
    ] {
        assert!(!found(&outcome, unsafe_finding), "{outcome}");
    }
}

#[test]
#[ignore = "known liveness stalls (#2024, #2032, #2033); runs in the non-blocking sim-seeds CI job"]
fn seeds() {
    let count = env_u64("FELIX_SIM_SEEDS").unwrap_or(4);
    let start = env_u64("FELIX_SIM_SEED_START").unwrap_or(1);
    let mut failed = Vec::new();
    for seed in start..start + count {
        log();
        let outcome = run_seed(seed, config(seed));
        println!("{outcome}");
        if !outcome.passed() {
            failed.push(outcome.replay_command());
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {count} seeds failed. To run one again:\n    {}",
        failed.len(),
        failed.join("\n    ")
    );
}

// Seeds that exposed a bug. Each runs one seed and asserts only the finding
// its issue is about, so it passes when that bug is fixed and not before.

/// Whether a run stalled: virtual time stopped because a task never waited.
fn stalled(outcome: &SimOutcome) -> bool {
    outcome.findings.iter().any(|f| f.checker == "stalled")
}

/// The replication driver retries a pending fence with a zero delay, forever,
/// when the gate awaits a generation the route does not have yet.
#[test]
fn seed_1_replication_driver_does_not_spin_on_a_pending_fence() {
    log();
    let outcome = run_seed(1, SimConfig::for_seed(1));
    assert!(!stalled(&outcome), "{outcome}");
}

/// More seeds that stall like seed 1. They stall at the first faults, so the
/// cluster checkers see little of them until it is fixed.
#[test]
fn seed_2_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(2, SimConfig::for_seed(2));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
fn seed_3_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(3, SimConfig::for_seed(3));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_4_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(4, SimConfig::for_seed(4));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
fn seed_6_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(6, SimConfig::for_seed(6));
    assert!(!stalled(&outcome), "{outcome}");
}

/// Whether `checker` reported anything.
fn found(outcome: &SimOutcome, checker: &str) -> bool {
    outcome.findings.iter().any(|f| f.checker == checker)
}

/// The control plane pauses and a shard leader crashes; once everything heals
/// a restarted leader stays in `fencing` and its shard is never served.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_6_every_shard_is_served_after_a_paused_control_plane_and_a_leader_crash() {
    log();
    let outcome = run_seed(6, SimConfig::for_seed(6));
    assert!(!stalled(&outcome), "the run stalled:\n{outcome}");
    assert!(!found(&outcome, "election-settles"), "{outcome}");
}

/// A leader's committed mark stays below its log tail after the faults heal,
/// so the shard cannot be read to its tail.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_6_every_shard_can_be_read_after_the_faults_heal() {
    log();
    let outcome = run_seed(6, SimConfig::for_seed(6));
    assert!(!stalled(&outcome), "the run stalled:\n{outcome}");
    assert!(!found(&outcome, "final-reads"), "{outcome}");
}

/// A broker frozen while its cache shards moved serves them again at the old
/// generation, next to the new leader, once it thaws.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_4_a_thawed_broker_does_not_serve_shards_that_moved() {
    log();
    let outcome = run_seed(4, SimConfig::for_seed(4));
    assert!(!stalled(&outcome), "the run stalled:\n{outcome}");
    assert!(!found(&outcome, "at-most-one-serving"), "{outcome}");
}

/// A cache read or delete after a failover or restart misses a put that a
/// quorum acknowledged. The search for a linearization only runs on a key the
/// simpler rules (stale read, phantom) found nothing on, so any of the three
/// is this bug; a run that stalled says nothing either way.
fn assert_cache_is_linearizable(seed: u64) {
    use felix_cluster::history::checker::Rule;
    log();
    let outcome = run_seed(seed, SimConfig::for_seed(seed));
    assert!(
        !stalled(&outcome),
        "the run stalled, so the cache was not checked:\n{outcome}"
    );
    let broken = outcome.report.violations.iter().any(|v| {
        matches!(
            v.rule,
            Rule::StaleRead | Rule::Phantom | Rule::NotLinearizable
        )
    });
    assert!(!broken, "{outcome}");
}

#[test]
fn seed_1_cache_keeps_acknowledged_puts_across_a_restart() {
    assert_cache_is_linearizable(1);
}

#[test]
fn seed_5_cache_keeps_acknowledged_puts_across_a_failover() {
    assert_cache_is_linearizable(5);
}

/// A seed that cuts the power during an append runs the same twice, and
/// recovery repaired the torn append it left.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_1_power_cuts_run_the_same_twice_and_are_repaired() {
    log();
    let config = SimConfig::for_seed(1).with_power_loss();
    let first = run_seed(1, config.clone());
    let second = run_seed(1, config);
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed 1 ran differently the second time: {difference}");
    }
    let cut_in_append = first
        .history
        .faults
        .iter()
        .any(|fault| fault.what.contains("lost power during an append"));
    assert!(cut_in_append, "no cut landed in an append:\n{first}");
    assert!(!found(&first, "torn-tail-repaired"), "{first}");
}

// Placement faults. Each test pins a seed whose run reaches one shape, checks
// that the run did reach it, and that the seed is the same run twice. They
// assert nothing about what the checkers found: the seeds that fail are
// listed in docs/simulation.md.

/// Run `seed` with placement faults twice, and fail unless both runs logged
/// every one of `marks` and are the same event for event.
fn placement_shape_runs_the_same_twice(seed: u64, marks: &[&str]) {
    log();
    let config = SimConfig::for_seed(seed).with_placement_faults();
    let first = run_seed(seed, config.clone());
    for mark in marks {
        assert!(
            first.history.faults.iter().any(|f| f.what.contains(mark)),
            "seed {seed} never logged `{mark}`:\n{first}"
        );
    }
    let second = run_seed(seed, config);
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed {seed} ran differently the second time: {difference}");
    }
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_21_a_shard_move_and_its_cancel_replay_the_same_twice() {
    placement_shape_runs_the_same_twice(21, &["move stream/", "cancel the move of stream/83: 200"]);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_1_a_drain_replays_the_same_twice() {
    placement_shape_runs_the_same_twice(1, &["drain b2: 200"]);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_12_a_node_removal_replays_the_same_twice() {
    placement_shape_runs_the_same_twice(12, &["drain b4: 200", "deregistered b4"]);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_33_abandoning_held_shards_replays_the_same_twice() {
    placement_shape_runs_the_same_twice(33, &["abandoned the log of stream/14: 200"]);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_3_kills_between_promotion_and_open_replay_the_same_twice() {
    placement_shape_runs_the_same_twice(
        3,
        &[
            "named leader of stream/4 at generation 5, before it opens",
            "elected leader of stream/5 at generation 7, before it opens",
        ],
    );
}

/// The shape behind #1922 and #1929: a seat being replaced, the leader lost,
/// and the leader named next lost before it opens.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_13_a_seat_race_replays_the_same_twice() {
    placement_shape_runs_the_same_twice(
        13,
        &[
            "seat race: stream/3 replaces b4 with b1; stop its leader b2",
            "named leader of stream/3 at generation 5, before it opens",
        ],
    );
}

/// With power loss on, placement faults cut the power where they would kill:
/// a kill makes an `OnCommit` broker forget a boot whose records were synced.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_3_placement_cuts_the_power_instead_of_killing() {
    log();
    let config = SimConfig::for_seed(3)
        .with_placement_faults()
        .with_power_loss();
    let outcome = run_seed(3, config);
    let faults = &outcome.history.faults;
    let before_open = |verb: &str| {
        faults
            .iter()
            .any(|f| f.what.starts_with(verb) && f.what.contains("before it opens"))
    };
    assert!(before_open("power-cut b"), "{outcome}");
    assert!(!before_open("kill b"), "{outcome}");
}
