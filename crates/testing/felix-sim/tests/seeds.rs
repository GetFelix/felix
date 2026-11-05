//! Runs whole simulated clusters from a seed.
//!
//! Each run builds three to five real brokers and a control plane in this
//! process, drives clients and a nemesis against them for 120 seconds of
//! virtual time and checks what happened. A failure prints its seed. To run
//! one seed again, with the log the failure printed:
//!
//! ```text
//! FELIX_SIM_SEED=<n> cargo test -p felix-sim --test seeds replay -- --nocapture
//! ```
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `FELIX_SIM_SEED` | the seed `replay` runs | unset: `replay` is skipped |
//! | `FELIX_SIM_SEEDS` | how many seeds `seeds` runs | 4 |
//! | `FELIX_SIM_SEED_START` | the first seed `seeds` runs | 1 |
//! | `RUST_LOG` | broker and harness logs | none |
//!
//! A run is repeatable to the extent described in `felix_sim::run`: the same
//! seed injects the same faults at the same virtual times, but the brokers'
//! own interleaving can differ, so a failure may take more than one replay to
//! show again.
//!
//! A seed that exposed a bug is pinned below with `#[ignore = "#<issue>"]`,
//! and runs with `cargo test -p felix-sim --test seeds -- --ignored`.

use felix_sim::cluster::SimConfig;
use felix_sim::run::{SimOutcome, run_seed};

fn log() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number, got {value:?}")),
    )
}

/// Run `seed` and panic with its outcome and replay command if it failed.
fn check(seed: u64) -> SimOutcome {
    log();
    let outcome = run_seed(seed, SimConfig::for_seed(seed));
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

#[test]
fn seeds() {
    let count = env_u64("FELIX_SIM_SEEDS").unwrap_or(4);
    let start = env_u64("FELIX_SIM_SEED_START").unwrap_or(1);
    let mut failed = Vec::new();
    for seed in start..start + count {
        log();
        let outcome = run_seed(seed, SimConfig::for_seed(seed));
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
#[ignore = "#1941"]
fn seed_1_replication_driver_does_not_spin_on_a_pending_fence() {
    log();
    let outcome = run_seed(1, SimConfig::for_seed(1));
    assert!(!stalled(&outcome), "{outcome}");
}

/// More seeds that stall like seed 1. They stall at the first faults, so the
/// cluster checkers see little of them until it is fixed.
#[test]
#[ignore = "#1941"]
fn seed_2_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(2, SimConfig::for_seed(2));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
#[ignore = "#1941"]
fn seed_3_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(3, SimConfig::for_seed(3));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
#[ignore = "#1941"]
fn seed_4_replication_driver_does_not_spin() {
    log();
    let outcome = run_seed(4, SimConfig::for_seed(4));
    assert!(!stalled(&outcome), "{outcome}");
}

#[test]
#[ignore = "#1941"]
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
#[ignore = "#1948, and #1941 while seed 6 stalls"]
fn seed_6_every_shard_is_served_after_a_paused_control_plane_and_a_leader_crash() {
    log();
    let outcome = run_seed(6, SimConfig::for_seed(6));
    assert!(!stalled(&outcome), "the run stalled:\n{outcome}");
    assert!(!found(&outcome, "election-settles"), "{outcome}");
}

/// A leader's committed mark stays below its log tail after the faults heal,
/// so the shard cannot be read to its tail.
#[test]
#[ignore = "#1949, and #1941 while seed 6 stalls"]
fn seed_6_every_shard_can_be_read_after_the_faults_heal() {
    log();
    let outcome = run_seed(6, SimConfig::for_seed(6));
    assert!(!stalled(&outcome), "the run stalled:\n{outcome}");
    assert!(!found(&outcome, "final-reads"), "{outcome}");
}

/// A broker frozen while its cache shards moved serves them again at the old
/// generation, next to the new leader, once it thaws.
#[test]
#[ignore = "#1955, and #1941 while seed 4 stalls"]
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
#[ignore = "#1943, and #1941 while seed 1 stalls"]
fn seed_1_cache_keeps_acknowledged_puts_across_a_restart() {
    assert_cache_is_linearizable(1);
}

#[test]
#[ignore = "#1943"]
fn seed_5_cache_keeps_acknowledged_puts_across_a_failover() {
    assert_cache_is_linearizable(5);
}
