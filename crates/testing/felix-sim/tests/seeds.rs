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

/// A get from a restarted broker misses a put that a quorum acknowledged.
#[test]
#[ignore = "#1943"]
fn seed_1_cache_get_after_a_restart_sees_the_acknowledged_put() {
    use felix_cluster::history::checker::Rule;
    log();
    let outcome = run_seed(1, SimConfig::for_seed(1));
    let broken = outcome
        .report
        .violations
        .iter()
        .any(|v| v.rule == Rule::NotLinearizable);
    assert!(!broken, "{outcome}");
}
