//! Seeds of the Raft group simulation (`felix_sim::raft`).
//!
//! Built with `RUSTFLAGS="--cfg tokio_unstable"`, a seed replays its run
//! event for event. Without the flag `select!` is unseeded and two runs of a
//! seed can differ, so the pinned seeds (`raft_seed_*`) and
//! `each_raft_seed_runs_the_same_twice` are skipped; CI's `sim-seeds` job
//! runs them with it. `raft_seeds` is the batch: `FELIX_SIM_SEED_START` and
//! `FELIX_SIM_SEEDS` pick the seeds and `FELIX_SIM_CLOCKS=bounded` skews the
//! members' timers. `each_raft_seed_runs_the_same_twice` takes the same
//! variables.

use std::time::Duration;

use felix_cluster::history::model::Action;
use felix_sim::raft::{Clocks, RaftOutcome, RaftSimConfig, run_seed};
use felix_sim::run::first_difference;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// What a seed must exercise to count. Seeds 1-40 on 2 shards each had at
// least 9 terms led, 2400 live deliveries, 2600 acknowledged writes, 530
// reads and 6 faults; the floors sit well below that.
const MIN_DELIVERIES: usize = 500;
const MIN_ACKNOWLEDGED: usize = 1000;
const MIN_READS: usize = 200;
const MIN_FAULTS: usize = 4;

/// Why `outcome` checked too little to mean anything, if it did.
fn vacuous(outcome: &RaftOutcome, shards: u32) -> Option<String> {
    let history = &outcome.history;
    let reads = history
        .ops
        .iter()
        .filter(|op| matches!(op.action, Action::Read { .. }))
        .count();
    let faults = history.faults.iter().filter(|f| f.what != "heal").count();
    if outcome.terms_led <= shards as usize {
        Some(format!(
            "leadership never moved ({} terms led)",
            outcome.terms_led
        ))
    } else if outcome.deliveries.len() < MIN_DELIVERIES {
        Some(format!("{} live deliveries", outcome.deliveries.len()))
    } else if history.acknowledged() < MIN_ACKNOWLEDGED {
        Some(format!("{} acknowledged writes", history.acknowledged()))
    } else if reads < MIN_READS {
        Some(format!("{reads} reads"))
    } else if faults < MIN_FAULTS {
        Some(format!("{faults} faults"))
    } else {
        None
    }
}

/// Run `seed` and say what failed, if anything did, with the whole outcome.
fn failure(seed: u64, clocks: Clocks) -> Option<String> {
    let config = RaftSimConfig {
        clocks,
        ..RaftSimConfig::default()
    };
    let shards = config.shards;
    let outcome = run_seed(seed, config);
    println!("{outcome}");
    let why = if outcome.passed() {
        vacuous(&outcome, shards).map(|why| format!("checked too little: {why}"))
    } else {
        Some(format!("failed: {}", outcome.first_failure()))
    };
    why.map(|why| format!("raft seed {seed} ({clocks:?}) {why}\n{outcome}"))
}

fn check(seed: u64, clocks: Clocks) {
    if let Some(failure) = failure(seed, clocks) {
        panic!("{failure}");
    }
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_1() {
    check(1, Clocks::Off);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_2_with_skewed_timers() {
    check(2, Clocks::Bounded);
}

/// A run that never finishes fails with a finding that says where it was,
/// and what the clients saw until then is still checked.
#[test]
fn a_run_past_its_virtual_bound_ends_with_a_finding() {
    let config = RaftSimConfig {
        run_limit: Duration::from_secs(20),
        ..RaftSimConfig::default()
    };
    let outcome = run_seed(1, config);
    println!("{outcome}");
    let stalled = "stalled: the run was still injecting faults after 20s of virtual time";
    assert_eq!(outcome.findings, [stalled]);
    assert!(outcome.report.violations.is_empty());
    assert!(outcome.history.acknowledged() > 100);
}

/// One member's flush fails while the faults run. The copy whose log it
/// poisoned stops, every shard still gets a leader that acknowledges a
/// write, nothing acknowledged is lost, and the member's other copy serves.
#[test]
fn a_failed_flush_stops_one_copy_and_the_group_carries_on() {
    for seed in [1, 2] {
        let config = RaftSimConfig {
            failed_flush: true,
            ..RaftSimConfig::default()
        };
        let outcome = run_seed(seed, config);
        println!("{outcome}");
        assert!(outcome.passed(), "{}", outcome.first_failure());
        let stopped: Vec<_> = outcome
            .history
            .faults
            .iter()
            .filter(|fault| fault.what.contains("a failed flush stopped"))
            .collect();
        assert_eq!(stopped.len(), 1, "one copy stops: {stopped:?}");
    }
}

fn clocks() -> Clocks {
    match std::env::var("FELIX_SIM_CLOCKS").as_deref() {
        Ok("bounded") => Clocks::Bounded,
        _ => Clocks::Off,
    }
}

/// What the seed draws is the same in every run, even without a seeded
/// `select!`: the member count and every fault's time. Which member a leader
/// isolation hits can differ there, since the runs can differ in who leads.
#[test]
fn a_seed_draws_the_same_faults_twice() {
    let config = || RaftSimConfig {
        nemesis: Duration::from_secs(15),
        ..RaftSimConfig::default()
    };
    let (first, second) = (run_seed(3, config()), run_seed(3, config()));
    let times = |faults: &[felix_cluster::history::model::FaultEvent]| -> Vec<u64> {
        faults.iter().map(|fault| fault.at).collect()
    };
    assert!(first.history.faults.len() >= 4, "{first}");
    assert_eq!(first.members, second.members);
    assert_eq!(times(&first.history.faults), times(&second.history.faults));
}

/// Each seed, run twice in one process, is the same run event for event.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn each_raft_seed_runs_the_same_twice() {
    let start = env("FELIX_SIM_SEED_START", 1);
    let count = env("FELIX_SIM_SEEDS", 2);
    let mut differed = Vec::new();
    for seed in start..start + count {
        let config = || RaftSimConfig {
            clocks: clocks(),
            ..RaftSimConfig::default()
        };
        let (first, second) = (run_seed(seed, config()), run_seed(seed, config()));
        match first_difference(&first.trace(), &second.trace()) {
            Some(difference) => differed.push(format!("raft seed {seed}: {difference}")),
            None => println!(
                "raft seed {seed}: the same twice, trace {:016x}, {} events",
                first.trace_hash(),
                first.trace().len()
            ),
        }
        assert_eq!(
            first.trace_hash() == second.trace_hash(),
            first.trace() == second.trace()
        );
    }
    assert!(
        differed.is_empty(),
        "{} of {count} raft seeds ran differently the second time:\n{}",
        differed.len(),
        differed.join("\n")
    );
}

#[test]
#[ignore = "a batch of seeds; run it by name"]
fn raft_seeds() {
    let clocks = clocks();
    let start = env("FELIX_SIM_SEED_START", 1);
    let count = env("FELIX_SIM_SEEDS", 4);
    // Every seed runs, so one batch says which seeds a change breaks.
    let failures: Vec<String> = (start..start + count)
        .filter_map(|seed| failure(seed, clocks))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {count} raft seeds failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
