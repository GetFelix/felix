//! Seeds of the Raft group simulation (`felix_sim::raft`).
//!
//! `raft_seed_*` run in the workspace suite. `raft_seeds` is the batch:
//! `FELIX_SIM_SEED_START` and `FELIX_SIM_SEEDS` pick the seeds and
//! `FELIX_SIM_CLOCKS=bounded` skews the members' timers.
//! `each_raft_seed_runs_the_same_twice` takes the same variables. It fails
//! today (#2449): a seed fixes the faults and the client plan, not when a
//! member starts an election.

use std::time::Duration;

use felix_sim::raft::{Clocks, RaftSimConfig, run_seed};
use felix_sim::run::first_difference;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Run `seed` and say what failed, if anything did.
fn failure(seed: u64, clocks: Clocks) -> Option<String> {
    let config = RaftSimConfig {
        clocks,
        ..RaftSimConfig::default()
    };
    let outcome = run_seed(seed, config);
    println!("{outcome}");
    if outcome.history.acknowledged() <= 100 {
        return Some(format!(
            "raft seed {seed} ({clocks:?}) acknowledged too little to check anything"
        ));
    }
    (!outcome.passed()).then(|| {
        format!(
            "raft seed {seed} ({clocks:?}) failed: {}",
            outcome.first_failure()
        )
    })
}

fn check(seed: u64, clocks: Clocks) {
    if let Some(failure) = failure(seed, clocks) {
        panic!("{failure}");
    }
}

#[test]
fn raft_seed_1() {
    check(1, Clocks::Off);
}

#[test]
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

fn clocks() -> Clocks {
    match std::env::var("FELIX_SIM_CLOCKS").as_deref() {
        Ok("bounded") => Clocks::Bounded,
        _ => Clocks::Off,
    }
}

/// What the seed draws is the same in every run: the member count and
/// every fault's time. Which member a leader isolation hits is not, since
/// the runs can differ in who leads (#2449).
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

#[test]
#[ignore = "#2449: openraft draws election timeouts from an unseeded RNG"]
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
