//! Seeds of the Raft group simulation (`felix_sim::raft`).
//!
//! `raft_seed_*` run in the workspace suite. `raft_seeds` is the batch:
//! `FELIX_SIM_SEED_START` and `FELIX_SIM_SEEDS` pick the seeds and
//! `FELIX_SIM_CLOCKS=bounded` skews the members' timers.

use felix_sim::raft::{Clocks, RaftSimConfig, run_seed};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn check(seed: u64, clocks: Clocks) {
    let config = RaftSimConfig {
        clocks,
        ..RaftSimConfig::default()
    };
    let outcome = run_seed(seed, config);
    println!("{outcome}");
    assert!(
        outcome.history.acknowledged() > 100,
        "seed {seed} acknowledged too little to check anything"
    );
    assert!(outcome.passed(), "raft seed {seed} ({clocks:?}) failed");
}

#[test]
fn raft_seed_1() {
    check(1, Clocks::Off);
}

#[test]
fn raft_seed_2_with_skewed_timers() {
    check(2, Clocks::Bounded);
}

#[test]
#[ignore = "a batch of seeds; run it by name"]
fn raft_seeds() {
    let clocks = match std::env::var("FELIX_SIM_CLOCKS").as_deref() {
        Ok("bounded") => Clocks::Bounded,
        _ => Clocks::Off,
    };
    let start = env("FELIX_SIM_SEED_START", 1);
    for seed in start..start + env("FELIX_SIM_SEEDS", 4) {
        check(seed, clocks);
    }
}
