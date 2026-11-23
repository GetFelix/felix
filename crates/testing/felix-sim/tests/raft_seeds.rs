//! Seeds of the Raft group simulation (`felix_sim::raft`).
//!
//! Built with `RUSTFLAGS="--cfg tokio_unstable"`, a seed replays its run
//! event for event. Without the flag `select!` is unseeded and two runs of a
//! seed can differ, so the pinned seeds (`raft_seed_*`) and
//! `each_raft_seed_runs_the_same_twice` are skipped; CI's `sim-seeds` job
//! runs them with it. `raft_seeds` is the batch: `FELIX_SIM_SEED_START` and
//! `FELIX_SIM_SEEDS` pick the seeds and `FELIX_SIM_CLOCKS=bounded` skews the
//! members' timers. `FELIX_SIM_WIPE=1` and `FELIX_SIM_RESTORE=1` add disk
//! faults: a member stopped, a shard directory or data root wiped or put
//! back, and the member started again. `FELIX_SIM_POWER_LOSS=1` or
//! `relaxed`, alone or with `FELIX_SIM_MAJORITY_CUT=1`, cut members' power
//! instead. `each_raft_seed_runs_the_same_twice` takes the same variables.

use std::time::Duration;

use felix_cluster::history::model::Action;
use felix_sim::raft::{Clocks, Disks, RaftOutcome, RaftSimConfig, run_seed};
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
fn vacuous(outcome: &RaftOutcome, shards: u32, disks: Disks) -> Option<String> {
    let history = &outcome.history;
    let reads = history
        .ops
        .iter()
        .filter(|op| matches!(op.action, Action::Read { .. }))
        .count();
    let faults = history
        .faults
        .iter()
        .filter(|f| {
            f.what != "heal" && !f.what.starts_with("save ") && !f.what.starts_with("skip ")
        })
        .count();
    // A disk fault waits for its shards to heal, so a group that never
    // heals would otherwise pass on link faults alone.
    let replaced = history
        .faults
        .iter()
        .filter(|f| f.what.starts_with("wipe ") || f.what.starts_with("restore "))
        .count()
        + outcome.power.cuts;
    if outcome.terms_led <= shards as usize {
        Some(format!(
            "leadership never moved ({} terms led)",
            outcome.terms_led
        ))
    } else if outcome.terms_opened <= shards as usize {
        Some(format!(
            "no shard opened in a second term ({} terms opened)",
            outcome.terms_opened
        ))
    } else if outcome.deliveries.len() < MIN_DELIVERIES {
        Some(format!("{} live deliveries", outcome.deliveries.len()))
    } else if history.acknowledged() < MIN_ACKNOWLEDGED {
        Some(format!("{} acknowledged writes", history.acknowledged()))
    } else if reads < MIN_READS {
        Some(format!("{reads} reads"))
    } else if faults < MIN_FAULTS {
        Some(format!("{faults} faults"))
    } else if outcome.leases == Some(0) {
        Some("no member passed the lease check, so none was judged".into())
    } else if disks.together() && outcome.power.together == 0 {
        Some("no members lost power together".into())
    } else if disks != Disks::Off && replaced == 0 {
        let skipped: Vec<&str> = history
            .faults
            .iter()
            .filter(|f| f.what.starts_with("skip "))
            .map(|f| f.what.as_str())
            .collect();
        Some(format!(
            "no directory wiped or put back ({})",
            if skipped.is_empty() {
                "none tried".into()
            } else {
                skipped.join("; ")
            }
        ))
    } else {
        None
    }
}

/// Run `seed` and say what failed, if anything did, with the whole outcome.
fn failure(seed: u64, clocks: Clocks, disks: Disks, leader_change: bool) -> Option<String> {
    run_checked(seed, clocks, disks, leader_change).err()
}

/// Run `seed` and hand back its outcome, or say what failed with the whole
/// outcome.
fn run_checked(
    seed: u64,
    clocks: Clocks,
    disks: Disks,
    leader_change: bool,
) -> Result<RaftOutcome, String> {
    let config = RaftSimConfig {
        clocks,
        disks,
        leader_change,
        ..RaftSimConfig::default()
    };
    let shards = config.shards;
    let outcome = run_seed(seed, config);
    println!("{outcome}");
    let why = if outcome.passed() {
        vacuous(&outcome, shards, disks).map(|why| format!("checked too little: {why}"))
    } else {
        Some(format!("failed: {}", outcome.first_failure()))
    };
    match why {
        Some(why) => Err(format!(
            "raft seed {seed} ({clocks:?}, {disks:?}) {why}\n{outcome}"
        )),
        None => Ok(outcome),
    }
}

fn check(seed: u64, clocks: Clocks, disks: Disks) -> RaftOutcome {
    run_checked(seed, clocks, disks, false).unwrap_or_else(|failure| panic!("{failure}"))
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_1() {
    check(1, Clocks::Off, Disks::Off);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_2_with_skewed_timers() {
    check(2, Clocks::Bounded, Disks::Off);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_2_with_wipes() {
    check(2, Clocks::Off, Disks::Wipe);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_1_with_restores() {
    check(1, Clocks::Off, Disks::Restore);
}

/// A leader elected just before the final reads had not opened its shard
/// yet, and the run reported no leader to read from. The final reads now
/// wait for an open leader like the recovery check does. The seed's cuts
/// also tear appends, so it shows torn tails are repaired on Raft.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_1233_with_power_loss() {
    let power = check(1233, Clocks::Off, Disks::PowerLoss).power;
    assert!(
        power.torn > 0 && power.repaired == power.torn,
        "no torn tail to judge, or one left unrepaired: {power:?}"
    );
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn raft_seed_920_with_majority_cuts() {
    check(920, Clocks::Off, Disks::MajorityCut);
}

/// A simulation's brokers share this process, and so do the tests. A log
/// poisoned in one run must not make a later run's power cuts rotate copies,
/// as one process's poison does on every root it holds.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn a_failed_flush_leaves_a_later_runs_power_cuts_alone() {
    let config = RaftSimConfig {
        failed_flush: true,
        ..RaftSimConfig::default()
    };
    let poisoned = run_seed(1, config);
    assert!(poisoned.passed(), "{}", poisoned.first_failure());
    check(1233, Clocks::Off, Disks::PowerLoss);
}

/// One shard's directory wiped on a majority at once leaves no copy that
/// may vote under its old id, so the shard stalls until an operator
/// re-forms it (#2350). Nothing acknowledged is reported lost, the other
/// shard carries on, every wiped copy rotated, and the stall is the only
/// finding.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs RUSTFLAGS=\"--cfg tokio_unstable\" for a seeded select!"
)]
fn a_majority_wipe_stalls_its_shard_and_loses_nothing() {
    let config = RaftSimConfig {
        disks: Disks::WipeMajority,
        nemesis: Duration::from_secs(20),
        leader_change: false,
        ..RaftSimConfig::default()
    };
    let outcome = run_seed(1, config);
    println!("{outcome}");
    assert!(
        outcome.report.violations.is_empty(),
        "{}",
        outcome.first_failure()
    );
    let stalled = "t/ns/s0: no leader acknowledged a write within 30s of the heal";
    assert!(outcome.findings.iter().any(|f| f == stalled), "{outcome}");
    let stall = |f: &str| {
        f == stalled
            || f == "t/ns/s0: no leader to take the final read from"
            // Members left at different points of the stalled log.
            || (f.starts_with("t/ns/s0: b") && f.ends_with(", or not the same ones"))
            || (f.starts_with("t/ns/s0: b")
                && f.ends_with(" still refuses reads 30s after the heal"))
    };
    let other: Vec<_> = outcome.findings.iter().filter(|f| !stall(f)).collect();
    assert!(other.is_empty(), "only s0 stalls: {other:?}");
    let majority = outcome.members / 2 + 1;
    let faults = |prefix: &str, what: &str| {
        let faults = outcome.history.faults.iter();
        faults
            .filter(|fault| fault.what.starts_with(prefix) && fault.what.contains(what))
            .count()
    };
    assert_eq!(faults("wipe ", "directory of t/ns/s0"), majority);
    assert_eq!(faults("t/ns/s0: ", "rotated its copy (Empty)"), majority);
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

/// The disk faults the environment asks for, with the variables of the
/// hybrid's modes: `FELIX_SIM_POWER_LOSS=1` or `relaxed`, and
/// `FELIX_SIM_MAJORITY_CUT=1`, alone on fsync on commit.
fn disks() -> Disks {
    let set = |name: &str| std::env::var(name).is_ok_and(|value| value == "1");
    let power = std::env::var("FELIX_SIM_POWER_LOSS").ok();
    let relaxed = power.as_deref() == Some("relaxed");
    if set("FELIX_SIM_RESTORE") {
        Disks::Restore
    } else if set("FELIX_SIM_WIPE") {
        Disks::Wipe
    } else if set("FELIX_SIM_MAJORITY_CUT") {
        if relaxed {
            Disks::MajorityCutRelaxed
        } else {
            Disks::MajorityCut
        }
    } else if relaxed {
        Disks::RelaxedPowerLoss
    } else if power.as_deref() == Some("1") {
        Disks::PowerLoss
    } else {
        Disks::Off
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
            disks: disks(),
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
    let (clocks, disks) = (clocks(), disks());
    let start = env("FELIX_SIM_SEED_START", 1);
    let count = env("FELIX_SIM_SEEDS", 4);
    // Every seed runs, so one batch says which seeds a change breaks.
    let failures: Vec<String> = (start..start + count)
        .filter_map(|seed| failure(seed, clocks, disks, true))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {count} raft seeds failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
