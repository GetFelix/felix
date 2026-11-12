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
//! | `FELIX_SIM_POWER_LOSS` | `1` cuts the power in place of kills for `replay`, `seeds` and `each_seed_runs_the_same_twice`, on brokers that sync on commit. `relaxed` does the same on brokers that do not, so a cut loses acknowledged bytes | unset: off |
//! | `FELIX_SIM_MAJORITY_CUT` | `1` also cuts the power under several brokers at once (power loss on, `relaxed` if set) | unset: off |
//! | `FELIX_SIM_WIPE` | `1` also wipes shard directories and whole data directories | unset: off |
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

use std::time::Duration;

use felix_cluster::history::checker::{Report, Rule, Violation};
use felix_sim::check::{Finding, Severity, Tally};
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
    let config = match std::env::var("FELIX_SIM_POWER_LOSS").as_deref() {
        Ok("1") => config.with_power_loss(),
        Ok("relaxed") => config.with_relaxed_power_loss(),
        _ => config,
    };
    let config = match std::env::var("FELIX_SIM_MAJORITY_CUT").as_deref() {
        Ok("1") => config.with_majority_cut(),
        _ => config,
    };
    match std::env::var("FELIX_SIM_WIPE").as_deref() {
        Ok("1") => config.with_wipes(),
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

/// Whether `finding` is the stall #2206 tracks: a leader that gave up opening
/// its shard (phase `failed`) with the control plane still `Assigning` and
/// no other copy to promote (the rest are `unassigned` or `closed`). It reads
/// the census the checker prints, and wants one for every stuck shard, so a
/// run with more stuck shards than the census shows does not match.
fn is_given_up_leader_stall(finding: &Finding) -> bool {
    if finding.checker != "election-settles" {
        return false;
    }
    // "N of M shards ... : stream/6 (0 serving), stream/20 (0 serving)"
    let Some((head, shards)) = finding.summary.split_once(" after the faults healed: ") else {
        return false;
    };
    let stuck: Vec<&str> = shards.split(", ").collect();
    let Some(count) = head
        .split_whitespace()
        .next()
        .and_then(|n| n.parse::<usize>().ok())
    else {
        return false;
    };
    // The checker prints a census for at most 3 stuck shards.
    if stuck.len() != count || count > 3 || stuck.iter().any(|s| !s.ends_with("(0 serving)")) {
        return false;
    }
    let mut named: Vec<String> = Vec::new();
    let mut censuses = 0;
    let mut leader = None;
    let mut given_up = false;
    let mut unassigned = false;
    let mut ok = true;
    let mut end_shard =
        |leader: &mut Option<String>, given_up: &mut bool, unassigned: &mut bool, ok: &mut bool| {
            if leader.take().is_some() {
                censuses += 1;
                *ok &= *given_up && *unassigned;
            }
            *given_up = false;
            *unassigned = false;
        };
    for line in finding.detail.lines() {
        if let Some(rest) = line.strip_prefix("shard ") {
            end_shard(&mut leader, &mut given_up, &mut unassigned, &mut ok);
            let Some((_, assignment)) = line.split_once("control plane: ") else {
                return false;
            };
            let mut words = assignment.split_whitespace();
            let (Some("leader"), Some(name)) = (words.next(), words.next()) else {
                return false;
            };
            if !assignment.contains(" state Assigning")
                || assignment.contains(" successor ")
                || assignment.contains(" joining ")
            {
                return false;
            }
            leader = Some(name.to_string());
            named.push(rest.split_whitespace().next().unwrap_or("").to_string());
        } else if let Some(lead) = &leader {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.first().is_none_or(|c| *c == "node") {
                continue;
            }
            let phase = cols.get(7).copied().unwrap_or("");
            if cols[0] == lead {
                given_up = phase == "failed";
            } else {
                match phase {
                    "unassigned" => unassigned = true,
                    "closed" => {}
                    _ => return false,
                }
            }
        }
    }
    end_shard(&mut leader, &mut given_up, &mut unassigned, &mut ok);
    let listed: Vec<&str> = stuck
        .iter()
        .map(|s| s.split_whitespace().next().unwrap_or(""))
        .collect();
    ok && censuses == count && named == listed
}

/// Whether `finding` is a missing final read that only covers shards in
/// `stalled` (shard numbers). The stalled shard is never served, so its final
/// read cannot succeed. A read missing on any other shard is a real failure.
fn is_final_read_of_stalled(finding: &Finding, stalled: &[&str]) -> bool {
    if finding.checker != "final-reads" {
        return false;
    }
    // "no final read for N shards, left out of the history check: events/20, events/6"
    let Some((_, lists)) = finding.summary.split_once("history check: ") else {
        return false;
    };
    lists.split(", ").all(|list| {
        list.rsplit('/')
            .next()
            .is_some_and(|n| stalled.contains(&n))
    })
}

/// Whether the run found nothing but the stall #2206 tracks: no history
/// violation, no undecided key, and every failing finding is that stall or
/// the missing final read of a shard it left unserved.
fn only_the_2206_stall(report: &Report, findings: &[Finding]) -> bool {
    let stalled: Vec<&str> = findings
        .iter()
        .filter(|f| is_given_up_leader_stall(f))
        .flat_map(|f| f.summary.split(": ").last().unwrap_or("").split(", "))
        .filter_map(|s| s.split_whitespace().next()?.rsplit('/').next())
        .collect();
    report.violations.is_empty()
        && report.undecided.is_empty()
        && findings
            .iter()
            .filter(|f| f.severity == Severity::Failure)
            .all(|f| is_given_up_leader_stall(f) || is_final_read_of_stalled(f, &stalled))
}

/// Fail on anything but the stall #2206 tracks, which is printed with its
/// issue instead.
fn assert_passed_but_for_2206(outcome: &SimOutcome) {
    assert!(
        only_the_2206_stall(&outcome.report, &outcome.findings),
        "{outcome}"
    );
    for finding in outcome.failures().filter(|f| is_given_up_leader_stall(f)) {
        println!("tracked stall #2206: {}", finding.summary);
    }
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
fn seed_3_power_cuts_run_the_same_twice_and_are_repaired() {
    log();
    let config = SimConfig::for_seed(3).with_power_loss();
    let first = run_seed(3, config.clone());
    let second = run_seed(3, config);
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed 3 ran differently the second time: {difference}");
    }
    let cut_in_append = first
        .history
        .faults
        .iter()
        .any(|fault| fault.what.contains("lost power during an append"));
    assert!(cut_in_append, "no cut landed in an append:\n{first}");
    assert!(!found(&first, "torn-tail-repaired"), "{first}");
}

/// Writes start before `record_generations` is final, so older segments hold
/// unstamped records; it is finalized mid-run and brokers are killed after,
/// so followers catch up across both kinds of segment. Every replica must name
/// the same writer for the records it holds (`record-generations-agree`),
/// and the run must have compared some, or it proves nothing.
#[test]
fn seed_3_replicas_agree_on_who_wrote_each_record() {
    log();
    let config = SimConfig {
        finalize_record_generations: Some(Duration::from_secs(20)),
        ..SimConfig::for_seed(3)
    };
    let outcome = run_seed(3, config);
    assert_passed_but_for_2206(&outcome);
    let tally = outcome
        .findings
        .iter()
        .filter(|f| f.checker == "record-generations-agree")
        .find_map(|f| Tally::parse(&f.summary))
        .unwrap_or_else(|| panic!("the checker reported no tally:\n{outcome}"));
    println!("record-generations-agree: {tally:?}");
    assert!(
        tally.compared > 0 && tally.stamped > 0 && tally.unread == 0,
        "{tally:?}\n{outcome}"
    );
    let faults = &outcome.history.faults;
    let finalized = faults
        .iter()
        .position(|f| f.what == "finalize record_generations")
        .unwrap_or_else(|| panic!("record_generations was not finalized:\n{outcome}"));
    assert!(
        faults[finalized..]
            .iter()
            .any(|f| f.what.starts_with("kill b")),
        "no broker was killed after the finalize:\n{outcome}"
    );
}

fn stall_finding(summary: &str, assignment: &str, rows: &[&str]) -> Finding {
    let census = format!(
        "shard stream/6  control plane: {assignment}\n  node  state\n{}",
        rows.join("\n")
    );
    Finding::new("election-settles", summary).with(census)
}

const GIVEN_UP: [&str; 3] = [
    "  b1    up        no    yes    1    9.0s     -              closed",
    "  b2    up        no    yes    1    9.0s     b2,10          failed",
    "  b3    up        no    yes    1    9.0s     -              unassigned",
];
const SUMMARY: &str = "1 of 178 shards are not served by exactly one broker 60s after the faults healed: stream/6 (0 serving)";
const ASSIGNING: &str = "leader b2 replicas [\"b1\", \"b2\", \"b3\"] generation 10 state Assigning";

#[test]
fn the_given_up_leader_shape_is_the_tracked_stall() {
    assert!(is_given_up_leader_stall(&stall_finding(
        SUMMARY, ASSIGNING, &GIVEN_UP
    )));
}

#[test]
fn another_election_settles_failure_is_not_the_tracked_stall() {
    // The leader is still opening, not given up.
    let opening = [
        GIVEN_UP[0],
        "  b2    up        no    yes    1    9.0s     b2,10          opening",
        GIVEN_UP[2],
    ];
    assert!(!is_given_up_leader_stall(&stall_finding(
        SUMMARY, ASSIGNING, &opening
    )));
    // The control plane settled on Active.
    let active = "leader b2 replicas [\"b1\"] generation 10 state Active";
    assert!(!is_given_up_leader_stall(&stall_finding(
        SUMMARY, active, &GIVEN_UP
    )));
    // A peer is ready to take over, so a copy was promotable.
    let promotable = [
        GIVEN_UP[0],
        GIVEN_UP[1],
        "  b3    up        no    yes    1    9.0s     -              fencing",
    ];
    assert!(!is_given_up_leader_stall(&stall_finding(
        SUMMARY,
        ASSIGNING,
        &promotable
    )));
    // More stuck shards than the census covers.
    let many = SUMMARY
        .replace("1 of", "2 of")
        .replace("(0 serving)", "(0 serving), stream/7 (0 serving)");
    assert!(!is_given_up_leader_stall(&stall_finding(
        &many, ASSIGNING, &GIVEN_UP
    )));
    // A shard served by someone.
    let served = SUMMARY.replace("0 serving", "2 serving");
    assert!(!is_given_up_leader_stall(&stall_finding(
        &served, ASSIGNING, &GIVEN_UP
    )));
    // Another checker.
    let mut other = stall_finding(SUMMARY, ASSIGNING, &GIVEN_UP);
    other.checker = "final-reads";
    assert!(!is_given_up_leader_stall(&other));
}

const CI_SUMMARY: &str = "2 of 178 shards are not served by exactly one broker 60s after the faults healed: stream/6 (0 serving), stream/20 (0 serving)";
const CI_HEADER: &str = "  node  state     cut   seeded inc  lease    routed(leader,g) phase      fence    tail     quorum  ";
const CI_ROWS: [&str; 3] = [
    "  b1    up        no    yes    0    6.9s     b2,10          closed     -        3        -       ",
    "  b2    up        no    yes    2    7.3s     b2,10          failed     -        4        -       ",
    "  b3    up        no    yes    1    8.0s     b2,10          unassigned -        4        -       ",
];

fn ci_census(shard: &str, generation: u32, rows: &[&str]) -> String {
    format!(
        "shard {shard}  control plane: leader b2 replicas [\"b1\", \"b3\"] generation {generation} state Assigning\n{CI_HEADER}\n{}\n",
        rows.join("\n")
    )
}

fn ci_finding(summary: &str, rows: &[&str]) -> Finding {
    Finding::new("election-settles", summary)
        .with(ci_census("stream/6", 10, rows))
        .with(ci_census("stream/20", 24, rows))
}

#[test]
fn the_text_ci_printed_for_two_shards_is_the_tracked_stall() {
    assert!(is_given_up_leader_stall(&ci_finding(CI_SUMMARY, &CI_ROWS)));
}

#[test]
fn the_tracked_stall_needs_matching_names_a_cap_and_whole_rows() {
    // Census names differ from the stuck list.
    let renamed = CI_SUMMARY.replace("stream/20", "stream/21");
    assert!(!is_given_up_leader_stall(&ci_finding(&renamed, &CI_ROWS)));
    // Four stuck shards: more than the checker prints censuses for.
    let four = "4 of 178 shards are not served by exactly one broker 60s after the faults healed: stream/6 (0 serving), stream/20 (0 serving), stream/7 (0 serving), stream/8 (0 serving)";
    let mut finding = ci_finding(four, &CI_ROWS);
    finding = finding
        .with(ci_census("stream/7", 3, &CI_ROWS))
        .with(ci_census("stream/8", 4, &CI_ROWS));
    assert!(!is_given_up_leader_stall(&finding));
    // A node row with no phase (a node that is down).
    let mut short = CI_ROWS;
    short[0] = "  b1    down      no    no     -    -        ";
    assert!(!is_given_up_leader_stall(&ci_finding(CI_SUMMARY, &short)));
}

/// A history violation beside the tracked stall still fails the run.
#[test]
fn a_history_violation_still_fails_beside_the_tracked_stall() {
    let stall = stall_finding(SUMMARY, ASSIGNING, &GIVEN_UP);
    let mut report = Report {
        violations: Vec::new(),
        faults: Vec::new(),
        undecided: Vec::new(),
    };
    assert!(only_the_2206_stall(&report, std::slice::from_ref(&stall)));
    report.violations.push(Violation {
        rule: Rule::LostWrite,
        list: "stream/6".into(),
        ops: Vec::new(),
        explanation: String::new(),
    });
    assert!(!only_the_2206_stall(&report, std::slice::from_ref(&stall)));
    report.violations.clear();
    report.undecided.push("key".into());
    assert!(!only_the_2206_stall(&report, std::slice::from_ref(&stall)));
    report.undecided.clear();
    let other = Finding::new("record-generations-agree", "differs");
    assert!(!only_the_2206_stall(&report, &[stall, other]));
}

fn final_reads(lists: &str) -> Finding {
    Finding::new(
        "final-reads",
        format!("no final read for 2 shards, left out of the history check: {lists}"),
    )
}

#[test]
fn a_missing_final_read_is_exempt_only_on_the_stalled_shards() {
    let stall = ci_finding(CI_SUMMARY, &CI_ROWS);
    let report = Report {
        violations: Vec::new(),
        faults: Vec::new(),
        undecided: Vec::new(),
    };
    // The stalled shards are stream/6 and stream/20.
    let same = final_reads("events/20, events/6");
    assert!(only_the_2206_stall(&report, &[stall.clone(), same]));
    // One shard the stall does not name.
    let different = final_reads("events/20, events/7");
    assert!(!only_the_2206_stall(&report, &[stall.clone(), different]));
    // No stall in the run: nothing to exempt the read against.
    assert!(!only_the_2206_stall(&report, &[final_reads("events/6")]));
    // A history violation beside the exempt read still fails.
    let mut bad = Report {
        violations: Vec::new(),
        faults: Vec::new(),
        undecided: vec!["key".into()],
    };
    assert!(!only_the_2206_stall(
        &bad,
        &[stall.clone(), final_reads("events/6")]
    ));
    bad.undecided.clear();
    bad.violations.push(Violation {
        rule: Rule::LostWrite,
        list: "stream/6".into(),
        ops: Vec::new(),
        explanation: String::new(),
    });
    assert!(!only_the_2206_stall(
        &bad,
        &[stall, final_reads("events/6")]
    ));
}

// Placement faults. Each test pins a seed whose run reaches one shape, checks
// that the run did reach it, and that the seed is the same run twice. They
// assert nothing about what the checkers found: the seeds that fail are
// listed in docs/simulation.md.

/// Run `seed` with placement faults twice, and fail unless both runs logged
/// every one of `marks` and are the same event for event. A `*` in a mark
/// stands for any text, so a mark can name a shape without the shard and
/// generation that any change to the broker's code may move.
fn placement_shape_runs_the_same_twice(seed: u64, marks: &[&str]) {
    log();
    let config = SimConfig::for_seed(seed).with_placement_faults();
    let first = run_seed(seed, config.clone());
    for mark in marks {
        assert!(
            first.history.faults.iter().any(|f| logged(&f.what, mark)),
            "seed {seed} never logged `{mark}`:\n{first}"
        );
    }
    let second = run_seed(seed, config);
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed {seed} ran differently the second time: {difference}");
    }
}

/// Whether `what` holds each `*`-separated part of `mark`, in order.
fn logged(what: &str, mark: &str) -> bool {
    let mut rest = what;
    mark.split('*').all(|part| match rest.find(part) {
        Some(at) => {
            rest = &rest[at + part.len()..];
            true
        }
        None => false,
    })
}

#[test]
fn a_mark_matches_its_parts_in_order() {
    let what = "kill b2, named leader of stream/4 at generation 5, before it opens";
    assert!(logged(what, "named leader of stream/4 at generation 5"));
    assert!(logged(what, "named leader of stream/*, before it opens"));
    assert!(!logged(what, "elected leader of stream/*, before it opens"));
    assert!(!logged(what, "before it opens*named leader"));
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
fn seed_44_abandoning_held_shards_replays_the_same_twice() {
    placement_shape_runs_the_same_twice(44, &["abandoned the log of stream/*: 200"]);
}

#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_7_kills_between_promotion_and_open_replay_the_same_twice() {
    placement_shape_runs_the_same_twice(
        7,
        &[
            "named leader of stream/*, before it opens",
            "elected leader of stream/*, before it opens",
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
            "seat race: stream/* replaces b* with b*; stop its leader b*",
            "named leader of stream/*, before it opens",
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

/// A crash that fires as the faults heal must not leave its broker down: the
/// heal has already restarted everyone, so the crash's own restart is the
/// only one left.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seeds_210_and_216_with_power_loss_serve_every_shard_after_a_crash_at_the_heal() {
    for seed in [210, 216] {
        log();
        let outcome = run_seed(seed, SimConfig::for_seed(seed).with_power_loss());
        assert!(!stalled(&outcome), "seed {seed} stalled:\n{outcome}");
        assert!(
            !found(&outcome, "election-settles"),
            "seed {seed}:\n{outcome}"
        );
        assert!(!found(&outcome, "final-reads"), "seed {seed}:\n{outcome}");
    }
}

/// Whether a fault took an acknowledged record it may not lose.
fn lost_acked(outcome: &SimOutcome) -> bool {
    outcome
        .failures()
        .any(|finding| finding.checker == "acked-on-some-disk")
}

/// A power cut under a relaxed fsync mode loses what the broker acknowledged
/// without syncing, so its restart sees a new boot, takes a new storage id and
/// runs the unclean-start handling; no acknowledged record ends up on no disk.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_3_relaxed_power_cuts_make_the_restart_distrust_its_copies() {
    log();
    let config = SimConfig::for_seed(3).with_relaxed_power_loss();
    let first = run_seed(3, config.clone());
    let second = run_seed(3, config);
    if let Some(difference) = first_difference(&first.trace(), &second.trace()) {
        panic!("seed 3 ran differently the second time: {difference}");
    }
    let cut = first
        .history
        .faults
        .iter()
        .any(|fault| fault.what.contains("lost power"));
    assert!(cut, "no broker lost power:\n{first}");
    assert!(!lost_acked(&first), "{first}");
    assert!(first.report.violations.is_empty(), "{first}");
}

/// Several brokers lose power in the same instant, and nothing acknowledged
/// is lost when they sync on commit.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_3_a_majority_cut_under_sync_on_commit_loses_nothing_acknowledged() {
    log();
    let outcome = run_seed(3, SimConfig::for_seed(3).with_majority_cut());
    let cut = outcome
        .history
        .faults
        .iter()
        .any(|fault| fault.what.contains("lose power together"));
    assert!(cut, "no majority cut ran:\n{outcome}");
    assert!(!lost_acked(&outcome), "{outcome}");
    assert!(outcome.report.violations.is_empty(), "{outcome}");
}

/// Wipes of a shard directory and of a data directory run, and every record
/// they take off the disks is one they may lose.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "replays this seed's run only under --cfg tokio_unstable"
)]
fn seed_3_wipes_lose_only_what_every_disk_holding_it_lost() {
    log();
    let outcome = run_seed(3, SimConfig::for_seed(3).with_wipes());
    let wipes = outcome
        .history
        .faults
        .iter()
        .filter(|fault| fault.what.starts_with("wipe "))
        .count();
    assert!(wipes > 0, "no wipe ran:\n{outcome}");
    assert!(!lost_acked(&outcome), "{outcome}");
    assert!(outcome.report.violations.is_empty(), "{outcome}");
}
