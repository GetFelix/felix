use std::time::Duration;

use felix_broker_service::sim::ShardStand;

use super::*;
use crate::check::Severity;
use crate::controlplane::CpFault;
use crate::nemesis::{Fault, Step};

fn routed(node: usize, generation: u64, leads: bool) -> Seen {
    Seen {
        node,
        name: format!("b{}", node + 1),
        stand: ShardStand {
            serving: leads,
            generation: leads.then_some(generation),
            routed_leader: Some(("b1".to_string(), generation)),
            opened_generation: leads.then_some(generation),
            acks_by_followers: true,
        },
    }
}

fn crash(node: usize, at: u64) -> Step {
    Step {
        at: Duration::from_secs(at),
        fault: Fault::Crash {
            node,
            down_for: Duration::from_secs(5),
            kill: false,
        },
    }
}

fn plan(steps: Vec<Step>) -> Plan {
    Plan { steps }
}

/// A shard led by b1 that moved from generation 1 to `latest`.
fn burned_to(latest: u64) -> GenerationBurn {
    let mut burn = GenerationBurn::default();
    burn.observe("stream/0", &[routed(0, 1, true)]);
    burn.observe("stream/0", &[routed(0, latest, true)]);
    burn
}

#[test]
fn many_more_generations_than_the_faults_explain_is_a_burn() {
    // One fault on the leader allows 2 * 1 + 2 = 4.
    let burn = burned_to(1 + 5);
    let found = burn.burned(&plan(vec![crash(0, 10)]));
    assert_eq!(found, vec![("stream/0".to_string(), 5, 4, 1)]);
}

#[test]
fn generations_within_the_bound_are_not() {
    assert!(
        burned_to(1 + 4)
            .burned(&plan(vec![crash(0, 10)]))
            .is_empty()
    );
    // No faults: only the base allowance.
    assert!(burned_to(1 + 2).burned(&plan(Vec::new())).is_empty());
    assert_eq!(burned_to(1 + 3).burned(&plan(Vec::new())).len(), 1);
}

#[test]
fn faults_that_cannot_unseat_the_leader_do_not_widen_the_bound() {
    let slow = Step {
        at: Duration::from_secs(1),
        fault: Fault::ControlPlane {
            fault: CpFault::Slow(Duration::from_secs(2)),
            lasts: Duration::from_secs(5),
        },
    };
    // b3 never led the shard.
    let plan = plan(vec![crash(2, 4), slow]);
    assert_eq!(burned_to(1 + 5).burned(&plan).len(), 1);
}

#[test]
fn a_paused_control_plane_counts_against_every_shard() {
    let paused = Step {
        at: Duration::from_secs(1),
        fault: Fault::ControlPlane {
            fault: CpFault::Paused,
            lasts: Duration::from_secs(5),
        },
    };
    assert!(burned_to(1 + 4).burned(&plan(vec![paused])).is_empty());
}

#[test]
fn the_worst_shard_comes_first() {
    let mut burn = GenerationBurn::default();
    for (shard, latest) in [("stream/0", 6), ("stream/1", 9)] {
        burn.observe(shard, &[routed(0, 1, true)]);
        burn.observe(shard, &[routed(0, latest, true)]);
    }
    let found = burn.burned(&Plan::default());
    assert_eq!(found[0].0, "stream/1");
    assert_eq!(found[1].0, "stream/0");
}

#[test]
fn a_burn_is_a_warning_and_does_not_fail_the_run() {
    let warning = Finding::warning("generation-burn", "too many");
    assert_eq!(warning.severity, Severity::Warning);
    assert_eq!(warning.to_string(), "[warning generation-burn] too many\n");
    assert_eq!(Finding::new("x", "y").severity, Severity::Failure);
}
