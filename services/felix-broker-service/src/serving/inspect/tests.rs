use std::time::Duration;

use felix_replication::status::{FenceStatus, FollowerStatus, ShardStatus};
use felix_router::{NodeRef, Route};

use super::*;

const ME: &str = "broker-a";

fn node(node_id: &str) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: "127.0.0.1:7000".parse().unwrap(),
        region: "local".to_string(),
        live: true,
    }
}

fn route(leader: &str, generation: u64) -> Route {
    Route {
        leader: node(leader),
        replicas: vec![node("broker-a"), node("broker-b"), node("broker-c")],
        generation,
        draining: false,
        successor: None,
    }
}

fn phase(phase: Phase, generation: u64) -> PhaseRecord {
    PhaseRecord {
        phase,
        generation,
        error: None,
    }
}

/// A broker in a cluster, leading the shard at generation 42 in `at`.
fn leading(at: Phase) -> Observed {
    Observed {
        node_id: Some(ME.to_string()),
        shards: 4,
        route: Some(route(ME, 42)),
        phase: Some(phase(at, 42)),
        lease: Some(Lease {
            valid: true,
            remaining: Duration::from_millis(7100),
        }),
        ..Observed::default()
    }
}

fn why(view: &ShardInspection) -> (bool, Option<&str>) {
    (view.serving, view.reason.as_deref())
}

#[test]
fn an_active_leader_with_a_lease_serves() {
    let view = build(leading(Phase::Active));
    assert_eq!(why(&view), (true, None));
    assert_eq!(view.role, "leader");
    assert_eq!(view.phase, "active");
    assert_eq!(
        view.lease,
        Some(InspectedLease {
            held: true,
            remaining_ms: 7100
        })
    );
    let assignment = view.assignment.expect("assignment");
    assert_eq!(assignment.replicas, ["broker-b", "broker-c"]);
}

#[test]
fn each_phase_that_does_not_serve_says_why() {
    for (at, reason) in [
        (Phase::Unassigned, "opening"),
        (Phase::Opening, "opening"),
        (Phase::Fencing, "fencing"),
        (Phase::Failed, "failed"),
        (Phase::Draining, "draining"),
        (Phase::Closed, "draining"),
    ] {
        let view = build(leading(at));
        assert_eq!(why(&view), (false, Some(reason)), "{at:?}");
        assert!(view.detail.is_some(), "{at:?} has no detail");
    }
}

#[test]
fn a_failed_open_carries_its_error() {
    let mut observed = leading(Phase::Failed);
    observed.phase.as_mut().unwrap().error = Some("disk full".to_string());
    assert_eq!(build(observed).detail.as_deref(), Some("disk full"));
}

#[test]
fn a_lapsed_lease_stops_an_active_leader_unless_its_followers_decide() {
    let mut observed = leading(Phase::Active);
    observed.lease = Some(Lease {
        valid: false,
        remaining: Duration::ZERO,
    });
    assert_eq!(why(&build(observed.clone())), (false, Some("lease_lapsed")));
    observed.lease_free = true;
    assert_eq!(why(&build(observed)), (true, None));
}

#[test]
fn an_older_generation_than_the_assignment_is_behind() {
    let mut observed = leading(Phase::Active);
    observed.phase = Some(phase(Phase::Active, 41));
    let view = build(observed);
    assert_eq!(why(&view), (false, Some("behind_generation")));
    assert_eq!(view.generation, Some(41));
}

#[test]
fn a_follower_or_a_stranger_is_not_assigned_here() {
    let mut follower = leading(Phase::Closed);
    follower.route = Some(route("broker-b", 42));
    follower.committed = Some(10);
    let view = build(follower);
    assert_eq!(view.role, "follower");
    assert_eq!(why(&view), (false, Some("not_assigned_here")));
    assert_eq!(
        view.detail.as_deref(),
        Some("broker-b leads it at generation 42")
    );
    // Only a leader's mark is a commit mark.
    assert_eq!(view.committed, None);

    let mut stranger = leading(Phase::Unassigned);
    stranger.route = None;
    let view = build(stranger);
    assert_eq!(view.role, "none");
    assert_eq!(why(&view), (false, Some("not_assigned_here")));
}

/// The case the design calls out: a promoted leader none of whose replicas
/// takes the fence.
#[test]
fn a_leader_stuck_fencing_names_who_took_it() {
    let mut observed = leading(Phase::Fencing);
    observed.status = Some(ShardStatus {
        generation: 42,
        tail: None,
        followers: vec![],
        fence: Some(FenceStatus {
            took: vec![],
            pending: vec!["broker-b".to_string(), "broker-c".to_string()],
            attempts: 4,
            retry_at: None,
            why: Some("0 of 2 replicas took the fence".to_string()),
        }),
        drain_pending: false,
        behind: false,
    });
    observed.fence_retry_in = Some(Duration::from_secs(2));
    let view = build(observed);
    assert_eq!(why(&view), (false, Some("fencing")));
    assert_eq!(
        view.detail.as_deref(),
        Some("0 of 2 replicas took the fence")
    );
    let fence = view.fence.expect("fence");
    assert_eq!(fence.attempts, 4);
    assert_eq!(fence.retry_in_ms, Some(2000));
    assert_eq!(fence.pending, ["broker-b", "broker-c"]);
    let states: Vec<_> = view
        .replicas
        .iter()
        .map(|r| (r.node_id.as_str(), r.fence, r.state.as_str()))
        .collect();
    assert_eq!(
        states,
        [
            ("broker-b", Some(false), "fencing"),
            ("broker-c", Some(false), "fencing")
        ]
    );
}

/// A fence recorded for an earlier promotion is not this one's.
#[test]
fn a_fence_from_another_generation_is_not_reported() {
    let mut observed = leading(Phase::Fencing);
    observed.status = Some(ShardStatus {
        generation: 40,
        tail: None,
        followers: vec![],
        fence: Some(FenceStatus {
            took: vec!["broker-b".to_string()],
            pending: vec![],
            attempts: 1,
            retry_at: None,
            why: None,
        }),
        drain_pending: false,
        behind: false,
    });
    let view = build(observed);
    assert_eq!(view.fence, None);
    assert_eq!(
        view.detail.as_deref(),
        Some("waiting for the first fence attempt to finish")
    );
}

#[test]
fn an_active_leader_lists_its_followers() {
    let mut observed = leading(Phase::Active);
    observed.committed = Some(1_048_510);
    observed.status = Some(ShardStatus {
        generation: 42,
        tail: Some(1_048_576),
        followers: vec![
            FollowerStatus {
                node_id: "broker-b".to_string(),
                learner: false,
                next_offset: 1_048_510,
                lag: Some(66),
                state: "shipping",
                halted: None,
            },
            FollowerStatus {
                node_id: "broker-c".to_string(),
                learner: true,
                next_offset: 901_223,
                lag: Some(147_353),
                state: "copying",
                halted: None,
            },
        ],
        fence: None,
        drain_pending: false,
        behind: false,
    });
    let view = build(observed);
    assert_eq!(view.tail, Some(1_048_576));
    assert_eq!(view.committed, Some(1_048_510));
    let rows: Vec<_> = view
        .replicas
        .iter()
        .map(|r| (r.role.as_str(), r.lag, r.state.as_str()))
        .collect();
    assert_eq!(
        rows,
        [
            ("follower", Some(66), "shipping"),
            ("learner", Some(147_353), "copying")
        ]
    );
}

#[test]
fn a_single_broker_leads_what_it_knows() {
    let known = build(Observed {
        shards: 1,
        ..Observed::default()
    });
    assert_eq!(
        (known.role.as_str(), known.serving, known.reason),
        ("leader", true, None)
    );
    let unknown = build(Observed::default());
    assert_eq!(why(&unknown), (false, Some("not_assigned_here")));
}

/// An attempt that stopped for something other than missing answers says so.
#[test]
fn a_fence_stopped_by_a_newer_generation_says_why() {
    let mut observed = leading(Phase::Fencing);
    observed.status = Some(ShardStatus {
        generation: 42,
        tail: None,
        followers: vec![],
        fence: Some(FenceStatus {
            took: vec![],
            pending: vec!["broker-b".to_string(), "broker-c".to_string()],
            attempts: 1,
            retry_at: None,
            why: Some("broker-b has accepted a newer generation: 43".to_string()),
        }),
        drain_pending: false,
        behind: false,
    });
    assert_eq!(
        build(observed).detail.as_deref(),
        Some("0 of 2 replicas took the fence; broker-b has accepted a newer generation: 43")
    );
}
