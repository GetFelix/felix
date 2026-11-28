use std::time::Duration;

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
        incarnation: 0,
        fence_storage_ids: None,
        member_storage_ids: None,
        pending: None,
        readmitted: Default::default(),
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

fn raft_member(role: &str, leader: &str) -> felix_wire::InspectedRaft {
    felix_wire::InspectedRaft {
        role: role.to_string(),
        term: 3,
        leader: Some(leader.to_string()),
        last_index: Some(9),
        committed: Some(9),
        voters: Vec::new(),
        stopped: None,
    }
}

/// A Raft shard's group elects its leader, so the route placement last
/// published can name another broker, or none yet.
#[test]
fn a_raft_member_reports_the_role_its_group_gave_it() {
    let elected = |role, route| {
        build(Observed {
            node_id: Some(ME.to_string()),
            shards: 4,
            route,
            raft: Some(raft_member(
                role,
                if role == "leader" { ME } else { "broker-b" },
            )),
            ..Observed::default()
        })
    };
    for route in [None, Some(route("broker-b", 7))] {
        let leader = elected("leader", route.clone());
        assert_eq!(
            (leader.role.as_str(), leader.phase.as_str(), why(&leader)),
            ("leader", "active", (true, None))
        );
        let follower = elected("follower", route);
        assert_eq!(follower.role, "follower");
        assert_eq!(why(&follower), (false, Some("not_assigned_here")));
        assert_eq!(
            follower.detail.as_deref(),
            Some("broker-b leads it at term 3")
        );
    }
}

/// **A Raft leader lists the members its group replicates to**, which no
/// replication board holds, and a follower lists none.
#[test]
fn a_raft_leader_reports_its_groups_members() {
    let member = |node: &str, state: &str| felix_wire::InspectedReplica {
        node_id: node.to_string(),
        role: "follower".to_string(),
        next_offset: None,
        lag: None,
        fence: None,
        state: state.to_string(),
        halted: None,
    };
    let members = vec![
        member("broker-b", "shipping"),
        member("broker-c", "stalled"),
    ];
    let observed = |role: &str| Observed {
        node_id: Some(ME.to_string()),
        shards: 1,
        route: Some(route(ME, 0)),
        raft: Some(raft_member(role, ME)),
        raft_replicas: (role == "leader").then(|| members.clone()),
        ..Observed::default()
    };
    assert_eq!(build(observed("leader")).replicas, members);
    assert!(build(observed("follower")).replicas.is_empty());
}

/// **A Raft shard's leader reports its log's tail.** The log is opened at the
/// shard's incarnation, which the unversioned lookup does not find.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_leader_reports_its_tail() {
    use std::sync::Arc;

    use crate::config::BrokerConfig;
    use crate::serving::quic::ClusterContext;
    use crate::serving::quic::handlers::publish::build_publish_context;
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    fixture::take_formed(&shards, assigned).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let at = fixture::write_batch(&shards, &orders, 1, 0, 0, 1, &[b"one"]).await;
    let publish_ctx = build_publish_context(
        Arc::clone(&broker),
        &BrokerConfig::default(),
        ClusterContext {
            engine: Arc::new(fixture::engine(&shards)),
            ..ClusterContext::default()
        },
    );

    let view = inspect(&broker, &publish_ctx, &orders).await;
    assert!(view.tail.is_some_and(|tail| tail > at), "{:?}", view.tail);
    EngineShards::shutdown(&*shards).await;
}
