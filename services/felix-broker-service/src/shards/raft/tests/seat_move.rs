//! Where requests go while a move outside the set brings a new broker in
//! (#3011).

use crate::shards::watch::RaftVoters;

use super::*;

fn voters_on(brokers: &[&str]) -> RaftVoters {
    RaftVoters {
        incarnation: INCARNATION,
        epoch: 0,
        members: brokers
            .iter()
            .enumerate()
            .map(|(copy, broker)| (broker.to_string(), copy as u64 + 1))
            .collect(),
        starts: Default::default(),
    }
}

fn placed(leader: &str, voters: &[&str]) -> Placed {
    Placed {
        leader: leader.to_string(),
        voters: voters.iter().map(|broker| broker.to_string()).collect(),
    }
}

/// **A move's destination with no member sends requests to a voter, not
/// "not ready".** Placement names broker-a leader before the group has added
/// it; broker-b and broker-c are the voters its leader reported.
#[tokio::test]
async fn a_destination_with_no_member_forwards_to_a_reported_voter() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = held_shards(&broker);
    let orders = key("orders", 0);
    let mut moved = assigned(orders.clone(), INCARNATION);
    moved.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    moved.raft_voters = Some(voters_on(&["broker-b", "broker-c"]));
    shards.take(&mut assignments([moved.clone()])).await;
    assert!(shards.engine_for(&orders).is_none());
    let addr: SocketAddr = "127.0.0.1:7000".parse().expect("addr");
    match shards.dispatch(&orders, &|_| Some(addr)) {
        Some(Dispatch::Forward { node_id, .. }) => assert_eq!(node_id, "broker-b"),
        other => panic!("expected a forward to broker-b, got {other:?}"),
    }

    // Another incarnation's voters say nothing about this group.
    moved.raft_voters = Some(RaftVoters {
        incarnation: INCARNATION + 1,
        ..voters_on(&["broker-b"])
    });
    shards.take(&mut assignments([moved])).await;
    assert!(matches!(
        shards.dispatch(&orders, &|_| Some(addr)),
        Some(Dispatch::Unavailable(Reason::NotReady))
    ));
    shards.shutdown().await;
}

/// **Every broker skips a placed leader the group does not hold yet.** A
/// voter placed here whose member is still opening is the one that will
/// serve, so it alone answers not ready.
#[test]
fn routes_skip_a_placed_leader_that_is_not_a_voter() {
    // Before any report: placement's leader, as before.
    assert_eq!(placed("broker-a", &[]).route("broker-b"), Some("broker-a"));
    assert_eq!(placed("broker-a", &[]).route("broker-a"), None);
    assert_eq!(placed("", &[]).route("broker-a"), None);
    // A voter leads in placement: everyone else goes there.
    let settled = placed("broker-b", &["broker-b", "broker-c"]);
    assert_eq!(settled.route("broker-d"), Some("broker-b"));
    assert_eq!(settled.route("broker-c"), Some("broker-b"));
    assert_eq!(settled.route("broker-b"), None);
    // Mid move: broker-a is named but holds no voter yet.
    let moving = placed("broker-a", &["broker-b", "broker-c"]);
    assert_eq!(moving.route("broker-d"), Some("broker-b"));
    assert_eq!(moving.route("broker-a"), Some("broker-b"));
    assert_eq!(moving.route("broker-b"), Some("broker-c"));
}

/// **A member that was elected here but is not leading yet answers not
/// ready, though the voter report still names others.** Forwarding would
/// bounce between it and the voter that knows it leads.
#[tokio::test]
async fn an_elected_member_not_leading_yet_does_not_forward() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards.stall_leading(true);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    take_formed(&shards, assigned(orders.clone(), INCARNATION)).await;
    until("the member leads", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    assert_eq!(shards.leading(&orders), None);
    shards.placed.lock().insert(
        orders.clone(),
        placed("broker-a", &["broker-b", "broker-c"]),
    );
    assert_eq!(
        shards.dispatch(&orders, &|name| addr_of(name)),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    EngineShards::shutdown(&*shards).await;
}

/// **A member that knows no leader and is not a reported voter forwards to
/// one.** Its peers are absent, so it never hears a leader.
#[tokio::test]
async fn a_non_voter_member_with_no_leader_forwards_to_a_voter() {
    let net = Arc::new(Loopback::default());
    let member = Member::start_with(
        &net,
        "broker-a",
        INCARNATION,
        ConsistencyLevel::Quorum,
        FsyncMode::None,
        false,
    )
    .await;
    let orders = key("orders", 0);
    member.shards.take(&mut assignments([replicated()])).await;
    until("the member opens", || member.shards.runtime.holds(&orders)).await;
    assert!(
        member
            .shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.leader_hint.is_none())
    );
    member.shards.placed.lock().insert(
        orders.clone(),
        placed("broker-a", &["broker-b", "broker-c"]),
    );
    match member.shards.dispatch(&orders, &|name| addr_of(name)) {
        Some(Dispatch::Forward { node_id, .. }) => assert_eq!(node_id, "broker-b"),
        other => panic!("expected a forward to broker-b, got {other:?}"),
    }
    stop(&[member]).await;
}

/// **Voters reported for another epoch count for nothing.**
#[test]
fn placement_ignores_voters_from_another_epoch() {
    let mut moved = assigned(key("orders", 0), INCARNATION);
    moved.raft_voters = Some(voters_on(&["broker-b"]));
    assert_eq!(Placed::of(&moved).voters.len(), 1);
    moved.raft_voters = Some(RaftVoters {
        epoch: 1,
        ..voters_on(&["broker-b"])
    });
    assert!(Placed::of(&moved).voters.is_empty());
    moved.raft_voters = Some(RaftVoters {
        incarnation: INCARNATION + 1,
        ..voters_on(&["broker-b"])
    });
    assert!(Placed::of(&moved).voters.is_empty());
}
