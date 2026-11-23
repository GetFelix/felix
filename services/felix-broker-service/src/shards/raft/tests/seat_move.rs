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
