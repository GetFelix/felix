use std::net::SocketAddr;

use felix_router::{NodeRef, Route};

use super::*;

fn node(node_id: &str) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: "10.0.0.1:7000".parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn route(replicas: &[&str], successor: Option<&str>) -> Route {
    Route {
        leader: node("broker-a"),
        replicas: replicas.iter().map(|id| node(id)).collect(),
        generation: 4,
        draining: false,
        successor: successor.map(str::to_string),
    }
}

fn cursors(nodes: &[&str]) -> Vec<FollowerCursor> {
    let addr: SocketAddr = "10.0.0.1:7000".parse().expect("addr");
    nodes
        .iter()
        .map(|id| FollowerCursor::new(*id, addr, 0))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn unlimited_never_waits() {
    let throttle = MoveThrottle::unlimited();
    throttle.charge(u64::MAX / 2);
    assert_eq!(throttle.wait(), Duration::ZERO);
}

/// A batch goes whole and is charged after; the next waits until the
/// bucket is back above zero, which keeps the average at the limit.
#[tokio::test(start_paused = true)]
async fn a_batch_past_the_budget_holds_the_next_one_back() {
    let throttle = MoveThrottle::new(1_000);
    assert_eq!(
        throttle.wait(),
        Duration::ZERO,
        "starts with a second's worth"
    );

    throttle.charge(1_500);
    assert_eq!(throttle.wait(), Duration::from_millis(500));

    tokio::time::advance(Duration::from_millis(300)).await;
    assert_eq!(throttle.wait(), Duration::from_millis(200));
    tokio::time::advance(Duration::from_millis(200)).await;
    assert_eq!(throttle.wait(), Duration::ZERO);
}

/// Idle time earns at most a second's worth, so a copy that starts after a
/// quiet hour is not let through at full speed.
#[tokio::test(start_paused = true)]
async fn idle_time_earns_at_most_a_seconds_worth() {
    let throttle = MoveThrottle::new(1_000);
    tokio::time::advance(Duration::from_secs(3600)).await;
    throttle.charge(2_000);
    assert_eq!(throttle.wait(), Duration::from_secs(1));
}

/// A pass waits at most its slice on the limit, then leaves the copy to
/// the next pass instead of holding every other follower's next pass back.
#[tokio::test(start_paused = true)]
async fn a_pass_waits_at_most_its_slice() {
    let throttle = MoveThrottle::new(1_000);
    throttle.charge(3_000);
    let started = Instant::now();
    assert!(!throttle.pace(started).await, "two seconds of debt");
    assert_eq!(started.elapsed(), PACE_SLICE);

    let throttle = MoveThrottle::new(1_000);
    throttle.charge(1_010);
    let started = Instant::now();
    assert!(
        throttle.pace(started).await,
        "ten milliseconds fit the slice"
    );
    assert_eq!(started.elapsed(), Duration::from_millis(10));
}

/// Only a destination the rest of the set can make a majority without is
/// paced: with one replica and a successor, a `Quorum` publish needs the
/// successor, so it is left alone.
#[test]
fn a_destination_the_quorum_needs_is_not_paced() {
    let alone = route(&["broker-b"], Some("broker-b"));
    assert_eq!(paced_destination(&alone, &cursors(&["broker-b"])), None);

    let three = route(&["broker-b", "broker-c", "broker-d"], Some("broker-d"));
    let followers = cursors(&["broker-b", "broker-c", "broker-d"]);
    assert_eq!(paced_destination(&three, &followers), Some("broker-d"));

    // One of the others halted: the successor may be needed after all.
    let mut halted = followers;
    halted[0].halted = Some(crate::Halt::Diverged);
    assert_eq!(paced_destination(&three, &halted), None);

    let no_move = route(&["broker-b", "broker-c"], None);
    assert_eq!(
        paced_destination(&no_move, &cursors(&["broker-b", "broker-c"])),
        None
    );
}
