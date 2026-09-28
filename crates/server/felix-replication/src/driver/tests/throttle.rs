//! A move's destination is shipped at the bandwidth limit; the followers the
//! quorum needs are not.

use super::*;

/// A router where the local broker leads the shard and is moving it to
/// `successor`, still copying.
fn moving_router(replicas: &[&str], successor: &str) -> Arc<ShardRouter> {
    moving_router_at(replicas, successor, 4)
}

fn moving_router_at(replicas: &[&str], successor: &str, generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: LOCAL.to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
            draining: false,
            successor: Some(successor.to_string()),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

async fn pass(
    follower: &AcceptingFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    throttle: &MoveThrottle,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
) {
    replicate_once_with(
        follower,
        broker,
        router,
        &Unfenced,
        &QuorumMarks::new(),
        None,
        cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &Rebuilds::disabled(),
        throttle,
    )
    .await;
}

fn shipped_to(follower: &AcceptingFollower, node: &str) -> usize {
    follower
        .batches()
        .iter()
        .filter(|(to, _, _)| to == node)
        .map(|(_, _, records)| records)
        .sum()
}

async fn append(broker: &Broker, count: usize) {
    let log = broker
        .durable_storage()
        .expect("durable")
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for i in 0..count {
        log.append(&[Bytes::from(format!("w{i}"))])
            .await
            .expect("append");
    }
}

/// With the limit spent, the destination waits for the next pass while the
/// follower the quorum counts on gets everything at once.
#[tokio::test]
async fn a_move_destination_is_held_to_the_limit() {
    let (broker, _dir) = leader_with(3).await;
    let router = moving_router(&["broker-b", "broker-c"], "broker-c");
    let follower = AcceptingFollower::default();
    // Four bytes a second: the first batch spends it, and the next is
    // seconds away.
    let throttle = MoveThrottle::new(4);
    let mut cursors = HashMap::new();

    pass(&follower, &broker, &router, &throttle, &mut cursors).await;
    assert_eq!(shipped_to(&follower, "broker-b"), 3);
    assert_eq!(shipped_to(&follower, "broker-c"), 3, "the first batch goes");

    append(&broker, 3).await;
    let started = std::time::Instant::now();
    pass(&follower, &broker, &router, &throttle, &mut cursors).await;
    assert_eq!(shipped_to(&follower, "broker-b"), 6);
    assert_eq!(shipped_to(&follower, "broker-c"), 3, "held to the limit");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "the pass waited its slice, not the whole debt: {:?}",
        started.elapsed()
    );
}

/// With one replica besides the destination, a `Quorum` publish needs the
/// destination: it is not held to the limit.
#[tokio::test]
async fn a_destination_the_quorum_needs_is_not_held_back() {
    let (broker, _dir) = leader_with(3).await;
    let router = moving_router(&["broker-c"], "broker-c");
    let follower = AcceptingFollower::default();
    let throttle = MoveThrottle::new(4);
    let mut cursors = HashMap::new();

    pass(&follower, &broker, &router, &throttle, &mut cursors).await;
    append(&broker, 3).await;
    pass(&follower, &broker, &router, &throttle, &mut cursors).await;
    assert_eq!(shipped_to(&follower, "broker-c"), 6);
}

/// A destination staged as a learner is left out of the quorum, so it is
/// paced even as the shard's only follower.
#[tokio::test]
async fn a_learner_is_held_to_the_limit() {
    let (broker, _dir) = leader_with(3).await;
    let follower = AcceptingFollower::default();
    let throttle = MoveThrottle::new(4);
    let mut cursors = HashMap::new();

    // Led alone first, so the destination is one the move added.
    pass(
        &follower,
        &broker,
        &router(LOCAL, &[], 3),
        &throttle,
        &mut cursors,
    )
    .await;
    let moving = moving_router_at(&["broker-c"], "broker-c", 4);
    pass(&follower, &broker, &moving, &throttle, &mut cursors).await;
    assert_eq!(shipped_to(&follower, "broker-c"), 3, "the first batch goes");

    append(&broker, 3).await;
    pass(&follower, &broker, &moving, &throttle, &mut cursors).await;
    assert_eq!(shipped_to(&follower, "broker-c"), 3, "held to the limit");
}

/// After the fence the shard is not served until the remainder is across,
/// so the remainder is never paced: a publish held for the cut-over would
/// otherwise wait on the limit.
#[tokio::test]
async fn the_remainder_after_the_fence_is_not_paced() {
    let (broker, _dir) = leader_with(3).await;
    let follower = AcceptingFollower::default();
    let throttle = MoveThrottle::new(4);
    let mut cursors = HashMap::new();

    let copying = moving_router(&["broker-b", "broker-c"], "broker-c");
    pass(&follower, &broker, &copying, &throttle, &mut cursors).await;
    assert_eq!(
        shipped_to(&follower, "broker-c"),
        3,
        "the first batch spends the limit"
    );

    append(&broker, 3).await;
    let fenced = draining_router(LOCAL, &["broker-c", "broker-b"], 4);
    pass(&follower, &broker, &fenced, &throttle, &mut cursors).await;
    assert_eq!(
        shipped_to(&follower, "broker-c"),
        6,
        "not held to the limit"
    );
}
