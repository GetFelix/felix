//! Several instances placing over one store: the lease decides who runs the
//! timed passes, and the placement token keeps the move limits whoever
//! writes.
use std::sync::Arc;
use std::time::Duration;

use super::reconciler::cluster;
use super::*;
use crate::store::ControlPlaneStore;
use crate::store::memory::InMemoryStore;

/// Copies in flight, as the limits count them.
async fn moving(store: &InMemoryStore) -> usize {
    store
        .list_shard_assignments()
        .await
        .expect("list")
        .iter()
        .filter(|assignment| super::super::operator::moving(assignment))
        .count()
}

/// Three shards on one broker, then a second broker joins: placement wants
/// to move one shard over, and there is one slot.
async fn imbalanced() -> InMemoryStore {
    let store = cluster(&["broker-x"]).await;
    let placed = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;
    assert_eq!(placed.placed, 3);
    let mut joining = node("broker-y", NodeLifecycle::Live, None);
    joining.spec.advertise_addr = "10.0.0.4:7700".to_string();
    store.register_node(joining).await.expect("join");
    store
}

/// One instance plans a pass while an operator's request on another starts
/// a move on a different shard. Each decided from a free slot and each
/// shard's generation is untouched by the other, so without the token both
/// land and two copies run under a limit of one.
#[tokio::test]
async fn a_pass_planned_before_an_operators_move_starts_nothing_beside_it() {
    let store = imbalanced().await;
    let liveness = Default::default();
    let policy = MovePolicy::default();
    assert_eq!(policy.max_concurrent, 1);

    let pass = super::super::reconciler::plan_pass(
        &store,
        &liveness,
        policy.clone(),
        &mut Default::default(),
    )
    .await
    .expect("plan");
    let planned: Vec<ShardKey> = pass.plan().moves().map(|(key, _, _)| key.clone()).collect();
    assert_eq!(planned.len(), 1, "one move planned: {planned:?}");
    let other = (0..3)
        .map(|shard| ShardKey {
            shard,
            ..planned[0].clone()
        })
        .find(|key| key != &planned[0])
        .expect("another shard");

    run_operator(
        &store,
        &liveness,
        policy,
        &PlacementWakes::default(),
        |catalog| start_move(catalog, &other, "broker-y"),
    )
    .await
    .expect("the operator's move starts");

    let applied = super::super::reconciler::apply_pass(
        &store,
        &pass,
        &mut super::super::metrics::MoveClock::default(),
        &PlacementWakes::default(),
    )
    .await;
    assert!(applied.fenced, "{applied:?}");
    assert_eq!(applied.moved, 0, "{applied:?}");
    assert_eq!(moving(&store).await, 1, "copies in flight");
}

/// An instance that held the lease, planned, and then paused past it must
/// not write once another instance has taken over.
#[tokio::test]
async fn an_ex_holders_pass_is_fenced_after_a_takeover() {
    let store = imbalanced().await;
    let liveness = Default::default();
    store
        .acquire_placement_lease("instance-a", 50)
        .await
        .expect("acquire")
        .expect("granted");
    let pass = super::super::reconciler::plan_pass(
        &store,
        &liveness,
        MovePolicy::default(),
        &mut Default::default(),
    )
    .await
    .expect("plan");
    assert_eq!(pass.plan().moves().count(), 1);

    tokio::time::sleep(Duration::from_millis(150)).await;
    store
        .acquire_placement_lease("instance-b", 60_000)
        .await
        .expect("acquire")
        .expect("an expired lease is taken over");

    let applied = super::super::reconciler::apply_pass(
        &store,
        &pass,
        &mut super::super::metrics::MoveClock::default(),
        &PlacementWakes::default(),
    )
    .await;
    assert!(applied.fenced, "{applied:?}");
    assert_eq!(moving(&store).await, 0, "the ex-holder's move landed");
}

/// Two instances' placement loops over one store: one holds the lease the
/// whole time the other runs, and when it stops the other takes over.
#[tokio::test]
async fn the_lease_stays_with_one_loop_and_moves_when_it_stops() {
    let store = Arc::new(imbalanced().await);
    let interval = Duration::from_millis(20);
    let spawn = |holder: &str, shutdown: &tokio_util::sync::CancellationToken| {
        spawn_reconciler(
            Arc::clone(&store) as _,
            Default::default(),
            MovePolicy::default(),
            interval,
            holder.to_string(),
            crate::raft::LeadershipGate::Always,
            Arc::new(PlacementWakes::default()),
            shutdown.clone(),
        )
    };
    let stop_a = tokio_util::sync::CancellationToken::new();
    let stop_b = tokio_util::sync::CancellationToken::new();
    let a = spawn("instance-a", &stop_a);
    wait_for_holder(&store, "instance-a").await;
    let b = spawn("instance-b", &stop_b);

    for _ in 0..10 {
        tokio::time::sleep(interval).await;
        assert_eq!(
            store.placement_holder().await.as_deref(),
            Some("instance-a"),
            "the lease changed hands while its holder ran"
        );
    }

    stop_a.cancel();
    a.await.expect("a stops");
    wait_for_holder(&store, "instance-b").await;
    stop_b.cancel();
    b.await.expect("b stops");
}

async fn wait_for_holder(store: &InMemoryStore, holder: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while store.placement_holder().await.as_deref() != Some(holder) {
        assert!(
            std::time::Instant::now() < deadline,
            "{holder} never took the lease"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
