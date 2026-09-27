//! A deregistered leader keeps its shards until the lease it was last granted
//! has run out, measured the way expiry measures a silent node.
use super::reconciler::{cluster, report, shard_zero};
use super::*;
use crate::config::NodeLivenessConfig;
use crate::store::ControlPlaneStore;
use crate::store::memory::InMemoryStore;

/// `orders` as one shard led by broker-x, with y and z reported caught up,
/// and broker-x last heartbeating `silent_for_ms` before now.
async fn led_by_x(silent_for_ms: u64) -> InMemoryStore {
    let store = cluster(&["broker-x", "broker-y", "broker-z"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(replicated_stream("orders", 1, 3))
        .await
        .expect("stream");
    let placed = store
        .put_shard_assignment(ShardAssignment {
            generation: 0,
            state: ShardState::Assigning,
            ..assigned("orders", "broker-x", &["broker-y", "broker-z"])
        })
        .await
        .expect("placed");
    report(&store, placed.generation, &["broker-y", "broker-z"], false).await;
    let now = store.now_millis().await.expect("clock");
    store
        .record_node_heartbeat("broker-x", 0, now - silent_for_ms)
        .await
        .expect("beat");
    store
}

async fn deregister(store: &InMemoryStore, node_id: &str) {
    store
        .set_node_lifecycle(node_id, NodeLifecycle::Left)
        .await
        .expect("leave");
}

async fn leader(store: &InMemoryStore) -> String {
    store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get")
        .leader
}

/// **The fence.** Deregistering a leader that heartbeated a moment ago does
/// not promote a follower: the broker may serve on the lease it holds, and
/// two leaders is what promoting now would make.
#[tokio::test]
async fn a_leader_deregistered_inside_its_lease_keeps_its_shard() {
    let store = led_by_x(0).await;
    deregister(&store, "broker-x").await;

    let outcome = reconcile_once(&store, &Default::default(), MovePolicy::default()).await;

    assert_eq!(outcome.placed, 0, "a follower was promoted: {outcome:?}");
    assert_eq!(outcome.failed, 0, "{outcome:?}");
    assert_eq!(leader(&store).await, "broker-x");
}

/// Past the expiry timeout is not enough. A broker gives its lease up a
/// quarter early by its own clock, and the regrant margin is what covers the
/// two clocks disagreeing; deregistering must not skip it.
#[tokio::test]
async fn the_regrant_margin_is_waited_out_too() {
    let liveness = NodeLivenessConfig::default();
    let store = led_by_x(liveness.expiry_timeout_ms + 100).await;
    deregister(&store, "broker-x").await;

    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;

    assert_eq!(outcome.placed, 0, "promoted inside the margin: {outcome:?}");
    assert_eq!(outcome.failed, 0, "{outcome:?}");
    assert_eq!(leader(&store).await, "broker-x");
}

/// Once the lease is provably over, the shard fails over to a caught-up
/// follower, as it would for a node found down.
#[tokio::test]
async fn a_leader_whose_lease_has_run_out_is_failed_over() {
    let liveness = NodeLivenessConfig::default();
    let store = led_by_x(liveness.silence_before_down_ms() + 1).await;
    deregister(&store, "broker-x").await;

    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;

    assert_eq!(outcome.placed, 1, "{outcome:?}");
    assert_ne!(leader(&store).await, "broker-x");
}

/// A broker still running after it was deregistered keeps heartbeating. The
/// answer grants it nothing, so it must not push the failover back either,
/// or its shards would wait for as long as the process lives.
#[tokio::test]
async fn heartbeats_after_leaving_do_not_hold_the_shard() {
    let liveness = NodeLivenessConfig::default();
    let store = led_by_x(liveness.silence_before_down_ms() + 1).await;
    deregister(&store, "broker-x").await;
    let now = store.now_millis().await.expect("clock");
    store
        .record_node_heartbeat("broker-x", 0, now)
        .await
        .expect("beat");

    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;

    assert_eq!(outcome.placed, 1, "{outcome:?}");
    assert_ne!(leader(&store).await, "broker-x");
}

/// Abandoning the log is no way around the fence: while the owner may still
/// serve, the shard is not waiting on a lost log.
#[tokio::test]
async fn an_unreplicated_shard_cannot_be_abandoned_inside_the_lease() {
    let liveness = NodeLivenessConfig::default();
    let store = cluster(&["broker-x", "broker-y"]).await;
    reconcile_once(&store, &liveness, MovePolicy::default()).await;
    let key = store
        .list_shard_assignments()
        .await
        .expect("list")
        .into_iter()
        .find(|a| a.leader == "broker-x")
        .expect("broker-x leads a shard")
        .key;
    let now = store.now_millis().await.expect("clock");
    store
        .record_node_heartbeat("broker-x", 0, now)
        .await
        .expect("beat");
    deregister(&store, "broker-x").await;

    let read = PlacementRead::load(&store, &liveness).await.expect("read");
    assert!(
        abandon_log(&read.catalog(MovePolicy::default()), &key).is_err(),
        "the log was abandoned while its owner may still serve it",
    );
}
