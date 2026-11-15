//! A Raft shard's copy ids and target, as every backend must record them:
//! the counter rises only when the target's members change, and no target
//! is written while a registered broker has not acknowledged the placement
//! incarnation (`docs/per-shard-raft.md`, "Where a target comes from").
use std::collections::BTreeMap;
use std::sync::Arc;

use super::shards::{clear, on_streams_of_its_own};
use super::switch::{both, read, sealed, written};
use crate::model::{NodeLifecycle, PlacementAck, ShardAssignment};
use crate::store::{ControlPlaneStore, StoreError};

pub(crate) async fn run_raft_target_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        copy_ids_write_the_target_and_only_a_change_moves_it(store).await;
        only_a_broker_in_a_raft_shards_set_reports(store).await;
        reports_of_other_kinds_keep_the_counter(store).await;
        the_target_waits_for_every_registered_broker(store).await;
    })
    .await;
}

/// A shard switched to Raft, led by broker-x with broker-y as its replica.
pub(crate) async fn switched(store: &dyn ControlPlaneStore) -> ShardAssignment {
    let order = sealed(store, 0).await;
    written(
        store
            .accept_seal_report(&order.key, both(order.generation), "broker-x")
            .await
            .expect("report"),
    )
}

async fn report(store: &dyn ControlPlaneStore, a: &ShardAssignment, node: &str, id: u64) -> bool {
    store
        .record_raft_copy_id(&a.key, node, id)
        .await
        .expect("copy id")
}

fn counter(a: &ShardAssignment) -> Option<u64> {
    a.raft_target.as_ref().map(|target| target.counter)
}

async fn copy_ids_write_the_target_and_only_a_change_moves_it(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    assert!(report(store, &shard, "broker-x", 11).await);
    let half = read(store, &shard.key).await;
    assert_eq!(counter(&half), None, "broker-y has not reported");
    assert_eq!(half.generation, shard.generation, "written in place");

    assert!(report(store, &shard, "broker-y", 12).await);
    let first = read(store, &shard.key).await;
    assert_eq!(counter(&first), Some(1));
    assert_eq!(
        first.raft_target.as_ref().expect("target").members,
        BTreeMap::from([("broker-x".to_string(), 11), ("broker-y".to_string(), 12)])
    );

    // The same id again: nothing written.
    assert!(!report(store, &shard, "broker-y", 12).await);
    assert_eq!(read(store, &shard.key).await, first);

    // A wiped copy reopens under a new id.
    assert!(report(store, &shard, "broker-y", 13).await);
    let second = read(store, &shard.key).await;
    assert_eq!(counter(&second), Some(2));
    assert_eq!(second.generation, shard.generation);
    clear(store).await;
}

async fn only_a_broker_in_a_raft_shards_set_reports(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    let err = store
        .record_raft_copy_id(&shard.key, "broker-z", 1)
        .await
        .expect_err("not in the set");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let hybrid = sealed(store, 1).await;
    let err = store
        .record_raft_copy_id(&hybrid.key, "broker-x", 1)
        .await
        .expect_err("not switched");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(read(store, &hybrid.key).await.raft_copy_ids.is_empty());
    clear(store).await;
}

/// A seal report sent again and a replica report leave the target as it is.
async fn reports_of_other_kinds_keep_the_counter(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    report(store, &shard, "broker-x", 21).await;
    report(store, &shard, "broker-y", 22).await;
    let before = read(store, &shard.key).await;
    store
        .accept_seal_report(&shard.key, both(shard.generation), "broker-x")
        .await
        .expect("report again");
    let _ = store
        .record_replica_report(
            super::replica_reports::report(shard.key.shard, shard.generation, &["broker-y"], 0),
            "broker-x",
        )
        .await;
    assert_eq!(read(store, &shard.key).await, before);
    clear(store).await;
}

/// FelixRaftPlacement's AllAcked over every registered broker, serving or
/// not. A broker that never acknowledges blocks the write until it is
/// deregistered.
async fn the_target_waits_for_every_registered_broker(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    let process = |node: &'static str| async move {
        store.get_node(node).await.expect("node").status.incarnation
    };
    let ack = |node: &'static str, incarnation: u64| async move {
        let heard = PlacementAck {
            incarnation,
            node_incarnation: process(node).await,
        };
        store.record_placement_ack(node, heard).await.expect("ack")
    };
    // broker-x holds an incarnation this store never issued, which raises
    // the store's past it: nobody has acknowledged the new one.
    assert!(ack("broker-x", 1).await);
    let current = store.placement_incarnation().await.expect("incarnation");
    assert!(current > 1);

    // An id that completes no target is kept.
    assert!(report(store, &shard, "broker-x", 31).await);
    let err = store
        .record_raft_copy_id(&shard.key, "broker-y", 32)
        .await
        .expect_err("not acknowledged");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(counter(&read(store, &shard.key).await), None);
    assert!(!store.placement_acked_by_all().await.expect("gate"));

    // A registered broker that is not serving, and never acknowledges.
    store
        .register_node(super::nodes::node("broker-z", 7599))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-z", NodeLifecycle::Left)
        .await
        .expect("left");
    assert!(ack("broker-x", current).await);
    assert!(ack("broker-y", current).await);
    let err = store
        .record_raft_copy_id(&shard.key, "broker-y", 32)
        .await
        .expect_err("broker-z has not acknowledged");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(!store.placement_acked_by_all().await.expect("gate"));

    store.delete_node("broker-z").await.expect("deregister");
    assert!(store.placement_acked_by_all().await.expect("gate"));
    assert!(report(store, &shard, "broker-y", 32).await);
    assert_eq!(counter(&read(store, &shard.key).await), Some(1));
    clear(store).await;
}
