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
        a_report_from_an_earlier_process_is_refused(store).await;
        a_report_from_before_a_delete_is_refused(store).await;
        each_target_is_listed_in_the_write_that_stores_it(store).await;
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
    try_report(store, a, node, id).await.expect("copy id")
}

/// A report from the process `node` is registered as now.
async fn try_report(
    store: &dyn ControlPlaneStore,
    a: &ShardAssignment,
    node: &str,
    id: u64,
) -> Result<bool, StoreError> {
    let process = store.get_node(node).await.expect("node").status;
    store
        .record_raft_copy_id(
            &a.key,
            node,
            process.registered_at_millis,
            process.incarnation,
            id,
        )
        .await
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
        .record_raft_copy_id(&shard.key, "broker-z", 1, 1, 1)
        .await
        .expect_err("not in the set");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let hybrid = sealed(store, 1).await;
    let err = try_report(store, &hybrid, "broker-x", 1)
        .await
        .expect_err("not switched");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(read(store, &hybrid.key).await.raft_copy_ids.is_empty());
    clear(store).await;
}

/// A wiped copy's old id, retried late by the process before the wipe,
/// must not replace the id its successor reported.
async fn a_report_from_an_earlier_process_is_refused(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    let before = store.get_node("broker-y").await.expect("node").status;
    store
        .register_node(super::nodes::node("broker-y", 7501))
        .await
        .expect("register again");
    let after = store.get_node("broker-y").await.expect("node").status;
    assert!(
        after.incarnation > before.incarnation,
        "{} -> {}",
        before.incarnation,
        after.incarnation
    );
    assert!(report(store, &shard, "broker-y", 42).await);
    let err = store
        .record_raft_copy_id(
            &shard.key,
            "broker-y",
            before.registered_at_millis,
            before.incarnation,
            41,
        )
        .await
        .expect_err("an earlier process");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(
        read(store, &shard.key).await.raft_copy_ids.get("broker-y"),
        Some(&42)
    );
    clear(store).await;
}

/// A node deleted and registered again restarts at incarnation 0, so a
/// report from the process before the delete matches on incarnation alone.
/// Its registration time does not.
async fn a_report_from_before_a_delete_is_refused(store: &dyn ControlPlaneStore) {
    let register_fresh = |at: u64| async move {
        clear(store).await;
        store.delete_node("broker-y").await.expect("delete");
        let mut node = super::nodes::node("broker-y", 7501);
        node.status.registered_at_millis = at;
        store.register_node(node).await.expect("register");
        store.get_node("broker-y").await.expect("node").status
    };
    let old = register_fresh(1_699_000_100_000).await;
    switched(store).await;

    let new = register_fresh(1_699_000_200_000).await;
    assert_eq!(new.incarnation, old.incarnation, "both fresh registrations");
    let shard = switched(store).await;
    assert!(report(store, &shard, "broker-y", 52).await);
    let err = store
        .record_raft_copy_id(
            &shard.key,
            "broker-y",
            old.registered_at_millis,
            old.incarnation,
            51,
        )
        .await
        .expect_err("a process from before the delete");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(
        read(store, &shard.key).await.raft_copy_ids.get("broker-y"),
        Some(&52)
    );
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
    let err = try_report(store, &shard, "broker-y", 32)
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
    let err = try_report(store, &shard, "broker-y", 32)
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

/// Brokers read a target only from the stored record, so a target listed in
/// the same write is on the epoch's list before any leader is asked for it.
async fn each_target_is_listed_in_the_write_that_stores_it(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    assert_eq!(
        shard.ordered_voter_sets(),
        Some(vec![]),
        "started on the switch"
    );
    for (node, id) in [("broker-x", 31), ("broker-y", 32), ("broker-y", 33)] {
        report(store, &shard, node, id).await;
        let record = read(store, &shard.key).await;
        let listed = record.raft_ordered.as_ref().expect("list").targets.clone();
        match &record.raft_target {
            Some(target) => assert_eq!(listed.last(), Some(target)),
            None => assert!(listed.is_empty()),
        }
    }
    let both: std::collections::BTreeSet<String> =
        ["broker-x".to_string(), "broker-y".to_string()].into();
    assert_eq!(
        read(store, &shard.key).await.ordered_voter_sets(),
        Some(vec![both.clone(), both])
    );
    clear(store).await;
}
