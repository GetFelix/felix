//! A Raft shard's copy ids and target, as every backend must record them:
//! the counter rises only when the target's members change, and no target
//! is written while a registered broker has not acknowledged the placement
//! incarnation (`docs/per-shard-raft.md`, "Where a target comes from").
use std::collections::BTreeMap;
use std::sync::Arc;

use super::shards::{
    NAMESPACE, TENANT, clear, next_stream, on_streams_of_its_own, seed, seed_with_replicas,
};
use super::switch::{active, both, read, sealed, written};
use crate::model::{
    Cache, CacheKey, ConsistencyLevel, NodeLifecycle, PlacementAck, ShardAssignment, ShardKind,
    ShardState,
};
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
        a_shard_not_on_raft_takes_no_copy_id(store).await;
    })
    .await;
}

/// On a store whose cluster runs durable streams on Raft: a shard that
/// started on Raft gets its target from the copy ids its brokers report.
pub(crate) async fn run_raft_native_target_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        a_shard_started_on_raft_gets_its_target(store).await;
        a_native_report_names_the_streams_incarnation(store).await;
        a_native_set_short_of_the_replica_count_takes_no_copy_id(store).await;
        a_cache_shard_started_on_raft_gets_its_target(store).await;
        a_set_holds_until_its_group_forms(store).await;
    })
    .await;
}

/// A report for a shard that started on Raft, from the process `node` is
/// registered as now.
async fn try_native_report(
    store: &dyn ControlPlaneStore,
    a: &ShardAssignment,
    node: &str,
    id: u64,
    shard_incarnation: Option<u64>,
) -> Result<bool, StoreError> {
    let process = store.get_node(node).await.expect("node").status;
    store
        .record_raft_copy_id(
            &a.key,
            node,
            process.registered_at_millis,
            process.incarnation,
            id,
            shard_incarnation,
            None,
        )
        .await
}

/// An active shard that started on Raft, on {broker-x, broker-y} with its
/// first target stored and no leader reported yet, so its set is held.
/// Registers broker-z, outside the set, which the caller deregisters.
pub(crate) async fn held_set(store: &dyn ControlPlaneStore) -> ShardAssignment {
    let shard = active(store, 0).await;
    let at = Some(shard.incarnation);
    store
        .register_node(super::nodes::node("broker-z", 7598))
        .await
        .expect("register");
    let current = store.placement_incarnation().await.expect("incarnation");
    for node in ["broker-x", "broker-y", "broker-z"] {
        let heard = PlacementAck {
            incarnation: current,
            node_incarnation: store.get_node(node).await.expect("node").status.incarnation,
        };
        store.record_placement_ack(node, heard).await.expect("ack");
    }
    try_native_report(store, &shard, "broker-x", 41, at)
        .await
        .expect("x");
    try_native_report(store, &shard, "broker-y", 42, at)
        .await
        .expect("y");
    let held = read(store, &shard.key).await;
    assert!(held.holds_unformed_set());
    held
}

/// A broker cut off from the control plane keeps forming the first target
/// it read. A second set with its own target would let another broker form
/// a second group beside it (#2794), so the set holds until a leader the
/// target names reports, and only then moves.
async fn a_set_holds_until_its_group_forms(store: &dyn ControlPlaneStore) {
    // What the cut-off broker-x will keep reading.
    let stale = held_set(store).await;
    let shard = stale.clone();
    let at = Some(shard.incarnation);
    let first = stale.raft_target.clone().expect("first target");

    let mut moved = stale.clone();
    moved.replicas = vec!["broker-z".to_string()];
    let refused = store.put_shard_assignment(moved.clone()).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict(_))),
        "a set change before the group forms: {refused:?}"
    );
    let _ = try_native_report(store, &shard, "broker-z", 43, at).await;
    // A broker outside the target leads no group of it.
    let mut report = super::raft_floors::entry(&shard, 1, 1);
    report.voters = Some(first.members.clone());
    let floor = |node: &'static str| {
        let report = report.clone();
        async move {
            store
                .record_raft_floors(node, vec![report])
                .await
                .expect("floors")
        }
    };
    floor("broker-z").await;
    assert!(store.put_shard_assignment(moved.clone()).await.is_err());
    let held = read(store, &shard.key).await;
    assert_eq!(
        held.raft_target.as_ref(),
        Some(&first),
        "every view names one target"
    );

    floor("broker-x").await;
    store
        .put_shard_assignment(moved)
        .await
        .expect("the group formed, so its set moves");
    // The move cleared the target; a voter already kept still reports.
    let mut lagging = super::raft_floors::entry(&shard, 1, 1);
    lagging.voters = Some([("broker-x".to_string(), 41)].into());
    store
        .record_raft_floors("broker-x", vec![lagging])
        .await
        .expect("floors");
    let kept = read(store, &shard.key).await.raft_voters.expect("voters");
    assert_eq!(kept.members.keys().collect::<Vec<_>>(), ["broker-x"]);
    for (node, id) in [("broker-x", 41), ("broker-z", 43)] {
        try_native_report(store, &shard, node, id, at)
            .await
            .expect(node);
    }
    let target = read(store, &shard.key).await.raft_target.expect("retarget");
    assert_eq!(
        target.members.keys().collect::<Vec<_>>(),
        ["broker-x", "broker-z"]
    );

    // The group has not reported adding broker-z, so a set of broker-z
    // alone would name only a broker it never reached (#2905). That holds
    // with broker-z's target stored, and against a late report of the old
    // voters, until a leader reports the new ones.
    let mut alone = read(store, &shard.key).await;
    alone.leader = "broker-z".to_string();
    alone.replicas.clear();
    assert!(
        matches!(
            store.put_shard_assignment(alone.clone()).await,
            Err(StoreError::Conflict(_))
        ),
        "a step the reported voters keep no majority of"
    );
    // A broker that lists itself among the voters, outside the target,
    // leads no group of it, and its voters would let the step through.
    let mut outsider = super::raft_floors::entry(&shard, 1, 1);
    let mut listed = target.members.clone();
    listed.insert("broker-q".to_string(), 9);
    outsider.voters = Some(listed);
    store
        .record_raft_floors("broker-q", vec![outsider])
        .await
        .expect("floors");
    assert!(
        matches!(
            store.put_shard_assignment(alone.clone()).await,
            Err(StoreError::Conflict(_))
        ),
        "voters from outside the target"
    );
    let mut applied = super::raft_floors::entry(&shard, 2, 1);
    applied.voters = Some(target.members.clone());
    let mut late = super::raft_floors::entry(&shard, 1, 1);
    late.voters = Some(first.members.clone());
    store
        .record_raft_floors("broker-x", vec![applied])
        .await
        .expect("floors");
    store
        .record_raft_floors("broker-x", vec![late])
        .await
        .expect("floors");
    store
        .put_shard_assignment(alone)
        .await
        .expect("the group reported broker-z, so the step goes ahead");
    clear(store).await;
    store.delete_node("broker-z").await.expect("deregister");
}

async fn a_shard_started_on_raft_gets_its_target(store: &dyn ControlPlaneStore) {
    let shard = active(store, 0).await;
    let at = Some(shard.incarnation);
    assert!(
        try_native_report(store, &shard, "broker-x", 21, at)
            .await
            .expect("x")
    );
    assert_eq!(
        counter(&read(store, &shard.key).await),
        None,
        "broker-y has not reported"
    );
    assert!(
        try_native_report(store, &shard, "broker-y", 22, at)
            .await
            .expect("y")
    );
    let stored = read(store, &shard.key).await;
    assert_eq!(counter(&stored), Some(1));
    let members = BTreeMap::from([("broker-x".to_string(), 21), ("broker-y".to_string(), 22)]);
    assert_eq!(
        stored.raft_target.as_ref().expect("target").members,
        members
    );
    // Epoch 0's list started with the first copy id, so it holds the first target a re-form counts.
    let voters = members.into_iter().collect();
    assert_eq!(stored.ordered_voter_sets(), Some(vec![voters]));
    assert_eq!(stored.generation, shard.generation, "written in place");
    clear(store).await;
}

/// A report that does not name the stream incarnation is one from before
/// level 41, which a shard that started on Raft never took; one naming
/// another incarnation is for a group that is gone.
async fn a_native_report_names_the_streams_incarnation(store: &dyn ControlPlaneStore) {
    let shard = active(store, 0).await;
    for at in [None, Some(shard.incarnation + 1)] {
        let err = try_native_report(store, &shard, "broker-x", 31, at)
            .await
            .expect_err("refused");
        assert!(
            matches!(err, StoreError::Conflict(_)),
            "{at:?}: got {err:?}"
        );
    }
    assert!(read(store, &shard.key).await.raft_copy_ids.is_empty());
    clear(store).await;
}

/// The control plane checks the set against the stream's replica count
/// itself: a broker's view of placement may be stale.
async fn a_native_set_short_of_the_replica_count_takes_no_copy_id(store: &dyn ControlPlaneStore) {
    next_stream();
    seed_with_replicas(store, 3).await;
    let shard = active(store, 0).await;
    let err = try_native_report(store, &shard, "broker-x", 61, Some(shard.incarnation))
        .await
        .expect_err("two brokers for three replicas");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    let stored = read(store, &shard.key).await;
    assert!(stored.raft_copy_ids.is_empty());
    assert!(stored.raft_ordered.is_none(), "no list starts either");
    clear(store).await;
    next_stream();
    seed(store).await;
}

/// A cache shard goes through the same copy ids and target as a stream
/// shard: its group forms only on a stored target, so without one it never
/// forms. The control plane checks the cache's incarnation and replica
/// count as it does a stream's.
async fn a_cache_shard_started_on_raft_gets_its_target(store: &dyn ControlPlaneStore) {
    // Two names: a deleted cache name is not created again before the
    // fleet finalizes `cache_incarnations`.
    let placed = async |name: &str, replication_factor| {
        store
            .create_cache(Cache {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                cache: name.to_string(),
                display_name: name.to_string(),
                shards: 1,
                replication_factor,
                consistency: ConsistencyLevel::Quorum,
                incarnation: 0,
            })
            .await
            .expect("create");
        let mut wanted = super::shards::assignment(0, "broker-x");
        wanted.key.kind = ShardKind::Cache;
        wanted.key.stream = name.to_string();
        wanted.replicas = vec!["broker-y".to_string()];
        store
            .put_shard_assignment(wanted.clone())
            .await
            .expect("assign");
        wanted.state = ShardState::Active;
        store.put_shard_assignment(wanted).await.expect("activate")
    };
    let gone = async |name: &str| {
        clear(store).await;
        store
            .delete_cache(&CacheKey {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                cache: name.to_string(),
            })
            .await
            .expect("delete");
    };

    let shard = placed("raft-cache-short", 3).await;
    let err = try_native_report(store, &shard, "broker-x", 71, Some(shard.incarnation))
        .await
        .expect_err("two brokers for three replicas");
    assert!(
        matches!(&err, StoreError::Conflict(why) if why.contains("replica count")),
        "got {err:?}"
    );
    gone("raft-cache-short").await;

    let shard = placed("raft-cache", 2).await;
    let at = Some(shard.incarnation);
    let err = try_native_report(store, &shard, "broker-x", 72, Some(shard.incarnation + 1))
        .await
        .expect_err("another incarnation");
    assert!(
        matches!(&err, StoreError::Conflict(why) if why.contains("incarnation")),
        "got {err:?}"
    );
    assert!(
        try_native_report(store, &shard, "broker-x", 72, at)
            .await
            .expect("x")
    );
    assert!(
        try_native_report(store, &shard, "broker-y", 73, at)
            .await
            .expect("y")
    );
    let stored = read(store, &shard.key).await;
    assert_eq!(counter(&stored), Some(1));
    let members = BTreeMap::from([("broker-x".to_string(), 72), ("broker-y".to_string(), 73)]);
    assert_eq!(
        stored.raft_target.as_ref().expect("target").members,
        members
    );
    assert_eq!(stored.generation, shard.generation, "written in place");
    gone("raft-cache").await;
}

/// On a hybrid cluster a shard that never switched is not on Raft, whatever
/// the report carries.
async fn a_shard_not_on_raft_takes_no_copy_id(store: &dyn ControlPlaneStore) {
    let shard = active(store, 0).await;
    let err = try_native_report(store, &shard, "broker-x", 41, Some(shard.incarnation))
        .await
        .expect_err("a hybrid shard");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(read(store, &shard.key).await.raft_copy_ids.is_empty());
    clear(store).await;
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
            None,
            None,
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
        .record_raft_copy_id(&shard.key, "broker-z", 1, 1, 1, None, None)
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
            None,
            None,
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
            None,
            None,
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
    let voters = |y: u64| -> std::collections::BTreeSet<(String, u64)> {
        [("broker-x".to_string(), 31), ("broker-y".to_string(), y)].into()
    };
    assert_eq!(
        read(store, &shard.key).await.ordered_voter_sets(),
        Some(vec![voters(32), voters(33)])
    );
    clear(store).await;
}
