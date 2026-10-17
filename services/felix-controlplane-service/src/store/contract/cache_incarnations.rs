//! Cache incarnations and tombstones, given and recorded the same way on
//! every backend, for the reasons `incarnations` gives for streams.
use std::sync::Arc;

use super::nodes::{node, with_features};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CacheTombstone, ConsistencyLevel, Namespace,
    NamespaceKey, ShardAssignment, ShardKey, ShardKind, ShardState, Tenant,
};
use crate::store::ControlPlaneStore;

const TENANT: &str = "shard-t";
const NAMESPACE: &str = "shard-ns";

/// Run on a store of its own: it finalizes `cache_incarnations`, which
/// changes how every later cache is created.
pub(crate) async fn run_cache_incarnation_contract(store: Arc<dyn ControlPlaneStore>) {
    super::shards::seed(store.as_ref()).await;
    let s = store.as_ref();
    before_the_finalize_a_cache_is_at_0_and_its_delete_names_nothing(s).await;
    finalize(s).await;
    a_create_proposing_nothing_still_gets_an_incarnation(s).await;
    a_names_incarnations_only_increase(s).await;
    a_batch_create_answers_the_incarnation_it_stored(s).await;
    a_cache_from_before_the_finalize_is_deleted_through_0(s).await;
    cascading_deletes_write_tombstones(s).await;
    an_assignment_names_the_incarnation_it_was_written_for(s).await;
    a_recreated_cache_never_repeats_a_generation(s).await;
    an_assignment_racing_a_recreate_names_the_cache_it_landed_on(store).await;
}

/// Before the finalize a create gets 0 whatever it proposed, and a delete
/// leaves no tombstone and a change exactly as before incarnations.
async fn before_the_finalize_a_cache_is_at_0_and_its_delete_names_nothing(
    store: &dyn ControlPlaneStore,
) {
    let created = store
        .create_cache(cache(NAMESPACE, "early", 7))
        .await
        .expect("create");
    assert_eq!(created.incarnation, 0);
    let since = next_seq(store).await;
    store
        .delete_cache(&key(NAMESPACE, "early"))
        .await
        .expect("delete");
    let deleted = change(store, since, "early", CacheChangeOp::Deleted).await;
    assert_eq!(deleted.deleted_through, None);
    assert!(deleted.cache.is_none(), "{:?}", deleted.cache);
    assert!(tombstones(store).await.is_empty());
    let (_, caches) = store
        .create_resources(Vec::new(), vec![cache(NAMESPACE, "early-batch", 7)])
        .await
        .expect("create in a batch");
    assert_eq!(caches[0].incarnation, 0);
    // Kept for the delete after the finalize.
    store
        .create_cache(cache(NAMESPACE, "legacy", 0))
        .await
        .expect("create");
}

async fn finalize(store: &dyn ControlPlaneStore) {
    let feature = felix_common::fleet::CACHE_INCARNATIONS.name();
    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        store
            .register_node(with_features(node(id, 7500 + i as u16), &[feature]))
            .await
            .expect("report the feature");
    }
    // A Raft member may apply the registrations a moment after they commit.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match store.finalize_fleet_feature(feature).await {
            Ok(_) => break,
            Err(err) => assert!(std::time::Instant::now() < deadline, "finalize: {err:?}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A create that read the fleet before the finalize proposes nothing, and
/// still gets an incarnation: the store decides at the write.
async fn a_create_proposing_nothing_still_gets_an_incarnation(store: &dyn ControlPlaneStore) {
    let created = store
        .create_cache(cache(NAMESPACE, "racer", 0))
        .await
        .expect("create");
    assert_ne!(created.incarnation, 0);
    assert_eq!(
        store
            .get_cache(&key(NAMESPACE, "racer"))
            .await
            .expect("read")
            .incarnation,
        created.incarnation
    );
    store
        .create_resources(Vec::new(), vec![cache(NAMESPACE, "racer-batch", 0)])
        .await
        .expect("create in a batch");
    assert_ne!(
        store
            .get_cache(&key(NAMESPACE, "racer-batch"))
            .await
            .expect("read")
            .incarnation,
        0
    );
}

/// Each create of a name gets a higher incarnation than the last, whatever
/// it proposed, and each delete records the incarnation it deleted in its
/// change and in the name's tombstone.
async fn a_names_incarnations_only_increase(store: &dyn ControlPlaneStore) {
    const NAME: &str = "again";
    let mut last = 0;
    for (round, proposed) in [1_000u64, 5, 1].into_iter().enumerate() {
        let since = next_seq(store).await;
        let incarnation = if round == 2 {
            store
                .create_resources(Vec::new(), vec![cache(NAMESPACE, NAME, proposed)])
                .await
                .expect("create in a batch");
            store
                .get_cache(&key(NAMESPACE, NAME))
                .await
                .expect("read")
                .incarnation
        } else {
            store
                .create_cache(cache(NAMESPACE, NAME, proposed))
                .await
                .expect("create")
                .incarnation
        };
        assert!(
            incarnation > last,
            "round {round}: {incarnation} after {last}"
        );
        let created = change(store, since, NAME, CacheChangeOp::Created).await;
        assert_eq!(
            created.cache.as_ref().map(|cache| cache.incarnation),
            Some(incarnation),
            "the change names another incarnation than the cache has"
        );

        let since = next_seq(store).await;
        store
            .delete_cache(&key(NAMESPACE, NAME))
            .await
            .expect("delete");
        let deleted = change(store, since, NAME, CacheChangeOp::Deleted).await;
        assert_eq!(deleted.deleted_through, Some(incarnation));
        assert_eq!(
            deleted.cache.as_ref().map(|cache| cache.incarnation),
            Some(incarnation)
        );
        assert!(
            tombstones(store)
                .await
                .contains(&tombstone(NAMESPACE, NAME, incarnation)),
            "no tombstone for {incarnation}"
        );
        last = incarnation;
    }
}

/// A batch create answers with the caches as stored, a proposal below the
/// name's tombstone raised.
async fn a_batch_create_answers_the_incarnation_it_stored(store: &dyn ControlPlaneStore) {
    let high = 1 << 60;
    store
        .create_cache(cache(NAMESPACE, "batched", high))
        .await
        .expect("create");
    store
        .delete_cache(&key(NAMESPACE, "batched"))
        .await
        .expect("delete");
    let (_, created) = store
        .create_resources(Vec::new(), vec![cache(NAMESPACE, "batched", 5)])
        .await
        .expect("create in a batch");
    assert_eq!(created.len(), 1);
    assert!(created[0].incarnation > high, "{}", created[0].incarnation);
    assert_eq!(
        store
            .get_cache(&key(NAMESPACE, "batched"))
            .await
            .expect("read")
            .incarnation,
        created[0].incarnation
    );
}

/// A cache created before the finalize and deleted after it is deleted
/// through incarnation 0: nothing can be created at 0 any more.
async fn a_cache_from_before_the_finalize_is_deleted_through_0(store: &dyn ControlPlaneStore) {
    let since = next_seq(store).await;
    store
        .delete_cache(&key(NAMESPACE, "legacy"))
        .await
        .expect("delete");
    let deleted = change(store, since, "legacy", CacheChangeOp::Deleted).await;
    assert_eq!(deleted.deleted_through, Some(0));
    assert!(
        tombstones(store)
            .await
            .contains(&tombstone(NAMESPACE, "legacy", 0))
    );
    let again = store
        .create_cache(cache(NAMESPACE, "legacy", 0))
        .await
        .expect("create again");
    assert!(again.incarnation > 0);
}

/// Deleting a namespace or a tenant deletes its caches, and each gets its
/// tombstone and a change naming it.
async fn cascading_deletes_write_tombstones(store: &dyn ControlPlaneStore) {
    const DOOMED: &str = "doomed";
    store
        .create_namespace(Namespace {
            tenant_id: TENANT.to_string(),
            namespace: DOOMED.to_string(),
            display_name: "Doomed".to_string(),
        })
        .await
        .expect("namespace");
    let created = store
        .create_cache(cache(DOOMED, "inside", 0))
        .await
        .expect("create");
    let since = next_seq(store).await;
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: TENANT.to_string(),
            namespace: DOOMED.to_string(),
        })
        .await
        .expect("delete namespace");
    let deleted = cascaded(store, since, &key(DOOMED, "inside")).await;
    assert_eq!(deleted.deleted_through, Some(created.incarnation));
    assert!(
        tombstones(store)
            .await
            .contains(&tombstone(DOOMED, "inside", created.incarnation))
    );

    const GONE: &str = "gone-t";
    store
        .create_tenant(Tenant {
            tenant_id: GONE.to_string(),
            display_name: "Gone".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: GONE.to_string(),
            namespace: DOOMED.to_string(),
            display_name: "Doomed".to_string(),
        })
        .await
        .expect("namespace");
    let created = store
        .create_cache(Cache {
            tenant_id: GONE.to_string(),
            ..cache(DOOMED, "inside", 0)
        })
        .await
        .expect("create");
    let since = next_seq(store).await;
    store.delete_tenant(GONE).await.expect("delete tenant");
    let gone = CacheKey {
        tenant_id: GONE.to_string(),
        ..key(DOOMED, "inside")
    };
    let deleted = cascaded(store, since, &gone).await;
    assert_eq!(deleted.deleted_through, Some(created.incarnation));
    assert!(tombstones(store).await.contains(&CacheTombstone {
        tenant_id: GONE.to_string(),
        ..tombstone(DOOMED, "inside", created.incarnation)
    }));
}

/// An assignment carries the incarnation of the cache it was written for,
/// whatever the writer passed, and its change keeps it after the cache is
/// created anew.
async fn an_assignment_names_the_incarnation_it_was_written_for(store: &dyn ControlPlaneStore) {
    const AGAIN: &str = "assigned";
    let assignment = ShardAssignment {
        // Ignored: the store reads the cache's.
        incarnation: 99,
        ..assignment(AGAIN)
    };
    let first = store
        .create_cache(cache(NAMESPACE, AGAIN, 1))
        .await
        .expect("create")
        .incarnation;
    let since = store
        .shard_assignment_snapshot()
        .await
        .expect("snapshot")
        .next_seq;
    store
        .put_shard_assignment(assignment.clone())
        .await
        .expect("assign");
    let held = || async {
        store
            .get_shard_assignment(&assignment.key)
            .await
            .expect("read")
            .incarnation
    };
    assert_eq!(held().await, first);

    store
        .delete_cache(&key(NAMESPACE, AGAIN))
        .await
        .expect("delete");
    let second = store
        .create_cache(cache(NAMESPACE, AGAIN, 2))
        .await
        .expect("create again")
        .incarnation;
    assert!(second > first, "{second} after {first}");
    store
        .put_shard_assignment(assignment.clone())
        .await
        .expect("assign again");
    assert_eq!(held().await, second);

    let written: Vec<u64> = store
        .shard_assignment_changes(since)
        .await
        .expect("changes")
        .items
        .iter()
        .filter(|change| change.key == assignment.key)
        .filter_map(|change| change.assignment.as_ref().map(|a| a.incarnation))
        .collect();
    assert_eq!(
        written,
        vec![first, second],
        "a change took the incarnation of a later cache"
    );
    store
        .delete_cache(&key(NAMESPACE, AGAIN))
        .await
        .expect("clean up");
}

/// A cache deleted and created again under the same name starts its shards
/// above every generation the deleted one reached, so brokers never take the
/// deleted cache's per-generation state for the new one's.
async fn a_recreated_cache_never_repeats_a_generation(store: &dyn ControlPlaneStore) {
    const REUSED: &str = "reused";
    let assignment = assignment(REUSED);
    let generation = || async {
        store
            .get_shard_assignment(&assignment.key)
            .await
            .expect("read")
            .generation
    };
    store
        .create_cache(cache(NAMESPACE, REUSED, 1))
        .await
        .expect("create");
    for _ in 0..3 {
        store
            .put_shard_assignment(assignment.clone())
            .await
            .expect("assign");
    }
    let deleted_reached = generation().await;

    store
        .delete_cache(&key(NAMESPACE, REUSED))
        .await
        .expect("delete");
    store
        .create_cache(cache(NAMESPACE, REUSED, 2))
        .await
        .expect("create again");
    store
        .put_shard_assignment(assignment.clone())
        .await
        .expect("assign again");
    let first = generation().await;
    assert!(
        first > deleted_reached,
        "the new cache's shard starts at {first}, the deleted one reached {deleted_reached}"
    );
    store
        .delete_cache(&key(NAMESPACE, REUSED))
        .await
        .expect("clean up");
}

/// An assignment written while its cache is deleted and created again names
/// the incarnation of the cache it ended up attached to, never the old one.
async fn an_assignment_racing_a_recreate_names_the_cache_it_landed_on(
    store: Arc<dyn ControlPlaneStore>,
) {
    const RACED: &str = "raced";
    let assignment = assignment(RACED);
    store
        .create_cache(cache(NAMESPACE, RACED, 1))
        .await
        .expect("create");
    for round in 0..50u64 {
        let assign = {
            let store = Arc::clone(&store);
            let assignment = assignment.clone();
            tokio::spawn(async move {
                // Refused when the cache is gone at that moment, which is fine.
                let _ = store.put_shard_assignment(assignment).await;
            })
        };
        let recreate = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .delete_cache(&key(NAMESPACE, RACED))
                    .await
                    .expect("delete");
                store
                    .create_cache(cache(NAMESPACE, RACED, round + 2))
                    .await
                    .expect("create again");
            })
        };
        recreate.await.expect("recreate");
        assign.await.expect("assign");
        if let Ok(held) = store.get_shard_assignment(&assignment.key).await {
            let current = store
                .get_cache(&key(NAMESPACE, RACED))
                .await
                .expect("cache")
                .incarnation;
            assert_eq!(
                held.incarnation, current,
                "round {round}: an assignment of the new cache names the old one"
            );
        }
    }
}

fn assignment(name: &str) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: name.to_string(),
            shard: 0,
            kind: ShardKind::Cache,
        },
        leader: "broker-x".to_string(),
        replicas: Vec::new(),
        generation: 0,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: Default::default(),
    }
}

async fn next_seq(store: &dyn ControlPlaneStore) -> u64 {
    store.cache_snapshot().await.expect("snapshot").next_seq
}

async fn change(
    store: &dyn ControlPlaneStore,
    since: u64,
    name: &str,
    op: CacheChangeOp,
) -> CacheChange {
    store
        .cache_changes(since)
        .await
        .expect("changes")
        .items
        .into_iter()
        .find(|change| {
            change.key == key(NAMESPACE, name)
                && std::mem::discriminant(&change.op) == std::mem::discriminant(&op)
        })
        .unwrap_or_else(|| panic!("no {op:?} change for {name}"))
}

async fn cascaded(store: &dyn ControlPlaneStore, since: u64, key: &CacheKey) -> CacheChange {
    let change = store
        .cache_changes(since)
        .await
        .expect("changes")
        .items
        .into_iter()
        .find(|change| change.key == *key)
        .expect("the cascade's change");
    assert!(matches!(change.op, CacheChangeOp::Deleted));
    change
}

async fn tombstones(store: &dyn ControlPlaneStore) -> Vec<CacheTombstone> {
    store.cache_tombstones().await.expect("tombstones")
}

fn tombstone(namespace: &str, name: &str, incarnation: u64) -> CacheTombstone {
    CacheTombstone {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        cache: name.to_string(),
        incarnation,
    }
}

fn key(namespace: &str, name: &str) -> CacheKey {
    CacheKey {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        cache: name.to_string(),
    }
}

fn cache(namespace: &str, name: &str, proposed: u64) -> Cache {
    Cache {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards: 1,
        replication_factor: 1,
        consistency: ConsistencyLevel::Leader,
        incarnation: proposed,
    }
}
