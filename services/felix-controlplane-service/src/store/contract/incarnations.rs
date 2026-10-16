//! Stream incarnations and tombstones, which every backend must give and
//! record the same way: brokers delete a stream's data on a tombstone, and
//! refuse an incarnation below one they hold, so a backend that gave a name a
//! lower incarnation, created one at 0 after the finalize, or lost a
//! tombstone would cost data or leave it behind for good.
use std::sync::Arc;

use super::nodes::{node, with_features};
use crate::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceKey, RetentionPolicy, Stream,
    StreamChange, StreamChangeOp, StreamKey, StreamKind, StreamTombstone,
};
use crate::store::ControlPlaneStore;

const TENANT: &str = "shard-t";
const NAMESPACE: &str = "shard-ns";

/// Run on a store of its own: it finalizes `stream_incarnations`, which
/// changes how every later stream is created.
pub(crate) async fn run_incarnation_contract(store: Arc<dyn ControlPlaneStore>) {
    super::shards::seed(store.as_ref()).await;
    let s = store.as_ref();
    before_the_finalize_a_stream_is_at_0_and_its_delete_names_nothing(s).await;
    finalize(s).await;
    a_create_proposing_nothing_still_gets_an_incarnation(s).await;
    a_names_incarnations_only_increase(s).await;
    a_batch_create_answers_the_incarnation_it_stored(s).await;
    a_stream_from_before_the_finalize_is_deleted_through_0(s).await;
    a_cascading_delete_writes_tombstones(s).await;
    super::shards::an_assignment_names_the_incarnation_it_was_written_for(s).await;
    super::shards::a_recreated_stream_never_repeats_a_generation(s).await;
    super::shards::an_assignment_racing_a_recreate_names_the_stream_it_landed_on(store).await;
}

/// Before the finalize a create gets 0 whatever it proposed, and a delete
/// leaves no tombstone: a stream created again at 0 then reopens the deleted
/// one's logs, so its data has to stay.
async fn before_the_finalize_a_stream_is_at_0_and_its_delete_names_nothing(
    store: &dyn ControlPlaneStore,
) {
    let created = store
        .create_stream(stream(NAMESPACE, "early", 7))
        .await
        .expect("create");
    assert_eq!(created.incarnation, 0);
    let since = next_seq(store).await;
    store
        .delete_stream(&key(NAMESPACE, "early"))
        .await
        .expect("delete");
    let deleted = change(store, since, "early", StreamChangeOp::Deleted).await;
    assert_eq!(deleted.deleted_through, None);
    assert!(tombstones(store).await.is_empty());
    // Kept for the delete after the finalize.
    store
        .create_stream(stream(NAMESPACE, "legacy", 0))
        .await
        .expect("create");
}

async fn finalize(store: &dyn ControlPlaneStore) {
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
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

/// A create that read the fleet before the finalize proposes nothing. The
/// store decides against the finalize as it stands at the write, so the
/// stream still gets an incarnation: one at 0 after the finalize is what
/// brokers would take for a deleted stream's.
// Through the store's API this still holds: Raft's proposer fills in a proposal
// for a level-14 entry, and only an older proposer's entry proposing 0 stays 0
// (`stream_entries_from_a_level_8_proposer_store_what_level_8_stores`).
async fn a_create_proposing_nothing_still_gets_an_incarnation(store: &dyn ControlPlaneStore) {
    let created = store
        .create_stream(stream(NAMESPACE, "racer", 0))
        .await
        .expect("create");
    assert_ne!(created.incarnation, 0);
    assert_eq!(
        store
            .get_stream(&key(NAMESPACE, "racer"))
            .await
            .expect("read")
            .incarnation,
        created.incarnation
    );
    store
        .create_resources(vec![stream(NAMESPACE, "racer-batch", 0)], Vec::new())
        .await
        .expect("create in a batch");
    assert_ne!(
        store
            .get_stream(&key(NAMESPACE, "racer-batch"))
            .await
            .expect("read")
            .incarnation,
        0
    );
}

/// Each create of a name gets a higher incarnation than the last, whatever
/// it proposed, and each delete records the incarnation it deleted in its
/// change and in the name's tombstone.
/// A batch create answers with the streams as stored. A proposal below the
/// name's tombstone is raised, and the caller must see the raised one.
async fn a_batch_create_answers_the_incarnation_it_stored(store: &dyn ControlPlaneStore) {
    let high = 1 << 60;
    store
        .create_stream(stream(NAMESPACE, "batched", high))
        .await
        .expect("create");
    store
        .delete_stream(&key(NAMESPACE, "batched"))
        .await
        .expect("delete");
    let (created, _) = store
        .create_resources(vec![stream(NAMESPACE, "batched", 5)], Vec::new())
        .await
        .expect("create in a batch");
    assert_eq!(created.len(), 1);
    assert!(created[0].incarnation > high, "{}", created[0].incarnation);
    assert_eq!(
        store
            .get_stream(&key(NAMESPACE, "batched"))
            .await
            .expect("read")
            .incarnation,
        created[0].incarnation
    );
}

async fn a_names_incarnations_only_increase(store: &dyn ControlPlaneStore) {
    const NAME: &str = "again";
    let mut last = 0;
    for (round, proposed) in [1_000u64, 5, 1].into_iter().enumerate() {
        let since = next_seq(store).await;
        let incarnation = if round == 2 {
            store
                .create_resources(vec![stream(NAMESPACE, NAME, proposed)], Vec::new())
                .await
                .expect("create in a batch");
            store
                .get_stream(&key(NAMESPACE, NAME))
                .await
                .expect("read")
                .incarnation
        } else {
            store
                .create_stream(stream(NAMESPACE, NAME, proposed))
                .await
                .expect("create")
                .incarnation
        };
        assert!(
            incarnation > last,
            "round {round}: {incarnation} after {last}"
        );
        let created = change(store, since, NAME, StreamChangeOp::Created).await;
        assert_eq!(
            created.stream.as_ref().map(|stream| stream.incarnation),
            Some(incarnation),
            "the change names another incarnation than the stream has"
        );

        let since = next_seq(store).await;
        store
            .delete_stream(&key(NAMESPACE, NAME))
            .await
            .expect("delete");
        let deleted = change(store, since, NAME, StreamChangeOp::Deleted).await;
        assert_eq!(deleted.deleted_through, Some(incarnation));
        assert!(
            tombstones(store)
                .await
                .contains(&tombstone(NAMESPACE, NAME, incarnation)),
            "no tombstone for {incarnation}"
        );
        last = incarnation;
    }
}

/// A stream created before the finalize and deleted after it is deleted
/// through incarnation 0: nothing can be created at 0 any more, so its data
/// can go.
async fn a_stream_from_before_the_finalize_is_deleted_through_0(store: &dyn ControlPlaneStore) {
    let since = next_seq(store).await;
    store
        .delete_stream(&key(NAMESPACE, "legacy"))
        .await
        .expect("delete");
    let deleted = change(store, since, "legacy", StreamChangeOp::Deleted).await;
    assert_eq!(deleted.deleted_through, Some(0));
    assert!(
        tombstones(store)
            .await
            .contains(&tombstone(NAMESPACE, "legacy", 0))
    );
    let again = store
        .create_stream(stream(NAMESPACE, "legacy", 0))
        .await
        .expect("create again");
    assert!(again.incarnation > 0);
}

/// Deleting a namespace deletes its streams, and each gets its tombstone.
async fn a_cascading_delete_writes_tombstones(store: &dyn ControlPlaneStore) {
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
        .create_stream(stream(DOOMED, "inside", 0))
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
    let changes = store.stream_changes(since).await.expect("changes");
    let deleted = changes
        .items
        .iter()
        .find(|change| change.key == key(DOOMED, "inside"))
        .expect("the cascade's change");
    assert!(matches!(deleted.op, StreamChangeOp::Deleted));
    assert_eq!(deleted.deleted_through, Some(created.incarnation));
    assert!(
        tombstones(store)
            .await
            .contains(&tombstone(DOOMED, "inside", created.incarnation))
    );
}

async fn next_seq(store: &dyn ControlPlaneStore) -> u64 {
    store.stream_snapshot().await.expect("snapshot").next_seq
}

async fn change(
    store: &dyn ControlPlaneStore,
    since: u64,
    name: &str,
    op: StreamChangeOp,
) -> StreamChange {
    store
        .stream_changes(since)
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

async fn tombstones(store: &dyn ControlPlaneStore) -> Vec<StreamTombstone> {
    store.stream_tombstones().await.expect("tombstones")
}

fn tombstone(namespace: &str, name: &str, incarnation: u64) -> StreamTombstone {
    StreamTombstone {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
        incarnation,
    }
}

fn key(namespace: &str, name: &str) -> StreamKey {
    StreamKey {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
    }
}

fn stream(namespace: &str, name: &str, proposed: u64) -> Stream {
    Stream {
        tenant_id: TENANT.to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: true,
        region: None,
        routing: Default::default(),
        incarnation: proposed,
    }
}
