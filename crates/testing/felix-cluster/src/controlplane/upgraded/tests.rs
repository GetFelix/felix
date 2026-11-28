use felix_controlplane_service::model::{StreamKey, Tenant};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};

use super::{NAMESPACE, NODE, STREAM, record_a_served_shard};

async fn store_with(tenant_id: &str) -> InMemoryStore {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(10_000),
    });
    store
        .create_tenant(Tenant {
            tenant_id: tenant_id.to_string(),
            display_name: tenant_id.to_string(),
        })
        .await
        .expect("tenant");
    store
}

/// **What is left is the history of a placed shard, and nothing that runs.**
/// The control plane reads a cluster as new only while its assignment feed
/// has never moved (`cluster::fleet::never_placed`), so the feed must show a
/// write. The broker and stream must be gone, or placement would wait on a
/// broker that never comes and a test would meet a stream it did not create.
#[tokio::test]
async fn a_served_shard_leaves_its_assignment_history_and_nothing_else() {
    let store = store_with("t1").await;
    let fresh = store.shard_assignment_changes(0).await.expect("changes");
    assert_eq!(fresh.next_seq, 0, "a new store has never placed a shard");

    record_a_served_shard(&store, "t1").await.expect("record");

    let changes = store.shard_assignment_changes(0).await.expect("changes");
    assert!(
        changes.next_seq > 0,
        "the feed shows the placement, so the store is not new"
    );
    assert!(
        store
            .shard_assignment_snapshot()
            .await
            .expect("snapshot")
            .items
            .is_empty(),
        "no shard is still assigned"
    );
    assert!(store.get_node(NODE).await.is_err(), "the broker is removed");
    assert!(
        store.list_nodes().await.expect("nodes").is_empty(),
        "no broker is left registered"
    );
    let stream = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
    };
    assert!(
        store.get_stream(&stream).await.is_err(),
        "the stream is deleted"
    );
}

/// A tenant that does not exist is refused rather than half recorded.
#[tokio::test]
async fn recording_for_a_missing_tenant_is_refused() {
    let store = store_with("t1").await;
    let err = record_a_served_shard(&store, "nobody")
        .await
        .expect_err("no such tenant");
    assert!(
        format!("{err:#}").contains("namespace"),
        "the namespace write is what fails: {err:#}"
    );
    assert_eq!(
        store
            .shard_assignment_changes(0)
            .await
            .expect("changes")
            .next_seq,
        0
    );
}
