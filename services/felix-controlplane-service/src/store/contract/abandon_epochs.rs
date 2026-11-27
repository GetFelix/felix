//! An abandon's epoch write, as every backend must keep it: one compare and
//! swap that checks the store's own dead set (`docs/per-shard-raft.md`,
//! "Abandoning a lost group").
use std::sync::Arc;

use felix_storage::raft_copy::DEFAULT_OFFSET_GAP;

use super::nodes::node;
use super::shards::{assignment, clear, key, on_streams_of_its_own};
use super::switch::read;
use crate::model::{AbandonEpochStart, NodeLifecycle, ShardState};
use crate::store::{ControlPlaneStore, StoreError};

pub(crate) async fn run_abandon_epoch_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        an_abandon_waits_for_the_stores_own_dead_set(store).await;
    })
    .await;
}

/// The write is refused while the store holds the placed broker as not
/// dead, whatever the operator read before asking, and lands once it does.
async fn an_abandon_waits_for_the_stores_own_dead_set(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut lost = node("abandon-a", 7611);
    lost.status.storage_id = Some("a".repeat(32));
    store.register_node(lost).await.expect("register a");
    store
        .register_node(node("abandon-b", 7612))
        .await
        .expect("register b");
    let mut wanted = assignment(0, "abandon-a");
    store
        .put_shard_assignment(wanted.clone())
        .await
        .expect("assign");
    wanted.state = ShardState::Active;
    store.put_shard_assignment(wanted).await.expect("activate");
    let shard = read(store, &key(0)).await;
    let start = AbandonEpochStart {
        incarnation: shard.incarnation,
        expected_epoch: shard.group_epoch(),
        expected_generation: shard.generation,
        leader: "abandon-b".to_string(),
        planned_at_millis: 1_800_000_000_000,
        offset_gap: DEFAULT_OFFSET_GAP,
    };

    let refused = store.abandon_raft_epoch(&shard.key, start.clone()).await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(why)) if why.contains("abandon-a")),
        "{refused:?}"
    );
    assert_eq!(read(store, &shard.key).await, shard);

    store
        .set_node_lifecycle("abandon-a", NodeLifecycle::Down)
        .await
        .expect("down");
    store.declare_node_dead("abandon-a").await.expect("declare");
    let written = store
        .abandon_raft_epoch(&shard.key, start.clone())
        .await
        .expect("abandon");
    assert_eq!(written.leader, "abandon-b");
    assert_eq!(written.group_epoch(), shard.group_epoch() + 1);
    assert_eq!(read(store, &shard.key).await, written);
    // Asked again, it answers the record instead of a second epoch.
    assert_eq!(
        store
            .abandon_raft_epoch(&shard.key, start)
            .await
            .expect("again"),
        written
    );
}
