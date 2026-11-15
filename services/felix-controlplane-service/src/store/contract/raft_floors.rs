//! A Raft shard's offset floor, as every backend must keep it: raised only,
//! per stream incarnation and reform epoch, and decided entry by entry so
//! one stale report never costs the batch (`docs/per-shard-raft.md`,
//! "Offset floor").
use std::sync::Arc;

use super::raft_targets::switched;
use super::shards::{clear, on_streams_of_its_own};
use super::switch::{both, read};
use crate::model::{RaftFloorReport, ShardAssignment, ShardKey};
use crate::store::ControlPlaneStore;

pub(crate) async fn run_raft_floor_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        the_floor_only_rises(store).await;
        a_bad_entry_skips_only_itself(store).await;
        other_writes_keep_the_floor(store).await;
        an_empty_batch_raises_nothing(store).await;
    })
    .await;
}

pub(crate) fn entry(a: &ShardAssignment, index: u64, term: u64) -> RaftFloorReport {
    RaftFloorReport {
        key: a.key.clone(),
        incarnation: a.incarnation,
        epoch: 0,
        index,
        term,
    }
}

async fn raise(store: &dyn ControlPlaneStore, floors: Vec<RaftFloorReport>) -> u32 {
    store
        .record_raft_floors("broker-x", floors)
        .await
        .expect("floors")
}

async fn floor(store: &dyn ControlPlaneStore, key: &ShardKey) -> Option<(u64, u64)> {
    read(store, key)
        .await
        .raft_floor
        .map(|floor| (floor.index, floor.term))
}

async fn the_floor_only_rises(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    assert_eq!(raise(store, vec![entry(&shard, 10, 2)]).await, 1);
    let held = read(store, &shard.key).await;
    assert_eq!(held.generation, shard.generation, "written in place");
    assert_eq!(floor(store, &shard.key).await, Some((10, 2)));

    // Equal and lower indexes are no-ops; a higher index at a lower term
    // contradicts the floor and is refused.
    for (index, term) in [(10, 2), (9, 4), (12, 1)] {
        assert_eq!(
            raise(store, vec![entry(&shard, index, term)]).await,
            0,
            "{index}@{term}"
        );
    }
    assert_eq!(read(store, &shard.key).await, held);

    assert_eq!(raise(store, vec![entry(&shard, 15, 3)]).await, 1);
    assert_eq!(floor(store, &shard.key).await, Some((15, 3)));
    clear(store).await;
}

/// A missing shard, a hybrid shard, another incarnation and a reform epoch
/// the shard has not reached are each skipped, and the good entry in the
/// same batch still raises its floor.
async fn a_bad_entry_skips_only_itself(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    // `sealed` would clear the switched shard, so the hybrid one is written
    // after it.
    let hybrid = store
        .put_shard_assignment(super::shards::assignment(1, "broker-x"))
        .await
        .expect("hybrid");
    let missing = ShardKey {
        shard: 7,
        ..shard.key.clone()
    };
    let batch = vec![
        RaftFloorReport {
            key: missing.clone(),
            ..entry(&shard, 5, 1)
        },
        entry(&hybrid, 5, 1),
        RaftFloorReport {
            incarnation: shard.incarnation + 1,
            ..entry(&shard, 50, 9)
        },
        RaftFloorReport {
            epoch: 1,
            ..entry(&shard, 60, 9)
        },
        entry(&shard, 5, 1),
    ];
    assert_eq!(raise(store, batch).await, 1);
    assert_eq!(floor(store, &shard.key).await, Some((5, 1)));
    assert_eq!(read(store, &hybrid.key).await.raft_floor, None);
    assert!(store.get_shard_assignment(&missing).await.is_err());
    clear(store).await;
}

/// A seal report sent again and a copy id report leave the floor as it is.
async fn other_writes_keep_the_floor(store: &dyn ControlPlaneStore) {
    let shard = switched(store).await;
    raise(store, vec![entry(&shard, 8, 1)]).await;
    store
        .accept_seal_report(&shard.key, both(shard.generation), "broker-x")
        .await
        .expect("report again");
    let process = store.get_node("broker-x").await.expect("node").status;
    store
        .record_raft_copy_id(
            &shard.key,
            "broker-x",
            process.registered_at_millis,
            process.incarnation,
            3,
        )
        .await
        .expect("copy id");
    assert_eq!(floor(store, &shard.key).await, Some((8, 1)));
    clear(store).await;
}

async fn an_empty_batch_raises_nothing(store: &dyn ControlPlaneStore) {
    assert_eq!(raise(store, Vec::new()).await, 0);
}
