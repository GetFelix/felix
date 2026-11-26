//! A re-form's epoch write, as every backend must keep it: one compare and
//! swap on the group epoch that the shard feed carries to the brokers
//! (`docs/per-shard-raft.md`, "Re-forming a group").
use std::sync::Arc;

use super::raft_targets::{held_set, try_native_report};
use super::shards::{assignment, clear, on_streams_of_its_own};
use super::switch::read;
use crate::model::{
    PlacementAck, RaftEpochBase, RaftLogId, ReformEpochStart, ShardAssignment, ShardState,
};
use crate::store::{ControlPlaneStore, StoreError};

pub(crate) async fn run_reform_epoch_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        a_reform_starts_the_next_epoch_once(store).await;
        a_group_of_one_starts_its_next_epoch_alone_once(store).await;
    })
    .await;
}

fn start(shard: &ShardAssignment, expected_epoch: u64, first_index: u64) -> ReformEpochStart {
    ReformEpochStart {
        incarnation: shard.incarnation,
        expected_epoch,
        planned_floor: None,
        base: RaftEpochBase {
            base_last: Some(RaftLogId {
                index: first_index - 3,
                epoch: expected_epoch,
                term: 1,
            }),
            first_index,
        },
        voters: [("broker-x".to_string(), 41)].into(),
        dead: Default::default(),
        starts: [(
            "broker-x".to_string(),
            felix_storage::raft_copy::WireStart {
                voter: Some(41),
                kept: None,
                floor: None,
                needs_accept_loss: false,
                accepted: false,
            },
        )]
        .into(),
    }
}

/// The write lands once: the shard reads at the next epoch with the plan's
/// base and voters, the feed carries it, and the same plan sent again (a
/// retried `apply`, or a second operator) is refused instead of starting a
/// third epoch from the same base.
async fn a_reform_starts_the_next_epoch_once(store: &dyn ControlPlaneStore) {
    let shard = held_set(store).await;
    store.delete_node("broker-z").await.expect("deregister");
    let before = store
        .shard_assignment_changes(0)
        .await
        .expect("changes")
        .next_seq;
    let plan = start(&shard, 0, 10);

    let written = store
        .start_reformed_epoch(&shard.key, plan.clone())
        .await
        .expect("epoch 1");
    assert_eq!(written.group_epoch(), 1);
    let held = read(store, &shard.key).await;
    assert_eq!(held, written);
    assert_eq!(held.generation, shard.generation, "written in place");
    let bases = held.raft_bases.as_ref().expect("bases");
    assert_eq!(bases.bases, vec![plan.base]);
    let seed = held.raft_seed.as_ref().expect("seed");
    assert_eq!((seed.epoch, &seed.members), (1, &plan.voters));
    // In the committed record, so a broker finds its start after a
    // metadata leader change drops the leader's plans.
    assert_eq!(seed.starts, plan.starts);
    let changes = store
        .shard_assignment_changes(before)
        .await
        .expect("changes");
    assert!(
        changes
            .items
            .iter()
            .any(|change| change.key == shard.key && change.assignment.as_ref() == Some(&held)),
        "the feed carries the new epoch"
    );

    let again = store.start_reformed_epoch(&shard.key, plan.clone()).await;
    assert!(
        matches!(&again, Err(StoreError::Conflict(why)) if why.contains("epoch 1")),
        "{again:?}"
    );
    assert_eq!(read(store, &shard.key).await, held);
    // The refusal reads as written for the plan that wrote it, so a write
    // whose request was dropped after it committed is not reported failed.
    assert!(held.started_by(&plan));
    assert!(!shard.started_by(&plan));
    assert!(!held.started_by(&start(&shard, 0, 11)), "another base");

    // Another incarnation's plan never lands, whatever its epoch.
    let stale = ReformEpochStart {
        incarnation: shard.incarnation + 1,
        ..start(&held, 1, 20)
    };
    let refused = store.start_reformed_epoch(&shard.key, stale).await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(why)) if why.contains("incarnation")),
        "{refused:?}"
    );
    assert_eq!(read(store, &shard.key).await, held);
}

/// An active shard of a replication factor 1 stream that started on Raft,
/// alone on broker-x, with its first target stored from broker-x's copy id
/// (41) and so its seed and ordered list.
pub(crate) async fn alone_shard(store: &dyn ControlPlaneStore) -> ShardAssignment {
    clear(store).await;
    let mut wanted = assignment(0, "broker-x");
    store
        .put_shard_assignment(wanted.clone())
        .await
        .expect("assign");
    wanted.state = ShardState::Active;
    let shard = store.put_shard_assignment(wanted).await.expect("activate");
    let current = store.placement_incarnation().await.expect("incarnation");
    for node in ["broker-x", "broker-y"] {
        let heard = PlacementAck {
            incarnation: current,
            node_incarnation: store.get_node(node).await.expect("node").status.incarnation,
        };
        store.record_placement_ack(node, heard).await.expect("ack");
    }
    try_native_report(store, &shard, "broker-x", 41, Some(shard.incarnation))
        .await
        .expect("copy id");
    let held = read(store, &shard.key).await;
    assert!(held.replicas.is_empty());
    held
}

/// A group of one's broker starts its next epoch on its own word, once:
/// another broker's request for the shard is refused, the same request sent
/// again answers the record with no second epoch and no second feed entry,
/// and a different plan from the old epoch is refused.
async fn a_group_of_one_starts_its_next_epoch_alone_once(store: &dyn ControlPlaneStore) {
    let shard = alone_shard(store).await;
    let plan = start(&shard, 0, 10);

    let other = store
        .start_alone_epoch(&shard.key, "broker-y", plan.clone())
        .await;
    assert!(
        matches!(&other, Err(StoreError::Conflict(why)) if why.contains("not alone")),
        "{other:?}"
    );
    assert_eq!(read(store, &shard.key).await, shard);

    let written = store
        .start_alone_epoch(&shard.key, "broker-x", plan.clone())
        .await
        .expect("epoch 1");
    assert_eq!(written.group_epoch(), 1);
    let held = read(store, &shard.key).await;
    assert_eq!(held, written);
    assert!(held.started_by(&plan));
    let seed = held.raft_seed.as_ref().expect("seed");
    assert_eq!(
        (seed.epoch, &seed.members, &seed.starts),
        (1, &plan.voters, &plan.starts)
    );
    let feed = store
        .shard_assignment_changes(0)
        .await
        .expect("changes")
        .next_seq;

    let again = store
        .start_alone_epoch(&shard.key, "broker-x", plan.clone())
        .await
        .expect("a replay answers the record");
    assert_eq!(again, held);
    assert_eq!(
        store
            .shard_assignment_changes(0)
            .await
            .expect("changes")
            .next_seq,
        feed,
        "a replay writes nothing"
    );

    let different = store
        .start_alone_epoch(&shard.key, "broker-x", start(&shard, 0, 11))
        .await;
    assert!(
        matches!(&different, Err(StoreError::Conflict(why)) if why.contains("epoch 1")),
        "{different:?}"
    );
    assert_eq!(read(store, &shard.key).await, held);
    clear(store).await;
}
