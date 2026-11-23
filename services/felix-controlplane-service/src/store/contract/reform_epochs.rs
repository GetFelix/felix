//! A re-form's epoch write, as every backend must keep it: one compare and
//! swap on the group epoch that the shard feed carries to the brokers
//! (`docs/per-shard-raft.md`, "Re-forming a group").
use std::sync::Arc;

use super::raft_targets::held_set;
use super::shards::on_streams_of_its_own;
use super::switch::read;
use crate::model::{RaftEpochBase, RaftLogId, ReformEpochStart, ShardAssignment};
use crate::store::{ControlPlaneStore, StoreError};

pub(crate) async fn run_reform_epoch_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    on_streams_of_its_own(store, async {
        a_reform_starts_the_next_epoch_once(store).await;
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
