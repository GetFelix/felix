//! A Raft shard's target through the log: what a new member, and a restore,
//! continue from.
use super::*;
use crate::store::contract::raft_targets::switched;
use crate::store::contract::shards::on_streams_of_its_own;

/// A report from the process `node` is registered as on `machine`.
async fn copy_id(
    machine: &MetadataStateMachine,
    key: &crate::model::ShardKey,
    node: &str,
    copy_id: u64,
) -> MetaCommand {
    let status = machine.store().get_node(node).await.expect("node").status;
    MetaCommand::RecordRaftCopyId {
        key: key.clone(),
        node_id: node.to_string(),
        node_registered_at_millis: status.registered_at_millis,
        node_incarnation: status.incarnation,
        copy_id,
        keep_ordered: true,
    }
}

async fn counter(machine: &MetadataStateMachine, key: &crate::model::ShardKey) -> Option<u64> {
    machine
        .store()
        .get_shard_assignment(key)
        .await
        .expect("assignment")
        .raft_target
        .map(|target| target.counter)
}

/// The counter is in the replicated record, so a member that takes over
/// from the log or a snapshot, as a new control-plane leader does, writes
/// the next value after the last committed one.
#[tokio::test]
async fn a_member_built_from_the_log_continues_the_counter() {
    let original = machine();
    let store: &dyn ControlPlaneStore = original.store().as_ref();
    on_streams_of_its_own(store, async {
        let shard = switched(store).await;
        for (node, id) in [("broker-x", 1), ("broker-y", 2)] {
            original
                .dispatch(copy_id(&original, &shard.key, node, id).await)
                .await
                .expect("copy id");
        }
        assert_eq!(counter(&original, &shard.key).await, Some(1));

        let taken_over = machine();
        taken_over.restore(&original.snapshot().await).await;
        taken_over
            .dispatch(copy_id(&taken_over, &shard.key, "broker-y", 3).await)
            .await
            .expect("new copy id");
        assert_eq!(counter(&taken_over, &shard.key).await, Some(2));
    })
    .await;
}

/// FelixRaftPlacement's CpRewind for targets: the restore brings back the
/// backup's counter and raises the incarnation, so the restored target's
/// version is above every one written before it. Nothing moves again until
/// every registered broker has acknowledged the new incarnation, and the
/// backup's list of ordered targets is never read as complete.
#[tokio::test]
async fn a_restore_keeps_the_backups_counter_and_raises_the_incarnation() {
    let machine = machine();
    let store: &dyn ControlPlaneStore = machine.store().as_ref();
    on_streams_of_its_own(store, async {
        let shard = switched(store).await;
        for (node, id) in [("broker-x", 1), ("broker-y", 2)] {
            machine
                .dispatch(copy_id(&machine, &shard.key, node, id).await)
                .await
                .expect("copy id");
        }
        let backup =
            crate::store::export::export_state_from(machine.store().as_ref()
                as &(dyn crate::store::ControlPlaneAuthStore + Send + Sync))
            .await
            .expect("export");
        machine
            .dispatch(copy_id(&machine, &shard.key, "broker-y", 3).await)
            .await
            .expect("copy id");
        assert_eq!(counter(&machine, &shard.key).await, Some(2));
        assert_eq!(
            ordered(&machine, &shard.key).await.map(|sets| sets.len()),
            Some(2)
        );

        machine
            .dispatch(MetaCommand::ImportState {
                state: Box::new(backup),
                overwrite: true,
                mark_unplaced: true,
                raise_incarnation: true,
                lose_ordered: true,
            })
            .await
            .expect("import");
        assert_eq!(
            machine.store().placement_incarnation().await.expect("read"),
            1
        );
        assert_eq!(counter(&machine, &shard.key).await, Some(1));
        // The backup's list misses the target ordered after it.
        assert_eq!(ordered(&machine, &shard.key).await, None);

        let refused = machine
            .dispatch(copy_id(&machine, &shard.key, "broker-y", 4).await)
            .await;
        assert!(
            matches!(refused, Err(MetaError::Conflict(_))),
            "a target went out before the brokers acknowledged the restore: {refused:?}"
        );
        assert_eq!(counter(&machine, &shard.key).await, Some(1));
    })
    .await;
}

/// Switch a sealed shard through the log; `keep` is what `settle_for_level`
/// would have set.
async fn switch(machine: &MetadataStateMachine, keep: bool) -> crate::model::ShardKey {
    let store: &dyn ControlPlaneStore = machine.store().as_ref();
    let order = crate::store::contract::switch::sealed(store, 0).await;
    machine
        .dispatch(MetaCommand::AcceptSealReport {
            key: order.key.clone(),
            report: crate::store::contract::switch::both(order.generation),
            sealer: "broker-x".to_string(),
            keep_ordered: keep,
        })
        .await
        .expect("report");
    order.key
}

async fn kept(
    machine: &MetadataStateMachine,
    key: &crate::model::ShardKey,
    node: &str,
    id: u64,
    keep: bool,
) {
    let mut command = copy_id(machine, key, node, id).await;
    if let MetaCommand::RecordRaftCopyId { keep_ordered, .. } = &mut command {
        *keep_ordered = keep;
    }
    machine.dispatch(command).await.expect("copy id");
}

async fn ordered(
    machine: &MetadataStateMachine,
    key: &crate::model::ShardKey,
) -> Option<Vec<std::collections::BTreeSet<(String, u64)>>> {
    machine
        .store()
        .get_shard_assignment(key)
        .await
        .expect("assignment")
        .ordered_voter_sets()
}

/// A shard that switched below level 39 has no list for the epoch, so later
/// kept targets do not make one: it reads as unknown.
#[tokio::test]
async fn a_switch_below_the_level_leaves_the_epoch_unknown() {
    let machine = machine();
    on_streams_of_its_own(machine.store().as_ref(), async {
        let key = switch(&machine, false).await;
        kept(&machine, &key, "broker-x", 1, true).await;
        kept(&machine, &key, "broker-y", 2, true).await;
        assert_eq!(ordered(&machine, &key).await, None);
    })
    .await;
}

/// A target written by an entry settled below the level is not on the list.
#[tokio::test]
async fn a_target_written_below_the_level_marks_the_list() {
    let machine = machine();
    on_streams_of_its_own(machine.store().as_ref(), async {
        let key = switch(&machine, true).await;
        kept(&machine, &key, "broker-x", 1, true).await;
        kept(&machine, &key, "broker-y", 2, true).await;
        assert_eq!(
            ordered(&machine, &key).await.map(|sets| sets.len()),
            Some(1)
        );
        kept(&machine, &key, "broker-y", 3, false).await;
        assert_eq!(ordered(&machine, &key).await, None);
    })
    .await;
}

/// The list is in the replicated record, so a member restored from a
/// snapshot, as a restarted or new control-plane member is, reads the same.
#[tokio::test]
async fn a_snapshot_keeps_the_list() {
    let original = machine();
    on_streams_of_its_own(original.store().as_ref(), async {
        let key = switch(&original, true).await;
        kept(&original, &key, "broker-x", 1, true).await;
        kept(&original, &key, "broker-y", 2, true).await;
        kept(&original, &key, "broker-y", 3, true).await;
        let held = ordered(&original, &key).await;
        assert_eq!(held.as_ref().map(Vec::len), Some(2));

        let restored = machine();
        restored.restore(&original.snapshot().await).await;
        assert_eq!(ordered(&restored, &key).await, held);
    })
    .await;
}
