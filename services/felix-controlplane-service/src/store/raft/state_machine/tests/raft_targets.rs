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
/// every registered broker has acknowledged the new incarnation.
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

        machine
            .dispatch(MetaCommand::ImportState {
                state: Box::new(backup),
                overwrite: true,
                mark_unplaced: true,
                raise_incarnation: true,
            })
            .await
            .expect("import");
        assert_eq!(
            machine.store().placement_incarnation().await.expect("read"),
            1
        );
        assert_eq!(counter(&machine, &shard.key).await, Some(1));

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
