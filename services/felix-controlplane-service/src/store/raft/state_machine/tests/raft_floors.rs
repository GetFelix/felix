//! A Raft shard's offset floor through the log: a member built from a
//! snapshot, and a restore from an export, hold the same floor.
use super::*;
use crate::store::contract::raft_floors::entry;
use crate::store::contract::raft_targets::switched;
use crate::store::contract::shards::on_streams_of_its_own;

async fn floor(machine: &MetadataStateMachine, key: &crate::model::ShardKey) -> Option<u64> {
    machine
        .store()
        .get_shard_assignment(key)
        .await
        .expect("assignment")
        .raft_floor
        .map(|floor| floor.index)
}

fn raise(shard: &crate::model::ShardAssignment, index: u64) -> MetaCommand {
    MetaCommand::RecordRaftFloors {
        node_id: "broker-x".to_string(),
        floors: vec![entry(shard, index, 1)],
    }
}

#[tokio::test]
async fn a_snapshot_carries_the_floor() {
    let original = machine();
    let store: &dyn ControlPlaneStore = original.store().as_ref();
    on_streams_of_its_own(store, async {
        let shard = switched(store).await;
        original.dispatch(raise(&shard, 7)).await.expect("floor");
        assert_eq!(floor(&original, &shard.key).await, Some(7));

        let taken_over = machine();
        taken_over.restore(&original.snapshot().await).await;
        assert_eq!(floor(&taken_over, &shard.key).await, Some(7));
        // And it still only rises there.
        taken_over.dispatch(raise(&shard, 6)).await.expect("lower");
        assert_eq!(floor(&taken_over, &shard.key).await, Some(7));
    })
    .await;
}

#[tokio::test]
async fn an_export_and_import_carry_the_floor() {
    let machine = machine();
    let store: &dyn ControlPlaneStore = machine.store().as_ref();
    on_streams_of_its_own(store, async {
        let shard = switched(store).await;
        machine.dispatch(raise(&shard, 9)).await.expect("floor");
        let backup =
            crate::store::export::export_state_from(machine.store().as_ref()
                as &(dyn crate::store::ControlPlaneAuthStore + Send + Sync))
            .await
            .expect("export");
        // Through JSON, as an export file goes.
        let backup =
            serde_json::from_slice(&serde_json::to_vec(&backup).expect("encode")).expect("decode");

        let restored = super::machine();
        restored
            .dispatch(MetaCommand::ImportState {
                state: Box::new(backup),
                overwrite: true,
                mark_unplaced: true,
                raise_incarnation: true,
                lose_ordered: true,
            })
            .await
            .expect("import");
        assert_eq!(floor(&restored, &shard.key).await, Some(9));
    })
    .await;
}
