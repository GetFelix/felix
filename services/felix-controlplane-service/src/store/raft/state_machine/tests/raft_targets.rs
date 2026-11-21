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
        shard_incarnation: None,
        hold_target: false,
        group_epoch: None,
        keep_seed: false,
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

/// A proposer that counts a down control-plane member at its join floor can
/// see a level below 45 and send a move without `hold_unformed`. Once an
/// entry has lifted the floor to 45, every member holds the set anyway, and
/// so does one restored from a snapshot (#2794).
#[tokio::test]
async fn a_held_set_stays_held_when_a_proposer_sees_a_lower_level() {
    use crate::raft::AppStateMachine;
    use crate::store::contract::raft_targets::held_set;
    let on_raft = || {
        let store = InMemoryStore::new(StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        });
        MetadataStateMachine::new(Arc::new(store.with_shard_engine("raft")))
    };
    let machine = on_raft();
    let store: &dyn ControlPlaneStore = machine.store().as_ref();
    on_streams_of_its_own(store, async {
        let held = held_set(store).await;
        let mut copy_id = copy_id(&machine, &held.key, "broker-x", 41).await;
        if let MetaCommand::RecordRaftCopyId {
            shard_incarnation,
            hold_target,
            ..
        } = &mut copy_id
        {
            *shard_incarnation = Some(held.incarnation);
            *hold_target = true;
        }
        machine.apply(&encode_command(&copy_id)).await;
        assert_eq!(machine.join_floor(), 45);

        let mut moved = held.clone();
        moved.replicas = vec!["broker-z".to_string()];
        let unflagged = encode_command(&MetaCommand::PutShardAssignment {
            assignment: moved,
            seq_generation: true,
            start_placed: true,
            hold_unformed: false,
        });
        let refused = decode_result(&machine.apply(&unflagged).await).expect("result");
        assert!(
            matches!(refused, Err(MetaError::Conflict(_))),
            "an unflagged move at floor 45: {refused:?}"
        );

        let restored = on_raft();
        restored.restore(&machine.snapshot().await).await;
        let refused = decode_result(&restored.apply(&unflagged).await).expect("result");
        assert!(matches!(refused, Err(MetaError::Conflict(_))));
        assert_eq!(
            restored
                .store()
                .get_shard_assignment(&held.key)
                .await
                .expect("assignment")
                .replicas,
            held.replicas
        );
    })
    .await;
}

/// A new cluster finalizes its features once every member is current, and
/// at level 45 that entry lifts the join floor, so a set is held from the
/// first target even if a member is down when it is stored (#2794). Every
/// write that can change the set is held, including the conditional write
/// that predates fencing.
#[tokio::test]
async fn a_finalize_at_level_45_holds_later_sets() {
    use crate::raft::AppStateMachine;
    use crate::store::contract::raft_targets::held_set;
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let machine = MetadataStateMachine::new(Arc::new(store.with_shard_engine("raft")));
    let finalize = encode_command(&MetaCommand::FinalizeFleetFeature {
        feature: felix_common::fleet::GENERATION_START.name().to_string(),
        hold_unformed: false,
        hold_formed: false,
    });
    let finalize = crate::store::raft::command::settle_for_level(&finalize, 45)
        .expect("settled")
        .expect("edited");
    machine.apply(&finalize).await;
    assert_eq!(machine.join_floor(), 45);

    let store: &dyn ControlPlaneStore = machine.store().as_ref();
    on_streams_of_its_own(store, async {
        let held = held_set(store).await;
        let mut moved = held.clone();
        moved.replicas = vec!["broker-z".to_string()];
        let unflagged = encode_command(&MetaCommand::PutShardAssignment {
            assignment: moved.clone(),
            seq_generation: true,
            start_placed: true,
            hold_unformed: false,
        });
        let refused = decode_result(&machine.apply(&unflagged).await).expect("result");
        assert!(
            matches!(refused, Err(MetaError::Conflict(_))),
            "an unflagged move after a level 45 finalize: {refused:?}"
        );

        // The conditional write that predates fencing has no hold field at
        // all, and is held by the floor the same way.
        let legacy = encode_command(&MetaCommand::PutShardAssignmentIf {
            assignment: moved,
            expected_generation: Some(held.generation),
            seq_generation: false,
        });
        let refused = decode_result(&machine.apply(&legacy).await).expect("result");
        assert!(
            matches!(refused, Err(MetaError::Conflict(_))),
            "a conditional move after a level 45 finalize: {refused:?}"
        );
        assert_eq!(
            machine
                .store()
                .get_shard_assignment(&held.key)
                .await
                .expect("assignment")
                .replicas,
            held.replicas
        );
    })
    .await;
}

/// Below level 47 no seed is written, so the re-form planner reads the seed
/// as unknown and marks the group, as before the level. An entry carrying
/// `keep_seed` keeps the epoch's first target as the seed, and a later
/// target (here a wiped copy's new id) leaves it alone.
#[tokio::test]
async fn the_seed_is_kept_only_from_the_level_that_knows_it() {
    for keep_seed in [false, true] {
        let machine = machine();
        let store: &dyn ControlPlaneStore = machine.store().as_ref();
        on_streams_of_its_own(store, async {
            let shard = switched(store).await;
            for (node, id) in [("broker-x", 1), ("broker-y", 2), ("broker-y", 3)] {
                let mut command = copy_id(&machine, &shard.key, node, id).await;
                if let MetaCommand::RecordRaftCopyId {
                    keep_seed: keep, ..
                } = &mut command
                {
                    *keep = keep_seed;
                }
                machine.dispatch(command).await.expect("copy id");
            }
            let held = store.get_shard_assignment(&shard.key).await.expect("shard");
            let seed = held
                .reform_record(&Default::default())
                .expect("epoch 0")
                .seed;
            let first = [("broker-x".to_string(), 1), ("broker-y".to_string(), 2)];
            assert_eq!(seed, keep_seed.then(|| first.into_iter().collect()));
        })
        .await;
    }
}
