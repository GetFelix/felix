//! The write that records a down voter's copy id from the voters its group
//! reported (#3313), against a store. The store decides each one against
//! the record it lands on, as it does a broker's own report.
use super::super::plan::VouchedCopy;
use super::super::progress::CopyProgress;
use super::*;
use crate::store::ControlPlaneStore;
use crate::store::contract::raft_floors::entry;
use crate::store::contract::raft_targets::{held_set, try_native_report};
use crate::store::contract::shards::{clear, on_streams_of_its_own};
use crate::store::memory::InMemoryStore;
use crate::store::{StoreConfig, StoreError};

const COPY_X: u64 = 41;
const VOTED_Z: u64 = 43;
const ROTATED_Z: u64 = 44;

fn raft_store() -> InMemoryStore {
    InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    })
    .with_shard_engine("raft")
}

/// `{x, y}` formed with voters `{x, y, z}`, then moved to `{x, z}`: the move
/// dropped the copy ids, `x` reported again, and `z` has not, though the
/// voters name it under `VOTED_Z`.
async fn z_unreported(store: &InMemoryStore) -> ShardAssignment {
    let held = held_set(store).await;
    let mut formed = entry(&held, 1, 1);
    let mut voters = held.raft_target.clone().expect("target").members;
    voters.insert("broker-z".to_string(), VOTED_Z);
    formed.voters = Some(voters);
    store
        .record_raft_floors("broker-x", vec![formed])
        .await
        .expect("floors");
    let mut moved = held.clone();
    moved.replicas = vec!["broker-z".to_string()];
    store.put_shard_assignment(moved).await.expect("move");
    let at = Some(held.incarnation);
    try_native_report(store, &held, "broker-x", COPY_X, at)
        .await
        .expect("x");
    let shard = store.get_shard_assignment(&held.key).await.expect("shard");
    assert_eq!(shard.raft_copy_ids.keys().collect::<Vec<_>>(), ["broker-x"]);
    assert!(shard.raft_target.is_none());
    shard
}

/// What placement plans for `shard`'s unreported `broker-z`, from the
/// registration as it is now.
async fn vouch(store: &InMemoryStore, shard: &ShardAssignment, copy_id: u64) -> VouchedCopy {
    let process = store.get_node("broker-z").await.expect("node").status;
    VouchedCopy {
        key: shard.key.clone(),
        node_id: "broker-z".to_string(),
        registered_at_millis: process.registered_at_millis,
        node_incarnation: process.incarnation,
        copy_id,
        shard_incarnation: shard.incarnation,
        group_epoch: shard.group_epoch(),
    }
}

async fn write(store: &InMemoryStore, vouched: &VouchedCopy) -> Result<bool, StoreError> {
    store
        .record_raft_copy_id(
            &vouched.key,
            &vouched.node_id,
            vouched.registered_at_millis,
            vouched.node_incarnation,
            vouched.copy_id,
            Some(vouched.shard_incarnation),
            Some(vouched.group_epoch),
        )
        .await
}

async fn stored(store: &InMemoryStore, shard: &ShardAssignment) -> ShardAssignment {
    store.get_shard_assignment(&shard.key).await.expect("shard")
}

fn is_conflict(result: Result<bool, StoreError>) -> bool {
    matches!(result, Err(StoreError::Conflict(_)))
}

#[tokio::test]
async fn a_vouch_records_the_copy_and_stores_the_target() {
    let store = raft_store();
    on_streams_of_its_own(&store, async {
        let shard = z_unreported(&store).await;
        assert_eq!(
            write(&store, &vouch(&store, &shard, VOTED_Z).await)
                .await
                .ok(),
            Some(true)
        );
        let after = stored(&store, &shard).await;
        assert_eq!(after.raft_copy_ids.get("broker-z"), Some(&VOTED_Z));
        let target = after.raft_target.expect("target");
        assert_eq!(
            target.members,
            BTreeMap::from([
                ("broker-x".to_string(), COPY_X),
                ("broker-z".to_string(), VOTED_Z)
            ])
        );
        clear(&store).await;
    })
    .await;
}

/// The broker registered again between the plan and the write, so the id is
/// for a process that no longer stands.
#[tokio::test]
async fn a_vouch_planned_on_an_old_registration_is_refused() {
    let store = raft_store();
    on_streams_of_its_own(&store, async {
        let shard = z_unreported(&store).await;
        let planned = vouch(&store, &shard, VOTED_Z).await;
        store
            .register_node(crate::store::contract::nodes::node("broker-z", 7598))
            .await
            .expect("register again");
        let now = store.get_node("broker-z").await.expect("node").status;
        assert!(now.incarnation > planned.node_incarnation);
        assert!(is_conflict(write(&store, &planned).await));
        assert!(
            !stored(&store, &shard)
                .await
                .raft_copy_ids
                .contains_key("broker-z")
        );
        clear(&store).await;
    })
    .await;
}

#[tokio::test]
async fn a_vouch_for_another_incarnation_or_epoch_is_refused() {
    let store = raft_store();
    on_streams_of_its_own(&store, async {
        let shard = z_unreported(&store).await;
        let planned = vouch(&store, &shard, VOTED_Z).await;
        let other_incarnation = VouchedCopy {
            shard_incarnation: planned.shard_incarnation + 1,
            ..planned.clone()
        };
        assert!(is_conflict(write(&store, &other_incarnation).await));
        let other_epoch = VouchedCopy {
            group_epoch: planned.group_epoch + 1,
            ..planned.clone()
        };
        assert!(is_conflict(write(&store, &other_epoch).await));
        assert!(
            !stored(&store, &shard)
                .await
                .raft_copy_ids
                .contains_key("broker-z")
        );
        clear(&store).await;
    })
    .await;
}

/// A pass another writer got ahead of was planned from what that writer
/// replaced, so none of its vouches is written. With the token current the
/// same vouch lands, so the refusal is the fence's.
#[tokio::test]
async fn a_fenced_pass_does_not_vouch() {
    let store = raft_store();
    on_streams_of_its_own(&store, async {
        let shard = z_unreported(&store).await;
        let vouched = vec![vouch(&store, &shard, VOTED_Z).await];
        let mut progress = CopyProgress::default();
        let mut plan = async || {
            super::super::reconciler::plan_pass(
                &store,
                &Default::default(),
                MovePolicy::default(),
                &mut progress,
            )
            .await
            .expect("plan")
        };
        let stale = plan().await;
        // A write the stale token stops, ahead of the vouches.
        let write_ahead = ShardPlan {
            key: shard.key.clone(),
            decision: Decision::Move(MoveStep::PendingSetTimedOut, shard.clone()),
        };
        let stale = stale.with_plan(vec![write_ahead], vouched.clone());
        // Another instance writes first, moving the placement token on.
        let other = plan().await;
        let other =
            super::super::reconciler::apply_pass(&store, &other, &mut Default::default()).await;
        assert!(other.placed > 0, "{other:?}");
        let outcome =
            super::super::reconciler::apply_pass(&store, &stale, &mut Default::default()).await;
        assert!(outcome.fenced, "{outcome:?}");
        assert!(
            !stored(&store, &shard)
                .await
                .raft_copy_ids
                .contains_key("broker-z")
        );

        let current = plan().await.with_plan(Vec::new(), vouched);
        let outcome =
            super::super::reconciler::apply_pass(&store, &current, &mut Default::default()).await;
        assert!(!outcome.fenced, "{outcome:?}");
        assert_eq!(
            stored(&store, &shard).await.raft_copy_ids.get("broker-z"),
            Some(&VOTED_Z)
        );
        clear(&store).await;
    })
    .await;
}

/// **A known limit** (#3322). `broker-z` reports a rotated copy between the
/// plan and the vouched write, and the write puts the voted id back over
/// it. The formed-target check does not hold the target back: the voted id
/// is the one the voters name, so the target is stored naming a copy
/// `broker-z` no longer runs, until `broker-z` reports again and its own id
/// replaces it. A copy answers only for its own member id, so that target
/// forms nothing in the meantime.
#[tokio::test]
async fn known_limit_a_vouch_overwrites_a_rotated_copy_reported_after_the_plan() {
    let store = raft_store();
    on_streams_of_its_own(&store, async {
        let shard = z_unreported(&store).await;
        let planned = vouch(&store, &shard, VOTED_Z).await;
        let at = Some(shard.incarnation);
        assert!(
            try_native_report(&store, &shard, "broker-z", ROTATED_Z, at)
                .await
                .expect("z")
        );
        let own = stored(&store, &shard).await;
        assert_eq!(own.raft_copy_ids.get("broker-z"), Some(&ROTATED_Z));
        let own_target = own.raft_target.expect("one voter of two still matches");
        assert_eq!(own_target.members.get("broker-z"), Some(&ROTATED_Z));

        assert!(write(&store, &planned).await.expect("vouch lands"));
        let after = stored(&store, &shard).await;
        assert_eq!(after.raft_copy_ids.get("broker-z"), Some(&VOTED_Z));
        let target = after.raft_target.expect("target");
        assert_eq!(target.members.get("broker-z"), Some(&VOTED_Z));
        assert!(target.counter > own_target.counter);

        // The broker's report is not refused as a repeat: it puts its copy
        // back.
        assert!(
            try_native_report(&store, &shard, "broker-z", ROTATED_Z, at)
                .await
                .expect("z again")
        );
        let healed = stored(&store, &shard).await;
        assert_eq!(healed.raft_copy_ids.get("broker-z"), Some(&ROTATED_Z));
        clear(&store).await;
    })
    .await;
}
