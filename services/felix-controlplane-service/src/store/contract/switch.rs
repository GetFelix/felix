//! A shard's switch to Raft, as every backend must record it: one seal
//! order per generation, one report or one give-up for it, and nothing
//! written to a shard that switched.
use std::collections::BTreeMap;

use super::shards::{assignment, clear, key};
use crate::model::{
    ElectedLeader, SealReport, SealedSideLog, ShardAssignment, ShardKey, ShardKind, ShardState,
};
use crate::store::{AssignmentWrite, ControlPlaneStore, StoreError};

const COPY_X: &str = "000102030405060708090a0b0c0d0e0f";
const COPY_Y: &str = "101112131415161718191a1b1c1d1e1f";

pub(super) async fn run_switch_contract(store: &dyn ControlPlaneStore) {
    a_seal_order_names_the_leader_at_the_next_generation(store).await;
    a_seal_is_ordered_only_over_the_generation_read(store).await;
    only_a_settled_stream_shard_is_sealed(store).await;
    one_report_wins_a_generation(store).await;
    a_report_of_another_generation_loses(store).await;
    a_give_up_never_lands_beside_a_report(store).await;
    a_report_never_lands_after_a_give_up(store).await;
    a_later_generation_gives_the_seal_up(store).await;
    no_shard_switches_without_a_seal(store).await;
    a_report_must_name_a_majority_with_the_sealer(store).await;
    a_report_must_name_the_copy_the_seal_was_ordered_from(store).await;
    nothing_is_written_to_a_switched_shard(store).await;
    a_seal_given_up_can_be_ordered_again(store).await;
}

/// An active shard led by broker-x with broker-y as its replica.
async fn active(store: &dyn ControlPlaneStore, shard: u32) -> ShardAssignment {
    clear(store).await;
    let mut wanted = assignment(shard, "broker-x");
    wanted.replicas = vec!["broker-y".to_string()];
    wanted.member_storage_ids = BTreeMap::from([("broker-x".to_string(), COPY_X.to_string())]);
    store
        .put_shard_assignment(wanted.clone())
        .await
        .expect("assign");
    wanted.state = ShardState::Active;
    store.put_shard_assignment(wanted).await.expect("activate")
}

async fn sealed(store: &dyn ControlPlaneStore, shard: u32) -> ShardAssignment {
    let current = active(store, shard).await;
    written(
        store
            .seal_shard(&current.key, current.generation)
            .await
            .expect("seal"),
    )
}

fn written(write: AssignmentWrite) -> ShardAssignment {
    match write {
        AssignmentWrite::Written(stored) => stored,
        other => panic!("expected a write, got {other:?}"),
    }
}

fn report(generation: u64, voters: &[(&str, &str)]) -> SealReport {
    let side = SealedSideLog {
        start: 0,
        end: 3,
        digest: "cd".repeat(32),
    };
    SealReport {
        generation,
        offset: 40,
        start: 2,
        digest: "ab".repeat(32),
        dead_letters: side.clone(),
        cursors: side,
        voters: voters
            .iter()
            .map(|(node, copy)| (node.to_string(), copy.to_string()))
            .collect(),
    }
}

fn both(generation: u64) -> SealReport {
    report(generation, &[("broker-x", COPY_X), ("broker-y", COPY_Y)])
}

async fn read(store: &dyn ControlPlaneStore, key: &ShardKey) -> ShardAssignment {
    store.get_shard_assignment(key).await.expect("assignment")
}

async fn a_seal_order_names_the_leader_at_the_next_generation(store: &dyn ControlPlaneStore) {
    let before = active(store, 0).await;
    assert!(before.switch.is_none());
    let after = written(
        store
            .seal_shard(&before.key, before.generation)
            .await
            .expect("seal"),
    );
    assert!(after.generation > before.generation);
    assert_eq!(after.leader, before.leader);
    assert_eq!(after.replicas, before.replicas);
    let order = after.sealing().expect("a seal order stands");
    assert_eq!(order.generation, after.generation);
    assert_eq!(order.sealer, "broker-x");
    assert_eq!(order.sealer_copy.as_deref(), Some(COPY_X));
    assert!(!after.switched());
    assert_eq!(read(store, &after.key).await, after);
}

async fn a_seal_is_ordered_only_over_the_generation_read(store: &dyn ControlPlaneStore) {
    let current = active(store, 0).await;
    let stale = store
        .seal_shard(&current.key, current.generation - 1)
        .await
        .expect("seal");
    assert_eq!(
        stale,
        AssignmentWrite::Stale {
            current: Some(current.generation)
        }
    );
    assert!(read(store, &current.key).await.switch.is_none());
    clear(store).await;
    assert_eq!(
        store.seal_shard(&key(0), 1).await.expect("seal"),
        AssignmentWrite::Stale { current: None }
    );
}

async fn only_a_settled_stream_shard_is_sealed(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let assigning = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assign");
    let err = store
        .seal_shard(&assigning.key, assigning.generation)
        .await
        .expect_err("not active");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let mut moving = active(store, 0).await;
    moving.successor = Some("broker-y".to_string());
    moving.move_reason = Some(crate::model::MoveReason::Operator);
    moving.move_started_at_millis = Some(1);
    let moving = store.put_shard_assignment(moving).await.expect("stage");
    let err = store
        .seal_shard(&moving.key, moving.generation)
        .await
        .expect_err("moving");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    clear(store).await;
    let mut cache = assignment(0, "broker-x");
    cache.key.kind = ShardKind::Cache;
    store
        .put_shard_assignment(cache.clone())
        .await
        .expect("assign");
    cache.state = ShardState::Active;
    let cache = store.put_shard_assignment(cache).await.expect("activate");
    let err = store
        .seal_shard(&cache.key, cache.generation)
        .await
        .expect_err("a cache");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(read(store, &cache.key).await.switch.is_none());
}

async fn one_report_wins_a_generation(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let first = both(order.generation);
    let switched = written(
        store
            .accept_seal_report(&order.key, first.clone())
            .await
            .expect("report"),
    );
    assert!(switched.switched());
    // The seal's generation is the base's: the write keeps it.
    assert_eq!(switched.generation, order.generation);
    assert_eq!(
        switched.switch.as_ref().and_then(|s| s.report.as_ref()),
        Some(&first)
    );

    // A second report of the same seal loses, whatever it says.
    let mut second = both(order.generation);
    second.offset += 1;
    assert_eq!(
        store
            .accept_seal_report(&order.key, second)
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(order.generation)
        }
    );
    assert_eq!(read(store, &order.key).await, switched);

    // The recorded one sent again is answered as recorded, and writes nothing.
    let again = written(
        store
            .accept_seal_report(&order.key, first)
            .await
            .expect("report"),
    );
    assert_eq!(again, switched);
}

/// The report names the seal it is for. One for another generation is not
/// this order's, though an order stands.
async fn a_report_of_another_generation_loses(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    for generation in [order.generation - 1, order.generation + 1] {
        assert_eq!(
            store
                .accept_seal_report(&order.key, both(generation))
                .await
                .expect("report"),
            AssignmentWrite::Stale {
                current: Some(order.generation)
            }
        );
    }
    assert_eq!(read(store, &order.key).await, order);
}

async fn a_give_up_never_lands_beside_a_report(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let switched = written(
        store
            .accept_seal_report(&order.key, both(order.generation))
            .await
            .expect("report"),
    );
    assert_eq!(
        store
            .give_up_seal(&order.key, order.generation)
            .await
            .expect("give up"),
        AssignmentWrite::Stale {
            current: Some(order.generation)
        }
    );
    assert_eq!(read(store, &order.key).await, switched);
}

async fn a_report_never_lands_after_a_give_up(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let reopened = written(
        store
            .give_up_seal(&order.key, order.generation)
            .await
            .expect("give up"),
    );
    assert!(reopened.generation > order.generation);
    assert!(reopened.switch.is_none());
    assert_eq!(reopened.leader, order.leader);
    assert_eq!(reopened.replicas, order.replicas);
    assert_eq!(
        store
            .accept_seal_report(&order.key, both(order.generation))
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(reopened.generation)
        }
    );
    // Nor one that claims the generation the shard reopened at.
    assert_eq!(
        store
            .accept_seal_report(&order.key, both(reopened.generation))
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(reopened.generation)
        }
    );
    assert!(!read(store, &order.key).await.switched());
    // A give-up is for one seal: a second finds none standing.
    assert_eq!(
        store
            .give_up_seal(&order.key, reopened.generation)
            .await
            .expect("give up"),
        AssignmentWrite::Stale {
            current: Some(reopened.generation)
        }
    );
    assert_eq!(read(store, &order.key).await, reopened);
}

/// A failover or any other placement write moves the shard past the seal's
/// generation, and the seal's report then finds nothing to win.
async fn a_later_generation_gives_the_seal_up(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let mut failover = order.clone();
    failover.leader = "broker-y".to_string();
    failover.replicas = vec!["broker-x".to_string()];
    let promoted = store.put_shard_assignment(failover).await.expect("promote");
    assert!(promoted.generation > order.generation);
    // Store-owned: the order the caller passed back did not survive.
    assert!(promoted.switch.is_none());
    assert_eq!(
        store
            .accept_seal_report(&order.key, both(order.generation))
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(promoted.generation)
        }
    );
    assert!(!read(store, &order.key).await.switched());
}

async fn no_shard_switches_without_a_seal(store: &dyn ControlPlaneStore) {
    let current = active(store, 0).await;
    assert_eq!(
        store
            .accept_seal_report(&current.key, both(current.generation))
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(current.generation)
        }
    );
    assert!(read(store, &current.key).await.switch.is_none());
    // A caller cannot write the record either.
    let mut forged = current.clone();
    forged.switch = Some(crate::model::ShardSwitch {
        generation: current.generation + 1,
        sealer: "broker-x".to_string(),
        sealer_copy: None,
        report: Some(both(current.generation + 1)),
    });
    let stored = store.put_shard_assignment(forged).await.expect("put");
    assert!(stored.switch.is_none());
}

async fn a_report_must_name_a_majority_with_the_sealer(store: &dyn ControlPlaneStore) {
    // Three copies and no recorded copy id, so each refusal below has one
    // reason.
    let _ = store
        .register_node(super::nodes::node("broker-z", 7502))
        .await;
    clear(store).await;
    let mut wanted = assignment(0, "broker-x");
    wanted.replicas = vec!["broker-y".to_string(), "broker-z".to_string()];
    store
        .put_shard_assignment(wanted.clone())
        .await
        .expect("assign");
    wanted.state = ShardState::Active;
    let current = store.put_shard_assignment(wanted).await.expect("activate");
    let order = written(
        store
            .seal_shard(&current.key, current.generation)
            .await
            .expect("seal"),
    );
    let refused = [
        // One of three copies.
        report(order.generation, &[("broker-x", COPY_X)]),
        // A majority, but the sealer did not acknowledge.
        report(
            order.generation,
            &[("broker-y", COPY_Y), ("broker-z", COPY_X)],
        ),
        // A node outside the set.
        report(
            order.generation,
            &[("broker-x", COPY_X), ("broker-w", COPY_Y)],
        ),
        // Not a copy id.
        report(order.generation, &[("broker-x", COPY_X), ("broker-y", "y")]),
    ];
    for report in refused {
        let err = store
            .accept_seal_report(&order.key, report.clone())
            .await
            .expect_err("refused");
        assert!(
            matches!(err, StoreError::Conflict(_)),
            "{report:?}: {err:?}"
        );
    }
    let mut undigested = both(order.generation);
    undigested.digest = "ab".to_string();
    let err = store
        .accept_seal_report(&order.key, undigested)
        .await
        .expect_err("no digest");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    // A refused report does not use the seal up: two of three then win it.
    assert_eq!(read(store, &order.key).await, order);
    assert!(
        written(
            store
                .accept_seal_report(&order.key, both(order.generation))
                .await
                .expect("report")
        )
        .switched()
    );
}

/// A sealer whose disk was replaced comes back under a new copy id, and
/// holds nothing the seal can rest on.
async fn a_report_must_name_the_copy_the_seal_was_ordered_from(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let err = store
        .accept_seal_report(
            &order.key,
            report(
                order.generation,
                &[("broker-x", COPY_Y), ("broker-y", COPY_Y)],
            ),
        )
        .await
        .expect_err("another copy");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(!read(store, &order.key).await.switched());
}

async fn nothing_is_written_to_a_switched_shard(store: &dyn ControlPlaneStore) {
    let order = sealed(store, 0).await;
    let switched = written(
        store
            .accept_seal_report(&order.key, both(order.generation))
            .await
            .expect("report"),
    );

    let mut failover = switched.clone();
    failover.leader = "broker-y".to_string();
    failover.replicas = vec!["broker-x".to_string()];
    let err = store
        .put_shard_assignment(failover.clone())
        .await
        .expect_err("a hybrid promotion");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    let token = super::shards::token(store).await;
    let err = store
        .put_shard_assignment_if(failover, Some(switched.generation), token)
        .await
        .expect_err("a placement write");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    let err = store
        .adopt_elected_leader(
            &switched.key,
            switched.generation,
            ElectedLeader {
                leader: "broker-y".to_string(),
                generation: switched.generation + 1,
                replicas: vec!["broker-x".to_string()],
                set_generation: switched.generation,
            },
        )
        .await
        .expect_err("a hybrid election");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    let err = store
        .seal_shard(&switched.key, switched.generation)
        .await
        .expect_err("a second seal");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(read(store, &switched.key).await, switched);

    // Deleting the stream's shard still works.
    store
        .delete_shard_assignment(&switched.key)
        .await
        .expect("delete");
}

async fn a_seal_given_up_can_be_ordered_again(store: &dyn ControlPlaneStore) {
    let first = sealed(store, 0).await;
    let reopened = written(
        store
            .give_up_seal(&first.key, first.generation)
            .await
            .expect("give up"),
    );
    let second = written(
        store
            .seal_shard(&reopened.key, reopened.generation)
            .await
            .expect("seal again"),
    );
    assert!(second.generation > reopened.generation);
    // The first seal's report is still on its way; only the second's wins.
    assert_eq!(
        store
            .accept_seal_report(&first.key, both(first.generation))
            .await
            .expect("report"),
        AssignmentWrite::Stale {
            current: Some(second.generation)
        }
    );
    let switched = written(
        store
            .accept_seal_report(&second.key, both(second.generation))
            .await
            .expect("report"),
    );
    assert_eq!(
        switched
            .switch
            .as_ref()
            .and_then(|switch| switch.report.as_ref())
            .map(|report| report.generation),
        Some(second.generation)
    );
    clear(store).await;
}
