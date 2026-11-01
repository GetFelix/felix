//! Adopting a leader that elected itself (`CpAdopt` in
//! docs/formal/FelixShard.tla): every backend writes it at the elected
//! generation, conditionally, and issues every later generation above it.
use super::nodes::node;
use super::shards::{assignment, clear, key, token};
use crate::model::{ElectedLeader, PendingSet, ShardAssignment};
use crate::store::{AssignmentWrite, ControlPlaneStore};

pub(super) async fn run(store: &dyn ControlPlaneStore) {
    let _ = store.register_node(node("broker-z", 7599)).await;
    a_leader_elected_on_the_set_is_adopted_at_its_generation(store).await;
    a_planner_on_an_older_read_loses_to_adoption(store).await;
    an_adoption_on_an_older_read_loses(store).await;
    an_election_not_above_the_generation_is_not_adopted(store).await;
    an_election_on_another_set_moves_only_the_generation(store).await;
    every_write_after_an_adoption_issues_above_it(store).await;
    an_election_on_the_old_set_is_recorded_not_adopted(store).await;
    an_election_on_the_pending_set_is_adopted_and_stays_pending(store).await;
}

/// x leading y and z.
async fn three(store: &dyn ControlPlaneStore) -> ShardAssignment {
    clear(store).await;
    store
        .put_shard_assignment(ShardAssignment {
            replicas: vec!["broker-y".to_string(), "broker-z".to_string()],
            ..assignment(0, "broker-x")
        })
        .await
        .expect("assign")
}

fn elected(leader: &str, generation: u64, replicas: &[&str], set_generation: u64) -> ElectedLeader {
    ElectedLeader {
        leader: leader.to_string(),
        generation,
        replicas: replicas.iter().map(|node| node.to_string()).collect(),
        set_generation,
    }
}

fn written(write: AssignmentWrite) -> ShardAssignment {
    match write {
        AssignmentWrite::Written(written) => written,
        other => panic!("expected a write, got {other:?}"),
    }
}

/// **y elected itself three generations past x's, on x's set: it becomes the
/// leader at exactly that generation**, x a follower, and the change feed
/// carries it.
async fn a_leader_elected_on_the_set_is_adopted_at_its_generation(store: &dyn ControlPlaneStore) {
    let current = three(store).await;
    let at = current.generation + 3;
    let adopted = written(
        store
            .adopt_elected_leader(
                &key(0),
                current.generation,
                elected(
                    "broker-y",
                    at,
                    &["broker-x", "broker-z"],
                    current.generation,
                ),
            )
            .await
            .expect("adopt"),
    );
    assert_eq!(adopted.leader, "broker-y");
    assert_eq!(adopted.generation, at);
    assert_eq!(adopted.replicas, vec!["broker-x", "broker-z"]);
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        adopted
    );
}

/// **A planner that read x's generation loses to the adoption that landed
/// since**, as it loses to any newer write.
async fn a_planner_on_an_older_read_loses_to_adoption(store: &dyn ControlPlaneStore) {
    let read = three(store).await;
    let fence = token(store).await;
    let at = read.generation + 1;
    written(
        store
            .adopt_elected_leader(
                &key(0),
                read.generation,
                elected("broker-y", at, &["broker-x", "broker-z"], read.generation),
            )
            .await
            .expect("adopt"),
    );
    let planned = store
        .put_shard_assignment_if(
            ShardAssignment {
                leader: "broker-z".to_string(),
                replicas: vec!["broker-x".to_string(), "broker-y".to_string()],
                ..read.clone()
            },
            Some(read.generation),
            fence,
        )
        .await
        .expect("plan");
    assert_eq!(planned, AssignmentWrite::Stale { current: Some(at) });
    assert_eq!(
        store
            .get_shard_assignment(&key(0))
            .await
            .expect("get")
            .leader,
        "broker-y"
    );
}

/// **An adoption decided on an older read loses to a newer write**, and
/// writes nothing.
async fn an_adoption_on_an_older_read_loses(store: &dyn ControlPlaneStore) {
    let read = three(store).await;
    let newer = store
        .put_shard_assignment(ShardAssignment {
            replicas: vec!["broker-y".to_string(), "broker-z".to_string()],
            ..assignment(0, "broker-x")
        })
        .await
        .expect("newer");
    let lost = store
        .adopt_elected_leader(
            &key(0),
            read.generation,
            elected(
                "broker-y",
                newer.generation + 5,
                &["broker-x", "broker-z"],
                0,
            ),
        )
        .await
        .expect("adopt");
    assert_eq!(
        lost,
        AssignmentWrite::Stale {
            current: Some(newer.generation)
        }
    );
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        newer
    );
}

/// **A generation the control plane has already reached is not adopted**:
/// it names that generation's leader already, or one above it.
async fn an_election_not_above_the_generation_is_not_adopted(store: &dyn ControlPlaneStore) {
    let current = three(store).await;
    let refused = store
        .adopt_elected_leader(
            &key(0),
            current.generation,
            elected("broker-y", current.generation, &["broker-x", "broker-z"], 0),
        )
        .await
        .expect("adopt");
    assert_eq!(
        refused,
        AssignmentWrite::Stale {
            current: Some(current.generation)
        }
    );
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        current
    );
}

/// **A leader on a set the control plane does not record is not adopted, but
/// its generation is taken**: x stays the leader, at y's generation, so the
/// next write is above it.
async fn an_election_on_another_set_moves_only_the_generation(store: &dyn ControlPlaneStore) {
    let current = three(store).await;
    let at = current.generation + 2;
    let recorded = written(
        store
            .adopt_elected_leader(
                &key(0),
                current.generation,
                elected("broker-y", at, &["broker-x"], 0),
            )
            .await
            .expect("record"),
    );
    assert_eq!(
        recorded,
        ShardAssignment {
            generation: at,
            ..current
        }
    );
}

/// **Rule 10: never at or below an elected generation.** Each kind of
/// write after an adoption, unconditional and conditional, lands above it.
async fn every_write_after_an_adoption_issues_above_it(store: &dyn ControlPlaneStore) {
    let current = three(store).await;
    let at = current.generation + 4;
    let adopted = written(
        store
            .adopt_elected_leader(
                &key(0),
                current.generation,
                elected("broker-y", at, &["broker-x", "broker-z"], 0),
            )
            .await
            .expect("adopt"),
    );
    let fence = token(store).await;
    let conditional = written(
        store
            .put_shard_assignment_if(adopted.clone(), Some(adopted.generation), fence)
            .await
            .expect("conditional"),
    );
    assert!(conditional.generation > at, "{conditional:?}");
    let unconditional = store
        .put_shard_assignment(adopted)
        .await
        .expect("unconditional");
    assert!(unconditional.generation > conditional.generation);
}

/// x is moving the shard from {x, y, z} toward {x, y, broker-nobody-else}:
/// with three nodes, {x, z} is the only other set, so z leaves.
async fn pending(store: &dyn ControlPlaneStore) -> ShardAssignment {
    let current = three(store).await;
    store
        .put_shard_assignment(ShardAssignment {
            replicas: vec!["broker-y".to_string(), "broker-z".to_string()],
            pending_set: Some(PendingSet {
                replicas: vec!["broker-y".to_string()],
                generation: current.generation + 1,
            }),
            ..assignment(0, "broker-x")
        })
        .await
        .expect("pending")
}

/// **ElectAdoptNoMatch's case: while a change is pending, a leader elected
/// on the old set is recorded, not adopted.** The new set's leader deposes
/// it with its newer set generation; the control plane keeps its leader and
/// its pending set, and only moves its generation past the election.
async fn an_election_on_the_old_set_is_recorded_not_adopted(store: &dyn ControlPlaneStore) {
    let current = pending(store).await;
    let at = current.generation + 1;
    let recorded = written(
        store
            .adopt_elected_leader(
                &key(0),
                current.generation,
                elected(
                    "broker-z",
                    at,
                    &["broker-x", "broker-y"],
                    current.generation - 1,
                ),
            )
            .await
            .expect("record"),
    );
    assert_eq!(recorded.leader, "broker-x");
    assert_eq!(recorded.generation, at);
    assert_eq!(recorded.pending_set, current.pending_set);
    assert_eq!(recorded.replicas, current.replicas);
}

/// **A leader elected on the pending set, at its generation, is adopted, and
/// the change stays pending** until that leader reports the joint start.
async fn an_election_on_the_pending_set_is_adopted_and_stays_pending(
    store: &dyn ControlPlaneStore,
) {
    let current = pending(store).await;
    let set_generation = current.pending_set.as_ref().expect("pending").generation;
    let at = current.generation + 1;
    let adopted = written(
        store
            .adopt_elected_leader(
                &key(0),
                current.generation,
                elected("broker-y", at, &["broker-x"], set_generation),
            )
            .await
            .expect("adopt"),
    );
    assert_eq!(adopted.leader, "broker-y");
    assert_eq!(adopted.generation, at);
    assert_eq!(adopted.replicas, vec!["broker-x", "broker-z"]);
    assert_eq!(
        adopted.pending_set,
        Some(PendingSet {
            replicas: vec!["broker-x".to_string()],
            generation: set_generation,
        })
    );
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        adopted
    );
}
