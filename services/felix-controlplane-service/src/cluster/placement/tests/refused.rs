//! A leader the control plane still names at a generation it promised to an
//! elected node: its refusal is the evidence placement acts on, by naming it
//! again above that generation (rule 10 of "Electing without the control
//! plane" in docs/replication-design.md). End to end against the memory
//! store, from the cut-over in `pending_set`.
use felix_common::membership::RefusedGeneration;

use super::pending_set::{fenced_move, pass};
use super::reconciler::shard_zero;
use super::*;
use crate::model::{ElectedLeader, ReplicaReport};
use crate::store::memory::InMemoryStore;
use crate::store::{AssignmentWrite, ControlPlaneStore};

const AHEAD: u64 = 5;

/// `assignment`'s leader reports at its generation, refusing `refused`.
async fn refuses(store: &InMemoryStore, assignment: &ShardAssignment, refused: u64) {
    let node_incarnations = store
        .list_nodes()
        .await
        .expect("nodes")
        .into_iter()
        .map(|node| (node.node_id, node.status.incarnation))
        .collect();
    let written = store
        .record_replica_report(
            ReplicaReport {
                key: shard_zero(),
                generation: assignment.generation,
                caught_up: Default::default(),
                offsets: Default::default(),
                reported_at_millis: store.now_millis().await.expect("clock"),
                drained: false,
                leader_offset: None,
                halted: Default::default(),
                counter_offsets: Default::default(),
                stream_incarnation: None,
                by_round: false,
                leader_incarnation: None,
                follower_storage: Default::default(),
                leader: Some(assignment.leader.clone()),
                node_incarnations,
                given_up: false,
                inherited_storage: Default::default(),
                joint_start_acked: None,
                refused: Some(RefusedGeneration {
                    generation: refused,
                    promised_to: "broker-d".to_string(),
                }),
            },
            &assignment.leader,
        )
        .await
        .expect("report");
    assert_eq!(written, crate::store::ReportWrite::Stored);
}

/// The cut-over from {a, b, c} to {b, c, d} pending under b, and d, in the
/// new set only, elected past it: recorded, not adopted. The assignment keeps
/// b at the elected generation.
async fn recorded_not_adopted() -> (InMemoryStore, ShardAssignment) {
    let (store, _) = fenced_move(true).await;
    let cut_over = pass(&store).await;
    let pending = cut_over.pending_set.clone().expect("pending");
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            cut_over.generation,
            ElectedLeader {
                leader: "broker-d".to_string(),
                generation: cut_over.generation + AHEAD,
                replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
                set_generation: pending.generation,
            },
        )
        .await
        .expect("record");
    let AssignmentWrite::Written(recorded) = written else {
        panic!("not recorded: {written:?}");
    };
    assert_eq!(recorded.leader, "broker-b", "recorded, not adopted");
    assert_eq!(recorded.generation, cut_over.generation + AHEAD);
    (store, recorded)
}

/// **The leader's refusal at the elected generation names it again above
/// it**, on the same set and with the pending change kept. Its open then
/// succeeds and its fence deposes the elected leader through the ballot.
#[tokio::test]
async fn a_refusal_at_the_elected_generation_names_the_leader_again_above_it() {
    let (store, recorded) = recorded_not_adopted().await;
    // Nothing without the refusal: the leader is simply silent.
    assert_eq!(pass(&store).await, recorded);

    refuses(&store, &recorded, recorded.generation).await;
    let renamed = pass(&store).await;

    assert!(renamed.generation > recorded.generation, "{renamed:?}");
    assert_eq!(renamed.leader, recorded.leader);
    assert_eq!(renamed.replicas, recorded.replicas);
    assert_eq!(renamed.pending_set, recorded.pending_set, "still pending");
    assert_eq!(renamed.state, recorded.state);
    assert!(renamed.fence_storage_ids.is_empty());
}

/// **A refusal of an older generation than the assignment's is not
/// evidence.** The leader refused a generation placement has since moved
/// past, so nothing is written for it.
#[tokio::test]
async fn a_refusal_of_an_older_generation_does_nothing() {
    let (store, recorded) = recorded_not_adopted().await;
    refuses(&store, &recorded, recorded.generation - 1).await;
    assert_eq!(pass(&store).await, recorded);
}

/// **The step is taken once.** The new generation is above the refused one,
/// so the stored report no longer describes the assignment.
#[tokio::test]
async fn the_refusal_is_answered_once() {
    let (store, recorded) = recorded_not_adopted().await;
    refuses(&store, &recorded, recorded.generation).await;
    let renamed = pass(&store).await;
    assert!(renamed.generation > recorded.generation);
    assert_eq!(pass(&store).await, renamed);
}

/// **A fenced move ends through its own steps.** The leader is stopped and
/// the successor waits on its drained report, so a new generation would
/// cancel the fence the move is waiting on.
#[tokio::test]
async fn a_fenced_move_is_left_to_its_own_steps() {
    let (store, fenced) = fenced_move(true).await;
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            fenced.generation,
            ElectedLeader {
                leader: "broker-d".to_string(),
                generation: fenced.generation + AHEAD,
                replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
                set_generation: fenced.generation,
            },
        )
        .await
        .expect("record");
    let AssignmentWrite::Written(recorded) = written else {
        panic!("not recorded: {written:?}");
    };
    assert_eq!(recorded.leader, "broker-a");
    assert_eq!(recorded.state, ShardState::Draining);

    refuses(&store, &recorded, recorded.generation).await;
    assert_eq!(pass(&store).await, recorded);
}
