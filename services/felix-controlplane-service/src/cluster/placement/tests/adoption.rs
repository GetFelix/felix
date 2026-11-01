//! Placement after the control plane adopted an elected leader: every
//! generation it issues is above the elected one (rule 10 of "Electing
//! without the control plane" in docs/replication-design.md). End to end
//! against the memory store, from the cut-over in `pending_set`.
use super::pending_set::{fenced_move, pass, report};
use super::reconciler::shard_zero;
use super::*;
use crate::model::ElectedLeader;
use crate::store::memory::InMemoryStore;
use crate::store::{AssignmentWrite, ControlPlaneStore};

/// How far past the control plane's generation the election lands, so a
/// write issued from the old generation would fall at or below it.
const AHEAD: u64 = 5;

/// The cut-over from {a, b, c} to {b, c, d} pending under b, and c, in both
/// sets, elected on the new one past it.
/// Answers the store, the adoption and the elected generation.
async fn adopted_on_the_pending_set() -> (InMemoryStore, ShardAssignment, u64) {
    let (store, _) = fenced_move(true).await;
    let cut_over = pass(&store).await;
    let pending = cut_over.pending_set.clone().expect("pending");
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            cut_over.generation,
            ElectedLeader {
                leader: "broker-c".to_string(),
                generation: cut_over.generation + AHEAD,
                replicas: vec!["broker-b".to_string(), "broker-d".to_string()],
                set_generation: pending.generation,
            },
        )
        .await
        .expect("adopt");
    let AssignmentWrite::Written(adopted) = written else {
        panic!("not adopted: {written:?}");
    };
    assert_eq!(adopted.leader, "broker-c");
    assert_eq!(
        adopted.pending_set.as_ref().map(|p| p.generation),
        Some(pending.generation)
    );
    (store, adopted, cut_over.generation + AHEAD)
}

/// **Taking the pending set issues above the adopted generation**, on the
/// adopted leader's own report of its joint start.
#[tokio::test]
async fn taking_a_pending_set_after_an_adoption_issues_above_it() {
    let (store, adopted, elected) = adopted_on_the_pending_set().await;
    let set_generation = adopted.pending_set.as_ref().expect("pending").generation;
    // Still pending: nothing but the adopted leader's word takes it.
    assert_eq!(pass(&store).await, adopted);
    report(&store, &adopted, false, Some(set_generation)).await;
    let taken = pass(&store).await;
    assert_eq!(taken.pending_set, None);
    assert_eq!(taken.leader, "broker-c");
    assert_eq!(
        taken.nodes().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["broker-b", "broker-c", "broker-d"])
    );
    assert!(taken.generation > elected, "{taken:?}");
}

/// **Dropping a pending set on the timeout issues above the adopted
/// generation.** Two passes by one instance, so it has watched the change.
#[tokio::test]
async fn a_pending_timeout_after_an_adoption_issues_above_it() {
    use super::super::metrics::MoveClock;
    use super::super::progress::CopyProgress;
    use super::super::reconciler::{apply_pass, plan_pass};
    let (store, _, elected) = adopted_on_the_pending_set().await;
    let policy = MovePolicy {
        pending_set_timeout_millis: Some(1),
        ..MovePolicy::default()
    };
    let mut progress = CopyProgress::default();
    let liveness = Default::default();
    let wakes = PlacementWakes::default();
    let mut clock = MoveClock::default();
    let first = plan_pass(&store, &liveness, policy.clone(), &mut progress)
        .await
        .expect("read");
    apply_pass(&store, &first, &mut clock, &wakes).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let second = plan_pass(&store, &liveness, policy, &mut progress)
        .await
        .expect("read");
    apply_pass(&store, &second, &mut clock, &wakes).await;
    let dropped = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(dropped.pending_set, None, "{dropped:?}");
    assert!(dropped.generation > elected, "{dropped:?}");
}

/// **A failover after an adoption issues above it**: the adopted leader's
/// broker goes down and a follower is promoted.
#[tokio::test]
async fn a_failover_after_an_adoption_issues_above_it() {
    let (store, adopted, elected) = adopted_on_the_pending_set().await;
    report(&store, &adopted, false, None).await;
    store
        .set_node_lifecycle("broker-c", NodeLifecycle::Down)
        .await
        .expect("down");
    let promoted = pass(&store).await;
    assert_ne!(promoted.leader, "broker-c", "{promoted:?}");
    assert!(promoted.generation > elected, "{promoted:?}");
}

/// **A pass that planned the cut-over from the fenced read loses to an
/// adoption that landed after its read**, and the adopted leader stays.
#[tokio::test]
async fn a_planner_on_an_older_read_loses_to_adoption() {
    use super::super::metrics::MoveClock;
    use super::super::progress::CopyProgress;
    use super::super::reconciler::{apply_pass, plan_pass};
    let (store, fenced) = fenced_move(true).await;
    let planned = plan_pass(
        &store,
        &Default::default(),
        MovePolicy::default(),
        &mut CopyProgress::default(),
    )
    .await
    .expect("read");
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            fenced.generation,
            ElectedLeader {
                leader: "broker-c".to_string(),
                generation: fenced.generation + AHEAD,
                replicas: vec!["broker-a".to_string(), "broker-b".to_string()],
                set_generation: fenced.generation,
            },
        )
        .await
        .expect("adopt");
    let AssignmentWrite::Written(adopted) = written else {
        panic!("not adopted: {written:?}");
    };
    assert_eq!(adopted.leader, "broker-c");
    assert_eq!(adopted.successor, None, "the move planned around a ends");
    apply_pass(
        &store,
        &planned,
        &mut MoveClock::default(),
        &PlacementWakes::default(),
    )
    .await;
    assert_eq!(
        store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get"),
        adopted,
        "the cut-over planned from the older read landed over the adoption"
    );
}

/// **While a change is pending, a leader elected on the old set is recorded,
/// not adopted** (`FelixShardElectAdoptNoMatch.cfg`'s case under
/// `AdoptMatch`): the new set's leader, carrying the newer set generation,
/// deposes it. The control plane keeps b and the pending set and takes only
/// the generation, so b's joint start must come at it, and what placement
/// writes next is above it.
#[tokio::test]
async fn an_election_on_the_old_set_is_recorded_not_adopted() {
    let (store, _) = fenced_move(true).await;
    let cut_over = pass(&store).await;
    let pending = cut_over.pending_set.clone().expect("pending");
    // b's joint start, held from before the election, takes nothing after.
    report(&store, &cut_over, false, Some(pending.generation)).await;
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            cut_over.generation,
            ElectedLeader {
                leader: "broker-a".to_string(),
                generation: cut_over.generation + AHEAD,
                replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
                set_generation: cut_over.generation - 1,
            },
        )
        .await
        .expect("record");
    let AssignmentWrite::Written(recorded) = written else {
        panic!("not recorded: {written:?}");
    };
    assert_eq!(
        recorded,
        ShardAssignment {
            generation: cut_over.generation + AHEAD,
            ..cut_over.clone()
        }
    );
    assert_eq!(pass(&store).await, recorded);
    report(&store, &recorded, false, Some(pending.generation)).await;
    let taken = pass(&store).await;
    assert_eq!(taken.pending_set, None);
    assert_eq!(taken.generation, recorded.generation + 1);
}
