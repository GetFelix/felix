//! Placement after the control plane adopted an elected leader: every
//! generation it issues is above the elected one (rule 10 of "Electing
//! without the control plane" in docs/replication-design.md). End to end
//! against the memory store, from the cut-over in `pending_set`.
use super::pending_set::{election_store, fenced_move, pass, report};
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

/// The control plane named broker-a at a generation broker-b was elected at
/// on {b, c, d}, and its own set is {a, b, c}. Answers the store and what
/// moving past the collision wrote.
async fn reissued_past_a_collision() -> (InMemoryStore, ShardAssignment) {
    let store = election_store(
        &["broker-a", "broker-b", "broker-c", "broker-d", "broker-e"],
        true,
    )
    .await;
    let named_a = store
        .put_shard_assignment(ShardAssignment {
            state: ShardState::Assigning,
            ..assigned("orders", "broker-a", &["broker-b", "broker-c"])
        })
        .await
        .expect("assign");
    let next = named_a
        .reissuing(
            &ElectedLeader {
                leader: "broker-b".to_string(),
                generation: named_a.generation,
                replicas: vec!["broker-c".to_string(), "broker-d".to_string()],
                set_generation: named_a.generation,
            },
            &|_| true,
        )
        .expect("reissued");
    let token = store.placement_token().await.expect("token");
    let written = store
        .put_shard_assignment_if(next, Some(named_a.generation), token)
        .await
        .expect("write");
    let AssignmentWrite::Written(reissued) = written else {
        panic!("not written: {written:?}");
    };
    assert_eq!(reissued.leader, "broker-b");
    assert_eq!(
        reissued.pending_set.as_ref().map(|p| p.generation),
        Some(reissued.generation)
    );
    (store, reissued)
}

/// **A failover while a reissue's change is pending keeps the set the
/// leader was elected on.** broker-b acknowledged on {b, c, d} before the
/// collision was found, then shipped only to {a, c}, the pending set, and
/// dies before its new generation's start reaches a majority of both. Its
/// report names a and c caught up. The failover promotes c from the elected
/// set and keeps d, which may hold what b acknowledged; a is not in it.
/// Nobody stands meanwhile: the route carries the pending change.
#[tokio::test]
async fn a_failover_during_a_reissue_keeps_the_elected_set() {
    let (store, reissued) = reissued_past_a_collision().await;
    assert_eq!(
        pass(&store).await,
        reissued,
        "taken without the leader's word"
    );
    // What the leader measured: the pending set it ships to.
    let shipped = ShardAssignment {
        replicas: reissued.pending_set.clone().expect("pending").replicas,
        ..reissued.clone()
    };
    report(&store, &shipped, false, None).await;
    store
        .set_node_lifecycle("broker-b", NodeLifecycle::Down)
        .await
        .expect("down");
    let promoted = pass(&store).await;
    assert_eq!(promoted.leader, "broker-c", "{promoted:?}");
    assert!(
        promoted.nodes().any(|node| node == "broker-d"),
        "{promoted:?}"
    );
    assert!(
        !promoted.nodes().any(|node| node == "broker-a"),
        "{promoted:?}"
    );
    assert_eq!(promoted.pending_set, None);
}

/// **The control plane's set comes back on the reissued leader's joint
/// start**, as for a cut-over, at the next generation.
#[tokio::test]
async fn a_reissue_takes_the_recorded_set_on_the_leaders_joint_start() {
    let (store, reissued) = reissued_past_a_collision().await;
    report(&store, &reissued, false, Some(reissued.generation)).await;
    let taken = pass(&store).await;
    assert_eq!(taken.leader, "broker-b");
    assert_eq!(taken.pending_set, None);
    assert_eq!(
        taken.nodes().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["broker-a", "broker-b", "broker-c"])
    );
    assert_eq!(taken.generation, reissued.generation + 1);
}

/// **A leader that stands again after a reissue it could not take is
/// recorded on the set it opened on, and the change it skipped is dropped.**
/// The reissue came from a stale report: broker-b had already promised that
/// generation to another candidate, so it stood one higher on {b, c, d} and
/// opened there. The control plane names b on {b, c, d}, and drops the
/// pending change back to {a, b, c}: only b could have taken it, and b
/// opened past it on the set it leaves.
#[tokio::test]
async fn an_election_past_a_reissue_keeps_the_elected_set_recorded() {
    let (store, reissued) = reissued_past_a_collision().await;
    let written = store
        .adopt_elected_leader(
            &shard_zero(),
            reissued.generation,
            ElectedLeader {
                leader: "broker-b".to_string(),
                generation: reissued.generation + 1,
                replicas: vec!["broker-c".to_string(), "broker-d".to_string()],
                set_generation: reissued.generation - 1,
            },
        )
        .await
        .expect("adopt");
    let AssignmentWrite::Written(recorded) = written else {
        panic!("not recorded: {written:?}");
    };
    assert_eq!(recorded.leader, "broker-b");
    assert_eq!(
        recorded
            .nodes()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["broker-b", "broker-c", "broker-d"]),
        "the control plane records b on a set it did not open on"
    );
    assert_eq!(
        recorded.pending_set, None,
        "a change only b could take, and b opened past it on the set it leaves"
    );
    assert_eq!(recorded.generation, reissued.generation + 1);
}
