//! Placement's fencing token and lease, which every backend must honour.
//!
//! Each case takes two handles, `a` and `b`, standing for two control-plane
//! instances. For Postgres they are separate connection pools over one
//! database; in memory and under Raft they share one store.
use std::sync::Arc;
use std::time::Duration;

use super::shards::{assignment, clear, key, seed, token};
use crate::cluster::placement::{MovePolicy, PlacementRead, start_move};
use crate::model::ShardState;
use crate::store::{AssignmentWrite, ControlPlaneStore};

/// Fencing and the move limits across two writers. Every backend.
pub(crate) async fn run_placement_contract(
    a: Arc<dyn ControlPlaneStore>,
    b: Arc<dyn ControlPlaneStore>,
) {
    seed(a.as_ref()).await;
    a_placement_write_is_fenced_by_any_placement_write_since_its_read(a.as_ref(), b.as_ref()).await;
    two_instances_cannot_both_take_the_last_move_slot(a.as_ref(), b.as_ref()).await;
}

/// A lease that expires by the store's clock. Every backend.
pub(crate) async fn run_expiring_lease_contract(
    a: Arc<dyn ControlPlaneStore>,
    b: Arc<dyn ControlPlaneStore>,
) {
    seed(a.as_ref()).await;
    the_placement_lease_has_one_holder_until_released(a.as_ref(), b.as_ref()).await;
    an_expired_lease_is_taken_over_and_fences_the_old_holder(a.as_ref(), b.as_ref()).await;
}

async fn a_placement_write_is_fenced_by_any_placement_write_since_its_read(
    a: &dyn ControlPlaneStore,
    b: &dyn ControlPlaneStore,
) {
    clear(a).await;
    let read = token(a).await;
    assert_eq!(
        token(b).await,
        read,
        "one token, whichever instance reads it"
    );

    let first = a
        .put_shard_assignment_if(assignment(0, "broker-x"), None, read)
        .await
        .expect("first");
    assert!(matches!(first, AssignmentWrite::Written(_)), "{first:?}");
    assert_eq!(
        token(b).await,
        read + 1,
        "a placement write advances the token"
    );

    // Another shard entirely: its generation is fine, but it was decided
    // before the write above landed.
    assert_eq!(
        b.put_shard_assignment_if(assignment(1, "broker-y"), None, read)
            .await
            .expect("second"),
        AssignmentWrite::Fenced { token: read + 1 },
    );
    assert!(
        b.get_shard_assignment(&key(1)).await.is_err(),
        "a fenced write writes nothing"
    );

    // A stale write writes nothing, so it leaves the token alone and the
    // writer's next write still lands.
    assert!(matches!(
        b.put_shard_assignment_if(assignment(0, "broker-y"), None, read + 1)
            .await
            .expect("stale"),
        AssignmentWrite::Stale { current: Some(0) },
    ));
    assert_eq!(token(a).await, read + 1);
    let chained = b
        .put_shard_assignment_if(assignment(1, "broker-y"), None, read + 1)
        .await
        .expect("chained");
    assert!(
        matches!(chained, AssignmentWrite::Written(_)),
        "{chained:?}"
    );
    assert_eq!(token(a).await, read + 2);
}

/// Two instances each decide a move from a read with one free slot, on
/// different shards, so the generation check has nothing to catch. Only
/// the first write lands; the second was decided without it.
async fn two_instances_cannot_both_take_the_last_move_slot(
    a: &dyn ControlPlaneStore,
    b: &dyn ControlPlaneStore,
) {
    clear(a).await;
    for shard in [0, 1] {
        let placed = a
            .put_shard_assignment_if(assignment(shard, "broker-x"), None, token(a).await)
            .await
            .expect("place");
        assert!(matches!(placed, AssignmentWrite::Written(_)), "{placed:?}");
    }
    let liveness = Default::default();
    let policy = MovePolicy::default();
    assert_eq!(policy.max_concurrent, 1, "the case needs a single slot");

    let (fence_a, read_a) = PlacementRead::load_fenced(a, &liveness)
        .await
        .expect("read a");
    let (fence_b, read_b) = PlacementRead::load_fenced(b, &liveness)
        .await
        .expect("read b");
    let move_a =
        start_move(&read_a.catalog(policy.clone()), &key(0), "broker-y").expect("a decides");
    let move_b =
        start_move(&read_b.catalog(policy.clone()), &key(1), "broker-y").expect("b decides");

    let written_a = a
        .put_shard_assignment_if(move_a.assignment, Some(move_a.expected_generation), fence_a)
        .await
        .expect("write a");
    assert!(
        matches!(written_a, AssignmentWrite::Written(_)),
        "{written_a:?}"
    );
    let written_b = b
        .put_shard_assignment_if(move_b.assignment, Some(move_b.expected_generation), fence_b)
        .await
        .expect("write b");
    assert!(
        matches!(written_b, AssignmentWrite::Fenced { .. }),
        "the second move landed beside the first: {written_b:?}"
    );

    let moving = a
        .list_shard_assignments()
        .await
        .expect("list")
        .into_iter()
        .filter(|assignment| {
            assignment.successor.is_some()
                || assignment.joining.is_some()
                || assignment.state == ShardState::Draining
        })
        .count();
    assert_eq!(moving, policy.max_concurrent, "moves in flight");
}

async fn the_placement_lease_has_one_holder_until_released(
    a: &dyn ControlPlaneStore,
    b: &dyn ControlPlaneStore,
) {
    const LONG: u64 = 60_000;
    let before = token(a).await;
    let first = a
        .acquire_placement_lease("instance-a", LONG)
        .await
        .expect("a acquires")
        .expect("a free lease is granted");
    assert!(first.taken);
    assert!(first.token > before, "a new holder advances the token");
    assert_eq!(
        b.acquire_placement_lease("instance-b", LONG)
            .await
            .expect("b tries"),
        None,
        "a held lease is not granted to another instance"
    );
    let renewed = a
        .acquire_placement_lease("instance-a", LONG)
        .await
        .expect("a renews")
        .expect("the holder renews");
    assert!(!renewed.taken);
    assert_eq!(
        renewed.token, first.token,
        "a renewal leaves the token alone"
    );

    b.release_placement_lease("instance-b")
        .await
        .expect("b releases nothing");
    assert_eq!(
        b.acquire_placement_lease("instance-b", LONG)
            .await
            .expect("b tries again"),
        None,
        "only the holder can release it"
    );
    a.release_placement_lease("instance-a")
        .await
        .expect("a releases");
    let taken = b
        .acquire_placement_lease("instance-b", LONG)
        .await
        .expect("b acquires")
        .expect("a released lease is granted at once");
    assert!(taken.taken);
    assert_eq!(taken.token, first.token + 1);
    b.release_placement_lease("instance-b")
        .await
        .expect("b releases");
}

/// An instance that paused past its lease must not write once another has
/// taken over, even a step it planned while it still held the lease.
async fn an_expired_lease_is_taken_over_and_fences_the_old_holder(
    a: &dyn ControlPlaneStore,
    b: &dyn ControlPlaneStore,
) {
    clear(a).await;
    let held = a
        .acquire_placement_lease("instance-a", 100)
        .await
        .expect("a acquires")
        .expect("granted");
    assert_eq!(
        b.acquire_placement_lease("instance-b", 60_000)
            .await
            .expect("b tries"),
        None
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    let taken = b
        .acquire_placement_lease("instance-b", 60_000)
        .await
        .expect("b acquires")
        .expect("an expired lease is granted");
    assert!(taken.taken);
    assert!(taken.token > held.token);

    assert_eq!(
        a.put_shard_assignment_if(assignment(0, "broker-x"), None, held.token)
            .await
            .expect("late write"),
        AssignmentWrite::Fenced { token: taken.token },
        "the old holder's write landed after the takeover"
    );
    assert_eq!(
        a.acquire_placement_lease("instance-a", 100)
            .await
            .expect("a tries"),
        None,
        "the old holder does not get it back while it is held"
    );
    b.release_placement_lease("instance-b")
        .await
        .expect("b releases");
}
