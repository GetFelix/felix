//! A joint fence leaving a set that names a copy since replaced: a member
//! back on new storage answers from another copy, so the one the set names
//! promises nothing again. Half of the set from the other members is then
//! enough. `ReplacedBlocking` in `docs/formal/FelixShardUnified.tla`.

use super::joint::{addr_of, kept_naming, replicas_with};
use super::*;

/// The leader's log keeps {broker-b, broker-c} at 4, naming broker-c's
/// first copy; the route is {broker-a, broker-b, broker-c}. `setup` sets
/// up the replicas, then this broker fences.
async fn fence_leaving_two(setup: impl FnOnce(&mut Replicas)) -> Outcome {
    let mut replicas = replicas_with(&[]);
    setup(&mut replicas);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    kept_naming(
        &leader,
        &["broker-b", "broker-c"],
        &[],
        &[("broker-c", storage_of("broker-c"))],
    )
    .await;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);
    fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route(),
        false,
        Some(&joint),
    )
    .await
}

fn new_storage(replicas: &mut Replicas, node: &str) {
    replicas.get_mut(node).storage = Some(StorageId::from_bytes([0xee; 16]));
}

/// **A fence leaving a 2-voter set opens on the other half once a member is
/// back on new storage.** broker-c restarted under a new storage id and
/// refuses as a copy that is not a voter yet. Its old copy is gone, so no
/// fence can take a majority of {broker-b, broker-c} again, and until this
/// leader serves it reports nothing the control plane could re-admit
/// broker-c on. broker-b's promise meets every majority of the set.
#[tokio::test]
async fn a_fence_leaving_a_set_with_a_replaced_copy_opens_on_the_other_half() {
    let outcome = fence_leaving_two(|replicas| {
        new_storage(replicas, "broker-c");
        replicas.get_mut("broker-c").refuses =
            Some("this copy is not a voter until it catches up with a set naming it");
    })
    .await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **The replaced member's new copy is not counted toward the half.**
/// broker-c answers from its new copy and broker-b is down: nothing the set
/// named promised, so the fence waits.
#[tokio::test]
async fn a_replaced_copy_is_not_counted_toward_the_half() {
    let outcome = fence_leaving_two(|replicas| {
        new_storage(replicas, "broker-c");
        replicas.get_mut("broker-b").reachable = false;
    })
    .await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a fence opened on the replaced copy alone: {outcome:?}");
    };
    assert!(
        why.contains("0 of 2 voters of the set being left")
            && why.contains("answered from a replaced copy, so 0 promises"),
        "{why}"
    );
}

/// **Silence is not a replacement.** broker-c does not answer: its named
/// copy may still promise another fence, so one of two is not enough.
#[tokio::test]
async fn a_silent_member_is_not_taken_for_replaced() {
    let outcome = fence_leaving_two(|replicas| {
        replicas.get_mut("broker-c").reachable = false;
    })
    .await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a fence opened on one of two with the other silent: {outcome:?}");
    };
    assert!(why.contains("1 of 2 voters of the set being left"), "{why}");
}
