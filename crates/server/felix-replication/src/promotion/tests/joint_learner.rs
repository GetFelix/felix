//! A joint fence leaving a set with a staged learner: a leader that counts
//! by attestation needs a majority of the set's voters and half of the set
//! with its learners, and does not ask a learner the route no longer names.
//! `LearnerBlocking` in `docs/formal/FelixShardUnified.tla`.

use super::joint::{addr_of, kept_before, replicas_with};
use super::unified::{TRUSTED, fence_unified, unified};
use super::*;

/// The leader's log keeps {broker-a, broker-b, broker-c} at 4 with
/// `learners` staged beside them; the route is {broker-a, broker-b,
/// broker-c}. broker-b answers, attesting a kept, trusted copy to a leader
/// that counts by attestation; broker-c refuses with
/// `refusal`, as a copy that cannot vote yet does. Each learner, named by
/// no route, refuses as not a replica if asked.
async fn fence_leaving_learners(
    learners: &[&str],
    refusal: &'static str,
    marks: Option<&crate::quorum::QuorumMarks>,
) -> (Outcome, Replicas) {
    let mut replicas = replicas_with(learners);
    replicas.get_mut("broker-b").attests = marks.map(|_| TRUSTED);
    replicas.get_mut("broker-c").refuses = Some(refusal);
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let members: Vec<&str> = [LEADER, "broker-b", "broker-c"]
        .into_iter()
        .chain(learners.iter().copied())
        .collect();
    kept_before(&leader, &members, learners).await;
    let addr_of = addr_of;
    let joint = Joint::new(&addr_of, None);
    let outcome = match marks {
        Some(marks) => fence_unified(&replicas, &leader, &route(), Some(&joint), marks).await,
        None => {
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
    };
    (outcome, replicas)
}

fn asked(replicas: &Replicas, node: &str) -> bool {
    replicas
        .sent(Kind::ElectionFence)
        .iter()
        .any(|to| to == node)
}

/// **A fence opens past the staged learner of the set it leaves.** The
/// control plane abandoned a move to broker-x, so the route leaves the set
/// that staged it, {broker-a, broker-b, broker-c, broker-x (learner)}.
/// broker-c's copy cannot vote until a set change readmits it, and
/// broker-x, a learner the route does not name, could only refuse. This
/// broker and broker-b are a majority of the voters and half of the set
/// with its learner, which meets every majority an acknowledgement on it
/// could have used, so the fence opens without asking broker-x. A leader
/// that does not count by attestation still needs a majority of all four.
#[tokio::test]
async fn a_fence_leaving_a_staged_learner_opens_on_half_the_set_with_it() {
    let refusal = "does not vote until a set change re-admits it";
    let marks = unified(true);
    let (outcome, replicas) = fence_leaving_learners(&["broker-x"], refusal, Some(&marks)).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert!(
        !asked(&replicas, "broker-x"),
        "asked a learner that must refuse"
    );

    let (outcome, replicas) = fence_leaving_learners(&["broker-x"], refusal, None).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a fence without attestations opened on 2 of 4: {outcome:?}");
    };
    assert!(why.contains("2 of 4 and 2 of 3 voters"), "{why}");
    assert!(asked(&replicas, "broker-x"));
}

/// **The same with the copy that led before a crash refusing.** A Stage
/// leaves {broker-a, broker-b, broker-c, broker-x (learner)}, broker-x left
/// from a move abandoned earlier, and broker-c refuses as the copy it led
/// before a crash. This broker and broker-b open the fence.
#[tokio::test]
async fn a_fence_leaving_a_learner_opens_past_a_led_before_refusal() {
    let refusal = "led the shard before a crash and no newer leader has shipped to it since";
    let marks = unified(true);
    let (outcome, _replicas) = fence_leaving_learners(&["broker-x"], refusal, Some(&marks)).await;
    let Outcome::Fenced { .. } = &outcome else {
        panic!("a fence with half of the set and its learner stayed closed: {outcome:?}");
    };
}

/// **With two learners, half of the voters is not enough.** The set being
/// left is {broker-a, broker-b, broker-c} with broker-x and broker-y staged.
/// A leader counting the learners as voters could have acknowledged on
/// broker-c, broker-x and broker-y, which this broker and broker-b do not
/// meet: 2 of 5 is short of half, so the fence waits, whoever counts.
#[tokio::test]
async fn a_fence_leaving_two_learners_still_needs_half_of_the_whole_set() {
    let refusal = "does not vote until a set change re-admits it";
    let marks = unified(true);
    let (outcome, _replicas) =
        fence_leaving_learners(&["broker-x", "broker-y"], refusal, Some(&marks)).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a fence opened on 2 of 5 of a set with two learners: {outcome:?}");
    };
    assert!(
        why.contains("2 of 5 (with its learners, half needed) and 2 of 3 voters"),
        "{why}"
    );
}
