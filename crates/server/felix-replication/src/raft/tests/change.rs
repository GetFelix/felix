//! The membership a group stores and the steps that change it: the target
//! version rides in the membership, and a step is written only on the
//! membership and the term it was decided on.

use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::membership::{RaftMembership, RaftMembershipEdit};

use super::*;
use crate::raft::group::Group;
use crate::raft::group::membership::{Basis, StepError};
use crate::raft::types::{Membership, MembershipEdit};

fn m(broker: &str) -> MemberId {
    MemberId::new(broker, 1)
}

fn set(brokers: &[&str]) -> BTreeSet<MemberId> {
    brokers.iter().map(|broker| m(broker)).collect()
}

fn v(counter: u64) -> TargetVersion {
    TargetVersion {
        incarnation: 1,
        counter,
    }
}

fn settled(voters: &[&str], learners: &[&str], counter: u64) -> Membership {
    Membership::new(vec![set(voters)], set(learners), v(counter))
}

#[test]
fn a_voter_that_leaves_is_never_kept_as_a_learner() {
    let before = settled(&["a", "b", "c"], &["d"], 1);
    let edit = MembershipEdit::voters(set(&["a", "b", "d"]), v(2));
    // Asked to keep it, as a caller of `change_membership` can.
    let after = edit.apply(&before, true).expect("apply");
    assert_eq!(after, settled(&["a", "b", "d"], &[], 2));
    assert!(RaftMembership::get_node(&after, &m("c")).is_none());
}

#[test]
fn a_joint_carries_the_version_of_its_goal() {
    let before = settled(&["a", "b", "c"], &["d", "e"], 1);
    let goal = MembershipEdit::voters(set(&["a", "d", "e"]), v(2))
        .apply(&before, false)
        .expect("apply");
    let joint = before.joint_with(&goal);
    assert_eq!(
        joint.get_joint_config(),
        &vec![set(&["a", "b", "c"]), set(&["a", "d", "e"])]
    );
    assert_eq!(joint.version(), v(2));
    // Leaving the joint keeps it.
    let left = MembershipEdit::flatten(set(&["a", "d", "e"]))
        .apply(&joint, false)
        .expect("apply");
    assert_eq!(left, goal);
}

#[test]
fn a_learner_joins_at_the_version_the_membership_has() {
    let before = settled(&["a", "b"], &[], 4);
    let after = MembershipEdit::learner(m("c"))
        .apply(&before, false)
        .expect("apply");
    assert_eq!(after, settled(&["a", "b"], &["c"], 4));
}

/// Whether a change needs a joint is decided by the voter sets alone.
#[test]
fn the_version_does_not_make_a_change_direct() {
    let before = settled(&["a", "b", "c"], &["d", "e"], 1);
    // {b, c} and {d, e} are majorities that do not meet.
    assert!(!before.supports_direct_transition_to(&settled(&["a", "d", "e"], &[], 2)));
    assert!(!before.supports_direct_transition_to(&settled(&["a", "d", "e"], &[], 1)));
    assert!(before.supports_direct_transition_to(&settled(&["a", "b", "c"], &[], 2)));
    assert!(before.supports_direct_transition_to(&settled(&["a", "b"], &[], 2)));
}

/// Group 0 with voters `a` and `b`, and `c` open with nothing in its log.
async fn pair() -> (Cluster, Arc<Group>) {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    for broker in ["a", "b"] {
        cluster
            .runtime(broker)
            .initialize(&shard(0), set(&["a", "b"]))
            .await
            .expect("initialize");
    }
    let leader = leader_of(&cluster).await;
    (cluster, leader)
}

/// The member of `a` and `b` that leads, once it has committed an entry
/// of its term.
async fn leader_of(cluster: &Cluster) -> Arc<Group> {
    eventually(|| {
        ["a", "b"].into_iter().find_map(|broker| {
            let status = cluster.runtime(broker).status(&shard(0))?;
            let ready = status.role == Role::Leader && status.committed == status.last_index;
            ready.then(|| cluster.runtime(broker).group(&shard(0)).expect("group"))
        })
    })
    .await
}

async fn basis(leader: &Group) -> Basis {
    leader.membership_basis().await.expect("a leader")
}

/// The latest membership in `broker`'s log.
async fn latest(cluster: &Cluster, broker: &str) -> Membership {
    let member = cluster.runtime(broker).group(&shard(0)).expect("group");
    member
        .raft()
        .with_raft_state(|state| state.membership_state.effective().membership().clone())
        .await
        .expect("state")
}

async fn everywhere(cluster: &Cluster, want: &Membership) {
    for broker in BROKERS {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let held = latest(cluster, broker).await;
            if held == *want {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{broker} holds {held}, not {want}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_reaches_every_member_with_its_version() {
    let (mut cluster, leader) = pair().await;
    assert_eq!(
        basis(&leader).await.membership,
        Membership::from(set(&["a", "b"]))
    );

    leader
        .add_learner(&basis(&leader).await, m("c"))
        .await
        .expect("learner");
    let learning = Membership::new(
        vec![set(&["a", "b"])],
        set(&["c"]),
        TargetVersion::default(),
    );
    everywhere(&cluster, &learning).await;

    leader
        .change_voters(&basis(&leader).await, set(&["a", "b", "c"]), v(3))
        .await
        .expect("change");
    everywhere(&cluster, &settled(&["a", "b", "c"], &[], 3)).await;

    // The voter that leaves is gone from the membership, not a learner.
    let kept = [&*leader.member.broker, "c"];
    leader
        .change_voters(&basis(&leader).await, set(&kept), v(4))
        .await
        .expect("change");
    let now = basis(&leader).await;
    assert_eq!(now.membership, settled(&kept, &[], 4));
    assert!(now.committed);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_decided_on_an_older_membership_writes_nothing() {
    let (mut cluster, leader) = pair().await;
    let old = basis(&leader).await;
    leader.add_learner(&old, m("c")).await.expect("learner");
    let held = basis(&leader).await;
    assert_ne!(held.at, old.at);

    let voters = set(&["a", "b", "c"]);
    let moved = leader.change_voters(&old, voters.clone(), v(2)).await;
    assert_eq!(moved, Err(StepError::Moved));
    assert_eq!(
        leader.stamp(&old, BTreeSet::new(), v(2)).await,
        Err(StepError::Moved)
    );

    // Decided under another term's leader.
    let mut earlier = held.clone();
    earlier.term -= 1;
    let moved = leader.change_voters(&earlier, voters, v(2)).await;
    assert_eq!(moved, Err(StepError::Moved));
    assert_eq!(
        leader.stamp(&earlier, BTreeSet::new(), v(2)).await,
        Err(StepError::Moved)
    );

    let after = basis(&leader).await;
    assert_eq!((after.at, &after.membership), (held.at, &held.membership));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_step_writes_a_membership_without_its_leader() {
    let (mut cluster, leader) = pair().await;
    leader
        .add_learner(&basis(&leader).await, m("c"))
        .await
        .expect("learner");
    let held = basis(&leader).await;
    let other = if leader.member == m("a") { "b" } else { "a" };
    let without = set(&[other, "c"]);

    let refused = leader.change_voters(&held, without.clone(), v(2)).await;
    assert_eq!(refused, Err(StepError::WithoutLeader));

    // A joint whose new voters leave the leader out is not left by it.
    let mut joint = held.clone();
    joint.membership = Membership::new(vec![set(&["a", "b"]), without], BTreeSet::new(), v(2));
    assert_eq!(
        leader.leave_joint(&joint).await,
        Err(StepError::WithoutLeader)
    );

    let after = basis(&leader).await;
    assert_eq!((after.at, &after.membership), (held.at, &held.membership));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stamp_keeps_the_voters_and_cuts_the_learners() {
    let (mut cluster, leader) = pair().await;
    leader
        .add_learner(&basis(&leader).await, m("c"))
        .await
        .expect("learner");
    leader
        .stamp(&basis(&leader).await, BTreeSet::new(), v(5))
        .await
        .expect("stamp");
    let stamped = settled(&["a", "b"], &[], 5);
    assert_eq!(basis(&leader).await.membership, stamped);
    for broker in ["a", "b"] {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        while latest(&cluster, broker).await != stamped {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{broker} has no stamp"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_version_survives_a_snapshot_and_a_restart() {
    let (mut cluster, leader) = pair().await;
    leader
        .add_learner(&basis(&leader).await, m("c"))
        .await
        .expect("learner");
    leader
        .change_voters(&basis(&leader).await, set(&["a", "b", "c"]), v(3))
        .await
        .expect("change");
    let want = settled(&["a", "b", "c"], &[], 3);
    everywhere(&cluster, &want).await;
    let last = basis(&leader).await.at.expect("an entry").index;
    drop(leader);
    cluster.committed_everywhere(0, last).await;
    for broker in BROKERS {
        cluster.snapshot(broker, 0, last).await;
    }

    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;
    everywhere(&cluster, &want).await;
    cluster.stop().await;
}
