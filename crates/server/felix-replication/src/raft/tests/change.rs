//! The membership a group stores and the steps that change it: the target
//! version rides in the membership, and a step is written only on the
//! membership and the term it was decided on.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

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

/// `broker`'s member, once it leads and has committed an entry of its term.
async fn leading(cluster: &Cluster, broker: &str) -> Arc<Group> {
    eventually(|| {
        let status = cluster.runtime(broker).status(&shard(0))?;
        let ready = status.role == Role::Leader && status.committed == status.last_index;
        ready.then(|| cluster.runtime(broker).group(&shard(0)).expect("group"))
    })
    .await
}

/// Group 0 with `a` as its only voter and leader, and `b` and `c` as
/// learners. Making all three voters needs a joint membership: `b` and `c`
/// are a majority of the three that `a` is not in.
async fn one_voter() -> (Cluster, Arc<Group>) {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    cluster
        .runtime("a")
        .initialize(&shard(0), set(&["a"]))
        .await
        .expect("initialize");
    let leader = leading(&cluster, "a").await;
    for learner in ["b", "c"] {
        leader
            .add_learner(&basis(&leader).await, m(learner))
            .await
            .expect("learner");
    }
    let learning = Membership::new(
        vec![set(&["a"])],
        set(&["b", "c"]),
        TargetVersion::default(),
    );
    everywhere(&cluster, &learning).await;
    assert!(!learning.supports_direct_transition_to(&settled(&BROKERS, &[], 2)));
    (cluster, leader)
}

/// The joint membership on the way from `one_voter` to all three at
/// version 2.
fn joint() -> Membership {
    Membership::new(vec![set(&["a"]), set(&BROKERS)], BTreeSet::new(), v(2))
}

/// Ask `leader` for all three as voters at version 2, while no append it
/// sends is answered: the joint entry reaches every member and cannot
/// commit. Returns the change, still waiting, and the entry the joint is at.
async fn stalled_in_the_joint(
    cluster: &Cluster,
    leader: &Arc<Group>,
) -> (
    tokio::task::JoinHandle<Result<(), StepError>>,
    Option<openraft::type_config::alias::LogIdOf<ShardTypes>>,
) {
    cluster.net.lose_answers.store(true, Relaxed);
    let decided = basis(leader).await;
    let change = tokio::spawn({
        let leader = Arc::clone(leader);
        async move { leader.change_voters(&decided, set(&BROKERS), v(2)).await }
    });
    // The joint already carries the version of the set it leads to.
    everywhere(cluster, &joint()).await;
    let held = basis(leader).await;
    assert_eq!(held.membership, joint());
    assert!(!held.committed);
    (change, held.at)
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

    // The same voters are not written again by a leader they leave out.
    let mut gone = held.clone();
    gone.membership = Membership::new(vec![without.clone()], BTreeSet::new(), v(1));
    assert_eq!(
        leader.stamp(&gone, BTreeSet::new(), v(2)).await,
        Err(StepError::WithoutLeader)
    );

    // A joint whose new voters leave the leader out is not left by it.
    let mut joint = held.clone();
    joint.membership = Membership::new(vec![set(&["a", "b"]), without], BTreeSet::new(), v(2));
    assert_eq!(
        leader.leave_joint(&joint).await,
        Err(StepError::WithoutLeader)
    );

    // Nothing else is written on a joint: the edit would flatten it to the
    // new voters, and here drop the leader.
    let learner = leader.add_learner(&joint, m("d")).await;
    assert_eq!(learner, Err(StepError::Joint));
    let stamp = leader.stamp(&joint, BTreeSet::new(), v(3)).await;
    assert_eq!(stamp, Err(StepError::Joint));
    let change = leader
        .change_voters(&joint, set(&["a", "b", "c"]), v(3))
        .await;
    assert_eq!(change, Err(StepError::Joint));

    // And a membership that is not joint is not left: that would write it
    // again.
    assert_eq!(leader.leave_joint(&held).await, Err(StepError::NotJoint));

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

/// One voter to three is not a direct change, so openraft writes a joint
/// entry and then the new set, both at the version asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_through_a_joint_keeps_its_version_in_both_entries() {
    let (mut cluster, leader) = one_voter().await;
    let before = basis(&leader).await.at.expect("an entry").index;
    let (change, at) = stalled_in_the_joint(&cluster, &leader).await;
    assert_eq!(at.expect("an entry").index, before + 1);

    // Answers are heard again: the joint commits, and openraft leaves it.
    cluster.net.lose_answers.store(false, Relaxed);
    change.await.expect("joined").expect("change");
    let want = settled(&BROKERS, &[], 2);
    everywhere(&cluster, &want).await;
    let after = basis(&leader).await;
    assert_eq!(after.membership, want);
    assert_eq!(after.at.expect("an entry").index, before + 2);
    assert!(after.committed);
    cluster.stop().await;
}

/// The leader that wrote the joint is replaced before it commits. The new
/// leader reads the joint, and its version, from its own log and leaves it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_leaves_the_joint_it_finds() {
    let (mut cluster, old) = one_voter().await;
    let (change, at) = stalled_in_the_joint(&cluster, &old).await;

    let term = old.transfer(m("b")).await.expect("a leader");
    eventually(|| {
        let status = cluster.runtime("b").status(&shard(0))?;
        (status.role == Role::Leader).then_some(())
    })
    .await;
    cluster.net.lose_answers.store(false, Relaxed);
    let new = leading(&cluster, "b").await;
    // The old leader does not finish the change it began.
    assert_eq!(change.await.expect("joined"), Err(StepError::NotLeader));
    assert!(old.membership_basis().await.is_none());

    let found = basis(&new).await;
    assert!(found.committed && found.term > term);
    assert_eq!((found.at, &found.membership), (at, &joint()));

    new.leave_joint(&found).await.expect("leave");
    let want = settled(&BROKERS, &[], 2);
    everywhere(&cluster, &want).await;
    let after = basis(&new).await;
    assert_eq!(after.membership, want);
    assert_eq!(
        after.at.expect("an entry").index,
        at.expect("an entry").index + 2
    );
    cluster.stop().await;
}

/// A step at a version below the membership's would let an older target
/// undo a newer one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_step_takes_the_version_back() {
    let (mut cluster, leader) = pair().await;
    leader
        .add_learner(&basis(&leader).await, m("c"))
        .await
        .expect("learner");
    leader
        .stamp(&basis(&leader).await, set(&["c"]), v(5))
        .await
        .expect("stamp");
    let held = basis(&leader).await;
    assert_eq!(held.membership, settled(&["a", "b"], &["c"], 5));

    let change = leader.change_voters(&held, set(&BROKERS), v(4)).await;
    assert_eq!(change, Err(StepError::OlderVersion));
    let stamp = leader.stamp(&held, BTreeSet::new(), v(4)).await;
    assert_eq!(stamp, Err(StepError::OlderVersion));
    let after = basis(&leader).await;
    assert_eq!((after.at, &after.membership), (held.at, &held.membership));

    // The version it has is not older: a leader grows the voters at it.
    leader
        .change_voters(&held, set(&BROKERS), v(5))
        .await
        .expect("change");
    assert_eq!(basis(&leader).await.membership, settled(&BROKERS, &[], 5));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_does_not_lead_has_no_basis_and_writes_no_step() {
    let (mut cluster, leader) = pair().await;
    let other = if leader.member == m("a") { "b" } else { "a" };
    let follower = cluster.runtime(other).group(&shard(0)).expect("group");
    assert!(follower.membership_basis().await.is_none());

    // On what the leader holds, so only who asks is wrong.
    let held = basis(&leader).await;
    assert_eq!(
        follower.add_learner(&held, m("c")).await,
        Err(StepError::NotLeader)
    );
    assert_eq!(
        follower.stamp(&held, BTreeSet::new(), v(2)).await,
        Err(StepError::NotLeader)
    );
    let after = basis(&leader).await;
    assert_eq!((after.at, &after.membership), (held.at, &held.membership));
    cluster.stop().await;
}
