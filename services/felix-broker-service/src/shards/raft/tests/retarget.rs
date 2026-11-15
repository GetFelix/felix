//! The broker tells a leading member its target once per term, and only
//! from a feed view fresh enough for that term.

use super::*;
use crate::shards::watch::{RaftTarget, RaftTargetVersion};

fn version(counter: u64) -> TargetVersion {
    TargetVersion {
        incarnation: 1,
        counter,
    }
}

/// `assignment` carrying a target of `voters` at `version(counter)`.
fn with_target(
    mut assignment: ShardAssignment,
    voters: &BTreeSet<MemberId>,
    counter: u64,
) -> ShardAssignment {
    assignment.target = Some(RaftTarget {
        version: RaftTargetVersion {
            incarnation: 1,
            counter,
        },
        members: voters
            .iter()
            .map(|voter| (voter.broker.to_string(), voter.copy))
            .collect(),
    });
    assignment
}

fn lease_of(shards: &RaftShards) -> Arc<LeaseState> {
    let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(10)));
    shards.follow_lease(Arc::clone(&lease));
    lease
}

fn told(shards: &RaftShards) -> Vec<(TargetVersion, u64)> {
    shards
        .told
        .lock()
        .iter()
        .map(|(_, version, term)| (*version, *term))
        .collect()
}

fn now() -> Option<tokio::time::Instant> {
    Some(tokio::time::Instant::now())
}

/// **A new leader tells a target only from a view at least as fresh as its
/// term.** A heartbeat sent before the term began gives no bar, and a view
/// fetched after it began is not enough while its checkpoint is below the
/// bar: a control-plane member that lags can answer late and still miss a
/// target written before the term.
#[tokio::test]
async fn a_target_waits_for_a_view_that_reaches_the_terms_bar() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let lease = lease_of(&shards);
    let orders = key("orders", 0);
    // Sent before the term began.
    lease.record_head(tokio::time::Instant::now() - Duration::from_millis(1), 1);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the member to lead", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("led");
    let voters = shards.runtime.voters(&orders).expect("voters");
    let fed = with_target(assigned(orders.clone(), INCARNATION), &voters, 1);
    shards.take(&mut assignments([fed.clone()])).await;

    shards.answered(now(), Some(100));
    assert_eq!(told(&shards), [], "a beat sent before the term gave a bar");
    assert!(
        lease.wants_head(),
        "the heartbeat does not ask for the head"
    );

    lease.record_head(tokio::time::Instant::now(), 5);
    let before = tokio::time::Instant::now();
    shards.answered(now(), Some(4));
    assert_eq!(told(&shards), [], "a view below the bar told a target");
    assert!(!lease.wants_head());

    shards.answered(Some(before - Duration::from_secs(1)), Some(9));
    assert_eq!(
        told(&shards),
        [],
        "a view from before the term told a target"
    );

    shards.answered(now(), Some(5));
    assert_eq!(told(&shards), [(version(1), term)]);
    shards.runtime.shutdown().await;
}

/// **A new target version is told in the same term.**
#[tokio::test]
async fn a_newer_target_is_told_in_the_same_term() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let lease = lease_of(&shards);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the member to lead", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("led");
    let voters = shards.runtime.voters(&orders).expect("voters");
    lease.record_head(tokio::time::Instant::now(), 1);

    for counter in [1, 2] {
        let fed = with_target(assigned(orders.clone(), INCARNATION), &voters, counter);
        shards.take(&mut assignments([fed])).await;
        shards.answered(now(), Some(1));
        // Told again on every pass, and kept only once.
        shards.answered(now(), Some(1));
    }
    assert_eq!(told(&shards), [(version(1), term), (version(2), term)]);
    until("the group to settle on its target", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.membership == MembershipProgress::Settled)
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A new leader tells the same version again for its own term, once its
/// feed has caught up.** The member that led before was told it for its
/// term only. Until the new leader's view reaches its bar it tells nothing,
/// and its group takes no step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_tells_its_target_again_once_its_feed_catches_up() {
    let (net, members, first) = trio().await;
    let orders = key("orders", 0);
    let leases: Vec<Arc<LeaseState>> = members
        .iter()
        .map(|member| lease_of(&member.shards))
        .collect();
    let voters = members[first]
        .shards
        .runtime
        .voters(&orders)
        .expect("voters");
    let fed = with_target(replicated(), &voters, 1);
    for (member, lease) in members.iter().zip(&leases) {
        member.shards.take(&mut assignments([fed.clone()])).await;
        lease.record_head(tokio::time::Instant::now(), 10);
    }
    let first_term = members[first].shards.leading(&orders).expect("led");
    members[first].shards.answered(now(), Some(10));
    assert_eq!(told(&members[first].shards), [(version(1), first_term)]);

    net.cut.lock().insert(members[first].name.to_string());
    let mut next = None;
    until("another member to serve a term", || {
        next = members
            .iter()
            .enumerate()
            .position(|(at, member)| at != first && member.shards.leading(&orders).is_some());
        next.is_some()
    })
    .await;
    let next = next.expect("a new leader");
    let shards = &members[next].shards;
    let term = shards.leading(&orders).expect("led");
    assert!(term > first_term);

    // A head from after the term began, and a view still below it.
    leases[next].record_head(tokio::time::Instant::now(), 20);
    shards.take(&mut assignments([fed.clone()])).await;
    shards.answered(now(), Some(19));
    assert_eq!(told(shards), []);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let progress = shards.runtime.status(&orders).expect("status").membership;
    assert_eq!(
        progress,
        MembershipProgress::Idle,
        "the new leader acted on no target"
    );

    shards.answered(now(), Some(20));
    assert_eq!(told(shards), [(version(1), term)]);
    until("the new leader to settle on the target", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.membership == MembershipProgress::Settled)
    })
    .await;
    net.cut.lock().clear();
    stop(&members).await;
}
