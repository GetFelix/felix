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

    // A cut only stops what reaches a broker, so a leader still heard
    // would keep its term: stop it instead.
    EngineShards::shutdown(&*members[first].shards).await;
    net.forget(members[first].name);
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
    for (at, member) in members.iter().enumerate() {
        if at != first {
            EngineShards::shutdown(&*member.shards).await;
        }
    }
}

/// `replicated()` with `leader` as placement's leader.
fn led_by(leader: &str) -> ShardAssignment {
    let mut assignment = replicated();
    assignment.leader = leader.to_string();
    assignment
}

/// **A successor does not act on a target it was told in an earlier term
/// (#2591).** `broker-a` leads first and is told v1, the three it formed
/// with. `broker-c` then leads and is told v2, which drops `broker-c` and
/// adds `broker-d`: it grows the group to all four at v1's version and hands
/// over to `broker-a`. `broker-a` still holds v1, whose version equals the
/// grown membership's, so acting on it would shrink the group back and drop
/// `broker-d`. Until a view fresh enough for its new term tells it v2, it
/// takes no step; then the move finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_does_not_act_on_a_target_told_in_an_earlier_term() {
    let (net, members, first) = trio().await;
    let orders = key("orders", 0);
    assert_eq!(members[first].name, "broker-a");
    let leases: Vec<Arc<LeaseState>> = members
        .iter()
        .map(|member| lease_of(&member.shards))
        .collect();
    let (a, c) = (&members[0], &members[2]);
    let three = a.shards.runtime.voters(&orders).expect("voters");
    let v1 = with_target(led_by("broker-a"), &three, 1);
    a.shards.take(&mut assignments([v1.clone()])).await;
    leases[0].record_head(tokio::time::Instant::now(), 10);
    a.shards.answered(now(), Some(10));
    let first_term = a.shards.leading(&orders).expect("led");
    assert_eq!(told(&a.shards), [(version(1), first_term)]);
    until("broker-a to stamp v1", || {
        a.shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.membership == MembershipProgress::Settled)
    })
    .await;

    // broker-d holds an empty member of the shard.
    let d = Member::start(&net, "broker-d").await;
    let mut four = replicated();
    four.replicas.push("broker-d".to_string());
    d.shards.take(&mut assignments([four])).await;

    for member in &members {
        member
            .shards
            .take(&mut assignments([led_by("broker-c")]))
            .await;
    }
    until("broker-c to lead", || c.shards.leading(&orders).is_some()).await;
    let c_term = c.shards.leading(&orders).expect("led");
    // Placement goes back to broker-a, so its next term is not handed on.
    for member in &members[..2] {
        member
            .shards
            .take(&mut assignments([led_by("broker-a")]))
            .await;
    }

    let v2_voters = c
        .shards
        .runtime
        .target_members(
            &orders,
            ["broker-a", "broker-b", "broker-d"],
            Duration::from_secs(5),
        )
        .await
        .expect("v2's members");
    let v2 = with_target(led_by("broker-c"), &v2_voters, 2);
    c.shards.take(&mut assignments([v2.clone()])).await;
    leases[2].record_head(tokio::time::Instant::now(), 20);
    c.shards.answered(now(), Some(20));
    assert_eq!(told(&c.shards), [(version(2), c_term)]);

    until("broker-c to hand over to broker-a", || {
        a.shards.leading(&orders).is_some_and(|term| term > c_term)
    })
    .await;
    let a_term = a.shards.leading(&orders).expect("led");
    let grown = a.shards.runtime.voters(&orders).expect("voters");
    assert!(
        grown.iter().any(|voter| *voter.broker == *"broker-d"),
        "broker-c handed over before growing: {grown:?}"
    );
    for _ in 0..50 {
        let voters = a.shards.runtime.voters(&orders).expect("voters");
        assert_eq!(voters, grown, "broker-a acted on v1 in a later term");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    a.shards.take(&mut assignments([v2.clone()])).await;
    leases[0].record_head(tokio::time::Instant::now(), 30);
    a.shards.answered(now(), Some(30));
    assert_eq!(
        told(&a.shards),
        [(version(1), first_term), (version(2), a_term)]
    );
    until("the group to settle on v2", || {
        a.shards.runtime.voters(&orders).as_ref() == Some(&v2_voters)
            && a.shards
                .runtime
                .status(&orders)
                .is_some_and(|status| status.membership == MembershipProgress::Settled)
    })
    .await;
    stop(&members).await;
    EngineShards::shutdown(&*d.shards).await;
}
