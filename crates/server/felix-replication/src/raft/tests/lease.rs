//! The leader lease: when a `Leader` write is acknowledged on the leader's
//! log alone, what gives the lease up, and the vote a restarted member
//! does not take. One test per rule of `docs/formal/FelixRaftLease.tla`.

use std::sync::atomic::Ordering::Relaxed;

use openraft::raft::AppendEntriesRequest;

use super::super::lease::{self, Lease, View, Wake};
use super::super::network::Reply;
use super::commit::{Led, led, lost};
use super::*;
use crate::engine::CommitError;

const SOON: Duration = Duration::from_secs(10);

fn vote_of(term: u64, broker: &str, committed: bool) -> VoteOf<ShardTypes> {
    let leader = LeaderIdOf::<ShardTypes>::new(term, MemberId::new(broker, 1));
    <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leader, committed)
}

fn to(broker: &str, body: Request) -> RaftRequest {
    RaftRequest {
        group: group(0),
        to: MemberId::new(broker, 1),
        beat: 0,
        deadline: Duration::from_secs(5),
        body,
    }
}

fn vote_request(term: u64, from: &str) -> Request {
    Request::Vote(VoteRequest::new(vote_of(term, from, false), None))
}

fn pre_vote_request(term: u64, from: &str) -> Request {
    Request::PreVote(VoteRequest::new(vote_of(term, from, false), None))
}

fn granted(answer: Result<RaftReply, Refused>) -> bool {
    match answer {
        Ok(RaftReply(Reply::Vote(answer))) => answer.vote_granted,
        Ok(_) => panic!("not a vote's answer"),
        Err(refused) => panic!("the request did not reach the member: {refused}"),
    }
}

/// Poll `check` for a short while: a rule that is missing fails the test
/// in seconds.
async fn soon(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + SOON;
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

impl Led {
    fn member(&self) -> Arc<super::super::group::Group> {
        let runtime = self.cluster.runtime(&self.leader.broker);
        runtime.group(&shard(0)).expect("group")
    }

    fn admit(&self) -> Result<(), CommitError> {
        self.engine.admit_local_ack(&shard(0), self.term)
    }

    /// The leader with a majority's answer inside its lease.
    async fn leased() -> Self {
        let led = led().await;
        soon("the lease never held", || led.admit().is_ok()).await;
        led
    }
}

/// Stop `broker`'s member of group 0 and start it again on its log.
async fn restart(cluster: &mut Cluster, broker: &str) {
    let runtime = cluster.runtime(broker);
    assert!(runtime.close(&shard(0)).await);
    let log = cluster.logs.remove(&(broker.to_string(), 0)).expect("log");
    log.close().await.expect("close");
    let log = open_log(cluster.root.path(), broker, 0, log_config());
    cluster.logs.insert((broker.into(), 0), log.clone());
    runtime.open(group(0), 1, log).await.expect("reopen");
}

/// **A `Leader` write is acknowledged on the leader's log while a majority
/// answered within the lease, and waits for its commit once it has not.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_is_acknowledged_locally_inside_the_lease_and_on_commit_outside_it() {
    let mut led = Led::leased().await;
    let other_term = led.engine.admit_local_ack(&shard(0), led.term + 1);
    assert_eq!(lost(&other_term), lease::NOT_LEADING);
    let follower = led.followers().next().expect("a follower");
    let follower = RaftEngine::new(led.cluster.runtime(&follower));
    let not_leader = follower.admit_local_ack(&shard(0), led.term);
    assert_eq!(lost(&not_leader), lease::NOT_LEADING);

    // Nobody hears the leader, so nothing commits. The lease still holds.
    led.cluster.net.cut.lock().extend(BROKERS.map(String::from));
    let written = led.propose(1).await;
    let acked = led.engine.await_ack(&shard(0), written, PATIENCE).await;
    assert_eq!(acked, Ok(()));
    assert_eq!(led.status().committed, Some(led.committed));

    tokio::time::sleep(timers().lease + Duration::from_millis(100)).await;
    assert_eq!(lost(&led.admit()), lease::LAPSED);
    let waiting = led.engine.await_ack(&shard(0), written, PATIENCE);
    let waiting = tokio::spawn(waiting);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "acknowledged outside the lease");
    assert_eq!(led.status().role, Role::Leader);

    // The fallback is the commit wait. It may also find the leader
    // deposed by the time the others are back.
    led.heal();
    if waiting.await.expect("the wait") == Ok(()) {
        assert!(led.status().committed >= Some(written.end - 1));
    }
    led.cluster.stop().await;
}

/// **An append that names a higher term gives the lease up before openraft
/// sees it.** openraft's role reaches the check late, so the check would
/// pass until then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_of_a_higher_term_voids_the_lease_before_openraft_sees_it() {
    let mut led = Led::leased().await;
    let member = led.member();
    let runtime = led.cluster.runtime(&led.leader.broker);
    // Park the core, so openraft cannot take the append.
    let (release, parked) = std::sync::mpsc::channel::<()>();
    let park = member.raft().external_request(move |_| {
        let _ = parked.recv();
    });
    park.await.expect("park");
    let from = led.followers().next().expect("a follower");
    let append = AppendEntriesRequest {
        vote: vote_of(led.term + 3, &from, true),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    let request = to(&led.leader.broker, Request::Append(append));
    let answered = tokio::time::timeout(Duration::from_millis(50), runtime.handle(request)).await;
    assert!(answered.is_err(), "the parked member answered");

    assert_eq!(led.status().role, Role::Leader);
    assert_eq!(member.lease().void(), led.term + 2);
    assert!(led.admit().is_err());
    release.send(()).expect("release");
    led.cluster.stop().await;
}

/// **An answer that names a higher vote gives the lease up.** The leader
/// adopts that vote and grants its candidate at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_answer_naming_a_higher_vote_voids_the_lease() {
    let mut led = Led::leased().await;
    let member = led.member();
    let follower = led.followers().next().expect("a follower");
    let higher = vote_of(led.term + 3, &follower, false);
    *led.cluster.net.higher_vote.lock() = Some((follower, higher));

    let void = led.term + 2;
    soon("the answer voided nothing", || {
        member.lease().void() == void
    })
    .await;
    assert!(led.admit().is_err());
    *led.cluster.net.higher_vote.lock() = None;
    led.cluster.stop().await;
}

/// **A vote it grants gives the lease up, on the answer. A pre-vote it
/// would grant changes no vote and gives nothing up.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_granted_vote_voids_the_lease_and_a_granted_pre_vote_does_not() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let runtime = cluster.runtime("a");
    let member = runtime.group(&shard(0)).expect("group");

    assert!(granted(
        runtime.handle(to("a", pre_vote_request(5, "b"))).await
    ));
    assert_eq!(member.lease().void(), 0);
    assert!(granted(runtime.handle(to("a", vote_request(5, "b"))).await));
    assert_eq!(member.lease().void(), 4);
    cluster.stop().await;
}

/// **A vote request openraft refuses gives nothing up**, so a candidate
/// nobody follows does not cost a healthy leader its lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_vote_request_costs_the_leader_nothing() {
    let mut led = Led::leased().await;
    let runtime = led.cluster.runtime(&led.leader.broker);
    let from = led.followers().next().expect("a follower");
    for request in [vote_request, pre_vote_request] {
        let request = to(&led.leader.broker, request(led.term + 5, &from));
        assert!(!granted(runtime.handle(request).await));
    }
    assert_eq!(led.member().lease().void(), 0);
    assert_eq!(led.status().term, led.term);
    soon("the lease was lost", || led.admit().is_ok()).await;
    led.cluster.stop().await;
}

/// **The void only grows.** A member that gave up term 1 for a transfer,
/// led term 2 and then voted in term 3 still refuses its late view of
/// term 1.
#[tokio::test]
async fn a_later_void_does_not_lift_an_earlier_one() {
    let lease = Lease::new(Duration::from_secs(1));
    let leading = |term: u64| {
        move || View {
            leads: Some(term),
            acked: Some(Duration::ZERO),
        }
    };
    assert_eq!(lease.admit(1, leading(1)), Ok(()));
    lease.void_through(1);
    assert_eq!(lease.admit(2, leading(2)), Ok(()));
    lease.named(3);
    assert_eq!(lease.void(), 2);
    // An older message names less than the void.
    lease.named(2);
    assert_eq!(lease.void(), 2);
    for term in [1, 2] {
        assert_eq!(lease.admit(term, leading(term)), Err(lease::VOID));
    }
    assert_eq!(lease.admit(3, leading(3)), Ok(()));
}

/// **A transfer gives up the leader's own term before it is asked for**,
/// whether or not it goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_voids_the_leaders_term_before_it_is_asked_for() {
    let mut led = Led::leased().await;
    assert_eq!(
        led.engine.transfer_starting(&shard(0)).await,
        Some(led.term)
    );
    assert_eq!(led.status().role, Role::Leader);
    assert_eq!(led.member().lease().void(), led.term);
    assert_eq!(lost(&led.admit()), lease::VOID);
    led.cluster.stop().await;
}

/// **A transfer request gives the term up before it leaves**, whoever
/// asked openraft for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_request_voids_the_term_before_it_leaves() {
    let mut led = Led::leased().await;
    led.cluster.net.lose_transfers.store(true, Relaxed);
    let target = MemberId::new(led.followers().next().expect("a follower"), 1);
    let member = led.member();
    let asked = member.raft().trigger().transfer_leader(target).await;
    asked.expect("asked");
    let void = led.term;
    soon("the request voided nothing", || {
        member.lease().void() == void
    })
    .await;
    assert!(led.admit().is_err());
    led.cluster.stop().await;
}

/// **`RaftEngine::transfer` gives the term up and hands the group on.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_ends_the_term_and_its_lease() {
    let mut led = Led::leased().await;
    let target = MemberId::new(led.followers().next().expect("a follower"), 1);
    assert_eq!(led.engine.transfer(&shard(0), target).await, Some(led.term));
    assert_eq!(lost(&led.admit()), lease::VOID);
    assert_eq!(
        led.engine.transfer(&shard(7), led.leader.clone()).await,
        None
    );
    // The target stands, or the old leader gives up waiting and does.
    let term = led.term;
    eventually(|| {
        (led.cluster
            .statuses(0)
            .iter()
            .any(|status| status.term > term))
        .then_some(())
    })
    .await;
    assert!(led.admit().is_err());
    led.cluster.stop().await;
}

/// **A jump of boottime over the monotonic clock refuses for one lease.**
/// A smaller one is added to the lease's age.
#[test]
fn a_suspend_refuses_for_one_lease() {
    let ms = Duration::from_millis;
    let lease = ms(1_500);
    let mut wake = Wake::starting(ms(10_000));
    assert_eq!(wake.look(ms(100), ms(10_100), lease), Some(ms(0)));
    assert_eq!(wake.look(ms(200), ms(10_205), lease), Some(ms(5)));
    // Suspended for three seconds.
    assert_eq!(wake.look(ms(300), ms(13_300), lease), None);
    assert_eq!(wake.look(ms(1_799), ms(14_799), lease), None);
    assert_eq!(wake.look(ms(1_800), ms(14_800), lease), Some(ms(0)));
}

/// **The suspend is seen by the check itself**, not by a task that runs
/// after it: the first check after the wake refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_check_after_a_suspend_refuses() {
    let mut led = Led::leased().await;
    led.member().lease().suspended_for(Duration::from_secs(5));
    assert_eq!(lost(&led.admit()), lease::WOKE);
    assert_eq!(led.status().role, Role::Leader);

    // The lease comes back once its clock has run one lease. Writes keep
    // the heartbeats short meanwhile.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while led.admit().is_err() {
        assert!(tokio::time::Instant::now() < deadline, "no lease again");
        led.propose(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    led.cluster.stop().await;
}

/// **The config refuses a lease that can outlast a follower's promise**:
/// `lease * (1 + p) / (1 - p) <= election_max`.
#[test]
fn a_lease_that_can_outlast_a_followers_promise_is_refused() {
    let with = |lease: u64, clock_drift_ppm: u32| RuntimeConfig {
        lease: Duration::from_millis(lease),
        clock_drift_ppm,
        ..RuntimeConfig::default()
    };
    assert!(RuntimeConfig::default().raft().is_ok());
    // 2 s * 0.9 / 1.1 is 1636.36 ms.
    assert!(with(1_636, 100_000).raft().is_ok());
    assert!(with(1_637, 100_000).raft().is_err());
    assert!(with(2_000, 0).raft().is_ok());
    assert!(with(2_001, 0).raft().is_err());
    assert!(with(1, 1_000_000).raft().is_err());
}

/// **A restarted member takes no vote and no pre-vote for one election
/// timeout, whatever vote it stored.** Here: a leader's own vote and its
/// followers' votes for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_holds_its_vote_after_a_leaders_term() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    let term = cluster.statuses(0)[0].term;
    cluster.stop().await;
    cluster.start(false);
    cluster.open(0).await;

    for broker in BROKERS {
        let runtime = cluster.runtime(broker);
        for request in [vote_request, pre_vote_request] {
            let dropped = runtime.handle(to(broker, request(term + 9, "x"))).await;
            assert!(
                matches!(dropped, Err(Refused::Unreachable(_))),
                "{broker} took a vote request just after it started"
            );
        }
        assert_eq!(runtime.status(&shard(0)).expect("status").term, term);
    }
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    for broker in BROKERS {
        let runtime = cluster.runtime(broker);
        let answer = runtime.handle(to(broker, vote_request(term + 9, "x")));
        assert!(answer.await.is_ok(), "{broker} still takes no vote");
    }
    cluster.stop().await;
}

/// **The same for a vote that is not a leader's**: one granted to a
/// candidate, and a candidate's own. A member that has never voted has
/// promised nothing and answers at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_holds_its_vote_after_an_election_nobody_won() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    cluster.net.cut.lock().extend(BROKERS.map(String::from));
    // a granted b's candidacy. b stood and heard from nobody.
    assert!(granted(
        cluster
            .runtime("a")
            .handle(to("a", vote_request(5, "b")))
            .await
    ));
    let stood = cluster.runtime("b");
    let stood = stood.initialize(&shard(0), members()).await;
    stood.expect("initialize");
    eventually(|| (cluster.runtime("b").status(&shard(0))?.term > 0).then_some(())).await;
    for broker in ["a", "b"] {
        restart(&mut cluster, broker).await;
    }

    for broker in ["a", "b"] {
        let runtime = cluster.runtime(broker);
        for request in [vote_request, pre_vote_request] {
            let dropped = runtime.handle(to(broker, request(9, "c"))).await;
            assert!(
                matches!(dropped, Err(Refused::Unreachable(_))),
                "{broker} took a vote request just after it started"
            );
        }
    }
    assert!(granted(
        cluster
            .runtime("c")
            .handle(to("c", vote_request(9, "a")))
            .await
    ));
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    assert!(granted(
        cluster
            .runtime("a")
            .handle(to("a", vote_request(9, "c")))
            .await
    ));
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}

/// **A member that takes no vote is not ticked**, so it does not ask for
/// one either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_is_not_ticked_while_it_holds_its_vote() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;

    tokio::time::sleep(timers().election_max / 2).await;
    for status in cluster.statuses(0) {
        assert_eq!(status.ticks, 0, "{} was ticked", status.member);
    }
    cluster.elected(0).await;
    cluster.stop().await;
}
