//! The leader lease: when a `Leader` write is acknowledged on the leader's
//! log alone, what gives the lease up, and the vote a restarted member
//! does not take. One test per rule of `docs/formal/FelixRaftLease.tla`.

use std::sync::atomic::Ordering::Relaxed;

use openraft::raft::{AppendEntriesRequest, SnapshotResponse, VoteResponse};
use openraft::type_config::alias::LogIdOf;

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

/// A candidacy `from` a member whose log is behind nobody's.
fn stands(term: u64, from: &str) -> Request {
    let last = LogIdOf::<ShardTypes>::new((term - 1).into(), 1 << 40);
    Request::Vote(VoteRequest::new(vote_of(term, from, false), Some(last)))
}

fn heartbeat(vote: VoteOf<ShardTypes>) -> Request {
    Request::Append(AppendEntriesRequest {
        vote,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    })
}

fn dropped(answer: &Result<RaftReply, Refused>) -> bool {
    matches!(answer, Err(Refused::Unreachable(_)))
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

/// Start `broker`'s member of group 0 on its log.
async fn open_on(cluster: &mut Cluster, broker: &str) {
    let log = open_log(cluster.root.path(), broker, 0, log_config());
    cluster.logs.insert((broker.into(), 0), log.clone());
    let opened = cluster.runtime(broker).open(group(0), 1, log).await;
    opened.expect("open");
}

/// A group with a committed record, stopped whole and its brokers started
/// again with no member open. Returns the old leader, its term and its
/// followers.
async fn stopped_whole(ticking: bool) -> (Cluster, MemberId, u64, [&'static str; 2]) {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    let leader = cluster.elected(0).await;
    let term = cluster.runtime(&leader.broker).status(&shard(0));
    let term = term.expect("status").term;
    cluster.stop().await;
    cluster.start(ticking);
    let followers = BROKERS
        .into_iter()
        .filter(|broker| *broker != &*leader.broker);
    let followers: Vec<_> = followers.collect();
    let followers = followers.try_into().expect("two followers");
    (cluster, leader, term, followers)
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

/// **A snapshot that names a higher term gives the lease up**, though the
/// member refuses the snapshot itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_of_a_higher_term_voids_the_lease() {
    let mut led = Led::leased().await;
    let runtime = led.cluster.runtime(&led.leader.broker);
    let from = led.followers().next().expect("a follower");
    let snapshot = Request::Snapshot {
        vote: vote_of(led.term + 3, &from, true),
        meta: Default::default(),
    };
    let answer = runtime.handle(to(&led.leader.broker, snapshot)).await;
    assert!(matches!(answer, Err(Refused::NoSnapshots(_))));
    assert_eq!(led.member().lease().void(), led.term + 2);
    assert_eq!(lost(&led.admit()), lease::VOID);
    led.cluster.stop().await;
}

/// **The answer to a snapshot names a vote openraft acts on**, as a
/// refused append's does.
#[test]
fn a_snapshots_answer_voids_by_the_vote_it_names() {
    use super::super::network::named;
    let answer = Reply::Snapshot(SnapshotResponse::new(vote_of(9, "c", true)));
    assert_eq!(named(&answer, false), Some(9));
}

/// **The lease runs from when the answered message was handed off, not
/// from when its answer came back.** openraft keeps that time, and Felix
/// reads it: with every answer `slow` on the way back, the age is never
/// under `slow`. An age that restarted on the answer would let the lease
/// outlast the follower's promise by the trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_is_as_old_as_the_message_a_majority_answered() {
    use openraft::Instant as _;
    use openraft::async_runtime::watch::WatchReceiver;
    let mut led = Led::leased().await;
    // Under the heartbeat interval, which is how long openraft waits.
    let slow = timers().heartbeat / 2;
    *led.cluster.net.answer_after.lock() = Some(slow);
    // Every answer from before is this old by then.
    tokio::time::sleep(slow).await;

    let member = led.member();
    let mut newest = None;
    let mut answers = 0;
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < until {
        // Writes keep the heartbeats short.
        led.propose(1).await;
        for _ in 0..10 {
            let acked = member.raft().metrics().borrow_watched().last_quorum_acked;
            let acked = acked.expect("the leader was answered");
            let age = acked.elapsed();
            assert!(
                age >= slow,
                "an answer {slow:?} late made the lease {age:?} old"
            );
            if newest != Some(acked) {
                newest = Some(acked);
                answers += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    assert!(answers > 3, "only {answers} answers came back");
    assert_eq!(led.status().role, Role::Leader);
    led.cluster.stop().await;
}

/// **The answer to a pre-vote this member asked for gives nothing up**,
/// whatever vote it names: a member that only asked has changed no vote.
#[test]
fn a_pre_votes_answer_voids_nothing() {
    use super::super::network::named;
    let answer = Reply::Vote(VoteResponse::new(vote_of(9, "c", false), None, false));
    assert_eq!(named(&answer, true), None);
    assert_eq!(named(&answer, false), Some(9));
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
    // No vote is answered either, so nothing else names a later term.
    led.cluster.net.lose_votes.store(true, Relaxed);
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
    let handed = led.engine.transfer(&shard(0), target.clone()).await;
    assert_eq!(handed, Some(led.term));
    assert_eq!(lost(&led.admit()), lease::VOID);
    assert_eq!(
        led.engine.transfer(&shard(7), led.leader.clone()).await,
        None
    );
    // The target stands at once, on the old leader's word.
    let next = led.term + 1;
    eventually(|| {
        let status = led.cluster.runtime(&target.broker).status(&shard(0))?;
        (status.role == Role::Leader && status.term == next).then_some(())
    })
    .await;
    assert!(led.admit().is_err());
    led.cluster.stop().await;
}

/// **A transfer nobody takes ends with the old leader standing again.**
/// It leads a later term on a new lease, and the term it gave up stays
/// given up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_that_expires_leaves_the_old_term_void() {
    let mut led = Led::leased().await;
    led.cluster.net.lose_transfers.store(true, Relaxed);
    // Nothing the followers send is heard, so only the old leader can win.
    led.cluster.net.muted.lock().extend(led.followers());
    let target = MemberId::new(led.followers().next().expect("a follower"), 1);
    let handed = led.engine.transfer(&shard(0), target).await;
    assert_eq!(handed, Some(led.term));
    // It waits for the target and sends nothing: still the old term's
    // leader as openraft has it, with no lease.
    tokio::time::sleep(timers().election_min / 4).await;
    let status = led.status();
    assert_eq!((status.role, status.term), (Role::Leader, led.term));
    assert_eq!(lost(&led.admit()), lease::VOID);

    let term = eventually(|| {
        let status = led.status();
        (status.role == Role::Leader && status.term > led.term).then_some(status.term)
    })
    .await;
    assert!(led.admit().is_err());
    assert!(led.member().lease().void() >= led.term);
    let engine = &led.engine;
    soon("no lease in the new term", || {
        engine.admit_local_ack(&shard(0), term).is_ok()
    })
    .await;
    led.heal();
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

/// **The clocks are read after openraft's answer.** A suspend that lands
/// between the two is seen, not passed with an answer from before it.
#[tokio::test]
async fn a_suspend_while_openraft_answers_refuses() {
    let lease = Lease::new(Duration::from_millis(1_500));
    let answered = lease.admit(1, || {
        lease.suspended_for(Duration::from_secs(5));
        View {
            leads: Some(1),
            acked: Some(Duration::ZERO),
        }
    });
    assert_eq!(answered, Err(lease::WOKE));
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
/// timeout when it may have promised a leader, and does not stand.** Here:
/// followers' votes for their leader. The old leader promised nobody: it
/// takes votes at once and stands for the next term as it starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_holds_its_vote_after_a_leaders_term() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    let term = cluster.statuses(0)[0].term;
    let leader = cluster.elected(0).await;
    cluster.stop().await;
    cluster.start(false);
    cluster.open(0).await;

    let followers = BROKERS
        .into_iter()
        .filter(|broker| *broker != &*leader.broker);
    for broker in followers.clone() {
        let runtime = cluster.runtime(broker);
        for request in [vote_request, pre_vote_request] {
            let dropped = runtime.handle(to(broker, request(term + 9, "x"))).await;
            assert!(
                matches!(dropped, Err(Refused::Unreachable(_))),
                "{broker} took a vote request just after it started"
            );
        }
        let status = runtime.status(&shard(0)).expect("status");
        assert_eq!(status.term, term, "{broker} stood as it started");
        assert_ne!(status.role, Role::Candidate);
    }
    let runtime = cluster.runtime(&leader.broker);
    let stood = || {
        runtime
            .status(&shard(0))
            .is_some_and(|status| status.term == term + 1)
    };
    soon("the old leader did not stand", stood).await;
    let answer = runtime.handle(to(&leader.broker, vote_request(term + 9, "x")));
    assert!(answer.await.is_ok(), "the old leader held its vote");

    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    for broker in followers {
        let runtime = cluster.runtime(broker);
        let answer = runtime.handle(to(broker, vote_request(term + 9, "x")));
        assert!(answer.await.is_ok(), "{broker} still takes no vote");
    }
    cluster.stop().await;
}

/// **A leader restarted alone is leader again within one vote round.** Its
/// stored vote is its own committed one, so it promised nobody, and its
/// followers grant the vote of the leader they follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_leader_leads_again_within_one_vote_round() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    let leader = cluster.elected(0).await;

    let started = tokio::time::Instant::now();
    restart(&mut cluster, &leader.broker).await;
    let again = cluster.elected(0).await;
    let took = started.elapsed();
    assert_eq!(again, leader);
    assert!(
        took < timers().election_min,
        "the leader took {took:?} to lead again"
    );
    cluster.stop().await;
}

/// **A sole voter is not held**: its promise binds nobody else. It leads
/// again as it starts, and after a vote for another it takes votes at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_sole_voter_is_not_held() {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    let alone = BTreeSet::from([MemberId::new("a", 1)]);
    let a = cluster.runtime("a");
    a.initialize(&shard(0), alone).await.expect("initialize");
    let leads = |cluster: &Cluster| {
        let status = cluster.runtime("a").status(&shard(0))?;
        (status.role == Role::Leader).then_some(status.term)
    };
    let term = eventually(|| leads(&cluster)).await;

    let started = tokio::time::Instant::now();
    restart(&mut cluster, "a").await;
    eventually(|| leads(&cluster)).await;
    let took = started.elapsed();
    assert!(
        took < timers().election_min,
        "a took {took:?} to lead again"
    );

    // A later leader's heartbeat: the stored vote is not a's own.
    let heartbeat = AppendEntriesRequest::<ShardTypes> {
        vote: vote_of(term + 5, "x", true),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    let taken = cluster
        .runtime("a")
        .handle(to("a", Request::Append(heartbeat)))
        .await;
    assert!(taken.is_ok());
    let status = cluster.runtime("a").status(&shard(0)).expect("status");
    assert_eq!(status.leader, Some(MemberId::new("x", 1)));
    restart(&mut cluster, "a").await;
    for request in [vote_request, pre_vote_request] {
        let answer = cluster
            .runtime("a")
            .handle(to("a", request(term + 9, "x")))
            .await;
        assert!(answer.is_ok(), "a sole voter held its vote");
    }
    cluster.stop().await;
}

/// **Only the sole voter itself is not held.** A learner of a one-voter
/// group took that voter's messages, and is held like any follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_learner_beside_a_sole_voter_is_held() {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    let alone = BTreeSet::from([MemberId::new("a", 1)]);
    let a = cluster.runtime("a");
    a.initialize(&shard(0), alone).await.expect("initialize");
    let voter = a.group(&shard(0)).expect("group");
    eventually(|| (a.status(&shard(0))?.role == Role::Leader).then_some(())).await;
    // Refused while the group's first set is not committed.
    let learner = MemberId::new("b", 1);
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let added = voter
            .raft()
            .add_learner(learner.clone(), Default::default(), true);
        match added.await {
            Ok(_) => break,
            Err(err) => assert!(tokio::time::Instant::now() < deadline, "no learner: {err}"),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let term = a.status(&shard(0)).expect("status").term;
    // The learner has taken the voter's append, and its vote with it.
    let b = cluster.runtime("b");
    eventually(|| {
        let (voter, learner) = (a.status(&shard(0))?, b.status(&shard(0))?);
        (learner.last_index == voter.last_index && learner.leader == voter.leader).then_some(())
    })
    .await;

    restart(&mut cluster, "b").await;
    for request in [vote_request, pre_vote_request] {
        let b = cluster.runtime("b");
        let asked = b.handle(to("b", request(term + 9, "x"))).await;
        assert!(dropped(&asked), "the learner took a vote request");
    }
    cluster.stop().await;
}

/// **A change down to one voter that is not committed frees nobody.**
/// The member's newest set names it alone, but the committed set still
/// has the voters that follow the leader, so it is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_alone_in_a_set_not_committed_is_held() {
    use openraft::entry::RaftEntry;
    use openraft::type_config::alias::EntryOf;
    let mut led = Led::leased().await;
    let b = led.followers().next().expect("a follower");
    // The leader sends nothing more: the entry below stays.
    led.cluster.net.cut.lock().insert(b.clone());
    let last = led.status().last_index.expect("a log");
    let alone = BTreeSet::from([MemberId::new(&*b, 1)]);
    let alone = Membership::new_with_defaults(vec![alone], []);
    let change = AppendEntriesRequest::<ShardTypes> {
        vote: vote_of(led.term, &led.leader.broker, true),
        prev_log_id: Some(LogIdOf::<ShardTypes>::new(led.term.into(), last)),
        entries: vec![EntryOf::<ShardTypes>::new_membership(
            LogIdOf::<ShardTypes>::new(led.term.into(), last + 1),
            alone,
        )],
        leader_commit: None,
    };
    let runtime = led.cluster.runtime(&b);
    let taken = runtime.handle(to(&b, Request::Append(change))).await;
    assert!(matches!(taken, Ok(RaftReply(Reply::Append(Ok(_))))));

    restart(&mut led.cluster, &b).await;
    let asked = runtime
        .handle(to(&b, vote_request(led.term + 9, "x")))
        .await;
    assert!(dropped(&asked), "{b} took a vote request");
    led.cluster.stop().await;
}

/// **A held member grants the leader its stored vote names, once.** The
/// vote is committed, so it names the leader this member followed, and
/// the promise the hold keeps was made to that leader. Any other
/// candidate is dropped, and so is that leader's pre-vote: openraft
/// refuses a pre-vote inside the window whoever asks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_member_grants_the_leader_its_stored_vote_names() {
    let (mut cluster, leader, term, followers) = stopped_whole(false).await;
    for broker in followers {
        open_on(&mut cluster, broker).await;
    }
    let old = &*leader.broker;
    for (broker, other) in [(followers[0], followers[1]), (followers[1], followers[0])] {
        let runtime = cluster.runtime(broker);
        let ask = |request| runtime.handle(to(broker, request));
        let asked = ask(stands(term + 1, other)).await;
        assert!(dropped(&asked), "{broker} took {other}'s request");
        let asked = ask(pre_vote_request(term + 1, old)).await;
        assert!(dropped(&asked), "{broker} took a pre-vote");

        assert!(granted(ask(stands(term + 1, old)).await));
        let member = runtime.group(&shard(0)).expect("group");
        assert_eq!(member.lease().void(), term);
        // The stored vote is now one that is not committed.
        let asked = ask(stands(term + 2, old)).await;
        assert!(dropped(&asked), "{broker} was asked twice");
    }
    cluster.stop().await;
}

/// **The stored vote lets its leader through only while it is the stored
/// vote.** The same leader's heartbeat keeps it. A later leader's append
/// replaces it, and the old leader is then held like anyone else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_leaders_append_ends_what_the_stored_vote_lets_through() {
    let (mut cluster, leader, term, [kept, replaced]) = stopped_whole(false).await;
    for broker in [kept, replaced] {
        open_on(&mut cluster, broker).await;
    }
    let old = &*leader.broker;

    let runtime = cluster.runtime(kept);
    let same = heartbeat(vote_of(term, old, true));
    assert!(runtime.handle(to(kept, same)).await.is_ok());
    assert!(granted(
        runtime.handle(to(kept, stands(term + 1, old))).await
    ));

    let runtime = cluster.runtime(replaced);
    let later = heartbeat(vote_of(term + 5, kept, true));
    assert!(runtime.handle(to(replaced, later)).await.is_ok());
    let asked = runtime.handle(to(replaced, stands(term + 9, old)));
    assert!(dropped(&asked.await), "the old leader was let through");
    cluster.stop().await;
}

/// **When every member restarts, the old leader leads again within one
/// vote round.** Its followers hold their votes, and grant the one
/// candidate their stored vote names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_restarted_whole_has_its_leader_back_within_one_vote_round() {
    let (mut cluster, leader, term, followers) = stopped_whole(true).await;
    for broker in followers {
        open_on(&mut cluster, broker).await;
    }
    let started = tokio::time::Instant::now();
    open_on(&mut cluster, &leader.broker).await;
    let again = cluster.elected(0).await;
    let took = started.elapsed();
    assert_eq!(again, leader);
    assert!(
        took < timers().election_min,
        "the group took {took:?} to have a leader again"
    );
    let status = cluster.runtime(&leader.broker).status(&shard(0));
    assert_eq!(status.expect("status").term, term + 1);
    cluster.stop().await;
}

/// **A held member handed the leadership stands, and is ticked once it
/// leads**, so it heartbeats inside its hold. Standing replaces its
/// stored vote, so the old leader is let through no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_member_handed_the_leadership_stands_and_is_ticked() {
    let mut led = Led::leased().await;
    let target = led.followers().next().expect("a follower");
    let started = tokio::time::Instant::now();
    restart(&mut led.cluster, &target).await;
    let runtime = led.cluster.runtime(&target);
    let dropped_one = runtime.handle(to(&target, vote_request(led.term + 9, "x")));
    assert!(dropped(&dropped_one.await), "{target} is not held");
    // openraft hands over only to a target that has taken an append
    // since it started.
    led.propose(1).await;
    let last = led.status().last_index;
    eventually(|| (runtime.status(&shard(0))?.last_index == last).then_some(())).await;

    let handed = MemberId::new(&*target, 1);
    let handed = led.engine.transfer(&shard(0), handed).await;
    assert_eq!(handed, Some(led.term));
    let next = led.term + 1;
    let ticks = eventually(|| {
        let status = runtime.status(&shard(0))?;
        (status.role == Role::Leader && status.term == next).then_some(status.ticks)
    })
    .await;
    assert_eq!(ticks, 0, "{target} was ticked before it led");
    let old = &*led.leader.broker;
    let asked = runtime.handle(to(&target, stands(led.term + 9, old)));
    assert!(dropped(&asked.await), "the old leader was let through");
    soon("the new leader was not ticked", || {
        runtime.status(&shard(0)).is_some_and(|now| now.ticks > 0)
    })
    .await;
    // All of it inside the hold, or the test showed nothing.
    assert!(started.elapsed() < timers().election_max);
    led.cluster.stop().await;
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
    // a's stored vote names b and is not committed: b never led, so the
    // promise a may have made was to somebody else.
    let a = cluster.runtime("a");
    let asked = a.handle(to("a", vote_request(6, "b"))).await;
    assert!(dropped(&asked), "a vote nobody won let b through");
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
    let leader = cluster.elected(0).await;
    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;

    tokio::time::sleep(timers().election_max / 2).await;
    for status in cluster.statuses(0) {
        if status.member != leader {
            assert_eq!(status.ticks, 0, "{} was ticked", status.member);
        }
    }
    cluster.elected(0).await;
    cluster.stop().await;
}
