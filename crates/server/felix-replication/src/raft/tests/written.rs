//! The early acknowledgement of a `Quorum` write on a stream that does not
//! fsync on commit: a majority of the voters has written the record.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use felix_storage::raft_log::LogPoint;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::impls::EmptyNode;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};

use super::super::group::Group;
use super::super::network::Reply;
use super::super::written::{Look, Reports, look, majority_wrote};
use super::*;
use crate::engine::{CommitError, Written};

/// Long enough for a report to cross and be counted, were it going to be.
const SETTLE: Duration = Duration::from_millis(400);

fn member(broker: &str) -> MemberId {
    MemberId::new(broker, 1)
}

fn at(term: u64, index: u64) -> LogPoint {
    LogPoint { term, index }
}

fn set(brokers: &[&str]) -> BTreeSet<MemberId> {
    brokers.iter().map(|broker| member(broker)).collect()
}

fn heard(reports: &[(&str, LogPoint)]) -> Vec<(MemberId, LogPoint)> {
    let named = reports.iter().map(|(broker, at)| (member(broker), *at));
    named.collect()
}

/// The count on three voters: the leader and one more that says it holds
/// the record. A member that has said nothing has written nothing.
#[test]
fn a_majority_is_the_leader_and_the_voters_that_said_so() {
    let voters = [set(&["a", "b", "c"])];
    let record = at(3, 9);
    assert!(!majority_wrote(&voters, &member("a"), &[], record));
    let behind = heard(&[("b", at(3, 8)), ("c", at(3, 8))]);
    assert!(!majority_wrote(&voters, &member("a"), &behind, record));
    let one = heard(&[("b", at(3, 9))]);
    assert!(majority_wrote(&voters, &member("a"), &one, record));
    let past = heard(&[("c", at(3, 12))]);
    assert!(majority_wrote(&voters, &member("a"), &past, record));
    assert!(!majority_wrote(&[], &member("a"), &one, record));
    assert!(!majority_wrote(
        &[BTreeSet::new()],
        &member("a"),
        &one,
        record
    ));
}

/// A learner holds the record and says so. It is not a voter, so the count
/// does not move, however many learners report.
#[test]
fn a_learners_report_is_not_counted() {
    let voters = [set(&["a", "b", "c"])];
    let learners = heard(&[("d", at(3, 9)), ("e", at(3, 9))]);
    assert!(!majority_wrote(&voters, &member("a"), &learners, at(3, 9)));
}

/// A leader that is no longer a voter does not count its own copy.
#[test]
fn a_leader_outside_the_voters_does_not_count_itself() {
    let voters = [set(&["b", "c", "d"])];
    let one = heard(&[("b", at(3, 9))]);
    assert!(!majority_wrote(&voters, &member("a"), &one, at(3, 9)));
    let two = heard(&[("b", at(3, 9)), ("c", at(3, 9))]);
    assert!(majority_wrote(&voters, &member("a"), &two, at(3, 9)));
}

/// A joint membership needs a majority of the old voters and of the new.
#[test]
fn a_joint_membership_needs_both_halves() {
    let joint = [set(&["a", "b", "c"]), set(&["a", "d", "e"])];
    let record = at(3, 9);
    let new_only = heard(&[("d", at(3, 9)), ("e", at(3, 9))]);
    assert!(!majority_wrote(&joint, &member("a"), &new_only, record));
    let old_only = heard(&[("b", at(3, 9)), ("c", at(3, 9))]);
    assert!(!majority_wrote(&joint, &member("a"), &old_only, record));
    let both = heard(&[("b", at(3, 9)), ("d", at(3, 9))]);
    assert!(majority_wrote(&joint, &member("a"), &both, record));
}

/// A log id of another term at or past the record's index says nothing
/// about the record: that log can hold another leader's entry there.
#[test]
fn a_report_at_another_term_is_not_counted() {
    let voters = [set(&["a", "b", "c"])];
    let record = at(3, 9);
    for term in [2, 4] {
        let other = heard(&[("b", at(term, 20)), ("c", at(term, 20))]);
        assert!(!majority_wrote(&voters, &member("a"), &other, record));
    }
}

/// The furthest position each sender named is kept, and a peer cannot make
/// the leader hold more than a voter set's worth of senders.
#[test]
fn the_furthest_report_of_each_sender_is_kept() {
    let reports = Reports::new();
    reports.heard(member("b"), at(3, 9));
    reports.heard(member("b"), at(3, 7));
    assert_eq!(reports.from(&member("b")), Some(at(3, 9)));
    reports.heard(member("b"), at(2, 30));
    assert_eq!(reports.from(&member("b")), Some(at(3, 9)));
    reports.heard(member("b"), at(4, 2));
    assert_eq!(reports.from(&member("b")), Some(at(4, 2)));
    assert_eq!(reports.from(&member("c")), None);

    for n in 0..200 {
        reports.heard(MemberId::new(format!("x{n}"), 1), at(1, n));
    }
    assert_eq!(reports.from(&member("b")), Some(at(4, 2)));
    let kept = (0..200).filter(|n| reports.from(&MemberId::new(format!("x{n}"), 1)).is_some());
    assert_eq!(kept.count(), 63);
    assert!(reports.from(&MemberId::new("x199", 1)).is_some());
}

/// Spawns a wait on `reports` for each entry `1..=count` at term 1 and
/// returns the waits and a count of their looks. `deposed` makes every
/// look find the voters unknown, as on a member that stopped leading.
fn waiting(
    reports: &Arc<Reports>,
    voters: &[BTreeSet<MemberId>],
    count: u64,
    deposed: &Arc<AtomicBool>,
) -> (Vec<tokio::task::JoinHandle<bool>>, Arc<AtomicUsize>) {
    let looks = Arc::new(AtomicUsize::new(0));
    let waits = (1..=count).map(|index| {
        let (reports, voters) = (Arc::clone(reports), voters.to_vec());
        let (looks, deposed) = (Arc::clone(&looks), Arc::clone(deposed));
        tokio::spawn(async move {
            let entry = at(1, index);
            let wrote = reports.written(entry, |heard| {
                looks.fetch_add(1, Ordering::Relaxed);
                if deposed.load(Ordering::Relaxed) {
                    return Look::Unknown;
                }
                look(&voters, &member("a"), heard, entry)
            });
            wrote.await
        })
    });
    (waits.collect(), looks)
}

/// Lets every spawned wait run until it sleeps. One round is enough on a
/// current-thread runtime; the rest is margin.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// Five voters, so a write needs two followers. One follower far ahead and
/// another stepping through the entries one report at a time wakes each
/// write about once, not every write on every report.
#[tokio::test(flavor = "current_thread")]
async fn a_report_wakes_only_the_writes_it_can_complete() {
    const WRITES: u64 = 300;
    let reports = Arc::new(Reports::new());
    let voters = [set(&["a", "b", "c", "d", "e"])];
    let deposed = Arc::new(AtomicBool::new(false));
    let (waits, looks) = waiting(&reports, &voters, WRITES, &deposed);
    settle().await;
    let before = looks.load(Ordering::Relaxed);

    reports.heard(member("b"), at(1, WRITES));
    for index in 1..=WRITES {
        reports.heard(member("c"), at(1, index));
        tokio::task::yield_now().await;
    }
    for wait in waits {
        assert!(wait.await.expect("the wait ran"), "a write was not counted");
    }
    let woken = looks.load(Ordering::Relaxed) - before;
    // One look per write once its report lands, with room for a wake that
    // lands before the write sleeps. A wake per report is WRITES^2 / 2.
    assert!(
        woken <= 2 * WRITES as usize,
        "{woken} looks for {WRITES} writes"
    );
}

/// Reports racing many waits from other threads: a report that lands
/// between a wait's look and its sleep still wakes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_report_is_lost_between_a_look_and_a_sleep() {
    const WRITES: u64 = 200;
    let voters = [set(&["a", "b", "c"])];
    for _ in 0..20 {
        let reports = Arc::new(Reports::new());
        let deposed = Arc::new(AtomicBool::new(false));
        let (waits, _) = waiting(&reports, &voters, WRITES, &deposed);
        let reporter = {
            let reports = Arc::clone(&reports);
            tokio::spawn(async move {
                for index in 1..=WRITES {
                    reports.heard(member("b"), at(1, index));
                    tokio::task::yield_now().await;
                }
            })
        };
        reporter.await.expect("the reporter ran");
        for wait in waits {
            let wrote = tokio::time::timeout(PATIENCE, wait).await;
            assert!(wrote.expect("woken").expect("the wait ran"));
        }
    }
}

/// A member that stops leading finds the voters unknown at its next look
/// and the wait ends without an answer. Here the look comes from a report
/// of a newer term, which sorts above every key and reaches every older
/// entry. A deposed leader's same-term reports sort below some keys and do
/// not wake those waits; the commit arm of the caller ends them.
#[tokio::test(flavor = "current_thread")]
async fn a_report_of_a_newer_term_ends_a_wait_on_a_lost_term() {
    let reports = Arc::new(Reports::new());
    let voters = [set(&["a", "b", "c"])];
    let deposed = Arc::new(AtomicBool::new(false));
    let (waits, _) = waiting(&reports, &voters, 20, &deposed);
    settle().await;
    deposed.store(true, Ordering::Relaxed);
    reports.heard(member("b"), at(2, 1));
    for wait in waits {
        let wrote = tokio::time::timeout(PATIENCE, wait).await;
        assert!(!wrote.expect("woken").expect("the wait ran"));
    }
}

/// A report that names a later entry in another term is no majority, so
/// the write waits on, and a later one in its own term completes it.
#[tokio::test(flavor = "current_thread")]
async fn a_report_at_another_term_does_not_end_the_wait() {
    let reports = Arc::new(Reports::new());
    let voters = [set(&["a", "b", "c"])];
    let deposed = Arc::new(AtomicBool::new(false));
    let (mut waits, _) = waiting(&reports, &voters, 1, &deposed);
    let wait = waits.pop().expect("one wait");
    settle().await;
    reports.heard(member("c"), at(2, 5));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "answered on another term's report");
    reports.heard(member("b"), at(1, 1));
    let wrote = tokio::time::timeout(PATIENCE, wait).await;
    assert!(wrote.expect("woken").expect("the wait ran"));
}

/// Voters a test can swap while a wait sleeps.
type SharedVoters = Arc<parking_lot::Mutex<Vec<BTreeSet<MemberId>>>>;

/// Starts a wait on the entry `at(1, 1)` whose voters can be swapped while
/// it sleeps, as a membership change does.
fn waiting_on_changing_voters(
    reports: &Arc<Reports>,
    voters: Vec<BTreeSet<MemberId>>,
) -> (SharedVoters, tokio::task::JoinHandle<bool>) {
    let voters = Arc::new(parking_lot::Mutex::new(voters));
    let wait = {
        let (reports, voters) = (Arc::clone(reports), Arc::clone(&voters));
        tokio::spawn(async move {
            let entry = at(1, 1);
            reports
                .written(entry, |heard| {
                    look(&voters.lock(), &member("a"), heard, entry)
                })
                .await
        })
    };
    (voters, wait)
}

/// Voters growing from three to five raise the shortfall from one follower
/// to two. The report that would have completed the write on three voters
/// moves it to the longer wait instead of answering.
#[tokio::test(flavor = "current_thread")]
async fn more_voters_raise_the_count_a_wait_needs() {
    let reports = Arc::new(Reports::new());
    let (voters, wait) = waiting_on_changing_voters(&reports, vec![set(&["a", "b", "c"])]);
    settle().await;
    *voters.lock() = vec![set(&["a", "b", "c", "d", "e"])];
    reports.heard(member("b"), at(1, 1));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "answered on one follower of five");
    reports.heard(member("c"), at(1, 1));
    let wrote = tokio::time::timeout(PATIENCE, wait).await;
    assert!(wrote.expect("woken").expect("the wait ran"));
}

/// Voters shrinking from five to three lower the shortfall: the follower
/// already written is a majority, and the next report wakes the wait.
#[tokio::test(flavor = "current_thread")]
async fn fewer_voters_lower_the_count_a_wait_needs() {
    let reports = Arc::new(Reports::new());
    let (voters, wait) =
        waiting_on_changing_voters(&reports, vec![set(&["a", "b", "c", "d", "e"])]);
    settle().await;
    reports.heard(member("b"), at(1, 1));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "answered on one follower of five");
    *voters.lock() = vec![set(&["a", "b", "c"])];
    reports.heard(member("d"), at(1, 1));
    let wrote = tokio::time::timeout(PATIENCE, wait).await;
    assert!(wrote.expect("woken").expect("the wait ran"));
}

/// A change to a joint membership needs a majority of both halves: a
/// follower of the old half alone does not answer, one of the new half does.
#[tokio::test(flavor = "current_thread")]
async fn a_joint_change_needs_both_halves_to_answer() {
    let reports = Arc::new(Reports::new());
    let (voters, wait) = waiting_on_changing_voters(&reports, vec![set(&["a", "b", "c"])]);
    settle().await;
    *voters.lock() = vec![set(&["a", "b", "c"]), set(&["a", "d", "e"])];
    reports.heard(member("b"), at(1, 1));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "answered without the new half");
    reports.heard(member("d"), at(1, 1));
    let wrote = tokio::time::timeout(PATIENCE, wait).await;
    assert!(wrote.expect("woken").expect("the wait ran"));
}

/// A learner far ahead holds the furthest position, so a voter's report
/// behind it leaves that position unchanged. The wait it completes still
/// wakes.
#[tokio::test(flavor = "current_thread")]
async fn a_learner_ahead_does_not_hide_a_voters_report() {
    let reports = Arc::new(Reports::new());
    let voters = [set(&["a", "b", "c"])];
    let deposed = Arc::new(AtomicBool::new(false));
    let (mut waits, _) = waiting(&reports, &voters, 5, &deposed);
    let wait = waits.pop().expect("the wait at 5");
    settle().await;
    reports.heard(member("learner"), at(1, 10));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!wait.is_finished(), "a learner made a majority");
    reports.heard(member("b"), at(1, 5));
    let wrote = tokio::time::timeout(PATIENCE, wait).await;
    assert!(wrote.expect("woken").expect("the wait ran"));
}

#[test]
fn a_look_names_the_largest_shortfall_of_any_set() {
    let record = at(3, 9);
    let a = member("a");
    let five = [set(&["a", "b", "c", "d", "e"])];
    assert_eq!(look(&five, &a, &[], record), Look::Short(2));
    let outside = [set(&["b", "c", "d"])];
    assert_eq!(look(&outside, &a, &[], record), Look::Short(2));
    let joint = [set(&["a", "b", "c"]), set(&["a", "d", "e", "f", "g"])];
    assert_eq!(look(&joint, &a, &[], record), Look::Short(2));
    let alone = [set(&["a"])];
    assert_eq!(look(&alone, &a, &[], record), Look::Wrote);
}

struct Led {
    cluster: Cluster,
    leader: MemberId,
    engine: RaftEngine,
}

impl Led {
    fn group(&self) -> Arc<Group> {
        let group = self.cluster.runtime(&self.leader.broker).group(&shard(0));
        group.expect("group")
    }

    fn status(&self, broker: &str) -> GroupStatus {
        self.cluster
            .runtime(broker)
            .status(&shard(0))
            .expect("status")
    }

    fn others(&self) -> Vec<&'static str> {
        let others = BROKERS
            .iter()
            .filter(|broker| **broker != &*self.leader.broker);
        others.copied().collect()
    }

    /// Write one record on the leader, acknowledged there once written.
    async fn propose(&self) -> Written {
        let record = Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"proposed"),
            headers: None,
        };
        let batch = Batch {
            expected: None,
            trim: None,
            commit: false,
            records: vec![record],
            ack: AckAt::Written,
            term: None,
            producer: None,
            publisher: None,
        };
        let written = self.engine.propose(&shard(0), batch).await;
        written.expect("written")
    }

    fn acked(&self, written: Written, ack: AckAt) -> impl Future<Output = Result<(), CommitError>> {
        self.engine
            .await_quorum_ack(&shard(0), written, ack, PATIENCE)
    }

    /// A `Quorum` write that waits on the written count, which makes the
    /// leader ask its followers for reports from then on.
    async fn prime(&self) -> Written {
        let written = self.propose().await;
        let acked = self.acked(written, AckAt::Written).await;
        acked.expect("acknowledged");
        self.engine
            .await_commit(&shard(0), written, PATIENCE)
            .await
            .expect("committed");
        written
    }

    /// Wait until the leader was told `broker` has written `written`.
    async fn told_by(&self, broker: &str, written: Written) {
        let group = self.group();
        let told = || {
            let from = group.reports().from(&member(broker))?;
            (from.term == written.generation && from.index + 1 >= written.end).then_some(())
        };
        eventually(told).await;
    }

    async fn not_acked(&self, written: Written) {
        let acked = tokio::time::timeout(SETTLE, self.acked(written, AckAt::Written)).await;
        assert!(acked.is_err(), "acknowledged: {acked:?}");
    }

    /// Tell the leader `from` has written up to `at`, as a peer would.
    async fn report(&self, from: &str, at: LogPoint) {
        let request = RaftRequest {
            group: group(0),
            to: self.leader.clone(),
            beat: 0,
            report: false,
            deadline: Duration::from_secs(1),
            body: Request::Written {
                from: member(from),
                at,
            },
        };
        let answer = self
            .cluster
            .runtime(&self.leader.broker)
            .handle(request)
            .await;
        assert!(answer.is_ok(), "the report was refused");
    }

    /// Ask `voter` for its vote for `candidate` at the next term, with a
    /// last log id of `last`. The request is a transfer from the leader
    /// `voter` follows, so its lease does not decide the answer and only the
    /// logs are compared.
    async fn ask_vote(&self, voter: &str, candidate: &str, last: LogPoint) -> bool {
        let follower = self.cluster.runtime(voter).group(&shard(0));
        let follower = follower.expect("group");
        let following = follower.raft().metrics().borrow_watched().vote.clone();
        let term = following.leader_id().term + 1;
        let next = LeaderIdOf::<ShardTypes>::new(term, member(candidate));
        let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(next, false);
        let last_log_id = LogIdOf::<ShardTypes> {
            leader_id: CommittedLeaderIdOf::<ShardTypes> { term: last.term },
            index: last.index,
        };
        let mut rpc = VoteRequest::new(vote, Some(last_log_id));
        rpc.leadership_transfer_from = Some(following);
        let request = RaftRequest {
            group: group(0),
            to: member(voter),
            beat: 0,
            report: false,
            deadline: Duration::from_secs(1),
            body: Request::Vote(rpc),
        };
        let answer = self.cluster.runtime(voter).handle(request).await;
        match answer.expect("the vote was refused").0 {
            Reply::Vote(answer) => answer.vote_granted,
            _ => panic!("not a vote"),
        }
    }
}

/// Three voters with a committed record and an engine on the leader.
async fn led() -> Led {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let committed = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, committed).await;
    let leader = cluster.elected(0).await;
    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    Led {
        cluster,
        leader,
        engine,
    }
}

/// Voters a and b with c a learner that is level, and an engine on the
/// leader. Returns the other voter too.
async fn led_with_a_learner() -> (Led, &'static str) {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    let voters = set(&["a", "b"]);
    for broker in ["a", "b"] {
        let runtime = cluster.runtime(broker);
        let formed = runtime.initialize(&shard(0), voters.clone()).await;
        formed.expect("initialize");
    }
    let leads = |broker: &&str| {
        let status = cluster.runtime(broker).status(&shard(0))?;
        (status.role == Role::Leader).then_some(status.member)
    };
    let leader = eventually(|| ["a", "b"].iter().find_map(leads)).await;
    let other = if &*leader.broker == "a" { "b" } else { "a" };
    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    let led = Led {
        cluster,
        leader,
        engine,
    };
    // A leader just elected refuses writes until a majority has answered it.
    let group = led.group();
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let learner = group.raft().add_learner(member("c"), EmptyNode {}, true);
        match learner.await {
            Ok(_) => break,
            Err(err) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(error = %err, "the learner was refused, trying again");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("the learner never joined: {err}"),
        }
    }
    (led, other)
}

/// The acknowledgement comes while no follower has fsynced the record, so
/// nothing has committed it, and it moves neither the commit index nor the
/// read bound. A leader nobody waited on this way asks for no report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_write_is_acknowledged_once_a_majority_has_written_it() {
    let mut led = led().await;
    let others = led.others();
    for broker in &others {
        let unasked = led.group().reports().from(&member(broker));
        assert_eq!(unasked, None, "{broker} reported unasked");
    }
    let before = led.prime().await;
    led.cluster.committed_everywhere(0, before.end - 1).await;

    let logs: Vec<DiskLog> = others
        .iter()
        .map(|broker| led.cluster.logs[&(broker.to_string(), 0)].clone())
        .collect();
    let held = futures::future::join_all(logs.iter().map(|log| log.hold_flushes())).await;
    let written = led.propose().await;
    let acked = tokio::time::timeout(PATIENCE, led.acked(written, AckAt::Written)).await;
    acked.expect("acknowledged in time").expect("acknowledged");

    let index = written.end - 1;
    let leader = led.status(&led.leader.broker);
    assert_eq!(leader.committed, Some(before.end - 1), "it committed");
    assert_eq!(led.engine.read_bound(&shard(0)), Ok(before.end));
    for broker in &others {
        let flushed = led.status(broker).flushed;
        assert!(
            flushed.is_none_or(|at| at.index < index),
            "{broker} fsynced"
        );
    }
    // A stream that fsyncs on commit still waits for the commit.
    let strict = tokio::time::timeout(SETTLE, led.acked(written, AckAt::Flushed)).await;
    assert!(
        strict.is_err(),
        "acknowledged before the commit: {strict:?}"
    );

    drop(held);
    let committed = led.engine.await_commit(&shard(0), written, PATIENCE).await;
    committed.expect("committed");
    led.cluster.stop().await;
}

/// The leader and a learner hold the record, and the learner says so. The
/// other voter does not have it, so nothing is acknowledged until it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_that_has_written_does_not_make_a_majority() {
    let (mut led, other) = led_with_a_learner().await;
    led.prime().await;

    let log = led.cluster.logs[&("c".to_string(), 0)].clone();
    let held = log.hold_flushes().await;
    led.cluster.net.cut.lock().insert(other.into());
    let written = led.propose().await;
    led.told_by("c", written).await;
    led.not_acked(written).await;

    led.cluster.net.cut.lock().clear();
    drop(held);
    let acked = led.acked(written, AckAt::Written).await;
    acked.expect("acknowledged");
    led.cluster.stop().await;
}

/// The membership is joint: the old voters are all three, the new one is
/// the leader alone. The new half has written the record and the old half
/// has not, so nothing is acknowledged. openraft goes through a joint
/// membership only when a majority of the old voters could miss every
/// majority of the new ones, which shrinking three voters to one does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_majority_of_one_half_of_a_joint_membership_is_not_enough() {
    let mut led = led().await;
    led.prime().await;
    let others = led.others();

    // The joint entry cannot commit without one more of the old voters.
    led.cluster
        .net
        .cut
        .lock()
        .extend(others.iter().map(|broker| broker.to_string()));
    let raft = led.group().raft().clone();
    let new = crate::raft::types::MembershipEdit::voters(
        [led.leader.clone()].into(),
        TargetVersion::default(),
    );
    let mut change = tokio::spawn(async move { raft.change_membership(new, true).await });
    let group = led.group();
    let joint = || {
        let metrics = group.raft().metrics();
        let metrics = metrics.borrow_watched();
        let sets = metrics.membership_config.membership().get_joint_config();
        (sets.len() == 2).then_some(())
    };
    tokio::select! {
        ended = &mut change => panic!("the change ended before it was joint: {ended:?}"),
        () = eventually(joint) => {}
    }

    let written = led.propose().await;
    led.not_acked(written).await;

    led.cluster.net.cut.lock().clear();
    let acked = led.acked(written, AckAt::Written).await;
    acked.expect("acknowledged");
    let changed = tokio::time::timeout(PATIENCE, change).await;
    changed
        .expect("in time")
        .expect("ran")
        .expect("the change committed");
    led.cluster.stop().await;
}

/// Reports whose log id is at or past the record but carries another term
/// do not count, from an older term or a newer one. One at the leader's own
/// term does. Before any report, silence counts for nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_report_from_another_term_does_not_make_a_majority() {
    let mut led = led().await;
    led.prime().await;
    let others = led.others();
    // The leader reaches no follower, so the only reports are the ones
    // handed to it here.
    led.cluster
        .net
        .cut
        .lock()
        .extend(others.iter().map(|broker| broker.to_string()));
    let written = led.propose().await;
    let (term, index) = (written.generation, written.end - 1);
    led.not_acked(written).await;

    led.report(others[0], at(term - 1, index + 9)).await;
    led.not_acked(written).await;
    led.report(others[1], at(term + 1, index + 9)).await;
    led.not_acked(written).await;
    led.report(others[0], at(term, index - 1)).await;
    led.not_acked(written).await;

    led.report(others[0], at(term, index)).await;
    let acked = tokio::time::timeout(PATIENCE, led.acked(written, AckAt::Written)).await;
    acked.expect("acknowledged in time").expect("acknowledged");
    let leader = led.status(&led.leader.broker);
    assert!(leader.committed < Some(index), "it committed");

    led.cluster.net.cut.lock().clear();
    led.cluster.stop().await;
}

/// A leader deposed before the count is reached is refused, reports or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_leader_acknowledges_nothing_on_reports() {
    let mut led = led().await;
    led.prime().await;
    let others = led.others();
    led.cluster
        .net
        .cut
        .lock()
        .insert(led.leader.broker.to_string());
    led.cluster
        .net
        .muted
        .lock()
        .insert(led.leader.broker.to_string());
    let written = led.propose().await;
    let (term, index) = (written.generation, written.end - 1);
    // The others elect, and the old leader hears of it once it is reachable.
    let elected = || {
        let next = others.iter().map(|broker| led.status(broker));
        let next = next.filter(|status| status.role == Role::Leader && status.term > term);
        next.map(|status| status.term).next()
    };
    eventually(elected).await;
    led.cluster.net.cut.lock().clear();
    led.cluster.net.muted.lock().clear();
    eventually(|| (led.status(&led.leader.broker).role != Role::Leader).then_some(())).await;

    led.report(others[0], at(term, index)).await;
    let acked = tokio::time::timeout(PATIENCE, led.acked(written, AckAt::Written)).await;
    let refused = acked.expect("answered in time");
    assert!(
        matches!(refused, Err(CommitError::LeadershipLost { .. })),
        "{refused:?}"
    );
    led.cluster.stop().await;
}

/// A follower that has written a record but not fsynced it refuses its vote
/// to a candidate whose log lacks the record. openraft compares the logs it
/// has accepted, written or not. Were it to compare fsynced logs only, a
/// record acknowledged on a written majority could be cut by a leader that
/// never had it, with no crash (`docs/formal/FelixRaftWritten.tla`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_has_written_a_record_refuses_a_candidate_without_it() {
    let mut led = led().await;
    let before = led.prime().await;
    let others = led.others();
    let (voter, candidate) = (others[0], others[1]);

    let log = led.cluster.logs[&(voter.to_string(), 0)].clone();
    let held = log.hold_flushes().await;
    let written = led.propose().await;
    led.told_by(voter, written).await;
    let (term, index) = (written.generation, written.end - 1);
    assert_eq!(
        before.end - 1,
        index - 1,
        "the record follows the primed one"
    );
    let flushed = led.status(voter).flushed;
    assert!(flushed.is_none_or(|at| at.index < index), "{voter} fsynced");

    let lacking = led.ask_vote(voter, candidate, at(term, index - 1)).await;
    assert!(
        !lacking,
        "{voter} voted for a log without its written record"
    );
    let holding = led.ask_vote(voter, candidate, at(term, index)).await;
    assert!(holding, "{voter} refused a log that holds the record");

    drop(held);
    led.cluster.stop().await;
}
