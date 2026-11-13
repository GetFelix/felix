//! The early acknowledgement of a `Quorum` write on a stream that does not
//! fsync on commit: a majority of the voters has written the record.

use felix_storage::raft_log::LogPoint;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::impls::EmptyNode;

use super::super::group::Group;
use super::super::written::{Reports, majority_wrote};
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
        };
        let batch = Batch {
            records: vec![record],
            ack: AckAt::Written,
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
    let new: BTreeSet<MemberId> = [led.leader.clone()].into();
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
