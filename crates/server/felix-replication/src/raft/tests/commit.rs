//! The commit index as the state machine publishes it: the read bound, a
//! write's wait for its commit, the read-index round, and when a shard
//! opens and closes.

use std::sync::atomic::Ordering::Relaxed;

use super::super::commit::{Commits, TermAt};
use super::*;
use crate::engine::{CommitError, EngineHost, Written};
use crate::quorum::Unreadable;

/// What a runtime told its host, in order.
#[derive(Default)]
struct Told(parking_lot::Mutex<Vec<(&'static str, u64)>>);

impl Told {
    fn all(&self) -> Vec<(&'static str, u64)> {
        self.0.lock().clone()
    }
}

impl EngineHost for Told {
    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        None
    }

    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn quiesced(&self, _key: &ShardKey) -> bool {
        true
    }

    fn opened(&self, _key: &ShardKey, generation: u64) {
        self.0.lock().push(("opened", generation));
    }

    fn deposed(&self, _key: &ShardKey, generation: u64) {
        self.0.lock().push(("deposed", generation));
    }
}

/// Three brokers, each reporting to its own host.
fn hosted() -> (Cluster, HashMap<&'static str, Arc<Told>>) {
    let cluster = Cluster::new(true);
    let hosts = BROKERS.map(|broker| {
        let told = Arc::new(Told::default());
        cluster.runtime(broker).set_host(told.clone());
        (broker, told)
    });
    (cluster, hosts.into_iter().collect())
}

pub(super) struct Led {
    pub(super) cluster: Cluster,
    hosts: HashMap<&'static str, Arc<Told>>,
    pub(super) leader: MemberId,
    pub(super) engine: RaftEngine,
    /// The leader's term, and the last committed index.
    pub(super) term: u64,
    pub(super) committed: u64,
}

/// A formed group with one committed record, and an engine on its leader.
pub(super) async fn led() -> Led {
    let (mut cluster, hosts) = hosted();
    cluster.form(0).await;
    let committed = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, committed).await;
    let leader = cluster.elected(0).await;
    let runtime = cluster.runtime(&leader.broker);
    let term = runtime.status(&shard(0)).expect("status").term;
    let engine = RaftEngine::new(runtime);
    eventually(|| (engine.read_bound(&shard(0)) == Ok(committed + 1)).then_some(())).await;
    Led {
        cluster,
        hosts,
        leader,
        engine,
        term,
        committed,
    }
}

impl Led {
    fn told(&self) -> Vec<(&'static str, u64)> {
        self.hosts[&*self.leader.broker].all()
    }

    pub(super) fn status(&self) -> GroupStatus {
        let status = self.cluster.runtime(&self.leader.broker).status(&shard(0));
        status.expect("status")
    }

    /// One past the commit index as `broker`'s Raft core holds it now.
    /// `status().committed` cannot stand in for it next to a read bound:
    /// the core publishes its metrics once per loop, so the state machine
    /// can apply entries, and move the bound, before they show as committed.
    pub(super) async fn committed_on(&self, broker: &str) -> u64 {
        let group = self.cluster.runtime(broker).group(&shard(0));
        let core = group.expect("group").raft().clone();
        let committed = core.with_raft_state(|state| state.local_committed().map(|id| id.index));
        let committed = tokio::time::timeout(PATIENCE, committed).await;
        let committed = committed.expect("in time").expect("the core answers");
        committed.map_or(0, |index| index + 1)
    }

    pub(super) fn followers(&self) -> impl Iterator<Item = String> + '_ {
        let others = BROKERS
            .iter()
            .filter(|broker| **broker != &*self.leader.broker);
        others.map(|broker| broker.to_string())
    }

    /// The leader reaches no follower. The followers still reach it.
    pub(super) fn cut_followers(&self) {
        self.cluster.net.cut.lock().extend(self.followers());
    }

    pub(super) fn heal(&self) {
        self.cluster.net.cut.lock().clear();
        self.cluster.net.muted.lock().clear();
    }

    /// Write `records` records on the leader and say where they landed.
    pub(super) async fn propose(&self, records: usize) -> Written {
        let record = Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"proposed"),
        };
        let batch = Batch {
            records: vec![record; records],
            ack: AckAt::Flushed,
            term: None,
        };
        let written = self.engine.propose(&shard(0), batch).await;
        written.expect("written")
    }
}

pub(super) fn lost(result: &Result<(), CommitError>) -> &'static str {
    match result {
        Err(CommitError::LeadershipLost { detail, .. }) => detail,
        other => panic!("not an unknown outcome: {other:?}"),
    }
}

fn at(term: u64, index: u64) -> LogPoint {
    LogPoint { term, index }
}

/// **The run of entries holding no record just below a record is counted,
/// and only that run.** A leader's blank entry and a membership change in a
/// row are one run of two; one further down does not count.
#[test]
fn the_commit_table_counts_the_entries_just_below_a_record_that_hold_none() {
    let mut commits = Commits::default();
    for (index, record) in [(0, false), (1, false), (2, true), (3, false), (4, false)] {
        commits.advance(at(1, index));
        if !record {
            commits.holds_no_record(index);
        }
    }
    commits.advance(at(1, 5));
    assert_eq!(commits.skipped_before(2), 2);
    assert_eq!(commits.skipped_before(5), 2);
    assert_eq!(commits.skipped_before(3), 0);
    assert_eq!(commits.skipped_before(6), 0);
}

#[test]
fn the_commit_table_knows_the_term_at_every_applied_index() {
    let mut commits = Commits::default();
    assert_eq!(commits.read_bound(), 0);
    assert_eq!(commits.term_at(0), TermAt::Pending);
    for point in [at(1, 0), at(1, 1), at(2, 2), at(2, 3), at(4, 4)] {
        commits.advance(point);
    }
    assert_eq!(commits.read_bound(), 5);
    let terms: Vec<_> = (0..6).map(|index| commits.term_at(index)).collect();
    let term = TermAt::Term;
    assert_eq!(
        terms,
        [term(1), term(1), term(2), term(2), term(4), TermAt::Pending]
    );
    // An entry applied twice moves nothing.
    commits.advance(at(9, 3));
    assert_eq!(commits.term_at(3), term(2));
    assert_eq!(commits.applied(), Some(at(4, 4)));

    // Only the last few changes of term are kept.
    for step in 0..8 {
        commits.advance(at(10 + step, 5 + step));
    }
    assert_eq!(commits.term_at(4), TermAt::Forgotten);
    assert_eq!(commits.term_at(5), term(10));
    assert_eq!(commits.term_at(12), term(17));

    // A member that resumes knows the term of its last applied entry only.
    let resumed = Commits::resumed(Some(at(3, 10)));
    assert_eq!(resumed.read_bound(), 11);
    assert_eq!(resumed.term_at(9), TermAt::Forgotten);
    assert_eq!(resumed.term_at(10), term(3));
}

/// A write waits until a majority holds it, and no longer than it is given.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_waits_for_its_commit() {
    let mut led = led().await;
    let written = led.propose(2).await;
    let committed = led.engine.await_commit(&shard(0), written, PATIENCE);
    assert_eq!(committed.await, Ok(()));

    led.cut_followers();
    let unheard = led.propose(1).await;
    let brief = Duration::from_millis(200);
    assert_eq!(
        led.engine.await_commit(&shard(0), unheard, brief).await,
        Err(CommitError::TimedOut {
            what: "batch",
            timeout: brief
        })
    );
    led.heal();
    let committed = led.engine.await_commit(&shard(0), unheard, PATIENCE);
    assert_eq!(committed.await, Ok(()));
    led.cluster.stop().await;
}

/// A leader writes a record no follower gets, and is deposed. The next
/// leader's entry takes the record's index and commits. The commit index
/// passing the index must not acknowledge the first leader's record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_whose_index_commits_under_another_term_is_refused() {
    let mut led = led().await;
    led.cut_followers();
    let written = led.propose(1).await;
    assert_eq!(
        written,
        Written {
            end: led.committed + 2,
            generation: led.term
        }
    );
    let waiting = led.engine.await_commit(&shard(0), written, PATIENCE);
    let waiting = tokio::spawn(waiting);

    // Nothing the old leader sends is heard, and it hears nothing.
    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old.clone()]);
    let theirs = led.cluster.write(0, b"theirs").await;
    assert!(theirs > led.committed + 1, "the index was not taken");
    let next = led.cluster.elected(0).await;
    assert_ne!(next, led.leader);

    led.heal();
    let bound = theirs + 1;
    eventually(|| (led.engine.read_bound(&shard(0)).ok()? >= bound).then_some(())).await;
    let after = led.engine.await_commit(&shard(0), written, PATIENCE).await;
    assert_eq!(
        lost(&after),
        "another leader's entry committed at the offset"
    );
    // The wait that began before the change is an unknown outcome too.
    lost(&waiting.await.expect("the wait"));

    // The old leader closed its term, and the new one opened its own.
    let closed = [("opened", led.term), ("deposed", led.term)];
    eventually(|| (led.told() == closed).then_some(())).await;
    let term = led.cluster.runtime(&next.broker).status(&shard(0));
    let term = term.expect("status").term;
    eventually(|| (led.hosts[&*next.broker].all() == [("opened", term)]).then_some(())).await;
    led.cluster.stop().await;
}

/// Whatever the leader has written and fsynced, a reader is shown nothing
/// past what a majority holds, on the leader or on a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_read_bound_never_passes_the_commit_index() {
    let mut led = led().await;
    let leader = led.leader.broker.to_string();
    let follower = led.followers().next().expect("a follower");
    let on_follower = RaftEngine::new(led.cluster.runtime(&follower));
    let held = led.committed + 1;
    // Read the bound first: the commit index only grows.
    let within = async |engine: &RaftEngine, broker: &str| {
        let bound = engine.read_bound(&shard(0)).expect("bound");
        let committed = led.committed_on(broker).await;
        assert!(bound <= committed, "bound {bound}, committed {committed}");
        bound
    };

    led.cut_followers();
    let mut end = held;
    for round in 0..6 {
        end = led.propose(round % 3 + 1).await.end;
        assert_eq!(within(&led.engine, &leader).await, held);
        let flushed = led.status().flushed.expect("flushed");
        assert_eq!(flushed.index + 1, end, "the leader holds it on disk");
    }
    assert_eq!(led.engine.read_bound(&shard(0)), Ok(held));

    led.heal();
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while within(&led.engine, &leader).await < end || within(&on_follower, &follower).await < end {
        assert!(tokio::time::Instant::now() < deadline, "never committed");
        tokio::task::yield_now().await;
    }
    assert_eq!(led.engine.read_bound(&shard(0)), Ok(end));

    let elsewhere = led.engine.read_bound(&shard(7));
    assert_eq!(elsewhere, Err(Unreadable::NotServed));
    led.cluster.stop().await;
}

/// A member that wins an election has not opened: it opens when the blank
/// entry of its term is committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_opens_only_once_its_blank_entry_commits() {
    let (mut cluster, hosts) = hosted();
    cluster.open(0).await;
    cluster.net.lose_appends.store(true, Relaxed);
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members())
            .await
            .expect("initialize");
    }
    eventually(|| leads(&cluster)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let opened: Vec<_> = hosts.values().flat_map(|told| told.all()).collect();
    assert_eq!(opened, [], "opened before any entry of the term committed");

    cluster.net.lose_appends.store(false, Relaxed);
    // A follower that has heard no append may start an election before the
    // first one reaches it, so the member that opens can be another one, or
    // the same one at a later term.
    opens_at_its_term(&cluster, &hosts).await;
    cluster.stop().await;
}

/// While appends are lost, a follower hears from no leader, so its election
/// timer runs out and it can win a later term. No leader opens while it
/// commits nothing, and the one that leads once appends flow opens alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leaders_that_commit_nothing_open_nothing() {
    let (mut cluster, hosts) = hosted();
    cluster.open(0).await;
    cluster.net.lose_appends.store(true, Relaxed);
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members())
            .await
            .expect("initialize");
    }
    let elected = eventually(|| leads(&cluster)).await;
    let later = |cluster: &Cluster| {
        let statuses = cluster.statuses(0).into_iter();
        let mut later = statuses.filter(|status| status.term > elected.term);
        later.find(|status| status.role == Role::Leader)
    };
    eventually(|| later(&cluster)).await;
    let opened: Vec<_> = hosts.values().flat_map(|told| told.all()).collect();
    assert_eq!(opened, [], "opened before any entry of the term committed");

    cluster.net.lose_appends.store(false, Relaxed);
    opens_at_its_term(&cluster, &hosts).await;
    cluster.stop().await;
}

/// The member of group 0 that leads the highest term now. Two can both
/// say they lead while appends are lost.
fn leads(cluster: &Cluster) -> Option<GroupStatus> {
    let statuses = cluster.statuses(0).into_iter();
    statuses
        .filter(|status| status.role == Role::Leader)
        .max_by_key(|status| status.term)
}

/// Wait for the leader every member follows to open at its own term with
/// something committed and every other host to be closed, then check that
/// every host was told of its terms in order: each term opened at most once,
/// and closed before a later one opened.
async fn opens_at_its_term(cluster: &Cluster, hosts: &HashMap<&'static str, Arc<Told>>) {
    let opened = || {
        let leader = cluster.leader(0)?;
        let status = cluster.runtime(&leader.broker).status(&shard(0))?;
        let told = hosts[&*leader.broker].all();
        (told.last() == Some(&("opened", status.term))).then_some(status)
    };
    let status = eventually(opened).await;
    assert!(status.committed.is_some(), "opened with nothing committed");
    let others_closed = || {
        let mut others = hosts
            .iter()
            .filter(|(broker, _)| **broker != &*status.member.broker);
        let open = |told: &Told| told.all().last().is_some_and(|(what, _)| *what == "opened");
        others.all(|(_, told)| !open(told)).then_some(())
    };
    eventually(others_closed).await;
    for (broker, told) in hosts {
        let told = told.all();
        let mut open = None;
        let mut last = 0;
        for &(what, term) in &told {
            match (what, open) {
                ("opened", None) if term > last => open = Some(term),
                ("deposed", Some(at)) if term == at => open = None,
                _ => panic!("{broker} was told out of order: {told:?}"),
            }
            last = term;
        }
    }
}

/// A transfer closes the shard before it is asked for, and the term does
/// not open again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_about_to_start_closes_the_shard() {
    let mut led = led().await;
    led.cut_followers();
    let written = led.propose(1).await;
    let waiting = led.engine.await_commit(&shard(0), written, PATIENCE);
    let waiting = tokio::spawn(waiting);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        led.engine.transfer_starting(&shard(0)).await,
        Some(led.term)
    );
    assert_eq!(led.told(), [("opened", led.term), ("deposed", led.term)]);
    assert_eq!(
        lost(&waiting.await.expect("the wait")),
        "this broker stopped leading the shard"
    );
    assert_eq!(led.engine.transfer_starting(&shard(7)).await, None);
    assert_eq!(led.engine.open_term(&shard(0)), None);

    led.heal();
    led.cluster.committed_everywhere(0, written.end - 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(led.status().role, Role::Leader);
    assert_eq!(led.told(), [("opened", led.term), ("deposed", led.term)]);
    led.cluster.stop().await;
}

/// A log that fails stops the group, which closes the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_that_stops_on_a_storage_error_closes_the_shard() {
    let mut led = led().await;
    let log = led.cluster.logs.remove(&(led.leader.broker.to_string(), 0));
    log.expect("log").close().await.expect("close");
    let batch = Batch {
        records: vec![Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"unwritten"),
        }],
        ack: AckAt::Written,
        term: None,
    };
    let refused = led.engine.propose(&shard(0), batch).await;
    assert!(refused.is_err(), "written to a closed log: {refused:?}");
    eventually(|| (led.status().role == Role::Stopped).then_some(())).await;
    let closed = [("opened", led.term), ("deposed", led.term)];
    eventually(|| (led.told() == closed).then_some(())).await;
    led.cluster.stop().await;
}

/// A leader no majority answers refuses writes and cannot confirm a read.
/// It is still the leader, so neither closes the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lapsed_lease_and_an_unconfirmed_read_do_not_close_the_shard() {
    let mut led = led().await;
    assert_eq!(led.engine.open_term(&shard(0)), Some(led.term));
    assert_eq!(led.engine.confirm_read(&shard(0), led.term).await, Ok(()));
    let follower = led.followers().next().expect("a follower");
    let follower = RaftEngine::new(led.cluster.runtime(&follower));
    assert_eq!(
        lost(&follower.confirm_read(&shard(0), led.term).await),
        "this broker does not have the shard open"
    );

    led.cluster.net.cut.lock().extend(BROKERS.map(String::from));
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    let batch = Batch {
        records: vec![Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"unheard"),
        }],
        ack: AckAt::Written,
        term: Some(led.term),
    };
    let refused = led.engine.propose(&shard(0), batch).await;
    assert_eq!(
        lost(&refused.map(|_| ())),
        "no majority has answered this leader lately, so nothing was written"
    );
    assert!(led.engine.admit_local_ack(&shard(0), led.term).is_err());
    let unconfirmed = led.engine.confirm_read(&shard(0), led.term).await;
    assert!(
        matches!(unconfirmed, Err(CommitError::TimedOut { what: "read", .. })),
        "{unconfirmed:?}"
    );
    assert_eq!(led.status().role, Role::Leader);
    assert_eq!(led.told(), [("opened", led.term)]);

    led.heal();
    led.cluster.stop().await;
}

/// A reader takes the open term, then its value. The broker is deposed,
/// another leader commits a record the value does not have, and the broker
/// is elected and opens again. The read must not be confirmed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_from_before_the_shard_was_opened_again_is_not_confirmed() {
    let mut led = led().await;
    let read_in = led.engine.open_term(&shard(0)).expect("open");

    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old.clone()]);
    let missed = led.cluster.write(0, b"missed by the read").await;
    led.heal();
    eventually(|| (led.engine.read_bound(&shard(0)).ok()? > missed).then_some(())).await;

    // Stand until this broker has the shard open again, in a later term.
    let member = led.cluster.runtime(&old).group(&shard(0)).expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let again = loop {
        match led.engine.open_term(&shard(0)) {
            Some(term) if term > read_in => break term,
            _ => assert!(tokio::time::Instant::now() < deadline, "never led again"),
        }
        member.raft().trigger().elect(false).await.expect("elect");
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    assert_eq!(
        lost(&led.engine.confirm_read(&shard(0), read_in).await),
        "the shard was opened again after the read began"
    );
    // A read that begins now is confirmed, unless the term moved again.
    if led.engine.open_term(&shard(0)) == Some(again) {
        let confirmed = led.engine.confirm_read(&shard(0), again).await;
        assert!(
            confirmed.is_ok() || led.engine.open_term(&shard(0)) != Some(again),
            "{confirmed:?}"
        );
    }
    led.cluster.stop().await;
}

/// A transfer that starts before the term opened closes it for good: the
/// term's commits open nothing, and the watcher does not wait on them. It
/// was waiting on the commit table when the transfer started, and a wait
/// for a term that cannot open is answered at once, every time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_term_closed_before_it_opened_is_not_waited_on() {
    let (mut cluster, hosts) = hosted();
    cluster.open(0).await;
    cluster.net.lose_appends.store(true, Relaxed);
    for runtime in cluster.runtimes() {
        runtime
            .initialize(&shard(0), members())
            .await
            .expect("initialize");
    }
    let leads = |cluster: &Cluster| {
        let statuses = cluster.statuses(0);
        let mut leaders = statuses
            .into_iter()
            .filter(|status| status.role == Role::Leader);
        leaders.next()
    };
    let elected = eventually(|| leads(&cluster)).await;
    let runtime = cluster.runtime(&elected.member.broker);
    let engine = RaftEngine::new(runtime.clone());
    let closed = engine.transfer_starting(&shard(0)).await;
    assert_eq!(closed, Some(elected.term));

    cluster.net.lose_appends.store(false, Relaxed);
    let applied = |term: u64| {
        let status = runtime.status(&shard(0))?;
        let bound = engine.read_bound(&shard(0)).ok()?;
        (bound > 1 && status.term == term).then_some(status)
    };
    eventually(|| applied(elected.term)).await;
    let member = runtime.group(&shard(0)).expect("group");
    let before = member.watcher_looks();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let looks = member.watcher_looks() - before;
    assert!(looks < 20, "the watcher took {looks} looks in 300 ms");
    let status = runtime.status(&shard(0)).expect("status");
    assert_eq!((status.role, status.term), (Role::Leader, elected.term));
    assert_eq!(hosts[&*elected.member.broker].all(), []);
    assert_eq!(engine.open_term(&shard(0)), None);
    cluster.stop().await;
}
