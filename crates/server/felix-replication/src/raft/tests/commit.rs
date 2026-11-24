//! The commit index as the state machine publishes it: the read bound, a
//! write's wait for its commit, the read-index round, and when a shard
//! opens and closes.

use std::sync::atomic::Ordering::Relaxed;

use felix_storage::waiters::Queue;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::super::commit::{Commits, Holds, TermAt};
use super::super::propose::{Leading, Lost};
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
    hosted_with(timers())
}

fn hosted_with(timers: RuntimeConfig) -> (Cluster, HashMap<&'static str, Arc<Told>>) {
    let cluster = Cluster::with(timers, true);
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
    led_with(timers()).await
}

pub(super) async fn led_with(timers: RuntimeConfig) -> Led {
    let (mut cluster, hosts) = hosted_with(timers);
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
            headers: None,
        };
        let batch = Batch {
            expected: None,
            trim: None,
            commit: false,
            records: vec![record; records],
            ack: AckAt::Flushed,
            term: None,
            producer: None,
            publisher: None,
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

/// **A publisher's claim keeps its record's skip count through any number
/// of later runs, and once no claim is left the counts go.** Each run here
/// holds one group entry and one record, far more runs than a short history
/// keeps; a count dropped before its publisher asks reads as a drop.
#[test]
fn a_claimed_skip_count_outlives_any_number_of_runs() {
    use super::super::commit::Skips;
    let mut commits = Commits::default();
    let (published, watched) = tokio::sync::watch::channel(commits.clone());
    let skips = Skips::default();
    let (first, second, third) = (
        skips.claim(&watched),
        skips.claim(&watched),
        skips.claim(&watched),
    );
    let mut bound = 0;
    for run in 0..200 {
        let record = 2 * run + 1;
        skips.applied(bound, &[(record, 1)], || {
            commits.advance(at(1, record));
            published.send_replace(commits.clone());
            true
        });
        bound = record + 1;
    }
    assert_eq!(first.skipped_before(1), 1);
    assert_eq!(second.skipped_before(399), 1);
    assert_eq!(third.skipped_before(2), 0);

    // Every claim is released, asked or dropped alike.
    skips.applied(bound, &[], || false);
    let late = skips.claim(&watched);
    assert_eq!(late.skipped_before(1), 0, "no claim was left to ask");
    // A claim taken now holds from the published bound on.
    let claim = skips.claim(&watched);
    skips.applied(bound, &[(401, 1)], || false);
    skips.applied(bound + 10, &[], || false);
    assert_eq!(claim.skipped_before(401), 1);
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

/// Whether the member leads, at which term, as a test says.
struct Leads(tokio::sync::watch::Receiver<(bool, u64)>);

impl Leading for Leads {
    async fn lost(&mut self, term: Option<u64>) -> Lost {
        let lost = |&(leads, at): &(bool, u64)| !leads || term.is_some_and(|term| term != at);
        let _ = self.0.wait_for(lost).await;
        Lost::Deposed
    }
}

type Wait = std::pin::Pin<Box<dyn Future<Output = Result<(), CommitError>> + Send>>;

/// The state machine's side of commit waits, without a group.
struct Applying {
    commits: Commits,
    published: tokio::sync::watch::Sender<Commits>,
    queue: Queue<u64>,
    leads: tokio::sync::watch::Sender<(bool, u64)>,
}

impl Applying {
    fn new() -> Self {
        Self {
            commits: Commits::default(),
            published: tokio::sync::watch::Sender::default(),
            queue: Queue::new(),
            leads: tokio::sync::watch::Sender::new((true, 2)),
        }
    }

    /// A write at `index` in term 2, waiting for its commit.
    fn wait(&self, index: u64) -> Wait {
        let written = Written {
            end: index + 1,
            generation: 2,
        };
        Box::pin(super::super::commit::committed(
            written,
            self.published.subscribe(),
            self.queue.waiters(),
            Leads(self.leads.subscribe()),
            CancellationToken::new(),
            Duration::from_secs(60),
        ))
    }

    /// One run of entries applied, published once as the machine does.
    fn apply(&mut self, run: &[LogPoint]) {
        for point in run {
            self.commits.advance(*point);
        }
        self.published.send_replace(self.commits.clone());
        let applied = self.commits.applied().map(|at| at.index);
        self.queue.advance(applied);
    }
}

/// **A commit wait is answered by the apply that reaches its index, in
/// index order, and not before.** The term check still applies: an index
/// another term committed is refused.
#[tokio::test(flavor = "current_thread")]
async fn a_commit_wait_is_answered_by_the_apply_that_reaches_it() {
    let mut machine = Applying::new();
    let mut waits: Vec<Wait> = (0..4).map(|index| machine.wait(index)).collect();
    for wait in &mut waits {
        assert!(wait.as_mut().now_or_never().is_none());
    }
    machine.apply(&[at(2, 0), at(2, 1)]);
    assert_eq!(waits[0].as_mut().now_or_never(), Some(Ok(())));
    assert_eq!(waits[1].as_mut().now_or_never(), Some(Ok(())));
    assert!(waits[2].as_mut().now_or_never().is_none());
    assert!(waits[3].as_mut().now_or_never().is_none());
    // Only the waits not yet answered are queued.
    assert_eq!(machine.queue.waiters().queued(), 2);
    machine.apply(&[at(3, 2), at(3, 3)]);
    let refused = waits[2].as_mut().now_or_never().expect("answered");
    assert_eq!(
        lost(&refused),
        "another leader's entry committed at the offset"
    );
    let refused = waits[3].as_mut().now_or_never().expect("answered");
    assert_eq!(
        lost(&refused),
        "another leader's entry committed at the offset"
    );
}

/// **A commit wait ends when its term is lost, with nothing applied.** The
/// queue never wakes it; the leadership check does.
#[tokio::test(flavor = "current_thread")]
async fn a_commit_wait_ends_on_a_lost_term_with_nothing_applied() {
    let machine = Applying::new();
    let mut wait = machine.wait(5);
    assert!(wait.as_mut().now_or_never().is_none());
    machine.leads.send_replace((true, 3));
    let told = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("woken by the lost term");
    assert_eq!(lost(&told), "this broker stopped leading the shard");
}

/// **A commit wait ends as stopped when the state machine is gone.**
#[tokio::test(flavor = "current_thread")]
async fn a_commit_wait_ends_when_the_state_machine_is_dropped() {
    let machine = Applying::new();
    let mut wait = machine.wait(5);
    assert!(wait.as_mut().now_or_never().is_none());
    let Applying {
        published,
        queue,
        leads,
        ..
    } = machine;
    drop((published, queue));
    let told = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("woken by the close");
    let stopped = Err(super::super::propose::stopped());
    assert_eq!(lost(&told), lost(&stopped));
    drop(leads);
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

/// **A bound past entries that hold no record ends, for the ring, at the
/// last record.** A membership change after the last record, then a new
/// term's blank: the ring stays at the record until the next publish, so a
/// state read waits only for that.
#[test]
fn records_end_below_a_trailing_membership_change_and_term_blank() {
    let mut commits = Commits::default();
    commits.advance(at(1, 0));
    commits.holds_no_record(0);
    commits.advance(at(1, 1));
    assert_eq!(commits.records_end(commits.read_bound()), 2);

    // A membership change in the same term.
    commits.advance(at(1, 2));
    commits.holds_no_record(2);
    assert_eq!(commits.records_end(commits.read_bound()), 2);
    // The next term's blank joins the run, and the older bound is still known.
    commits.advance(at(2, 3));
    commits.holds_no_record(3);
    assert_eq!(commits.records_end(4), 2);
    assert_eq!(commits.records_end(3), 2);

    // A record after the run: the bound is the record's.
    commits.advance(at(2, 4));
    assert_eq!(commits.records_end(5), 5);
    assert_eq!(commits.records_end(4), 2);
}

/// Apply `entries` to `commits` as one run, in order from `from`, and
/// return the skip counts it pushed.
fn apply_run(
    commits: &mut Commits,
    quiet: &mut std::ops::Range<u64>,
    from: u64,
    entries: &[Holds],
) -> Vec<(u64, u64)> {
    let mut skipped = Vec::new();
    for (index, holds) in (from..).zip(entries) {
        commits.apply(at(1, index), *holds, quiet, &mut skipped);
    }
    skipped
}

fn piece(position: u32, len: u32) -> Holds {
    Holds::Piece { position, len }
}

/// **An undecided conditional batch holds the read bound at its first
/// piece, across runs, and a whole one releases it.** The applied index
/// moves with every piece, so a wait on an apply is not held. The blank
/// below the batch is skipped before its first record, as for any record.
#[test]
fn an_undecided_batch_holds_the_read_bound_until_it_is_whole() {
    let mut commits = Commits::default();
    let mut quiet = 0..0;
    let skipped = apply_run(
        &mut commits,
        &mut quiet,
        0,
        &[Holds::Record, Holds::Nothing, piece(0, 3), piece(1, 3)],
    );
    assert!(skipped.is_empty());
    assert_eq!(commits.read_bound(), 2, "held at the first piece");
    assert_eq!(commits.applied_end(), 4);
    assert_eq!(commits.records_end(commits.read_bound()), 1);
    // The last piece lands in a later run.
    let skipped = apply_run(&mut commits, &mut quiet, 4, &[piece(2, 3)]);
    assert_eq!(
        skipped,
        [(2, 1)],
        "the blank at 1 is skipped before the batch"
    );
    assert_eq!(commits.read_bound(), 5);
    assert_eq!(commits.records_end(5), 5);
    let skipped = apply_run(&mut commits, &mut quiet, 5, &[Holds::Record]);
    assert!(
        skipped.is_empty(),
        "nothing between the batch and the record"
    );
}

/// **A batch cut by another entry holds no record, and its pieces count in
/// the next record's skips.** A new leader's blank lands where the third
/// piece belonged: the bound moves past the two pieces, which join the run
/// below the next record with the blank before the batch and the cutter.
#[test]
fn a_cut_batch_holds_no_record_and_its_pieces_are_skipped() {
    let mut commits = Commits::default();
    let mut quiet = 0..0;
    let skipped = apply_run(
        &mut commits,
        &mut quiet,
        0,
        &[Holds::Record, Holds::Nothing, piece(0, 3), piece(1, 3)],
    );
    assert!(skipped.is_empty());
    assert_eq!(commits.read_bound(), 2);
    let skipped = apply_run(&mut commits, &mut quiet, 4, &[Holds::Nothing]);
    assert!(skipped.is_empty());
    assert_eq!(commits.read_bound(), 5, "the cut batch no longer holds it");
    assert_eq!(commits.records_end(5), 1, "the pieces hold no record");
    let skipped = apply_run(&mut commits, &mut quiet, 5, &[Holds::Record]);
    assert_eq!(skipped, [(5, 4)], "the blank, both pieces and the cutter");

    // Cut by the first piece of the next batch, which then completes.
    let skipped = apply_run(
        &mut commits,
        &mut quiet,
        6,
        &[piece(0, 2), piece(0, 2), piece(1, 2), Holds::Record],
    );
    assert_eq!(skipped, [(7, 1)], "the cut piece at 6 is skipped");
    assert_eq!(commits.read_bound(), 10);
}

/// The offsets `resolve` reads as records from `marks`, laid out from `from`.
fn resolved(from: u64, marks: &[Holds]) -> Vec<u64> {
    use felix_storage::log::{CondPiece, LogRecord, RecordMark, conditional};
    let records: Vec<_> = (from..)
        .zip(marks)
        .map(|(offset, holds)| LogRecord {
            offset,
            timestamp_micros: 1,
            checksum: 0,
            payload: Bytes::from_static(b"r"),
            mark: match *holds {
                Holds::Record => RecordMark::None,
                Holds::Piece { position, len } => {
                    RecordMark::CondPiece(CondPiece { position, len })
                }
                Holds::Nothing => RecordMark::GenerationStart,
            },
            publisher: None,
            headers: None,
            generation: Some(1),
        })
        .collect();
    let end = from + marks.len() as u64;
    conditional::resolve(records, end, end)
        .records
        .into_iter()
        .filter(|record| record.mark.is_data())
        .map(|record| record.offset)
        .collect()
}

/// **A member whose log starts inside a batch counts its rest as `resolve`
/// reads it.** After a purge snapshot at a batch's first piece, the member
/// applies the other three: they are records, as a disk read returns them,
/// and nothing is skipped. The bound waits at the first piece it holds,
/// never below the snapshot. Cut, the same rest holds no record on both
/// sides.
#[test]
fn a_batch_applied_from_mid_way_is_counted_as_resolve_reads_it() {
    let whole = [piece(1, 4), piece(2, 4), piece(3, 4), Holds::Record];
    let mut commits = Commits::resumed(Some(at(1, 10)));
    let mut quiet = 0..0;
    let skipped = apply_run(&mut commits, &mut quiet, 11, &whole[..2]);
    assert!(skipped.is_empty());
    assert_eq!(commits.read_bound(), 11, "at the first piece it holds");
    let skipped = apply_run(&mut commits, &mut quiet, 13, &whole[2..]);
    assert!(skipped.is_empty(), "every piece is a record: {skipped:?}");
    assert_eq!(commits.records_end(15), 15);
    assert_eq!(resolved(11, &whole), [11, 12, 13, 14]);

    let cut = [piece(1, 4), piece(2, 4), Holds::Nothing, Holds::Record];
    let mut commits = Commits::resumed(Some(at(1, 10)));
    let mut quiet = 0..0;
    let skipped = apply_run(&mut commits, &mut quiet, 11, &cut);
    assert_eq!(skipped, [(14, 3)]);
    assert_eq!(resolved(11, &cut), [14]);
}

/// **A member that resumes inside a batch reads the open batch back and
/// keeps the bound at its first piece.** Its pieces are then cut or
/// completed as on a member that applied them in one go.
#[test]
fn a_member_resumed_inside_a_batch_keeps_its_bound_there() {
    let batch = super::super::commit::OpenBatch {
        first: 7,
        next: 2,
        len: 3,
    };
    let mut commits = Commits::resumed(Some(at(1, 8))).in_batch(Some(batch));
    assert_eq!(commits.read_bound(), 7);
    assert_eq!(commits.applied_end(), 9);
    let mut quiet = 0..0;
    let mut whole = commits.clone();
    assert!(apply_run(&mut whole, &mut quiet.clone(), 9, &[piece(2, 3)]).is_empty());
    assert_eq!(whole.read_bound(), 10);
    let skipped = apply_run(
        &mut commits,
        &mut quiet,
        9,
        &[Holds::Nothing, Holds::Record],
    );
    assert_eq!(skipped, [(10, 3)]);
    assert_eq!(commits.read_bound(), 11);
}

/// **An idle shard's term blank leaves the records where they were.** A
/// new leader's blank entry moves the read bound, and nothing else follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_term_blank_on_an_idle_shard_ends_no_records() {
    let mut led = led().await;
    let records = led.committed + 1;
    assert_eq!(led.engine.records_end(&shard(0), records), records);

    let follower = led.followers().next().expect("a follower");
    let member = led
        .cluster
        .runtime(&follower)
        .group(&shard(0))
        .expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let bound = loop {
        let bound = led.engine.read_bound(&shard(0)).expect("bound");
        if bound > records {
            break bound;
        }
        assert!(tokio::time::Instant::now() < deadline, "no new term");
        member.raft().trigger().elect(false).await.expect("elect");
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    assert_eq!(led.engine.records_end(&shard(0), bound), records);
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
    // The first leader is cut off for the others to elect a later one. If
    // it stayed reachable it would answer their pre-votes with its own
    // committed vote, and a follower that sees that answer before the other
    // follower's grant drops the round, which can repeat for good.
    cluster
        .net
        .cut
        .lock()
        .insert(elected.member.broker.to_string());
    let later = |cluster: &Cluster| {
        let statuses = cluster.statuses(0).into_iter();
        let mut later = statuses.filter(|status| status.term > elected.term);
        later.find(|status| status.role == Role::Leader)
    };
    eventually(|| later(&cluster)).await;
    let opened: Vec<_> = hosts.values().flat_map(|told| told.all()).collect();
    assert_eq!(opened, [], "opened before any entry of the term committed");

    cluster.net.cut.lock().clear();
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
        expected: None,
        trim: None,
        commit: false,
        records: vec![Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"unwritten"),
            headers: None,
        }],
        ack: AckAt::Written,
        term: None,
        producer: None,
        publisher: None,
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
        expected: None,
        trim: None,
        commit: false,
        records: vec![Record {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"unheard"),
            headers: None,
        }],
        ack: AckAt::Written,
        term: Some(led.term),
        producer: None,
        publisher: None,
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

/// A leader inside its lease passes the read barrier with no read-index
/// round: no successor can have been elected, so no trim it lacks was
/// answered. Cut off past its lease, it runs the round and is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_inside_its_lease_passes_the_read_barrier_with_no_round() {
    let mut led = led().await;
    let runtime = led.cluster.runtime(&led.leader.broker);
    let group = runtime.group(&shard(0)).expect("group");
    let mut leased = false;
    for _ in 0..50 {
        if led.engine.admit_local_ack(&shard(0), led.term).is_ok() {
            assert_eq!(runtime.read_barrier(&shard(0)).await, Ok(()));
            leased = group.lease_reads.load(Relaxed) > 0;
            if leased {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(leased, "no read was served on the lease");

    led.cluster.net.cut.lock().extend(BROKERS.map(String::from));
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    let served = group.lease_reads.load(Relaxed);
    assert!(runtime.read_barrier(&shard(0)).await.is_err());
    assert_eq!(group.lease_reads.load(Relaxed), served);

    led.heal();
    led.cluster.stop().await;
}

/// A write answered on the leader's lease is not committed yet. A read that
/// starts after that answer waits for it: here the answered entry is never
/// written, so the barrier runs out instead of letting the read through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_after_a_lease_answer_waits_for_that_entry() {
    let mut led = led().await;
    let runtime = led.cluster.runtime(&led.leader.broker);
    let group = runtime.group(&shard(0)).expect("group");
    let mut leased = false;
    for _ in 0..50 {
        if led.engine.admit_local_ack(&shard(0), led.term).is_ok() {
            leased = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(leased, "the leader never held its lease");
    assert_eq!(runtime.read_barrier(&shard(0)).await, Ok(()));
    let past = group.read_bound() + 5;
    group
        .admit_local_ack_of(Written {
            end: past,
            generation: led.term,
        })
        .expect("answered on the lease");
    assert!(runtime.read_barrier(&shard(0)).await.is_err());
    assert!(
        led.engine.confirm_read(&shard(0), led.term).await.is_err(),
        "a confirmed read must wait for the answered entry too"
    );
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

/// Only the leader reports a floor, and it is the committed log id.
#[tokio::test]
async fn only_the_leader_reports_its_committed_log_id() {
    let led = led().await;
    let committed = led.committed_on(&led.leader.broker).await - 1;
    for broker in BROKERS {
        let commits = led.cluster.runtime(broker).led_commits();
        if broker == &*led.leader.broker {
            let [(group, at)] = commits.as_slice() else {
                panic!("one led group, got {commits:?}");
            };
            assert_eq!(*group.shard, shard(0));
            assert!(at.index >= committed, "{at:?} below {committed}");
            assert_eq!(at.term, led.term);
        } else {
            assert!(commits.is_empty(), "{broker} leads nothing");
        }
    }
}

mod skipped_counts {
    use openraft::entry::RaftEntry;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::type_config::alias::{EntryOf, LogIdOf};

    use super::super::super::machine::{Machine, Opened};
    use super::*;

    type Run = Vec<std::io::Result<openraft::storage::EntryResponder<ShardTypes>>>;

    async fn machine(dir: &std::path::Path) -> Opened {
        let log = open_log(dir, "m", 0, log_config());
        let snapshots = felix_storage::raft_log::SnapshotStore::open(dir).expect("store");
        Machine::open(snapshots, ShardKind::Stream, Some(&log), None)
            .await
            .expect("open")
    }

    fn id(index: u64) -> LogIdOf<ShardTypes> {
        LogIdOf::<ShardTypes>::new(1.into(), index)
    }

    /// Records at the indexes in `records`, blank entries elsewhere in
    /// `from..to`, as one apply run.
    fn run(
        from: u64,
        to: u64,
        records: &[u64],
    ) -> impl futures::Stream<Item = std::io::Result<openraft::storage::EntryResponder<ShardTypes>>>
    + Unpin {
        let entries: Run = (from..to)
            .map(|index| {
                let entry = if records.contains(&index) {
                    let command = Command {
                        timestamp_micros: index,
                        payload: bytes::Bytes::from_static(b"record"),
                        producer: None,
                        expected: None,
                        trim: None,
                        commit: false,
                        group: false,
                        publisher: None,
                        headers: None,
                        written: None,
                        cond: None,
                    };
                    EntryOf::<ShardTypes>::new_normal(id(index), command)
                } else {
                    EntryOf::<ShardTypes>::new_blank(id(index))
                };
                Ok((entry, None))
            })
            .collect();
        futures::stream::iter(entries)
    }

    /// **A run of entries holding no record that straddles apply runs is
    /// counted whole for the record above it.** The count rides the
    /// machine, not one `apply` call, so openraft cutting a run anywhere
    /// does not turn skipped offsets into a drop.
    #[tokio::test]
    async fn a_quiet_run_across_apply_runs_counts_whole() {
        let dir = tempfile::tempdir().expect("dir");
        let Opened {
            mut machine,
            commits,
            skips,
            ..
        } = machine(dir.path()).await;
        let claim = skips.claim(&commits);
        machine.apply(run(1, 2, &[1])).await.expect("apply");
        machine.apply(run(2, 60, &[])).await.expect("apply");
        machine.apply(run(60, 101, &[])).await.expect("apply");
        machine.apply(run(101, 102, &[101])).await.expect("apply");
        assert_eq!(claim.skipped_before(101), 99);
    }

    /// **A member that installs a snapshot counts the quiet entries it
    /// applies after it, but not those below it.** The count is kept in
    /// memory only, so the first record above a snapshot cut reads short by
    /// the blank entries just under the cut. Documented in
    /// `docs/per-shard-raft.md`.
    #[tokio::test]
    async fn a_snapshot_install_forgets_the_quiet_run_below_its_cut() {
        let sender_dir = tempfile::tempdir().expect("dir");
        let mut sender = machine(sender_dir.path()).await.machine;
        sender.apply(run(1, 51, &[1])).await.expect("apply");
        let built = sender
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build");

        let dir = tempfile::tempdir().expect("dir");
        let Opened {
            mut machine,
            commits,
            skips,
            ..
        } = machine(dir.path()).await;
        machine
            .install_snapshot(&built.meta, built.snapshot)
            .await
            .expect("install");
        let (early, late) = (skips.claim(&commits), skips.claim(&commits));
        machine.apply(run(51, 52, &[51])).await.expect("apply");
        machine.apply(run(52, 62, &[61])).await.expect("apply");
        assert_eq!(
            early.skipped_before(51),
            0,
            "the 49 below the cut are unknown"
        );
        assert_eq!(
            late.skipped_before(61),
            9,
            "the quiet run above it is counted"
        );
    }
}

/// A stream shard's state machine over the log and snapshot store in `dir`.
async fn stream_machine(dir: &std::path::Path) -> super::super::machine::Opened {
    let log = open_log(dir, "m", 0, log_config());
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir).expect("store");
    super::super::machine::Machine::open(snapshots, ShardKind::Stream, Some(&log), None)
        .await
        .expect("open")
}

/// Entries of a stream shard's log: a record, a conditional piece or a
/// leader's blank, at `(term, index)`.
fn entries(
    of: &[((u64, u64), Holds)],
) -> impl futures::Stream<Item = std::io::Result<openraft::storage::EntryResponder<ShardTypes>>> + Unpin
{
    use openraft::entry::RaftEntry;
    use openraft::type_config::alias::{EntryOf, LogIdOf};
    let entries: Vec<_> = of
        .iter()
        .map(|&((term, index), holds)| {
            let id = LogIdOf::<ShardTypes>::new(term.into(), index);
            let cond = match holds {
                Holds::Piece { position, len } => Some(types::CondPlace { position, len }),
                Holds::Record | Holds::Nothing => None,
            };
            let entry = match holds {
                Holds::Nothing => EntryOf::<ShardTypes>::new_blank(id),
                Holds::Record | Holds::Piece { .. } => EntryOf::<ShardTypes>::new_normal(
                    id,
                    Command {
                        timestamp_micros: index,
                        payload: Bytes::from_static(b"r"),
                        producer: None,
                        expected: None,
                        trim: None,
                        commit: false,
                        group: false,
                        publisher: None,
                        headers: None,
                        written: None,
                        cond,
                    },
                ),
            };
            Ok((entry, None))
        })
        .collect();
    futures::stream::iter(entries)
}

/// **A batch of which only a prefix committed, cut by the next leader's
/// blank, is never read into and counts in the next record's skips.** The
/// member applies the two pieces in one run and the new term's blank where
/// the third belonged in the next: the read bound it publishes stays at the
/// first piece until the blank, then moves past the whole cut batch, and
/// the publisher of the next record hears of all three offsets.
#[tokio::test]
async fn a_batch_cut_by_the_next_leader_is_never_read_into_and_is_skipped() {
    use openraft::storage::RaftStateMachine;
    let dir = tempfile::tempdir().expect("dir");
    let super::super::machine::Opened {
        mut machine,
        commits,
        skips,
        ..
    } = stream_machine(dir.path()).await;
    let claim = skips.claim(&commits);
    let bounds = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let watching = {
        let (mut commits, bounds) = (commits.clone(), bounds.clone());
        tokio::spawn(async move {
            while commits.changed().await.is_ok() {
                bounds.lock().push(commits.borrow_and_update().read_bound());
            }
        })
    };
    machine
        .apply(entries(&[
            ((1, 1), Holds::Record),
            ((1, 2), piece(0, 3)),
            ((1, 3), piece(1, 3)),
        ]))
        .await
        .expect("apply");
    assert_eq!(commits.borrow().read_bound(), 2);
    assert_eq!(commits.borrow().applied_end(), 4);
    machine
        .apply(entries(&[((2, 4), Holds::Nothing)]))
        .await
        .expect("apply");
    assert_eq!(commits.borrow().read_bound(), 5);
    machine
        .apply(entries(&[((2, 5), Holds::Record)]))
        .await
        .expect("apply");
    assert_eq!(claim.skipped_before(5), 3, "both pieces and the blank");
    drop(machine);
    watching.await.expect("watcher");
    let bounds = bounds.lock().clone();
    assert!(
        bounds.iter().all(|bound| !(3..=4).contains(bound)),
        "a bound inside the batch: {bounds:?}"
    );
}

/// **A member that resumes with its last applied entry inside a batch
/// keeps the read bound at the batch's first piece.** It reads the open
/// batch back from its log, after an install and after a restart, and the
/// batch's last piece then releases the bound as on any member.
#[tokio::test]
async fn a_member_resuming_inside_a_batch_reads_it_back_from_its_log() {
    use felix_storage::log::{AppendRecord, CondPiece, RecordMark};
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    let applied = [
        ((1, 1), Holds::Record),
        ((1, 2), piece(0, 3)),
        ((1, 3), piece(1, 3)),
    ];
    let sender_dir = tempfile::tempdir().expect("dir");
    let mut sender = stream_machine(sender_dir.path()).await.machine;
    sender.apply(entries(&applied)).await.expect("apply");
    let built = sender
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("build");
    assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(3));

    // The receiver's log holds the same entries, as a member's log does
    // up to where it applied.
    let dir = tempfile::tempdir().expect("dir");
    let log = open_log(dir.path(), "m", 0, log_config());
    let mark = |mark| AppendRecord {
        payload: Bytes::from_static(b"r"),
        timestamp_micros: 1,
        mark,
        publisher: None,
        headers: None,
    };
    let cond = |position| RecordMark::CondPiece(CondPiece { position, len: 3 });
    log.append_v9_for_tests(
        &[
            mark(RecordMark::GenerationStart),
            mark(RecordMark::None),
            mark(cond(0)),
            mark(cond(1)),
        ],
        1,
    )
    .await
    .expect("append");
    drop(log);
    let opened = stream_machine(dir.path()).await;
    let mut receiver = opened.machine;
    receiver
        .install_snapshot(&built.meta, built.snapshot)
        .await
        .expect("install");
    assert_eq!(opened.commits.borrow().read_bound(), 2, "after the install");
    assert_eq!(opened.commits.borrow().applied_end(), 4);
    drop(receiver);

    let reopened = stream_machine(dir.path()).await;
    assert_eq!(reopened.commits.borrow().read_bound(), 2, "after a restart");
    let mut machine = reopened.machine;
    machine
        .apply(entries(&[((1, 4), piece(2, 3))]))
        .await
        .expect("apply");
    assert_eq!(reopened.commits.borrow().read_bound(), 5);
}

/// Every read bound each member publishes from now on, by broker.
fn watch_bounds(cluster: &Cluster) -> Arc<parking_lot::Mutex<Vec<(&'static str, u64)>>> {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    for broker in BROKERS {
        let group = cluster.runtime(broker).group(&shard(0)).expect("group");
        let mut commits = group.commits();
        let seen = seen.clone();
        tokio::spawn(async move {
            loop {
                let bound = commits.borrow_and_update().read_bound();
                seen.lock().push((broker, bound));
                if commits.changed().await.is_err() {
                    return;
                }
            }
        });
    }
    seen
}

/// Propose a conditional batch of `records` records expected at `at`.
async fn propose_at(engine: &RaftEngine, at: u64, records: usize) -> Written {
    let record = Record {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"piece"),
        headers: None,
    };
    let batch = Batch {
        expected: Some(at),
        trim: None,
        commit: false,
        records: vec![record; records],
        ack: AckAt::Flushed,
        term: None,
        producer: None,
        publisher: None,
    };
    engine.propose(&shard(0), batch).await.expect("written")
}

/// Wait until `broker` has applied everything below `end`.
async fn applied_on(cluster: &Cluster, broker: &str, end: u64) -> Commits {
    let group = cluster.runtime(broker).group(&shard(0)).expect("group");
    eventually(|| {
        let commits = group.commits().borrow().clone();
        (commits.applied_end() >= end).then_some(commits)
    })
    .await
}

/// **A leader change that commits only a prefix of a conditional batch
/// leaves no member's read bound inside it, and the next record is told of
/// the cut pieces.** One entry per append, and the followers never get the
/// third piece: the first two commit and every member holds its bound at
/// the first. The leader is then lost, the next one's blank takes the third
/// piece's index and cuts the batch, and the bound moves past all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_change_that_cuts_a_batch_leaves_no_bound_inside_it() {
    let mut led = led_with(RuntimeConfig {
        append_entries: 1,
        ..timers()
    })
    .await;
    let at = led.committed + 1;
    let seen = watch_bounds(&led.cluster);
    *led.cluster.net.lose_entries_from.lock() = Some(at + 2);
    let written = propose_at(&led.engine, at, 3).await;
    assert_eq!(written.end, at + 3);
    for broker in BROKERS {
        let commits = applied_on(&led.cluster, broker, at + 2).await;
        assert_eq!(commits.read_bound(), at, "{broker} holds it at the batch");
    }

    let old = led.leader.broker.to_string();
    led.cluster.net.muted.lock().insert(old.clone());
    *led.cluster.net.cut.lock() = HashSet::from([old.clone()]);
    *led.cluster.net.lose_entries_from.lock() = None;
    let next = led.cluster.elected(0).await;
    assert_ne!(next, led.leader);
    let engine = RaftEngine::new(led.cluster.runtime(&next.broker));
    eventually(|| (engine.read_bound(&shard(0)).ok()? > at + 2).then_some(())).await;
    let group = led.cluster.runtime(&next.broker).group(&shard(0));
    let claim = group.expect("group").claim_skips();
    let first = led.cluster.write(0, b"after").await;
    let record = first + 1;
    applied_on(&led.cluster, &next.broker, record).await;
    assert_eq!(
        claim.skipped_before(first),
        first - at,
        "every offset since the last record holds none"
    );

    led.heal();
    for broker in BROKERS {
        let commits = applied_on(&led.cluster, broker, record).await;
        assert_eq!(commits.read_bound(), record, "{broker}");
    }
    let inside: Vec<_> = seen
        .lock()
        .iter()
        .filter(|(_, bound)| (at + 1..at + 3).contains(bound))
        .copied()
        .collect();
    assert!(inside.is_empty(), "bounds inside the batch: {inside:?}");
    led.cluster.stop().await;
}

/// **A leader whose lease lapses with a batch half committed keeps every
/// reader out of it.** The first two pieces commit, then no follower is
/// heard: the bound stays at the first piece and a read cannot be
/// confirmed. Healed, the leader commits the last piece and the bound moves
/// past the whole batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lapsed_lease_mid_batch_keeps_readers_out_of_it() {
    let mut led = led_with(RuntimeConfig {
        append_entries: 1,
        ..timers()
    })
    .await;
    let at = led.committed + 1;
    let seen = watch_bounds(&led.cluster);
    *led.cluster.net.lose_entries_from.lock() = Some(at + 2);
    let written = propose_at(&led.engine, at, 3).await;
    applied_on(&led.cluster, &led.leader.broker, at + 2).await;
    led.cluster.net.cut.lock().extend(BROKERS.map(String::from));
    *led.cluster.net.lose_entries_from.lock() = None;
    // The lapse is the leader's own, so wait for it to say so; a fixed sleep
    // is short whenever the machine is slow to run the leader's timers.
    eventually(|| {
        let status = led.cluster.runtime(&led.leader.broker).status(&shard(0))?;
        status.leader.is_none().then_some(())
    })
    .await;
    assert_eq!(led.engine.read_bound(&shard(0)), Ok(at));
    let unconfirmed = led.engine.confirm_read(&shard(0), led.term).await;
    assert!(
        matches!(unconfirmed, Err(CommitError::TimedOut { what: "read", .. })),
        "{unconfirmed:?}"
    );
    assert_eq!(led.engine.read_bound(&shard(0)), Ok(at));

    // Healed, the followers have heard no leader for longer than an
    // election timeout and neither holds the last piece, so either may win
    // an election before the leader's next append reaches them and rightly
    // discard it. The leader's appends are held until a follower has stood
    // twice, as a loaded machine can delay them, and the followers' asks are
    // lost so the leader is the one to commit the piece.
    let asked = |net: &Net| {
        let asks = net.asks.lock();
        led.followers()
            .map(|broker| asks.get(&broker).copied().unwrap_or(0))
            .collect::<Vec<_>>()
    };
    let before = asked(&led.cluster.net);
    led.cluster
        .net
        .unheard_candidates
        .lock()
        .extend(led.followers());
    led.cluster.net.lose_appends.store(true, Relaxed);
    led.heal();
    eventually(|| {
        let now = asked(&led.cluster.net);
        let stood_twice = |(now, before): (&u32, &u32)| now - before >= 4;
        now.iter().zip(&before).any(stood_twice).then_some(())
    })
    .await;
    led.cluster.net.lose_appends.store(false, Relaxed);
    assert_eq!(
        led.engine.await_commit(&shard(0), written, PATIENCE).await,
        Ok(())
    );
    led.cluster.net.unheard_candidates.lock().clear();
    for broker in BROKERS {
        let commits = applied_on(&led.cluster, broker, at + 3).await;
        assert!(commits.read_bound() >= at + 3, "{broker}");
    }
    let inside: Vec<_> = seen
        .lock()
        .iter()
        .filter(|(_, bound)| (at + 1..at + 3).contains(bound))
        .copied()
        .collect();
    assert!(inside.is_empty(), "bounds inside the batch: {inside:?}");
    led.cluster.stop().await;
}

/// **A commit goes to the followers on the next append.** Without the
/// ticker no heartbeat goes out, so every entry-less append here carries a
/// commit and nothing else. openraft would send one per follower per write;
/// held for the write after it, nearly none go. The hold is long so a
/// loaded machine still writes within it. The last commit, with no write
/// after it, reaches every follower once the hold ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_goes_to_the_followers_on_the_next_append() {
    const WRITES: u64 = 50;
    let config = RuntimeConfig {
        commit_hold: Duration::from_millis(150),
        ..timers()
    };
    let mut cluster = Cluster::with(config, false);
    cluster.open(0).await;
    cluster
        .runtime("a")
        .initialize(&shard(0), members())
        .await
        .expect("initialize");
    cluster.elected(0).await;
    cluster.write(0, b"first").await;
    let before = cluster.net.empty_appends.load(Relaxed);
    let mut last = 0;
    for _ in 0..WRITES {
        last = cluster.write(0, b"record").await;
    }
    let empty = cluster.net.empty_appends.load(Relaxed) - before;
    assert!(
        empty < WRITES / 5,
        "{empty} entry-less appends for {WRITES} writes"
    );
    cluster.committed_everywhere(0, last).await;
    cluster.stop().await;
}
