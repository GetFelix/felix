//! What the driver does with an exchange's answer once its pass has ended,
//! and what it leaves on the status board for a mark that is stuck.
//!
//! These drive `Shards` by hand, one event at a time, so which pass is under
//! way when an answer lands is exact rather than a matter of timing.

use felix_router::ShardRouter;

use super::*;
use crate::status::ShardStatusBoard;

const TAIL: u64 = 3;

struct Rig {
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    marks: Arc<QuorumMarks>,
    status: ShardStatusBoard,
    peers: AcceptingFollower,
    rebuilds: Rebuilds,
    throttle: MoveThrottle,
    _dir: TempDir,
}

impl Rig {
    async fn new() -> Self {
        let (broker, dir) = super::quorum::quorum_leader(TAIL as usize, 4).await;
        Self {
            broker,
            router: router(LOCAL, &["broker-b", "broker-c"], 4),
            marks: Arc::new(QuorumMarks::new()),
            status: ShardStatusBoard::new(),
            peers: AcceptingFollower::default(),
            rebuilds: Rebuilds::new(RebuildPolicy::default()),
            throttle: MoveThrottle::unlimited(),
            _dir: dir,
        }
    }

    fn shards(&self) -> Shards<'_, AcceptingFollower> {
        Shards::new(Context {
            requester: &self.peers,
            broker: &self.broker,
            router: &self.router,
            fence: &Unfenced,
            gate: &crate::promotion::NoGate,
            marks: &self.marks,
            status: &self.status,
            reporter: None,
            rebuilds: &self.rebuilds,
            throttle: &self.throttle,
            refresh: Duration::from_secs(300),
        })
    }

    fn live(&self) -> crate::status::LiveReport {
        self.status
            .live_report(&watch_key(&key()))
            .expect("live state")
    }

    fn status(&self) -> crate::status::ShardStatus {
        self.status.get(&watch_key(&key())).expect("status")
    }
}

/// Run one pass to its end and fold it.
async fn pass(shards: &mut Shards<'_, AcceptingFollower>, wake: WakeKind) {
    shards.scan(wake);
    let (scan, ended) = shards.passes.next().await.expect("a pass");
    shards.fold(scan, ended);
}

/// A rig whose first pass has run, so both followers sit at the tail.
async fn settled(rig: &Rig) -> Shards<'_, AcceptingFollower> {
    let mut shards = rig.shards();
    pass(&mut shards, WakeKind::Tick).await;
    assert_eq!(rig.marks.offset(&watch_key(&key()), 4), Some(TAIL));
    shards
}

/// **The status counts followers busy from earlier passes.** b's exchange
/// outlives the pass that launched it; the next pass launches c alone and
/// must still show b as busy, and where its exchange started.
#[tokio::test(start_paused = true)]
async fn the_status_lists_followers_busy_from_an_earlier_pass() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    shards.hold_busy(&key(), "broker-b");
    pass(&mut shards, WakeKind::Tick).await;
    let status = rig.status();
    let b = status
        .followers
        .iter()
        .find(|f| f.node_id == "broker-b")
        .expect("b");
    assert!(b.busy, "b's earlier exchange was not shown");
    assert_eq!(b.busy_at, Some(TAIL));
    let c = status
        .followers
        .iter()
        .find(|f| f.node_id == "broker-c")
        .expect("c");
    assert!(!c.busy);
    assert_eq!(c.busy_at, None);
}

/// **The status counts passes since the mark moved.** The first pass moves
/// the mark; two more leave it where it is.
#[tokio::test(start_paused = true)]
async fn the_status_counts_passes_since_the_mark_moved() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    assert_eq!(rig.live().passes_since_mark, 0);
    pass(&mut shards, WakeKind::Tick).await;
    pass(&mut shards, WakeKind::Tick).await;
    assert_eq!(rig.live().passes_since_mark, 2);
}

/// **The status says what woke the shard last.**
#[tokio::test(start_paused = true)]
async fn the_status_names_the_last_wake() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    for (wake, name) in [
        (WakeKind::Append, "append"),
        (WakeKind::Routes, "routes"),
        (WakeKind::Tick, "tick"),
    ] {
        pass(&mut shards, wake).await;
        assert_eq!(rig.live().last_wake, Some(name));
    }

    // An answer after the pass ended is written at once: no pass is coming
    // to carry it.
    let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
    shards.apply(answer);
    assert_eq!(rig.live().last_wake, Some("straggler answer"));

    // One handed to a pass under way shows when that pass ends.
    let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
    shards.scan(WakeKind::Tick);
    shards.apply(answer);
    let (scan, ended) = shards.passes.next().await.expect("a pass");
    shards.fold(scan, ended);
    assert_eq!(rig.live().last_wake, Some("busy answer"));
}

/// **The live state follows the pass and its exchanges, not the fold.** A
/// pass that never ends still shows that it is running, how long, and which
/// exchanges are out and since when.
#[tokio::test(start_paused = true)]
async fn the_live_state_shows_a_pass_that_has_not_ended() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    let watched = watch_key(&key());
    let report = rig.status.live_report(&watched).expect("report");
    assert_eq!(report.pass, None, "a folded pass is not running");
    assert_eq!(report.last_scan, Some(1));
    assert_eq!(report.driver_scan, 1);
    assert!(report.launched.is_empty(), "{:?}", report.launched);

    // A pass starts and is never polled to its end: the stuck-pass case.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    shards.scan(WakeKind::Tick);
    tokio::time::sleep(Duration::from_millis(4900)).await;
    let report = rig.status.live_report(&watched).expect("report");
    assert_eq!(report.driver_scan, 2);
    assert_eq!(report.last_scan, Some(1), "no pass has ended since");
    assert_eq!(report.pass, Some((2, 4900)));
    assert_eq!(
        report.launched,
        vec![
            format!("broker-b@{TAIL} sent 4900ms ago"),
            format!("broker-c@{TAIL} sent 4900ms ago"),
        ],
    );
}

/// An exchange stays listed from the pass that launched it until its answer,
/// across the pass folding, and keeps its first launch time when a later
/// pass does not launch it again.
#[tokio::test(start_paused = true)]
async fn the_live_state_keeps_an_exchange_until_it_is_answered() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    let watched = watch_key(&key());
    shards.hold_busy(&key(), "broker-b");
    // `hold_busy` stands in for a pass's straggler, so list its launch.
    rig.status.pass_started(
        &watched,
        1,
        Some(TAIL),
        vec![("broker-b".to_string(), TAIL)],
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;
    pass(&mut shards, WakeKind::Tick).await;
    let report = rig.status.live_report(&watched).expect("report");
    assert_eq!(report.pass, None);
    assert_eq!(
        report.launched,
        vec![format!("broker-b@{TAIL} sent 1000ms ago")]
    );

    let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
    shards.apply(answer);
    let report = rig.status.live_report(&watched).expect("report");
    assert!(report.launched.is_empty(), "{:?}", report.launched);
}

/// **The wake and pass count are live.** A pass that has not ended already
/// shows what woke it and that it counts, so a stuck pass is not described by
/// the previous fold.
#[tokio::test(start_paused = true)]
async fn the_wake_and_pass_count_are_live_while_a_pass_runs() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    assert_eq!(rig.live().passes_since_mark, 0);
    shards.scan(WakeKind::Append);
    let live = rig.live();
    assert!(live.pass.is_some());
    assert_eq!(live.last_wake, Some("append"));
    assert_eq!(live.passes_since_mark, 1);
    // The next wake lands on the running pass and is still shown.
    shards.scan(WakeKind::Routes);
    assert_eq!(rig.live().last_wake, Some("routes"));
}

/// A pass that ends on a shard no longer led leaves no live state behind.
#[tokio::test(start_paused = true)]
async fn a_pass_ending_on_a_lost_shard_leaves_no_live_state() {
    let rig = Rig::new().await;
    let mut shards = rig.shards();
    shards.scan(WakeKind::Tick);
    assert!(rig.status.live_report(&watch_key(&key())).is_some());
    publish(&rig.router, "broker-b", &["broker-c"], 5);
    let (scan, ended) = shards.passes.next().await.expect("a pass");
    shards.fold(scan, ended);
    assert!(rig.status.live_report(&watch_key(&key())).is_none());
}
