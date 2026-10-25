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

/// **A checked answer below the tail starts a pass.** The follower answered
/// the pass's own question and is still short of the tail that pass read, so
/// the wake that read it may have gone to a pass that launched nobody.
#[tokio::test(start_paused = true)]
async fn a_checked_answer_below_the_tail_starts_a_pass() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    let answer = shards.answer_from(&key(), "broker-b", TAIL - 1, true);
    shards.apply(answer);
    assert_eq!(shards.passes.len(), 1, "no pass owed to the follower");
}

/// **An answer at the tail starts nothing.** Applied over and over, as a
/// follower that keeps answering level would, it must not become a loop.
#[tokio::test(start_paused = true)]
async fn an_answer_at_the_tail_starts_no_pass() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    for _ in 0..20 {
        let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
        shards.apply(answer);
    }
    assert_eq!(shards.passes.len(), 0);
}

/// An answer the pass could not vouch for, or from a halted follower, is left
/// for the next wake: a peer that fails fast would be dialled in a loop.
#[tokio::test(start_paused = true)]
async fn an_unchecked_or_halted_answer_below_the_tail_starts_no_pass() {
    let rig = Rig::new().await;
    let mut shards = settled(&rig).await;
    let unchecked = shards.answer_from(&key(), "broker-b", TAIL - 1, false);
    shards.apply(unchecked);
    assert_eq!(shards.passes.len(), 0, "an unchecked answer started a pass");

    let halted = shards.halted_answer_from(&key(), "broker-c", TAIL - 1);
    shards.apply(halted);
    assert_eq!(shards.passes.len(), 0, "a halted follower started a pass");
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
    assert_eq!(rig.status().passes_since_mark, 0);
    pass(&mut shards, WakeKind::Tick).await;
    pass(&mut shards, WakeKind::Tick).await;
    assert_eq!(rig.status().passes_since_mark, 2);
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
        assert_eq!(rig.status().last_wake, Some(name));
    }

    // An answer after the pass ended is written at once: no pass is coming
    // to carry it.
    let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
    shards.apply(answer);
    assert_eq!(rig.status().last_wake, Some("straggler answer"));

    // One handed to a pass under way shows when that pass ends.
    let answer = shards.answer_from(&key(), "broker-b", TAIL, true);
    shards.scan(WakeKind::Tick);
    shards.apply(answer);
    let (scan, ended) = shards.passes.next().await.expect("a pass");
    shards.fold(scan, ended);
    assert_eq!(rig.status().last_wake, Some("busy answer"));
}
