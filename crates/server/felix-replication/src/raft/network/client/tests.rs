use std::sync::{Arc, Mutex};

use futures::channel::mpsc;
use openraft::entry::RaftEntry;
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf};
use openraft::vote::{RaftLeaderId, RaftVote};

use super::*;
use crate::raft::types::MemberId;

const HOLD: Duration = Duration::from_millis(1);
/// Long enough that a test lands the next append well inside it.
const WINDOW: Duration = Duration::from_millis(10);

/// A follower that takes every append whole, and records when each came.
#[derive(Clone, Default)]
struct Follower(Arc<Mutex<Vec<(tokio::time::Instant, Append)>>>);

impl Follower {
    /// What each append was, as `(term, prev, last, commit)`.
    fn sent(&self) -> Vec<(u64, u64, u64, u64)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, rpc)| shape(rpc))
            .collect()
    }

    fn sent_at(&self) -> Vec<tokio::time::Instant> {
        self.0.lock().unwrap().iter().map(|(at, _)| *at).collect()
    }
}

impl NetAppend<ShardTypes> for Follower {
    async fn append_entries(&mut self, rpc: Append, _: RPCOption) -> Answer {
        let matched = rpc
            .entries
            .last()
            .map(|entry| entry.log_id())
            .or(rpc.prev_log_id);
        self.0
            .lock()
            .unwrap()
            .push((tokio::time::Instant::now(), rpc));
        Ok(Ok(matched))
    }
}

fn log_id(index: u64) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes> {
        leader_id: CommittedLeaderIdOf::<ShardTypes> { term: 2 },
        index,
    }
}

fn append(term: u64, prev: u64, last: u64, commit: u64) -> Append {
    let leader = openraft::impls::leader_id_std::LeaderId::new(term, MemberId::new("a", 1));
    Append {
        vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
        prev_log_id: Some(log_id(prev)),
        leader_commit: Some(log_id(commit)),
        entries: (prev + 1..=last)
            .map(|index| EntryOf::<ShardTypes>::new_blank(log_id(index)))
            .collect(),
    }
}

fn shape(rpc: &Append) -> (u64, u64, u64, u64) {
    let index = |log_id: Option<LogIdOf<ShardTypes>>| log_id.map_or(0, |log_id| log_id.index);
    let last = rpc
        .entries
        .last()
        .map(|entry| entry.log_id())
        .or(rpc.prev_log_id);
    (
        rpc.vote.leader_id().term(),
        index(rpc.prev_log_id),
        index(last),
        index(rpc.leader_commit),
    )
}

fn option() -> RPCOption {
    RPCOption::new(Duration::from_secs(1))
}

/// Sends `appends` through the stream, then waits `then` with the input
/// still open before ending it. Returns every answer.
async fn run(follower: &mut Follower, appends: Vec<Append>, then: Duration) -> Vec<Answer> {
    let (tx, rx) = mpsc::unbounded();
    for rpc in appends {
        tx.unbounded_send(rpc).unwrap();
    }
    let answers = stream_append(follower, rx, option(), Wait::Hold(HOLD))
        .await
        .unwrap();
    let ending = async move {
        tokio::time::sleep(then).await;
        drop(tx);
    };
    let (answers, ()) = tokio::join!(answers.collect::<Vec<_>>(), ending);
    answers
}

#[tokio::test(start_paused = true)]
async fn a_commit_only_append_goes_with_the_next_append() {
    let mut follower = Follower::default();
    let answers = run(
        &mut follower,
        vec![append(2, 5, 5, 5), append(2, 5, 7, 5)],
        Duration::ZERO,
    )
    .await;
    assert_eq!(follower.sent(), vec![(2, 5, 7, 5)]);
    // One answer acknowledges both requests: openraft drains its in-flight
    // queue up to the matching log id.
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].as_ref().unwrap(), &Ok(Some(log_id(7))));
}

#[tokio::test(start_paused = true)]
async fn a_later_commit_replaces_an_earlier_one() {
    let mut follower = Follower::default();
    let start = tokio::time::Instant::now();
    run(
        &mut follower,
        vec![append(2, 5, 5, 4), append(2, 5, 5, 5)],
        Duration::from_millis(10),
    )
    .await;
    assert_eq!(follower.sent(), vec![(2, 5, 5, 5)]);
    // The hold runs from the first one, so a stream of commits cannot keep
    // the followers waiting.
    let waited = follower.sent_at()[0] - start;
    assert!(waited >= HOLD && waited < HOLD * 2, "{waited:?}");
}

#[tokio::test(start_paused = true)]
async fn a_commit_with_nothing_after_it_goes_after_the_hold() {
    let mut follower = Follower::default();
    let start = tokio::time::Instant::now();
    run(
        &mut follower,
        vec![append(2, 5, 5, 5)],
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(follower.sent(), vec![(2, 5, 5, 5)]);
    let waited = follower.sent_at()[0] - start;
    assert!(waited >= HOLD && waited < HOLD * 2, "{waited:?}");
}

#[tokio::test(start_paused = true)]
async fn a_held_append_goes_when_openraft_ends_the_stream() {
    let mut follower = Follower::default();
    let start = tokio::time::Instant::now();
    let answers = run(&mut follower, vec![append(2, 5, 5, 5)], Duration::ZERO).await;
    assert_eq!(follower.sent(), vec![(2, 5, 5, 5)]);
    assert_eq!(follower.sent_at()[0], start);
    assert_eq!(answers.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_append_that_would_answer_differently_replaces_nothing() {
    for next in [
        // Another vote: the follower may refuse one and take the other.
        append(3, 5, 7, 5),
        // Another previous entry to check.
        append(2, 4, 7, 5),
        // A lower commit, which would leave the follower behind.
        append(2, 5, 7, 4),
    ] {
        let mut follower = Follower::default();
        let held = append(2, 5, 5, 5);
        let shapes = vec![shape(&held), shape(&next)];
        let answers = run(&mut follower, vec![held, next], Duration::ZERO).await;
        assert_eq!(follower.sent(), shapes);
        assert_eq!(answers.len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn an_append_with_entries_is_never_held() {
    let mut follower = Follower::default();
    let start = tokio::time::Instant::now();
    run(
        &mut follower,
        vec![append(2, 5, 7, 5)],
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(follower.sent_at(), vec![start]);
}

type Shape = (u64, u64, u64, u64);

/// Records what reaches the wire and when, answering each append whole.
#[derive(Default)]
struct Wire(parking_lot::Mutex<Vec<(tokio::time::Instant, Shape)>>);

impl Wire {
    fn sent(&self) -> Vec<Shape> {
        self.0.lock().iter().map(|(_, shape)| *shape).collect()
    }

    fn sent_at(&self) -> Vec<tokio::time::Instant> {
        self.0.lock().iter().map(|(at, _)| *at).collect()
    }
}

#[async_trait::async_trait]
impl crate::raft::network::RaftTransport for Wire {
    async fn send(
        &self,
        request: crate::raft::network::RaftRequest,
    ) -> Result<crate::raft::network::RaftReply, crate::raft::network::Refused> {
        let crate::raft::network::Request::Append(rpc) = request.body else {
            unreachable!("only appends here");
        };
        self.0
            .lock()
            .push((tokio::time::Instant::now(), shape(&rpc)));
        let matched = rpc
            .entries
            .last()
            .map(|entry| entry.log_id())
            .or(rpc.prev_log_id);
        Ok(crate::raft::network::RaftReply(
            crate::raft::network::Reply::Append(Ok(matched)),
        ))
    }
}

/// One follower's appends, through heartbeat coalescing to `wire`.
struct Windowed(Arc<dyn crate::raft::network::RaftTransport>);

impl Windowed {
    fn new(wire: &Arc<Wire>) -> Self {
        let wire: Arc<dyn crate::raft::network::RaftTransport> = wire.clone();
        Self(crate::raft::beats::Beats::wrap(wire, WINDOW))
    }
}

impl NetAppend<ShardTypes> for Windowed {
    async fn append_entries(&mut self, rpc: Append, _: RPCOption) -> Answer {
        use crate::raft::network::{Ask, Beat, RaftReply, RaftRequest, Reply, Request};
        let request = RaftRequest {
            group: crate::raft::types::GroupId::new(
                crate::ShardKey {
                    tenant_id: "t".into(),
                    namespace: "ns".into(),
                    stream: "s".into(),
                    shard: 0,
                    kind: crate::ShardKind::Stream,
                },
                0,
            ),
            to: MemberId::new("b", 1),
            beat: Beat::default(),
            report: Ask::Nothing,
            deadline: Duration::from_secs(1),
            body: Request::Append(rpc),
        };
        match self.0.send(request).await {
            Ok(RaftReply(Reply::Append(answer))) => Ok(answer),
            other => panic!("not an append's answer: {:?}", other.is_ok()),
        }
    }
}

/// As [`run`], with the commit waiting in the heartbeat window.
async fn run_windowed(wire: &Arc<Wire>, appends: Vec<(Duration, Append)>) -> Vec<Answer> {
    run_windowed_after(wire, Duration::ZERO, appends).await
}

/// As [`run_windowed`], with a hold of `before` ahead of the window.
async fn run_windowed_after(
    wire: &Arc<Wire>,
    before: Duration,
    appends: Vec<(Duration, Append)>,
) -> Vec<Answer> {
    let (tx, rx) = mpsc::unbounded();
    let mut network = Windowed::new(wire);
    let answers = stream_append(&mut network, rx, option(), Wait::InWindow { before })
        .await
        .unwrap();
    let feeding = async move {
        for (after, rpc) in appends {
            tokio::time::sleep(after).await;
            tx.unbounded_send(rpc).unwrap();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(tx);
    };
    let (answers, ()) = tokio::join!(answers.collect::<Vec<_>>(), feeding);
    answers
}

/// With heartbeats together, the commit waits once: in their window, with
/// no hold before it, and on no task of its own.
#[tokio::test(start_paused = true)]
async fn a_lone_commit_waits_only_the_heartbeat_window() {
    let wire = Arc::new(Wire::default());
    let start = tokio::time::Instant::now();
    let before = crate::raft::beats::counts::read();
    let answers = run_windowed(&wire, vec![(Duration::ZERO, append(2, 5, 5, 5))]).await;
    let (timers, spawns) = crate::raft::beats::counts::read();
    assert_eq!(wire.sent(), vec![(2, 5, 5, 5)]);
    assert_eq!(answers.len(), 1);
    assert_eq!(
        (timers - before.0, spawns - before.1),
        (1, 0),
        "(timers, tasks) for one commit"
    );
    let waited = wire.sent_at()[0] - start;
    assert!(waited >= WINDOW && waited < WINDOW + HOLD * 2, "{waited:?}");
}

/// The next append, coming while the commit waits in the window, takes it
/// back and goes in its place.
#[tokio::test(start_paused = true)]
async fn the_next_append_takes_a_waiting_commit_back() {
    let wire = Arc::new(Wire::default());
    let answers = run_windowed(
        &wire,
        vec![
            (Duration::ZERO, append(2, 5, 5, 5)),
            (WINDOW / 2, append(2, 5, 7, 5)),
        ],
    )
    .await;
    assert_eq!(wire.sent(), vec![(2, 5, 7, 5)]);
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].as_ref().unwrap(), &Ok(Some(log_id(7))));
}

/// Once the window has sent the commit, the next append waits for its
/// answer and goes after it: never two appends to one follower at once.
#[tokio::test(start_paused = true)]
async fn a_commit_already_sent_is_not_taken_back() {
    let wire = Arc::new(Wire::default());
    let answers = run_windowed(
        &wire,
        vec![
            (Duration::ZERO, append(2, 5, 5, 5)),
            (WINDOW * 3, append(2, 5, 7, 5)),
        ],
    )
    .await;
    assert_eq!(wire.sent(), vec![(2, 5, 5, 5), (2, 5, 7, 5)]);
    assert_eq!(answers.len(), 2);
}

/// What would not replace a held commit does not take a waiting one back.
#[tokio::test(start_paused = true)]
async fn an_append_that_would_answer_differently_takes_nothing_back() {
    for next in [append(3, 5, 7, 5), append(2, 4, 7, 5), append(2, 5, 7, 4)] {
        let wire = Arc::new(Wire::default());
        let held = append(2, 5, 5, 5);
        let shapes = vec![shape(&held), shape(&next)];
        let answers =
            run_windowed(&wire, vec![(Duration::ZERO, held), (Duration::ZERO, next)]).await;
        assert_eq!(wire.sent(), shapes);
        assert_eq!(answers.len(), 2);
    }
}

/// An append with entries goes at once, window or not.
#[tokio::test(start_paused = true)]
async fn an_append_with_entries_never_waits_in_the_window() {
    let wire = Arc::new(Wire::default());
    let start = tokio::time::Instant::now();
    run_windowed(&wire, vec![(Duration::ZERO, append(2, 5, 7, 5))]).await;
    assert_eq!(wire.sent_at(), vec![start]);
}

/// A hold longer than the window holds what it has beyond the window first,
/// so a commit waits as long in all as the hold alone, and the next append
/// can replace it all that time.
#[tokio::test(start_paused = true)]
async fn a_hold_longer_than_the_window_waits_the_hold_in_all() {
    let wire = Arc::new(Wire::default());
    let start = tokio::time::Instant::now();
    run_windowed_after(&wire, WINDOW, vec![(Duration::ZERO, append(2, 5, 5, 5))]).await;
    let waited = wire.sent_at()[0] - start;
    assert!(
        waited >= WINDOW * 2 && waited < WINDOW * 2 + HOLD * 2,
        "{waited:?}"
    );
    for after in [WINDOW / 2, WINDOW * 3 / 2] {
        let wire = Arc::new(Wire::default());
        run_windowed_after(
            &wire,
            WINDOW,
            vec![
                (Duration::ZERO, append(2, 5, 5, 5)),
                (after, append(2, 5, 7, 5)),
            ],
        )
        .await;
        assert_eq!(wire.sent(), vec![(2, 5, 7, 5)], "next after {after:?}");
    }
}
