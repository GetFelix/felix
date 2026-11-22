use std::sync::{Arc, Mutex};

use futures::channel::mpsc;
use openraft::entry::RaftEntry;
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf};
use openraft::vote::{RaftLeaderId, RaftVote};

use super::*;
use crate::raft::types::MemberId;

const HOLD: Duration = Duration::from_millis(1);

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
    let answers = stream_append(follower, rx, option(), HOLD).await.unwrap();
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
    run(
        &mut follower,
        vec![append(2, 5, 5, 4), append(2, 5, 5, 5)],
        Duration::from_millis(10),
    )
    .await;
    assert_eq!(follower.sent(), vec![(2, 5, 5, 5)]);
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
