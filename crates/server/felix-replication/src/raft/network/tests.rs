use std::pin::Pin;

use futures::FutureExt;
use openraft::entry::RaftEntry;
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use tokio::sync::{mpsc, oneshot};

use super::*;

const TERM: u64 = 2;

fn log_id(index: u64) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes> {
        leader_id: CommittedLeaderIdOf::<ShardTypes> { term: TERM },
        index,
    }
}

fn vote(term: u64) -> VoteOf<ShardTypes> {
    let leader = openraft::impls::leader_id_std::LeaderId::new(term, MemberId::new("a", 1));
    VoteOf::<ShardTypes>::from_leader_id(leader, true)
}

/// The append of entries `prev + 1 ..= last` under `term`'s vote.
fn append(term: u64, prev: u64, last: u64) -> AppendEntriesRequest<ShardTypes> {
    AppendEntriesRequest {
        vote: vote(term),
        prev_log_id: Some(log_id(prev)),
        entries: (prev + 1..=last)
            .map(|index| EntryOf::<ShardTypes>::new_blank(log_id(index)))
            .collect(),
        leader_commit: None,
    }
}

/// Appends of one entry each, `1..=count`.
fn appends(count: u64) -> Vec<AppendEntriesRequest<ShardTypes>> {
    (0..count)
        .map(|prev| append(TERM, prev, prev + 1))
        .collect()
}

fn lost() -> Answer {
    Err(RPCError::Unreachable(Unreachable::new(
        &std::io::Error::other("lost"),
    )))
}

type Out = Pin<Box<dyn Stream<Item = Answer> + Send>>;

/// A follower the test answers by hand: every append sent waits in `sent`
/// with the means to answer it.
struct Follower {
    sent: mpsc::UnboundedReceiver<(AppendEntriesRequest<ShardTypes>, oneshot::Sender<Answer>)>,
    waiting: Vec<(
        AppendEntriesRequest<ShardTypes>,
        Option<oneshot::Sender<Answer>>,
    )>,
}

impl Follower {
    fn new(input: Vec<AppendEntriesRequest<ShardTypes>>, window: usize) -> (Self, Out) {
        Self::cutting(input, window, |_| false)
    }

    /// With `fit` standing in for the byte budget.
    fn cutting(
        input: Vec<AppendEntriesRequest<ShardTypes>>,
        window: usize,
        fit: impl FnMut(&mut AppendEntriesRequest<ShardTypes>) -> bool + Send + 'static,
    ) -> (Self, Out) {
        let (tx, sent) = mpsc::unbounded_channel();
        let out = pipeline(futures::stream::iter(input), window, fit, move |rpc| {
            let (answer, answered) = oneshot::channel();
            tx.send((rpc, answer)).expect("the test holds the receiver");
            async move { answered.await.unwrap_or_else(|_| lost()) }
        });
        let follower = Self {
            sent,
            waiting: Vec::new(),
        };
        (follower, Box::pin(out))
    }

    /// How many appends have been sent so far.
    fn sent(&mut self) -> usize {
        while let Ok((rpc, answer)) = self.sent.try_recv() {
            self.waiting.push((rpc, Some(answer)));
        }
        self.waiting.len()
    }

    /// Answer the `nth` append sent (from 0).
    fn answer(&mut self, nth: usize, answer: Answer) {
        self.sent();
        let sender = self.waiting[nth].1.take().expect("answered once");
        let _ = sender.send(answer);
    }

    /// Answer the `nth` append as taken whole.
    fn take(&mut self, nth: usize) {
        self.sent();
        let rpc = &self.waiting[nth].0;
        let last = rpc
            .entries
            .last()
            .map(|entry| entry.log_id())
            .or(rpc.prev_log_id);
        self.answer(nth, Ok(Ok(last)));
    }
}

/// The next answer, if one is ready without waiting.
fn ready(out: &mut Out) -> Option<Option<Answer>> {
    out.next().now_or_never()
}

#[track_caller]
fn assert_acked(out: &mut Out, index: u64) {
    match ready(out) {
        Some(Some(Ok(Ok(Some(acked))))) => assert_eq!(acked, log_id(index)),
        other => panic!("expected an acknowledgement of {index}, got {other:?}"),
    }
}

#[track_caller]
fn assert_ended(out: &mut Out) {
    assert!(matches!(ready(out), Some(None)), "the stream goes on");
}

/// Sends the first append alone and takes it, which opens the window.
#[track_caller]
fn open_window(follower: &mut Follower, out: &mut Out) {
    assert!(ready(out).is_none());
    assert_eq!(follower.sent(), 1, "one append before the first answer");
    follower.take(0);
    assert_acked(out, 1);
    assert!(ready(out).is_none());
}

#[tokio::test]
async fn appends_leave_before_the_ones_ahead_are_answered() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    open_window(&mut follower, &mut out);
    assert_eq!(follower.sent(), 5, "the whole window is on its way");
}

/// A follower that never answers, such as one that just timed out, gets
/// one append per stream, not a window of them.
#[tokio::test]
async fn a_follower_that_does_not_answer_gets_one_append_per_stream() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    assert!(ready(&mut out).is_none());
    assert_eq!(follower.sent(), 1);
    follower.answer(0, lost());
    assert!(matches!(ready(&mut out), Some(Some(Err(_)))));
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 1);
}

#[tokio::test]
async fn answers_are_yielded_in_the_order_the_appends_left() {
    let (mut follower, mut out) = Follower::new(appends(5), 4);
    open_window(&mut follower, &mut out);
    for nth in [4, 3, 2] {
        follower.take(nth);
    }
    assert!(ready(&mut out).is_none(), "the first is still unanswered");
    follower.take(1);
    for index in 2..=5 {
        assert_acked(&mut out, index);
    }
    assert_ended(&mut out);
}

#[tokio::test]
async fn no_more_than_the_window_is_in_flight() {
    let (mut follower, mut out) = Follower::new(appends(10), 3);
    open_window(&mut follower, &mut out);
    assert_eq!(follower.sent(), 4);
    follower.take(1);
    assert_acked(&mut out, 2);
    assert!(ready(&mut out).is_none());
    assert_eq!(follower.sent(), 5, "one answer makes room for one append");
}

#[tokio::test]
async fn a_failed_append_ends_the_stream_and_what_followed_is_not_reported() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    open_window(&mut follower, &mut out);
    follower.take(4);
    follower.take(3);
    follower.answer(2, lost());
    follower.take(1);
    assert_acked(&mut out, 2);
    assert!(matches!(ready(&mut out), Some(Some(Err(_)))));
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 5, "nothing is sent past the failure");
}

/// The follower took the third append before the second, so it had to
/// refuse it. openraft must not hear that refusal: it names an entry the
/// follower has just acknowledged.
#[tokio::test]
async fn a_conflict_behind_an_acknowledged_append_ends_the_stream_unreported() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    open_window(&mut follower, &mut out);
    let conflict = AppendError::Conflict {
        log_id: log_id(2),
        hint: None,
    };
    follower.answer(2, Ok(Err(conflict)));
    follower.take(3);
    follower.take(1);
    assert_acked(&mut out, 2);
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 5);
}

#[tokio::test]
async fn a_conflict_on_the_first_append_is_reported() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    assert!(ready(&mut out).is_none());
    let conflict = AppendError::Conflict {
        log_id: log_id(0),
        hint: None,
    };
    follower.answer(0, Ok(Err(conflict)));
    assert!(matches!(
        ready(&mut out),
        Some(Some(Ok(Err(AppendError::Conflict { log_id: at, .. })))) if at == log_id(0)
    ));
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 1);
}

#[tokio::test]
async fn a_higher_vote_ends_the_stream() {
    let (mut follower, mut out) = Follower::new(appends(8), 4);
    open_window(&mut follower, &mut out);
    follower.take(1);
    follower.answer(2, Ok(Err(AppendError::HigherVote(vote(TERM + 1)))));
    follower.take(3);
    assert_acked(&mut out, 2);
    assert!(matches!(
        ready(&mut out),
        Some(Some(Ok(Err(AppendError::HigherVote(_)))))
    ));
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 5);
}

/// A follower that took only part of an append ends the stream, as the
/// one-at-a-time stream does.
#[tokio::test]
async fn a_partial_acknowledgement_ends_the_stream() {
    let input = vec![append(TERM, 0, 3), append(TERM, 3, 5)];
    let (mut follower, mut out) = Follower::new(input, 4);
    assert!(ready(&mut out).is_none());
    follower.answer(0, Ok(Ok(Some(log_id(2)))));
    assert_acked(&mut out, 2);
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 1);
}

/// openraft opens a stream per leader vote. An append under another one is
/// not sent, and the stream ends once the appends ahead are answered.
#[tokio::test]
async fn an_append_under_another_vote_is_not_sent() {
    let mut input = appends(3);
    input.push(append(TERM + 1, 3, 4));
    input.push(append(TERM, 3, 4));
    let (mut follower, mut out) = Follower::new(input, 4);
    open_window(&mut follower, &mut out);
    assert_eq!(follower.sent(), 3);
    follower.take(1);
    follower.take(2);
    assert_acked(&mut out, 2);
    assert_acked(&mut out, 3);
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 3);
}

/// An append without entries goes on another stream than the appends
/// ahead of it, so it waits for them to land.
#[tokio::test]
async fn an_append_without_entries_waits_for_the_flight_to_land() {
    let input = vec![append(TERM, 0, 1), append(TERM, 1, 2), append(TERM, 2, 2)];
    let (mut follower, mut out) = Follower::new(input, 4);
    open_window(&mut follower, &mut out);
    assert_eq!(follower.sent(), 2);
    follower.take(1);
    assert_acked(&mut out, 2);
    assert!(ready(&mut out).is_none());
    assert_eq!(follower.sent(), 3, "it leaves once nothing is ahead of it");
    assert!(follower.waiting[2].0.entries.is_empty());
    follower.take(2);
    assert_acked(&mut out, 2);
    assert_ended(&mut out);
}

/// An append with entries carries a commit at least as new as the one an
/// append without entries waits to carry, and goes in its place.
#[tokio::test]
async fn an_append_with_entries_replaces_one_without() {
    let input = vec![
        append(TERM, 0, 1),
        append(TERM, 1, 2),
        append(TERM, 2, 2),
        append(TERM, 2, 3),
    ];
    let (mut follower, mut out) = Follower::new(input, 4);
    open_window(&mut follower, &mut out);
    assert_eq!(follower.sent(), 3, "the entries do not wait");
    assert!(!follower.waiting[2].0.entries.is_empty());
    follower.take(2);
    follower.take(1);
    assert_acked(&mut out, 2);
    assert_acked(&mut out, 3);
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 3);
}

/// Nothing goes behind an append cut to fit one message, and its answer
/// reads as partial, which ends the stream.
#[tokio::test]
async fn nothing_is_sent_behind_a_cut_append() {
    let input = vec![append(TERM, 0, 4), append(TERM, 4, 5)];
    let fit = |rpc: &mut AppendEntriesRequest<ShardTypes>| {
        let cut = rpc.entries.len() > 2;
        rpc.entries.truncate(2);
        cut
    };
    let (mut follower, mut out) = Follower::cutting(input, 4, fit);
    assert!(ready(&mut out).is_none());
    assert_eq!(follower.sent(), 1);
    assert_eq!(follower.waiting[0].0.entries.len(), 2);
    follower.take(0);
    assert_acked(&mut out, 2);
    assert_ended(&mut out);
    assert_eq!(follower.sent(), 1);
}
