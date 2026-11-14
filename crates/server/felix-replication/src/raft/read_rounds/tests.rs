use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::*;

/// The answer, or a failure instead of a hang if none comes.
async fn answered(read: Entered<u64>) -> Option<u64> {
    tokio::time::timeout(Duration::from_secs(10), read.answer())
        .await
        .expect("the read was told something")
}

/// Rounds the test answers. Each round asked sends the test its number
/// and a slot, and answers what the test puts in the slot.
struct Scripted {
    rounds: Arc<ReadRounds<u64>>,
    started: mpsc::UnboundedSender<(u64, oneshot::Sender<u64>)>,
    calls: Arc<AtomicU64>,
}

impl Scripted {
    fn new() -> (Self, mpsc::UnboundedReceiver<(u64, oneshot::Sender<u64>)>) {
        let (started, asked) = mpsc::unbounded_channel();
        let scripted = Self {
            rounds: Arc::default(),
            started,
            calls: Arc::default(),
        };
        (scripted, asked)
    }

    fn enter(&self) -> Entered<u64> {
        let started = self.started.clone();
        let calls = Arc::clone(&self.calls);
        self.rounds
            .enter(&tokio::runtime::Handle::current(), move || {
                let started = started.clone();
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                async move {
                    let (answer, answered) = oneshot::channel();
                    started.send((n, answer)).expect("test is listening");
                    answered.await.expect("test answers")
                }
            })
    }
}

/// The bug batching invites: a read joins a round that was already asked
/// before the read took its value, and is confirmed by an answer that does
/// not cover it. Each round here answers its own number, so a read that
/// gets a round asked before it arrived is caught.
#[tokio::test]
async fn a_read_that_arrives_during_a_round_waits_for_the_next_one() {
    let (scripted, mut asked) = Scripted::new();
    let early = scripted.enter();
    let (first, answer_first) = asked.recv().await.expect("a round started");
    assert_eq!(first, 1);

    // Arrives after round 1 was asked.
    let late = scripted.enter();
    let mut late = tokio::spawn(late.answer());
    answer_first.send(first).expect("round 1 is waiting");
    assert_eq!(early.answer().await, Some(1));

    let (second, answer_second) = tokio::select! {
        biased;
        started = asked.recv() => started.expect("a second round"),
        answered = &mut late => {
            panic!("the late read was answered by a round asked before it: {answered:?}")
        }
    };
    assert_eq!(second, 2);
    answer_second.send(second).expect("round 2 is waiting");
    assert_eq!(late.await.expect("join"), Some(2));
    assert_eq!(scripted.rounds.asked(), 2);
}

/// Every read that arrives during one round shares the next. Reads that
/// arrive together cost two rounds at most, however many there are.
#[tokio::test]
async fn reads_that_arrive_during_one_round_share_the_next() {
    let (scripted, mut asked) = Scripted::new();
    let first = scripted.enter();
    let (_, answer_first) = asked.recv().await.expect("a round started");
    let waiting: Vec<_> = (0..64)
        .map(|_| tokio::spawn(scripted.enter().answer()))
        .collect();
    answer_first.send(1).expect("round 1 is waiting");
    assert_eq!(first.answer().await, Some(1));

    let (second, answer_second) = asked.recv().await.expect("a second round");
    answer_second.send(second).expect("round 2 is waiting");
    for read in waiting {
        assert_eq!(read.await.expect("join"), Some(2));
    }
    assert_eq!(scripted.rounds.asked(), 2);
    assert!(asked.try_recv().is_err(), "a third round started");
}

/// Once no read waits, the task ends; the next read starts a new one.
#[tokio::test]
async fn a_read_after_the_rounds_end_starts_a_new_one() {
    let (scripted, mut asked) = Scripted::new();
    let read = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a round started");
    answer.send(n).expect("waiting");
    assert_eq!(read.answer().await, Some(1));

    let read = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a new round started");
    answer.send(n).expect("waiting");
    assert_eq!(read.answer().await, Some(2));
}

/// Reads that arrive before the round is asked share it. One that gives
/// up does not take the round from the others.
#[tokio::test]
async fn reads_that_arrive_before_a_round_is_asked_share_it() {
    let (scripted, mut asked) = Scripted::new();
    let gives_up = scripted.enter();
    let stays = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a round started");
    drop(gives_up);
    answer.send(n).expect("waiting");
    assert_eq!(stays.answer().await, Some(1));
    assert_eq!(scripted.rounds.asked(), 1);
}

/// A round whose task ends without an answer tells its reads, and the
/// reads waiting for the round after it, nothing. Reads that come later
/// are not stuck behind it.
#[tokio::test]
async fn reads_are_not_stuck_behind_a_round_that_ended_without_an_answer() {
    let (scripted, mut asked) = Scripted::new();
    let first = scripted.enter();
    let (_, answer_first) = asked.recv().await.expect("a round started");
    let next = scripted.enter();
    // The round's task panics on the dropped slot and ends.
    drop(answer_first);
    assert_eq!(answered(first).await, None);
    assert_eq!(answered(next).await, None);

    let later = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a new round started");
    answer.send(n).expect("waiting");
    assert_eq!(answered(later).await, Some(n));
}

/// A round every read gave up on before it was asked is not asked.
#[tokio::test]
async fn a_round_no_read_waits_for_is_not_asked() {
    let (scripted, mut asked) = Scripted::new();
    let first = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a round started");
    drop(scripted.enter());
    answer.send(n).expect("waiting");
    assert_eq!(first.answer().await, Some(1));

    let read = scripted.enter();
    let (n, answer) = asked.recv().await.expect("a new round started");
    answer.send(n).expect("waiting");
    assert_eq!(read.answer().await, Some(2));
    assert_eq!(scripted.rounds.asked(), 2);
}

/// A runtime that shuts down drops the task without running it. Its reads
/// are told nothing, and a read on a live runtime starts a new task.
#[test]
fn a_task_dropped_before_it_ran_does_not_strand_later_reads() {
    let rounds: Arc<ReadRounds<u64>> = Arc::default();
    let gone = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let stranded = rounds.enter(gone.handle(), || async { 1 });
    drop(gone);

    let live = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");
    live.block_on(async {
        assert_eq!(answered(stranded).await, None);
        let read = rounds.enter(&tokio::runtime::Handle::current(), || async { 2 });
        assert_eq!(answered(read).await, Some(2));
    });
}
