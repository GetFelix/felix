use std::sync::atomic::{AtomicUsize, Ordering};

use openraft::EntryPayload;
use openraft::raft::AppendEntriesRequest;
use openraft::type_config::alias::{EntryOf, LeaderIdOf, LogIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use tokio::time::Instant;

use super::*;
use crate::raft::network::{Ask, Beat, Reply};
use crate::raft::types::{GroupId, MemberId, ShardTypes};
use crate::{ShardKey, ShardKind};

const WINDOW: Duration = Duration::from_millis(1);

fn group(n: u32) -> GroupId {
    GroupId::new(
        ShardKey {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "s".into(),
            shard: n,
            kind: ShardKind::Stream,
        },
        0,
    )
}

fn append(n: u32, to: &str, entries: Vec<EntryOf<ShardTypes>>) -> RaftRequest {
    let leader = LeaderIdOf::<ShardTypes>::new(1, MemberId::new("a", 1));
    RaftRequest {
        group: group(n),
        to: MemberId::new(to, 1),
        beat: Beat::default(),
        report: Ask::Nothing,
        deadline: Duration::from_secs(1),
        body: Request::Append(AppendEntriesRequest {
            vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
            prev_log_id: None,
            entries,
            leader_commit: None,
        }),
    }
}

fn beat(n: u32, to: &str) -> RaftRequest {
    append(n, to, Vec::new())
}

/// The answer that names group `n`, so a test can tell whose answer it got.
fn answer_for(n: u32) -> Answer {
    let at = LogIdOf::<ShardTypes>::new(1.into(), u64::from(n));
    Ok(RaftReply(Reply::Append(Ok(Some(at)))))
}

fn named(answer: &Answer) -> Option<u64> {
    match answer {
        Ok(RaftReply(Reply::Append(Ok(Some(at))))) => Some(at.index),
        _ => None,
    }
}

/// A transport that answers each group with its own number, and counts
/// messages: one per `send`, one per `send_beats`.
#[derive(Default)]
struct Counting {
    messages: AtomicUsize,
    batches: parking_lot::Mutex<Vec<Vec<u32>>>,
    /// The group whose heartbeat this transport fails.
    fails: Option<u32>,
}

#[async_trait]
impl RaftTransport for Counting {
    async fn send(&self, request: RaftRequest) -> Answer {
        self.messages.fetch_add(1, Ordering::SeqCst);
        answer_for(request.group.shard.shard)
    }

    async fn send_beats(&self, beats: Vec<RaftRequest>, _wait: Duration) -> Vec<Answer> {
        self.messages.fetch_add(1, Ordering::SeqCst);
        let groups: Vec<u32> = beats.iter().map(|beat| beat.group.shard.shard).collect();
        self.batches.lock().push(groups.clone());
        groups
            .into_iter()
            .map(|n| match Some(n) == self.fails {
                true => Err(Refused::Unreachable("lost".into())),
                false => answer_for(n),
            })
            .collect()
    }
}

#[tokio::test(start_paused = true)]
async fn heartbeats_to_one_broker_within_the_window_go_as_one_message() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let sent = (0..8).map(|n| beats.send(beat(n, "b")));
    let answers = futures::future::join_all(sent).await;
    // Each caller got its own group's answer.
    for (n, answer) in answers.iter().enumerate() {
        assert_eq!(named(answer), Some(n as u64));
    }
    assert_eq!(inner.messages.load(Ordering::SeqCst), 1);
    assert_eq!(*inner.batches.lock(), vec![(0..8).collect::<Vec<_>>()]);
}

#[tokio::test(start_paused = true)]
async fn each_broker_gets_its_own_message() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let sent = (0..6).map(|n| beats.send(beat(n, if n % 2 == 0 { "b" } else { "c" })));
    let answers = futures::future::join_all(sent).await;
    for (n, answer) in answers.iter().enumerate() {
        assert_eq!(named(answer), Some(n as u64));
    }
    let mut batches = inner.batches.lock().clone();
    batches.sort();
    assert_eq!(batches, vec![vec![0, 2, 4], vec![1, 3, 5]]);
}

#[tokio::test(start_paused = true)]
async fn a_heartbeat_waits_no_longer_than_the_window() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let start = Instant::now();
    let _ = beats.send(beat(0, "b")).await;
    assert!(start.elapsed() <= WINDOW + Duration::from_millis(1));
    // A heartbeat after the window starts a new message.
    let _ = beats.send(beat(1, "b")).await;
    assert_eq!(*inner.batches.lock(), vec![vec![0], vec![1]]);
}

#[tokio::test(start_paused = true)]
async fn an_append_with_entries_goes_at_once_on_its_own() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), Duration::from_secs(60));
    let entry = EntryOf::<ShardTypes> {
        log_id: LogIdOf::<ShardTypes>::new(1.into(), 1),
        payload: EntryPayload::Blank,
    };
    let start = Instant::now();
    let answer = beats.send(append(3, "b", vec![entry])).await;
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "an append with entries waited"
    );
    assert_eq!(named(&answer), Some(3));
    assert!(inner.batches.lock().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_full_batch_goes_without_waiting_out_the_window() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), Duration::from_secs(60));
    let start = Instant::now();
    let sent = (0..MAX_BEATS as u32).map(|n| beats.send(beat(n, "b")));
    let answers = futures::future::join_all(sent).await;
    assert_eq!(start.elapsed(), Duration::ZERO, "a full batch waited");
    assert!(answers.iter().all(Result::is_ok));
    assert_eq!(inner.batches.lock().len(), 1);
    // One more starts the next batch, which waits for its window.
    let _ = beats.send(beat(0, "b")).await;
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    let lens: Vec<_> = inner.batches.lock().iter().map(Vec::len).collect();
    assert_eq!(lens, vec![MAX_BEATS, 1]);
}

#[tokio::test(start_paused = true)]
async fn one_groups_failure_fails_only_its_own_heartbeat() {
    let inner = Arc::new(Counting {
        fails: Some(2),
        ..Default::default()
    });
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let answers = futures::future::join_all((0..5).map(|n| beats.send(beat(n, "b")))).await;
    for (n, answer) in answers.iter().enumerate() {
        match n {
            2 => assert!(matches!(answer, Err(Refused::Unreachable(_)))),
            _ => assert_eq!(named(answer), Some(n as u64)),
        }
    }
}

#[tokio::test]
async fn a_zero_window_sends_each_on_its_own() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), Duration::ZERO);
    let _ = futures::future::join_all((0..4).map(|n| beats.send(beat(n, "b")))).await;
    assert_eq!(inner.messages.load(Ordering::SeqCst), 4);
    assert!(inner.batches.lock().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_answers_keep_the_heartbeats_order() {
    let beats: Vec<_> = (0..6).map(|n| beat(n, "b")).collect();
    // Later groups answer sooner.
    let answers = answer_each(beats, WINDOW, |beat| async move {
        let n = beat.group.shard.shard;
        tokio::time::sleep(Duration::from_micros(u64::from(100 * (6 - n)))).await;
        answer_for(n)
    })
    .await;
    let named: Vec<_> = answers.iter().map(named).collect();
    assert_eq!(named, (0..6).map(|n| Some(n as u64)).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_silent_member_holds_the_others_up_for_one_wait_at_most() {
    let beats: Vec<_> = (0..4).map(|n| beat(n, "b")).collect();
    let start = Instant::now();
    let answers = answer_each(beats, WINDOW, |beat| async move {
        let n = beat.group.shard.shard;
        if n == 1 {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        answer_for(n)
    })
    .await;
    assert_eq!(start.elapsed(), Duration::from_millis(5) + WINDOW);
    for (n, answer) in answers.iter().enumerate() {
        match n {
            1 => assert!(
                matches!(answer, Err(Refused::Slow(to)) if to.group == group(1)),
                "{:?}",
                answer.as_ref().err()
            ),
            _ => assert_eq!(named(answer), Some(n as u64)),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_slow_broker_is_waited_for_until_its_first_answer() {
    let beats: Vec<_> = (0..3).map(|n| beat(n, "b")).collect();
    // Every member takes far longer than the wait: the broker is slow, so
    // none is answered as slow.
    let answers = answer_each(beats, WINDOW, |beat| async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        answer_for(beat.group.shard.shard)
    })
    .await;
    assert!(answers.iter().all(Result::is_ok));
}

/// A window opened for one heartbeat costs one timer and no task: its
/// opener waits it out and sends it.
#[tokio::test(start_paused = true)]
async fn a_lone_heartbeat_costs_one_timer_and_no_task() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let before = counts::read();
    let start = Instant::now();
    let answer = beats.send(beat(4, "b")).await;
    let after = counts::read();
    assert_eq!(named(&answer), Some(4));
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    assert!(start.elapsed() >= WINDOW && start.elapsed() < WINDOW * 2);
    assert_eq!(*inner.batches.lock(), vec![vec![4]]);
}

/// Many groups' commits to one broker, each one its client may take back,
/// still go as one message.
#[tokio::test(start_paused = true)]
async fn held_commits_to_one_broker_go_as_one_message() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let sent = (0..8).map(|n| holding(Held::new(), beats.send(beat(n, "b"))));
    let answers = futures::future::join_all(sent).await;
    for (n, answer) in answers.iter().enumerate() {
        assert_eq!(named(answer), Some(n as u64));
    }
    assert_eq!(*inner.batches.lock(), vec![(0..8).collect::<Vec<_>>()]);
}

/// A heartbeat taken back is left out of its batch, and the others go at
/// the window's end even when the one taken back opened the window.
#[tokio::test(start_paused = true)]
async fn a_commit_taken_back_is_left_out_and_the_rest_still_go() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let opener = Held::new();
    let mut first = Box::pin(holding(Arc::clone(&opener), beats.send(beat(0, "b"))));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let rest = futures::future::join_all((1..4).map(|n| beats.send(beat(n, "b"))));
    let taking_back = async {
        tokio::task::yield_now().await;
        assert!(opener.withdraw(), "a commit in its window is taken back");
        drop(first);
    };
    let start = Instant::now();
    let (answers, ()) = tokio::join!(rest, taking_back);
    assert!(start.elapsed() >= WINDOW && start.elapsed() < WINDOW * 2);
    for (n, answer) in answers.iter().enumerate() {
        assert_eq!(named(answer), Some(n as u64 + 1));
    }
    assert_eq!(*inner.batches.lock(), vec![vec![1, 2, 3]]);
}

/// A window whose only heartbeat is taken back sends nothing, and once a
/// batch is taken to be sent nothing in it can be taken back.
#[tokio::test(start_paused = true)]
async fn a_window_with_nothing_left_sends_nothing() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let held = Held::new();
    let mut send = Box::pin(holding(Arc::clone(&held), beats.send(beat(0, "b"))));
    assert!(futures::poll!(send.as_mut()).is_pending());
    assert!(held.withdraw());
    drop(send);
    tokio::time::sleep(WINDOW * 3).await;
    assert_eq!(inner.messages.load(Ordering::SeqCst), 0);

    let held = Held::new();
    let answer = holding(Arc::clone(&held), beats.send(beat(1, "b"))).await;
    assert_eq!(named(&answer), Some(1));
    assert!(!held.withdraw(), "a sent commit was taken back");
}

/// A window whose opener was taken back, but that still runs to the end of
/// the window, sends the heartbeat left in it with its own answer to its
/// own caller and nothing of the opener's.
#[tokio::test(start_paused = true)]
async fn an_opener_taken_back_does_not_take_the_answer_left_in_its_window() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let opener = Held::new();
    let mut first = Box::pin(holding(Arc::clone(&opener), beats.send(beat(1, "b"))));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let mut second = Box::pin(beats.send(beat(2, "b")));
    assert!(futures::poll!(second.as_mut()).is_pending());
    tokio::time::sleep(WINDOW * 2).await;
    assert!(opener.withdraw());
    // The opener runs once more before its caller drops it.
    let _ = futures::poll!(first.as_mut());
    drop(first);
    let answer = second.await;
    assert_eq!(
        named(&answer),
        Some(2),
        "the heartbeat left lost its answer"
    );
    assert_eq!(*inner.batches.lock(), vec![vec![2]]);
}

/// An opener dropped once its window has ended, before it ran again to
/// send, leaves the others to the guard, which sends them at once.
#[tokio::test(start_paused = true)]
async fn an_opener_dropped_after_its_window_still_gets_the_others_sent() {
    let inner = Arc::new(Counting::default());
    let beats = Beats::wrap(inner.clone(), WINDOW);
    let mut first = Box::pin(beats.send(beat(1, "b")));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let mut second = Box::pin(beats.send(beat(2, "b")));
    assert!(futures::poll!(second.as_mut()).is_pending());
    tokio::time::sleep(WINDOW * 2).await;
    let ended = Instant::now();
    drop(first);
    let answer = second.await;
    assert_eq!(named(&answer), Some(2));
    assert_eq!(ended.elapsed(), Duration::ZERO, "the others waited");
    assert_eq!(*inner.batches.lock(), vec![vec![1, 2]]);
}
