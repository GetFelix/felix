//! The commit hold, counted through the real router, heartbeat batcher and
//! client on virtual time: what a lone commit costs, how the held commits
//! of many groups share one message, and what ends a hold early. See
//! `docs/per-shard-raft.md`, "A commit goes on the next append".

use futures::StreamExt;
use futures::channel::mpsc::{UnboundedSender, unbounded};
use openraft::RaftNetworkFactory;
use openraft::entry::RaftEntry;
use openraft::network::{RPCOption, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, TransferLeaderRequest};
use openraft::type_config::alias::{EntryOf, LogIdOf};

use super::super::beats::{Beats, counts};
use super::super::network::{Hold, Links, Network, RaftReply, RaftRequest, Reply, Router};
use super::*;
use crate::raft::network::{RaftTransport, Refused};
use crate::raft::types::MemberId;

type Append = AppendEntriesRequest<ShardTypes>;

/// One message as the follower's broker got it.
#[derive(Debug, Clone)]
struct Sent {
    broker: String,
    at: tokio::time::Instant,
    /// Appends in it that carry no entries.
    commits: usize,
    entries: usize,
    transfer: bool,
}

/// Records every message, and accepts every append whole.
#[derive(Default)]
struct Wire(parking_lot::Mutex<Vec<Sent>>);

fn answer(request: &RaftRequest) -> Result<RaftReply, Refused> {
    match &request.body {
        Request::Append(rpc) => {
            let matched = rpc
                .entries
                .last()
                .map(|entry| entry.log_id)
                .or(rpc.prev_log_id);
            Ok(RaftReply(Reply::Append(Ok(matched))))
        }
        Request::TransferLeader(_) => Ok(RaftReply(Reply::TransferLeader(Ok(())))),
        _ => unreachable!("only appends and transfers here"),
    }
}

impl Wire {
    fn record(&self, requests: &[RaftRequest]) {
        let count = |entries: bool| {
            requests
                .iter()
                .filter(|request| match &request.body {
                    Request::Append(rpc) => rpc.entries.is_empty() != entries,
                    _ => false,
                })
                .count()
        };
        self.0.lock().push(Sent {
            broker: requests[0].to().broker.to_string(),
            at: tokio::time::Instant::now(),
            commits: count(false),
            entries: count(true),
            transfer: matches!(requests[0].body, Request::TransferLeader(_)),
        });
    }

    fn sent(&self) -> Vec<Sent> {
        self.0.lock().clone()
    }

    /// Messages that carried a commit-only append, per broker.
    fn commit_messages(&self, broker: &str) -> Vec<Sent> {
        self.sent()
            .into_iter()
            .filter(|sent| sent.broker == broker && sent.commits > 0)
            .collect()
    }
}

#[async_trait]
impl RaftTransport for Wire {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        self.record(std::slice::from_ref(&request));
        answer(&request)
    }

    async fn send_beats(
        &self,
        beats: Vec<RaftRequest>,
        _wait: Duration,
    ) -> Vec<Result<RaftReply, Refused>> {
        self.record(&beats);
        beats.iter().map(answer).collect()
    }
}

const FOLLOWERS: [&str; 2] = ["b", "c"];

/// The leader's side of `groups` groups on one broker, sharing a transport.
struct Rig {
    config: RuntimeConfig,
    wire: Arc<Wire>,
    networks: Vec<Network>,
    base: tokio::time::Instant,
}

impl Rig {
    fn new(groups: u32) -> Self {
        let config = timers();
        let wire = Arc::new(Wire::default());
        let inner: Arc<dyn RaftTransport> = wire.clone();
        let transport = Beats::wrap(inner, config.beat_window);
        let base = tokio::time::Instant::now();
        let networks = (0..groups)
            .map(|n| Network {
                router: Router::new(
                    Arc::clone(&transport),
                    Arc::new(pace().0),
                    Arc::new(super::super::lease::Lease::new(config.lease)),
                    Arc::new(super::super::written::Reports::new()),
                    Arc::new(super::super::reach::Reach::new(base, config.heartbeat)),
                    Arc::new(super::super::reach::Refusals::new(group(n))),
                    Arc::new(config.raft(ShardKind::Stream).expect("timers")),
                ),
                group: group(n),
                commit_hold: Hold {
                    every: config.commit_hold,
                    base,
                },
                links: Arc::new(Links::default()),
            })
            .collect();
        Self {
            config,
            wire,
            networks,
            base,
        }
    }

    fn hold(&self) -> Duration {
        self.config.commit_hold
    }

    /// The longest a follower waits to learn a commit with nothing after it.
    fn bound(&self) -> Duration {
        self.hold() * 2 + self.config.beat_window
    }

    async fn client(&mut self, group: usize, to: &str) -> super::super::network::Client {
        self.networks[group]
            .new_client(MemberId::new(to, 1), &openraft::impls::EmptyNode {})
            .await
    }

    async fn heartbeat_client(&mut self, group: usize, to: &str) -> super::super::network::Client {
        self.networks[group]
            .new_heartbeat_client(MemberId::new(to, 1), &openraft::impls::EmptyNode {})
            .await
    }
}

fn log_id(index: u64) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes>::new(2.into(), index)
}

/// An append under term 2 after `prev`, with entries up to `last`.
fn append(prev: u64, last: u64, commit: u64) -> Append {
    let leader = LeaderIdOf::<ShardTypes>::new(2, MemberId::new("a", 1));
    Append {
        vote: <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leader, true),
        prev_log_id: Some(log_id(prev)),
        leader_commit: Some(log_id(commit)),
        entries: (prev + 1..=last)
            .map(|index| EntryOf::<ShardTypes>::new_blank(log_id(index)))
            .collect(),
    }
}

/// Runs `client`'s stream over what `input` is sent, answering into the
/// returned count until the input ends.
fn stream(
    mut client: super::super::network::Client,
) -> (UnboundedSender<Append>, tokio::task::JoinHandle<usize>) {
    let (tx, rx) = unbounded();
    let task = tokio::spawn(async move {
        let answers = client
            .stream_append(rx, RPCOption::new(Duration::from_secs(1)))
            .await
            .expect("stream");
        answers.count().await
    });
    (tx, task)
}

/// **A lone commit costs one message per follower, one hold later.** With
/// no write after it, the commit-only append waits out its hold on the
/// grid and goes to each follower as a batch of one: one hold timer and one
/// window timer per follower, no task.
#[tokio::test(start_paused = true)]
async fn a_lone_commit_costs_one_message_per_follower() {
    let mut rig = Rig::new(1);
    let before = counts::read();
    let start = tokio::time::Instant::now();
    let mut inputs = Vec::new();
    for to in FOLLOWERS {
        let (tx, task) = stream(rig.client(0, to).await);
        tx.unbounded_send(append(5, 5, 5)).expect("send");
        inputs.push((tx, task));
    }
    tokio::time::sleep(rig.bound() + Duration::from_millis(5)).await;
    let after = counts::read();
    for to in FOLLOWERS {
        let sent = rig.wire.commit_messages(to);
        assert_eq!(sent.len(), 1, "commit-only messages to {to}: {sent:?}");
        let delay = sent[0].at - start;
        assert!(
            delay >= rig.hold() && delay <= rig.bound(),
            "{to} had the commit after {delay:?}"
        );
    }
    assert_eq!(
        (after.0 - before.0, after.1 - before.1, after.2 - before.2),
        (4, 0, 2),
        "(timers, tasks, batches) for one commit to two followers"
    );
    for (tx, task) in inputs {
        drop(tx);
        assert_eq!(task.await.expect("stream"), 1);
    }
}

/// **A write inside the hold carries the commit.** The commit-only append
/// is never sent, and the write is not held at all.
#[tokio::test(start_paused = true)]
async fn a_write_inside_the_hold_sends_no_commit_only_message() {
    let mut rig = Rig::new(1);
    let mut inputs = Vec::new();
    for to in FOLLOWERS {
        let (tx, task) = stream(rig.client(0, to).await);
        tx.unbounded_send(append(5, 5, 5)).expect("send");
        inputs.push((tx, task));
    }
    // Inside the hold, and far past the 1 ms the hold used to be.
    let gap = Duration::from_millis(10);
    tokio::time::sleep(gap).await;
    let wrote = tokio::time::Instant::now();
    for (tx, _) in &inputs {
        tx.unbounded_send(append(5, 6, 5)).expect("send");
    }
    tokio::time::sleep(rig.bound() * 2).await;
    for to in FOLLOWERS {
        let sent: Vec<Sent> = rig
            .wire
            .sent()
            .into_iter()
            .filter(|sent| sent.broker == to)
            .collect();
        assert_eq!(sent.len(), 1, "messages to {to}: {sent:?}");
        assert_eq!((sent[0].commits, sent[0].entries), (0, 1));
        assert_eq!(sent[0].at, wrote, "the write to {to} waited");
    }
    for (tx, task) in inputs {
        drop(tx);
        assert_eq!(task.await.expect("stream"), 1);
    }
}

/// **The held commits of many groups go as one message per follower.** 48
/// groups commit one at a time, spread over one hold. Their holds end on the
/// same grid point, so each follower's broker gets one message with all 48,
/// where each group on its own would have sent 48.
#[tokio::test(start_paused = true)]
async fn the_held_commits_of_many_groups_share_one_message() {
    const GROUPS: u32 = 48;
    let mut rig = Rig::new(GROUPS);
    let mut inputs = Vec::new();
    for n in 0..GROUPS as usize {
        for to in FOLLOWERS {
            inputs.push((n, stream(rig.client(n, to).await)));
        }
    }
    let hold = rig.hold();
    let step = hold / GROUPS;
    for n in 0..GROUPS as usize {
        // Commits at step, 2 * step, ..., one hold: all inside one grid step.
        tokio::time::sleep_until(rig.base + step * (n as u32 + 1)).await;
        for (group, (tx, _)) in &inputs {
            if *group == n {
                tx.unbounded_send(append(5, 5, 5)).expect("send");
            }
        }
    }
    tokio::time::sleep(rig.bound() * 2).await;
    for to in FOLLOWERS {
        let sent = rig.wire.commit_messages(to);
        let carried: usize = sent.iter().map(|sent| sent.commits).sum();
        assert_eq!(
            (sent.len(), carried),
            (1, GROUPS as usize),
            "messages to {to}, and commits in them: {sent:?}"
        );
        let delay = sent[0].at - (rig.base + step);
        assert!(delay <= rig.bound(), "the first commit waited {delay:?}");
    }
    for (_, (tx, task)) in inputs {
        drop(tx);
        assert_eq!(task.await.expect("stream"), 1);
    }
}

/// Sends one heartbeat from group 0's leader to `to`, as openraft's
/// heartbeat worker does, and waits for its answer.
async fn heartbeat(rig: &mut Rig, to: &str, beat: Append) {
    let mut beats = rig.heartbeat_client(0, to).await;
    let answers = beats
        .stream_append(
            Box::pin(futures::stream::once(async { beat })),
            RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("stream");
    assert_eq!(answers.count().await, 1);
}

/// **A heartbeat that carries the held commit replaces it.** The held
/// append is never sent, not even in the heartbeat's batch, and is not
/// sent later when its hold would have ended.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_that_carries_the_held_commit_skips_it() {
    let mut rig = Rig::new(1);
    let skips = counts::skips();
    let (tx, task) = stream(rig.client(0, "b").await);
    tx.unbounded_send(append(5, 5, 5)).expect("send");
    tokio::time::sleep(Duration::from_millis(3)).await;
    heartbeat(&mut rig, "b", append(5, 5, 5)).await;
    tokio::time::sleep(rig.bound() * 2).await;
    let sent = rig.wire.commit_messages("b");
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].commits, 1, "only the heartbeat went: {sent:?}");
    assert_eq!(counts::skips() - skips, 1);
    drop(tx);
    assert_eq!(task.await.expect("stream"), 0);
}

/// **A heartbeat that does not carry the held commit takes it along.** The
/// hold ends, and the held append goes in the heartbeat's batch, so it
/// costs no message of its own.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_behind_the_held_commit_takes_it_along() {
    let mut rig = Rig::new(1);
    let (tx, task) = stream(rig.client(0, "b").await);
    tx.unbounded_send(append(5, 5, 5)).expect("send");
    tokio::time::sleep(Duration::from_millis(3)).await;
    let beat_at = tokio::time::Instant::now();
    // Built before the commit reached 5.
    heartbeat(&mut rig, "b", append(5, 5, 4)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let sent = rig.wire.commit_messages("b");
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].commits, 2, "the held commit rode the heartbeat");
    assert!(sent[0].at - beat_at <= rig.config.beat_window);
    drop(tx);
    assert_eq!(task.await.expect("stream"), 1);
}

/// **A held commit holds up no shutdown, leadership loss or transfer.**
/// openraft ends the stream's input when the leader stops or steps down,
/// and the held append goes at once. A transfer goes on its own call and
/// is not held behind it.
#[tokio::test(start_paused = true)]
async fn a_held_commit_holds_up_no_stop_or_transfer() {
    let mut rig = Rig::new(1);
    let (tx, task) = stream(rig.client(0, "b").await);
    tx.unbounded_send(append(5, 5, 5)).expect("send");
    tokio::time::sleep(Duration::from_millis(3)).await;
    let transferred = tokio::time::Instant::now();
    let mut other = rig.client(0, "b").await;
    let leader = LeaderIdOf::<ShardTypes>::new(2, MemberId::new("a", 1));
    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leader, true);
    let request = TransferLeaderRequest::new(vote, MemberId::new("b", 1), Some(log_id(5)));
    other
        .transfer_leader(request, RPCOption::new(Duration::from_secs(1)))
        .await
        .expect("transferred")
        .expect("taken");
    let transfers: Vec<Sent> = rig.wire.sent().into_iter().filter(|s| s.transfer).collect();
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].at, transferred, "the transfer waited");
    assert!(rig.wire.commit_messages("b").is_empty(), "still held");
    tokio::time::sleep(Duration::from_millis(2)).await;
    let stopped = tokio::time::Instant::now();
    drop(tx);
    assert_eq!(task.await.expect("stream"), 1);
    let sent = rig.wire.commit_messages("b");
    assert_eq!(sent.len(), 1);
    assert!(
        sent[0].at - stopped <= rig.config.beat_window,
        "the stop waited {:?}",
        sent[0].at - stopped
    );
}

/// **Writes flat out never wait.** Appends that carry entries go the
/// instant openraft hands them over, one after another, with a commit-only
/// append between them taken out.
#[tokio::test(start_paused = true)]
async fn writes_flat_out_never_wait() {
    let mut rig = Rig::new(1);
    let (tx, task) = stream(rig.client(0, "b").await);
    let start = tokio::time::Instant::now();
    tx.unbounded_send(append(5, 6, 5)).expect("send");
    tx.unbounded_send(append(6, 6, 6)).expect("send");
    tx.unbounded_send(append(6, 8, 6)).expect("send");
    tx.unbounded_send(append(8, 9, 8)).expect("send");
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    drop(tx);
    assert_eq!(task.await.expect("stream"), 3);
    let sent = rig.wire.sent();
    assert_eq!(sent.len(), 3, "{sent:?}");
    assert!(
        sent.iter()
            .all(|sent| sent.at == start && sent.commits == 0)
    );
}

/// The hold and the heartbeat window together stay under 50 ms.
#[test]
fn the_default_hold_keeps_a_follower_within_50_ms() {
    let config = RuntimeConfig::default();
    assert!(config.commit_hold * 2 + config.beat_window <= Duration::from_millis(50));
    assert!(config.commit_hold >= Duration::from_millis(20));
}

/// **A read on a follower right after a write does not wait out the hold.**
/// With no ticker, the only heartbeats are the ones the leader's read-index
/// round sends, and the follower learns the commit from that round's
/// heartbeat. The hold is long, so a read that waited for the held append
/// would take 150 ms or more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_read_right_after_a_write_is_not_held() {
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
    let leader = cluster.elected(0).await;
    let follower = BROKERS
        .into_iter()
        .find(|broker| **broker != *leader.broker)
        .expect("a follower");
    cluster.write(0, b"first").await;
    // Past the first write's own commit, which may still be held.
    tokio::time::sleep(Duration::from_millis(400)).await;
    for _ in 0..3 {
        cluster.write(0, b"record").await;
        let asked = std::time::Instant::now();
        let read = cluster.runtime(follower).read_barrier(&shard(0)).await;
        let took = asked.elapsed();
        assert_eq!(read, Ok(()));
        assert!(
            took < Duration::from_millis(100),
            "a follower read right after a write took {took:?}"
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    cluster.stop().await;
}
