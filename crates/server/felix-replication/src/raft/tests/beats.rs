//! Heartbeats sent together: how many messages idle groups cost a broker,
//! and the lease under the wait. See `docs/per-shard-raft.md`, "Heartbeats
//! sent together".

use std::sync::atomic::Ordering::Relaxed;

use openraft::Instant as _;
use openraft::async_runtime::watch::WatchReceiver;

use super::commit::led_with;
use super::*;

/// Messages a second each broker sends while `groups` groups sit idle,
/// heartbeating at the short gap, with heartbeats held for `window`.
async fn messages_per_broker_second(groups: u32, window: Duration) -> f64 {
    let timers = RuntimeConfig {
        // Every heartbeat at the short gap, so the count does not hang on
        // when the groups slow down.
        idle_heartbeat: timers().heartbeat,
        beat_window: window,
        ..timers()
    };
    let mut cluster = Cluster::with(timers, true);
    for n in 0..groups {
        cluster.form(n).await;
    }
    // Past the elections and their first appends.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let start = cluster.net.messages();
    let began = std::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sent = cluster.net.messages() - start;
    let rate = sent as f64 / began.elapsed().as_secs_f64() / BROKERS.len() as f64;
    cluster.stop().await;
    rate
}

/// The count behind the change: Raft messages per broker per second at a
/// number of idle groups, with heartbeats each on their own and together.
/// `FELIX_TEST_BEAT_GROUPS` sets the number of groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_groups_cost_a_broker_fewer_messages_with_heartbeats_together() {
    let groups = std::env::var("FELIX_TEST_BEAT_GROUPS")
        .ok()
        .and_then(|groups| groups.parse().ok())
        .unwrap_or(24);
    let alone = messages_per_broker_second(groups, Duration::ZERO).await;
    let together = messages_per_broker_second(groups, Duration::from_millis(1)).await;
    println!(
        "{groups} idle groups: {alone:.0} messages/s per broker alone, {together:.0} together"
    );
    assert!(
        together * 4.0 < alone,
        "{together:.0}/s together is not well under {alone:.0}/s alone"
    );
}

/// The lease counts from when openraft handed a heartbeat off, before it
/// waited for others: with every answer from a heartbeat that waited the
/// window, the lease is always at least the window old. A follower's
/// promise starts when it takes the heartbeat, after the wait, so the wait
/// only shortens the lease against that promise and never lengthens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_is_no_younger_than_the_heartbeat_wait() {
    // Far longer than any real window, so the age shows above the noise.
    let window = Duration::from_millis(80);
    let timers = RuntimeConfig {
        beat_window: window,
        ..timers()
    };
    let led = led_with(timers).await;
    let member = led
        .cluster
        .runtime(&led.leader.broker)
        .group(&shard(0))
        .expect("group");
    // Only heartbeats go out from here: no write waits for an answer.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let beats = led.cluster.net.batched_beats.load(Relaxed);
    let mut answers = 0;
    let mut newest = None;
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < until {
        let acked = member.raft().metrics().borrow_watched().last_quorum_acked;
        let acked = acked.expect("the leader was answered");
        let age = acked.elapsed();
        assert!(
            age >= window,
            "a heartbeat that waited {window:?} made the lease {age:?} old"
        );
        if newest != Some(acked) {
            newest = Some(acked);
            answers += 1;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(answers > 3, "only {answers} answers came back");
    assert!(
        led.cluster.net.batched_beats.load(Relaxed) > beats,
        "no heartbeat waited"
    );
    // The lease still holds: the wait is well inside it.
    assert!(
        led.engine.admit_local_ack(&shard(0), led.term).is_ok(),
        "the lease lapsed under a {window:?} wait"
    );
    let mut cluster = led.cluster;
    cluster.stop().await;
}

/// A window as long as the heartbeat would hold every heartbeat past the
/// next one, so the runtime refuses it.
#[test]
fn a_window_as_long_as_the_heartbeat_is_refused() {
    let config = RuntimeConfig {
        beat_window: RuntimeConfig::default().heartbeat,
        ..RuntimeConfig::default()
    };
    assert!(config.raft(crate::ShardKind::Stream).is_err());
}

/// Records when each follower's broker is sent an entry-less append.
#[derive(Default)]
struct Wire(parking_lot::Mutex<Vec<(String, tokio::time::Instant)>>);

#[async_trait]
impl RaftTransport for Wire {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let Request::Append(rpc) = &request.body else {
            unreachable!("only appends here");
        };
        let matched = rpc
            .entries
            .last()
            .map(|entry| entry.log_id)
            .or(rpc.prev_log_id);
        self.0
            .lock()
            .push((request.to().broker.to_string(), tokio::time::Instant::now()));
        Ok(RaftReply(super::network::Reply::Append(Ok(matched))))
    }
}

/// **A lone commit waits once.** On an RF 3 leader, the append that carries
/// only a new commit goes to each follower through openraft's client and
/// the heartbeat window. It waits in the window only, with no hold before
/// it: one timer per follower and no task, and each follower has the
/// commit one window after it.
#[tokio::test(start_paused = true)]
async fn a_lone_commit_reaches_each_follower_after_one_wait() {
    use futures::StreamExt;
    use openraft::RaftNetworkFactory;
    use openraft::network::{RPCOption, RaftNetworkV2};
    use openraft::type_config::alias::LogIdOf;

    let config = timers();
    let wire = Arc::new(Wire::default());
    let inner: Arc<dyn RaftTransport> = wire.clone();
    let transport = super::super::beats::Beats::wrap(inner, config.beat_window);
    let router = super::super::network::Router::new(
        transport,
        Arc::new(pace().0),
        Arc::new(super::super::lease::Lease::new(config.lease)),
        Arc::new(super::super::written::Reports::new()),
        Arc::new(super::super::reach::Reach::new(
            tokio::time::Instant::now(),
            config.heartbeat,
        )),
        Arc::new(super::super::reach::Refusals::new(group(0))),
        Arc::new(config.raft(ShardKind::Stream).expect("timers")),
    );
    let mut network = super::super::network::Network {
        router,
        group: group(0),
        commit_hold: config.commit_hold,
        beat_window: config.beat_window,
    };
    let leader = LeaderIdOf::<ShardTypes>::new(2, MemberId::new("a", 1));
    let at = LogIdOf::<ShardTypes>::new(2.into(), 5);
    let commit = || openraft::raft::AppendEntriesRequest::<ShardTypes> {
        vote: <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leader.clone(), true),
        prev_log_id: Some(at),
        leader_commit: Some(at),
        entries: Vec::new(),
    };
    let mut clients = Vec::new();
    for follower in ["b", "c"] {
        let client = network
            .new_client(MemberId::new(follower, 1), &openraft::impls::EmptyNode {})
            .await;
        clients.push(client);
    }
    let before = super::super::beats::counts::read();
    let start = tokio::time::Instant::now();
    let mut inputs = Vec::new();
    let streams = clients.iter_mut().map(|client| {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tx.unbounded_send(commit()).expect("send");
        inputs.push(tx);
        async move {
            let mut answers = client
                .stream_append(rx, RPCOption::new(Duration::from_secs(1)))
                .await
                .expect("stream");
            answers.next().await.expect("an answer").expect("taken")
        }
    });
    let answers = futures::future::join_all(streams).await;
    let after = super::super::beats::counts::read();
    assert!(answers.iter().all(|answer| *answer == Ok(Some(at))));
    let (timers, tasks) = (after.0 - before.0, after.1 - before.1);
    assert_eq!(
        (timers, tasks),
        (2, 0),
        "waits and tasks for one commit to two followers"
    );
    let mut sent = wire.0.lock().clone();
    sent.sort();
    assert_eq!(sent.len(), 2);
    for (follower, at) in sent {
        let delay = at - start;
        assert!(
            delay >= config.beat_window && delay < config.beat_window * 2,
            "{follower} had the commit after {delay:?}"
        );
    }
}
