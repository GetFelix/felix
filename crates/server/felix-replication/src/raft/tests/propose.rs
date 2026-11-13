//! `RaftEngine::propose`: the publish queue, the task that drains it, and
//! what a publisher waits on.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use felix_storage::log::ReadRange;
use felix_storage::raft_log::LogPositions;
use futures::FutureExt;
use openraft::entry::RaftEntry;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::type_config::alias::{EntryOf, LogIdOf};
use openraft_multi::GroupRouter;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::super::network::Router;
use super::super::propose::{
    DRAIN_BYTES, DRAIN_RECORDS, Leading, Lost, Span, Waiting, acknowledged, drain, split,
};
use super::*;
use crate::engine::{CommitError, Written};

fn batch(payloads: &[&'static [u8]], ack: AckAt) -> Batch {
    let records = payloads.iter().map(|payload| Record {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(payload),
    });
    Batch {
        records: records.collect(),
        ack,
        term: None,
    }
}

fn admitted(term: u64, payloads: &[&'static [u8]]) -> Batch {
    Batch {
        term: Some(term),
        ..batch(payloads, AckAt::Flushed)
    }
}

/// A formed group with one committed record, and an engine on its leader.
async fn led() -> (Cluster, MemberId, RaftEngine, u64) {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let committed = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, committed).await;
    let leader = cluster.elected(0).await;
    let engine = RaftEngine::new(cluster.runtime(&leader.broker));
    (cluster, leader, engine, committed)
}

fn status(cluster: &Cluster, member: &MemberId) -> GroupStatus {
    let status = cluster.runtime(&member.broker).status(&shard(0));
    status.expect("status")
}

/// The payloads `broker` holds from offset `start`.
async fn payloads(cluster: &Cluster, broker: &str, start: u64) -> Vec<Bytes> {
    let log = &cluster.logs[&(broker.to_string(), 0)];
    let range = ReadRange {
        start,
        max_bytes: 1 << 20,
    };
    let records = log.read_range_with_trims(range).await.expect("read");
    records.into_iter().map(|record| record.payload).collect()
}

fn lost(result: &Result<Written, CommitError>) -> &'static str {
    match result {
        Err(CommitError::LeadershipLost { detail, .. }) => detail,
        other => panic!("not an unknown outcome: {other:?}"),
    }
}

/// Perf item a. On one thread nothing else runs until the test waits, so
/// the task finds every batch queued and makes one call for all of them.
#[tokio::test]
async fn batches_queued_together_go_to_openraft_in_one_call() {
    let (mut cluster, leader, engine, committed) = led().await;
    assert_eq!(status(&cluster, &leader).client_writes, 0);

    let queued: Vec<_> = (0..16)
        .map(|_| engine.propose(&shard(0), batch(&[b"one", b"two"], AckAt::Flushed)))
        .collect();
    let written = futures::future::join_all(queued).await;

    let term = status(&cluster, &leader).term;
    for (place, written) in written.into_iter().enumerate() {
        let end = committed + 1 + 2 * (place as u64 + 1);
        let generation = term;
        assert_eq!(written, Ok(Written { end, generation }));
    }
    assert_eq!(status(&cluster, &leader).client_writes, 1);
    cluster.stop().await;
}

/// A batch takes its place when `propose` is called. One publisher goes
/// away before its future is polled and one after: both batches are still
/// written, and the batch behind them lands after both.
#[tokio::test]
async fn a_publisher_that_goes_away_keeps_its_place() {
    let (mut cluster, leader, engine, committed) = led().await;

    let never_polled = engine.propose(&shard(0), batch(&[b"a0", b"a1"], AckAt::Flushed));
    let mut polled = Box::pin(engine.propose(&shard(0), batch(&[b"b0"], AckAt::Flushed)));
    let behind = engine.propose(&shard(0), batch(&[b"c0", b"c1", b"c2"], AckAt::Flushed));
    drop(never_polled);
    assert!(futures::poll!(&mut polled).is_pending());
    drop(polled);

    let written = behind.await.expect("written");
    assert_eq!(written.end, committed + 1 + 6);
    cluster.committed_everywhere(0, written.end - 1).await;
    let expected: [&[u8]; 6] = [b"a0", b"a1", b"b0", b"c0", b"c1", b"c2"];
    for broker in BROKERS {
        eventually(|| {
            let flushed = cluster.runtime(broker).status(&shard(0))?.flushed?;
            (flushed.index >= written.end - 1).then_some(())
        })
        .await;
        assert_eq!(
            payloads(&cluster, broker, committed + 1).await,
            expected,
            "{broker}"
        );
    }
    assert_eq!(leader, cluster.elected(0).await, "the leader changed");
    cluster.stop().await;
}

/// A member that does not lead refuses, and appends nothing. So does a
/// shard with no group here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_does_not_lead_refuses_and_appends_nothing() {
    let (mut cluster, new, _, _) = led().await;
    let old = BROKERS.iter().find(|broker| ***broker != *new.broker);
    let old = MemberId::new(*old.expect("a follower"), 1);
    let engine = RaftEngine::new(cluster.runtime(&old.broker));

    let before = status(&cluster, &old).last_index;
    let refused = engine
        .propose(&shard(0), batch(&[b"late"], AckAt::Written))
        .await;
    assert_eq!(lost(&refused), "this broker does not lead the shard");
    tokio::time::sleep(Duration::from_millis(300)).await;
    for member in [&old, &new] {
        assert_eq!(status(&cluster, member).last_index, before, "{member}");
    }
    assert_eq!(status(&cluster, &old).client_writes, 1);

    let nowhere = engine
        .propose(&shard(9), batch(&[b"x"], AckAt::Written))
        .await;
    assert_eq!(lost(&nowhere), "this broker holds no group for the shard");
    cluster.stop().await;
}

/// A leader that cannot commit still answers a publish by the stream's
/// policy: both acknowledgements are of its own log. The commit index does
/// not move, and no local acknowledgement is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ack_follows_the_policy_and_not_the_commit() {
    let (mut cluster, leader, engine, committed) = led().await;
    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.cut.lock().insert((*broker).into());
    }
    let term = status(&cluster, &leader).term;

    let written = engine
        .propose(&shard(0), batch(&[b"p0", b"p1"], AckAt::Written))
        .await;
    let end = committed + 3;
    let generation = term;
    assert_eq!(written, Ok(Written { end, generation }));

    let flushed = engine
        .propose(&shard(0), batch(&[b"q0"], AckAt::Flushed))
        .await;
    let end = committed + 4;
    assert_eq!(flushed, Ok(Written { end, generation }));
    let now = status(&cluster, &leader);
    assert!(now.flushed.is_some_and(|at| at.index >= end - 1));
    assert_eq!(now.committed, Some(committed));

    assert!(engine.admit_local_ack(&shard(0), term).is_err());
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}

/// openraft refuses a write when no majority has answered the leader for
/// its lease, and an idle group heartbeats about that far apart. The task
/// asks for a heartbeat and waits for the answer before it sends the
/// write, so a write after a silence is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_after_a_silence_renews_the_lease_first() {
    let (mut cluster, leader, engine, committed) = led().await;
    let term = status(&cluster, &leader).term;
    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.cut.lock().insert((*broker).into());
    }
    // Past the lease, and short of the followers' election timeout.
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;
    cluster.net.cut.lock().clear();

    let written = engine
        .propose(&shard(0), batch(&[b"heard"], AckAt::Flushed))
        .await;
    let (end, generation) = (committed + 2, term);
    assert_eq!(written, Ok(Written { end, generation }));
    cluster.stop().await;
}

/// With no majority in reach for a whole lease of asking, the write is
/// sent, openraft refuses it, and nothing is appended. Nobody can reach
/// anybody here, so no member is elected meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_no_majority_answers_refuses_and_appends_nothing() {
    let (mut cluster, leader, engine, committed) = led().await;
    cluster.net.cut.lock().extend(BROKERS.map(String::from));
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;

    let asked = tokio::time::Instant::now();
    let refused = engine
        .propose(&shard(0), batch(&[b"unheard"], AckAt::Written))
        .await;
    assert_eq!(
        lost(&refused),
        "no majority has answered this leader lately, so nothing was written"
    );
    assert!(asked.elapsed() >= timers().election_max, "it did not ask");
    assert_eq!(status(&cluster, &leader).last_index, Some(committed));
    assert_eq!(status(&cluster, &leader).role, Role::Leader);
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}

/// A leader that has been deposed and has heard of its successor refuses,
/// and appends nothing anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_leaders_propose_returns_leadership_lost_and_appends_nothing() {
    let (mut cluster, old, engine, _) = led().await;
    let broker = old.broker.to_string();
    cluster.net.cut.lock().insert(broker.clone());
    cluster.net.muted.lock().insert(broker.clone());
    let new = cluster.elected(0).await;
    assert_ne!(new, old);
    let term = status(&cluster, &new).term;
    cluster.net.cut.lock().clear();
    cluster.net.muted.lock().clear();
    eventually(|| {
        let now = status(&cluster, &old);
        let caught_up = now.last_index == status(&cluster, &new).last_index;
        (now.role == Role::Follower && now.term == term && caught_up).then_some(())
    })
    .await;

    let before = status(&cluster, &new).last_index;
    let refused = engine
        .propose(&shard(0), batch(&[b"late"], AckAt::Written))
        .await;
    assert_eq!(lost(&refused), "this broker does not lead the shard");
    tokio::time::sleep(Duration::from_millis(300)).await;
    for member in [&old, &new] {
        assert_eq!(status(&cluster, member).last_index, before, "{member}");
    }
    cluster.stop().await;
}

/// openraft's answer to one call is handed on while the task renews the
/// lease for the next call, which takes a whole lease when no majority
/// answers. On one thread the task has taken the second batch before
/// openraft has answered the first.
#[tokio::test]
async fn an_answer_is_not_held_behind_the_next_calls_lease() {
    let (mut cluster, _, engine, _) = led().await;
    cluster.net.cut.lock().extend(BROKERS.map(String::from));
    tokio::time::sleep(timers().election_max + Duration::from_millis(100)).await;

    let asked = tokio::time::Instant::now();
    let first = tokio::spawn(engine.propose(&shard(0), batch(&[b"first"], AckAt::Written)));
    // The task is renewing the lease for the first batch's call.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = engine.propose(&shard(0), batch(&[b"second"], AckAt::Written));
    let refused = first.await.expect("joined");
    let lapsed = "no majority has answered this leader lately, so nothing was written";
    assert_eq!(lost(&refused), lapsed);
    let lease = timers().election_max;
    let took = asked.elapsed();
    assert!(took < lease + lease / 2, "the first answer took {took:?}");
    assert_eq!(lost(&second.await), lapsed);
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}

/// One call takes no batch that would bring it past the byte or the record
/// bound, so batches with no payload are bounded too. A batch past a bound
/// on its own goes alone, and the batch behind it starts the next call.
#[test]
fn a_drain_stops_at_its_bounds() {
    let sized = |records: usize, bytes: usize| Batch {
        records: (0..records)
            .map(|_| Record {
                timestamp_micros: 1,
                payload: Bytes::from(vec![0; bytes]),
            })
            .collect(),
        ack: AckAt::Written,
        term: None,
    };
    // The batches each drain takes.
    let drains = |batches: Vec<Batch>| {
        let (queue, mut queued) = mpsc::unbounded_channel();
        let mut waiting = Vec::new();
        for batch in batches {
            let (proposal, wait) = split(batch).expect("split");
            assert!(queue.send(proposal).is_ok(), "not queued");
            waiting.push(wait);
        }
        let (mut held, mut drains) = (None, Vec::new());
        while let Some(first) = held.take().or_else(|| queued.try_recv().ok()) {
            drains.push(drain(first, &mut queued, &mut held).answers.len());
        }
        drains
    };
    let mib = 1 << 20;
    let empty = (0..5).map(|_| sized(DRAIN_RECORDS / 4, 0)).collect();
    assert_eq!(drains(empty), [4, 1]);
    let many = vec![sized(DRAIN_RECORDS + 1, 0), sized(1, 0)];
    assert_eq!(drains(many), [1, 1]);
    let large = vec![sized(1, 3 * mib), sized(1, 3 * mib), sized(1, mib)];
    assert_eq!(drains(large), [1, 2]);
    let huge = vec![sized(1, DRAIN_BYTES + 1), sized(1, 0)];
    assert_eq!(drains(huge), [1, 1]);
}

/// The publish task runs where the group does. A first publisher on a
/// runtime that then ends does not take the task with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_publish_task_outlives_the_first_publishers_runtime() {
    let (mut cluster, _, engine, committed) = led().await;
    let elsewhere = engine.clone();
    let first = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let written = runtime.block_on(async {
            elsewhere
                .propose(&shard(0), batch(&[b"first"], AckAt::Flushed))
                .await
        });
        drop(runtime);
        written
    });
    let first = first.join().expect("joined").expect("written");
    assert_eq!(first.end, committed + 2);

    let second = engine
        .propose(&shard(0), batch(&[b"second"], AckAt::Flushed))
        .await;
    assert_eq!(second.map(|written| written.end), Ok(committed + 3));
    cluster.stop().await;
}

/// A member standing for election may win and append what it was sent,
/// so its publisher is told the batch may still be written. Votes are lost
/// here, so the followers stand and none wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_candidates_publisher_is_told_the_batch_may_still_be_written() {
    let (mut cluster, leader, _, _) = led().await;
    let standing = BROKERS.iter().find(|broker| ***broker != *leader.broker);
    let standing = MemberId::new(*standing.expect("a follower"), 1);
    let engine = RaftEngine::new(cluster.runtime(&standing.broker));
    cluster.net.muted.lock().insert(leader.broker.to_string());
    cluster.net.lose_votes.store(true, Relaxed);

    let electing = "this broker is standing for election, so the batch may still be written";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never told while standing"
        );
        eventually(|| (status(&cluster, &standing).role == Role::Candidate).then_some(())).await;
        let told = engine
            .propose(&shard(0), batch(&[b"standing"], AckAt::Written))
            .await;
        // openraft's refusal can come first, or the member may have
        // stopped standing between the two looks.
        if lost(&told) == electing {
            break;
        }
    }
    cluster.net.lose_votes.store(false, Relaxed);
    cluster.net.muted.lock().clear();
    cluster.stop().await;
}

/// A group whose core stops without being closed, as on a storage error,
/// tells its publisher it stopped. On one thread the publisher looks before
/// the task has made the call.
#[tokio::test]
async fn a_publisher_is_told_when_the_groups_core_stops() {
    let (mut cluster, leader, engine, _) = led().await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    member.raft().shutdown().await.expect("shut down");

    let told = engine
        .propose(&shard(0), batch(&[b"x"], AckAt::Flushed))
        .await;
    assert_eq!(lost(&told), "the shard's group stopped");
    drop(member);
    cluster.stop().await;
}

/// The fsync policy places the acknowledgement: only fsync on commit waits
/// for the flush.
#[test]
fn only_fsync_on_commit_waits_for_the_flush() {
    let periodic = FsyncMode::Periodic {
        interval: Duration::from_millis(5),
    };
    assert_eq!(AckAt::for_fsync(&FsyncMode::OnCommit), AckAt::Flushed);
    assert_eq!(AckAt::for_fsync(&periodic), AckAt::Written);
    assert_eq!(AckAt::for_fsync(&FsyncMode::None), AckAt::Written);
}

/// A publisher waiting on a group that is closed is told, without waiting
/// for the group: its core is parked, so nothing else can answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publisher_is_told_when_its_group_stops() {
    let (mut cluster, leader, engine, _) = led().await;
    let runtime = cluster.runtime(&leader.broker);
    let member = runtime.group(&shard(0)).expect("group");
    let (release, parked) = std::sync::mpsc::channel::<()>();
    member
        .raft()
        .external_request(move |_| {
            let _ = parked.recv();
        })
        .await
        .expect("park");

    let mut waiting = Box::pin(engine.propose(&shard(0), batch(&[b"x"], AckAt::Flushed)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(futures::poll!(&mut waiting).is_pending());

    let closing = tokio::spawn(async move { runtime.close(&shard(0)).await });
    let told = tokio::time::timeout(Duration::from_secs(10), waiting).await;
    assert_eq!(lost(&told.expect("told")), "the shard's group stopped");
    let after = engine
        .propose(&shard(0), batch(&[b"y"], AckAt::Flushed))
        .await;
    assert!(after.is_err(), "a closing group took a publish");

    release.send(()).expect("release");
    assert!(closing.await.expect("close"));
    cluster.stop().await;
}

/// A group of one has nobody to send an entry to, so nothing but the
/// publish itself can make it busy again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_puts_an_idle_group_back_on_fast_heartbeats() {
    let idle = RuntimeConfig {
        idle_after: Duration::from_secs(1),
        ..timers()
    };
    let (fast, slow) = (idle.heartbeat, idle.idle_heartbeat);
    let millis = |duration: Duration| duration.as_millis() as u64;
    let mut cluster = Cluster::with(idle, true);
    let runtime = cluster.runtime("a");
    let log = open_log(cluster.root.path(), "a", 0, log_config());
    cluster.logs.insert(("a".into(), 0), log.clone());
    runtime.open(group(0), 1, log).await.expect("open");
    let alone = BTreeSet::from([MemberId::new("a", 1)]);
    runtime
        .initialize(&shard(0), alone)
        .await
        .expect("initialize");
    let member = runtime.group(&shard(0)).expect("group");
    eventually(|| (member.status().role == Role::Leader).then_some(())).await;
    eventually(|| (member.leader_gap() == millis(slow)).then_some(())).await;
    // Let a slow check go by, so the next is a whole idle interval away.
    let seen = member.status().ticks;
    eventually(|| (member.status().ticks > seen).then_some(())).await;

    let before = member.status().ticks;
    RaftEngine::new(runtime.clone())
        .propose(&shard(0), batch(&[b"record"], AckAt::Flushed))
        .await
        .expect("written");
    assert_eq!(member.leader_gap(), millis(fast));
    // Fast checks are 250 ms apart, so 900 ms holds three. Idle ones are
    // 2 s apart, so it would hold none.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let checks = member.status().ticks - before;
    assert!(checks >= 2, "{checks} checks in 900 ms after a publish");
    drop(member);
    cluster.stop().await;
}

/// Sees whether an append left with a token on a command.
#[derive(Default)]
struct Wire {
    appends: std::sync::atomic::AtomicU32,
    token_sent: AtomicBool,
}

#[async_trait]
impl RaftTransport for Wire {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        if let Request::Append(rpc) = &request.body {
            self.appends.fetch_add(1, Relaxed);
            let tokens = rpc.entries.iter().filter(|entry| {
                matches!(&entry.payload, openraft::EntryPayload::Normal(command) if command.written.is_some())
            });
            if tokens.count() > 0 {
                self.token_sent.store(true, Relaxed);
            }
        }
        Err(Refused::Unreachable("only watching".into()))
    }
}

/// A transport inside one process hands a message over as it is. A token
/// that went with it would be resolved by a follower's append.
#[tokio::test]
async fn a_token_does_not_leave_with_an_append() {
    let wire = Arc::new(Wire::default());
    let transport: Arc<dyn RaftTransport> = wire.clone();
    let router = Router::new(transport, Arc::new(pace().0));
    let (token, written) = WrittenToken::channel();
    let command = Command {
        timestamp_micros: 7,
        payload: Bytes::from_static(b"p"),
        written: Some(token),
    };
    let leads = LeaderIdOf::<ShardTypes>::new(3, MemberId::new("a", 1));
    let rpc = AppendEntriesRequest::<ShardTypes> {
        vote: <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true),
        prev_log_id: None,
        entries: vec![EntryOf::<ShardTypes>::new_normal(
            LogIdOf::<ShardTypes>::new(3.into(), 0),
            command,
        )],
        leader_commit: None,
    };
    let option = RPCOption::new(Duration::from_secs(1));
    let sent = router
        .append_entries(MemberId::new("b", 1), group(0), rpc, option)
        .await;
    assert!(sent.is_err());
    assert_eq!(wire.appends.load(Relaxed), 1);
    assert!(!wire.token_sent.load(Relaxed), "a token was sent to a peer");
    assert_eq!(written.await, None, "something resolved the token");
}

/// A group that stops while a call is unanswered ends openraft's stream
/// with one error. Every batch of the call is told, and the stream is not
/// read past its end, which would panic the shard's publish task.
#[tokio::test]
async fn a_call_cut_short_answers_every_batch_as_stopped() {
    use futures::StreamExt;
    use openraft::error::Fatal;
    // As openraft builds it: one error, then the end, then a panic.
    let results = futures::stream::unfold(Some(()), |more| async move {
        more.map(|()| (Err(Fatal::<ShardTypes>::Stopped), None))
    });
    let (first, first_told) = oneshot::channel();
    let (second, second_told) = oneshot::channel();
    super::super::propose::answer(results.boxed(), vec![(2, first), (3, second)], false).await;
    for told in [first_told, second_told] {
        let told = told.await.expect("told");
        assert!(
            matches!(told, Err(CommitError::LeadershipLost { detail, .. }) if detail == "the shard's group stopped"),
            "{told:?}"
        );
    }
}

/// Records that name a term are refused one by one. A batch with a record
/// written and a record refused is told so, not that nothing was written,
/// and the batch behind it gets its own answer.
#[tokio::test]
async fn a_batch_written_in_part_is_told_so() {
    use futures::StreamExt;
    use openraft::error::{ClientWriteError, ForwardToLeader};
    use openraft::raft::ClientWriteResponse;
    let applied = |index| {
        let applied = ClientWriteResponse::<ShardTypes> {
            log_id: LogIdOf::<ShardTypes>::new(3.into(), index),
            data: super::super::types::Applied,
            membership: None,
        };
        Ok(Ok(applied.into()))
    };
    let refused = || {
        Ok(Err(ClientWriteError::ForwardToLeader(
            ForwardToLeader::empty(),
        )))
    };
    let results = futures::stream::iter([applied(7), refused(), refused(), applied(8)]);
    let (first, first_told) = oneshot::channel();
    let (second, second_told) = oneshot::channel();
    let (third, third_told) = oneshot::channel();
    let answers = vec![(2, first), (1, second), (1, third)];
    super::super::propose::answer(results.boxed(), answers, true).await;
    let detail = |told: Result<Span, CommitError>| match told {
        Err(CommitError::LeadershipLost { detail, .. }) => detail,
        other => panic!("not an unknown outcome: {other:?}"),
    };
    assert_eq!(
        detail(first_told.await.expect("told")),
        "only some of the batch's records were written"
    );
    assert_eq!(
        detail(second_told.await.expect("told")),
        "this broker does not lead the term the batch was admitted at, so nothing was written"
    );
    let at = LogPoint { term: 3, index: 8 };
    let whole = Span {
        first: at,
        last: at,
    };
    assert_eq!(third_told.await.expect("told"), Ok(whole));
}

/// One call names one term, so a batch admitted at another starts the next
/// call. It keeps its place.
#[test]
fn a_drain_takes_batches_admitted_at_one_term() {
    let (queue, mut queued) = mpsc::unbounded_channel();
    let mut waiting = Vec::new();
    for term in [Some(4), Some(4), Some(5), None, None] {
        let one = Batch {
            term,
            ..batch(&[b"r"], AckAt::Written)
        };
        let (proposal, wait) = split(one).expect("split");
        assert!(queue.send(proposal).is_ok(), "not queued");
        waiting.push(wait);
    }
    let (mut held, mut drains) = (None, Vec::new());
    while let Some(first) = held.take().or_else(|| queued.try_recv().ok()) {
        let drain = drain(first, &mut queued, &mut held);
        drains.push((drain.term, drain.answers.len()));
    }
    assert_eq!(drains, [(Some(4), 2), (Some(5), 1), (None, 2)]);
}

/// **A batch admitted at one term is not written in the next.** The broker
/// is elected again between the caller's look and the proposal. Written at
/// the new term, the batch would commit while its publisher is told it
/// failed, and a retry would write it twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_admitted_at_an_earlier_term_is_not_written() {
    let (mut cluster, leader, engine, committed) = led().await;
    let open = || engine.open_term(&shard(0));
    let earlier = eventually(open).await;

    // Another member leads for a while and commits a record.
    let old = leader.broker.to_string();
    cluster.net.muted.lock().insert(old.clone());
    cluster.net.cut.lock().insert(old);
    let between = cluster.write(0, b"between").await;
    cluster.net.muted.lock().clear();
    cluster.net.cut.lock().clear();
    eventually(|| (engine.read_bound(&shard(0)).ok()? > between).then_some(())).await;

    // Stand until this broker has the shard open again, in a later term.
    let member = cluster.runtime(&leader.broker).group(&shard(0));
    let member = member.expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while open().is_none_or(|term| term <= earlier) {
        assert!(tokio::time::Instant::now() < deadline, "never led again");
        member.raft().trigger().elect(false).await.expect("elect");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let stale = engine.propose(&shard(0), admitted(earlier, &[b"stale 0", b"stale 1"]));
    assert_eq!(
        lost(&stale.await),
        "this broker does not lead the term the batch was admitted at, so nothing was written"
    );

    // A batch admitted now is written, unless the term moves again.
    let written = loop {
        assert!(tokio::time::Instant::now() < deadline, "never written");
        let Some(term) = open() else {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        };
        let current = admitted(term, &[b"current 0", b"current 1", b"current 2"]);
        match engine.propose(&shard(0), current).await {
            Ok(written) => {
                assert_eq!(written.generation, term);
                break written;
            }
            Err(_) => assert_ne!(open(), Some(term), "refused at its own term"),
        }
    };
    cluster.committed_everywhere(0, written.end - 1).await;
    for broker in BROKERS {
        let held = payloads(&cluster, broker, committed + 1).await;
        assert!(
            !held.iter().any(|payload| payload.starts_with(b"stale")),
            "{broker} holds the stale batch: {held:?}"
        );
    }
    cluster.stop().await;
}

type Token = Pin<Box<dyn Future<Output = Option<LogPoint>> + Send>>;

/// Whether the member leads, and at which term, as a test says.
struct Told(watch::Receiver<(bool, u64)>);

impl Leading for Told {
    async fn lost(&mut self, term: Option<u64>) -> Lost {
        let lost = |&(leads, at): &(bool, u64)| !leads || term.is_some_and(|term| term != at);
        let _ = self.0.wait_for(lost).await;
        Lost::Deposed
    }
}

/// One publisher's wait with every signal in the test's hands.
struct Signals {
    first: Option<oneshot::Sender<LogPoint>>,
    last: Option<oneshot::Sender<LogPoint>>,
    answer: Option<oneshot::Sender<Result<Span, CommitError>>>,
    positions: watch::Sender<LogPositions>,
    leading: watch::Sender<(bool, u64)>,
    stop: CancellationToken,
    wait: Pin<Box<dyn Future<Output = Result<Written, CommitError>> + Send>>,
}

const TERM: u64 = 3;

fn at(term: u64, index: u64) -> LogPoint {
    LogPoint { term, index }
}

/// A batch of three records, expected at indexes 5 to 7 of term 3.
fn waiting(ack: AckAt) -> Signals {
    let token = || {
        let (resolve, written) = oneshot::channel::<LogPoint>();
        let written: Token = Box::pin(async move { written.await.ok() });
        (resolve, written)
    };
    let ((first, first_at), (last, last_at)) = (token(), token());
    let (answer, answered) = oneshot::channel();
    let (positions, flushed) = watch::channel(LogPositions::default());
    let (leading, leads) = watch::channel((true, TERM));
    let stop = CancellationToken::new();
    let waiting = Waiting {
        ack,
        records: 3,
        first: Some(first_at),
        last: last_at,
        answer: answered,
    };
    Signals {
        first: Some(first),
        last: Some(last),
        answer: Some(answer),
        positions,
        leading,
        stop: stop.clone(),
        wait: Box::pin(acknowledged(waiting, flushed, Told(leads), stop)),
    }
}

impl Signals {
    fn written(&mut self, first: LogPoint, last: LogPoint) {
        self.first.take().expect("once").send(first).expect("sent");
        self.last.take().expect("once").send(last).expect("sent");
    }

    fn flushed(&self, flushed: LogPoint) {
        self.positions.send_modify(|at| at.flushed = Some(flushed));
    }

    /// What the wait has answered, if it has.
    fn now(&mut self) -> Option<Result<Written, CommitError>> {
        self.wait.as_mut().now_or_never()
    }
}

const LANDED: Written = Written {
    end: 8,
    generation: TERM,
};

/// A stream that does not fsync on commit acknowledges once written.
#[tokio::test]
async fn a_written_ack_does_not_wait_for_the_flush() {
    let mut signals = waiting(AckAt::Written);
    assert_eq!(signals.now(), None);
    signals.written(at(TERM, 5), at(TERM, 7));
    assert_eq!(signals.now(), Some(Ok(LANDED)));
}

/// A stream that fsyncs on commit waits until the flush covers its last
/// record.
#[tokio::test]
async fn a_flushed_ack_waits_for_the_fsync() {
    let mut signals = waiting(AckAt::Flushed);
    signals.written(at(TERM, 5), at(TERM, 7));
    assert_eq!(signals.now(), None);
    signals.flushed(at(TERM, 6));
    assert_eq!(signals.now(), None);
    signals.flushed(at(TERM, 7));
    assert_eq!(signals.now(), Some(Ok(LANDED)));
}

/// openraft answers when the batch is applied, which is committed. That
/// ends either wait, with the indexes openraft gave.
#[tokio::test]
async fn a_committed_batch_is_acknowledged_whatever_its_log_says() {
    for ack in [AckAt::Written, AckAt::Flushed] {
        let mut signals = waiting(ack);
        if ack == AckAt::Flushed {
            signals.written(at(TERM, 5), at(TERM, 7));
        }
        assert_eq!(signals.now(), None);
        let applied = Span {
            first: at(TERM, 5),
            last: at(TERM, 7),
        };
        let _ = signals.answer.take().expect("once").send(Ok(applied));
        assert_eq!(signals.now(), Some(Ok(LANDED)), "{ack:?}");
    }
}

/// openraft's refusal ends the wait while the token is still out, and
/// after it resolved.
#[tokio::test]
async fn a_refusal_ends_the_wait() {
    let refusal = CommitError::LeadershipLost {
        what: "write",
        detail: "refused",
    };
    for after_written in [false, true] {
        let mut signals = waiting(AckAt::Flushed);
        if after_written {
            signals.written(at(TERM, 5), at(TERM, 7));
        }
        assert_eq!(signals.now(), None);
        let answer = signals.answer.take().expect("once");
        let _ = answer.send(Err(refusal.clone()));
        assert_eq!(signals.now(), Some(Err(refusal.clone())));
    }
}

/// A token has no bound of its own. A publisher whose leader is deposed
/// stops waiting for it, and for the flush after it.
#[tokio::test]
async fn a_deposed_leaders_publisher_stops_waiting() {
    let mut unwritten = waiting(AckAt::Written);
    assert_eq!(unwritten.now(), None);
    unwritten.leading.send_replace((false, TERM));
    let told = unwritten.now().expect("told");
    assert_eq!(lost(&told), "this broker does not lead the shard");

    // Leading again, at a later term: not the leader that wrote the batch.
    let mut unflushed = waiting(AckAt::Flushed);
    unflushed.written(at(TERM, 5), at(TERM, 7));
    assert_eq!(unflushed.now(), None);
    unflushed.leading.send_replace((true, TERM + 1));
    let told = unflushed.now().expect("told");
    assert_eq!(lost(&told), "this broker does not lead the shard");
}

/// A flush under a later term is another leader's. Whether the batch is
/// still under it is not known, so it is not acknowledged.
#[tokio::test]
async fn a_later_terms_flush_acknowledges_nothing() {
    let mut signals = waiting(AckAt::Flushed);
    signals.written(at(TERM, 5), at(TERM, 7));
    signals.flushed(at(TERM + 1, 9));
    let told = signals.now().expect("told");
    assert_eq!(lost(&told), "this broker does not lead the shard");
}

/// Stopping the group ends both waits.
#[tokio::test]
async fn a_stopped_groups_publisher_stops_waiting() {
    for after_written in [false, true] {
        let mut signals = waiting(AckAt::Flushed);
        if after_written {
            signals.written(at(TERM, 5), at(TERM, 7));
        }
        assert_eq!(signals.now(), None);
        signals.stop.cancel();
        let told = signals.now().expect("told");
        assert_eq!(lost(&told), "the shard's group stopped");
    }
}

/// A token that resolves as not written, and tokens that do not name one
/// run of the batch's length, are an unknown outcome. Nothing is guessed
/// from the one index that is known.
#[tokio::test]
async fn a_batch_not_reported_whole_is_an_unknown_outcome() {
    // Not reported written: openraft's answer says why.
    let mut dropped = waiting(AckAt::Written);
    dropped.first.take();
    dropped
        .last
        .take()
        .expect("once")
        .send(at(TERM, 7))
        .expect("sent");
    assert_eq!(dropped.now(), None);
    let refusal = CommitError::LeadershipLost {
        what: "write",
        detail: "refused",
    };
    let answer = dropped.answer.take().expect("once");
    let _ = answer.send(Err(refusal.clone()));
    assert_eq!(dropped.now(), Some(Err(refusal)));

    for (first, last) in [
        (at(TERM, 4), at(TERM, 7)),
        (at(TERM, 6), at(TERM, 7)),
        (at(TERM - 1, 5), at(TERM, 7)),
        (at(TERM, 7), at(TERM, 5)),
    ] {
        let mut signals = waiting(AckAt::Written);
        signals.written(first, last);
        let told = signals.now().expect("told");
        assert_eq!(lost(&told), "the entry was not reported written");
    }
}
