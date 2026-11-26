//! #3263: a leader's openraft core runs every log storage call inline, so a
//! call that waits on the disk stops its heartbeats and writes for as long.
//! These measure how long the core goes without answering while one of
//! the leader's flushes is held, as an fsync on a slow disk holds it.

use std::time::Instant;

use super::*;

/// How long the leader's flushes are held: one slow fsync.
const SLOW_FLUSH: Duration = Duration::from_secs(3);

/// Hold `log`'s flushes for [`SLOW_FLUSH`] from when this returns, as one
/// slow fsync would.
async fn hold_flushes_a_while(log: DiskLog) {
    let (held, holding) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let guard = log.hold_flushes().await;
        let _ = held.send(());
        tokio::time::sleep(SLOW_FLUSH).await;
        drop(guard);
    });
    holding.await.expect("held");
}

/// Ask `raft`'s core something every 10 ms for `window`, and return the
/// longest it took to answer.
async fn longest_answer(
    raft: &openraft::Raft<ShardTypes, machine::Machine>,
    window: Duration,
) -> Duration {
    let until = Instant::now() + window;
    let mut longest = Duration::ZERO;
    while Instant::now() < until {
        let asked = Instant::now();
        let _ = tokio::time::timeout(PATIENCE, raft.with_raft_state(|_| ())).await;
        longest = longest.max(asked.elapsed());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    longest
}

/// Write one record through `raft`: how long the answer took, and the
/// error when it did not commit.
async fn commit_time(
    raft: &openraft::Raft<ShardTypes, machine::Machine>,
) -> (Duration, Option<String>) {
    let asked = Instant::now();
    let written = raft.client_write(Command {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"during"),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: false,
        publisher: None,
        headers: None,
        written: None,
        cond: None,
    });
    let answer = tokio::time::timeout(PATIENCE, written)
        .await
        .expect("answered");
    (asked.elapsed(), answer.err().map(|err| err.to_string()))
}

/// The leader of a written-commit group (periodic fsync), with one follower
/// cut off, its log snapshotted through `last`, and its flushes held.
/// Returns the leader's raft, its log and the index the snapshot covers.
async fn slow_leader(
    cluster: &mut Cluster,
) -> (
    MemberId,
    openraft::Raft<ShardTypes, machine::Machine>,
    DiskLog,
    u64,
) {
    cluster.form_relaxed(0, &BROKERS).await;
    let last = cluster.write_many(0, 300).await;
    cluster.committed_everywhere(0, last).await;
    let leader = cluster.elected(0).await;
    cluster.snapshot(&leader.broker, 0, last).await;
    let raft = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group")
        .raft()
        .clone();
    let log = cluster.logs[&(leader.broker.to_string(), 0)].clone();
    (leader, raft, log, last)
}

/// Control: with nothing but appends and heartbeats, a held flush does not
/// reach the core of a written-commit leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_flush_alone_leaves_a_written_commit_leader_answering() {
    let mut cluster = Cluster::new(true);
    let (leader, raft, log, _) = slow_leader(&mut cluster).await;
    let follower = BROKERS
        .iter()
        .find(|broker| **broker != &*leader.broker)
        .expect("a follower");
    cluster.net.cut.lock().insert((*follower).into());

    hold_flushes_a_while(log).await;
    let (longest, commit) = tokio::join!(longest_answer(&raft, SLOW_FLUSH), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        commit_time(&raft).await
    });
    eprintln!("no purge: longest core answer {longest:?}, commit {commit:?}");
    assert!(
        longest < Duration::from_millis(500),
        "the core waited {longest:?}"
    );
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}

/// **A purge does not wait on the flush lock on the core** (#3263). openraft runs a leader's purge on
/// the core, and holds one back while a replication stream reads the range,
/// so a purge asked for while a follower is going away runs when that
/// follower's stream fails. A purge inside the log must not wait on the
/// flush: the core keeps answering and a write commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_core_keeps_answering_while_a_purge_waits_on_a_slow_flush() {
    let mut cluster = Cluster::new(true);
    let (leader, raft, log, last) = slow_leader(&mut cluster).await;
    let follower = BROKERS
        .iter()
        .find(|broker| **broker != &*leader.broker)
        .expect("a follower");
    cluster.net.cut.lock().insert((*follower).into());

    hold_flushes_a_while(log).await;
    raft.trigger()
        .purge_log(last / 2)
        .await
        .expect("purge asked");
    let (longest, commit) = tokio::join!(longest_answer(&raft, SLOW_FLUSH), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        commit_time(&raft).await
    });
    eprintln!("purge: longest core answer {longest:?}, commit {commit:?}");
    assert!(
        longest < Duration::from_millis(500),
        "the leader's core answered nothing for {longest:?} while its purge waited on a flush"
    );
    assert!(
        commit.0 < Duration::from_millis(500) && commit.1.is_none(),
        "a write: {commit:?}"
    );
    let deadline = Instant::now() + PATIENCE;
    while cluster.purged(&leader.broker, 0) < Some(last / 2) {
        assert!(Instant::now() < deadline, "the purge never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cluster.net.cut.lock().clear();
    cluster.stop().await;
}
