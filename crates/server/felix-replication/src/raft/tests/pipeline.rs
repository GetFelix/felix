//! Several appends on their way to one follower at once: whatever order
//! they arrive or run in, no leader reads a follower as having lost what
//! it acknowledged, and every member ends with the same log.

use std::sync::atomic::Ordering::Relaxed;

use felix_storage::log::ReadRange;

use super::*;

/// Commit `count` records with distinct payloads, a few at a time so the
/// leader has appends to send while others are unanswered. Returns the
/// last index.
async fn pump(cluster: &Cluster, count: u64) -> u64 {
    let mut last = cluster.write(0, b"first").await;
    let mut next = 0;
    while next < count {
        let leader = cluster.elected(0).await;
        let Some(member) = cluster.runtime(&leader.broker).group(&shard(0)) else {
            continue;
        };
        let writes = (next..count.min(next + 40)).map(|record| {
            let member = Arc::clone(&member);
            async move {
                // Spread out, so they do not all leave in one append.
                tokio::time::sleep(Duration::from_micros(record % 40 * 150)).await;
                let command = Command {
                    timestamp_micros: 1_700_000_000,
                    payload: Bytes::from(format!("record {record}")),
                    written: None,
                    producer: None,
                    expected: None,
                };
                member.raft().client_write(command).await
            }
        });
        for written in futures::future::join_all(writes)
            .await
            .into_iter()
            .flatten()
        {
            last = last.max(written.log_id.index);
        }
        next += 40;
    }
    last.max(cluster.write(0, b"last").await)
}

/// Every member holds the same records in the same order.
async fn assert_same_logs(cluster: &Cluster) {
    let mut logs = Vec::new();
    for broker in BROKERS {
        let range = ReadRange {
            start: 0,
            max_bytes: 1 << 26,
        };
        let records = cluster.logs[&(broker.to_string(), 0)]
            .read_range_with_trims(range)
            .await
            .expect("read");
        let payloads: Vec<Bytes> = records.into_iter().map(|record| record.payload).collect();
        logs.push(payloads);
    }
    assert!(logs[0].len() > 2, "nothing was written");
    for (broker, log) in BROKERS.iter().zip(&logs).skip(1) {
        assert_eq!(log.len(), logs[0].len(), "{broker} holds another count");
        let differs = log.iter().zip(&logs[0]).position(|(a, b)| a != b);
        assert_eq!(differs, None, "{broker} and a differ at this offset");
    }
}

/// The peer listener answers each append in a task of its own. Taken in
/// the order they came, they reach the log in that order whichever task
/// runs first, so no follower answers one with a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_taken_in_order_reach_the_log_in_order() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let term = cluster.statuses(0)[0].term;
    cluster.net.disorder(Disorder::LateTasks);

    let last = pump(&cluster, 400).await;
    cluster.committed_everywhere(0, last).await;

    assert_eq!(cluster.net.conflicts.load(Relaxed), 0);
    assert_eq!(cluster.statuses(0)[0].term, term);
    assert_same_logs(&cluster).await;
    cluster.stop().await;
}

/// An append that arrives ahead of the one sent before it is answered
/// with a conflict at entries the leader then hears are held. The leader
/// does not read that as a follower that lost them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_that_overtakes_the_one_ahead_is_not_a_reversion() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let term = cluster.statuses(0)[0].term;
    cluster.net.disorder(Disorder::LateArrivals);

    let last = pump(&cluster, 400).await;
    *cluster.net.disorder.lock() = None;
    cluster.committed_everywhere(0, last).await;

    assert!(cluster.net.conflicts.load(Relaxed) > 0, "none overtook");
    assert_eq!(cluster.statuses(0)[0].term, term, "a leader was lost");
    assert_same_logs(&cluster).await;
    cluster.stop().await;
}

/// An append is lost while those sent behind it arrive, so the follower
/// truly lacks what they follow. The leader sends from the lost one again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_behind_a_lost_append_is_sent_again() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let term = cluster.statuses(0)[0].term;
    cluster.net.disorder(Disorder::Lost);

    let last = pump(&cluster, 400).await;
    *cluster.net.disorder.lock() = None;
    cluster.committed_everywhere(0, last).await;

    assert!(
        cluster.net.conflicts.load(Relaxed) > 0,
        "none arrived behind"
    );
    assert_eq!(cluster.statuses(0)[0].term, term, "a leader was lost");
    assert_same_logs(&cluster).await;
    cluster.stop().await;
}

/// A follower stops and starts again with appends on their way to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_restarted_under_appends_ends_with_the_same_log() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let follower = BROKERS
        .into_iter()
        .find(|broker| *broker != &*leader.broker)
        .expect("a follower");
    cluster.net.disorder(Disorder::LateTasks);

    let mut last = 0;
    for _ in 0..3 {
        let restart = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(cluster.runtime(follower).close(&shard(0)).await);
            follower
        };
        let (written, follower) = tokio::join!(pump(&cluster, 200), restart);
        last = last.max(written);
        let log = cluster
            .logs
            .remove(&(follower.to_string(), 0))
            .expect("log");
        log.close().await.expect("close");
        let log = open_log(cluster.root.path(), follower, 0, log_config());
        cluster.logs.insert((follower.to_string(), 0), log.clone());
        let opened = cluster.runtime(follower).open(group(0), 1, log).await;
        opened.expect("open");
    }
    let last = last.max(cluster.write(0, b"after").await);
    cluster.committed_everywhere(0, last).await;

    assert_same_logs(&cluster).await;
    cluster.stop().await;
}
