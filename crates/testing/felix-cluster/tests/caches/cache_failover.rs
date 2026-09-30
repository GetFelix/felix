//! A cache survives losing the broker that owned its shard — the last
//! acceptance criterion of shared caches (#278).
//!
//! A cache is a log, so replicating one is the same machinery that replicates a
//! stream — the leader ships records at their offsets and a promoted follower
//! serves what it was shipped. What makes it worth its own test is that the
//! cache reaches those records through an index rather than by replaying, and
//! a follower's index is built from a log that grew underneath it.
//!
//! Run with `cargo test -p felix-cluster --test caches cache_failover::`.
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, Fault, StreamSpec};
use serial_test::serial;

const CACHE: &str = "sessions";

/// One cache shard held by all three brokers, so killing any one leaves two
/// that still have it.
fn replicated_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::replicated(CACHE, 1, 3)],
        ..Default::default()
    }
}

/// Wait until the leader has shipped past `before` and reports no lag.
///
/// Killing a leader that has shipped nothing tests startup, not failover. The
/// baseline has to be sampled before the write, because the counter only moves
/// on a real exchange and an idle pass leaves it alone.
async fn replication_settled(cluster: &Cluster, leader: &str, before: f64) {
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the cache write to reach every follower",
        || async {
            let shipped = cluster
                .metric(leader, "felix_broker_replication_shipped_total")
                .await
                .ok()
                .flatten()
                .unwrap_or(0.0);
            let lag = cluster
                .metric(leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten();
            shipped > before && matches!(lag, Some(lag) if lag == 0.0)
        },
    )
    .await
    .expect("a healthy replicated cache shard should reach its followers");
}

/// Wait until the control plane has named an owner other than `gone`.
///
/// Kept free of anything slow. Placement only promotes a replica the leader
/// reported caught up, and that report expires — so the window opens when the
/// dead leader is finally declared not-live and closes a couple of seconds
/// later. A loop that also tried to *read* between attempts spends the window
/// waiting on a broker that cannot answer yet, and misses it.
///
/// The harness's control plane does not run the reconciler on a timer, so a
/// failure test has to step placement itself.
async fn failover_from(cluster: &Cluster, gone: &str, budget: Duration) -> bool {
    felix_cluster::wait::until(budget, "a new cache shard owner", || async {
        cluster.place_shards().await;
        match cluster.shard_owner_of("cache", CACHE, 0).await {
            Ok(owner) => owner != gone,
            Err(_) => false,
        }
    })
    .await
    .is_ok()
}

/// Read a key until some broker answers, or the budget runs out.
///
/// Being named owner and being ready to serve are different moments: a promoted
/// broker learns of its own promotion through the same watch as everything
/// else, and has to open the shard before it can answer for it.
async fn read_until(
    cluster: &Cluster,
    readers: &[String],
    key: &str,
    budget: Duration,
) -> Option<Vec<u8>> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        for reader in readers {
            if let Ok(Some(value)) = cluster.cache_get_via(reader, CACHE, key).await {
                return Some(value);
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **The acceptance criterion.** A value written before the owning broker dies
/// is still readable afterwards.
#[serial]
#[tokio::test]
async fn a_cache_value_survives_the_loss_of_its_owner() {
    let mut cluster = Cluster::start(replicated_cache())
        .await
        .expect("start cluster");

    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let shipped_before = cluster
        .metric(&leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);

    cluster
        .cache_put_via(&leader, CACHE, "k", b"survives")
        .await
        .expect("cache put");
    // Readable before the kill, so a failure afterwards is about the failover
    // rather than about the write never landing.
    assert_eq!(
        cluster
            .cache_get_via(&leader, CACHE, "k")
            .await
            .expect("cache get"),
        Some(b"survives".to_vec()),
    );

    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted after {leader} died",
    );

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &leader)
        .collect();

    let read = read_until(&cluster, &survivors, "k", Duration::from_secs(45)).await;
    assert_eq!(
        read.as_deref(),
        Some(&b"survives"[..]),
        "the cache lost its contents when {leader} died",
    );
}

/// Retained state until some survivor serves it, or the budget runs out.
///
/// A watch is redirected rather than forwarded, so most survivors answer
/// `not_leader` — the loop simply tries each until it lands on the promoted
/// owner, exactly as a redirect-following client would.
async fn retained_watch_until(
    cluster: &Cluster,
    survivors: &[String],
    key: &str,
    budget: Duration,
) -> Option<(
    (felix_client::Client, felix_client::CacheWatch),
    u64,
    Vec<u8>,
)> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        for node in survivors {
            let Ok((client, mut watch)) = cluster.cache_watch_retained_via(node, CACHE, key).await
            else {
                continue;
            };
            if watch.retained_count() != Some(1) {
                continue;
            }
            let item = tokio::time::timeout(Duration::from_secs(5), watch.recv()).await;
            if let Ok(Some(felix_client::CacheWatchItem::Change(change))) = item {
                let value = change.value.map(|value| value.to_vec()).unwrap_or_default();
                return Some(((client, watch), change.offset, value));
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **The failover criterion of retained watches (#349).** A promoted
/// replica serves the retained value
/// from its rebuilt index — and the watch is *live* on it: a write after the
/// failover reaches the watcher that joined after it.
#[serial]
#[tokio::test]
async fn a_retained_watch_survives_the_loss_of_the_owner() {
    let mut cluster = Cluster::start(replicated_cache())
        .await
        .expect("start cluster");

    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let shipped_before = cluster
        .metric(&leader, "felix_broker_replication_shipped_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0);

    cluster
        .cache_put_via(&leader, CACHE, "k", b"retained")
        .await
        .expect("cache put");
    replication_settled(&cluster, &leader, shipped_before).await;
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted after {leader} died",
    );

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &leader)
        .collect();

    let Some(((_client, mut watch), offset, value)) =
        retained_watch_until(&cluster, &survivors, "k", Duration::from_secs(45)).await
    else {
        panic!("no promoted replica served the retained value after {leader} died");
    };
    assert_eq!(
        value.as_slice(),
        b"retained",
        "the promoted replica served the wrong retained value",
    );

    // And the watch is live on the promoted owner: a write after failover
    // reaches the watcher that joined after it, at a later offset.
    cluster
        .cache_put_via(&survivors[0], CACHE, "k", b"after-failover")
        .await
        .expect("cache put after failover");
    let live = tokio::time::timeout(Duration::from_secs(15), watch.recv())
        .await
        .expect("the promoted watch went quiet")
        .expect("the promoted watch ended");
    match live {
        felix_client::CacheWatchItem::Change(change) => {
            assert_eq!(change.value.as_deref(), Some(&b"after-failover"[..]));
            assert!(
                change.offset > offset,
                "the promoted log rewound its offsets",
            );
        }
        other => panic!("expected a live change, got {other:?}"),
    }
}

/// Read a counter until some survivor serves the expected sum.
async fn counter_until(
    cluster: &Cluster,
    readers: &[String],
    key: &str,
    expected: i64,
    budget: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + budget;
    loop {
        for reader in readers {
            if let Ok(Some(sum)) = cluster.counter_get_via(reader, CACHE, key).await
                && sum == expected
            {
                return true;
            }
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **The failover criterion of counters (#350).** The sum survives losing
/// the broker that
/// accumulated it — the promoted replica folds the true sum from its shipped
/// log — and keeps counting from there: the counter is live state on the
/// replacement, not a relic.
#[serial]
#[tokio::test]
async fn a_counter_survives_the_loss_of_its_owner() {
    let mut cluster = Cluster::start(replicated_cache())
        .await
        .expect("start cluster");

    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");

    let mut expected = 0i64;
    for delta in [30, -5, 17] {
        expected += delta;
        let sum = cluster
            .counter_add_via(&leader, CACHE, "views", delta)
            .await
            .expect("counter add");
        assert_eq!(sum, expected, "the running answer drifted before the kill");
    }

    // A pass for the counter log to reach the replicas, then take the leader
    // out.
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted after {leader} died",
    );

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &leader)
        .collect();

    assert!(
        counter_until(
            &cluster,
            &survivors,
            "views",
            expected,
            Duration::from_secs(45)
        )
        .await,
        "the promoted replica lost the sum (wanted {expected})",
    );

    // And it keeps counting: an add lands on the promoted owner and continues
    // from the true sum rather than restarting it.
    let continued = cluster
        .counter_add_via(&survivors[0], CACHE, "views", 8)
        .await
        .expect("counter add after failover");
    assert_eq!(
        continued,
        expected + 8,
        "the promoted replica restarted the count",
    );
}

fn quorum_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::quorum(CACHE, 1, 3)],
        ..Default::default()
    }
}

/// A `Quorum` cache does not acknowledge a write no majority holds. With both
/// followers paused the leader has the write and nobody to confirm it, and
/// saying it succeeded would promise survival it cannot deliver.
#[serial]
#[tokio::test]
async fn a_quorum_cache_write_without_a_majority_is_refused() {
    let cluster = Cluster::start(quorum_cache()).await.expect("start cluster");
    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != &leader)
        .collect();

    for follower in &followers {
        cluster.pause_node(follower).expect("pause");
    }
    let outcome = cluster
        .cache_put_via(&leader, CACHE, "k", b"no-majority")
        .await;
    for follower in &followers {
        let _ = cluster.resume_node(follower);
    }

    assert!(
        outcome.is_err(),
        "a Quorum cache write was acknowledged with no majority available",
    );
    cluster.shutdown().await;
}

/// **A `Quorum`-acknowledged cache write survives its leader**, with no wait
/// for replication between the acknowledgement and the kill: the
/// acknowledgement itself is the promise that a majority holds it.
#[serial]
#[tokio::test]
async fn a_quorum_acknowledged_cache_write_survives_its_leader() {
    let mut cluster = Cluster::start(quorum_cache()).await.expect("start cluster");
    let leader = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");

    cluster
        .cache_put_via(&leader, CACHE, "k", b"survives")
        .await
        .expect("cache put under quorum");
    cluster.kill_node(&leader).expect("kill the owner");

    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "a replica should have been promoted",
    );
    let readers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != &leader)
        .collect();
    let value = read_until(&cluster, &readers, "k", Duration::from_secs(30)).await;
    assert_eq!(
        value.as_deref(),
        Some(&b"survives"[..]),
        "a quorum-acknowledged cache write did not survive its leader",
    );
    cluster.shutdown().await;
}

/// **A deposed cache leader rejoins and keeps what was committed (#863).**
///
/// The leader is cut off from its followers and takes one more write, so it
/// dies holding a record no majority has. The follower promoted in its place
/// writes at that offset too. When the old leader comes back it has to drop
/// that one record and follow; before cache leaders recorded where their
/// generation began, it could not tell the record from committed history,
/// halted, and refused the rebuild the new leader then asked for, for good.
#[serial]
#[tokio::test]
async fn a_deposed_cache_leader_rejoins_after_a_failover() {
    let mut cluster = Cluster::start(quorum_cache()).await.expect("start cluster");
    let old = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != &old)
        .collect();

    let mut committed = Vec::new();
    for i in 0..5 {
        let key = format!("before-{i}");
        cluster
            .cache_put_via(&old, CACHE, &key, key.as_bytes())
            .await
            .expect("cache put under quorum");
        committed.push(key);
    }

    // One more write that only the old leader holds.
    let cut = Fault::Refuse {
        node: old.clone(),
        peers: followers.clone(),
    };
    cluster.inject(&cut).await.expect("cut the followers off");
    let alone = tokio::time::timeout(
        Duration::from_secs(20),
        cluster.cache_put_via(&old, CACHE, "alone", b"no-majority"),
    )
    .await;
    assert!(
        !matches!(alone, Ok(Ok(()))),
        "a write only the leader holds was acknowledged",
    );
    cluster.kill_node(&old).expect("kill the old leader");
    cluster.heal(&cut).await.expect("heal");

    assert!(
        failover_from(&cluster, &old, Duration::from_secs(30)).await,
        "no replica was promoted after {old} died",
    );
    let new = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("new owner");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the promoted leader to take writes",
        || async {
            cluster
                .cache_put_via(&new, CACHE, "after-0", b"after-0")
                .await
                .is_ok()
        },
    )
    .await
    .expect("the promoted leader never took a write");
    committed.push("after-0".to_string());
    for i in 1..5 {
        let key = format!("after-{i}");
        cluster
            .cache_put_via(&new, CACHE, &key, key.as_bytes())
            .await
            .expect("cache put on the promoted leader");
        committed.push(key);
    }

    cluster
        .restart_node(&old)
        .await
        .expect("restart the old leader");

    // Failover left the dead broker out of the replica set. Draining the other
    // follower brings it back as the replacement, so the new leader ships to
    // it and has to get past the record it holds alone.
    let other = followers
        .iter()
        .find(|id| **id != new)
        .expect("a third broker")
        .clone();
    cluster
        .drain_node(&other)
        .await
        .expect("drain the other follower");
    let level = felix_cluster::wait::until(
        Duration::from_secs(60),
        "the old leader to rejoin the replica set",
        || async {
            cluster.place_shards().await;
            let halted = cluster
                .metric(&new, "felix_broker_replication_halted")
                .await
                .ok()
                .flatten()
                .unwrap_or(0.0);
            let report = cluster.cache_replica_report(CACHE, 0).await.ok().flatten();
            // The report leaves out a halted or stalled follower.
            halted == 0.0
                && report.is_some_and(|report| {
                    report.leader_offset.is_some()
                        && report.offsets.get(&old).copied() == report.leader_offset
                })
        },
    )
    .await;
    let report = cluster.cache_replica_report(CACHE, 0).await.ok().flatten();
    assert!(
        level.is_ok(),
        "{old} did not rejoin after the failover; last report {report:?}",
    );

    // Drain the new leader too, so the only place the shard can go is the
    // broker that rejoined, and read every committed key from it there.
    cluster
        .drain_node(&new)
        .await
        .expect("drain the new leader");
    cluster
        .drain_until_empty(&new, 1, Duration::from_secs(60))
        .await
        .expect("the drain completes");
    let owner = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("owner after the drain");
    assert_eq!(owner, old, "the drain did not hand the shard to {old}");
    for key in &committed {
        let value = read_until(
            &cluster,
            std::slice::from_ref(&owner),
            key,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(
            value.as_deref(),
            Some(key.as_bytes()),
            "{owner} lost the committed key {key}",
        );
    }
    cluster.shutdown().await;
}
