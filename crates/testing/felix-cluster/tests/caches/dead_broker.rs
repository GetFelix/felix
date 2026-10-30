//! A cluster client that outlives the broker owning a cache shard: a watch
//! open on it carries on, and requests stop going to it.
//!
//! Run with `cargo test -p felix-cluster --test caches dead_broker::`.
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_client::{
    CacheCondition, CacheWatchFilter, CacheWatchItem, Client, ShardedCacheWatchItem,
};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig};
use serial_test::serial;

use super::cache_failover::{failover_from, replication_settled};

const CACHE: &str = "sessions";
const KEY: &str = "k";

fn replicated_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        caches: vec![CacheSpec::replicated(CACHE, 1, 3)],
        ..Default::default()
    }
}

/// The cluster, the shard's owner, and a cluster client seeded only with a
/// broker that does not own it, after `before` is written and replicated.
async fn start_with_value(before: &[u8]) -> (Cluster, String, Arc<Client>) {
    let cluster = Cluster::start(replicated_cache())
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
        .cache_put_via(&leader, CACHE, KEY, before)
        .await
        .expect("cache put");
    replication_settled(&cluster, &leader, shipped_before).await;

    let seed = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a broker that does not own the shard");
    let client = felix_cluster::client::connect_cluster(
        &[cluster.node(&seed).expect("seed").client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    (cluster, leader, Arc::new(client))
}

/// **A watch on a killed owner carries on at the promoted one.** The watch
/// is open on the owner when it dies, so nothing but the transport says the
/// connection is gone. It must not end or hang: a change written after the
/// failover arrives on the same handle.
#[serial]
#[tokio::test]
async fn a_cache_watch_survives_losing_its_broker() {
    let (mut cluster, leader, client) = start_with_value(b"before").await;
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let mut watch = client
        .watch_cache_retained(
            &tenant,
            &namespace,
            CACHE,
            CacheWatchFilter::Key(KEY.to_string()),
        )
        .await
        .expect("watch");
    match tokio::time::timeout(Duration::from_secs(10), watch.recv()).await {
        Ok(Some(CacheWatchItem::Change(change))) => {
            assert_eq!(change.value.as_deref(), Some(&b"before"[..]));
        }
        other => panic!("expected the retained value, got {other:?}"),
    }

    cluster.kill_node(&leader).expect("kill the owner");
    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted"
    );
    let writer = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a surviving broker");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a put after the failover",
        || async {
            cluster
                .cache_put_via(&writer, CACHE, KEY, b"after")
                .await
                .is_ok()
        },
    )
    .await
    .expect("put after the failover");

    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    loop {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(CacheWatchItem::Change(change)))
                if change.value.as_deref() == Some(&b"after"[..]) =>
            {
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!(
                "the watch ended after its broker died instead of following: {:?}",
                watch.error()
            ),
            Err(_) => panic!("the watch never delivered the change written after the failover"),
        }
    }
    assert_eq!(watch.reconnects(), 1, "reopened once, after the kill");

    cluster.shutdown().await;
}

/// **Cache requests stop waiting on a killed owner.** The client learned the
/// owner from a put before the kill. Once it fails to connect there, it asks
/// again rather than waiting out a connect timeout on every later request.
#[serial]
#[tokio::test]
async fn cache_requests_stop_going_to_a_killed_owner() {
    let (mut cluster, leader, client) = start_with_value(b"before").await;
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    client
        .cache_put(&tenant, &namespace, CACHE, KEY, "routed".into(), None)
        .await
        .expect("a put that learns the owner");

    cluster.kill_node(&leader).expect("kill the owner");
    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted"
    );
    // A client that sends nothing meanwhile notices the dead connection only
    // through its idle timeout. Until then a request fails on the open
    // connection, which already drops the route; after it, connecting fails,
    // and that is the case under test.
    let dead = cluster.node(&leader).expect("leader").client_addr;
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the connection to the dead owner to time out",
        || async {
            !client
                .connections_per_node()
                .await
                .iter()
                .any(|(addr, live)| *addr == dead && *live > 0)
        },
    )
    .await
    .expect("the dead owner's connection should close");
    // The first request after that may still try the dead owner.
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a put after the failover",
        || async {
            client
                .cache_put(&tenant, &namespace, CACHE, KEY, "first".into(), None)
                .await
                .is_ok()
        },
    )
    .await
    .expect("put after the failover");

    for i in 0..3 {
        let started = Instant::now();
        let condition = match client
            .cache_get_versioned(&tenant, &namespace, CACHE, KEY)
            .await
            .expect("read after the failover")
        {
            Some(current) => CacheCondition::Version(current.version),
            None => CacheCondition::Absent,
        };
        let claimed = client
            .cache_put_if(
                &tenant,
                &namespace,
                CACHE,
                KEY,
                format!("claim-{i}").into(),
                None,
                condition,
            )
            .await
            .expect("a claim after the failover");
        assert!(claimed.applied, "claim {i} was refused: {claimed:?}");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "request {i} took {elapsed:?}: still waiting on the dead owner"
        );
    }

    cluster.shutdown().await;
}

/// **A shard that loses its broker mid-state does not complete the state
/// early.** The killed owner's shard holds more retained values than the
/// watcher's client buffers, and the watcher stops reading after its first
/// value, so the kill lands inside that shard's state phase. `StateComplete`
/// must wait for the reopened shard's full state.
#[serial]
#[tokio::test]
async fn a_sharded_retained_watch_restarts_a_state_phase_cut_by_a_kill() {
    const KEYS: usize = 3000;
    // About 24 MiB a shard, past the 16 MiB stream window the watcher's
    // client is held to, so the killed owner cannot have sent its whole state
    // before it dies.
    const VALUE_BYTES: usize = 16 * 1024;

    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        caches: vec![CacheSpec::replicated(CACHE, 2, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
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
    let addrs: Vec<_> = cluster
        .node_ids()
        .iter()
        .map(|node| cluster.node(node).expect("node").client_addr)
        .collect();
    let writer = Arc::new(
        felix_cluster::client::connect_cluster(&addrs, &tenant, &cluster.client_token())
            .await
            .expect("connect writer"),
    );
    let value = bytes::Bytes::from(vec![7u8; VALUE_BYTES]);
    for start in (0..KEYS).step_by(64) {
        let mut puts = tokio::task::JoinSet::new();
        for i in start..(start + 64).min(KEYS) {
            let (writer, tenant, namespace, value) = (
                Arc::clone(&writer),
                tenant.clone(),
                namespace.clone(),
                value.clone(),
            );
            puts.spawn(async move {
                writer
                    .cache_put(
                        &tenant,
                        &namespace,
                        CACHE,
                        &format!("p:{i:05}"),
                        value,
                        None,
                    )
                    .await
            });
        }
        while let Some(put) = puts.join_next().await {
            put.expect("put task").expect("cache put");
        }
    }
    replication_settled(&cluster, &leader, shipped_before).await;

    let seed = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a broker that does not own shard 0");
    let watcher = Arc::new(
        felix_cluster::client::connect_cluster_with(
            &[cluster.node(&seed).expect("seed").client_addr],
            &tenant,
            &cluster.client_token(),
            |config| {
                config.client_sub_queue_capacity = 1;
                // A shared connection's floor, the transport default, is then
                // what bounds a stream.
                config.event_stream_recv_window = 0;
                config.cache_stream_recv_window = 0;
            },
        )
        .await
        .expect("connect watcher"),
    );
    let mut watch = watcher
        .watch_cache_sharded_retained(&tenant, &namespace, CACHE, "p:")
        .await
        .expect("watch");
    assert_eq!(watch.retained_count(), Some(KEYS as u64));

    let mut seen = HashSet::new();
    let mut delivered = 0usize;
    let mut record = |item: &ShardedCacheWatchItem| {
        if let ShardedCacheWatchItem::Change { change, .. } = item {
            seen.insert(change.key.clone());
            delivered += 1;
        }
    };
    loop {
        let item = tokio::time::timeout(Duration::from_secs(10), watch.recv())
            .await
            .expect("a retained value")
            .expect("the watch ended");
        record(&item);
        if matches!(item, ShardedCacheWatchItem::Change { shard: 0, .. }) {
            break;
        }
    }

    cluster.kill_node(&leader).expect("kill shard 0's owner");
    assert!(
        failover_from(&cluster, &leader, Duration::from_secs(30)).await,
        "no replica was promoted"
    );

    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(90));
    loop {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(ShardedCacheWatchItem::StateComplete)) => break,
            Ok(Some(ShardedCacheWatchItem::ShardClosed { shard, error })) => {
                panic!("shard {shard} closed instead of following: {error:?}")
            }
            Ok(Some(item)) => record(&item),
            Ok(None) => panic!("the watch ended before its state was complete"),
            Err(_) => panic!("the state never completed after the kill"),
        }
    }
    assert_eq!(
        seen.len(),
        KEYS,
        "StateComplete arrived with {} of {KEYS} keys delivered",
        seen.len()
    );
    assert!(
        delivered > KEYS,
        "only {delivered} values arrived, so the kill missed the state phase and tested nothing"
    );

    cluster.shutdown().await;
}
