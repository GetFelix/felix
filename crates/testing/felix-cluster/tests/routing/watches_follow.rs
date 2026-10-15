//! Cache watches through a `ClusterClient`, one shard or all of them, follow
//! their shard when it moves rather than ending.
//!
//! Run with `cargo test -p felix-cluster --test routing watches_follow::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_client::{CacheWatchFilter, CacheWatchItem, ShardedCacheWatchItem};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig};
use serial_test::serial;
use tokio::sync::Mutex;

const CACHE: &str = "sessions";
const KEY: &str = "session-1";

/// A key and the value written to it.
type Write = (String, Vec<u8>);

/// **Every acknowledged write, in order, across the switch-over.** A watch
/// reads one key from the start of the cache log while a writer keeps
/// updating it and the shard moves to another broker. Without being reopened,
/// it sees every acknowledged value in write order and none twice.
#[serial]
#[tokio::test]
async fn a_cache_watch_follows_its_shard_to_the_new_owner() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        caches: vec![CacheSpec::new(CACHE, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let addr = |node: &str| cluster.node(node).expect("node").client_addr;

    let mut owed: Vec<Vec<u8>> = Vec::new();
    for i in 0..20 {
        let value = format!("before-{i}").into_bytes();
        felix_cluster::wait::until(Duration::from_secs(30), "the cache put to land", || async {
            cluster
                .cache_put_via(&owner, CACHE, KEY, &value)
                .await
                .is_ok()
        })
        .await
        .expect("put before the move");
        owed.push(value);
    }

    // Opened on the owner, so the move is what the watch has to survive.
    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&owner)],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut watch = reader
        .watch_cache(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            CacheWatchFilter::Key(KEY.to_string()),
            Some(0),
        )
        .await
        .expect("watch");

    // Writes through the other broker, which forwards to whichever broker
    // leads, for the whole move.
    let writer =
        felix_cluster::client::connect(addr(&other), &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let putter = {
        let (acknowledged, stop) = (Arc::clone(&acknowledged), Arc::clone(&stop));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let value = format!("during-{i}").into_bytes();
                i += 1;
                // Only acknowledged writes are owed.
                if writer
                    .cache_put(
                        &tenant,
                        &namespace,
                        CACHE,
                        KEY,
                        bytes::Bytes::from(value.clone()),
                        None,
                    )
                    .await
                    .is_ok()
                {
                    acknowledged.lock().await.push(value);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.drain_node(&owner).await.expect("drain");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the cache shard to move",
        || async {
            cluster.place_shards().await;
            cluster
                .shard_owner_of("cache", CACHE, 0)
                .await
                .is_ok_and(|now| now == other)
        },
    )
    .await
    .expect("move");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    putter.await.expect("putter");
    owed.extend(acknowledged.lock().await.iter().cloned());
    assert!(
        owed.len() > 20,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    let mut offsets = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut moves = 0;
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|value| values.contains(value)) {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(CacheWatchItem::Change(change))) => {
                offsets.push(change.offset);
                values.push(change.value.expect("never deleted").to_vec());
            }
            Ok(Some(CacheWatchItem::ShardMoved(_))) => moves += 1,
            Ok(Some(CacheWatchItem::Lagged { resume_from })) => {
                panic!("the watch lagged (resume from {resume_from}), which is not a move")
            }
            Ok(None) => panic!(
                "the watch ended after {} changes instead of following the shard",
                values.len()
            ),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged writes",
                owed.iter().filter(|v| values.contains(v)).count(),
                owed.len()
            ),
        }
    }

    assert!(moves > 0, "the watch never saw its shard move");
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "offsets must rise across the move: {offsets:?}"
    );
    let mut unique = values.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), values.len(), "a change arrived twice");
    // A write that failed on the client can still have been applied, so the
    // watch may see values nobody was told about; the owed ones keep their order.
    let seen_owed: Vec<&Vec<u8>> = values.iter().filter(|v| owed.contains(v)).collect();
    let owed_refs: Vec<&Vec<u8>> = owed.iter().collect();
    assert_eq!(seen_owed, owed_refs, "acknowledged writes out of order");
    cluster.shutdown().await;
}

/// **A sharded watch follows each shard.** A prefix watch over a two-shard
/// cache reads from the start while a writer keeps updating keys on both
/// shards and one broker is drained. Every acknowledged value reaches the
/// watch in its key's write order, none twice, and no shard's watch ends.
#[serial]
#[tokio::test]
async fn a_sharded_cache_watch_follows_each_shard_to_its_new_owner() {
    const SHARDS: u32 = 2;
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        caches: vec![CacheSpec::new(CACHE, SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let drained = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &drained)
        .expect("two brokers");
    let addr = |node: &str| cluster.node(node).expect("node").client_addr;
    let key = |i: usize| format!("k{}", i % 8);

    let mut owed: Vec<Write> = Vec::new();
    for i in 0..16 {
        let value = format!("before-{i}").into_bytes();
        felix_cluster::wait::until(Duration::from_secs(30), "the cache put to land", || async {
            cluster
                .cache_put_via(&other, CACHE, &key(i), &value)
                .await
                .is_ok()
        })
        .await
        .expect("put before the move");
        owed.push((key(i), value));
    }

    let reader = Arc::new(
        felix_cluster::client::connect_cluster(
            &[addr(&drained)],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut watch = reader
        .watch_cache_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            "k",
            Some((0..SHARDS).map(|shard| (shard, 0)).collect()),
        )
        .await
        .expect("watch");

    let writer =
        felix_cluster::client::connect(addr(&other), &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("writer");
    let acknowledged: Arc<Mutex<Vec<Write>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let putter = {
        let (acknowledged, stop) = (Arc::clone(&acknowledged), Arc::clone(&stop));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let (key, value) = (key(i), format!("during-{i}").into_bytes());
                i += 1;
                if writer
                    .cache_put(
                        &tenant,
                        &namespace,
                        CACHE,
                        &key,
                        bytes::Bytes::from(value.clone()),
                        None,
                    )
                    .await
                    .is_ok()
                {
                    acknowledged.lock().await.push((key, value));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.drain_node(&drained).await.expect("drain");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the drained broker's cache shards to move",
        || async {
            cluster.place_shards().await;
            cluster.cache_shard_owners(CACHE).await.is_ok_and(|owners| {
                owners.len() == SHARDS as usize && owners.values().all(|node| node == &other)
            })
        },
    )
    .await
    .expect("move");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    putter.await.expect("putter");
    owed.extend(acknowledged.lock().await.iter().cloned());
    assert!(
        owed.len() > 16,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    let mut seen: Vec<Write> = Vec::new();
    let mut moves = 0;
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|write| seen.contains(write)) {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(ShardedCacheWatchItem::Change { change, .. })) => {
                seen.push((change.key, change.value.expect("never deleted").to_vec()));
            }
            Ok(Some(ShardedCacheWatchItem::ShardMoved { .. })) => moves += 1,
            Ok(Some(other)) => panic!("a shard's watch ended: {other:?}"),
            Ok(None) => panic!("the watch ended after {} changes", seen.len()),
            Err(_) => panic!(
                "timed out with {} of {} acknowledged writes",
                owed.iter().filter(|w| seen.contains(w)).count(),
                owed.len()
            ),
        }
    }

    assert!(moves > 0, "no shard's watch saw its shard move");
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), seen.len(), "a change arrived twice");
    for k in (0..8).map(key) {
        let owed_here: Vec<_> = owed.iter().filter(|(at, _)| at == &k).collect();
        let seen_here: Vec<_> = seen
            .iter()
            .filter(|write| write.0 == k && owed.contains(write))
            .collect();
        assert_eq!(
            seen_here, owed_here,
            "acknowledged writes to {k} out of order"
        );
    }
    cluster.shutdown().await;
}
