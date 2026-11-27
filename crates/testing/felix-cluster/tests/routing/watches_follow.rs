//! Cache watches through a `Client`, one shard or all of them, keep
//! delivering when their shard moves rather than ending or going stale.
//!
//! A watch either ends as moved and follows the shard, or stays on a broker
//! that still serves it. On the hybrid a move closes the old owner, so the
//! watch always follows. On Raft the old broker can stay a voter, following
//! the new leader, until it is removed from the group; only that removal
//! ends its watches. Both are fine as long as the watch keeps up with writes
//! made on the new owner.
//!
//! Run with `cargo test -p felix-cluster --test routing watches_follow::`.
use std::collections::{BTreeMap, BTreeSet};
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
/// it sees every acknowledged value in write order and none twice, and then
/// every value written on the new owner, at the offsets the new owner's log
/// holds them.
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
        put(&cluster, &owner, KEY, &value).await;
        owed.push(value);
    }
    let history = owed.clone();

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

    // Written on the new owner once it leads, to several keys so the watched
    // key's offsets are sparse. A watch left on a stale broker misses these.
    wait_until_leading(&cluster, &other, &[0]).await;
    let keys = [KEY, "session-2", "session-3"];
    let mut fresh: Vec<Vec<u8>> = Vec::new();
    for i in 0..12 {
        let (key, value) = (keys[i % keys.len()], format!("after-{i}").into_bytes());
        put(&cluster, &other, key, &value).await;
        if key == KEY {
            fresh.push(value);
        }
    }
    owed.extend(fresh.iter().cloned());
    let reference = reference_changes(&cluster, &other, KEY, &fresh).await;

    let mut offsets = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut moves = 0;
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|value| values.contains(value)) {
        match next_item(&mut watch, deadline, &owed, &values).await {
            WatchStep::Change(offset, value) => {
                offsets.push(offset);
                values.push(value);
            }
            WatchStep::Moved => moves += 1,
        }
    }

    // A broker that left the group serves the shard no longer, so a watch
    // still on it must end as moved and follow.
    let left = left_group(&cluster, &other, &owner, 0).await;
    if left && moves == 0 {
        let deadline =
            tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
        while moves == 0 {
            match next_item(&mut watch, deadline, &owed, &values).await {
                WatchStep::Change(offset, value) => {
                    offsets.push(offset);
                    values.push(value);
                }
                WatchStep::Moved => moves += 1,
            }
        }
    }
    eprintln!(
        "watch path: {} (old broker left the group: {left})",
        if moves > 0 {
            format!("followed {moves} move(s)")
        } else {
            "stayed on a broker that still serves the shard".to_string()
        }
    );
    if left {
        assert!(
            moves > 0,
            "the old broker left the group and the watch never saw its shard move"
        );
    }

    for value in &history {
        assert!(
            values.contains(value),
            "history value {value:?} was not replayed"
        );
    }
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

    // From the first fresh write on, the watch holds exactly what the new
    // owner's log holds for the key: same offsets, same values, no gap.
    let start = reference
        .iter()
        .find(|(_, value)| fresh.contains(value))
        .map(|(offset, _)| *offset)
        .expect("the reference saw the fresh writes");
    let seen_fresh: Vec<(u64, Vec<u8>)> = offsets
        .iter()
        .copied()
        .zip(values.iter().cloned())
        .filter(|(offset, _)| *offset >= start)
        .collect();
    let reference_fresh: Vec<(u64, Vec<u8>)> = reference
        .into_iter()
        .filter(|(offset, _)| *offset >= start)
        .collect();
    assert_eq!(
        seen_fresh, reference_fresh,
        "the watch's changes after the move differ from the new owner's log"
    );
    cluster.shutdown().await;
}

/// **A sharded watch follows each shard.** A prefix watch over a two-shard
/// cache reads from the start while a writer keeps updating keys on both
/// shards and one broker is drained. Every acknowledged value reaches the
/// watch in its key's write order, none twice, and no shard's watch ends.
/// Values written on the new owner afterwards arrive at the offsets its log
/// holds them, on every shard.
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
    let drained_shards: Vec<u32> = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("owners")
        .into_iter()
        .filter(|(_, node)| node == &drained)
        .map(|(shard, _)| shard)
        .collect();

    let mut owed: Vec<Write> = Vec::new();
    for i in 0..16 {
        let value = format!("before-{i}").into_bytes();
        put(&cluster, &other, &key(i), &value).await;
        owed.push((key(i), value));
    }
    let history = owed.clone();

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
                // A put held up by the move is bounded, so the test never
                // waits on a put that nobody answers.
                if tokio::time::timeout(
                    Duration::from_secs(10),
                    writer.cache_put(
                        &tenant,
                        &namespace,
                        CACHE,
                        &key,
                        bytes::Bytes::from(value.clone()),
                        None,
                    ),
                )
                .await
                .is_ok_and(|put| put.is_ok())
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
    tokio::time::timeout(felix_cluster::wait::budget(Duration::from_secs(30)), putter)
        .await
        .expect("the putter to stop")
        .expect("putter");
    owed.extend(acknowledged.lock().await.iter().cloned());
    assert!(
        owed.len() > 16,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    // Written on the new owner once it leads every shard, to every key.
    let all: Vec<u32> = (0..SHARDS).collect();
    wait_until_leading(&cluster, &other, &all).await;
    let mut fresh: Vec<Write> = Vec::new();
    for i in 0..16 {
        let value = format!("after-{i}").into_bytes();
        put(&cluster, &other, &key(i), &value).await;
        fresh.push((key(i), value));
    }
    owed.extend(fresh.iter().cloned());
    let reference = sharded_reference_changes(&cluster, &other, SHARDS, &fresh).await;

    let mut seen: Vec<Write> = Vec::new();
    let mut changes: BTreeMap<u32, Vec<(u64, Write)>> = BTreeMap::new();
    let mut moved: BTreeSet<u32> = BTreeSet::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|write| seen.contains(write)) {
        let (shard, step) = next_sharded_item(&mut watch, deadline, &owed, &seen).await;
        record(shard, step, &mut seen, &mut changes, &mut moved);
    }

    // Each drained shard whose old broker left its group must end as moved.
    let mut left: BTreeSet<u32> = BTreeSet::new();
    for &shard in &drained_shards {
        if left_group(&cluster, &other, &drained, shard).await {
            left.insert(shard);
        }
    }
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    while !left.is_subset(&moved) {
        let (shard, step) = next_sharded_item(&mut watch, deadline, &owed, &seen).await;
        record(shard, step, &mut seen, &mut changes, &mut moved);
    }
    eprintln!(
        "watch path: shards that followed a move {moved:?}; drained shards {drained_shards:?}, \
         of which the old broker left {left:?}; the rest stayed on a broker that still serves them"
    );

    for write in &history {
        assert!(
            seen.contains(write),
            "history write {write:?} was not replayed"
        );
    }
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
    for (shard, expected) in reference {
        let start = expected
            .iter()
            .find(|(_, write)| fresh.contains(write))
            .map(|(offset, _)| *offset)
            .expect("the reference saw the shard's fresh writes");
        let got = changes.get(&shard).cloned().unwrap_or_default();
        assert!(
            got.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "shard {shard}'s offsets must rise across the move"
        );
        let got: Vec<_> = got
            .into_iter()
            .filter(|(offset, _)| *offset >= start)
            .collect();
        let expected: Vec<_> = expected
            .into_iter()
            .filter(|(offset, _)| *offset >= start)
            .collect();
        assert_eq!(
            got, expected,
            "shard {shard}'s changes after the move differ from the new owner's log"
        );
    }
    cluster.shutdown().await;
}

/// What a watch delivered, past the items that fail the test outright.
enum WatchStep {
    Change(u64, Vec<u8>),
    Moved,
}

/// The watch's next change or move. Panics on a lag, an end or the deadline.
async fn next_item(
    watch: &mut felix_client::ClusterCacheWatch,
    deadline: tokio::time::Instant,
    owed: &[Vec<u8>],
    values: &[Vec<u8>],
) -> WatchStep {
    match tokio::time::timeout_at(deadline, watch.recv()).await {
        Ok(Some(CacheWatchItem::Change(change))) => {
            WatchStep::Change(change.offset, change.value.expect("never deleted").to_vec())
        }
        Ok(Some(CacheWatchItem::ShardMoved(_))) => WatchStep::Moved,
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

/// The sharded watch's next change or move, and its shard. Panics on any
/// other item, an end or the deadline.
async fn next_sharded_item(
    watch: &mut felix_client::ShardedCacheWatch,
    deadline: tokio::time::Instant,
    owed: &[Write],
    seen: &[Write],
) -> (u32, Option<(u64, Write)>) {
    match tokio::time::timeout_at(deadline, watch.recv()).await {
        Ok(Some(ShardedCacheWatchItem::Change { shard, change })) => (
            shard,
            Some((
                change.offset,
                (change.key, change.value.expect("never deleted").to_vec()),
            )),
        ),
        Ok(Some(ShardedCacheWatchItem::ShardMoved { shard, .. })) => (shard, None),
        Ok(Some(other)) => panic!("a shard's watch ended: {other:?}"),
        Ok(None) => panic!("the watch ended after {} changes", seen.len()),
        Err(_) => panic!(
            "timed out with {} of {} acknowledged writes",
            owed.iter().filter(|w| seen.contains(w)).count(),
            owed.len()
        ),
    }
}

/// Files one sharded item: a change under its shard, or a move.
fn record(
    shard: u32,
    step: Option<(u64, Write)>,
    seen: &mut Vec<Write>,
    changes: &mut BTreeMap<u32, Vec<(u64, Write)>>,
    moved: &mut BTreeSet<u32>,
) {
    match step {
        Some((offset, write)) => {
            seen.push(write.clone());
            changes.entry(shard).or_default().push((offset, write));
        }
        None => {
            moved.insert(shard);
        }
    }
}

async fn put(cluster: &Cluster, via: &str, key: &str, value: &[u8]) {
    felix_cluster::wait::until(Duration::from_secs(30), "the cache put to land", || async {
        cluster.cache_put_via(via, CACHE, key, value).await.is_ok()
    })
    .await
    .expect("cache put");
}

/// Waits until `node` leads each of `shards`. On the hybrid placement naming
/// it, which the caller already waited for, is what leading means.
async fn wait_until_leading(cluster: &Cluster, node: &str, shards: &[u32]) {
    if !cluster.raft() {
        return;
    }
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the new owner to lead the cache's shards",
        || async {
            for &shard in shards {
                let member = cluster.raft_cache_member_of(node, CACHE, shard).await;
                if !member.is_some_and(|member| member.role == "leader") {
                    return false;
                }
            }
            true
        },
    )
    .await
    .expect("the new owner leads");
}

/// Whether `old` is out of `shard`'s group, by the report of `leader`'s
/// member. Always true on the hybrid, where a move closes the old owner.
async fn left_group(cluster: &Cluster, leader: &str, old: &str, shard: u32) -> bool {
    if !cluster.raft() {
        return true;
    }
    cluster
        .raft_cache_member_of(leader, CACHE, shard)
        .await
        .is_some_and(|member| {
            member.role == "leader"
                && !member.voters.is_empty()
                && !member.voters.iter().any(|voter| voter == old)
        })
}

/// Every change to `key` a fresh watch on `node` reads from the start, up
/// to and including each of `fresh`: what the new owner's log holds.
async fn reference_changes(
    cluster: &Cluster,
    node: &str,
    key: &str,
    fresh: &[Vec<u8>],
) -> Vec<(u64, Vec<u8>)> {
    let client = Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(node).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reference reader"),
    );
    let mut watch = client
        .watch_cache(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            CacheWatchFilter::Key(key.to_string()),
            Some(0),
        )
        .await
        .expect("reference watch");
    let mut changes: Vec<(u64, Vec<u8>)> = Vec::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    while !fresh
        .iter()
        .all(|value| changes.iter().any(|(_, seen)| seen == value))
    {
        let values: Vec<Vec<u8>> = changes.iter().map(|(_, value)| value.clone()).collect();
        match next_item(&mut watch, deadline, fresh, &values).await {
            WatchStep::Change(offset, value) => changes.push((offset, value)),
            WatchStep::Moved => panic!("the reference watch on the new owner saw a move"),
        }
    }
    changes
}

/// [`reference_changes`] for every shard of a prefix watch, by shard.
async fn sharded_reference_changes(
    cluster: &Cluster,
    node: &str,
    shards: u32,
    fresh: &[Write],
) -> BTreeMap<u32, Vec<(u64, Write)>> {
    let client = Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(node).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reference reader"),
    );
    let mut watch = client
        .watch_cache_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            "k",
            Some((0..shards).map(|shard| (shard, 0)).collect()),
        )
        .await
        .expect("reference watch");
    let mut seen: Vec<Write> = Vec::new();
    let mut changes: BTreeMap<u32, Vec<(u64, Write)>> = BTreeMap::new();
    let mut moved: BTreeSet<u32> = BTreeSet::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    while !fresh.iter().all(|write| seen.contains(write)) {
        let (shard, step) = next_sharded_item(&mut watch, deadline, fresh, &seen).await;
        record(shard, step, &mut seen, &mut changes, &mut moved);
    }
    assert!(
        moved.is_empty(),
        "the reference watch on the new owner saw a move"
    );
    changes
}
