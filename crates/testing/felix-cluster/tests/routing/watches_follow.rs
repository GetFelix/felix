//! Cache watches through a `Client`, one shard or all of them, follow
//! their shard when it moves rather than ending.
//!
//! Run with `cargo test -p felix-cluster --test routing watches_follow::`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use felix_client::{CacheWatchFilter, CacheWatchItem, ShardedCacheWatchItem};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig};
use serial_test::serial;
use tokio::sync::Mutex;

const CACHE: &str = "sessions";
const KEY: &str = "session-1";

/// A key and the value written to it.
type Write = (String, Vec<u8>);

/// A value and the millisecond it was acknowledged at.
type Timed<T> = (u128, T);

// Diagnostic branch for the watch-follow failures on Raft. It never merges.

/// Debug logging for the watch path only, for the brokers and this process.
const BROKER_LOG: &str = "info,felix_broker_service::shards=debug,\
felix_broker_service::serving::quic::handlers::cache_watch=debug,\
felix_broker_service::serving::quic::handlers::redirect=debug";
const CLIENT_LOG: &str = "warn,felix_client::cluster=debug,felix_client::cache=debug";

/// Everything the watch client saw, on one clock.
struct Trace {
    start: Instant,
    lines: Vec<String>,
}

impl Trace {
    fn new() -> Self {
        // Serial tests, and nothing else has touched the environment yet.
        unsafe { std::env::set_var("FELIX_DIAG_3335", "1") };
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(CLIENT_LOG))
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .try_init();
        Self {
            start: Instant::now(),
            lines: Vec::new(),
        }
    }

    fn ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    fn note(&mut self, text: impl std::fmt::Display) {
        let line = format!("[{:>7}ms] {text}", self.ms());
        self.lines.push(line);
    }

    /// Print the trace; the diagnostic run then fails so its logs are kept.
    fn finish(&self, name: &str) {
        eprintln!("==== watch trace: {name} ====");
        for line in &self.lines {
            eprintln!("{line}");
        }
        eprintln!("==== end of watch trace ====");
    }
}

fn diagnostic_stop() {
    if std::env::var_os("FELIX_DIAG_3335").is_some() {
        panic!("diagnostic run: trace above");
    }
}

fn text(value: &[u8]) -> String {
    String::from_utf8_lossy(value).into_owned()
}

/// **Every acknowledged write, in order, across the switch-over.** A watch
/// reads one key from the start of the cache log while a writer keeps
/// updating it and the shard moves to another broker. Without being reopened,
/// it sees every acknowledged value in write order and none twice.
#[serial]
#[tokio::test]
async fn a_cache_watch_follows_its_shard_to_the_new_owner() {
    let mut trace = Trace::new();
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        caches: vec![CacheSpec::new(CACHE, 1)],
        broker_env: vec![("RUST_LOG".to_string(), BROKER_LOG.to_string())],
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
    trace.note(format!("watch opened on {owner}, from offset 0"));
    let acknowledged: Arc<Mutex<Vec<Timed<Vec<u8>>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let clock = trace.start;
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
                    let at = clock.elapsed().as_millis();
                    acknowledged.lock().await.push((at, value));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    trace.note(format!(
        "move requested: drain {owner}, shard goes to {other}"
    ));
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
    trace.note("move complete: harness sees the new owner");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    putter.await.expect("putter");
    let acked = acknowledged.lock().await.clone();
    owed.extend(acked.iter().map(|(_, value)| value.clone()));
    assert!(
        owed.len() > 20,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    let mut offsets = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut moves = 0;
    let mut failure = None;
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|value| values.contains(value)) {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(CacheWatchItem::Change(change))) => {
                trace.note(format!(
                    "change offset={} key={} value={}",
                    change.offset,
                    change.key,
                    change.value.as_deref().map_or("<deleted>".into(), text)
                ));
                offsets.push(change.offset);
                values.push(change.value.expect("never deleted").to_vec());
            }
            Ok(Some(CacheWatchItem::ShardMoved(moved))) => {
                trace.note(format!("moved {moved:?}"));
                moves += 1;
            }
            Ok(Some(CacheWatchItem::Lagged { resume_from })) => {
                trace.note(format!("lagged resume_from={resume_from}"));
                failure = Some(format!(
                    "the watch lagged (resume from {resume_from}), which is not a move"
                ));
                break;
            }
            Ok(None) => {
                trace.note("watch ended (recv returned None)");
                failure = Some(format!(
                    "the watch ended after {} changes instead of following the shard",
                    values.len()
                ));
                break;
            }
            Err(_) => {
                trace.note("recv timed out");
                failure = Some(format!(
                    "timed out with {} of {} acknowledged writes",
                    owed.iter().filter(|v| values.contains(v)).count(),
                    owed.len()
                ));
                break;
            }
        }
    }
    trace.note(format!(
        "reconnects={} moves={} resume_from={} in_state_phase={} watch_error={:?}",
        watch.reconnects(),
        watch.moves(),
        watch.resume_from(),
        watch.in_state_phase(),
        watch.error()
    ));
    trace.note("attached broker: the client API exposes no address for the watch's connection");
    trace.note(format!("last event offset: {:?}", offsets.last().copied()));
    for (at, value) in &acked {
        if !values.contains(value) {
            trace.note(format!(
                "MISSING acknowledged write {} (acknowledged at {at}ms; puts return no offset)",
                text(value)
            ));
        }
    }
    trace.finish("a_cache_watch_follows_its_shard_to_the_new_owner");
    if let Some(failure) = failure {
        panic!("{failure}");
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
    diagnostic_stop();
}

/// **A sharded watch follows each shard.** A prefix watch over a two-shard
/// cache reads from the start while a writer keeps updating keys on both
/// shards and one broker is drained. Every acknowledged value reaches the
/// watch in its key's write order, none twice, and no shard's watch ends.
#[serial]
#[tokio::test]
async fn a_sharded_cache_watch_follows_each_shard_to_its_new_owner() {
    const SHARDS: u32 = 2;
    let mut trace = Trace::new();
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        caches: vec![CacheSpec::new(CACHE, SHARDS)],
        broker_env: vec![("RUST_LOG".to_string(), BROKER_LOG.to_string())],
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
    trace.note(format!("sharded watch opened on {drained}, from offset 0"));
    let acknowledged: Arc<Mutex<Vec<Timed<Write>>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let clock = trace.start;
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
                    let at = clock.elapsed().as_millis();
                    acknowledged.lock().await.push((at, (key, value)));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    trace.note(format!(
        "move requested: drain {drained}, shards go to {other}"
    ));
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
    trace.note("move complete: harness sees the new owners");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    putter.await.expect("putter");
    let acked = acknowledged.lock().await.clone();
    owed.extend(acked.iter().map(|(_, write)| write.clone()));
    assert!(
        owed.len() > 16,
        "nothing was acknowledged during the move, so it proves nothing"
    );

    let mut seen: Vec<Write> = Vec::new();
    let mut moves = 0;
    let mut failure = None;
    let mut last_offset = std::collections::BTreeMap::new();
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    while !owed.iter().all(|write| seen.contains(write)) {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(ShardedCacheWatchItem::Change { shard, change })) => {
                trace.note(format!(
                    "change shard={shard} offset={} key={} value={}",
                    change.offset,
                    change.key,
                    change.value.as_deref().map_or("<deleted>".into(), text)
                ));
                last_offset.insert(shard, change.offset);
                seen.push((change.key, change.value.expect("never deleted").to_vec()));
            }
            Ok(Some(item @ ShardedCacheWatchItem::ShardMoved { .. })) => {
                trace.note(format!("{item:?}"));
                moves += 1;
            }
            Ok(Some(other)) => {
                trace.note(format!("{other:?}"));
                failure = Some(format!("a shard's watch ended: {other:?}"));
                break;
            }
            Ok(None) => {
                trace.note("watch ended (recv returned None)");
                failure = Some(format!("the watch ended after {} changes", seen.len()));
                break;
            }
            Err(_) => {
                trace.note("recv timed out");
                failure = Some(format!(
                    "timed out with {} of {} acknowledged writes",
                    owed.iter().filter(|w| seen.contains(w)).count(),
                    owed.len()
                ));
                break;
            }
        }
    }
    trace.note(format!(
        "resume_offsets={:?} last offset per shard={last_offset:?}",
        watch.resume_offsets()
    ));
    trace.note(
        "no reconnect or move counters or attached broker on a sharded watch; \
         moves are the ShardMoved events above",
    );
    for (at, write) in &acked {
        if !seen.contains(write) {
            trace.note(format!(
                "MISSING acknowledged write {}={} (acknowledged at {at}ms; puts return no offset)",
                write.0,
                text(&write.1)
            ));
        }
    }
    trace.finish("a_sharded_cache_watch_follows_each_shard_to_its_new_owner");
    if let Some(failure) = failure {
        panic!("{failure}");
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
    diagnostic_stop();
}
