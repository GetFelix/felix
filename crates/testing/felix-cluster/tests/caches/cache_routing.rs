//! A cache whose keys are owned by different brokers.
//!
//! Every broker used to serve its own copy of every cache (#278), so a `put` on
//! one was invisible to a `get` on another — and worse, two brokers could hold
//! *different* values for one key with nothing to reconcile them. A miss is
//! detectable; divergence is not.
//!
//! These tests are written from the client's side on purpose. The unit tests
//! prove the routing decision; these prove the thing a user would notice.
//!
//! Run with `cargo test -p felix-cluster --test caches cache_routing::`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use felix_client::{ShardedCacheWatch, ShardedCacheWatchItem};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const CACHE: &str = "sessions";
const SHARDS: u32 = 4;

fn with_cache() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        // A stream is still needed: the harness gates readiness on a publish
        // being accepted, which is the only honest proof the cluster can serve.
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::new(CACHE, SHARDS)],
        ..Default::default()
    }
}

/// The headline property. A value written through any broker is readable
/// through every other, because exactly one of them owns the key and the rest
/// forward to it.
#[tokio::test]
#[serial]
async fn a_value_written_through_one_broker_is_readable_through_every_other() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let nodes: Vec<String> = cluster.node_ids();

    for (i, writer) in nodes.iter().enumerate() {
        let key = format!("key-from-{writer}");
        let value = format!("value-{i}");
        cluster
            .cache_put_via(writer, CACHE, &key, value.as_bytes())
            .await
            .expect("cache put");

        for reader in &nodes {
            let read = cluster
                .cache_get_via(reader, CACHE, &key)
                .await
                .expect("cache get");
            assert_eq!(
                read.as_deref(),
                Some(value.as_bytes()),
                "{key} written through {writer} was not readable through {reader}",
            );
        }
    }
}

/// The divergence itself. Two brokers writing the same key must not end up with
/// two answers: the second write goes to the same owner as the first, so every
/// reader sees the second value and nothing is left holding the first.
#[tokio::test]
#[serial]
async fn two_brokers_writing_one_key_do_not_diverge() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    assert!(nodes.len() >= 2);

    let key = "contended";
    cluster
        .cache_put_via(&nodes[0], CACHE, key, b"first")
        .await
        .expect("first write");
    cluster
        .cache_put_via(&nodes[1], CACHE, key, b"second")
        .await
        .expect("second write");

    for reader in &nodes {
        let read = cluster
            .cache_get_via(reader, CACHE, key)
            .await
            .expect("cache get");
        assert_eq!(
            read.as_deref(),
            Some(&b"second"[..]),
            "{reader} still answers with a value the last write replaced",
        );
    }
}

/// Ownership is actually spread. If every shard landed on one broker the tests
/// above would pass without a single operation ever being forwarded, and the
/// routing they exist to check would be untested.
#[tokio::test]
#[serial]
async fn cache_shards_are_spread_across_brokers() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");

    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");

    assert_eq!(
        owners.len(),
        SHARDS as usize,
        "every cache shard should have a leader, got {owners:?}",
    );
    let distinct: HashSet<&String> = owners.values().collect();
    assert!(
        distinct.len() > 1,
        "all {SHARDS} cache shards landed on one broker: {owners:?}",
    );
}

/// Keys must actually land in different shards. One shard holding everything
/// would make the cluster tests above pass while exercising a single owner.
#[tokio::test]
#[serial]
async fn keys_are_spread_across_cache_shards() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");
    let nodes = cluster.node_ids();

    // Written through one broker and read through a *different* one, on
    // purpose. A same-broker round trip succeeds whether or not the key was
    // routed — it would just find the local copy — so it could not tell a
    // working keyspace split from the divergence this replaced.
    for n in 0..40 {
        let key = format!("spread-{n}");
        cluster
            .cache_put_via(&nodes[0], CACHE, &key, key.as_bytes())
            .await
            .expect("cache put");
    }
    for n in 0..40 {
        let key = format!("spread-{n}");
        let read = cluster
            .cache_get_via(&nodes[n % nodes.len()], CACHE, &key)
            .await
            .expect("cache get");
        assert_eq!(read.as_deref(), Some(key.as_bytes()), "{key} was lost");
    }

    assert!(
        owners.values().collect::<HashSet<_>>().len() > 1,
        "the keyspace was not actually split: {owners:?}",
    );
}

/// A cache and a stream may share a name, and when they do they are unrelated.
/// A broker that confused them would route cache keys to the stream's owner.
#[tokio::test]
#[serial]
async fn a_cache_named_after_a_stream_is_a_different_thing() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::new("orders", SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let nodes = cluster.node_ids();
    cluster
        .cache_put_via(&nodes[0], "orders", "k", b"cached")
        .await
        .expect("cache put");

    for reader in &nodes {
        let read = cluster
            .cache_get_via(reader, "orders", "k")
            .await
            .expect("cache get");
        assert_eq!(read.as_deref(), Some(&b"cached"[..]), "via {reader}");
    }

    // And the stream of the same name still works, on its own owner.
    let owner = cluster.owner("orders").await.expect("stream owner");
    cluster
        .publish_via(&owner, "orders", b"streamed".to_vec())
        .await
        .expect("publish to the stream of the same name");
}

/// A prefix spans shards, since keys sharing one hash apart. A prefix watch
/// that names no shard used to read shard 0 and look like it was working while
/// missing every key elsewhere; it is refused instead, and naming the shard
/// still works.
#[tokio::test]
#[serial]
async fn a_prefix_watch_on_a_multi_shard_cache_must_name_its_shard() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");
    let prefix = || felix_client::CacheWatchFilter::Prefix("user:".to_string());

    let client = cluster
        .client_on(&cluster.node_ids()[0])
        .await
        .expect("client");
    let err = client
        .watch_cache(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            prefix(),
            None,
        )
        .await
        .expect_err("an unaddressed prefix watch on a multi-shard cache must be refused");
    assert!(
        err.to_string().contains(&format!("has {SHARDS} shards")),
        "the refusal should say why: {err}",
    );

    let (shard, owner) = owners.iter().next().expect("a shard");
    let client = cluster.client_on(owner).await.expect("client on owner");
    client
        .watch_cache_shard(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            prefix(),
            Some(*shard),
            None,
        )
        .await
        .expect("a prefix watch naming its shard, on that shard's owner, is served");
}

async fn cluster_client(cluster: &Cluster) -> Arc<felix_client::ClusterClient> {
    Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect a cluster client"),
    )
}

async fn next_item(watch: &mut ShardedCacheWatch) -> ShardedCacheWatchItem {
    tokio::time::timeout(Duration::from_secs(20), watch.recv())
        .await
        .expect("the sharded watch went quiet")
        .expect("the sharded watch ended")
}

/// One prefix watch through `watch_cache_sharded` sees a write to every shard
/// the prefix spans, each shard's watch served by that shard's own owner, and
/// nothing outside the prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_prefix_watch_sees_writes_to_every_shard() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let owners = cluster
        .cache_shard_owners(CACHE)
        .await
        .expect("cache shard owners");
    assert!(
        owners.values().collect::<HashSet<_>>().len() > 1,
        "every shard has one owner, so no redirect would be followed: {owners:?}",
    );

    let client = cluster_client(&cluster).await;
    let mut watch = client
        .watch_cache_sharded(&cluster.tenant_id, &cluster.namespace, CACHE, "user:", None)
        .await
        .expect("sharded prefix watch");
    assert_eq!(watch.shards(), SHARDS);
    assert_eq!(watch.retained_count(), None);

    let via = cluster.node_ids()[0].clone();
    cluster
        .cache_put_via(&via, CACHE, "other:ignored", b"x")
        .await
        .expect("put outside the prefix");
    let keys: Vec<String> = (0..40).map(|n| format!("user:{n}")).collect();
    for key in &keys {
        cluster
            .cache_put_via(&via, CACHE, key, key.as_bytes())
            .await
            .expect("cache put");
    }

    let mut seen: HashMap<String, u32> = HashMap::new();
    while seen.len() < keys.len() {
        match next_item(&mut watch).await {
            ShardedCacheWatchItem::Change { shard, change } => {
                assert!(change.key.starts_with("user:"), "{} leaked in", change.key);
                assert_eq!(change.value.as_deref(), Some(change.key.as_bytes()));
                seen.insert(change.key, shard);
            }
            other => panic!("unexpected item {other:?}"),
        }
    }
    let shards_seen: HashSet<u32> = seen.values().copied().collect();
    assert_eq!(
        shards_seen.len(),
        SHARDS as usize,
        "40 keys should reach every shard, got {shards_seen:?}",
    );

    // Every shard has moved past what it delivered, so resuming from here
    // replays nothing already seen.
    let resume = watch.resume_offsets();
    assert_eq!(resume.len(), SHARDS as usize);
    drop(watch);
    cluster
        .cache_put_via(&via, CACHE, "user:after", b"user:after")
        .await
        .expect("put while not watching");
    let mut resumed = client
        .watch_cache_sharded(
            &cluster.tenant_id,
            &cluster.namespace,
            CACHE,
            "user:",
            Some(resume),
        )
        .await
        .expect("resume the sharded watch");
    match next_item(&mut resumed).await {
        ShardedCacheWatchItem::Change { change, .. } => assert_eq!(change.key, "user:after"),
        other => panic!("expected the write made while away, got {other:?}"),
    }
}

/// Retained delivery across shards: every shard's current values arrive, then
/// one `StateComplete` once all of them have, then live changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_retained_watch_marks_when_every_shard_holds_its_state() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let via = cluster.node_ids()[0].clone();
    let keys: Vec<String> = (0..20).map(|n| format!("user:{n}")).collect();
    for key in &keys {
        cluster
            .cache_put_via(&via, CACHE, key, key.as_bytes())
            .await
            .expect("cache put");
    }

    let client = cluster_client(&cluster).await;
    let mut watch = client
        .watch_cache_sharded_retained(&cluster.tenant_id, &cluster.namespace, CACHE, "user:")
        .await
        .expect("sharded retained watch");
    assert_eq!(watch.retained_count(), Some(keys.len() as u64));

    let mut retained: HashSet<String> = HashSet::new();
    loop {
        match next_item(&mut watch).await {
            ShardedCacheWatchItem::Change { change, .. } => {
                retained.insert(change.key);
            }
            ShardedCacheWatchItem::StateComplete => break,
            other => panic!("unexpected item {other:?}"),
        }
    }
    assert_eq!(retained, keys.iter().cloned().collect::<HashSet<_>>());

    cluster
        .cache_put_via(&via, CACHE, "user:live", b"live")
        .await
        .expect("live put");
    match next_item(&mut watch).await {
        ShardedCacheWatchItem::Change { change, .. } => assert_eq!(change.key, "user:live"),
        other => panic!("expected the live change, got {other:?}"),
    }
}

/// An empty prefix is a definite zero on every shard, so the state is complete
/// before anything arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn a_sharded_retained_watch_on_an_empty_prefix_is_complete_at_once() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let client = cluster_client(&cluster).await;
    let mut watch = client
        .watch_cache_sharded_retained(&cluster.tenant_id, &cluster.namespace, CACHE, "nobody:")
        .await
        .expect("sharded retained watch");
    assert_eq!(watch.retained_count(), Some(0));
    assert_eq!(
        next_item(&mut watch).await,
        ShardedCacheWatchItem::StateComplete
    );
}

/// **A cluster client sends a cache request straight to the key's owner.** It
/// asks who owns the cache's shards once, so a client seeded with a broker
/// that does not own the key still reaches the owner without a forward, and
/// its value is what every broker reads.
#[tokio::test]
#[serial]
async fn a_cluster_client_routes_cache_requests_to_the_owner() {
    let cluster = Cluster::start(with_cache()).await.expect("start cluster");
    let key = "routed";
    let shard = felix_wire::routing::shard_for(SHARDS, Some(key.as_bytes()));
    let owner = cluster
        .shard_owner_of("cache", CACHE, shard)
        .await
        .expect("the key's owner");
    let addr_of = |node_id: &str| {
        cluster
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.client_addr)
            .expect("a client address")
    };
    let seed = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != owner)
        .expect("a broker that does not own the key");

    let client = felix_cluster::client::connect_cluster(
        &[addr_of(&seed)],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    let (tenant, namespace) = (cluster.tenant_id.as_str(), cluster.namespace.as_str());
    client
        .cache_put(tenant, namespace, CACHE, key, "v1".into(), None)
        .await
        .expect("put");

    let connected: Vec<_> = client
        .connections_per_node()
        .await
        .into_iter()
        .map(|(addr, _)| addr)
        .collect();
    assert!(
        connected.contains(&addr_of(&owner)),
        "the put did not go to the owner {owner}; connected to {connected:?}",
    );
    assert_eq!(
        client
            .cache_get(tenant, namespace, CACHE, key)
            .await
            .expect("get")
            .as_deref(),
        Some(&b"v1"[..])
    );
    let read = cluster
        .cache_get_via(&seed, CACHE, key)
        .await
        .expect("read through the seed");
    assert_eq!(read.as_deref(), Some(&b"v1"[..]));
    assert_eq!(
        client
            .cache_delete(tenant, namespace, CACHE, key)
            .await
            .expect("delete")
            .as_deref(),
        Some(&b"v1"[..])
    );
    assert_eq!(
        client
            .cache_get(tenant, namespace, CACHE, key)
            .await
            .expect("get after delete"),
        None
    );
}
