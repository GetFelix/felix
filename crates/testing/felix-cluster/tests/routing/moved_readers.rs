//! A shard that moves away ends the subscriptions and cache watches its old
//! leader was serving, rather than leaving them open and silent.
//!
//! The old leader stays up throughout -- a join moves shards off a healthy
//! broker -- so nothing about the connection tells the client its feed has
//! stopped. Ending the stream is the only signal it gets to resubscribe.
//!
//! Run with `cargo test -p felix-cluster --test routing moved_readers::`.
use std::collections::HashMap;
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const CACHE: &str = "sessions";
const SHARDS: u32 = 2;

/// Read from a subscription until the broker ends it, collecting what it
/// delivered. `Err` if it is still open, and silent, when the budget runs out.
async fn subscription_ends(
    subscription: &mut felix_client::Subscription,
    budget: Duration,
) -> Result<Vec<Vec<u8>>, String> {
    let deadline = tokio::time::Instant::now() + felix_cluster::wait::budget(budget);
    let mut delivered = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, subscription.next_event()).await {
            Ok(Ok(Some(event))) => delivered.push(event.payload.to_vec()),
            Ok(Ok(None)) | Ok(Err(_)) => return Ok(delivered),
            Err(_) => {
                return Err(format!(
                    "still open after {budget:?}, {} records delivered",
                    delivered.len()
                ));
            }
        }
    }
}

/// The same for a cache watch.
async fn watch_ends(watch: &mut felix_client::CacheWatch, budget: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + felix_cluster::wait::budget(budget);
    loop {
        match tokio::time::timeout_at(deadline, watch.recv()).await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(_) => return Err(format!("still open after {budget:?}")),
        }
    }
}

/// A key of `CACHE` for each of its shards.
fn cache_key_per_shard() -> HashMap<u32, String> {
    let mut keys = HashMap::new();
    for i in 0.. {
        let key = format!("user-{i}");
        let shard = felix_wire::routing::shard_for(SHARDS, Some(key.as_bytes()));
        keys.entry(shard).or_insert(key);
        if keys.len() == SHARDS as usize {
            break;
        }
    }
    keys
}

fn stream_key(cluster: &Cluster, shard: u32) -> String {
    format!(
        "stream/{}/{}/{STREAM}/{shard}",
        cluster.tenant_id, cluster.namespace
    )
}

fn cache_key(cluster: &Cluster, shard: u32) -> String {
    format!(
        "cache/{}/{}/{CACHE}/{shard}",
        cluster.tenant_id, cluster.namespace
    )
}

/// **Readers on the old leader are ended.** A broker joins and takes shards
/// from the one that held them all; every subscription and cache watch on the
/// old leader for a shard that moved ends, after delivering what it had
/// queued, while the old leader keeps running. Draining the old leader then
/// moves the rest, and ends theirs too.
#[serial]
#[tokio::test]
async fn a_moved_shard_ends_its_readers_on_the_old_leader() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        caches: vec![CacheSpec::new(CACHE, SHARDS)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();

    let mut sent: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
    for i in 0..20 {
        let key = format!("k{i}");
        let shard = felix_wire::routing::shard_for(SHARDS, Some(key.as_bytes()));
        let payload = format!("keyed-{i}").into_bytes();
        // Settled: the first publishes after start can meet a shard that is
        // not servable yet.
        cluster
            .publish_keyed_via_settled(
                &first,
                STREAM,
                key.as_bytes(),
                payload.clone(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
        sent.entry(shard).or_default().push(payload);
    }
    let cache_keys = cache_key_per_shard();
    for key in cache_keys.values() {
        felix_cluster::wait::until(Duration::from_secs(30), "the cache put to land", || async {
            cluster
                .cache_put_via(&first, CACHE, key, b"v")
                .await
                .is_ok()
        })
        .await
        .expect("cache put");
    }

    // Held for the whole test: dropping a client closes its connection, which
    // would end the feed for a reason that has nothing to do with the move.
    let mut subscriptions = HashMap::new();
    for shard in 0..SHARDS {
        let subscription = cluster
            .replay_shard(&first, STREAM, shard)
            .await
            .expect("subscribe on the leader");
        subscriptions.insert(stream_key(&cluster, shard), (shard, subscription));
    }
    let mut watches = HashMap::new();
    for (shard, key) in &cache_keys {
        let watch = cluster
            .cache_watch_retained_via(&first, CACHE, key)
            .await
            .expect("watch on the leader");
        watches.insert(cache_key(&cluster, *shard), watch);
    }

    let joined = cluster.add_node().await.expect("add a broker");
    felix_cluster::wait::until(
        Duration::from_secs(90),
        "the joiner to lead half the shards",
        || async {
            cluster.place_shards_moving(2).await;
            match cluster.shard_owners().await {
                Ok(owners) => owners.values().filter(|leader| **leader == joined).count() == 2,
                Err(_) => false,
            }
        },
    )
    .await
    .expect("rebalance");

    let owners = cluster.shard_owners().await.expect("owners");
    let moved: Vec<String> = owners
        .iter()
        .filter(|(_, leader)| **leader == joined)
        .map(|(key, _)| key.clone())
        .collect();
    let mut failures = Vec::new();
    check_ended(
        &moved,
        &mut subscriptions,
        &mut watches,
        &sent,
        &mut failures,
    )
    .await;
    // Still up and answering: the feeds ended because of the move, not
    // because the broker went away.
    assert!(
        cluster
            .metric(&first, "felix_broker_forwards_total")
            .await
            .is_ok(),
        "the old leader should still be running"
    );

    // The rest, by draining the old leader while it stays up.
    cluster.drain_node(&first).await.expect("drain");
    cluster
        .drain_until_empty(&first, 2, Duration::from_secs(120))
        .await
        .expect("the old leader should end up leading nothing");
    let rest: Vec<String> = subscriptions
        .keys()
        .chain(watches.keys())
        .cloned()
        .collect();
    check_ended(
        &rest,
        &mut subscriptions,
        &mut watches,
        &sent,
        &mut failures,
    )
    .await;
    assert!(
        failures.is_empty(),
        "readers on {first} were not ended when their shards moved: {failures:?}"
    );
    cluster.shutdown().await;
}

/// Every reader for a shard in `moved` must end; each is removed once it has.
async fn check_ended(
    moved: &[String],
    subscriptions: &mut HashMap<
        String,
        (u32, (felix_client::Connection, felix_client::Subscription)),
    >,
    watches: &mut HashMap<String, (felix_client::Connection, felix_client::CacheWatch)>,
    sent: &HashMap<u32, Vec<Vec<u8>>>,
    failures: &mut Vec<String>,
) {
    for key in moved {
        if let Some((shard, (_client, mut subscription))) = subscriptions.remove(key) {
            match subscription_ends(&mut subscription, Duration::from_secs(30)).await {
                Ok(delivered) => {
                    // Ended after what it had, not instead of it.
                    let missing: Vec<_> = sent
                        .get(&shard)
                        .into_iter()
                        .flatten()
                        .filter(|payload| !delivered.contains(payload))
                        .map(|payload| String::from_utf8_lossy(payload).into_owned())
                        .collect();
                    if !missing.is_empty() {
                        failures.push(format!("{key} ended without delivering {missing:?}"));
                    }
                }
                Err(why) => failures.push(format!("subscription {key}: {why}")),
            }
        }
        if let Some((_client, mut watch)) = watches.remove(key)
            && let Err(why) = watch_ends(&mut watch, Duration::from_secs(30)).await
        {
            failures.push(format!("watch {key}: {why}"));
        }
    }
}
