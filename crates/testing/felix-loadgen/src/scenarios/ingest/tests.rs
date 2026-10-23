use std::collections::BTreeMap;

use felix_wire::routing::{ShardRouting, shard_for_routing};

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::{Tally, failure, reconnect_with, spread_keys};

fn per_shard(keys: &[String], shards: u32, routing: ShardRouting) -> BTreeMap<u32, usize> {
    let mut counts = BTreeMap::new();
    for key in keys {
        *counts
            .entry(shard_for_routing(routing, shards, Some(key.as_bytes())))
            .or_default() += 1;
    }
    counts
}

#[test]
fn sequential_names_miss_shards() {
    // Why the search exists: k0..k11 reach only 8 of 12 shards.
    let names: Vec<String> = (0..12).map(|i| format!("k{i}")).collect();
    assert_eq!(per_shard(&names, 12, ShardRouting::Modulo).len(), 8);
}

#[test]
fn twelve_keys_cover_twelve_shards() {
    let keys = spread_keys(12, 12, ShardRouting::Modulo);
    assert_eq!(keys.len(), 12);
    let counts = per_shard(&keys, 12, ShardRouting::Modulo);
    assert_eq!(counts.len(), 12);
    assert!(counts.values().all(|&n| n == 1));
}

#[test]
fn forty_eight_keys_put_four_on_each_shard() {
    for routing in [ShardRouting::Modulo, ShardRouting::JumpHash] {
        let keys = spread_keys(48, 12, routing);
        let counts = per_shard(&keys, 12, routing);
        assert_eq!(counts.len(), 12, "{routing:?}");
        assert!(counts.values().all(|&n| n == 4), "{routing:?}: {counts:?}");
    }
}

#[test]
fn fewer_keys_than_shards_land_on_distinct_shards() {
    let keys = spread_keys(5, 12, ShardRouting::Modulo);
    let counts = per_shard(&keys, 12, ShardRouting::Modulo);
    assert_eq!(counts.len(), 5);
}

#[test]
fn uneven_counts_differ_by_at_most_one() {
    let keys = spread_keys(14, 12, ShardRouting::Modulo);
    let counts = per_shard(&keys, 12, ShardRouting::Modulo);
    assert_eq!(counts.len(), 12);
    let (min, max) = (counts.values().min(), counts.values().max());
    assert_eq!((min, max), (Some(&1), Some(&2)));
}

#[test]
fn consecutive_keys_hit_different_shards() {
    let keys = spread_keys(24, 12, ShardRouting::Modulo);
    let shards: Vec<u32> = keys
        .iter()
        .map(|k| shard_for_routing(ShardRouting::Modulo, 12, Some(k.as_bytes())))
        .collect();
    assert_eq!(shards[..12], shards[12..]);
    assert_eq!(per_shard(&keys[..12], 12, ShardRouting::Modulo).len(), 12);
}

fn addrs(n: u16) -> Vec<SocketAddr> {
    (0..n)
        .map(|i| format!("127.0.0.1:{}", 7000 + i).parse().unwrap())
        .collect()
}

/// **A dead broker is skipped, not fatal**: the publisher moves on to the
/// next one and keeps going.
#[tokio::test]
async fn reconnect_moves_to_the_next_live_broker() {
    let brokers = addrs(3);
    let dead = brokers[1];
    let tried = AtomicUsize::new(0);
    let (index, got) = reconnect_with(
        &brokers,
        0,
        Duration::from_secs(5),
        Duration::from_millis(1),
        |addr| {
            tried.fetch_add(1, Ordering::SeqCst);
            async move {
                if addr == dead {
                    anyhow::bail!("connection refused")
                }
                Ok(addr)
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(index, 2);
    assert_eq!(got, brokers[2]);
    assert_eq!(tried.load(Ordering::SeqCst), 2);
}

/// **A broker that comes back is retried until the budget is spent**, and
/// only then is the failure returned.
#[tokio::test]
async fn reconnect_retries_then_gives_up() {
    let brokers = addrs(2);
    let calls = AtomicUsize::new(0);
    let ok = reconnect_with(
        &brokers,
        0,
        Duration::from_secs(5),
        Duration::from_millis(1),
        |_| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 3 {
                    anyhow::bail!("down")
                }
                Ok(n)
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(ok.1, 3);

    let err = reconnect_with(
        &brokers,
        0,
        Duration::from_millis(20),
        Duration::from_millis(5),
        |_| async { Err::<(), _>(anyhow::anyhow!("down")) },
    )
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("no broker accepted"), "{err:#}");
}

/// **A failed send is counted, not raised**, acks are tallied by shard, and
/// an error with no broker answer behind it is a transport failure that
/// remembers which connection it ran on.
#[test]
fn tally_counts_failures_and_per_shard_acks() {
    let mut tally = Tally::default();
    tally.acked_one(0, Duration::from_millis(1), 10, 2);
    tally.acked_one(0, Duration::from_millis(1), 10, 2);
    tally.acked_one(1, Duration::from_millis(1), 4, 0);
    assert_eq!(tally.per_shard.get(&2), Some(&20));
    assert_eq!(tally.per_shard.get(&0), Some(&4));
    assert_eq!(tally.acked_sends, 3);
    let failed = failure(&anyhow::anyhow!("connection lost"), 4);
    assert!(failed.transport);
    assert_eq!(failed.generation, 4);
    let mut total = Tally::default();
    total.merge(tally);
    assert_eq!(total.acked, 24);
    assert_eq!(total.acked_sends, 3);
    assert_eq!(total.per_shard.get(&2), Some(&20));
}
