use std::collections::BTreeMap;

use felix_wire::routing::{ShardRouting, shard_for_routing};

use super::spread_keys;

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
