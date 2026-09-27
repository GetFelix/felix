use super::*;

/// The mapping is frozen. A client and a broker that disagree send records
/// to different shards for the same key, so these values are a contract
/// rather than a characterisation of the current implementation.
#[test]
fn the_mapping_is_stable() {
    assert_eq!(shard_for(8, Some(b"customer-0")), 0);
    assert_eq!(shard_for(8, Some(b"customer-4")), 2);
    assert_eq!(shard_for(8, Some(b"customer-1")), 3);
    assert_eq!(shard_for(8, Some(b"customer-6")), 4);
    assert_eq!(shard_for(8, Some(b"customer-2")), 6);
    assert_eq!(shard_for(12, Some(b"k0")), 6);
    assert_eq!(shard_for(12, Some(b"k1")), 10);
    assert_eq!(shard_for(4, Some(b"")), 3);
}

#[test]
fn one_shard_or_no_key_is_always_shard_zero() {
    assert_eq!(shard_for(0, Some(b"anything")), 0);
    assert_eq!(shard_for(1, Some(b"anything")), 0);
    assert_eq!(shard_for(16, None), 0);
}

#[test]
fn the_same_key_always_lands_on_the_same_shard() {
    for shards in 2..=64u32 {
        let first = shard_for(shards, Some(b"customer-42"));
        for _ in 0..8 {
            assert_eq!(shard_for(shards, Some(b"customer-42")), first);
        }
        assert!(first < shards);
    }
}

#[test]
fn keys_spread_across_shards() {
    let seen: std::collections::HashSet<_> = (0..64)
        .map(|i| shard_for(8, Some(format!("key-{i}").as_bytes())))
        .collect();
    assert!(seen.len() > 1, "every key landed on one shard: {seen:?}");
}

/// Frozen like the modulo values: a jump-hash stream's records are placed by
/// these answers, and a client and a broker must reach the same ones.
#[test]
fn the_jump_hash_mapping_is_stable() {
    let jump = |shards, key: &[u8]| shard_for_routing(ShardRouting::JumpHash, shards, Some(key));
    let observed: Vec<u32> = [
        b"customer-0".as_slice(),
        b"customer-1",
        b"customer-2",
        b"customer-4",
        b"customer-6",
    ]
    .iter()
    .map(|key| jump(8, key))
    .collect();
    assert_eq!(observed, JUMP_8);
    assert_eq!(jump(12, b"k0"), JUMP_12_K0);
    assert_eq!(jump(12, b"k1"), JUMP_12_K1);
}

const JUMP_8: [u32; 5] = [3, 0, 5, 2, 7];
const JUMP_12_K0: u32 = 3;
const JUMP_12_K1: u32 = 4;

#[test]
fn modulo_routing_is_the_original_mapping() {
    for shards in 1..=32u32 {
        for i in 0..64 {
            let key = format!("key-{i}");
            assert_eq!(
                shard_for_routing(ShardRouting::Modulo, shards, Some(key.as_bytes())),
                shard_for(shards, Some(key.as_bytes()))
            );
        }
    }
}

#[test]
fn jump_hash_moves_only_the_keys_a_new_shard_takes() {
    let keys: Vec<String> = (0..10_000).map(|i| format!("key-{i}")).collect();
    let place = |routing, shards| -> Vec<u32> {
        keys.iter()
            .map(|key| shard_for_routing(routing, shards, Some(key.as_bytes())))
            .collect()
    };
    let moved = |routing| {
        place(routing, 8)
            .iter()
            .zip(place(routing, 9))
            .filter(|(before, after)| **before != *after)
            .count()
    };
    // A ninth shard should take about a ninth of the keys and leave every
    // other key where it was. Modulo moves close to eight ninths.
    let jump_moved = moved(ShardRouting::JumpHash);
    assert!(
        (800..=1_500).contains(&jump_moved),
        "jump hash moved {jump_moved} of 10000"
    );
    assert!(moved(ShardRouting::Modulo) > 8_000);
    for (before, after) in place(ShardRouting::JumpHash, 8)
        .iter()
        .zip(place(ShardRouting::JumpHash, 9))
    {
        assert!(
            *before == after || after == 8,
            "a key moved between old shards"
        );
    }
}

#[test]
fn jump_hash_stays_in_range_and_spreads() {
    for shards in 1..=64u32 {
        let mut seen = std::collections::HashSet::new();
        for i in 0..512 {
            let shard = shard_for_routing(
                ShardRouting::JumpHash,
                shards,
                Some(format!("key-{i}").as_bytes()),
            );
            assert!(shard < shards);
            seen.insert(shard);
        }
        assert_eq!(seen.len() as u32, shards);
    }
    assert_eq!(shard_for_routing(ShardRouting::JumpHash, 16, None), 0);
}

#[test]
fn routing_mode_spells_as_snake_case() {
    assert_eq!(
        serde_json::to_string(&ShardRouting::JumpHash).unwrap(),
        "\"jump_hash\""
    );
    assert_eq!(
        serde_json::from_str::<ShardRouting>("\"modulo\"").unwrap(),
        ShardRouting::Modulo
    );
}
