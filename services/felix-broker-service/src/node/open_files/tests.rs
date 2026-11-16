use super::*;

const DEFAULT_SEALED: usize = crate::config::durable::DEFAULT_MAX_OPEN_SEALED_SEGMENTS;

#[test]
fn the_default_soft_limit_holds_no_shards_beside_the_default_cache() {
    assert_eq!(shards_within(1024, DEFAULT_SEALED), 0);
    assert_eq!(shards_within(0, DEFAULT_SEALED), 0);
    assert_eq!(shards_within(SOCKET_RESERVE, 0), 0);
}

#[test]
fn the_configured_cache_comes_off_the_shard_estimate() {
    assert_eq!(shards_within(13_024, 0), 2_000);
    assert_eq!(shards_within(13_024, 1_200), 1_800);
    assert_eq!(shards_within(13_024, 100_000), 0);
}

#[test]
fn a_common_hard_limit_holds_the_gates_shards() {
    // systemd's default hard limit.
    assert!(shards_within(524_288, DEFAULT_SEALED) > 5_000 * 10);
    // The limit the Compose file sets.
    assert!(shards_within(65_536, DEFAULT_SEALED) > 5_000);
}

#[test]
fn the_cache_is_held_to_a_quarter_of_the_limit() {
    assert_eq!(sealed_slots_within(1_024, DEFAULT_SEALED), 256);
    assert_eq!(sealed_slots_within(524_288, DEFAULT_SEALED), DEFAULT_SEALED);
    assert_eq!(sealed_slots_within(8, DEFAULT_SEALED), 16);
    assert_eq!(sealed_slots_within(1_024, 100), 100);
}
