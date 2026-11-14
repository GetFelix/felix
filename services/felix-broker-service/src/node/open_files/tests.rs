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

#[test]
fn the_target_is_the_hard_limit_and_never_below_the_soft_one() {
    assert_eq!(target(1024, 4096, u64::MAX), 4096);
    assert_eq!(target(4096, 4096, u64::MAX), 4096);
    assert_eq!(target(8192, 4096, u64::MAX), 8192);
}

#[test]
fn the_target_stays_under_the_platform_ceiling() {
    // macOS reports an unlimited hard limit.
    assert_eq!(target(256, u64::MAX, 10_240), 10_240);
    assert_eq!(target(20_000, u64::MAX, 10_240), 20_000);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn raising_leaves_the_soft_limit_at_the_target() {
    let soft = || {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid `rlimit` for the call to fill.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
            0
        );
        (limit.rlim_cur, limit.rlim_max)
    };
    let (before, hard) = soft();
    let (reported_before, after) = raise().expect("raise");
    assert_eq!(reported_before, before);
    assert_eq!(after, target(before, hard, ceiling()));
    assert_eq!(soft().0, after);
    // A second call has nothing to do.
    assert_eq!(raise().expect("raise"), (after, after));
}
