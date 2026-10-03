#[test]
fn a_feature_bit_is_only_supported_when_advertised() {
    assert!(crate::supports_feature(
        crate::FEATURE_TOPOLOGY,
        crate::FEATURE_TOPOLOGY
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_TOPOLOGY));
}

/// The two feature bits are distinct, and neither implies the other.
#[test]
fn the_feature_bits_do_not_overlap() {
    assert_ne!(crate::FEATURE_TOPOLOGY, crate::FEATURE_REDIRECT);
    assert!(!crate::supports_feature(
        crate::FEATURE_TOPOLOGY,
        crate::FEATURE_REDIRECT
    ));
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_REDIRECT
    ));
}

/// Feature bits say a request *exists*; frame flags select a payload layout.
/// They are separate number spaces, and a new feature must not disturb either
/// the frozen v1 flag set or the bits already handed out.
#[test]
fn cache_delete_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_DELETE & (crate::FEATURE_TOPOLOGY | crate::FEATURE_REDIRECT),
        0,
        "the new bit overlaps one already in use",
    );
    assert_eq!(
        crate::FEATURE_CONSUMER_GROUP
            & (crate::FEATURE_TOPOLOGY | crate::FEATURE_REDIRECT | crate::FEATURE_CACHE_DELETE),
        0,
        "the consumer-group bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_DELETE
    ));
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CONSUMER_GROUP
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_CONSUMER_GROUP));
    assert_eq!(
        crate::FEATURE_GROUP_DEAD_LETTERS
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP),
        0,
        "the dead-letter bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_GROUP_DEAD_LETTERS
    ));
    // Serving groups does not imply serving dead letters: a broker built before
    // these requests existed advertises the first bit and not the second.
    assert!(!crate::supports_feature(
        crate::FEATURE_CONSUMER_GROUP,
        crate::FEATURE_GROUP_DEAD_LETTERS
    ));
    // Silence from a peer that predates negotiation must not be read as support.
    assert!(!crate::supports_feature(0, crate::FEATURE_CACHE_DELETE));
}

/// The watch feature is a new bit: disjoint from every bit already handed out,
/// absent from a silent peer, and never implied by the other cache features.
#[test]
fn cache_watch_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_WATCH
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS),
        0,
        "the watch bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_WATCH
    ));
    // Deleting does not imply watching: a broker built before watches exist
    // advertises the delete bit and not this one.
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_DELETE,
        crate::FEATURE_CACHE_WATCH
    ));
    // Silence from a peer that predates negotiation must not be read as support.
    assert!(!crate::supports_feature(0, crate::FEATURE_CACHE_WATCH));
}

/// The retained bit is new, disjoint, and never implied by the watch bit: a
/// broker built when the watch bit meant live-and-resume only must not be
/// asked for retained delivery it would silently not perform.
#[test]
fn cache_watch_retained_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_CACHE_WATCH_RETAINED
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS
                | crate::FEATURE_CACHE_WATCH),
        0,
        "the retained bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_WATCH,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
    assert!(!crate::supports_feature(
        0,
        crate::FEATURE_CACHE_WATCH_RETAINED
    ));
}

/// The counters bit is new and disjoint, absent from silence, and not implied
/// by any cache feature.
#[test]
fn counters_is_a_new_feature_bit_and_disturbs_nothing() {
    assert_eq!(
        crate::FEATURE_COUNTERS
            & (crate::FEATURE_TOPOLOGY
                | crate::FEATURE_REDIRECT
                | crate::FEATURE_CACHE_DELETE
                | crate::FEATURE_CONSUMER_GROUP
                | crate::FEATURE_GROUP_DEAD_LETTERS
                | crate::FEATURE_STREAM_SHARDS
                | crate::FEATURE_CACHE_WATCH
                | crate::FEATURE_CACHE_WATCH_RETAINED),
        0,
        "the counters bit overlaps one already in use",
    );
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_COUNTERS
    ));
    assert!(!crate::supports_feature(
        crate::FEATURE_CACHE_DELETE | crate::FEATURE_CACHE_WATCH,
        crate::FEATURE_COUNTERS
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_COUNTERS));
}

/// The bit is new, disjoint, and absent from silence.
#[test]
fn idempotent_producer_is_a_new_feature_bit_and_disturbs_nothing() {
    let older = crate::FEATURE_TOPOLOGY
        | crate::FEATURE_REDIRECT
        | crate::FEATURE_CACHE_DELETE
        | crate::FEATURE_CONSUMER_GROUP
        | crate::FEATURE_GROUP_DEAD_LETTERS
        | crate::FEATURE_STREAM_SHARDS
        | crate::FEATURE_CACHE_WATCH
        | crate::FEATURE_CACHE_WATCH_RETAINED
        | crate::FEATURE_COUNTERS;
    assert_eq!(crate::FEATURE_IDEMPOTENT_PRODUCER & older, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_IDEMPOTENT_PRODUCER
    ));
    assert!(!crate::supports_feature(
        older,
        crate::FEATURE_IDEMPOTENT_PRODUCER
    ));
    assert!(!crate::supports_feature(
        0,
        crate::FEATURE_IDEMPOTENT_PRODUCER
    ));
}

/// The cache-shards bit is new and disjoint, absent from silence, and not
/// implied by the stream-shards bit: a broker that only knows `stream_shards`
/// has no arm for `cache_shards`.
#[test]
fn cache_shards_is_a_new_feature_bit_and_disturbs_nothing() {
    let older = crate::FEATURE_TOPOLOGY
        | crate::FEATURE_REDIRECT
        | crate::FEATURE_CACHE_DELETE
        | crate::FEATURE_CONSUMER_GROUP
        | crate::FEATURE_GROUP_DEAD_LETTERS
        | crate::FEATURE_STREAM_SHARDS
        | crate::FEATURE_CACHE_WATCH
        | crate::FEATURE_CACHE_WATCH_RETAINED
        | crate::FEATURE_COUNTERS
        | crate::FEATURE_IDEMPOTENT_PRODUCER;
    assert_eq!(crate::FEATURE_CACHE_SHARDS & older, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_CACHE_SHARDS
    ));
    assert!(!crate::supports_feature(older, crate::FEATURE_CACHE_SHARDS));
    assert!(!crate::supports_feature(0, crate::FEATURE_CACHE_SHARDS));
}

#[test]
fn error_codes_is_a_new_feature_bit_and_disturbs_nothing() {
    let others = crate::KNOWN_FEATURES & !crate::FEATURE_ERROR_CODES;
    assert_eq!(crate::FEATURE_ERROR_CODES & others, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_ERROR_CODES
    ));
}

#[test]
fn shard_moved_is_a_new_feature_bit_and_disturbs_nothing() {
    let others = crate::KNOWN_FEATURES & !crate::FEATURE_SHARD_MOVED;
    assert_eq!(crate::FEATURE_SHARD_MOVED & others, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_SHARD_MOVED
    ));
}

#[test]
fn sequence_reused_is_a_new_feature_bit_and_disturbs_nothing() {
    let others = crate::KNOWN_FEATURES & !crate::FEATURE_SEQUENCE_REUSED;
    assert_eq!(crate::FEATURE_SEQUENCE_REUSED & others, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_SEQUENCE_REUSED
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_SEQUENCE_REUSED));
}

#[test]
fn stream_publish_window_is_a_new_feature_bit_and_disturbs_nothing() {
    let others = crate::KNOWN_FEATURES & !crate::FEATURE_STREAM_PUBLISH_WINDOW;
    assert_eq!(crate::FEATURE_STREAM_PUBLISH_WINDOW & others, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_STREAM_PUBLISH_WINDOW
    ));
}

#[test]
fn shard_owners_is_a_new_feature_bit_and_disturbs_nothing() {
    let others = crate::KNOWN_FEATURES & !crate::FEATURE_SHARD_OWNERS;
    assert_eq!(crate::FEATURE_SHARD_OWNERS & others, 0);
    assert!(crate::supports_feature(
        crate::KNOWN_FEATURES,
        crate::FEATURE_SHARD_OWNERS
    ));
    assert!(!crate::supports_feature(0, crate::FEATURE_SHARD_OWNERS));
}

/// **Every feature bit is its own.** The per-bit tests above mask a bit out of
/// `KNOWN_FEATURES`, which two constants sharing a value pass. This lists them
/// all, so a new bit given a value already in use fails here.
#[test]
fn every_feature_bit_is_distinct_and_known() {
    let bits = [
        ("FEATURE_TOPOLOGY", crate::FEATURE_TOPOLOGY),
        ("FEATURE_REDIRECT", crate::FEATURE_REDIRECT),
        ("FEATURE_CACHE_DELETE", crate::FEATURE_CACHE_DELETE),
        ("FEATURE_CONSUMER_GROUP", crate::FEATURE_CONSUMER_GROUP),
        (
            "FEATURE_GROUP_DEAD_LETTERS",
            crate::FEATURE_GROUP_DEAD_LETTERS,
        ),
        ("FEATURE_STREAM_SHARDS", crate::FEATURE_STREAM_SHARDS),
        ("FEATURE_CACHE_WATCH", crate::FEATURE_CACHE_WATCH),
        (
            "FEATURE_CACHE_WATCH_RETAINED",
            crate::FEATURE_CACHE_WATCH_RETAINED,
        ),
        ("FEATURE_COUNTERS", crate::FEATURE_COUNTERS),
        (
            "FEATURE_IDEMPOTENT_PRODUCER",
            crate::FEATURE_IDEMPOTENT_PRODUCER,
        ),
        ("FEATURE_CACHE_SHARDS", crate::FEATURE_CACHE_SHARDS),
        ("FEATURE_ERROR_CODES", crate::FEATURE_ERROR_CODES),
        ("FEATURE_SHARD_MOVED", crate::FEATURE_SHARD_MOVED),
        ("FEATURE_UNSUPPORTED", crate::FEATURE_UNSUPPORTED),
        ("FEATURE_SEQUENCE_REUSED", crate::FEATURE_SEQUENCE_REUSED),
        ("FEATURE_PUBLISH_PIPELINE", crate::FEATURE_PUBLISH_PIPELINE),
        ("FEATURE_ATOMIC_COMMIT", crate::FEATURE_ATOMIC_COMMIT),
        (
            "FEATURE_STREAM_PUBLISH_WINDOW",
            crate::FEATURE_STREAM_PUBLISH_WINDOW,
        ),
        ("FEATURE_GROUP_CONSUMER", crate::FEATURE_GROUP_CONSUMER),
        ("FEATURE_SHARD_OWNERS", crate::FEATURE_SHARD_OWNERS),
    ];
    let mut seen = 0u32;
    for (name, bit) in bits {
        assert_eq!(bit.count_ones(), 1, "{name} is not a single bit");
        assert_eq!(seen & bit, 0, "{name} reuses a bit already taken");
        seen |= bit;
    }
    assert_eq!(
        seen,
        crate::KNOWN_FEATURES,
        "KNOWN_FEATURES and this list disagree"
    );
}
