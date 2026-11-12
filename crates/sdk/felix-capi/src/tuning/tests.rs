use std::mem::offset_of;
use std::ptr;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

fn options() -> FelixConnectOptions {
    FelixConnectOptions {
        size: size_of::<FelixConnectOptions>(),
        publishers: false,
        timestamps: false,
        token_fetch: None,
        token_user_data: ptr::null_mut(),
        token_release: None,
        reconnect_attempts: 0,
        reconnect_deadline_ms: 0,
        reconnect_backoff_ms: 0,
        reconnect_max_backoff_ms: 0,
        client_cert_file: ptr::null(),
        client_key_file: ptr::null(),
        disable_record_headers: false,
        ack_on_commit: false,
        no_publish_shard_streams: false,
        client_sub_queue_policy: 0,
        publish_sharding: 0,
        client_sub_queue_capacity: 0,
        broker_sub_queue_capacity: 0,
        publish_conn_pool: 0,
        publish_streams_per_conn: 0,
        publish_queue_depth: 0,
        publish_shard_streams: 0,
        cache_conn_pool: 0,
        cache_streams_per_conn: 0,
        event_conn_pool: 0,
        cluster_conn_pool: 0,
        cluster_streams_per_conn: 0,
        event_router_max_pending: 0,
        publish_inflight_bytes: 0,
        max_frame_bytes: 0,
        event_conn_recv_window: 0,
        event_stream_recv_window: 0,
        event_send_window: 0,
        cache_conn_recv_window: 0,
        cache_stream_recv_window: 0,
        cache_send_window: 0,
        connect_timeout_ms: 0,
        finish_timeout_ms: 0,
    }
}

fn read(options: &FelixConnectOptions) -> Result<Tuning, Failure> {
    // SAFETY: `options` is live and `size` stays within it.
    unsafe { Tuning::read(options, options.size) }
}

fn config() -> ClientConfig {
    let quinn = felix_client::quic_client_config(None, false).expect("quic config");
    ClientConfig::optimized_defaults(quinn)
}

#[test]
fn all_zero_changes_nothing() {
    let tuning = read(&options()).unwrap();
    assert_eq!(tuning, Tuning::default());
    let (mut tuned, plain) = (config(), config());
    tuning.apply(&mut tuned);
    assert_eq!(tuned.publish_conn_pool, plain.publish_conn_pool);
    assert_eq!(tuned.publish_shard_streams, plain.publish_shard_streams);
    assert_eq!(tuned.max_frame_bytes, plain.max_frame_bytes);
    assert_eq!(tuned.connect_timeout, plain.connect_timeout);
    assert_eq!(tuned.client_sub_queue_policy, plain.client_sub_queue_policy);
    assert_eq!(tuned.broker_sub_queue_capacity, None);
    assert!(!tuned.ack_on_commit);
}

#[test]
fn every_field_reaches_the_config() {
    let mut raw = options();
    raw.client_sub_queue_policy = FELIX_QUEUE_BLOCK;
    raw.publish_sharding = FELIX_SHARDING_ROUND_ROBIN;
    raw.client_sub_queue_capacity = 11;
    raw.broker_sub_queue_capacity = 12;
    raw.publish_conn_pool = 13;
    raw.publish_streams_per_conn = 14;
    raw.publish_queue_depth = 15;
    raw.publish_shard_streams = 16;
    raw.cache_conn_pool = 17;
    raw.cache_streams_per_conn = 18;
    raw.event_conn_pool = 19;
    raw.cluster_conn_pool = 20;
    raw.cluster_streams_per_conn = 21;
    raw.event_router_max_pending = 22;
    raw.publish_inflight_bytes = 23;
    raw.max_frame_bytes = 24;
    raw.event_conn_recv_window = 25;
    raw.event_stream_recv_window = 26;
    raw.event_send_window = 27;
    raw.cache_conn_recv_window = 28;
    raw.cache_stream_recv_window = 29;
    raw.cache_send_window = 30;
    raw.connect_timeout_ms = 31_000;
    raw.finish_timeout_ms = 32_000;
    raw.ack_on_commit = true;
    let mut config = config();
    read(&raw).unwrap().apply(&mut config);
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.publish_sharding, PublishSharding::RoundRobin);
    assert_eq!(config.client_sub_queue_capacity, 11);
    assert_eq!(config.broker_sub_queue_capacity, Some(12));
    assert_eq!(config.publish_conn_pool, 13);
    assert_eq!(config.publish_streams_per_conn, 14);
    assert_eq!(config.publish_queue_depth, 15);
    assert_eq!(config.publish_shard_streams, 16);
    assert_eq!(config.cache_conn_pool, 17);
    assert_eq!(config.cache_streams_per_conn, 18);
    assert_eq!(config.event_conn_pool, 19);
    assert_eq!(config.cluster_conn_pool, 20);
    assert_eq!(config.cluster_streams_per_conn, 21);
    assert_eq!(config.event_router_max_pending, 22);
    assert_eq!(config.publish_inflight_bytes, 23);
    assert_eq!(config.max_frame_bytes, 24);
    assert_eq!(config.event_conn_recv_window, 25);
    assert_eq!(config.event_stream_recv_window, 26);
    assert_eq!(config.event_send_window, 27);
    assert_eq!(config.cache_conn_recv_window, 28);
    assert_eq!(config.cache_stream_recv_window, 29);
    assert_eq!(config.cache_send_window, 30);
    assert_eq!(config.connect_timeout, Duration::from_secs(31));
    assert_eq!(config.finish_timeout, Duration::from_secs(32));
    assert!(config.ack_on_commit);
}

#[test]
fn the_other_policy_and_sharding_values_map() {
    let mut raw = options();
    for (code, want) in [
        (FELIX_QUEUE_DROP_NEW, ClientSubQueuePolicy::DropNew),
        (FELIX_QUEUE_DROP_OLD, ClientSubQueuePolicy::DropOld),
    ] {
        raw.client_sub_queue_policy = code;
        let mut config = config();
        read(&raw).unwrap().apply(&mut config);
        assert_eq!(config.client_sub_queue_policy, want);
    }
    raw.publish_sharding = FELIX_SHARDING_HASH_STREAM;
    let mut config = config();
    config.publish_sharding = PublishSharding::RoundRobin;
    read(&raw).unwrap().apply(&mut config);
    assert_eq!(config.publish_sharding, PublishSharding::HashStream);
}

#[test]
fn no_shard_streams_asks_for_none_and_beats_a_count() {
    let mut raw = options();
    raw.no_publish_shard_streams = true;
    raw.publish_shard_streams = 9;
    let mut config = config();
    read(&raw).unwrap().apply(&mut config);
    assert_eq!(config.publish_shard_streams, 0);
}

#[test]
fn an_unknown_enum_value_is_refused() {
    let mut raw = options();
    raw.client_sub_queue_policy = 9;
    let failure = read(&raw).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("client_sub_queue_policy"));
    let mut raw = options();
    raw.publish_sharding = -1;
    let failure = read(&raw).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("publish_sharding"));
}

#[test]
fn a_window_a_quic_integer_cannot_hold_is_refused() {
    let mut raw = options();
    raw.cache_send_window = 1 << 62;
    let failure = read(&raw).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("cache_send_window"));
    raw.cache_send_window = (1 << 62) - 1;
    assert!(read(&raw).is_ok());
}

#[test]
fn timeouts_are_capped_at_a_year() {
    let mut raw = options();
    raw.connect_timeout_ms = u64::MAX;
    let mut config = config();
    read(&raw).unwrap().apply(&mut config);
    assert_eq!(config.connect_timeout, A_YEAR);
}

#[test]
fn a_caller_built_before_the_fields_existed_gets_the_defaults() {
    let mut raw = options();
    raw.client_sub_queue_policy = 9;
    raw.publish_conn_pool = 5;
    raw.max_frame_bytes = 7;
    raw.ack_on_commit = true;
    raw.finish_timeout_ms = 5;
    // A struct that ends right after the fields it knew about.
    raw.size = offset_of!(FelixConnectOptions, ack_on_commit);
    assert_eq!(read(&raw).unwrap(), Tuning::default());
}

#[test]
fn a_field_is_read_only_when_size_covers_all_of_it() {
    let mut raw = options();
    raw.publish_conn_pool = 5;
    raw.max_frame_bytes = 7;
    raw.finish_timeout_ms = 5;
    raw.size = offset_of!(FelixConnectOptions, publish_conn_pool) + 3;
    assert_eq!(read(&raw).unwrap().publish_conn_pool, None);
    raw.size = offset_of!(FelixConnectOptions, publish_conn_pool) + 4;
    assert_eq!(read(&raw).unwrap().publish_conn_pool, Some(5));
    // The u64 after it is still out of reach.
    assert_eq!(read(&raw).unwrap().max_frame_bytes, None);
    raw.size = offset_of!(FelixConnectOptions, max_frame_bytes) + 8;
    let tuning = read(&raw).unwrap();
    assert_eq!(tuning.max_frame_bytes, Some(7));
    assert_eq!(tuning.finish_timeout, None);
}

#[test]
fn the_new_fields_sit_after_the_old_ones_at_the_end() {
    // Appended fields keep every earlier offset, which is what a caller built
    // against the older header relies on.
    let disable = offset_of!(FelixConnectOptions, disable_record_headers);
    assert!(offset_of!(FelixConnectOptions, ack_on_commit) > disable);
    assert!(
        offset_of!(FelixConnectOptions, finish_timeout_ms)
            > offset_of!(FelixConnectOptions, ack_on_commit)
    );
    assert_eq!(
        offset_of!(FelixConnectOptions, finish_timeout_ms) + 8,
        size_of::<FelixConnectOptions>()
    );
}

/// The offsets the C# marshaller and the header promise on a 64-bit target.
#[cfg(target_pointer_width = "64")]
#[test]
fn the_option_layout_is_stable() {
    assert_eq!(offset_of!(FelixConnectOptions, disable_record_headers), 88);
    assert_eq!(offset_of!(FelixConnectOptions, ack_on_commit), 89);
    assert_eq!(
        offset_of!(FelixConnectOptions, no_publish_shard_streams),
        90
    );
    assert_eq!(offset_of!(FelixConnectOptions, client_sub_queue_policy), 92);
    assert_eq!(offset_of!(FelixConnectOptions, publish_sharding), 96);
    assert_eq!(
        offset_of!(FelixConnectOptions, client_sub_queue_capacity),
        100
    );
    assert_eq!(
        offset_of!(FelixConnectOptions, broker_sub_queue_capacity),
        104
    );
    assert_eq!(offset_of!(FelixConnectOptions, publish_conn_pool), 108);
    assert_eq!(
        offset_of!(FelixConnectOptions, event_router_max_pending),
        144
    );
    assert_eq!(offset_of!(FelixConnectOptions, publish_inflight_bytes), 152);
    assert_eq!(offset_of!(FelixConnectOptions, max_frame_bytes), 160);
    assert_eq!(offset_of!(FelixConnectOptions, cache_send_window), 208);
    assert_eq!(offset_of!(FelixConnectOptions, connect_timeout_ms), 216);
    assert_eq!(offset_of!(FelixConnectOptions, finish_timeout_ms), 224);
    assert_eq!(size_of::<FelixConnectOptions>(), 232);
}
