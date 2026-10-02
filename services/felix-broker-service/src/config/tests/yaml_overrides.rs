//! The YAML config file, folded over what the environment already gave.
//!
//! Driven through `apply` rather than a file so the whole precedence table
//! is one test rather than one per key — an override that is parsed but
//! never assigned is otherwise invisible until an operator sets it.

use super::*;

fn parse(yaml: &str) -> BrokerConfigOverride {
    serde_yaml_ng::from_str(yaml).expect("parse the override")
}

/// **A key nobody reads is refused, not ignored.**
///
/// The test above proves every key that parses reaches the config. This
/// is the other half: a key that does *not* parse must say so. Without
/// it an operator who writes `metrics_bnid` gets the default, no error,
/// and a broker listening somewhere they did not ask for — and the file
/// they are looking at says otherwise.
#[test]
fn a_key_the_broker_does_not_know_is_refused() {
    let err = serde_yaml_ng::from_str::<BrokerConfigOverride>("metrics_bnid: \"0.0.0.0:1\"")
        .expect_err("a misspelled key must not be accepted");
    assert!(
        err.to_string().contains("metrics_bnid"),
        "the error has to name the key, or it cannot be acted on: {err}",
    );
}

/// **Every key in the file has to reach the config.** A key that
/// deserializes and is then never assigned looks like a setting that
/// silently does nothing.
#[test]
fn every_key_in_the_file_reaches_the_config() {
    // The flags start false so the assertions below prove an
    // assignment rather than agreeing with a default that is already
    // true.
    let mut config = BrokerConfig {
        ack_on_commit: false,
        disable_timings: false,
        pub_ingress_wait: false,
        subscriber_single_writer_per_conn: false,
        ..Default::default()
    };
    config
        .apply(parse(
            r#"
            quic_bind: "127.0.0.1:5999"
            metrics_bind: "127.0.0.1:8999"
            controlplane_url: "http://cp.example:8443"
            controlplane_sync_interval_ms: 1004
            ack_on_commit: true
            max_frame_bytes: 1006
            publish_queue_wait_timeout_ms: 1007
            ack_wait_timeout_ms: 1008
            disable_timings: true
            control_stream_drain_timeout_ms: 1010
            shutdown_drain_timeout_ms: 1011
            pub_conn_recv_window: 1012
            pub_stream_recv_window: 1013
            cache_send_window: 1014
            event_batch_max_events: 1015
            event_batch_max_bytes: 1016
            event_batch_max_delay_us: 1017
            fanout_batch_size: 1018
            pub_workers_per_conn: 1019
            pub_queue_depth: 1020
            pub_inflight_bytes: 1021
            pub_conn_inflight_bytes: 1022
            pub_ingress_wait: true
            core_shards: 1024
            subscriber_queue_capacity: 1025
            max_subscriptions_per_conn: 1026
            subscriber_queue_policy: "drop_old"
            subscriber_writer_lanes: 1028
            subscriber_lane_queue_depth: 1029
            subscriber_lane_queue_policy: "block"
            max_subscriber_writer_lanes: 1031
            subscriber_lane_shard: "round_robin_pin"
            subscriber_single_writer_per_conn: true
            subscriber_flush_max_items: 1034
            subscriber_flush_max_delay_us: 1035
            subscriber_max_bytes_per_write: 1036
            sub_streams_per_conn: 1037
            sub_stream_mode: "hashed_pool"
"#,
        ))
        .expect("apply");

    assert_eq!(config.quic_bind, "127.0.0.1:5999".parse().unwrap());
    assert_eq!(config.metrics_bind, "127.0.0.1:8999".parse().unwrap());
    assert_eq!(
        config.controlplane_url.as_deref(),
        Some("http://cp.example:8443")
    );
    assert_eq!(config.controlplane_sync_interval_ms, 1004);
    assert!(config.ack_on_commit);
    assert_eq!(config.max_frame_bytes, 1006);
    assert_eq!(config.publish_queue_wait_timeout_ms, 1007);
    assert_eq!(config.ack_wait_timeout_ms, 1008);
    assert!(config.disable_timings);
    assert_eq!(config.control_stream_drain_timeout_ms, 1010);
    assert_eq!(config.shutdown_drain_timeout_ms, 1011);
    assert_eq!(config.pub_conn_recv_window, Some(1012));
    assert_eq!(config.pub_stream_recv_window, Some(1013));
    assert_eq!(config.cache_send_window, 1014);
    assert_eq!(config.event_batch_max_events, 1015);
    assert_eq!(config.event_batch_max_bytes, 1016);
    assert_eq!(config.event_batch_max_delay_us, 1017);
    assert_eq!(config.fanout_batch_size, 1018);
    assert_eq!(config.pub_workers_per_conn, 1019);
    assert_eq!(config.pub_queue_depth, 1020);
    assert_eq!(config.pub_inflight_bytes, 1021);
    assert_eq!(config.pub_conn_inflight_bytes, 1022);
    assert!(config.pub_ingress_wait);
    assert_eq!(config.core_shards, 1024);
    assert_eq!(config.subscriber_queue_capacity, 1025);
    assert_eq!(config.max_subscriptions_per_conn, 1026);
    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);
    assert_eq!(config.subscriber_writer_lanes, 1028);
    assert_eq!(config.subscriber_lane_queue_depth, 1029);
    assert_eq!(config.subscriber_lane_queue_policy, SubQueuePolicy::Block);
    assert_eq!(config.max_subscriber_writer_lanes, 1031);
    assert_eq!(
        config.subscriber_lane_shard,
        SubscriberLaneShard::RoundRobinPin
    );
    assert!(config.subscriber_single_writer_per_conn);
    assert_eq!(config.subscriber_flush_max_items, 1034);
    assert_eq!(config.subscriber_flush_max_delay_us, 1035);
    assert_eq!(config.subscriber_max_bytes_per_write, 1036);
    assert_eq!(config.sub_streams_per_conn, 1037);
    assert_eq!(config.sub_stream_mode, SubStreamMode::HashedPool);
}

/// A file that names no keys changes nothing. Absent is not zero: the
/// environment's value has to survive a config file that says nothing
/// about it.
#[test]
fn an_empty_file_leaves_the_config_alone() {
    let mut config = BrokerConfig {
        controlplane_url: Some("http://from-the-environment".to_string()),
        core_shards: 7,
        ..Default::default()
    };

    config.apply(parse("{}")).expect("apply");

    assert_eq!(
        config.controlplane_url.as_deref(),
        Some("http://from-the-environment")
    );
    assert_eq!(config.core_shards, 7);
}

/// **A zero does not disable a subsystem.** These are sizes and lane
/// counts: zero lanes or a zero-capacity queue is not a smaller
/// configuration, it is a broker that delivers nothing, so a file
/// asking for one is ignored rather than obeyed.
#[test]
fn a_zero_is_ignored_where_zero_would_mean_off() {
    let defaults = BrokerConfig::default();
    let mut config = BrokerConfig::default();

    config
        .apply(parse(
            r#"
            cache_send_window: 0
            event_batch_max_bytes: 0
            event_batch_max_events: 0
            fanout_batch_size: 0
            max_subscriber_writer_lanes: 0
            max_subscriptions_per_conn: 0
            pub_conn_inflight_bytes: 0
            pub_conn_recv_window: 0
            pub_inflight_bytes: 0
            pub_queue_depth: 0
            pub_stream_recv_window: 0
            pub_workers_per_conn: 0
            shutdown_drain_timeout_ms: 0
            sub_streams_per_conn: 0
            subscriber_flush_max_items: 0
            subscriber_lane_queue_depth: 0
            subscriber_max_bytes_per_write: 0
            subscriber_queue_capacity: 0
            subscriber_writer_lanes: 0
"#,
        ))
        .expect("apply");

    assert_eq!(
        config.pub_conn_recv_window, defaults.pub_conn_recv_window,
        "pub_conn_recv_window was disabled by a zero"
    );
    assert_eq!(
        config.cache_send_window, defaults.cache_send_window,
        "cache_send_window was disabled by a zero"
    );
    assert_eq!(
        config.pub_stream_recv_window, defaults.pub_stream_recv_window,
        "pub_stream_recv_window was disabled by a zero"
    );
    assert_eq!(
        config.event_batch_max_bytes, defaults.event_batch_max_bytes,
        "event_batch_max_bytes was disabled by a zero"
    );
    assert_eq!(
        config.event_batch_max_events, defaults.event_batch_max_events,
        "event_batch_max_events was disabled by a zero"
    );
    assert_eq!(
        config.fanout_batch_size, defaults.fanout_batch_size,
        "fanout_batch_size was disabled by a zero"
    );
    assert_eq!(
        config.max_subscriber_writer_lanes, defaults.max_subscriber_writer_lanes,
        "max_subscriber_writer_lanes was disabled by a zero"
    );
    assert_eq!(
        config.max_subscriptions_per_conn, defaults.max_subscriptions_per_conn,
        "max_subscriptions_per_conn was disabled by a zero"
    );
    assert_eq!(
        config.pub_conn_inflight_bytes, defaults.pub_conn_inflight_bytes,
        "pub_conn_inflight_bytes was disabled by a zero"
    );
    assert_eq!(
        config.pub_inflight_bytes, defaults.pub_inflight_bytes,
        "pub_inflight_bytes was disabled by a zero"
    );
    assert_eq!(
        config.pub_queue_depth, defaults.pub_queue_depth,
        "pub_queue_depth was disabled by a zero"
    );
    assert_eq!(
        config.pub_workers_per_conn, defaults.pub_workers_per_conn,
        "pub_workers_per_conn was disabled by a zero"
    );
    assert_eq!(
        config.shutdown_drain_timeout_ms, defaults.shutdown_drain_timeout_ms,
        "shutdown_drain_timeout_ms was disabled by a zero"
    );
    assert_eq!(
        config.sub_streams_per_conn, defaults.sub_streams_per_conn,
        "sub_streams_per_conn was disabled by a zero"
    );
    assert_eq!(
        config.subscriber_flush_max_items, defaults.subscriber_flush_max_items,
        "subscriber_flush_max_items was disabled by a zero"
    );
    assert_eq!(
        config.subscriber_lane_queue_depth, defaults.subscriber_lane_queue_depth,
        "subscriber_lane_queue_depth was disabled by a zero"
    );
    assert_eq!(
        config.subscriber_max_bytes_per_write, defaults.subscriber_max_bytes_per_write,
        "subscriber_max_bytes_per_write was disabled by a zero"
    );
    assert_eq!(
        config.subscriber_queue_capacity, defaults.subscriber_queue_capacity,
        "subscriber_queue_capacity was disabled by a zero"
    );
    assert_eq!(
        config.subscriber_writer_lanes, defaults.subscriber_writer_lanes,
        "subscriber_writer_lanes was disabled by a zero"
    );
}

/// An address that does not parse fails the load and says which key,
/// rather than leaving the broker bound somewhere the operator did not
/// ask for.
#[test]
fn an_unparseable_address_names_the_key_it_came_from() {
    let mut config = BrokerConfig::default();
    let err = config
        .apply(parse("quic_bind: not-an-address"))
        .expect_err("an unparseable address should fail");
    assert!(err.to_string().contains("quic_bind"), "{err}");

    let mut config = BrokerConfig::default();
    let err = config
        .apply(parse("metrics_bind: also-not-an-address"))
        .expect_err("an unparseable address should fail");
    assert!(err.to_string().contains("metrics_bind"), "{err}");
}

/// An unknown queue policy leaves the configured one in place. The
/// alternative is a typo silently switching a broker's overflow
/// behaviour to something the operator did not choose.
#[test]
fn an_unknown_queue_policy_leaves_the_configured_one() {
    let mut config = BrokerConfig {
        subscriber_queue_policy: SubQueuePolicy::DropOld,
        ..Default::default()
    };

    config
        .apply(parse("subscriber_queue_policy: drop_everything"))
        .expect("apply");

    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);
}
