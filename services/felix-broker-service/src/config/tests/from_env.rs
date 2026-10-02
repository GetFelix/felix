//! Configuration read from the environment alone.

use super::*;

#[serial]
#[test]
fn from_env_uses_defaults() {
    clear_felix_env();
    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.quic_bind.to_string(), "0.0.0.0:5000");
    assert_eq!(config.metrics_bind.to_string(), "0.0.0.0:8080");
    assert!(config.controlplane_url.is_none());
    assert_eq!(config.controlplane_sync_interval_ms, 2000);
    assert!(!config.ack_on_commit);
    assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    assert_eq!(
        config.publish_queue_wait_timeout_ms,
        DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS
    );
    assert_eq!(config.ack_wait_timeout_ms, DEFAULT_ACK_WAIT_TIMEOUT_MS);
    assert_eq!(config.disable_timings, DEFAULT_DISABLE_TIMINGS);
    assert_eq!(
        config.control_stream_drain_timeout_ms,
        DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS
    );
    assert_eq!(config.pub_inflight_bytes, DEFAULT_PUB_INFLIGHT_BYTES);
    assert_eq!(config.core_shards, 0);
    assert_eq!(
        config.subscriber_queue_capacity,
        DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
    );
    assert_eq!(
        config.subscriber_queue_policy,
        DEFAULT_SUBSCRIBER_QUEUE_POLICY
    );
    assert_eq!(
        config.subscriber_writer_lanes,
        DEFAULT_SUBSCRIBER_WRITER_LANES
    );
    assert_eq!(
        config.subscriber_lane_queue_depth,
        DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH
    );
    assert_eq!(
        config.subscriber_lane_queue_policy,
        DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY
    );
    assert_eq!(
        config.max_subscriber_writer_lanes,
        DEFAULT_MAX_SUBSCRIBER_WRITER_LANES
    );
    assert_eq!(config.subscriber_lane_shard, DEFAULT_SUBSCRIBER_LANE_SHARD);
    assert!(!config.subscriber_single_writer_per_conn);
    assert_eq!(
        config.subscriber_flush_max_items,
        DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS
    );
    assert_eq!(
        config.subscriber_flush_max_delay_us,
        DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US
    );
    assert_eq!(
        config.subscriber_max_bytes_per_write,
        DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE
    );
    assert_eq!(config.sub_streams_per_conn, DEFAULT_SUB_STREAMS_PER_CONN);
    assert_eq!(config.sub_stream_mode, DEFAULT_SUB_STREAM_MODE);
}

/// **The ack waiter outlasts the quorum wait it may be sitting on.** Giving
/// up first reports a timeout for a publish the broker is still correctly
/// waiting for, and throws away the quorum wait's more specific answer.
#[test]
fn the_ack_wait_outlasts_the_quorum_wait() {
    let config = BrokerConfig::default();
    assert!(
        config.ack_wait_timeout().as_millis() as u64 > config.publish_quorum_timeout_ms,
        "ack wait {:?} does not outlast the quorum wait {}ms",
        config.ack_wait_timeout(),
        config.publish_quorum_timeout_ms,
    );
}

/// A configured ack wait longer than the quorum wait is kept as configured.
#[test]
fn a_longer_configured_ack_wait_is_left_alone() {
    let config = BrokerConfig {
        ack_wait_timeout_ms: 30_000,
        publish_quorum_timeout_ms: 5_000,
        ..BrokerConfig::default()
    };
    assert_eq!(config.ack_wait_timeout().as_millis() as u64, 30_000);
}

#[serial]
#[test]
fn from_env_respects_env_vars() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "127.0.0.1:6000");
        env::set_var("FELIX_BROKER_METRICS_BIND", "127.0.0.1:9000");
        env::set_var(
            "FELIX_CONTROLPLANE_URL",
            "http://controlplane.example.com:8443",
        );
        env::set_var("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS", "5000");
        env::set_var("FELIX_ACK_ON_COMMIT", "true");
        env::set_var("FELIX_MAX_FRAME_BYTES", "32000000");
        env::set_var("FELIX_PUBLISH_QUEUE_WAIT_MS", "3000");
        env::set_var("FELIX_ACK_WAIT_TIMEOUT_MS", "4000");
        env::set_var("FELIX_DISABLE_TIMINGS", "yes");
        env::set_var("FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS", "100");
        env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "128");
        env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "512000");
        env::set_var("FELIX_FANOUT_BATCH", "256");
        env::set_var("FELIX_SUB_WRITER_LANES", "8");
        env::set_var("FELIX_SUB_LANE_QUEUE_DEPTH", "4096");
        env::set_var("FELIX_SUB_QUEUE_MODE", "drop_old");
        env::set_var("FELIX_MAX_SUB_WRITER_LANES", "16");
        env::set_var("FELIX_SUB_LANE_SHARD", "connection_id_hash");
        env::set_var("FELIX_SUB_QUEUE_POLICY", "block");
        env::set_var("FELIX_SUB_SINGLE_WRITER_PER_CONN", "false");
        env::set_var("FELIX_SUB_FLUSH_MAX_ITEMS", "32");
        env::set_var("FELIX_SUB_FLUSH_MAX_DELAY_US", "150");
        env::set_var("FELIX_SUB_MAX_BYTES_PER_WRITE", "131072");
        env::set_var("FELIX_SUB_STREAMS_PER_CONN", "8");
        env::set_var("FELIX_SUB_STREAM_MODE", "hashed_pool");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.quic_bind.to_string(), "127.0.0.1:6000");
    assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9000");
    assert_eq!(
        config.controlplane_url,
        Some("http://controlplane.example.com:8443".to_string())
    );
    assert_eq!(config.controlplane_sync_interval_ms, 5000);
    assert!(config.ack_on_commit);
    assert_eq!(config.max_frame_bytes, 32000000);
    assert_eq!(config.publish_queue_wait_timeout_ms, 3000);
    assert_eq!(config.ack_wait_timeout_ms, 4000);
    assert!(config.disable_timings);
    assert_eq!(config.control_stream_drain_timeout_ms, 100);
    assert_eq!(config.event_batch_max_events, 128);
    assert_eq!(config.event_batch_max_bytes, 512000);
    assert_eq!(config.fanout_batch_size, 256);
    assert_eq!(config.subscriber_writer_lanes, 8);
    assert_eq!(config.subscriber_lane_queue_depth, 4096);
    assert_eq!(config.subscriber_lane_queue_policy, SubQueuePolicy::DropOld);
    assert_eq!(config.max_subscriber_writer_lanes, 16);
    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::Block);
    assert_eq!(
        config.subscriber_lane_shard,
        SubscriberLaneShard::ConnectionIdHash
    );
    assert!(!config.subscriber_single_writer_per_conn);
    assert_eq!(config.subscriber_flush_max_items, 32);
    assert_eq!(config.subscriber_flush_max_delay_us, 150);
    assert_eq!(config.subscriber_max_bytes_per_write, 131072);
    assert_eq!(config.sub_streams_per_conn, 8);
    assert_eq!(config.sub_stream_mode, SubStreamMode::HashedPool);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_ack_on_commit_variations() {
    clear_felix_env();
    for val in &["1", "true", "yes"] {
        unsafe {
            env::set_var("FELIX_ACK_ON_COMMIT", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(config.ack_on_commit, "expected true for {}", val);
    }
    for val in &["0", "false", "no", "anything"] {
        unsafe {
            env::set_var("FELIX_ACK_ON_COMMIT", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(!config.ack_on_commit, "expected false for {}", val);
    }
    clear_felix_env();
}

#[serial]
#[test]
fn from_env_filters_zero_values() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_MAX_FRAME_BYTES", "0");
        env::set_var("FELIX_PUBLISH_QUEUE_WAIT_MS", "0");
        env::set_var("FELIX_FANOUT_BATCH", "0");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    // Should use defaults when 0 is provided
    assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    assert_eq!(
        config.publish_queue_wait_timeout_ms,
        DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS
    );
    assert_eq!(config.fanout_batch_size, 64);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_rejects_invalid_socket_addr() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "not-a-valid-address");
    }
    let result = BrokerConfig::from_env();
    assert!(result.is_err());
    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_all_window_settings() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_BROKER_PUB_CONN_RECV_WINDOW", "512000000");
        env::set_var("FELIX_BROKER_PUB_STREAM_RECV_WINDOW", "128000000");
        env::set_var("FELIX_CACHE_SEND_WINDOW", "512000000");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.pub_recv_windows(), (512000000, 128000000));
    assert_eq!(config.cache_send_window, 512000000);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_worker_and_queue_settings() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_BROKER_PUB_WORKERS_PER_CONN", "8");
        env::set_var("FELIX_BROKER_PUB_QUEUE_DEPTH", "2048");
        env::set_var("FELIX_SUBSCRIBER_QUEUE_CAPACITY", "256");
        env::set_var("FELIX_SUB_QUEUE_POLICY", "drop_old");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.pub_workers_per_conn, 8);
    assert_eq!(config.pub_queue_depth, 2048);
    assert_eq!(config.subscriber_queue_capacity, 256);
    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_core_shards() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_CORE_SHARDS", "4");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.core_shards, 4);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_pub_inflight_bytes() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_BROKER_PUBLISH_INFLIGHT_BYTES", "8388608");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.pub_inflight_bytes, 8388608);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_batch_settings() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "256");
        env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1048576");
        env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "500");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.event_batch_max_events, 256);
    assert_eq!(config.event_batch_max_bytes, 1048576);
    assert_eq!(config.event_batch_max_delay_us, 500);

    clear_felix_env();
}
