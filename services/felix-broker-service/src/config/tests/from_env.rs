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
    assert!(
        config.ack_durable_on_commit,
        "durable streams ack after commit unless the operator opts out"
    );
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
        config.subscriber_queue_max_bytes,
        DEFAULT_SUBSCRIBER_QUEUE_MAX_BYTES
    );
    assert_eq!(config.subscriber_queue_max_bytes_total, 0);
    assert_eq!(
        config.subscriber_egress_max_bytes,
        DEFAULT_SUBSCRIBER_EGRESS_MAX_BYTES
    );
    assert_eq!(config.replay_ring_max_bytes, DEFAULT_REPLAY_RING_MAX_BYTES);
    assert_eq!(config.replay_ring_max_bytes_total, 0);
    assert_eq!(config.cache_watch_max_bytes, DEFAULT_CACHE_WATCH_MAX_BYTES);
    assert_eq!(config.cache_watch_max_bytes_total, 0);
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

/// A forward waits out the owner's quorum wait and gets its answer, and the
/// publish budget covers that whole forward.
#[test]
fn a_forward_outlasts_the_owners_quorum_wait() {
    let config = BrokerConfig::default();
    let quorum = std::time::Duration::from_millis(config.publish_quorum_timeout_ms);
    assert!(config.peer_forward_timeout() > quorum);
    assert!(config.forward_budget() >= config.peer_forward_timeout());
    assert!(config.ack_wait_timeout() > config.forward_budget());
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
    for val in &["1", "true", "yes", "on", " TRUE ", "Yes"] {
        unsafe {
            env::set_var("FELIX_ACK_ON_COMMIT", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(config.ack_on_commit, "expected true for {}", val);
        assert!(config.ack_durable_on_commit, "expected true for {}", val);
    }
    for val in &["0", "false", "no", "off", " False ", "NO"] {
        unsafe {
            env::set_var("FELIX_ACK_ON_COMMIT", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(!config.ack_on_commit, "expected false for {}", val);
        assert!(
            !config.ack_durable_on_commit,
            "{val} must opt durable streams out"
        );
    }
    // A value that is not a boolean must not weaken the default.
    for val in &["anything", "", "ture", "2"] {
        unsafe {
            env::set_var("FELIX_ACK_ON_COMMIT", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(!config.ack_on_commit, "{val:?} must not ack everything");
        assert!(
            config.ack_durable_on_commit,
            "{val:?} must keep durable acks on commit"
        );
    }
    clear_felix_env();
}

/// Record headers are on unless an operator turns them off, and a value that
/// is not a boolean leaves them on.
#[serial]
#[test]
fn from_env_record_headers_default_on() {
    clear_felix_env();
    assert!(BrokerConfig::from_env().expect("from_env").record_headers);
    for val in &["0", "false", "no", "off", " False "] {
        unsafe {
            env::set_var("FELIX_RECORD_HEADERS", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(!config.record_headers, "expected off for {val:?}");
    }
    for val in &["1", "true", "yes", "on", "anything", ""] {
        unsafe {
            env::set_var("FELIX_RECORD_HEADERS", val);
        }
        let config = BrokerConfig::from_env().expect("from_env");
        assert!(config.record_headers, "expected on for {val:?}");
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

/// Zero is a setting here, not a value to filter out: it turns the bound off.
#[serial]
#[test]
fn from_env_reads_the_subscriber_byte_bounds() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_SUB_QUEUE_MAX_BYTES", "0");
        env::set_var("FELIX_SUB_QUEUE_MAX_BYTES_TOTAL", "1073741824");
        env::set_var("FELIX_SUB_EGRESS_MAX_BYTES", "0");
        env::set_var("FELIX_REPLAY_RING_MAX_BYTES", "1");
        env::set_var("FELIX_REPLAY_RING_MAX_BYTES_TOTAL", "2");
        env::set_var("FELIX_CACHE_WATCH_MAX_BYTES", "3");
        env::set_var("FELIX_CACHE_WATCH_MAX_BYTES_TOTAL", "4");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.subscriber_queue_max_bytes, 0);
    assert_eq!(config.subscriber_queue_max_bytes_total, 1073741824);
    assert_eq!(config.subscriber_egress_max_bytes, 0);
    assert_eq!(config.replay_ring_max_bytes, 1);
    assert_eq!(config.replay_ring_max_bytes_total, 2);
    assert_eq!(config.cache_watch_max_bytes, 3);
    assert_eq!(config.cache_watch_max_bytes_total, 4);

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

/// **A metrics bind that is not an address is refused**, naming the variable,
/// rather than falling back to the default port.
#[serial]
#[test]
fn an_unparseable_metrics_bind_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_BROKER_METRICS_BIND", "not-an-address");
    }
    let err = BrokerConfig::from_env().expect_err("bad metrics bind");
    assert!(
        format!("{err:#}").contains("FELIX_BROKER_METRICS_BIND"),
        "{err:#}"
    );
}

/// **The IO runtime thread count is trimmed, a blank value means unset, and a
/// non-number is an error** naming the value, not a silent default.
#[serial]
#[test]
fn io_runtime_threads_is_parsed_strictly() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_IO_RUNTIME_THREADS", "   ");
    }
    assert_eq!(
        BrokerConfig::from_env().expect("blank").io_runtime_threads,
        None
    );

    unsafe {
        env::set_var("FELIX_IO_RUNTIME_THREADS", " 6 ");
    }
    assert_eq!(
        BrokerConfig::from_env().expect("six").io_runtime_threads,
        Some(6)
    );

    unsafe {
        env::set_var("FELIX_IO_RUNTIME_THREADS", "lots");
    }
    let err = BrokerConfig::from_env().expect_err("not a number");
    let message = format!("{err:#}");
    assert!(message.contains("FELIX_IO_RUNTIME_THREADS"), "{message}");
    assert!(message.contains("lots"), "{message}");
}

/// **Starting without the control plane takes the usual truthy spellings in
/// any case; anything else leaves it off**, since it gives up a safety check.
#[serial]
#[test]
fn start_without_controlplane_needs_an_explicit_yes() {
    for (value, expected) in [
        ("1", true),
        ("TRUE", true),
        (" yes ", true),
        ("On", true),
        ("0", false),
        ("maybe", false),
        ("", false),
    ] {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_START_WITHOUT_CONTROLPLANE", value);
        }
        let config = BrokerConfig::from_env().expect("config");
        assert_eq!(config.start_without_controlplane, expected, "{value:?}");
    }
}

/// **A refresh token passed by value is refused**: refreshing spends it, and
/// the broker would have nowhere to write the replacement. A blank value is
/// treated as unset.
#[serial]
#[test]
fn a_refresh_token_by_value_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_REFRESH_TOKEN", "a-refresh-token");
    }
    let err = BrokerConfig::from_env().expect_err("by value");
    let message = err.to_string();
    assert!(
        message.contains("FELIX_NODE_REFRESH_TOKEN_FILE"),
        "{message}"
    );

    unsafe {
        env::set_var("FELIX_NODE_REFRESH_TOKEN", "  ");
    }
    assert!(
        BrokerConfig::from_env().is_ok(),
        "a blank value is no value"
    );
}

/// **The refresh token file path is trimmed, and a blank one is no file.**
#[serial]
#[test]
fn the_refresh_token_file_path_is_trimmed() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_REFRESH_TOKEN_FILE", "  /run/felix/refresh  ");
    }
    assert_eq!(
        BrokerConfig::from_env()
            .expect("path")
            .node_refresh_token_file,
        Some(std::path::PathBuf::from("/run/felix/refresh"))
    );

    unsafe {
        env::set_var("FELIX_NODE_REFRESH_TOKEN_FILE", " ");
    }
    assert_eq!(
        BrokerConfig::from_env()
            .expect("blank")
            .node_refresh_token_file,
        None
    );
}

/// **A token file that cannot be read is an error naming the variable and the
/// path**, not an empty credential the control plane would refuse later.
#[serial]
#[test]
fn an_unreadable_token_file_is_refused() {
    let dir = TempDir::new().expect("dir");
    let missing = dir.path().join("absent.token");
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_TOKEN_FILE", missing.to_str().expect("path"));
    }
    let err = BrokerConfig::from_env().expect_err("missing file");
    let message = format!("{err:#}");
    assert!(message.contains("FELIX_NODE_TOKEN_FILE"), "{message}");
    assert!(message.contains("absent.token"), "{message}");
}

/// **`FELIX_QUORUM_READS` defaults to the majority round, takes `lease` in any
/// case, and refuses anything else** rather than guess which safety level was
/// meant.
#[serial]
#[test]
fn quorum_reads_take_majority_or_lease_only() {
    use crate::config::env::quorum_reads_by_lease;

    clear_felix_env();
    assert!(!quorum_reads_by_lease().expect("unset"));
    for (value, lease) in [("", false), ("majority", false), (" LEASE ", true)] {
        unsafe {
            env::set_var("FELIX_QUORUM_READS", value);
        }
        assert_eq!(quorum_reads_by_lease().expect(value), lease, "{value:?}");
    }
    unsafe {
        env::set_var("FELIX_QUORUM_READS", "fast");
    }
    let err = quorum_reads_by_lease().expect_err("unknown mode");
    assert!(err.to_string().contains("fast"), "{err}");
}

/// **An explicit listener count of zero, or one whose ports run past 65535, is
/// refused**; a non-number names the value.
#[serial]
#[test]
fn quic_listener_counts_that_cannot_bind_are_refused() {
    for (bind, count, needle) in [
        ("0.0.0.0:5000", "0", "is 0"),
        ("0.0.0.0:65534", "3", "runs past 65535"),
        ("0.0.0.0:5000", "four", "four"),
    ] {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_QUIC_BIND", bind);
            env::set_var("FELIX_QUIC_LISTENERS", count);
        }
        let err = BrokerConfig::from_env().expect_err(count);
        let message = format!("{err:#}");
        assert!(message.contains(needle), "{count}: {message}");
    }
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:65534");
        env::set_var("FELIX_QUIC_LISTENERS", "2");
    }
    assert_eq!(
        BrokerConfig::from_env().expect("fits").quic_listeners,
        2,
        "a range ending exactly at 65535 fits"
    );
}

/// **The admission, group, publish-connection and shard-move limits are read
/// from their variables**, and the group's visibility ceiling is never below
/// its visibility timeout.
#[serial]
#[test]
fn from_env_reads_the_admission_group_and_move_limits() {
    clear_felix_env();
    let vars = [
        ("FELIX_PREAUTH_MAX_FRAME_BYTES", "4096"),
        ("FELIX_PREAUTH_MAX_STREAMS_PER_CONN", "3"),
        ("FELIX_AUTH_TIMEOUT_MS", "0"),
        ("FELIX_MAX_CLIENT_CONNECTIONS", "77"),
        ("FELIX_GROUP_VISIBILITY_TIMEOUT_MS", "90000"),
        ("FELIX_GROUP_MAX_ATTEMPTS", "9"),
        ("FELIX_GROUP_MAX_WAIT_MS", "1500"),
        ("FELIX_GROUP_MAX_VISIBILITY_MS", "1000"),
        ("FELIX_GROUP_MAX_IN_FLIGHT", "250"),
        ("FELIX_BROKER_PUB_FLUSH_CONCURRENCY", "5"),
        ("FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES", "65536"),
        ("FELIX_BROKER_PUBLISH_CONN_TOTAL_INFLIGHT_BYTES", "131072"),
        ("FELIX_BROKER_PUBLISH_WINDOW", "17"),
        ("FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX", "8192"),
        ("FELIX_MAX_SUBSCRIPTIONS_PER_CONN", "12"),
        ("FELIX_MAX_SUBSCRIPTIONS_PER_CONN_TOTAL", "40"),
        ("FELIX_PUBLISH_QUORUM_TIMEOUT_MS", "2500"),
        ("FELIX_SHARD_MOVE_HOLD_MS", "0"),
        ("FELIX_SHARD_MOVE_HOLD_MAX", "33"),
    ];
    for (key, value) in vars {
        unsafe {
            env::set_var(key, value);
        }
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(config.preauth_max_frame_bytes, 4096);
    assert_eq!(config.preauth_max_streams_per_conn, 3);
    assert_eq!(config.auth_timeout_ms, 0, "zero means no auth deadline");
    assert_eq!(config.max_client_connections, 77);
    assert_eq!(config.group_visibility_timeout_ms, 90_000);
    assert_eq!(config.group_max_attempts, 9);
    assert_eq!(config.group_max_wait_ms, 1500);
    assert_eq!(
        config.group_max_visibility_ms, 90_000,
        "the ceiling is never below the visibility timeout"
    );
    assert_eq!(config.group_max_in_flight, 250);
    assert_eq!(config.pub_flush_concurrency, 5);
    assert_eq!(config.pub_conn_inflight_bytes, 65_536);
    assert_eq!(config.pub_conn_total_inflight_bytes, Some(131_072));
    assert_eq!(config.publish_window, 17);
    assert_eq!(config.subscriber_queue_capacity_max, 8192);
    assert_eq!(config.max_subscriptions_per_conn, 12);
    assert_eq!(config.max_subscriptions_per_conn_total, Some(40));
    assert_eq!(config.publish_quorum_timeout_ms, 2500);
    assert_eq!(config.shard_move_hold_ms, 0);
    assert_eq!(config.shard_move_hold_max, 33);
}

/// **A zero where zero would disable a limit falls back to the default**,
/// while optional totals stay unset.
#[serial]
#[test]
fn zero_admission_and_group_limits_keep_their_defaults() {
    clear_felix_env();
    for key in [
        "FELIX_PREAUTH_MAX_FRAME_BYTES",
        "FELIX_PREAUTH_MAX_STREAMS_PER_CONN",
        "FELIX_MAX_CLIENT_CONNECTIONS",
        "FELIX_GROUP_VISIBILITY_TIMEOUT_MS",
        "FELIX_GROUP_MAX_ATTEMPTS",
        "FELIX_GROUP_MAX_IN_FLIGHT",
        "FELIX_BROKER_PUBLISH_CONN_TOTAL_INFLIGHT_BYTES",
        "FELIX_MAX_SUBSCRIPTIONS_PER_CONN_TOTAL",
        "FELIX_PUBLISH_QUORUM_TIMEOUT_MS",
    ] {
        unsafe {
            env::set_var(key, "0");
        }
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(
        config.preauth_max_frame_bytes,
        DEFAULT_PREAUTH_MAX_FRAME_BYTES
    );
    assert_eq!(
        config.preauth_max_streams_per_conn,
        DEFAULT_PREAUTH_MAX_STREAMS_PER_CONN
    );
    assert_eq!(
        config.max_client_connections,
        DEFAULT_MAX_CLIENT_CONNECTIONS
    );
    assert_eq!(
        config.group_visibility_timeout_ms,
        DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS
    );
    assert_eq!(config.group_max_attempts, DEFAULT_GROUP_MAX_ATTEMPTS);
    assert_eq!(config.group_max_in_flight, DEFAULT_GROUP_MAX_IN_FLIGHT);
    assert_eq!(config.pub_conn_total_inflight_bytes, None);
    assert_eq!(config.max_subscriptions_per_conn_total, None);
    assert_eq!(
        config.publish_quorum_timeout_ms,
        DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS
    );
}
