use anyhow::Result;

use super::*;

#[test]
fn runtime_config_is_per_client_and_order_independent() -> Result<()> {
    let quinn = quinn::ClientConfig::try_with_platform_verifier()?;
    let mut block = ClientConfig::optimized_defaults(quinn.clone());
    block.client_sub_queue_policy = ClientSubQueuePolicy::Block;
    block.client_sub_queue_capacity = 11;
    block.event_router_max_pending = 13;
    block.max_frame_bytes = 17;
    block.bench_embed_ts = true;

    let mut drop_new = ClientConfig::optimized_defaults(quinn);
    drop_new.client_sub_queue_policy = ClientSubQueuePolicy::DropNew;
    drop_new.client_sub_queue_capacity = 19;
    drop_new.event_router_max_pending = 23;
    drop_new.max_frame_bytes = 29;

    for (first, second) in [(&block, &drop_new), (&drop_new, &block)] {
        let first = first.runtime_config();
        let second = second.runtime_config();
        assert_ne!(
            first.client_sub_queue_policy,
            second.client_sub_queue_policy
        );
        assert_ne!(
            first.client_sub_queue_capacity,
            second.client_sub_queue_capacity
        );
        assert_ne!(
            first.event_router_max_pending,
            second.event_router_max_pending
        );
        assert_ne!(first.max_frame_bytes, second.max_frame_bytes);
        assert_ne!(first.bench_embed_ts, second.bench_embed_ts);
    }
    Ok(())
}

#[allow(deprecated)]
#[test]
fn config_optimized_defaults() {
    use crate::config::*;
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);

    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(
        config.publish_streams_per_conn,
        DEFAULT_PUB_STREAMS_PER_CONN
    );
    assert_eq!(config.publish_chunk_bytes, DEFAULT_PUBLISH_CHUNK_BYTES);
    assert_eq!(config.publish_queue_depth, DEFAULT_PUBLISH_QUEUE_DEPTH);
    assert_eq!(
        config.publish_inflight_bytes,
        DEFAULT_PUBLISH_INFLIGHT_BYTES
    );
    assert_eq!(config.cache_conn_pool, DEFAULT_CACHE_CONN_POOL);
    assert_eq!(
        config.cache_streams_per_conn,
        DEFAULT_CACHE_STREAMS_PER_CONN
    );
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
    assert_eq!(
        config.event_conn_recv_window,
        DEFAULT_EVENT_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.event_stream_recv_window,
        DEFAULT_EVENT_STREAM_RECV_WINDOW
    );
    assert_eq!(config.event_send_window, DEFAULT_EVENT_SEND_WINDOW);
    assert_eq!(
        config.cache_conn_recv_window,
        DEFAULT_CACHE_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.cache_stream_recv_window,
        DEFAULT_CACHE_STREAM_RECV_WINDOW
    );
    assert_eq!(config.cache_send_window, DEFAULT_CACHE_SEND_WINDOW);
    assert_eq!(
        config.event_router_max_pending,
        DEFAULT_EVENT_ROUTER_MAX_PENDING
    );
    assert_eq!(
        config.client_sub_queue_capacity,
        DEFAULT_CLIENT_SUB_QUEUE_CAPACITY
    );
    assert_eq!(
        config.client_sub_queue_policy,
        ClientSubQueuePolicy::DropNew
    );
    assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    assert!(!config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_from_env_variables() {
    use crate::config::*;

    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "2");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "3");
        std::env::set_var("FELIX_PUBLISH_CHUNK_BYTES", "32768");
        std::env::set_var("FELIX_PUBLISH_QUEUE_DEPTH", "32");
        std::env::set_var("FELIX_PUBLISH_INFLIGHT_BYTES", "2097152");
        std::env::set_var("FELIX_PUBLISH_SHARDING", "rr");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "4");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "5");
        std::env::set_var("FELIX_EVENT_CONN_POOL", "6");
        std::env::set_var("FELIX_EVENT_CONN_RECV_WINDOW", "1024");
        std::env::set_var("FELIX_EVENT_STREAM_RECV_WINDOW", "2048");
        std::env::set_var("FELIX_EVENT_SEND_WINDOW", "4096");
        std::env::set_var("FELIX_CACHE_CONN_RECV_WINDOW", "8192");
        std::env::set_var("FELIX_CACHE_STREAM_RECV_WINDOW", "16384");
        std::env::set_var("FELIX_CACHE_SEND_WINDOW", "32768");
        std::env::set_var("FELIX_EVENT_ROUTER_MAX_PENDING", "1000");
        std::env::set_var("FELIX_CLIENT_SUB_QUEUE_CAPACITY", "2048");
        std::env::set_var("FELIX_CLIENT_SUB_QUEUE_POLICY", "block");
        std::env::set_var("FELIX_MAX_FRAME_BYTES", "8388608");
        std::env::set_var("FELIX_BENCH_EMBED_TS", "true");
    }

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");

    assert_eq!(config.publish_conn_pool, 2);
    assert_eq!(config.publish_streams_per_conn, 3);
    assert_eq!(config.publish_chunk_bytes, 32768);
    assert_eq!(config.publish_queue_depth, 32);
    assert_eq!(config.publish_inflight_bytes, 2097152);
    assert_eq!(config.cache_conn_pool, 4);
    assert_eq!(config.cache_streams_per_conn, 5);
    assert_eq!(config.event_conn_pool, 6);
    assert_eq!(config.event_conn_recv_window, 1024);
    assert_eq!(config.event_stream_recv_window, 2048);
    assert_eq!(config.event_send_window, 4096);
    assert_eq!(config.cache_conn_recv_window, 8192);
    assert_eq!(config.cache_stream_recv_window, 16384);
    assert_eq!(config.cache_send_window, 32768);
    assert_eq!(config.event_router_max_pending, 1000);
    assert_eq!(config.client_sub_queue_capacity, 2048);
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.max_frame_bytes, 8388608);
    assert!(config.bench_embed_ts);

    // Clean up
    unsafe {
        std::env::remove_var("FELIX_PUB_CONN_POOL");
        std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_PUBLISH_CHUNK_BYTES");
        std::env::remove_var("FELIX_PUBLISH_QUEUE_DEPTH");
        std::env::remove_var("FELIX_PUBLISH_INFLIGHT_BYTES");
        std::env::remove_var("FELIX_PUBLISH_SHARDING");
        std::env::remove_var("FELIX_CACHE_CONN_POOL");
        std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_EVENT_CONN_POOL");
        std::env::remove_var("FELIX_EVENT_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_SEND_WINDOW");
        std::env::remove_var("FELIX_CACHE_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_SEND_WINDOW");
        std::env::remove_var("FELIX_EVENT_ROUTER_MAX_PENDING");
        std::env::remove_var("FELIX_CLIENT_SUB_QUEUE_CAPACITY");
        std::env::remove_var("FELIX_CLIENT_SUB_QUEUE_POLICY");
        std::env::remove_var("FELIX_MAX_FRAME_BYTES");
        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
fn config_from_yaml_file() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let yaml = r#"
publish_conn_pool: 10
publish_streams_per_conn: 20
publish_chunk_bytes: 65536
publish_queue_depth: 48
publish_inflight_bytes: 3145728
publish_sharding: "hash_stream"
cache_conn_pool: 30
cache_streams_per_conn: 40
event_conn_pool: 50
event_conn_recv_window: 10240
event_stream_recv_window: 20480
event_send_window: 40960
cache_conn_recv_window: 81920
cache_stream_recv_window: 163840
cache_send_window: 327680
event_router_max_pending: 2000
client_sub_queue_capacity: 3072
client_sub_queue_policy: drop_old
max_frame_bytes: 16777216
bench_embed_ts: true
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    assert_eq!(config.publish_conn_pool, 10);
    assert_eq!(config.publish_streams_per_conn, 20);
    assert_eq!(config.publish_chunk_bytes, 65536);
    assert_eq!(config.publish_queue_depth, 48);
    assert_eq!(config.publish_inflight_bytes, 3145728);
    assert_eq!(config.cache_conn_pool, 30);
    assert_eq!(config.cache_streams_per_conn, 40);
    assert_eq!(config.event_conn_pool, 50);
    assert_eq!(config.event_conn_recv_window, 10240);
    assert_eq!(config.event_stream_recv_window, 20480);
    assert_eq!(config.event_send_window, 40960);
    assert_eq!(config.cache_conn_recv_window, 81920);
    assert_eq!(config.cache_stream_recv_window, 163840);
    assert_eq!(config.cache_send_window, 327680);
    assert_eq!(config.event_router_max_pending, 2000);
    assert_eq!(config.client_sub_queue_capacity, 3072);
    assert_eq!(
        config.client_sub_queue_policy,
        ClientSubQueuePolicy::DropOld
    );
    assert_eq!(config.max_frame_bytes, 16777216);
    assert!(config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_yaml_overrides_ignore_zero_values() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    unsafe {
        std::env::remove_var("FELIX_PUB_CONN_POOL");
        std::env::remove_var("FELIX_CACHE_CONN_POOL");
        std::env::remove_var("FELIX_EVENT_CONN_POOL");
    }

    let yaml = r#"
publish_conn_pool: 0
cache_conn_pool: 5
event_conn_pool: 0
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    // Zero values should be ignored, defaults used
    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(config.cache_conn_pool, 5);
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
}

#[allow(deprecated)]
#[test]
fn config_invalid_yaml_file_returns_error() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let invalid_yaml = r#"
publish_conn_pool: [invalid
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(invalid_yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some(path));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_nonexistent_file_returns_error() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some("/nonexistent/path/config.yaml"));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_transport_configs() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.event_conn_recv_window = 12345;
    config.event_stream_recv_window = 23456;
    config.event_send_window = 34567;
    config.cache_conn_recv_window = 45678;
    config.cache_stream_recv_window = 56789;
    config.cache_send_window = 67890;

    let base_transport = TransportConfig::default();

    let event_transport = event_transport_config(base_transport.clone(), &config);
    assert_eq!(event_transport.receive_window, 12345);
    assert_eq!(event_transport.stream_receive_window, 23456);
    assert_eq!(event_transport.send_window, 34567);

    let cache_transport = cache_transport_config(base_transport, &config);
    assert_eq!(cache_transport.receive_window, 45678);
    assert_eq!(cache_transport.stream_receive_window, 56789);
    assert_eq!(cache_transport.send_window, 67890);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_bool_parsing() {
    unsafe {
        // Test various true values
        for val in &["1", "true", "TRUE", "yes", "YES"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(config.bench_embed_ts, "Failed for value: {}", val);
        }

        // Test false values
        for val in &["0", "false", "FALSE", "no", "NO", "random"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(!config.bench_embed_ts, "Failed for value: {}", val);
        }

        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_sharding_variants() {
    use crate::publish::PublishSharding;

    unsafe {
        // Test round robin
        std::env::set_var("FELIX_PUB_SHARDING", "rr");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::RoundRobin
        ));

        // Test hash_stream
        std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        // Test invalid value (should default to HashStream)
        std::env::set_var("FELIX_PUB_SHARDING", "invalid");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        std::env::remove_var("FELIX_PUB_SHARDING");
    }
}

/// `0` turns shard streams off, so unlike the pool sizes it is not ignored.
#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn zero_shard_streams_from_env_is_honoured() {
    let quinn = quinn::ClientConfig::with_platform_verifier();
    unsafe {
        std::env::set_var("FELIX_PUB_SHARD_STREAMS", "0");
    }
    let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
    unsafe {
        std::env::remove_var("FELIX_PUB_SHARD_STREAMS");
    }
    assert_eq!(config.publish_shard_streams, 0);
}
