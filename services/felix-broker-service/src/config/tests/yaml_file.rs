//! Configuration read from a YAML file layered over the environment.

use super::*;

#[serial]
#[test]
fn from_env_or_yaml_no_file_uses_defaults() {
    clear_felix_env();
    let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
    assert_eq!(config.quic_bind.to_string(), "0.0.0.0:5000");
    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_file_not_found_with_explicit_path_fails() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let nonexistent = tmpdir.path().join("nonexistent.yml");
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", nonexistent.to_str().unwrap());
    }
    let result = BrokerConfig::from_env_or_yaml();
    assert!(result.is_err());
    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_overrides_with_valid_yaml() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(
        &config_path,
        r#"
quic_bind: "127.0.0.1:5555"
metrics_bind: "127.0.0.1:9999"
controlplane_url: "http://test-cp:8443"
controlplane_sync_interval_ms: 5000
ack_on_commit: true
max_frame_bytes: 32000000
event_batch_max_events: 128
"#,
    )
    .unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
    }

    let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
    assert_eq!(config.quic_bind.to_string(), "127.0.0.1:5555");
    assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9999");
    assert_eq!(
        config.controlplane_url,
        Some("http://test-cp:8443".to_string())
    );
    assert_eq!(config.controlplane_sync_interval_ms, 5000);
    assert!(config.ack_on_commit);
    assert_eq!(config.max_frame_bytes, 32000000);
    assert_eq!(config.event_batch_max_events, 128);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_invalid_yaml_fails() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("bad.yml");
    fs::write(&config_path, "this is not: valid: yaml:").unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
    }

    let result = BrokerConfig::from_env_or_yaml();
    assert!(result.is_err());

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_invalid_socket_in_yaml_fails() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(&config_path, "quic_bind: \"not-a-socket\"").unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
    }

    let result = BrokerConfig::from_env_or_yaml();
    assert!(result.is_err());

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_filters_zero_values_in_yaml() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(
        &config_path,
        r#"
pub_conn_recv_window: 0
event_batch_max_events: 0
fanout_batch_size: 0
"#,
    )
    .unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
    }

    let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
    // Should keep env defaults when yaml has 0
    assert_eq!(config.pub_conn_recv_window, None);
    assert_eq!(config.event_batch_max_events, 64);
    assert_eq!(config.fanout_batch_size, 64);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_partial_override() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(
        &config_path,
        r#"
quic_bind: "127.0.0.1:7777"
max_frame_bytes: 8000000
"#,
    )
    .unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        env::set_var("FELIX_BROKER_METRICS_BIND", "127.0.0.1:9090");
    }

    let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
    // YAML override
    assert_eq!(config.quic_bind.to_string(), "127.0.0.1:7777");
    assert_eq!(config.max_frame_bytes, 8000000);
    // Env var
    assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9090");
    // Default
    assert_eq!(config.controlplane_sync_interval_ms, 2000);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_or_yaml_all_window_overrides() {
    clear_felix_env();
    let tmpdir = TempDir::new().unwrap();
    let config_path = tmpdir.path().join("config.yml");
    fs::write(
        &config_path,
        r#"
pub_conn_recv_window: 128000000
pub_stream_recv_window: 32000000
cache_send_window: 128000000
pub_workers_per_conn: 16
pub_queue_depth: 4096
pub_inflight_bytes: 134217728
subscriber_queue_capacity: 96
subscriber_queue_policy: block
subscriber_single_writer_per_conn: false
"#,
    )
    .unwrap();
    unsafe {
        env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
    }

    let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
    assert_eq!(config.pub_recv_windows(), (128000000, 32000000));
    assert_eq!(config.cache_send_window, 128000000);
    assert_eq!(config.pub_workers_per_conn, 16);
    assert_eq!(config.pub_queue_depth, 4096);
    assert_eq!(config.pub_inflight_bytes, 134217728);
    assert_eq!(config.subscriber_queue_capacity, 96);
    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::Block);
    assert!(!config.subscriber_single_writer_per_conn);

    clear_felix_env();
}
