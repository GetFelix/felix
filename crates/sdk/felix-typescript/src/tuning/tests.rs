use felix_client::{ClientConfig, ClientSubQueuePolicy, PublishSharding};

use super::ClientTuning;

fn none() -> ClientTuning {
    ClientTuning {
        publishers: None,
        timestamps: None,
        record_headers: None,
        client_sub_queue_policy: None,
        client_sub_queue_capacity: None,
        broker_sub_queue_capacity: None,
        publish_conn_pool: None,
        publish_streams_per_conn: None,
        publish_queue_depth: None,
        publish_inflight_bytes: None,
        publish_sharding: None,
        publish_shard_streams: None,
        cache_conn_pool: None,
        cache_streams_per_conn: None,
        event_conn_pool: None,
        cluster_conn_pool: None,
        cluster_streams_per_conn: None,
        event_conn_recv_window: None,
        event_stream_recv_window: None,
        event_send_window: None,
        cache_conn_recv_window: None,
        cache_stream_recv_window: None,
        cache_send_window: None,
        event_router_max_pending: None,
        max_frame_bytes: None,
        ack_on_commit: None,
        connect_timeout: None,
        finish_timeout: None,
    }
}

fn config() -> ClientConfig {
    let quinn = crate::tls::client_config(None, None, None, false).expect("a default TLS config");
    ClientConfig::optimized_defaults(quinn)
}

#[test]
fn nothing_set_changes_nothing() {
    let before = config();
    let mut after = config();
    none().apply(&mut after).unwrap();
    assert_eq!(after.publish_conn_pool, before.publish_conn_pool);
    assert_eq!(after.max_frame_bytes, before.max_frame_bytes);
    assert_eq!(after.publishers, before.publishers);
    assert_eq!(after.record_headers, before.record_headers);
}

#[test]
fn every_kind_of_field_reaches_the_config() {
    let mut config = config();
    ClientTuning {
        publishers: Some(true),
        timestamps: Some(true),
        record_headers: Some(false),
        client_sub_queue_policy: Some("block".into()),
        client_sub_queue_capacity: Some(77.0),
        broker_sub_queue_capacity: Some(9.0),
        publish_sharding: Some("round_robin".into()),
        publish_shard_streams: Some(0.0),
        event_send_window: Some(1_048_576.0),
        max_frame_bytes: Some(2_000_000.0),
        ack_on_commit: Some(true),
        connect_timeout: Some(2500.0),
        ..none()
    }
    .apply(&mut config)
    .unwrap();
    assert!(config.publishers && config.timestamps && config.ack_on_commit);
    assert!(!config.record_headers);
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.client_sub_queue_capacity, 77);
    assert_eq!(config.broker_sub_queue_capacity, Some(9));
    assert_eq!(config.publish_sharding, PublishSharding::RoundRobin);
    assert_eq!(config.publish_shard_streams, 0);
    assert_eq!(config.event_send_window, 1_048_576);
    assert_eq!(config.max_frame_bytes, 2_000_000);
    assert_eq!(config.connect_timeout.as_millis(), 2500);
}

#[test]
fn values_that_do_not_fit_are_refused() {
    for bad in [0.0, -1.0, 1.5, f64::NAN, f64::INFINITY] {
        let mut config = config();
        let tuning = ClientTuning {
            publish_conn_pool: Some(bad),
            ..none()
        };
        assert!(tuning.apply(&mut config).is_err(), "{bad} was accepted");
    }
    let mut config = config();
    let tuning = ClientTuning {
        client_sub_queue_policy: Some("sometimes".into()),
        ..none()
    };
    assert!(tuning.apply(&mut config).is_err());
    let tuning = ClientTuning {
        publish_sharding: Some("random".into()),
        ..none()
    };
    assert!(tuning.apply(&mut config).is_err());
    let tuning = ClientTuning {
        broker_sub_queue_capacity: Some(5_000_000_000.0),
        ..none()
    };
    assert!(tuning.apply(&mut config).is_err());
}
