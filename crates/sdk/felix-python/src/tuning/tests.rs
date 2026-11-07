use std::time::Duration;

use felix_client::{ClientConfig, ClientSubQueuePolicy, PublishSharding};

use super::{ClientTuning, apply_options, at_least_one, parse_queue_policy, parse_sharding};

fn config() -> ClientConfig {
    ClientConfig::optimized_defaults(crate::tls::client_config(None, None, None, false).unwrap())
}

#[test]
fn a_count_must_be_at_least_one() {
    assert!(at_least_one("n", Some(0_usize)).is_err());
    assert_eq!(at_least_one("n", Some(3_usize)).unwrap(), Some(3));
    assert_eq!(at_least_one::<u64>("n", None).unwrap(), None);
}

#[test]
fn names_parse_and_unknown_ones_are_refused() {
    assert_eq!(parse_sharding("round_robin").unwrap(), PublishSharding::RoundRobin);
    assert!(parse_sharding("random").is_err());
    assert_eq!(parse_queue_policy("block").unwrap(), ClientSubQueuePolicy::Block);
    assert!(parse_queue_policy("newest").is_err());
}

#[test]
fn only_the_named_fields_change_the_config() {
    let mut config = config();
    let before = config.cache_conn_pool;
    let tuning = ClientTuning {
        publish_conn_pool: Some(7),
        client_sub_queue_policy: Some(ClientSubQueuePolicy::Block),
        broker_sub_queue_capacity: Some(9),
        finish_timeout: Some(Duration::from_secs(3)),
        ..ClientTuning::default()
    };
    apply_options(&mut config, Some(true), None, Some(false), Some(&tuning));
    assert_eq!(config.publish_conn_pool, 7);
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.broker_sub_queue_capacity, Some(9));
    assert_eq!(config.finish_timeout, Duration::from_secs(3));
    assert_eq!(config.cache_conn_pool, before);
    assert!(config.publishers);
    assert!(!config.timestamps);
    assert!(!config.record_headers);
}
