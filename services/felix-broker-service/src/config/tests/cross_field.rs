//! Pairs that are each fine alone and wrong together.
//!
//! The defaults are all correctly ordered, so these only fire for someone
//! who inverted one — which is exactly the case that produced behaviour
//! nobody configured and no error to explain it.

use base64::Engine;

use super::*;

#[test]
fn the_defaults_are_valid() {
    // If this ever fails, a default was changed into a contradiction
    // and every broker would refuse to start.
    BrokerConfig::default().validate().expect("defaults");
}

/// The pair this module exists for, in its newest form: each setting
/// is fine alone, and together they put four listeners on one thread.
#[test]
fn an_io_runtime_pool_too_small_for_the_listeners_is_refused() {
    let config = BrokerConfig {
        quic_listeners: 4,
        io_runtime_threads: Some(1),
        ..BrokerConfig::default()
    };
    let err = config
        .validate()
        .expect_err("1 runtime cannot serve 4 listeners");
    let message = format!("{err:#}");
    assert!(message.contains("FELIX_IO_RUNTIME_THREADS"), "{message}");
    assert!(message.contains("FELIX_QUIC_LISTENERS"), "{message}");
    // Says what to use instead, rather than only what is wrong.
    assert!(message.contains("Use 5"), "{message}");
}

/// Zero is the documented way to turn the pool off and put drivers back
/// on the app runtime, where tokio spreads them. Not a conflict.
#[test]
fn turning_the_pool_off_is_not_a_conflict() {
    BrokerConfig {
        quic_listeners: 8,
        io_runtime_threads: Some(0),
        ..BrokerConfig::default()
    }
    .validate()
    .expect("0 disables the pool");
}

/// Unset is the ordinary case: nothing to disagree with, because the
/// pool is derived from the listener count.
#[test]
fn an_underived_pool_is_not_a_conflict() {
    BrokerConfig {
        quic_listeners: 8,
        io_runtime_threads: None,
        ..BrokerConfig::default()
    }
    .validate()
    .expect("unset derives");
}

/// A pool sized exactly right is accepted, so the error names a number
/// that actually works.
#[test]
fn a_pool_sized_for_the_listeners_is_accepted() {
    let config = BrokerConfig {
        quic_listeners: 4,
        io_runtime_threads: Some(5),
        ..BrokerConfig::default()
    };
    assert_eq!(config.server_endpoints(), 4);
    config.validate().expect("5 runtimes serve 4 listeners");
}

#[test]
fn a_batch_larger_than_a_frame_is_refused() {
    let config = BrokerConfig {
        max_frame_bytes: 64 * 1024,
        event_batch_max_bytes: 128 * 1024,
        ..BrokerConfig::default()
    };
    let err = config
        .validate()
        .expect_err("a batch cannot exceed a frame");
    let message = format!("{err:#}");
    assert!(message.contains("event_batch_max_bytes"), "{message}");
    assert!(message.contains("max_frame_bytes"), "{message}");
}

#[test]
fn a_per_connection_limit_above_the_broker_wide_one_is_refused() {
    let config = BrokerConfig {
        pub_inflight_bytes: 1024,
        pub_conn_inflight_bytes: 2048,
        ..BrokerConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn a_stream_window_above_the_connection_window_is_refused() {
    let config = BrokerConfig {
        cache_conn_recv_window: 1024,
        cache_stream_recv_window: 2048,
        ..BrokerConfig::default()
    };
    assert!(config.validate().is_err());
}

fn expiring_token(exp: i64) -> String {
    let encode = |value: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
    };
    format!(
        "{}.{}.{}",
        encode(&serde_json::json!({"alg": "EdDSA", "typ": "JWT"})),
        encode(&serde_json::json!({"tid": "acme", "exp": exp, "sub": "node-1"})),
        "not-a-real-signature",
    )
}

fn joining_with(token: &str, refresh: bool, token_file: bool) -> BrokerConfig {
    BrokerConfig {
        controlplane_token: token.to_string(),
        membership: Some(MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "10.0.0.1:5000".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            refresh_token_file: refresh.then(|| "/run/felix/refresh".into()),
            node_token_file: token_file.then(|| "/run/felix/node.token".into()),
            region: "us-west-2".to_string(),
            zone: None,
            region_bridges: Vec::new(),
            features: Default::default(),
        }),
        ..BrokerConfig::default()
    }
}

/// The outage this check exists to move forward in time.
///
/// The heartbeat carries this token and the heartbeat is the lease
/// renewal, so an expiring credential nothing can renew is a broker
/// that stops serving its shards at a time already determined. Saying
/// so at startup costs a failed rollout; not saying so costs an
/// incident an hour later with no change to blame.
#[test]
fn an_expiring_credential_with_no_way_to_renew_it_is_refused() {
    let config = joining_with(&expiring_token(1_700_000_900), false, false);
    let err = config.validate().expect_err("should refuse");
    let message = err.to_string();
    assert!(message.contains("nothing can renew it"), "{message}");
    // Names both ways out, because the error is the only place an
    // operator meets this.
    assert!(
        message.contains("FELIX_NODE_REFRESH_TOKEN_FILE"),
        "{message}"
    );
    assert!(message.contains("FELIX_NODE_TOKEN_FILE"), "{message}");
}

#[test]
fn a_refresh_file_makes_an_expiring_credential_fine() {
    joining_with(&expiring_token(1_700_000_900), true, false)
        .validate()
        .expect("refresh renews it");
}

/// A file is a seam something else can write; a value is not. That is
/// the whole distinction the check turns on, so it is asserted rather
/// than implied.
#[test]
fn a_token_file_makes_an_expiring_credential_fine() {
    joining_with(&expiring_token(1_700_000_900), false, true)
        .validate()
        .expect("an external rotator can renew it");
}

#[test]
fn a_credential_that_never_expires_is_left_alone() {
    // Not a Felix token, so there is no `exp` to act on. Guessing would
    // refuse a deployment whose credential this broker cannot read and
    // has no business judging.
    joining_with("opaque-token", false, false)
        .validate()
        .expect("nothing to schedule against");
}

#[test]
fn a_broker_not_joining_a_cluster_is_left_alone() {
    // No membership means no heartbeat and no lease, so an expiring
    // credential costs it nothing.
    let config = BrokerConfig {
        controlplane_token: expiring_token(1_700_000_900),
        membership: None,
        ..BrokerConfig::default()
    };
    config.validate().expect("no cluster to fall out of");
}

/// Equal is fine everywhere. The limits bound each other; they do not
/// have to differ, and refusing equality would fail a configuration
/// that behaves exactly as written.
#[test]
fn equal_limits_are_allowed() {
    let config = BrokerConfig {
        max_frame_bytes: 64 * 1024,
        event_batch_max_bytes: 64 * 1024,
        pub_inflight_bytes: 4096,
        pub_conn_inflight_bytes: 4096,
        cache_conn_recv_window: 8192,
        cache_stream_recv_window: 8192,
        ..BrokerConfig::default()
    };
    config.validate().expect("equal limits");
}
