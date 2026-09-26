use std::time::Duration;

use super::*;

fn config() -> BrokerConfig {
    BrokerConfig {
        preauth_max_frame_bytes: 4 * 1024,
        preauth_max_streams_per_conn: 2,
        ..BrokerConfig::default()
    }
}

#[test]
fn frame_cap_is_small_until_the_stream_authenticates() {
    let config = config();
    let gate = PreAuthGate::new(&config);
    assert_eq!(gate.frame_cap(false, config.max_frame_bytes), 4 * 1024);
    assert_eq!(
        gate.frame_cap(true, config.max_frame_bytes),
        config.max_frame_bytes
    );
}

#[test]
fn frame_cap_never_exceeds_the_general_cap() {
    let config = BrokerConfig {
        preauth_max_frame_bytes: 1024 * 1024,
        max_frame_bytes: 8 * 1024,
        ..BrokerConfig::default()
    };
    let gate = PreAuthGate::new(&config);
    assert_eq!(gate.frame_cap(false, config.max_frame_bytes), 8 * 1024);
}

#[tokio::test]
async fn streams_past_the_limit_wait_for_a_slot() {
    let gate = PreAuthGate::new(&config());
    let first = gate.admit_stream().await;
    let _second = gate.admit_stream().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), gate.admit_stream())
            .await
            .is_err(),
        "a third unauthenticated stream must wait"
    );
    drop(first);
    let _third = tokio::time::timeout(Duration::from_secs(1), gate.admit_stream())
        .await
        .expect("a slot frees when a stream authenticates or ends");
}

#[tokio::test]
async fn authenticated_resolves_once_marked_even_if_marked_first() {
    let gate = PreAuthGate::new(&config());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), gate.authenticated())
            .await
            .is_err()
    );
    gate.mark_authenticated();
    tokio::time::timeout(Duration::from_secs(1), gate.authenticated())
        .await
        .expect("already authenticated");
}

#[test]
fn auth_timeout_zero_disables_the_deadline() {
    let config = BrokerConfig {
        auth_timeout_ms: 0,
        ..BrokerConfig::default()
    };
    assert_eq!(auth_timeout(&config), None);
    let config = BrokerConfig {
        auth_timeout_ms: 1500,
        ..BrokerConfig::default()
    };
    assert_eq!(auth_timeout(&config), Some(Duration::from_millis(1500)));
}

#[test]
fn connection_limit_refuses_at_the_cap_and_frees_on_drop() {
    let limit = ConnectionLimit::new(2);
    let a = limit.try_admit().expect("first");
    let _b = limit.try_admit().expect("second");
    assert!(limit.try_admit().is_none());
    drop(a);
    assert!(limit.try_admit().is_some());
}

#[test]
fn connection_limit_is_shared_between_clones() {
    let limit = ConnectionLimit::new(1);
    let other = limit.clone();
    let _held = limit.try_admit().expect("first");
    assert!(other.try_admit().is_none());
}
