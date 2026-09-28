//! What `--print-config` renders, and the one thing it must never render.

use super::*;

fn with_membership(token: &str) -> BrokerConfig {
    BrokerConfig {
        controlplane_token: token.to_string(),
        membership: Some(MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "10.0.0.1:5000".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            refresh_token_file: None,
            node_token_file: None,
            region: "us-west-2".to_string(),
            region_bridges: Vec::new(),
        }),
        ..BrokerConfig::default()
    }
}

/// **The credential never appears.**
///
/// `--print-config` exists to be pasted into an issue, so a token that
/// reaches the output has been published. This is the assertion that
/// has to hold even if every other field's rendering changes.
#[test]
fn the_credential_is_never_printed() {
    let rendered =
        serde_yaml_ng::to_string(&with_membership("super-secret-value")).expect("render");
    assert!(
        !rendered.contains("super-secret-value"),
        "the credential reached the output:\n{rendered}",
    );
    assert!(rendered.contains("controlplane_token: <redacted>"));
}

/// Redacted, not omitted: whether a token is set at all is exactly what
/// someone debugging a registration failure needs to see.
#[test]
fn an_absent_credential_says_so_rather_than_vanishing() {
    let rendered = serde_yaml_ng::to_string(&with_membership("")).expect("render");
    assert!(
        rendered.contains("controlplane_token: <unset>"),
        "{rendered}"
    );
}

/// Durations come out in the unit their variables are named for.
/// Serde's default for `Duration` is `{ secs, nanos }`, which is
/// unreadable beside `FELIX_PEER_REQUEST_TIMEOUT_MS`.
#[test]
fn peer_timeouts_are_printed_as_milliseconds() {
    let config = BrokerConfig {
        peer_transport: Some(felix_replication::peer::PeerTransportConfig {
            request_timeout: std::time::Duration::from_millis(2500),
            ..felix_replication::peer::PeerTransportConfig::default()
        }),
        ..BrokerConfig::default()
    };
    let rendered = serde_yaml_ng::to_string(&config).expect("render");
    assert!(rendered.contains("request_timeout: 2500"), "{rendered}");
}

/// `{:?}` is the other way a config reaches a log, and it must redact the
/// same way.
#[test]
fn debug_never_shows_the_credential() {
    let rendered = format!("{:?}", with_membership("super-secret-value"));
    assert!(!rendered.contains("super-secret-value"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
}
