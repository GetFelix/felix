use std::time::Duration;

use super::*;

/// **A standalone broker adopts a token file something rewrote**, as a
/// cluster member does. Its catalog sync presents this credential, and an
/// exchange token lasts minutes, so reading the file once at startup left a
/// development broker locked out a quarter of an hour in.
#[tokio::test(start_paused = true)]
async fn a_standalone_broker_adopts_a_rewritten_token_file() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("node.token");
    std::fs::write(&path, "first").expect("write");
    let config = BrokerConfig {
        controlplane_url: Some("http://127.0.0.1:1".to_string()),
        node_token_file: Some(path.clone()),
        ..BrokerConfig::default()
    };
    assert!(config.membership.is_none());
    let credential = NodeCredential::new("first".to_string());
    let shutdown = CancellationToken::new();

    keep_credential_current(
        &config,
        &reqwest::Client::new(),
        &Some(credential.clone()),
        &shutdown,
    );
    std::fs::write(&path, "second").expect("rewrite");
    tokio::time::sleep(credential::rotate::POLL_INTERVAL + Duration::from_secs(1)).await;

    assert_eq!(credential.bearer().as_str(), "second");
    shutdown.cancel();
}
