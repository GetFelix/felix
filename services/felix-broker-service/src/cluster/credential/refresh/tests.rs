//! Persistence and backoff — the parts that do not need a control plane.
//!
//! The loop itself is exercised end to end in
//! `tests/credential_refresh.rs`, against a stub control plane that mints
//! short-lived tokens.
use super::*;

#[test]
fn the_replacement_is_written_whole_or_not_at_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("refresh.token");
    std::fs::write(&path, "original\n").expect("seed");

    persist(&path, "replacement").expect("persist");
    assert_eq!(
        std::fs::read_to_string(&path).expect("read").trim(),
        "replacement",
    );
    // The temporary is gone, not left beside the real file for an operator to
    // wonder about.
    assert!(!path.with_extension("tmp").exists());
}

#[test]
fn persisting_leaves_nothing_behind_on_a_bad_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("no-such-dir").join("refresh.token");
    assert!(persist(&path, "replacement").is_err());
    assert!(!path.exists());
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    // Bounded, because the control plane being unreachable for an hour must
    // not mean the next attempt is an hour away — the credential expires long
    // before that.
    let delays: Vec<_> = (0..10).map(backoff).collect();
    assert_eq!(delays[0], Duration::from_secs(1));
    assert!(delays.windows(2).all(|pair| pair[1] >= pair[0]));
    assert!(
        delays.iter().all(|delay| *delay <= Duration::from_secs(60)),
        "backoff exceeded its ceiling: {delays:?}",
    );
}

/// Shutdown cancelled while a refresh is in flight still lets that refresh
/// write its replacement token before the loop exits.
///
/// The control plane rotates the refresh token as soon as it answers, so a
/// loop that gave up mid-request would leave a spent token on disk for the
/// next start to present. The broker's drain waits for this loop to exit on
/// the strength of this.
#[tokio::test]
async fn shutdown_mid_refresh_still_saves_the_rotated_token() {
    use std::sync::Arc;

    use axum::routing::post;
    use base64::Engine;
    use tokio::sync::Notify;

    let requested = Arc::new(Notify::new());
    let app = axum::Router::new().route(
        "/v1/tenants/acme/token/refresh",
        post({
            let requested = Arc::clone(&requested);
            move || async move {
                requested.notify_one();
                // Long enough for the test to cancel while this is outstanding.
                tokio::time::sleep(Duration::from_millis(500)).await;
                axum::Json(serde_json::json!({
                    "felix_token": token_expiring_in(600),
                    "expires_in": 600,
                    "token_type": "Bearer",
                    "refresh_token": "refresh-1",
                    "refresh_expires_in": 3600,
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let token_file = dir.path().join("refresh.token");
    std::fs::write(&token_file, "refresh-0\n").expect("seed");

    let shutdown = CancellationToken::new();
    // Six seconds of life puts the first refresh at the five-second floor.
    let refresh_loop = tokio::spawn(run(
        RefreshConfig {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            credential: NodeCredential::new(token_expiring_in(6)),
            refresh_token_file: token_file.clone(),
        },
        shutdown.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(20), requested.notified())
        .await
        .expect("the loop never asked for a refresh");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), refresh_loop)
        .await
        .expect("the loop did not exit after shutdown")
        .expect("the loop panicked");

    assert_eq!(
        std::fs::read_to_string(&token_file).expect("read").trim(),
        "refresh-1",
        "the loop stopped without saving the token the control plane rotated to",
    );

    fn token_expiring_in(seconds: i64) -> String {
        let encode = |value: serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
        };
        format!(
            "{}.{}.signature",
            encode(serde_json::json!({"alg": "EdDSA", "typ": "JWT"})),
            encode(serde_json::json!({
                "tid": "acme",
                "exp": crate::cluster::credential::now_secs() + seconds,
                "sub": "node-1",
            })),
        )
    }
}

#[test]
fn debug_never_shows_the_tokens() {
    let response = RefreshResponse {
        felix_token: "access-secret".to_string(),
        refresh_token: "refresh-secret".to_string(),
    };
    let rendered = format!("{response:?}");
    assert!(!rendered.contains("access-secret"), "{rendered}");
    assert!(!rendered.contains("refresh-secret"), "{rendered}");
}
