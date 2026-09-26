//! The refresh loop against a control plane that mints short-lived tokens.
//!
//! The acceptance question for #373 is whether a broker stays authenticated
//! *past* the lifetime of the token it started with. A stub control plane
//! stands in for the real one because what is under test is the broker's half:
//! that it refreshes in time, rotates the stored token, keeps serving when a
//! refresh fails, and never adopts a token it could not write the other half
//! of.
//!
//! Run with `cargo test -p felix-broker-service --test credential_refresh`.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::routing::post;
use base64::Engine;
use felix_broker_service::cluster::credential::{NodeCredential, refresh};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const TENANT: &str = "acme";

/// A Felix-shaped access token. Only the payload matters: the broker reads it
/// to choose a URL and a wake-up time, and never verifies it.
fn access_token(expires_in: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    let encode = |value: serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
    };
    format!(
        "{}.{}.{}",
        encode(json!({"alg": "EdDSA", "typ": "JWT"})),
        encode(json!({"tid": TENANT, "exp": now + expires_in, "sub": "node-1"})),
        "signature",
    )
}

#[derive(Clone)]
struct StubControlPlane {
    /// How many refreshes have been served.
    refreshes: Arc<AtomicUsize>,
    /// Refresh tokens presented, in order — so a test can prove rotation.
    presented: Arc<std::sync::Mutex<Vec<String>>>,
    /// Refuse this many requests before serving any.
    refuse_first: Arc<AtomicU64>,
    /// Lifetime of each minted access token.
    ttl_secs: i64,
}

async fn refresh_handler(
    State(state): State<StubControlPlane>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let presented = body["refresh_token"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    // The node credential is for the control plane's API, which refuses a
    // broker-audience token; asking for the default would get one.
    if body["audience"] != "felix-controlplane" {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if state.refuse_first.load(Ordering::Acquire) > 0 {
        state.refuse_first.fetch_sub(1, Ordering::AcqRel);
        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    state.presented.lock().expect("lock").push(presented);
    let round = state.refreshes.fetch_add(1, Ordering::AcqRel) + 1;
    Ok(Json(json!({
        "felix_token": access_token(state.ttl_secs),
        "expires_in": state.ttl_secs,
        "token_type": "Bearer",
        // Rotated, like the real endpoint: each answer hands back a different
        // refresh token, and the one presented is spent.
        "refresh_token": format!("refresh-{round}"),
        "refresh_expires_in": 3600,
    })))
}

async fn spawn_control_plane(state: StubControlPlane) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = axum::Router::new()
        .route(
            &format!("/v1/tenants/{TENANT}/token/refresh"),
            post(refresh_handler),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    (addr, handle)
}

struct Harness {
    credential: NodeCredential,
    state: StubControlPlane,
    token_file: std::path::PathBuf,
    shutdown: CancellationToken,
    refresh_loop: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
    _server: tokio::task::JoinHandle<()>,
}

/// Start the loop against a stub, with an access token that expires in
/// `ttl_secs` and a refresh token on disk.
async fn start(ttl_secs: i64, refuse_first: u64) -> Harness {
    let dir = tempfile::tempdir().expect("tempdir");
    let token_file = dir.path().join("refresh.token");
    std::fs::write(&token_file, "refresh-0\n").expect("seed the refresh token");

    let state = StubControlPlane {
        refreshes: Arc::new(AtomicUsize::new(0)),
        presented: Arc::new(std::sync::Mutex::new(Vec::new())),
        refuse_first: Arc::new(AtomicU64::new(refuse_first)),
        ttl_secs,
    };
    let (addr, server) = spawn_control_plane(state.clone()).await;

    let credential = NodeCredential::new(access_token(ttl_secs));
    let shutdown = CancellationToken::new();
    let refresh_loop = tokio::spawn(refresh::run(
        refresh::RefreshConfig {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            credential: credential.clone(),
            refresh_token_file: token_file.clone(),
        },
        shutdown.clone(),
    ));

    Harness {
        credential,
        state,
        token_file,
        shutdown,
        refresh_loop,
        _dir: dir,
        _server: server,
    }
}

impl Harness {
    /// Stop the loop and wait for it to exit.
    ///
    /// A refresh already under way runs to the end, including writing the
    /// replacement token, so anything read before the loop exits can see the
    /// stub's answer without the file that goes with it.
    async fn stop(&mut self) {
        self.shutdown.cancel();
        (&mut self.refresh_loop)
            .await
            .expect("refresh loop panicked");
    }
}

/// Wait until `refreshes` have been served, or give up.
async fn wait_for_refreshes(harness: &Harness, want: usize, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if harness.state.refreshes.load(Ordering::Acquire) >= want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// The acceptance criterion: still authenticated past the first token's life.
///
/// A 9-second TTL means the loop refreshes about every 6 seconds; four
/// refreshes is well past the point where a broker holding its startup token
/// would have been answering 401 for a while.
#[tokio::test]
async fn a_broker_stays_authenticated_past_one_token_lifetime() {
    let mut harness = start(9, 0).await;
    let first = harness.credential.bearer();

    assert!(
        wait_for_refreshes(&harness, 4, Duration::from_secs(45)).await,
        "only {} refreshes; a broker on a 9s token would have fallen out of \
         the cluster by now",
        harness.state.refreshes.load(Ordering::Acquire),
    );

    // The credential in everyone's hands is not the one it started with.
    assert_ne!(
        *harness.credential.bearer(),
        *first,
        "the loop ran but the credential every caller reads was never swapped",
    );
    harness.stop().await;
}

/// Each refresh presents the token the previous one handed back.
#[tokio::test]
async fn the_stored_refresh_token_rotates() {
    let mut harness = start(9, 0).await;
    assert!(wait_for_refreshes(&harness, 3, Duration::from_secs(45)).await);
    harness.stop().await;

    let presented = harness.state.presented.lock().expect("lock").clone();
    assert_eq!(
        &presented[..3],
        &["refresh-0", "refresh-1", "refresh-2"],
        "a refresh token was presented twice, or the replacement was not used; \
         against the real control plane the second presentation is a replay and \
         revokes the whole chain",
    );

    // And the file holds the newest one, so a restart resumes the chain rather
    // than replaying a spent token.
    let stored = std::fs::read_to_string(&harness.token_file).expect("read");
    assert_eq!(
        stored.trim(),
        presented
            .last()
            .map(|token| {
                let round: usize = token.trim_start_matches("refresh-").parse().expect("round");
                format!("refresh-{}", round + 1)
            })
            .expect("a presented token")
            .as_str(),
        "the file does not hold the replacement, so a restart would present a \
         spent token",
    );
}

/// A refusal is retried, not fatal, and the credential survives it.
#[tokio::test]
async fn a_failed_refresh_is_retried_rather_than_fatal() {
    // Three refusals first: enough to exercise backoff without outliving the
    // token, which is the balance the loop has to strike.
    let mut harness = start(12, 3).await;

    assert!(
        wait_for_refreshes(&harness, 1, Duration::from_secs(60)).await,
        "the loop gave up after refusals instead of retrying",
    );
    // Still holding a usable credential throughout — a broker must not be taken
    // down by a control plane that is briefly unwell.
    assert!(!harness.credential.bearer().is_empty());
    harness.stop().await;
}

/// A credential that is not a Felix token ends the loop, rather than looping
/// against a URL it cannot build.
#[tokio::test]
async fn an_unreadable_credential_stops_the_loop_instead_of_spinning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let token_file = dir.path().join("refresh.token");
    std::fs::write(&token_file, "refresh-0\n").expect("seed");

    let state = StubControlPlane {
        refreshes: Arc::new(AtomicUsize::new(0)),
        presented: Arc::new(std::sync::Mutex::new(Vec::new())),
        refuse_first: Arc::new(AtomicU64::new(0)),
        ttl_secs: 60,
    };
    let (addr, _server) = spawn_control_plane(state.clone()).await;
    let shutdown = CancellationToken::new();

    // An opaque token: no claims, so no tenant to call and no expiry to
    // schedule against.
    let task = tokio::spawn(refresh::run(
        refresh::RefreshConfig {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            credential: NodeCredential::new("an-opaque-token"),
            refresh_token_file: token_file,
        },
        shutdown.clone(),
    ));

    // It returns on its own, without the shutdown token being cancelled.
    let ended = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(
        ended.is_ok(),
        "the loop kept running against a credential it cannot refresh",
    );
    assert_eq!(state.refreshes.load(Ordering::Acquire), 0);
}

/// Cancelling the shutdown token ends the loop.
#[tokio::test]
async fn shutdown_ends_the_loop() {
    let mut harness = start(600, 0).await;
    // The loop selects on the token before every sleep, so a cancelled one
    // returns at once rather than waiting out the refresh delay, which at a
    // 600s TTL would be minutes.
    tokio::time::timeout(Duration::from_secs(5), harness.stop())
        .await
        .expect("the loop kept waiting after shutdown was cancelled");
    assert_eq!(harness.state.refreshes.load(Ordering::Acquire), 0);
}
