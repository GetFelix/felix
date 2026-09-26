//! The bootstrap listener's mTLS gate.
//!
//! What is under test is the refusal, not the ceremony: a client without a
//! certificate signed by the configured CA must be stopped at the handshake,
//! before any request — token included — reaches the router.
use std::sync::Arc;

use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_bootstrap_router};
use felix_controlplane_service::config::BootstrapTlsConfig;
use felix_controlplane_service::store::{ControlPlaneAuthStore, ControlPlaneStore, StoreConfig};
use tokio_util::sync::CancellationToken;

struct TestPki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

/// A CA, a server certificate for `localhost`, and a client certificate, all
/// freshly generated so nothing in the repo looks like a real credential.
fn generate_pki() -> TestPki {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let ca_pem = ca_cert.pem();
    let ca = rcgen::Issuer::new(ca_params, ca_key);

    let server_key = rcgen::KeyPair::generate().expect("server key");
    let server_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    let server_cert = server_params
        .signed_by(&server_key, &ca)
        .expect("server cert");

    let client_key = rcgen::KeyPair::generate().expect("client key");
    let client_params =
        rcgen::CertificateParams::new(vec!["bootstrap-client".to_string()]).expect("client params");
    let client_cert = client_params
        .signed_by(&client_key, &ca)
        .expect("client cert");

    TestPki {
        ca_pem,
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

fn state_with_token() -> AppState {
    let store = Arc::new(
        felix_controlplane_service::store::memory::InMemoryStore::new(StoreConfig {
            changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(
                felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
            ),
        }),
    );
    let state_store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = store.clone();
    AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: state_store,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_tokens: vec!["secret".to_string()],
        node_liveness: Default::default(),
        readiness: Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    }
}

/// Serve the bootstrap router over mTLS on an ephemeral port; returns the
/// port, the shutdown token, and the server task.
async fn serve(
    pki: &TestPki,
    dir: &std::path::Path,
) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server.key");
    let ca_path = dir.join("client-ca.pem");
    std::fs::write(&cert_path, &pki.server_cert_pem).expect("write cert");
    std::fs::write(&key_path, &pki.server_key_pem).expect("write key");
    std::fs::write(&ca_path, &pki.ca_pem).expect("write ca");

    let tls = felix_controlplane_service::server::tls::load_server_config(&BootstrapTlsConfig {
        cert_path: cert_path.to_string_lossy().into_owned(),
        key_path: key_path.to_string_lossy().into_owned(),
        client_ca_path: ca_path.to_string_lossy().into_owned(),
    })
    .expect("tls config");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let router = build_bootstrap_router(state_with_token());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(felix_controlplane_service::server::tls::serve_tls(
        "bootstrap",
        listener,
        router,
        tls,
        shutdown.clone(),
    ));
    (port, shutdown, task)
}

fn initialize_body() -> serde_json::Value {
    serde_json::json!({
        "display_name": "Tenant One",
        "idp_issuers": [],
        "initial_admin_principals": ["p:admin"]
    })
}

#[tokio::test]
async fn a_client_certificate_from_the_configured_ca_is_required() {
    let pki = generate_pki();
    let dir = tempfile::tempdir().expect("tempdir");
    let (port, shutdown, task) = serve(&pki, dir.path()).await;
    let url = format!("https://localhost:{port}/internal/bootstrap/tenants/t1/initialize");
    let ca = reqwest::Certificate::from_pem(pki.ca_pem.as_bytes()).expect("ca");

    // With the client certificate: the handshake passes and the API answers.
    let identity_pem = format!("{}{}", pki.client_key_pem, pki.client_cert_pem);
    let with_cert = reqwest::Client::builder()
        .use_rustls_tls()
        .add_root_certificate(ca.clone())
        .identity(reqwest::Identity::from_pem(identity_pem.as_bytes()).expect("identity"))
        .build()
        .expect("client");
    let response = with_cert
        .post(&url)
        .header("X-Felix-Bootstrap-Token", "secret")
        .json(&initialize_body())
        .send()
        .await
        .expect("request with client cert");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Without one, the correct token never gets a chance to matter: the
    // request fails at the TLS layer instead of reaching the handler.
    let without_cert = reqwest::Client::builder()
        .use_rustls_tls()
        .add_root_certificate(ca.clone())
        .build()
        .expect("client");
    let refused = without_cert
        .post(&url)
        .header("X-Felix-Bootstrap-Token", "secret")
        .json(&initialize_body())
        .send()
        .await;
    assert!(
        refused.is_err(),
        "a client with no certificate must be refused, got {refused:?}"
    );

    // A certificate from a different CA is exactly as unauthenticated as none.
    let stranger = generate_pki();
    let stranger_pem = format!("{}{}", stranger.client_key_pem, stranger.client_cert_pem);
    let wrong_ca = reqwest::Client::builder()
        .use_rustls_tls()
        .add_root_certificate(ca)
        .identity(reqwest::Identity::from_pem(stranger_pem.as_bytes()).expect("identity"))
        .build()
        .expect("client");
    let refused = wrong_ca
        .post(&url)
        .header("X-Felix-Bootstrap-Token", "secret")
        .json(&initialize_body())
        .send()
        .await;
    assert!(
        refused.is_err(),
        "a certificate from another CA must be refused, got {refused:?}"
    );

    shutdown.cancel();
    let _ = task.await;
}

/// Startup must fail on unreadable material, not come up half-secured.
#[test]
fn missing_key_material_fails_config_load() {
    let err = felix_controlplane_service::server::tls::load_server_config(&BootstrapTlsConfig {
        cert_path: "/nonexistent/cert.pem".to_string(),
        key_path: "/nonexistent/key.pem".to_string(),
        client_ca_path: "/nonexistent/ca.pem".to_string(),
    })
    .expect_err("missing files should fail");
    assert!(err.to_string().contains("bootstrap TLS certificate"));
}
