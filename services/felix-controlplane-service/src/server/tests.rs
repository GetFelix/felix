use serial_test::serial;

use super::*;
use crate::config::{
    BootstrapConfig, DEFAULT_READINESS_CACHE_TTL_MS, DEFAULT_READINESS_TIMEOUT_MS,
    NodeLivenessConfig, PostgresConfig, StorageBackend,
};

/// Memory-backed, bootstrap off, every port ephemeral.
fn config() -> ControlPlaneConfig {
    ControlPlaneConfig {
        bind_addr: "127.0.0.1:0".parse().expect("bind"),
        api_tls: None,
        metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
        region_id: "local".to_string(),
        storage: StorageBackend::Memory,
        postgres: None,
        raft: None,
        changes_limit: 10,
        change_retention_max_rows: Some(20),
        oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
        bootstrap: BootstrapConfig {
            enabled: false,
            bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
            token: None,
            previous_token: None,
            tls: None,
        },
        node_liveness: NodeLivenessConfig::default(),
        shard_moves: placement::MovePolicy::default(),
        shutdown_drain_timeout_ms: 25_000,
        shutdown_predrain_ms: 0,
        readiness_timeout_ms: DEFAULT_READINESS_TIMEOUT_MS,
        readiness_cache_ttl_ms: DEFAULT_READINESS_CACHE_TTL_MS,
    }
}

fn with_bootstrap(mut config: ControlPlaneConfig) -> ControlPlaneConfig {
    config.bootstrap.enabled = true;
    config.bootstrap.token = Some("bootstrap-token".to_string());
    config
}

#[tokio::test]
async fn build_state_memory_backend() {
    let (state, _raft) = build_state(config(), Readiness::ready(), &Default::default())
        .await
        .expect("state");
    assert_eq!(state.region.region_id, "local");
    assert!(!state.features.durable_storage);
}

#[tokio::test]
async fn build_state_postgres_requires_config() {
    let config = ControlPlaneConfig {
        storage: StorageBackend::Postgres,
        ..config()
    };
    let err = build_state(config, Readiness::ready(), &Default::default())
        .await
        .err()
        .expect("missing postgres");
    assert!(err.to_string().contains("postgres configuration missing"));
}

#[tokio::test]
async fn build_state_postgres_attempts_connection_when_config_present() {
    let config = with_bootstrap(ControlPlaneConfig {
        storage: StorageBackend::Postgres,
        postgres: Some(PostgresConfig {
            url: "postgres://postgres:postgres@127.0.0.1:1/postgres".to_string(),
            max_connections: 1,
            connect_timeout_ms: 500,
            acquire_timeout_ms: 500,
        }),
        ..config()
    });
    let err = build_state(config, Readiness::ready(), &Default::default())
        .await
        .err()
        .expect("connect should fail");
    let text = err.to_string();
    assert!(text.contains("pool") || text.contains("connect") || text.contains("Connection"));
}

#[tokio::test]
#[serial]
async fn run_with_shutdown_starts_and_stops_without_bootstrap() {
    run(config(), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    })
    .await
    .expect("run should stop cleanly");
}

#[tokio::test]
#[serial]
async fn run_with_shutdown_starts_and_stops_with_bootstrap() {
    run(with_bootstrap(config()), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    })
    .await
    .expect("run should stop cleanly");
}

/// A CA on disk and a certificate for `localhost` it issued.
fn api_pki(dir: &std::path::Path) -> (crate::config::ApiTlsConfig, reqwest::Certificate) {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = params.self_signed(&ca_key).expect("ca");
    let ca = rcgen::Issuer::new(params, ca_key);
    let key = rcgen::KeyPair::generate().expect("key");
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("params")
        .signed_by(&key, &ca)
        .expect("sign");
    let cert_path = dir.join("api.pem");
    let key_path = dir.join("api.key.pem");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, key.serialize_pem()).expect("write key");
    (
        crate::config::ApiTlsConfig {
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
        },
        reqwest::Certificate::from_pem(ca_cert.pem().as_bytes()).expect("ca"),
    )
}

/// With a certificate configured, the API answers over TLS to a client that
/// trusts the issuing CA, and not over plain HTTP.
#[tokio::test]
#[serial]
async fn the_api_serves_the_configured_certificate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (api_tls, ca) = api_pki(dir.path());
    // `run` does not report its port, so take a free one first.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe")
        .local_addr()
        .expect("addr")
        .port();
    let config = ControlPlaneConfig {
        bind_addr: format!("127.0.0.1:{port}").parse().expect("bind"),
        api_tls: Some(api_tls),
        ..config()
    };
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(run(config, async move {
        let _ = stopped.await;
    }));

    let trusting = reqwest::Client::builder()
        .add_root_certificate(ca)
        .build()
        .expect("client");
    let url = format!("https://localhost:{port}/v1/system/live");
    let mut answered = None;
    for _ in 0..100 {
        if let Ok(response) = trusting.get(&url).send().await {
            answered = Some(response.status());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let status = answered.expect("the API never answered over TLS");
    assert!(status.is_success(), "{status}");

    let untrusting = reqwest::Client::new();
    assert!(
        untrusting.get(&url).send().await.is_err(),
        "a client without the CA accepted the certificate"
    );
    let plain = untrusting
        .get(format!("http://localhost:{port}/v1/system/live"))
        .send()
        .await;
    assert!(
        plain.map(|r| !r.status().is_success()).unwrap_or(true),
        "the API answered plain HTTP"
    );

    let _ = stop.send(());
    server.await.expect("join").expect("run");
}

#[tokio::test]
#[serial]
async fn unreadable_api_key_material_fails_startup() {
    let config = ControlPlaneConfig {
        api_tls: Some(crate::config::ApiTlsConfig {
            cert_path: "/nonexistent/api.pem".to_string(),
            key_path: "/nonexistent/api.key.pem".to_string(),
        }),
        ..config()
    };
    let err = run(config, std::future::pending())
        .await
        .expect_err("started without its certificate");
    assert!(
        format!("{err:#}").contains("FELIX_CONTROLPLANE_TLS_CERT"),
        "{err:#}"
    );
}
