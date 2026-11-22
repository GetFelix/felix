use serial_test::serial;

use super::*;
use crate::config::{
    BootstrapConfig, DEFAULT_READINESS_CACHE_TTL_MS, DEFAULT_READINESS_TIMEOUT_MS,
    NodeLivenessConfig, StorageBackend,
};

/// Memory-backed, bootstrap off, every port ephemeral.
fn config() -> ControlPlaneConfig {
    ControlPlaneConfig {
        bind_addr: "127.0.0.1:0".parse().expect("bind"),
        api_tls: None,
        api_ui: crate::config::ApiUi::Off,
        api_ui_path: crate::config::DEFAULT_API_UI_PATH.to_string(),
        metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
        region_id: "local".to_string(),
        storage: StorageBackend::Memory,
        raft: None,
        shard_engine: Default::default(),
        changes_limit: 10,
        change_retention_max_rows: Some(20),
        oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
        bootstrap: BootstrapConfig {
            enabled: false,
            bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
            token: None,
            previous_token: None,
            tls: None,
            dev_tokens: false,
            operator_token_digests: Vec::new(),
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
            client_ca_path: None,
            bind_subject: false,
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
        .tls_certs_only([ca])
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

/// A CA, written to `dir/<name>-ca.pem`, that issues client certificates.
struct ClientCa {
    path: String,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl ClientCa {
    fn new(dir: &std::path::Path, name: &str) -> Self {
        let key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).expect("ca");
        let path = dir.join(format!("{name}-ca.pem"));
        std::fs::write(&path, cert.pem()).expect("write ca");
        Self {
            path: path.display().to_string(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// A client certificate with `san` as a URI SAN, as a reqwest identity.
    fn issue(&self, san: &str) -> reqwest::Identity {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names = vec![rcgen::SanType::URI(san.try_into().expect("uri"))];
        let cert = params.signed_by(&key, &self.issuer).expect("sign");
        reqwest::Identity::from_pem(format!("{}\n{}", cert.pem(), key.serialize_pem()).as_bytes())
            .expect("identity")
    }
}

/// Start `run` with `api_tls` on a free port; returns its port and a stop.
async fn start_api(
    api_tls: crate::config::ApiTlsConfig,
) -> (
    u16,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
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
    // Wait for the listener to accept connections.
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    (port, stop, server)
}

fn client(ca: &reqwest::Certificate, identity: Option<reqwest::Identity>) -> reqwest::Client {
    let builder = reqwest::Client::builder().tls_certs_only([ca.clone()]);
    match identity {
        Some(identity) => builder.identity(identity),
        None => builder,
    }
    .build()
    .expect("client")
}

/// With a client CA, the API refuses the handshake to a client with no
/// certificate or one from another CA, and serves one the CA issued.
#[tokio::test]
#[serial]
async fn the_api_requires_a_client_certificate_from_the_configured_ca() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut api_tls, server_ca) = api_pki(dir.path());
    let clients = ClientCa::new(dir.path(), "clients");
    let other = ClientCa::new(dir.path(), "other");
    api_tls.client_ca_path = Some(clients.path.clone());
    let (port, stop, server) = start_api(api_tls).await;
    let url = format!("https://localhost:{port}/v1/system/live");

    let issued = client(&server_ca, Some(clients.issue("felix:principal:broker-1")))
        .get(&url)
        .send()
        .await
        .expect("a certificate from the client CA is served");
    assert!(issued.status().is_success(), "{}", issued.status());

    assert!(
        client(&server_ca, None).get(&url).send().await.is_err(),
        "served a client without a certificate"
    );
    assert!(
        client(&server_ca, Some(other.issue("felix:principal:broker-1")))
            .get(&url)
            .send()
            .await
            .is_err(),
        "served a certificate from another CA"
    );

    let _ = stop.send(());
    server.await.expect("join").expect("run");
}

/// With subject binding, a bearer token must name the principal the client
/// certificate was issued to.
#[tokio::test]
#[serial]
async fn the_api_binds_a_bearer_token_to_the_client_certificate() {
    use base64::Engine as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut api_tls, server_ca) = api_pki(dir.path());
    let clients = ClientCa::new(dir.path(), "clients");
    api_tls.client_ca_path = Some(clients.path.clone());
    api_tls.bind_subject = true;
    let (port, stop, server) = start_api(api_tls).await;
    let url = format!("https://localhost:{port}/v1/system/live");
    let http = client(&server_ca, Some(clients.issue("felix:principal:alice")));
    // Only the claims matter to the binding; the signature is the handler's
    // to check.
    let token = |sub: &str| {
        let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"sub":"{sub}","tid":"t1"}}"#));
        format!("e30.{claims}.sig")
    };

    let own = http
        .get(&url)
        .bearer_auth(token("alice"))
        .send()
        .await
        .expect("own token");
    assert!(own.status().is_success(), "{}", own.status());

    let stolen = http
        .get(&url)
        .bearer_auth(token("mallory"))
        .send()
        .await
        .expect("another principal's token");
    assert_eq!(stolen.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body = stolen.text().await.expect("body");
    assert!(body.contains("not issued to the token"), "{body}");

    // A delegated token: issued for bob, presented by its actor alice.
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(r#"{"sub":"bob","tid":"t1","act":{"sub":"alice"}}"#);
    let delegated = http
        .get(&url)
        .bearer_auth(format!("e30.{claims}.sig"))
        .send()
        .await
        .expect("delegated token");
    assert!(delegated.status().is_success(), "{}", delegated.status());

    let anonymous = http.get(&url).send().await.expect("no token");
    assert!(anonymous.status().is_success(), "{}", anonymous.status());

    let _ = stop.send(());
    server.await.expect("join").expect("run");
}

/// The page is mounted by `run` only when configured: 404 when off, the
/// pinned page at the configured path when on.
#[tokio::test]
#[serial]
async fn the_api_reference_page_is_served_only_when_enabled() {
    for (api_ui, expected) in [
        (crate::config::ApiUi::Off, reqwest::StatusCode::NOT_FOUND),
        (crate::config::ApiUi::Scalar, reqwest::StatusCode::OK),
    ] {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("probe")
            .local_addr()
            .expect("addr")
            .port();
        let config = ControlPlaneConfig {
            bind_addr: format!("127.0.0.1:{port}").parse().expect("bind"),
            api_ui,
            api_ui_path: "/reference".to_string(),
            ..config()
        };
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(run(config, async move {
            let _ = stopped.await;
        }));

        let client = reqwest::Client::new();
        let mut status = None;
        for _ in 0..100 {
            if let Ok(response) = client
                .get(format!("http://127.0.0.1:{port}/reference"))
                .send()
                .await
            {
                status = Some(response.status());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(status.expect("the API never answered"), expected);

        let _ = stop.send(());
        server.await.expect("join").expect("run");
    }
}

#[tokio::test]
#[serial]
async fn unreadable_api_key_material_fails_startup() {
    let config = ControlPlaneConfig {
        api_tls: Some(crate::config::ApiTlsConfig {
            cert_path: "/nonexistent/api.pem".to_string(),
            key_path: "/nonexistent/api.key.pem".to_string(),
            client_ca_path: None,
            bind_subject: false,
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

/// A group entry that fails stops the member, so its store is closed
/// instead of left to the process exit.
#[tokio::test]
async fn a_failed_group_entry_stops_the_member() {
    use crate::raft::{PeerSecurity, RaftHandle, RaftSettings};
    use crate::store::StoreConfig;
    use crate::store::memory::InMemoryStore;
    use crate::store::raft::state_machine::MetadataStateMachine;

    let dir = tempfile::tempdir().expect("tempdir");
    // A waiver that does not decode fails the entry of an empty member.
    std::fs::write(dir.path().join("form-without"), b"not json").expect("waiver");
    let machine = Arc::new(MetadataStateMachine::new(Arc::new(InMemoryStore::new(
        StoreConfig {
            changes_limit: 10,
            change_retention_max_rows: Some(20),
        },
    ))));
    let security = PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        previous_token: None,
        next_token: None,
        tls: None,
    };
    let settings = RaftSettings::new(1, dir.path().into(), security);
    let handle = RaftHandle::start(settings, machine).await.expect("start");
    super::state::enter_group_or_stop(
        &handle,
        Default::default(),
        crate::raft::InitialClusterState::New,
        CancellationToken::new(),
    )
    .await
    .expect_err("the entry fails");
    assert!(handle.is_stopped(), "the member was left running");
}

/// A member's routes get its node only once it has entered the group. A
/// node attached first would answer votes before it knows whether it may,
/// and one whose entry failed would stay attached after it stopped.
#[tokio::test]
async fn a_member_is_attached_only_after_it_enters_the_group() {
    use crate::config::RaftBackendConfig;
    use crate::raft::{PeerSecurity, RaftMember};
    use crate::store::StoreConfig;
    use crate::store::memory::InMemoryStore;
    use crate::store::raft::state_machine::MetadataStateMachine;

    let dir = tempfile::tempdir().expect("tempdir");
    // A waiver that does not decode fails the entry of an empty member.
    std::fs::write(dir.path().join("form-without"), b"not json").expect("waiver");
    let machine = Arc::new(MetadataStateMachine::new(Arc::new(InMemoryStore::new(
        StoreConfig {
            changes_limit: 10,
            change_retention_max_rows: Some(20),
        },
    ))));
    let security = PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        previous_token: None,
        next_token: None,
        tls: None,
    };
    let raft_cfg = RaftBackendConfig {
        node_id: 1,
        data_dir: dir.path().into(),
        peers: Default::default(),
        peer_bind_addr: "127.0.0.1:0".parse().expect("addr"),
        security: security.clone(),
        insecure_peers: false,
        initial_cluster_state: crate::raft::InitialClusterState::New,
        heartbeat_ms: None,
        election_timeout_min_ms: None,
        election_timeout_max_ms: None,
        snapshot_logs_since_last: None,
        logs_kept_behind_snapshot: None,
        snapshot_max_bytes: None,
        floor_report_after_ms: None,
        write_timeout_ms: None,
    };
    let member = RaftMember::open(
        crate::raft::RaftSettings::new(1, dir.path().into(), security),
        machine,
    )
    .await
    .expect("open");
    let started = super::state::start_node(&member, &raft_cfg, &CancellationToken::new()).await;
    assert!(started.is_err(), "the entry did not fail");
    assert!(
        !member.is_attached(),
        "a member that never entered is served"
    );
}

/// A member whose certificate does not name it must fail to start, not come
/// up with its API serving and no peer listener.
#[tokio::test]
async fn a_member_under_a_certificate_that_is_not_its_own_does_not_start() {
    use crate::config::RaftBackendConfig;
    use crate::raft::{MEMBER_URI_PREFIX, PeerSecurity, PeerTls};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let ca = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        format!("{MEMBER_URI_PREFIX}test-cluster:2")
            .try_into()
            .expect("uri"),
    ));
    let cert = params.signed_by(&key, &ca).expect("cert");
    std::fs::write(path("ca.pem"), ca_cert.pem()).expect("ca");
    std::fs::write(path("peer.pem"), cert.pem()).expect("cert");
    std::fs::write(path("peer.key"), key.serialize_pem()).expect("key");

    let mut config = config();
    config.storage = StorageBackend::Raft;
    config.raft = Some(RaftBackendConfig {
        node_id: 1,
        data_dir: dir.path().join("raft"),
        peers: Default::default(),
        peer_bind_addr: "127.0.0.1:0".parse().expect("addr"),
        security: PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: None,
            previous_token: None,
            next_token: None,
            tls: Some(PeerTls::new(
                path("peer.pem"),
                path("peer.key"),
                path("ca.pem"),
            )),
        },
        insecure_peers: false,
        initial_cluster_state: crate::raft::InitialClusterState::New,
        heartbeat_ms: None,
        election_timeout_min_ms: None,
        election_timeout_max_ms: None,
        snapshot_logs_since_last: None,
        logs_kept_behind_snapshot: None,
        snapshot_max_bytes: None,
        floor_report_after_ms: None,
        write_timeout_ms: None,
    });
    let err = match build_state(config, Readiness::ready(), &Default::default()).await {
        Ok(_) => panic!("a member started under member 2's certificate"),
        Err(err) => err,
    };
    assert!(
        format!("{err:#}").contains("issued to raft member 2"),
        "{err:#}"
    );
}
