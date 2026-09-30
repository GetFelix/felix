//! Demonstrate end-to-end cross-tenant isolation using real Felix components.
//!
//! # What this demo proves
//! - Tokens minted for tenant `t1` cannot access tenant `t2` resources.
//! - Tokens minted for tenant `t2` without RBAC cannot access `t2` resources.
//! - Authorization decisions are enforced by the broker using real metadata sync.
//!
//! # Flow summary
//! 1. Start a fake ES256 OIDC IdP (discovery + JWKS).
//! 2. Start the control plane (Postgres-backed) on a random local port.
//! 3. Start the broker with control-plane sync.
//! 4. Bootstrap tenants `t1` and `t2` via the bootstrap API.
//! 5. Create namespace/stream/cache in both tenants.
//! 6. Exchange upstream ID tokens for Felix tokens in both tenants.
//! 7. Validate that `t1` token succeeds on `t1` and is denied on `t2`.
//! 8. Validate that `t2` token is denied on `t2` (no RBAC granted).
//!
//! - Uses only existing control-plane routes; no demo-only APIs.
//! - Uses ES256 (no RSA) for the fake IdP.
//! - Prints a summary table and exits non-zero on failure.
use anyhow::{bail, Context, Result};
use axum::http::StatusCode;
use axum::{Json, Router};
use base64::Engine as _;
use felix_broker::Broker;
use felix_broker_service::cluster::catalog_sync as broker_catalog_sync;
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{Client, ClientConfig};
use felix_controlplane_service::api::bootstrap::BootstrapInitializeRequest;
use felix_controlplane_service::api::types::{
    CacheCreateRequest, NamespaceCreateRequest, StreamCreateRequest,
};
use felix_controlplane_service::api::{build_bootstrap_router, build_router, AppState};
use felix_controlplane_service::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use felix_controlplane_service::auth::principal::principal_id;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::config::{
    PostgresConfig, DEFAULT_CHANGES_LIMIT, DEFAULT_CHANGE_RETENTION_MAX_ROWS,
};
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, StreamKind,
};
use felix_controlplane_service::store::{postgres::PostgresStore, ControlPlaneStore, StoreConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const BOOTSTRAP_TOKEN: &str = "demo-bootstrap-token";
const TENANT_T1: &str = "t1";
const TENANT_T2: &str = "t2";
const NAMESPACE: &str = "default";
const STREAM: &str = "orders";
const CACHE: &str = "primary";
const ALICE_SUB: &str = "p:alice";
const ADMIN_SUB: &str = "p:admin";
/// The broker's own principal, minted in t1 with `node.view:cluster:*` --
/// which tenant signs a cluster-scoped token is immaterial.
const BROKER_SUB: &str = "p:broker";
const IDP_KID: &str = "kid-1";

const EC_PRIVATE_KEY_DER_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgkcZLhh5bmc6yfv8ZrDxWybm+E+aoz2euIJD3fM73VSyhRANCAAQRkD6ZJEwqBms4JDddpbTjl4Ro49h8WRoNVnEcR/Tp6LhwGGZ8Ku1Gw9spY/BCsiW+5AqIqVlNVgGgJFMRbR1V";
const EC_JWK_X: &str = "EZA-mSRMKgZrOCQ3XaW045eEaOPYfFkaDVZxHEf06eg";
const EC_JWK_Y: &str = "uHAYZnwq7UbD2ylj8EKyJb7kCoipWU1WAaAkUxFtHVU";
const ID_TOKEN_IAT: i64 = 1_700_000_000;
const ID_TOKEN_EXP: i64 = 2_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::time::Duration;

    async fn has_postgres() -> bool {
        if std::env::var("FELIX_TEST_DATABASE_URL").is_ok()
            || std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL").is_ok()
            || std::env::var("DATABASE_URL").is_ok()
        {
            return true;
        }
        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect("127.0.0.1:55432"),
        )
        .await
        .is_ok()
    }

    #[tokio::test]
    async fn cross_tenant_demo_end_to_end() -> Result<()> {
        if !has_postgres().await {
            eprintln!("skipping cross-tenant demo: postgres not available");
            return Ok(());
        }
        let mut report = DemoReport::new();
        tokio::time::timeout(Duration::from_secs(30), run_demo(&mut report))
            .await
            .context("cross-tenant demo timeout")?
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut report = DemoReport::new();
    let result = run_demo(&mut report).await;
    report.print_summary();
    result
}

async fn run_demo(report: &mut DemoReport) -> Result<()> {
    println!("== Felix Demo: Cross-Tenant Isolation (E2E) ==");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("build http client")?;

    let (idp_addr, idp_handle) = spawn_fake_idp().await?;
    let idp_base = format!("http://{idp_addr}");
    wait_for_idp(&http, &idp_base).await?;
    report.pass("STEP 0 fake IdP up", format!("addr={idp_addr}"));

    report.pass("STEP 1 connect postgres", "starting");
    let store = connect_store().await?;
    let (cp_addr, cp_handle) = spawn_controlplane(store).await?;
    let cp_base = format!("http://{cp_addr}");
    wait_for_controlplane(&http, &cp_base).await?;
    report.pass("STEP 2 control plane up", format!("addr={cp_addr}"));

    let admin_principal = principal_id(&idp_base, ADMIN_SUB);
    let alice_principal = principal_id(&idp_base, ALICE_SUB);
    let broker_principal = principal_id(&idp_base, BROKER_SUB);

    let status = bootstrap_tenant_t1(
        &http,
        &cp_base,
        &idp_base,
        &admin_principal,
        &alice_principal,
        &broker_principal,
    )
    .await?;
    report.check_http("STEP 3 bootstrap tenant t1", status, StatusCode::OK)?;

    let status = bootstrap_tenant_t2(&http, &cp_base, &idp_base, &admin_principal).await?;
    report.check_http("STEP 4 bootstrap tenant t2", status, StatusCode::OK)?;

    // The broker reads the metadata feeds with a credential of its own; the
    // admin creates the metadata with theirs. Neither works without a token.
    let broker_id_token = mint_id_token(&idp_base, BROKER_SUB, &["role:broker"])?;
    let broker_token =
        exchange_token(&http, &cp_base, TENANT_T1, &broker_id_token, CONTROLPLANE).await?;
    let admin_id_token = mint_id_token(&idp_base, ADMIN_SUB, &[])?;
    let t1_admin = exchange_token(&http, &cp_base, TENANT_T1, &admin_id_token, CONTROLPLANE).await?;
    let t2_admin = exchange_token(&http, &cp_base, TENANT_T2, &admin_id_token, CONTROLPLANE).await?;
    report.pass("STEP 5 broker and admin token exchange", "status=200");

    let (broker_addr, broker_cert, broker, broker_handles) =
        spawn_broker(&cp_base, &broker_token).await?;
    report.pass("STEP 6 broker up", format!("addr={broker_addr}"));

    report.check_http(
        "STEP 7 create t1 namespace",
        create_namespace(&http, &cp_base, TENANT_T1, &t1_admin).await?,
        StatusCode::CREATED,
    )?;
    report.check_http(
        "STEP 8 create t1 stream",
        create_stream(&http, &cp_base, TENANT_T1, &t1_admin).await?,
        StatusCode::CREATED,
    )?;
    report.check_http(
        "STEP 9 create t1 cache",
        create_cache(&http, &cp_base, TENANT_T1, &t1_admin).await?,
        StatusCode::CREATED,
    )?;

    report.check_http(
        "STEP 10 create t2 namespace",
        create_namespace(&http, &cp_base, TENANT_T2, &t2_admin).await?,
        StatusCode::CREATED,
    )?;
    report.check_http(
        "STEP 11 create t2 stream",
        create_stream(&http, &cp_base, TENANT_T2, &t2_admin).await?,
        StatusCode::CREATED,
    )?;
    report.check_http(
        "STEP 12 create t2 cache",
        create_cache(&http, &cp_base, TENANT_T2, &t2_admin).await?,
        StatusCode::CREATED,
    )?;
    wait_for_broker_metadata(&broker, Duration::from_secs(12)).await?;

    let alice_id_token = mint_id_token(&idp_base, ALICE_SUB, &[])?;
    let t1_token = exchange_token(&http, &cp_base, TENANT_T1, &alice_id_token, BROKER).await?;
    report.pass("STEP 13 token exchange t1", "status=200");

    let t2_token = exchange_token(&http, &cp_base, TENANT_T2, &alice_id_token, BROKER).await?;
    report.pass("STEP 14 token exchange t2", "status=200");

    let t1_client = build_client(&broker_addr, &broker_cert, TENANT_T1, &t1_token).await?;
    let t2_client = build_client(&broker_addr, &broker_cert, TENANT_T2, &t2_token).await?;
    let t1_token_on_t2_client =
        build_client(&broker_addr, &broker_cert, TENANT_T2, &t1_token).await;

    let t1_publish = retry_op("publish", Duration::from_secs(6), || {
        publish_once(&t1_client, TENANT_T1)
    })
    .await;
    report.check_op("STEP 15 t1 publish allowed", t1_publish, true)?;

    let t1_sub = retry_op("subscribe", Duration::from_secs(6), || {
        subscribe_and_receive(&t1_client, TENANT_T1)
    })
    .await;
    report.check_op("STEP 16 t1 subscribe allowed", t1_sub, true)?;

    let t1_cache = retry_op("cache", Duration::from_secs(6), || {
        cache_roundtrip(&t1_client, TENANT_T1)
    })
    .await;
    report.check_op("STEP 17 t1 cache allowed", t1_cache, true)?;

    let t1_on_t2_publish = retry_op("publish", Duration::from_secs(6), || async {
        match &t1_token_on_t2_client {
            Ok(client) => publish_once(client, TENANT_T2).await,
            Err(err) => Err(anyhow::anyhow!("client auth failed: {err}")),
        }
    })
    .await;
    report.check_op(
        "STEP 18 t1 token on t2 publish denied",
        t1_on_t2_publish,
        false,
    )?;

    let t1_on_t2_sub = retry_op("subscribe", Duration::from_secs(6), || async {
        match &t1_token_on_t2_client {
            Ok(client) => subscribe_once(client, TENANT_T2).await,
            Err(err) => Err(anyhow::anyhow!("client auth failed: {err}")),
        }
    })
    .await;
    report.check_op(
        "STEP 19 t1 token on t2 subscribe denied",
        t1_on_t2_sub,
        false,
    )?;

    let t1_on_t2_cache = retry_op("cache", Duration::from_secs(6), || async {
        match &t1_token_on_t2_client {
            Ok(client) => cache_roundtrip(client, TENANT_T2).await,
            Err(err) => Err(anyhow::anyhow!("client auth failed: {err}")),
        }
    })
    .await;
    report.check_op("STEP 20 t1 token on t2 cache denied", t1_on_t2_cache, false)?;

    let t2_publish = retry_op("publish", Duration::from_secs(6), || {
        publish_once(&t2_client, TENANT_T2)
    })
    .await;
    report.check_op("STEP 21 t2 token publish denied", t2_publish, false)?;

    let t2_sub = retry_op("subscribe", Duration::from_secs(6), || {
        subscribe_once(&t2_client, TENANT_T2)
    })
    .await;
    report.check_op("STEP 22 t2 token subscribe denied", t2_sub, false)?;

    let t2_cache = retry_op("cache", Duration::from_secs(6), || {
        cache_roundtrip(&t2_client, TENANT_T2)
    })
    .await;
    report.check_op("STEP 23 t2 token cache denied", t2_cache, false)?;

    idp_handle.abort();
    cp_handle.abort();
    for handle in broker_handles {
        handle.abort();
    }

    Ok(())
}

/// Starts a minimal OIDC IdP server that serves discovery + JWKS.
async fn spawn_fake_idp() -> Result<(SocketAddr, JoinHandle<()>)> {
    let jwks = json!({
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "x": EC_JWK_X,
                "y": EC_JWK_Y,
                "use": "sig",
                "alg": "ES256",
                "kid": IDP_KID
            }
        ]
    });

    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            axum::routing::get({
                move |axum::extract::State(state): axum::extract::State<IdpState>| {
                    let jwks_uri = format!("{}/jwks.json", state.issuer);
                    let discovery = json!({
                        "issuer": state.issuer,
                        "jwks_uri": jwks_uri,
                    });
                    async move { Json(discovery) }
                }
            }),
        )
        .route(
            "/jwks.json",
            axum::routing::get({
                let jwks = jwks.clone();
                move || {
                    let jwks = jwks.clone();
                    async move { Json(jwks) }
                }
            }),
        )
        .with_state(IdpState {
            issuer: String::new(),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let issuer = format!("http://{addr}");

    let app = app.with_state(IdpState { issuer });
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("fake idp server error: {err}");
        }
    });
    Ok((addr, handle))
}

#[derive(Clone)]
struct IdpState {
    issuer: String,
}

async fn connect_store() -> Result<Arc<PostgresStore>> {
    let urls = resolve_db_urls();
    let store_config = StoreConfig {
        changes_limit: DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    };

    let mut last_err = None;
    for url in urls {
        if let Err(err) = wait_for_postgres(&url, Duration::from_secs(8)).await {
            last_err = Some(anyhow::anyhow!(err.to_string()));
            continue;
        }
        let pg = PostgresConfig {
            url,
            max_connections: 5,
            connect_timeout_ms: 4_000,
            acquire_timeout_ms: 4_000,
        };
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            let attempt = tokio::time::timeout(
                Duration::from_secs(2),
                PostgresStore::connect(&pg, store_config.clone()),
            )
            .await;
            match attempt {
                Ok(Ok(store)) => return Ok(Arc::new(store)),
                Ok(Err(err)) => last_err = Some(err.into()),
                Err(_) => last_err = Some(anyhow::anyhow!("connect timeout")),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    if let Some(err) = last_err {
        eprintln!("postgres connection error: {err}");
    }
    bail!(
        "failed to connect to postgres; ensure pg:up is running and FELIX_TEST_DATABASE_URL is set"
    )
}

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<()> {
    let Some((host, port)) = parse_pg_host_port(url) else {
        return Ok(());
    };
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let addr = format!("{host}:{port}");
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("postgres not reachable at {host}:{port}");
}

fn parse_pg_host_port(url: &str) -> Option<(String, u16)> {
    let after_at = url.rsplit_once('@')?.1;
    let host_port = after_at.split('/').next()?;
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().ok()?),
        None => (host_port.to_string(), 5432),
    };
    Some((host, port))
}

fn resolve_db_urls() -> Vec<String> {
    if let Ok(url) = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        return vec![url];
    }
    vec![
        "postgres://postgres:postgres@127.0.0.1:55432/postgres".to_string(),
        "postgres://postgres:postgres@host.docker.internal:55432/postgres".to_string(),
    ]
}

/// Starts the control plane HTTP server on a random local port.
async fn spawn_controlplane(store: Arc<PostgresStore>) -> Result<(SocketAddr, JoinHandle<()>)> {
    let state = AppState {
        region: felix_controlplane_service::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: felix_controlplane_service::api::types::FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_tokens: vec![BOOTSTRAP_TOKEN.to_string()],
        node_liveness: Default::default(),
        // The demo's store is in-memory, which has nothing to be unready about.
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };

    let app = build_router(state.clone()).merge(build_bootstrap_router(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("controlplane server error: {err}");
        }
    });
    Ok((addr, handle))
}

/// Starts a real broker QUIC server and a control-plane sync loop.
async fn spawn_broker(
    controlplane_url: &str,
    credential: &str,
) -> Result<(
    SocketAddr,
    CertificateDer<'static>,
    Arc<Broker>,
    Vec<JoinHandle<()>>,
)> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.controlplane_url = Some(controlplane_url.to_string());
    config.controlplane_sync_interval_ms = 200;
    config.disable_timings = true;

    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        transport,
    )?);
    let addr = server.local_addr()?;

    let controlplane_url = controlplane_url.to_string();
    let auth = Arc::new(BrokerAuth::new(controlplane_url.clone()));
    let broker_config = config.clone();
    let sync_interval_ms = config.controlplane_sync_interval_ms;
    let broker_quic = Arc::clone(&broker);
    let broker_sync = Arc::clone(&broker);
    let server_quic = Arc::clone(&server);
    let broker_task = tokio::spawn(async move {
        if let Err(err) = quic::serve(server_quic, broker_quic, broker_config, auth).await {
            eprintln!("broker server error: {err}");
        }
    });

    let credential = felix_broker_service::cluster::credential::NodeCredential::new(credential);
    let sync_task = tokio::spawn(async move {
        let interval = Duration::from_millis(sync_interval_ms);
        if let Err(err) = broker_catalog_sync::start_sync(
            broker_sync,
            controlplane_url,
            interval,
            Some(credential),
        )
        .await
        {
            eprintln!("controlplane sync error: {err}");
        }
    });

    Ok((addr, cert, broker, vec![broker_task, sync_task]))
}

async fn wait_for_broker_metadata(broker: &Broker, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let t1_ready = broker.namespace_exists(TENANT_T1, NAMESPACE).await
            && broker.stream_exists(TENANT_T1, NAMESPACE, STREAM).await
            && broker.cache_exists(TENANT_T1, NAMESPACE, CACHE).await;
        let t2_ready = broker.namespace_exists(TENANT_T2, NAMESPACE).await
            && broker.stream_exists(TENANT_T2, NAMESPACE, STREAM).await
            && broker.cache_exists(TENANT_T2, NAMESPACE, CACHE).await;
        if t1_ready && t2_ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("broker metadata sync timeout");
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

async fn build_client(
    broker_addr: &SocketAddr,
    broker_cert: &CertificateDer<'static>,
    tenant_id: &str,
    token: &str,
) -> Result<Client> {
    let mut roots = RootCertStore::empty();
    roots.add(broker_cert.clone())?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.auth_tenant_id = Some(tenant_id.to_string());
    config.auth_token = Some(token.to_string());
    match tokio::time::timeout(
        Duration::from_secs(3),
        Client::connect(*broker_addr, "localhost", config),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => bail!("broker client connect timeout"),
    }
}

async fn bootstrap_tenant_t1(
    http: &reqwest::Client,
    cp_base: &str,
    idp_base: &str,
    admin_principal: &str,
    alice_principal: &str,
    broker_principal: &str,
) -> Result<StatusCode> {
    let idp = IdpIssuerConfig {
        issuer: idp_base.to_string(),
        audiences: vec!["felix-controlplane".to_string()],
        discovery_url: None,
        jwks_url: Some(format!("{idp_base}/jwks.json")),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: Some("groups".to_string()),
        },
    };

    let mut policies = vec![
        PolicyRule {
            subject: "role:reader".to_string(),
            // Canonical tenant-qualified stream object.
            object: format!("stream:{TENANT_T1}/{NAMESPACE}/{STREAM}"),
            action: "stream.publish".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            object: format!("stream:{TENANT_T1}/{NAMESPACE}/{STREAM}"),
            action: "stream.subscribe".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            // Canonical tenant-qualified cache object.
            object: format!("cache:{TENANT_T1}/{NAMESPACE}/{CACHE}"),
            action: "cache.read".to_string(),
        },
        PolicyRule {
            subject: "role:reader".to_string(),
            object: format!("cache:{TENANT_T1}/{NAMESPACE}/{CACHE}"),
            action: "cache.write".to_string(),
        },
    ];

    // Cluster scope sits outside the tenant hierarchy, so no tenant admin can
    // grant it later; bootstrap is where the broker's read access enters.
    policies.push(PolicyRule {
        subject: "role:broker".to_string(),
        object: "cluster:*".to_string(),
        action: "node.view".to_string(),
    });

    let groupings = vec![
        GroupingRule {
            user: alice_principal.to_string(),
            role: "role:reader".to_string(),
        },
        GroupingRule {
            user: broker_principal.to_string(),
            role: "role:broker".to_string(),
        },
    ];

    let body = BootstrapInitializeRequest {
        display_name: "Tenant One".to_string(),
        idp_issuers: vec![idp],
        initial_admin_principals: vec![admin_principal.to_string()],
        policies,
        groupings,
    };

    let url = format!("{cp_base}/internal/bootstrap/tenants/{TENANT_T1}/initialize");
    let response = http
        .post(url)
        .header("X-Felix-Bootstrap-Token", BOOTSTRAP_TOKEN)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

async fn bootstrap_tenant_t2(
    http: &reqwest::Client,
    cp_base: &str,
    idp_base: &str,
    admin_principal: &str,
) -> Result<StatusCode> {
    let idp = IdpIssuerConfig {
        issuer: idp_base.to_string(),
        audiences: vec!["felix-controlplane".to_string()],
        discovery_url: None,
        jwks_url: Some(format!("{idp_base}/jwks.json")),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: Some("groups".to_string()),
        },
    };

    let body = BootstrapInitializeRequest {
        display_name: "Tenant Two".to_string(),
        idp_issuers: vec![idp],
        initial_admin_principals: vec![admin_principal.to_string()],
        // Token exchange requires non-empty permissions; grant a harmless
        // permission on an unrelated resource so the token can be minted.
        policies: vec![PolicyRule {
            subject: "role:reader".to_string(),
            // Keep this tenant-qualified so cross-tenant checks stay explicit.
            object: format!("stream:{TENANT_T2}/{NAMESPACE}/other"),
            action: "stream.subscribe".to_string(),
        }],
        groupings: vec![GroupingRule {
            user: principal_id(idp_base, ALICE_SUB),
            role: "role:reader".to_string(),
        }],
    };

    let url = format!("{cp_base}/internal/bootstrap/tenants/{TENANT_T2}/initialize");
    let response = http
        .post(url)
        .header("X-Felix-Bootstrap-Token", BOOTSTRAP_TOKEN)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

async fn create_namespace(
    http: &reqwest::Client,
    cp_base: &str,
    tenant_id: &str,
    admin_token: &str,
) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{tenant_id}/namespaces");
    let body = NamespaceCreateRequest {
        namespace: NAMESPACE.to_string(),
        display_name: "Default".to_string(),
    };
    let response = http
        .post(url)
        .bearer_auth(admin_token)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

async fn create_stream(
    http: &reqwest::Client,
    cp_base: &str,
    tenant_id: &str,
    admin_token: &str,
) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{tenant_id}/namespaces/{NAMESPACE}/streams");
    let body = StreamCreateRequest {
        stream: STREAM.to_string(),
        kind: StreamKind::Stream,
        shards: 1,
        // Leader-only: this demo is about tenant isolation, not replication.
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: false,
        region: None,
        routing: None,
    };
    let response = http
        .post(url)
        .bearer_auth(admin_token)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

async fn create_cache(
    http: &reqwest::Client,
    cp_base: &str,
    tenant_id: &str,
    admin_token: &str,
) -> Result<StatusCode> {
    let url = format!("{cp_base}/v1/tenants/{tenant_id}/namespaces/{NAMESPACE}/caches");
    let body = CacheCreateRequest {
        cache: CACHE.to_string(),
        display_name: "Primary".to_string(),
        ..Default::default()
    };
    let response = http
        .post(url)
        .bearer_auth(admin_token)
        .json(&body)
        .send()
        .await?;
    Ok(response.status())
}

#[derive(Serialize, Deserialize)]
struct TokenExchangeResponse {
    felix_token: String,
}

/// Token audience for connecting to a broker.
const BROKER: &str = "felix-broker";
/// Token audience for the control plane's API, including a broker's own
/// node credential.
const CONTROLPLANE: &str = "felix-controlplane";

/// Exchanges an upstream ID token for a Felix access token for a tenant and
/// `audience`.
async fn exchange_token(
    http: &reqwest::Client,
    cp_base: &str,
    tenant_id: &str,
    id_token: &str,
    audience: &str,
) -> Result<String> {
    let url = format!("{cp_base}/v1/tenants/{tenant_id}/token/exchange");
    let response = http
        .post(url)
        .bearer_auth(id_token)
        .json(&json!({ "audience": audience }))
        .send()
        .await?;
    let status = response.status();
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "token exchange failed: tenant={} status={} body={}",
            tenant_id,
            status,
            body
        );
    }
    let body: TokenExchangeResponse = response.json().await?;
    Ok(body.felix_token)
}

/// Mints a deterministic ES256 ID token for the fake IdP.
fn mint_id_token(issuer: &str, subject: &str, groups: &[&str]) -> Result<String> {
    let claims = json!({
        "iss": issuer,
        "sub": subject,
        "aud": "felix-controlplane",
        "iat": ID_TOKEN_IAT,
        "exp": ID_TOKEN_EXP,
        "groups": groups,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(IDP_KID.to_string());

    let der = base64::engine::general_purpose::STANDARD.decode(EC_PRIVATE_KEY_DER_B64)?;
    let key = EncodingKey::from_ec_der(&der);
    let token = jsonwebtoken::encode(&header, &claims, &key)?;
    Ok(token)
}

async fn publish_once(client: &Client, tenant_id: &str) -> Result<()> {
    let publisher = client.publisher().await?;
    publisher
        .publish(
            tenant_id,
            NAMESPACE,
            STREAM,
            b"cross-tenant-demo".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    Ok(())
}

async fn subscribe_once(client: &Client, tenant_id: &str) -> Result<()> {
    let _sub = client.subscribe(tenant_id, NAMESPACE, STREAM).await?;
    Ok(())
}

async fn subscribe_and_receive(client: &Client, tenant_id: &str) -> Result<()> {
    let mut sub = client.subscribe(tenant_id, NAMESPACE, STREAM).await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            tenant_id,
            NAMESPACE,
            STREAM,
            b"cross-tenant-demo".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    let event = tokio::time::timeout(Duration::from_secs(2), sub.next_event()).await;
    match event {
        Ok(Ok(Some(_))) => Ok(()),
        Ok(Ok(None)) => bail!("subscription closed"),
        Ok(Err(err)) => bail!("subscribe error: {err}"),
        Err(_) => bail!("subscribe timeout"),
    }
}

async fn cache_roundtrip(client: &Client, tenant_id: &str) -> Result<()> {
    client
        .cache_put(
            tenant_id,
            NAMESPACE,
            CACHE,
            "key",
            b"value".to_vec().into(),
            Some(10_000),
        )
        .await?;
    let value = client.cache_get(tenant_id, NAMESPACE, CACHE, "key").await?;
    if value.is_none() {
        bail!("cache miss");
    }
    Ok(())
}

async fn wait_for_idp(http: &reqwest::Client, idp_base: &str) -> Result<()> {
    let url = format!("{idp_base}/.well-known/openid-configuration");
    for _ in 0..10 {
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("fake IdP did not become ready");
}

async fn wait_for_controlplane(http: &reqwest::Client, cp_base: &str) -> Result<()> {
    let url = format!("{cp_base}/v1/system/health");
    for _ in 0..10 {
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("control plane did not become ready");
}

/// Retries a broker operation until it succeeds or the timeout elapses.
async fn retry_op<F, Fut>(label: &str, timeout: Duration, mut op: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let start = Instant::now();
    loop {
        match with_timeout(label, Duration::from_secs(2), op()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                let msg = err.to_string();
                if is_retryable_broker_error(&msg) {
                    if start.elapsed() > timeout {
                        bail!("{label} timeout: {msg}");
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

fn is_retryable_broker_error(message: &str) -> bool {
    message.contains("not found")
        || message.contains("None")
        || message.contains("closed")
        || message.contains("timeout")
}

async fn with_timeout<T>(
    label: &str,
    timeout: Duration,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => bail!("{label} timed out"),
    }
}

struct DemoReport {
    steps: Vec<DemoStep>,
}

struct DemoStep {
    label: String,
    status: String,
}

impl DemoReport {
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    fn pass(&mut self, label: &str, detail: impl Into<String>) {
        let detail = detail.into();
        let status = if detail.is_empty() {
            "PASS".to_string()
        } else {
            format!("PASS ({detail})")
        };
        println!("{label}: {status}");
        self.steps.push(DemoStep {
            label: label.to_string(),
            status,
        });
    }

    fn check_http(&mut self, label: &str, actual: StatusCode, expected: StatusCode) -> Result<()> {
        if actual == expected {
            self.pass(label, format!("status={}", actual.as_u16()));
            Ok(())
        } else {
            let status = format!("FAIL (status={})", actual.as_u16());
            println!("{label}: {status}");
            self.steps.push(DemoStep {
                label: label.to_string(),
                status,
            });
            bail!("{label} failed")
        }
    }

    fn check_op(&mut self, label: &str, result: Result<()>, expect_ok: bool) -> Result<()> {
        match (result.is_ok(), expect_ok) {
            (true, true) => {
                self.pass(label, "status=OK");
                Ok(())
            }
            (false, false) => {
                let err = result.err().map(|e| e.to_string()).unwrap_or_default();
                let status = format!("PASS (denied: {err})");
                println!("{label}: {status}");
                self.steps.push(DemoStep {
                    label: label.to_string(),
                    status,
                });
                Ok(())
            }
            (true, false) => {
                let status = "FAIL (expected denied)".to_string();
                println!("{label}: {status}");
                self.steps.push(DemoStep {
                    label: label.to_string(),
                    status,
                });
                bail!("{label} failed")
            }
            (false, true) => {
                let err = result.err().map(|e| e.to_string()).unwrap_or_default();
                let status = format!("FAIL ({err})");
                println!("{label}: {status}");
                self.steps.push(DemoStep {
                    label: label.to_string(),
                    status,
                });
                bail!("{label} failed")
            }
        }
    }

    fn print_summary(&self) {
        println!("\nSummary");
        for step in &self.steps {
            println!("- {}: {}", step.label, step.status);
        }
    }
}
