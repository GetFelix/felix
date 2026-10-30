use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{ClientConfig, Connection};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];

#[tokio::test]
async fn notifications_demo_smoke() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    for tenant in ["t1", "t2"] {
        broker.register_tenant(tenant).await?;
        broker.register_namespace(tenant, "default").await?;
        broker
            .register_stream(tenant, "default", "alerts", StreamMetadata::default())
            .await?;
        broker
            .register_cache(tenant, "default", "last_alerts", CacheMetadata::default())
            .await?;
    }

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1", "t2"], Duration::from_secs(900))?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let transport = TransportConfig::default();
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        transport,
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        broker,
        config,
        demo_auth.auth,
    ));

    let client_t1 = build_client_for_tenant(&cert, addr, "t1", &demo_auth.tokens).await?;
    let client_t2 = build_client_for_tenant(&cert, addr, "t2", &demo_auth.tokens).await?;

    let publisher_t1 = client_t1.publisher().await?;
    let publisher_t2 = client_t2.publisher().await?;

    for idx in 1..=3 {
        let alert_t1 = format!("t1 alert #{idx}");
        let alert_t2 = format!("t2 alert #{idx}");
        publisher_t1
            .publish(
                "t1",
                "default",
                "alerts",
                alert_t1.as_bytes().to_vec(),
                AckMode::PerMessage,
            )
            .await?;
        publisher_t2
            .publish(
                "t2",
                "default",
                "alerts",
                alert_t2.as_bytes().to_vec(),
                AckMode::PerMessage,
            )
            .await?;
    }

    let payload = serde_json::to_vec(&vec![
        "t1 alert #1".to_string(),
        "t1 alert #2".to_string(),
        "t1 alert #3".to_string(),
    ])?;
    client_t1
        .cache_put(
            "t1",
            "default",
            "last_alerts",
            "alerts",
            payload.into(),
            None,
        )
        .await?;

    let snapshot = client_t1
        .cache_get("t1", "default", "last_alerts", "alerts")
        .await?;
    assert!(snapshot.is_some());

    let cross = client_t1.subscribe("t2", "default", "alerts").await;
    assert!(cross.is_err());

    server_task.abort();
    Ok(())
}

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

fn demo_auth_for_tenants(tenants: &[&str], ttl: Duration) -> Result<DemoAuthBundle> {
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    let mut tokens = HashMap::new();
    let mut jwks_per_tenant = HashMap::new();

    for tenant in tenants {
        let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = build_demo_jwks("demo-k1", &public_key)?;
        let key_material = TenantKeyMaterial {
            kid: "demo-k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: DEMO_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        };
        key_store.insert((*tenant).to_string(), key_material);
        jwks_per_tenant.insert((*tenant).to_string(), jwks);
    }

    let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", ttl, Arc::new(key_store));
    for tenant in tenants {
        let perms = vec![
            format!("tenant.manage:tenant:{tenant}"),
            format!("ns.manage:namespace:{tenant}/*"),
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
            format!("cache.read:cache:{tenant}/*/*"),
            format!("cache.write:cache:{tenant}/*/*"),
        ];
        let token = issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?;
        tokens.insert((*tenant).to_string(), token);
    }

    let key_store = Arc::new(
        felix_broker_service::serving::auth::ControlPlaneKeyStore::new(
            "http://127.0.0.1".to_string(),
            Arc::new(TenantKeyCache::default()),
        ),
    );
    for (tenant, jwks) in jwks_per_tenant {
        key_store.insert_jwks(&TenantId::new(&tenant), jwks);
    }
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));

    Ok(DemoAuthBundle { auth, tokens })
}

fn build_demo_jwks(kid: &str, public_key: &[u8; 32]) -> Result<Jwks> {
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Ok(Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    })
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

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}

async fn build_client_for_tenant(
    cert: &CertificateDer<'static>,
    addr: std::net::SocketAddr,
    tenant: &str,
    tokens: &HashMap<String, String>,
) -> Result<Connection> {
    let mut config = build_client_config(cert.clone())?;
    config.auth_tenant_id = Some(tenant.to_string());
    config.auth_token = tokens.get(tenant).cloned();
    Connection::connect(addr, "localhost", config).await
}
