//! `shard_inspect` and `subscriptions_list` against a real broker over QUIC:
//! the feature is advertised, an operator token is answered, and a tenant
//! token is refused.
//!
//! One broker, in memory, with no cluster behind it. A cluster's fence and
//! replica positions are the felixctl cluster test's to cover.
//!
//! Run with `cargo test -p felix-broker-service --test inspect`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::{Client, ClientConfig, ShardKind};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const TEST_PRIVATE_KEY: [u8; 32] = [13u8; 32];

struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    issuer: FelixTokenIssuer,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Running {
    async fn client(&self, perms: &[&str]) -> Result<Client> {
        self.client_as("p:operator", perms).await
    }

    async fn client_as(&self, subject: &str, perms: &[&str]) -> Result<Client> {
        let token = self.issuer.mint(
            &TenantId::new("ops"),
            subject,
            perms.iter().map(|perm| perm.to_string()).collect(),
        )?;
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.clone())?;
        let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
        let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
        config.auth_tenant_id = Some("ops".to_string());
        config.auth_token = Some(token);
        Client::connect(self.addr, "localhost", config).await
    }
}

/// A broker holding `acme/default/orders` and `ops/default/events`, verifying tokens for tenant `ops`.
async fn start() -> Result<Running> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("acme").await?;
    broker.register_namespace("acme", "default").await?;
    broker
        .register_stream("acme", "default", "orders", StreamMetadata::default())
        .await?;
    broker.register_tenant("ops").await?;
    broker.register_namespace("ops", "default").await?;
    broker
        .register_stream("ops", "default", "events", StreamMetadata::default())
        .await?;

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "ops".to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(keys),
    );
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new("ops"), jwks);
    let task = tokio::spawn(felix_broker_service::serving::quic::serve(
        server,
        broker,
        felix_broker_service::config::BrokerConfig::from_env()?,
        Arc::new(BrokerAuth::with_key_store(key_store)),
    ));
    Ok(Running {
        addr,
        cert: cert_der,
        issuer,
        task,
    })
}

/// An operator inspects another tenant's shard. A broker with no cluster
/// leads everything it holds and reports no generation or replicas.
#[tokio::test]
async fn an_operator_inspects_any_tenants_shard() -> Result<()> {
    let running = start().await?;
    let client = running.client(&["node.view:cluster:*"]).await?;
    assert!(client.supports_inspect());

    let view = client
        .inspect_shard(ShardKind::Stream, "acme", "default", "orders", 0)
        .await?;
    assert_eq!(view.role, "leader");
    assert_eq!(view.phase, "active");
    assert!(view.serving, "{view:?}");
    assert_eq!(view.shards, 1);
    assert!(view.replicas.is_empty());

    let missing = client
        .inspect_shard(ShardKind::Stream, "acme", "default", "missing", 0)
        .await?;
    assert_eq!(missing.shards, 0);
    assert_eq!(missing.reason.as_deref(), Some("not_assigned_here"));
    Ok(())
}

/// A tenant's own grants, however wide, do not reach the cluster.
#[tokio::test]
async fn a_tenant_token_is_refused() -> Result<()> {
    let running = start().await?;
    let client = running
        .client(&["stream.manage:stream:acme/*/*", "node.view:*"])
        .await?;
    let refused = client
        .inspect_shard(ShardKind::Stream, "acme", "default", "orders", 0)
        .await
        .expect_err("a tenant token was answered");
    assert!(
        format!("{refused:#}").contains("node.view:cluster:*"),
        "{refused:#}"
    );
    Ok(())
}

/// A subscriber shows up with its connection and the principal it
/// authenticated as, and only to a token with cluster scope.
#[tokio::test]
async fn an_operator_lists_subscriptions_with_their_owners() -> Result<()> {
    let running = start().await?;
    let reader = running
        .client_as("p:reader", &["stream.subscribe:stream:ops/*/*"])
        .await?;
    let _subscription = reader.subscribe("ops", "default", "events").await?;

    let refused = reader
        .list_subscriptions(Default::default(), None, None)
        .await
        .expect_err("a tenant token was answered");
    assert!(
        format!("{refused:#}").contains("node.view:cluster:*"),
        "{refused:#}"
    );

    let operator = running.client(&["node.view:cluster:*"]).await?;
    // Registered before `Subscribed` is answered, but the owner is attached
    // just after, so give it a moment.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let page = loop {
        let page = operator
            .list_subscriptions(Default::default(), None, None)
            .await?;
        if page.subscriptions.iter().any(|s| s.principal.is_some())
            || tokio::time::Instant::now() > deadline
        {
            break page;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let [listed] = page.subscriptions.as_slice() else {
        panic!("one subscription: {page:?}");
    };
    assert_eq!(
        (listed.tenant_id.as_str(), listed.stream.as_str()),
        ("ops", "events")
    );
    assert_eq!(listed.principal.as_deref(), Some("p:reader"));
    assert!(listed.connection.is_some(), "{listed:?}");
    assert!(
        listed
            .peer
            .as_deref()
            .is_some_and(|peer| peer.starts_with("127.0.0.1:"))
    );
    assert_eq!(listed.policy, "drop_new");
    assert!(page.next_cursor.is_none());
    Ok(())
}
