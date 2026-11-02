//! Lease tokens end to end over QUIC: minted on a conditional cache write,
//! scoped to what the caller's lease templates name, and ended at `exp`.
//!
//! Run with `cargo test -p felix-broker-service --test lease_tokens`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_broker_service::serving::lease::{DEFAULT_MAX_TTL, LeaseTokens};
use felix_client::{ClientConfig, Connection, LeaseToken};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{AckMode, CacheCondition};
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::time::timeout;

const TENANT_KEY: [u8; 32] = [7u8; 32];
const LEASE_SEED: [u8; 32] = [11u8; 32];
const NODE: &str = "node-a";

/// What a felixd node holds: the lease cache, and templates for its outputs.
/// Nothing that names a workload's output directly.
fn node_perms() -> Vec<String> {
    vec![
        "cache.write:cache:t1/prod/leases".to_string(),
        "cache.read:cache:t1/prod/leases".to_string(),
        "stream.publish:stream:t1/prod/wl.{leases}.out".to_string(),
        // Lets a lease token reach the mint, to be refused there.
        "cache.write:cache:t1/prod/leases/{leases}".to_string(),
    ]
}

struct Fixture {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    issuer: FelixTokenIssuer,
    server_task: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

impl Fixture {
    fn token(&self, perms: Vec<String>) -> Result<String> {
        Ok(self.issuer.mint(&TenantId::new("t1"), NODE, perms)?)
    }

    async fn connect(&self, token: String) -> Result<Connection> {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.clone())?;
        let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
        let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
        config.auth_tenant_id = Some("t1".to_string());
        config.auth_token = Some(token);
        Connection::connect(self.addr, "localhost", config).await
    }
}

async fn start(lease_keys: bool) -> Result<Fixture> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "prod").await?;
    for stream in ["wl.w7.out", "wl.w8.out"] {
        broker
            .register_stream("t1", "prod", stream, StreamMetadata::default())
            .await?;
    }
    broker
        .register_cache("t1", "prod", "leases", CacheMetadata::default())
        .await?;

    let public_key = Ed25519SigningKey::from_bytes(&TENANT_KEY)
        .verifying_key()
        .to_bytes();
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
        "t1".to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TENANT_KEY,
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
    key_store.insert_jwks(&TenantId::new("t1"), jwks);
    let lease = lease_keys
        .then(|| LeaseTokens::from_keys(&STANDARD.encode(LEASE_SEED), DEFAULT_MAX_TTL))
        .transpose()?
        .map(Arc::new);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store).with_lease_tokens(lease));

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
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(felix_broker_service::serving::quic::serve(
        server, broker, config, auth,
    ));
    Ok(Fixture {
        addr,
        cert: cert_der,
        issuer,
        server_task,
    })
}

async fn claim(node: &Connection, key: &str, ttl_ms: u64) -> Result<felix_client::LeaseWrite> {
    node.cache_put_if_lease(
        "t1",
        "prod",
        "leases",
        key,
        Bytes::from_static(b"node-a"),
        Some(ttl_ms),
        CacheCondition::Absent,
    )
    .await
}

async fn publish(connection: &Connection, stream: &str) -> Result<()> {
    connection
        .publisher()
        .await?
        .publish_batch(
            "t1",
            "prod",
            stream,
            vec![b"out".to_vec()],
            AckMode::PerBatch,
        )
        .await
        .map(drop)
}

fn token(write: &felix_client::LeaseWrite) -> LeaseToken {
    write
        .token
        .clone()
        .expect("an applied claim carries a token")
}

#[tokio::test]
#[serial]
async fn the_holder_gets_a_token_for_its_own_output_only() -> Result<()> {
    let fixture = start(true).await?;
    let node = fixture.connect(fixture.token(node_perms())?).await?;

    let claimed = claim(&node, "w7", 30_000).await?;
    assert!(claimed.result.applied);
    assert!(claimed.token_refused.is_none());
    let lease = token(&claimed);
    assert!(!format!("{lease:?}").contains(&lease.token), "redacted");

    // The node's own token cannot write the output; the lease token can, and
    // only for the key it holds.
    assert!(publish(&node, "wl.w7.out").await.is_err());
    let holder = node.with_identity_token("t1", lease.token.clone()).await?;
    publish(&holder, "wl.w7.out").await?;
    let holder = node.with_identity_token("t1", lease.token.clone()).await?;
    assert!(publish(&holder, "wl.w8.out").await.is_err());

    // A second claim loses the write, and with it any token.
    let lost = claim(&node, "w7", 30_000).await?;
    assert!(!lost.result.applied);
    assert!(lost.token.is_none());

    // A renewal at the held version gets a fresh token.
    let renewed = node
        .cache_put_if_lease(
            "t1",
            "prod",
            "leases",
            "w7",
            Bytes::from_static(b"node-a"),
            Some(30_000),
            CacheCondition::Version(claimed.result.version.expect("version")),
        )
        .await?;
    assert!(renewed.result.applied);
    assert_ne!(token(&renewed).token, lease.token);
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_caller_without_templates_or_with_a_lease_token_is_refused_before_the_write() -> Result<()>
{
    let fixture = start(true).await?;
    let plain = fixture
        .connect(fixture.token(vec![
            "cache.write:cache:t1/prod/leases".to_string(),
            "cache.read:cache:t1/prod/leases".to_string(),
        ])?)
        .await?;
    assert!(claim(&plain, "w7", 30_000).await.is_err());
    assert!(
        plain
            .cache_get_versioned("t1", "prod", "leases", "w7")
            .await?
            .is_none(),
        "a refused mint writes nothing"
    );

    let node = fixture.connect(fixture.token(node_perms())?).await?;
    let lease = token(&claim(&node, "w8", 30_000).await?);
    let holder = node.with_identity_token("t1", lease.token).await?;
    assert!(claim(&holder, "w9", 30_000).await.is_err());
    assert!(
        node.cache_get_versioned("t1", "prod", "leases", "w9")
            .await?
            .is_none(),
        "a lease token minting writes nothing"
    );
    Ok(())
}

/// What keeps a stale holder from writing after its lease: the session the
/// token opened ends at `exp`, as a revoked one does.
#[tokio::test]
#[serial]
async fn a_lease_token_session_ends_at_exp() -> Result<()> {
    let fixture = start(true).await?;
    let node = fixture.connect(fixture.token(node_perms())?).await?;
    let lease = token(&claim(&node, "w7", 1_500).await?);
    let holder = node.with_identity_token("t1", lease.token.clone()).await?;
    let publisher = holder.publisher().await?;
    publisher
        .publish_batch(
            "t1",
            "prod",
            "wl.w7.out",
            vec![b"held".to_vec()],
            AckMode::PerBatch,
        )
        .await?;

    // exp is at most 1.5 s away, and the leeway 2 s more.
    let ended = timeout(Duration::from_secs(8), async {
        loop {
            let sent = publisher
                .publish_batch(
                    "t1",
                    "prod",
                    "wl.w7.out",
                    vec![b"late".to_vec()],
                    AckMode::PerBatch,
                )
                .await;
            if sent.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(ended.is_ok(), "a lease token's stream outlived its exp");
    assert!(
        node.with_identity_token("t1", lease.token).await.is_err(),
        "an expired lease token opens nothing new"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_broker_without_the_key_neither_mints_nor_accepts() -> Result<()> {
    let minting = start(true).await?;
    let node = minting.connect(minting.token(node_perms())?).await?;
    let lease = token(&claim(&node, "w7", 30_000).await?);

    let keyless = start(false).await?;
    let node = keyless.connect(keyless.token(node_perms())?).await?;
    let err = claim(&node, "w7", 30_000)
        .await
        .expect_err("not advertised");
    assert!(err.to_string().contains("lease"), "{err:#}");
    assert!(
        node.with_identity_token("t1", lease.token).await.is_err(),
        "a broker without the key refuses lease tokens"
    );
    Ok(())
}
