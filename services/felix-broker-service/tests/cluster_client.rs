//! `ClusterClient` against a real broker, for the calls that need one to
//! mean anything: flushing unacknowledged publishes, keyed idempotent
//! publishes, asking who owns a shard, and cache requests.
//!
//! One broker, in memory, with no cluster behind it. Routing across brokers
//! is the cluster harness's to test; this proves the wiring.
//!
//! Run with `cargo test -p felix-broker-service --test cluster_client`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::{AckMode, ClientConfig, ClusterClient, ShardKind, ShardOwner};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use jsonwebtoken::Algorithm;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::time::timeout;

const TEST_PRIVATE_KEY: [u8; 32] = [11u8; 32];

/// A broker serving tenant `t1`, and a cluster client connected to it.
struct Running {
    cluster: Arc<ClusterClient>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start() -> Result<Running> {
    unsafe {
        std::env::set_var("FELIX_ACK_ON_COMMIT", "false");
    }
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;
    broker
        .register_cache("t1", "default", "sessions", CacheMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let (auth, token) = auth_fixture("t1");
    let task = tokio::spawn(felix_broker_service::serving::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        felix_broker_service::config::BrokerConfig::from_env()?,
        auth,
    ));

    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some(token);
    let cluster = Arc::new(ClusterClient::connect(&[addr], "localhost", config).await?);
    Ok(Running { cluster, task })
}

/// **`finish` flushes every unacknowledged publish.** An `AckMode::None`
/// publish returns once queued, so a process that exits straight after its
/// last one loses what is still queued unless it waits for this.
#[tokio::test]
#[serial]
async fn finish_flushes_unacknowledged_publishes() -> Result<()> {
    let running = start().await?;
    let mut subscription = running.cluster.subscribe("t1", "default", "orders").await?;

    const SENT: usize = 200;
    for n in 0..SENT {
        running
            .cluster
            .publish(
                "t1",
                "default",
                "orders",
                format!("{n}").into_bytes(),
                AckMode::None,
            )
            .await?;
    }
    running.cluster.finish().await?;

    let mut received = 0;
    while received < SENT {
        let event = timeout(Duration::from_secs(5), subscription.next_event())
            .await??
            .expect("the subscription stays open");
        assert_eq!(event.payload.as_ref(), format!("{received}").as_bytes());
        received += 1;
    }

    // Publishing through a finished client fails rather than queueing a
    // record nothing will send.
    let after = running
        .cluster
        .publish("t1", "default", "orders", b"late".to_vec(), AckMode::None)
        .await;
    assert!(after.is_err(), "a publish after finish was accepted");
    Ok(())
}

/// **A keyed idempotent publish lands, once each.** The broker routes the
/// batch by its key and checks the sequence against that shard.
#[tokio::test]
#[serial]
async fn a_keyed_idempotent_publish_lands_once() -> Result<()> {
    let running = start().await?;
    let mut subscription = running.cluster.subscribe("t1", "default", "orders").await?;
    let producer = running.cluster.idempotent_producer().await?;

    for (n, key) in ["alice", "bob", "alice"].into_iter().enumerate() {
        producer
            .publish_keyed(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from(key),
                format!("{n}").into_bytes(),
            )
            .await?;
    }
    producer
        .publish_batch_keyed(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"carol"),
            vec![b"3".to_vec(), b"4".to_vec()],
        )
        .await?;

    for expected in ["0", "1", "2", "3", "4"] {
        let event = timeout(Duration::from_secs(5), subscription.next_event())
            .await??
            .expect("the subscription stays open");
        assert_eq!(event.payload.as_ref(), expected.as_bytes());
    }
    assert!(
        timeout(Duration::from_millis(200), subscription.next_event())
            .await
            .is_err(),
        "a record landed twice"
    );
    Ok(())
}

/// **A broker with no cluster owns every shard it knows of.** It answers one
/// shard with no node id, and nothing for a name it does not know, including
/// a cache sharing a stream's name.
#[tokio::test]
#[serial]
async fn a_single_broker_reports_itself_as_every_owner() -> Result<()> {
    let running = start().await?;
    let client = running.cluster.client().await;
    assert!(client.supports_shard_owners());

    let owners = client
        .shard_owners("t1", "default", "orders", ShardKind::Stream)
        .await?;
    assert_eq!(
        owners,
        [ShardOwner {
            shard: 0,
            node_id: None,
            addr: None,
            generation: 0,
            unavailable: None,
        }]
    );
    assert!(
        client
            .shard_owners("t1", "default", "missing", ShardKind::Stream)
            .await?
            .is_empty()
    );
    assert!(
        client
            .shard_owners("t1", "default", "orders", ShardKind::Cache)
            .await?
            .is_empty(),
        "a stream is not a cache of the same name"
    );
    Ok(())
}

/// **Cache requests work through the cluster client.** With no cluster there
/// is no owner to route to, so they go through the broker in use, and a
/// counter is refused by a broker with nowhere to keep it rather than sent.
#[tokio::test]
#[serial]
async fn cache_requests_go_through_the_cluster_client() -> Result<()> {
    let running = start().await?;
    let cluster = &running.cluster;

    cluster
        .cache_put("t1", "default", "sessions", "alice", "online".into(), None)
        .await?;
    assert_eq!(
        cluster
            .cache_get("t1", "default", "sessions", "alice")
            .await?
            .as_deref(),
        Some(&b"online"[..])
    );
    assert_eq!(
        cluster
            .cache_delete("t1", "default", "sessions", "alice")
            .await?
            .as_deref(),
        Some(&b"online"[..])
    );
    assert_eq!(
        cluster
            .cache_get("t1", "default", "sessions", "alice")
            .await?,
        None
    );
    let err = cluster
        .counter_add("t1", "default", "sessions", "hits", 1)
        .await
        .expect_err("an in-memory broker keeps no counters");
    assert!(format!("{err:#}").contains("counters"), "{err:#}");
    Ok(())
}

fn auth_fixture(tenant_id: &str) -> (Arc<BrokerAuth>, String) {
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
    let mut key_materials = std::collections::HashMap::new();
    key_materials.insert(
        tenant_id.to_string(),
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
        Arc::new(key_materials),
    );
    let perms = vec![
        format!("stream.publish:stream:{tenant_id}/*/*"),
        format!("stream.subscribe:stream:{tenant_id}/*/*"),
        format!("cache.read:cache:{tenant_id}/*/*"),
        format!("cache.write:cache:{tenant_id}/*/*"),
    ];
    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:test", perms)
        .expect("mint token");
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    (Arc::new(BrokerAuth::with_key_store(key_store)), token)
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    Ok((server_config, cert_der))
}
