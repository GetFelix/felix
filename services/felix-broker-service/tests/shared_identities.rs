//! Several principals over one client's connections (`Client::with_identity`).
//!
//! A gateway holds one `Client` and acts for each of its users through a
//! handle that shares the connections. What has to hold is that the broker
//! still checks every request against the token of the user it is for: one
//! user's grants never cover another's publish, subscribe, cache or group
//! request, even though their streams sit on the same connections, and one
//! user's token expiring or being cut off leaves the others alone.
//!
//! Run with `cargo test -p felix-broker-service --test shared_identities`.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use felix_authz::{
    FelixClaims, FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, CacheMetadata, StreamMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{Client, ClientConfig, Subscription, TokenFuture, TokenProvider};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const PRIVATE_KEY: [u8; 32] = [11u8; 32];
const KID: &str = "k1";
const TENANT: &str = "t1";
const NS: &str = "default";
const CACHE: &str = "sessions";
const QUEUE: &str = "jobs";

/// What each user may do: its own feed, its own keys in the shared cache, and
/// its own group on the shared queue.
fn user_perms(user: &str) -> Vec<String> {
    vec![
        format!("stream.publish:stream:{TENANT}/{NS}/{user}-feed"),
        format!("stream.subscribe:stream:{TENANT}/{NS}/{user}-feed"),
        format!("cache.read:cache:{TENANT}/{NS}/{CACHE}/{user}:*"),
        format!("cache.write:cache:{TENANT}/{NS}/{CACHE}/{user}:*"),
        format!("stream.publish:stream:{TENANT}/{NS}/{QUEUE}"),
        format!("group.consume:group:{TENANT}/{NS}/{QUEUE}/{user}"),
    ]
}

struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    issuer: FelixTokenIssuer,
    task: tokio::task::JoinHandle<Result<()>>,
    _dir: tempfile::TempDir,
}

impl Running {
    async fn start() -> Result<Self> {
        Self::start_with(felix_broker_service::config::BrokerConfig::from_env()?).await
    }

    async fn start_with(config: felix_broker_service::config::BrokerConfig) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let log = || LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let cache = LogCache::open(root, log()).context("open the cache log")?;
        let dead_letters = felix_broker::DeadLetters::open(root.join("dead-letters"), log())?;
        let groups = felix_broker::ConsumerGroups::open(root.join("groups"), log())?;
        let storage = felix_broker::DurableStorage::open(root.join("streams"), log())?;
        let broker = Arc::new(
            Broker::new(Box::new(cache))
                .with_durable_storage(storage)
                .with_consumer_groups(
                    Arc::new(groups),
                    Arc::new(dead_letters),
                    Duration::from_secs(30),
                    3,
                ),
        );
        broker.register_tenant(TENANT).await?;
        broker.register_namespace(TENANT, NS).await?;
        broker
            .register_cache(TENANT, NS, CACHE, CacheMetadata::default())
            .await?;
        for stream in ["alice-feed", "bob-feed", "carol-feed", QUEUE] {
            broker
                .register_stream(
                    TENANT,
                    NS,
                    stream,
                    StreamMetadata {
                        durable: true,
                        shards: 1,
                        ..Default::default()
                    },
                )
                .await?;
        }

        let signing_key = Ed25519SigningKey::from_bytes(&PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = Jwks {
            keys: vec![felix_authz::Jwk {
                kty: "OKP".to_string(),
                kid: KID.to_string(),
                alg: "EdDSA".to_string(),
                use_field: felix_authz::KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some(URL_SAFE_NO_PAD.encode(public_key)),
            }],
        };
        let mut materials = HashMap::new();
        materials.insert(
            TENANT.to_string(),
            TenantKeyMaterial {
                kid: KID.to_string(),
                alg: jsonwebtoken::Algorithm::EdDSA,
                private_key: PRIVATE_KEY,
                public_key,
                jwks: jwks.clone(),
            },
        );
        let issuer = FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(900),
            Arc::new(materials),
        );
        let key_store = Arc::new(
            felix_broker_service::serving::auth::ControlPlaneKeyStore::new(
                "http://127.0.0.1".to_string(),
                Arc::new(TenantKeyCache::default()),
            ),
        );
        key_store.insert_jwks(&TenantId::new(TENANT), jwks);
        let auth = Arc::new(BrokerAuth::with_key_store(key_store));

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
        let task = tokio::spawn(quic::serve(server, broker, config, auth));
        Ok(Self {
            addr,
            cert: cert_der,
            issuer,
            task,
            _dir: dir,
        })
    }

    fn token(&self, user: &str) -> Result<String> {
        Ok(self
            .issuer
            .mint(&TenantId::new(TENANT), user, user_perms(user))?)
    }

    /// The gateway's own client. Its token can publish nowhere a user can.
    async fn gateway(&self) -> Result<Client> {
        self.gateway_with(|_| {}).await
    }

    async fn gateway_with(&self, tune: impl FnOnce(&mut ClientConfig)) -> Result<Client> {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.clone())?;
        let mut config = ClientConfig::from_env_or_yaml(
            QuinnClientConfig::with_root_certificates(Arc::new(roots))?,
            None,
        )?;
        config.auth_tenant_id = Some(TENANT.to_string());
        config.auth_token = Some(self.issuer.mint(
            &TenantId::new(TENANT),
            "gateway",
            vec![format!("cache.read:cache:{TENANT}/{NS}/{CACHE}/gateway:*")],
        )?);
        // A poll right after an acked publish must see it.
        config.ack_on_commit = true;
        tune(&mut config);
        Client::connect(self.addr, "localhost", config).await
    }

    async fn user(&self, gateway: &Client, user: &str) -> Result<Client> {
        gateway.with_identity_token(TENANT, self.token(user)?).await
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A token the broker accepts in every way except that it expired an hour ago.
fn expired_token(user: &str) -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let claims = FelixClaims {
        iss: "felix-auth".to_string(),
        aud: "felix-broker".to_string(),
        sub: user.to_string(),
        tid: TENANT.to_string(),
        exp: now - 3600,
        iat: now - 4500,
        jti: None,
        perms: user_perms(user),
    };
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    header.kid = Some(KID.to_string());
    let der = Ed25519SigningKey::from_bytes(&PRIVATE_KEY).to_pkcs8_der()?;
    Ok(jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_ed_der(der.as_bytes()),
    )?)
}

/// A user's token source, which the test can expire or cut off.
struct UserTokens(Mutex<Result<String, String>>);

impl UserTokens {
    fn set(&self, next: Result<String, String>) {
        *self.0.lock().unwrap() = next;
    }
}

impl TokenProvider for UserTokens {
    fn token(&self) -> TokenFuture<'_> {
        let current = self.0.lock().unwrap().clone();
        Box::pin(async move { current.map_err(anyhow::Error::msg) })
    }
}

async fn publish(client: &Client, stream: &str, payload: &str) -> Result<()> {
    client
        .publisher()
        .await?
        .publish(
            TENANT,
            NS,
            stream,
            payload.as_bytes().to_vec(),
            AckMode::PerMessage,
        )
        .await
        .map(|_| ())
}

async fn next_payload(sub: &mut Subscription) -> Result<String> {
    let event = tokio::time::timeout(Duration::from_secs(5), sub.next_event())
        .await
        .context("no event in time")??
        .context("subscription ended")?;
    Ok(String::from_utf8(event.payload.to_vec())?)
}

#[tokio::test]
async fn a_user_publishes_only_where_its_own_token_allows() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let held = gateway.connection_count();
    let alice = running.user(&gateway, "alice").await?;
    let bob = running.user(&gateway, "bob").await?;
    assert_eq!(
        gateway.connection_count(),
        held,
        "a user added a connection"
    );

    publish(&alice, "alice-feed", "a").await?;
    publish(&bob, "bob-feed", "b").await?;
    assert!(
        publish(&alice, "bob-feed", "from alice").await.is_err(),
        "alice published to bob's stream"
    );
    assert!(
        publish(&gateway, "alice-feed", "from the gateway")
            .await
            .is_err(),
        "the gateway's own token published to alice's stream"
    );
    // Alice's refusal is hers alone.
    publish(&bob, "bob-feed", "b2").await?;
    Ok(())
}

#[tokio::test]
async fn a_user_subscribes_only_to_its_own_streams_and_gets_only_its_events() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let alice = running.user(&gateway, "alice").await?;
    let bob = running.user(&gateway, "bob").await?;

    assert!(
        alice.subscribe(TENANT, NS, "bob-feed").await.is_err(),
        "alice subscribed to bob's stream"
    );
    assert!(
        gateway.subscribe(TENANT, NS, "bob-feed").await.is_err(),
        "the gateway's own token subscribed to bob's stream"
    );

    let mut alice_sub = alice.subscribe(TENANT, NS, "alice-feed").await?;
    let mut bob_sub = bob.subscribe(TENANT, NS, "bob-feed").await?;
    publish(&bob, "bob-feed", "b1").await?;
    publish(&alice, "alice-feed", "a1").await?;
    publish(&bob, "bob-feed", "b2").await?;

    assert_eq!(next_payload(&mut alice_sub).await?, "a1");
    assert_eq!(next_payload(&mut bob_sub).await?, "b1");
    assert_eq!(next_payload(&mut bob_sub).await?, "b2");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), alice_sub.next_event())
            .await
            .is_err(),
        "alice's subscription delivered something that was not hers"
    );
    Ok(())
}

#[tokio::test]
async fn a_user_reads_and_writes_only_its_own_cache_keys() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let alice = running.user(&gateway, "alice").await?;
    let bob = running.user(&gateway, "bob").await?;

    alice
        .cache_put(TENANT, NS, CACHE, "alice:1", "a".into(), None)
        .await?;
    bob.cache_put(TENANT, NS, CACHE, "bob:1", "b".into(), None)
        .await?;

    // A refused request ends the stream it came on, as on any client, so
    // each refusal below is the last request on its handle.
    assert!(
        alice
            .cache_put(TENANT, NS, CACHE, "bob:1", "x".into(), None)
            .await
            .is_err(),
        "alice wrote bob's key"
    );
    assert!(
        gateway
            .cache_get(TENANT, NS, CACHE, "alice:1")
            .await
            .is_err(),
        "the gateway's own token read alice's key"
    );
    assert_eq!(
        bob.cache_get(TENANT, NS, CACHE, "bob:1").await?.as_deref(),
        Some(&b"b"[..]),
        "bob's value changed, or alice's refusal reached bob's stream"
    );
    assert!(
        bob.cache_get(TENANT, NS, CACHE, "alice:1").await.is_err(),
        "bob read alice's key"
    );

    let alice = running.user(&gateway, "alice").await?;
    assert_eq!(
        alice
            .cache_get(TENANT, NS, CACHE, "alice:1")
            .await?
            .as_deref(),
        Some(&b"a"[..])
    );
    Ok(())
}

#[tokio::test]
async fn a_user_works_only_its_own_consumer_group() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let alice = running.user(&gateway, "alice").await?;
    let bob = running.user(&gateway, "bob").await?;
    publish(&alice, QUEUE, "job").await?;

    assert!(
        alice
            .group_poll(TENANT, NS, QUEUE, 0, "bob", 10)
            .await
            .is_err(),
        "alice polled bob's group"
    );
    assert!(
        bob.group_poll(TENANT, NS, QUEUE, 0, "alice", 10)
            .await
            .is_err(),
        "bob polled alice's group"
    );
    let alice_jobs = alice.group_poll(TENANT, NS, QUEUE, 0, "alice", 10).await?;
    assert_eq!(alice_jobs.len(), 1);
    assert!(
        bob.group_ack(TENANT, NS, QUEUE, 0, "alice", alice_jobs[0].offset)
            .await
            .is_err(),
        "bob acknowledged alice's record"
    );
    // Bob's own group reads the same queue from the start.
    assert_eq!(
        bob.group_poll(TENANT, NS, QUEUE, 0, "bob", 10).await?.len(),
        1
    );
    alice
        .group_ack(TENANT, NS, QUEUE, 0, "alice", alice_jobs[0].offset)
        .await?;
    Ok(())
}

#[tokio::test]
async fn one_users_token_expiring_leaves_the_others_alone() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let carol_tokens = Arc::new(UserTokens(Mutex::new(Ok(running.token("carol")?))));
    let carol = gateway
        .with_identity(TENANT, Arc::clone(&carol_tokens) as Arc<dyn TokenProvider>)
        .await?;
    let bob = running.user(&gateway, "bob").await?;
    let mut bob_sub = bob.subscribe(TENANT, NS, "bob-feed").await?;
    let held = gateway.connection_count();

    carol_tokens.set(Ok(expired_token("carol")?));
    assert!(
        carol.subscribe(TENANT, NS, "carol-feed").await.is_err(),
        "a stream opened on an expired token"
    );
    assert!(
        gateway
            .with_identity(TENANT, Arc::clone(&carol_tokens) as Arc<dyn TokenProvider>)
            .await
            .is_err(),
        "an identity was built on an expired token"
    );

    assert_eq!(
        gateway.connection_count(),
        held,
        "a refusal cost a connection"
    );
    publish(&bob, "bob-feed", "still here").await?;
    assert_eq!(next_payload(&mut bob_sub).await?, "still here");
    let mut fresh = bob.subscribe(TENANT, NS, "bob-feed").await?;
    publish(&bob, "bob-feed", "again").await?;
    assert_eq!(next_payload(&mut fresh).await?, "again");
    Ok(())
}

#[tokio::test]
async fn one_user_cut_off_leaves_the_others_alone() -> Result<()> {
    let running = Running::start().await?;
    let gateway = running.gateway().await?;
    let carol_tokens = Arc::new(UserTokens(Mutex::new(Ok(running.token("carol")?))));
    let carol = gateway
        .with_identity(TENANT, Arc::clone(&carol_tokens) as Arc<dyn TokenProvider>)
        .await?;
    let alice = running.user(&gateway, "alice").await?;

    // What a refresh returns once the control plane has revoked the chain.
    carol_tokens.set(Err("refresh token revoked".to_string()));
    assert!(
        carol.subscribe(TENANT, NS, "carol-feed").await.is_err(),
        "a stream opened without a token"
    );

    alice
        .cache_put(TENANT, NS, CACHE, "alice:after", "ok".into(), None)
        .await?;
    let mut alice_sub = alice.subscribe(TENANT, NS, "alice-feed").await?;
    publish(&alice, "alice-feed", "a").await?;
    assert_eq!(next_payload(&mut alice_sub).await?, "a");
    Ok(())
}

/// The subscription cap is per user on a shared connection: alice at her cap
/// is refused, and bob, on the same connection, still subscribes.
#[tokio::test]
async fn one_user_at_the_subscription_cap_leaves_the_others_room() -> Result<()> {
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.max_subscriptions_per_conn = 2;
    let running = Running::start_with(config).await?;
    // One event connection, so every subscription shares it.
    let gateway = running
        .gateway_with(|config| config.event_conn_pool = 1)
        .await?;
    let alice = running.user(&gateway, "alice").await?;
    let bob = running.user(&gateway, "bob").await?;

    let _a1 = alice.subscribe(TENANT, NS, "alice-feed").await?;
    let a2 = alice.subscribe(TENANT, NS, "alice-feed").await?;
    let refused = alice
        .subscribe(TENANT, NS, "alice-feed")
        .await
        .err()
        .context("alice went past her cap")?;
    assert!(
        format!("{refused:#}").contains("max subscriptions per connection exceeded"),
        "{refused:#}"
    );

    let mut b1 = bob.subscribe(TENANT, NS, "bob-feed").await?;
    let _b2 = bob.subscribe(TENANT, NS, "bob-feed").await?;
    publish(&bob, "bob-feed", "b").await?;
    assert_eq!(next_payload(&mut b1).await?, "b");

    // A subscription alice drops gives her slot back. The broker notices a
    // dropped subscription when it next writes to it, hence the publishes.
    drop(a2);
    let mut freed = None;
    for _ in 0..50 {
        publish(&alice, "alice-feed", "a").await?;
        if let Ok(sub) = alice.subscribe(TENANT, NS, "alice-feed").await {
            freed = Some(sub);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    freed.context("alice's dropped subscription never freed its slot")?;
    Ok(())
}
