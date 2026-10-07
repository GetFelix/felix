//! The cache outliving the process that wrote it.
//!
//! The storage-level tests prove the log-backed cache keeps its entries; this
//! proves the broker is actually wired to it. Both matter: a correct
//! implementation nothing is connected to would pass the first and fail here.
//!
//! "Restart" is the broker and its storage torn down and rebuilt over the same
//! directory, which is what a process restart is from the cache's point of
//! view: nothing in memory survives, and only the log does.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{FelixTokenIssuer, Jwks, TenantId, TenantKeyCache, TenantKeyMaterial};
use felix_broker::{Broker, CacheMetadata};
use felix_broker_service::serving::{auth::BrokerAuth, quic};
use felix_client::{Client, ClientConfig};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const CACHE: &str = "sessions";
const QUEUE: &str = "jobs";

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

/// A broker serving over QUIC with its cache on `root`.
struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    tokens: HashMap<String, String>,
    server: Arc<QuicServer>,
    task: tokio::task::JoinHandle<Result<()>>,
    broker: Arc<Broker>,
}

async fn start(root: &std::path::Path) -> Result<Running> {
    let cache = LogCache::open(
        root,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .context("open the cache log")?;

    // Group state gets its own root under the same directory, exactly as the
    // broker binary arranges it, so this exercises the real layout.
    let dead_letters = felix_broker::DeadLetters::open(
        root.join("dead-letters"),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )?;
    let groups = felix_broker::ConsumerGroups::open(
        root.join("groups"),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )?;
    // Stream logs under their own root, as the binary arranges them. A
    // consumer group reads a durable stream, so the broker needs both.
    let storage = felix_broker::DurableStorage::open(
        root.join("streams"),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )?;
    let broker = Arc::new(
        Broker::new(Box::new(cache))
            .with_durable_storage(storage)
            .with_consumer_groups(
                Arc::new(groups),
                Arc::new(dead_letters),
                Duration::from_secs(30),
                // Low, so the poison-record test does not need many rounds.
                2,
            ),
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", CACHE, CacheMetadata::default())
        .await?;
    // A durable stream for the consumer-group tests. Durable because a group
    // reads a log, and an ephemeral stream has none.
    broker
        .register_stream(
            "t1",
            "default",
            QUEUE,
            felix_broker::StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await?;

    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let demo_auth = demo_auth_for_tenants(&["t1"], Duration::from_secs(900))?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        demo_auth.auth,
    ));

    Ok(Running {
        addr,
        cert,
        tokens: demo_auth.tokens,
        server,
        task,
        broker,
    })
}

impl Running {
    async fn client(&self) -> Result<Client> {
        self.client_as("t1").await
    }

    /// A client presenting the token minted under `token`.
    async fn client_as(&self, token: &str) -> Result<Client> {
        let mut config = build_client_config(self.cert.clone())?;
        config.auth_tenant_id = Some("t1".to_string());
        config.auth_token = self.tokens.get(token).cloned();
        // The group tests poll straight after an acked publish. By default the
        // broker acks a publish once it is queued, before the write, and a poll
        // that lands first misses it. Commit acks make the ack mean written.
        config.ack_on_commit = true;
        let client = Client::connect(self.addr, "localhost", config).await?;
        anyhow::ensure!(
            client.supports_ack_on_commit(),
            "the broker did not take commit acks"
        );
        Ok(client)
    }

    /// Stop serving, the way a process going away does.
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.server);
    }
}

/// **A cached value outlives the broker that stored it.** The claim the whole
/// design exists to make true: the cache is a log, so it is on disk, so a
/// restart does not lose it.
#[tokio::test]
async fn a_cached_value_survives_a_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    running
        .client()
        .await?
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:1",
            b"alice".to_vec().into(),
            None,
        )
        .await?;
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let value = restarted
        .client()
        .await?
        .cache_get("t1", "default", CACHE, "user:1")
        .await?;
    assert_eq!(
        value.as_deref(),
        Some(&b"alice"[..]),
        "the cache lost its entry across a restart",
    );
    restarted.stop().await;
    Ok(())
}

/// The newest write is the one that survives, not the first.
#[tokio::test]
async fn the_latest_value_is_the_one_that_survives() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    let client = running.client().await?;
    for value in ["one", "two", "three"] {
        client
            .cache_put(
                "t1",
                "default",
                CACHE,
                "user:1",
                value.as_bytes().to_vec().into(),
                None,
            )
            .await?;
    }
    drop(client);
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let value = restarted
        .client()
        .await?
        .cache_get("t1", "default", CACHE, "user:1")
        .await?;
    assert_eq!(value.as_deref(), Some(&b"three"[..]));
    restarted.stop().await;
    Ok(())
}

fn demo_auth_for_tenants(tenants: &[&str], ttl: Duration) -> Result<DemoAuthBundle> {
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    let mut tokens = HashMap::new();
    let mut jwks_per_tenant = HashMap::new();

    for tenant in tenants {
        let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = build_demo_jwks("demo-k1", &public_key)?;
        key_store.insert(
            (*tenant).to_string(),
            TenantKeyMaterial {
                kid: "demo-k1".to_string(),
                alg: jsonwebtoken::Algorithm::EdDSA,
                private_key: DEMO_PRIVATE_KEY,
                public_key,
                jwks: jwks.clone(),
            },
        );
        jwks_per_tenant.insert((*tenant).to_string(), jwks);
    }

    let issuer = FelixTokenIssuer::new("felix-auth", "felix-broker", ttl, Arc::new(key_store));
    for tenant in tenants {
        let perms = vec![
            format!("tenant.manage:tenant:{tenant}"),
            format!("ns.manage:namespace:{tenant}/*"),
            format!("cache.read:cache:{tenant}/*/*"),
            format!("cache.write:cache:{tenant}/*/*"),
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
            format!("group.manage:stream:{tenant}/*/*"),
        ];
        tokens.insert(
            (*tenant).to_string(),
            issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?,
        );
        // What a consumer holds: enough to work a group, not to operate one.
        let consumer = vec![
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
        ];
        tokens.insert(
            format!("{tenant}:consumer"),
            issuer.mint(&TenantId::new(*tenant), "p:consumer", consumer)?,
        );
        // A consumer scoped to one group of the queue.
        let worker = vec![
            format!("stream.publish:stream:{tenant}/*/*"),
            format!("stream.subscribe:stream:{tenant}/*/*"),
            format!("group.consume:group:{tenant}/default/{QUEUE}/workers"),
        ];
        tokens.insert(
            format!("{tenant}:worker"),
            issuer.mint(&TenantId::new(*tenant), "p:worker", worker)?,
        );
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

    Ok(DemoAuthBundle {
        auth: Arc::new(BrokerAuth::with_key_store(key_store)),
        tokens,
    })
}

fn build_demo_jwks(kid: &str, public_key: &[u8; 32]) -> Result<Jwks> {
    Ok(Jwks {
        keys: vec![felix_authz::Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: felix_authz::KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    })
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    ClientConfig::from_env_or_yaml(
        QuinnClientConfig::with_root_certificates(Arc::new(roots))?,
        None,
    )
}

/// **A delete outlives the broker that performed it.** The case #279 exists for:
/// until the wire carried a delete, this could only be written against the
/// storage layer, never end to end.
///
/// A tombstone that did not survive a restart would resurrect the value, which
/// is worse than never having deleted it — the caller was told it was gone.
#[tokio::test]
async fn a_deleted_key_stays_deleted_across_a_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    let client = running.client().await?;
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:1",
            b"alice".to_vec().into(),
            None,
        )
        .await?;
    let removed = client
        .cache_delete("t1", "default", CACHE, "user:1")
        .await?;
    assert_eq!(
        removed.as_deref(),
        Some(&b"alice"[..]),
        "a delete should report the value it removed",
    );
    assert_eq!(
        client.cache_get("t1", "default", CACHE, "user:1").await?,
        None,
        "the key should be gone before the restart",
    );
    running.stop().await;

    let restarted = start(dir.path()).await?;
    assert_eq!(
        restarted
            .client()
            .await?
            .cache_get("t1", "default", CACHE, "user:1")
            .await?,
        None,
        "the deleted value came back after a restart",
    );
    restarted.stop().await;
    Ok(())
}

/// Deleting a key that was never there is an answer, not a failure.
#[tokio::test]
async fn deleting_a_missing_key_reports_nothing_removed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;

    let removed = running
        .client()
        .await?
        .cache_delete("t1", "default", CACHE, "never-written")
        .await?;

    assert_eq!(removed, None);
    running.stop().await;
    Ok(())
}

/// A consumer group's position outlives the broker that recorded it, through
/// the same wiring the binary uses.
///
/// The storage-level tests prove the cursors persist; this proves the broker is
/// actually connected to them. A correct store nothing is wired to would pass
/// the first and fail here.
#[tokio::test]
async fn a_consumer_group_position_survives_a_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let running = start(dir.path()).await?;
    let groups = running
        .broker
        .consumer_groups()
        .expect("a durable broker keeps group state")
        .clone();
    groups
        .commit("t1", "default", "orders", 0, "workers", 900)
        .await?;
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let position = restarted
        .broker
        .consumer_groups()
        .expect("a durable broker keeps group state")
        .committed("t1", "default", "orders", 0, "workers")
        .await?;
    assert_eq!(
        position,
        Some(900),
        "the group lost its position across a restart",
    );
    restarted.stop().await;
    Ok(())
}

/// **An atomic commit, end to end.** The event reaches the queue's consumer
/// group at the commit's offset, the state carries that offset as its
/// version, and both survive a restart. An operation on another stream is
/// refused on the client, before anything is sent.
#[tokio::test]
async fn a_commit_is_seen_by_the_queue_and_the_state_together() -> Result<()> {
    use felix_client::{CommitError, CommitOp};

    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let receipt = client
        .commit(
            "t1",
            "default",
            b"job-1",
            vec![
                CommitOp::enqueue(QUEUE, "run job-1"),
                CommitOp::put(QUEUE, "job-1", "queued"),
            ],
        )
        .await?;
    let claimed = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].offset, receipt.offset);
    assert_eq!(claimed[0].payload, "run job-1");
    let state = client
        .state_get("t1", "default", QUEUE, b"job-1", "job-1")
        .await?;
    assert_eq!(state.value.as_deref(), Some(&b"queued"[..]));
    assert_eq!(state.version, Some(receipt.offset));

    let split = client
        .commit(
            "t1",
            "default",
            b"job-2",
            vec![
                CommitOp::enqueue(QUEUE, "run job-2"),
                CommitOp::put("elsewhere", "job-2", "queued"),
            ],
        )
        .await
        .expect_err("a second stream is a second log");
    assert!(matches!(
        split.downcast_ref::<CommitError>(),
        Some(CommitError::NotOnOwningShard { index: 1, .. })
    ));
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let state = restarted
        .client()
        .await?
        .state_get("t1", "default", QUEUE, b"job-1", "job-1")
        .await?;
    assert_eq!(state.value.as_deref(), Some(&b"queued"[..]));
    assert_eq!(state.version, Some(receipt.offset));
    restarted.stop().await;
    Ok(())
}

/// **A client consuming a queue, end to end.** Until the wire carried group
/// operations this could not be written at all: the machinery was reachable
/// only from inside the broker.
#[tokio::test]
async fn a_client_polls_and_acknowledges_a_consumer_group() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let publisher = client.publisher().await?;
    for job in ["one", "two", "three"] {
        publisher
            .publish(
                "t1",
                "default",
                QUEUE,
                job.as_bytes().to_vec(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
    }

    let claimed = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    let payloads: Vec<String> = claimed
        .iter()
        .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8"))
        .collect();
    assert_eq!(payloads, vec!["one", "two", "three"]);

    // A second poll sees nothing: the first consumer still holds them.
    assert!(
        client
            .group_poll("t1", "default", QUEUE, 0, "workers", 10)
            .await?
            .is_empty(),
        "a claimed record was handed out twice",
    );

    for record in &claimed {
        client
            .group_ack("t1", "default", QUEUE, 0, "workers", record.offset)
            .await?;
    }

    running.stop().await;

    // Finished work is not handed out again after a restart.
    let restarted = start(dir.path()).await?;
    let after = restarted
        .client()
        .await?
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert!(
        after.is_empty(),
        "the group repeated work it had already finished",
    );
    restarted.stop().await;
    Ok(())
}

/// A record handed back is redelivered at once, without waiting out the
/// visibility timeout.
#[tokio::test]
async fn a_nacked_record_is_polled_again() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"retry me".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    let first = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(first.len(), 1);

    client
        .group_nack("t1", "default", QUEUE, 0, "workers", first[0].offset)
        .await?;

    let again = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(again.len(), 1, "a handed-back record was not redelivered");
    assert_eq!(again[0].offset, first[0].offset);

    running.stop().await;
    Ok(())
}

/// Two groups over one stream are independent: each sees every record.
#[tokio::test]
async fn two_groups_each_see_every_record() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"shared".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    let first = client
        .group_poll("t1", "default", QUEUE, 0, "alpha", 10)
        .await?;
    let second = client
        .group_poll("t1", "default", QUEUE, 0, "beta", 10)
        .await?;

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1, "one group consumed another's work");
    running.stop().await;
    Ok(())
}

/// **The operator's loop, end to end.** A record that always fails is given up
/// on, listed, and can be put back once the reason is fixed.
#[tokio::test]
async fn a_dead_letter_can_be_listed_and_redriven() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"poison".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    // The harness broker gives up after two attempts. Hand it back each time.
    for _ in 0..2 {
        let claimed = client
            .group_poll("t1", "default", QUEUE, 0, "workers", 10)
            .await?;
        assert_eq!(claimed.len(), 1);
        client
            .group_nack("t1", "default", QUEUE, 0, "workers", claimed[0].offset)
            .await?;
    }

    // The third poll gives up on it and moves past.
    assert!(
        client
            .group_poll("t1", "default", QUEUE, 0, "workers", 10)
            .await?
            .is_empty(),
        "a record past its attempt bound was delivered again",
    );
    let dead = client
        .group_dead_letters("t1", "default", QUEUE, 0, "workers")
        .await?;
    assert_eq!(dead, vec![0]);

    // The reason it failed is fixed, so put it back.
    client
        .group_redrive("t1", "default", QUEUE, 0, "workers", 0)
        .await?;
    assert!(
        client
            .group_dead_letters("t1", "default", QUEUE, 0, "workers")
            .await?
            .is_empty(),
        "a redriven record is still listed as given up on",
    );

    let again = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(again.len(), 1, "the redriven record was not delivered");
    assert_eq!(again[0].attempts, 1, "its attempt count did not start over");

    running.stop().await;
    Ok(())
}

/// Discarding drops it from the list without delivering it again.
#[tokio::test]
async fn a_dead_letter_can_be_discarded() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"unprocessable".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;
    for _ in 0..2 {
        let claimed = client
            .group_poll("t1", "default", QUEUE, 0, "workers", 10)
            .await?;
        client
            .group_nack("t1", "default", QUEUE, 0, "workers", claimed[0].offset)
            .await?;
    }
    client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;

    client
        .group_discard("t1", "default", QUEUE, 0, "workers", 0)
        .await?;

    assert!(
        client
            .group_dead_letters("t1", "default", QUEUE, 0, "workers")
            .await?
            .is_empty(),
    );
    assert!(
        client
            .group_poll("t1", "default", QUEUE, 0, "workers", 10)
            .await?
            .is_empty(),
        "a discarded record was delivered again",
    );
    running.stop().await;
    Ok(())
}

/// Redriving something that was never given up on is refused. Reporting success
/// would have an operator waiting for a delivery that is not coming.
#[tokio::test]
async fn redriving_an_offset_that_is_not_a_dead_letter_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;

    let refused = running
        .client()
        .await?
        .group_redrive("t1", "default", QUEUE, 0, "workers", 41)
        .await;

    assert!(refused.is_err(), "a redrive of nothing reported success");
    running.stop().await;
    Ok(())
}

/// A waiting poll returns as soon as work arrives, rather than after its full
/// wait. This is what lets a consumer idle on one request instead of spinning.
#[tokio::test]
async fn a_waiting_poll_wakes_when_work_arrives() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;
    let publisher = running.client().await?.publisher().await?;

    let started = std::time::Instant::now();
    let waiting = tokio::spawn(async move {
        client
            .group_poll_wait(
                "t1",
                "default",
                QUEUE,
                0,
                "workers",
                10,
                Duration::from_secs(10),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    publisher
        .publish(
            "t1",
            "default",
            QUEUE,
            b"late".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    let claimed = waiting.await??;
    assert_eq!(claimed.len(), 1, "the waiting poll missed the record");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the poll waited out its budget instead of waking: {:?}",
        started.elapsed(),
    );
    running.stop().await;
    Ok(())
}

/// A wait that finds nothing answers empty rather than hanging or erroring.
#[tokio::test]
async fn a_waiting_poll_with_no_work_answers_empty() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;

    let started = std::time::Instant::now();
    let claimed = running
        .client()
        .await?
        .group_poll_wait(
            "t1",
            "default",
            QUEUE,
            0,
            "workers",
            10,
            Duration::from_millis(200),
        )
        .await?;

    assert!(claimed.is_empty());
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "the poll returned before its wait was up",
    );
    running.stop().await;
    Ok(())
}

/// **Reading a stream is not operating its groups.** A consumer holding only
/// `stream.subscribe` works the queue as it always could, but redriving or
/// discarding a dead letter takes `group.manage` (or `stream.manage`): either
/// one changes what every other consumer of the group sees.
#[tokio::test]
async fn a_consumer_works_a_group_but_cannot_operate_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let consumer = running.client_as("t1:consumer").await?;
    consumer
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"job".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    let claimed = consumer
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(claimed.len(), 1, "a consumer could not poll its group");
    consumer
        .group_ack("t1", "default", QUEUE, 0, "workers", claimed[0].offset)
        .await?;
    consumer
        .group_dead_letters("t1", "default", QUEUE, 0, "workers")
        .await?;

    let redrive = running
        .client_as("t1:consumer")
        .await?
        .group_redrive("t1", "default", QUEUE, 0, "workers", 0)
        .await;
    let redrive = format!(
        "{:#}",
        redrive.expect_err("a consumer redrove a dead letter")
    );
    assert_refused_for_authz(&redrive);
    let discard = running
        .client_as("t1:consumer")
        .await?
        .group_discard("t1", "default", QUEUE, 0, "workers", 0)
        .await;
    let discard = format!(
        "{:#}",
        discard.expect_err("a consumer discarded a dead letter")
    );
    assert_refused_for_authz(&discard);

    running.stop().await;
    Ok(())
}

/// **A group grant scopes a consumer to that group.** Its `stream.subscribe`
/// still reads the stream, but no longer works the stream's other groups.
#[tokio::test]
async fn a_consumer_granted_one_group_works_only_that_group() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let worker = running.client_as("t1:worker").await?;
    worker
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            QUEUE,
            b"job".to_vec(),
            felix_wire::AckMode::PerMessage,
        )
        .await?;

    let claimed = worker
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(claimed.len(), 1, "the worker could not poll its own group");
    worker
        .group_ack("t1", "default", QUEUE, 0, "workers", claimed[0].offset)
        .await?;

    let other = running
        .client_as("t1:worker")
        .await?
        .group_poll("t1", "default", QUEUE, 0, "billing", 10)
        .await;
    let other = format!("{:#}", other.expect_err("the worker polled another group"));
    assert!(
        other.contains("forbidden") || other.contains("closed"),
        "refused for the wrong reason: {other}"
    );

    running.stop().await;
    Ok(())
}

/// Offset 0 is not a dead letter, so a request that got past authorization is
/// refused with "not a dead letter". The broker answers an authorization failure
/// with `forbidden` and then closes the stream, and the client can see either.
fn assert_refused_for_authz(refusal: &str) {
    assert!(
        !refusal.contains("not a dead letter")
            && (refusal.contains("forbidden") || refusal.contains("closed")),
        "refused for the wrong reason: {refusal}"
    );
}

/// Acknowledging an offset the group never handed out is refused as the
/// consumer's mistake, not reported as a storage fault to retry.
#[tokio::test]
async fn an_ack_past_the_tail_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let refused = client
        .group_ack("t1", "default", QUEUE, 0, "workers", 1_000)
        .await;
    let refused = format!("{:#}", refused.expect_err("an ack past the tail was taken"));
    assert!(
        refused.contains("not handed out"),
        "refused for the wrong reason: {refused}",
    );

    running.stop().await;
    Ok(())
}

/// **A group's lifecycle, end to end.** Created at the tail it skips what came
/// before, a seek replays, describe reports where it stands, and a deleted
/// group starts again from the beginning.
#[tokio::test]
async fn a_group_is_created_moved_described_and_deleted() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;
    assert!(client.supports_group_admin());
    let publisher = client.publisher().await?;
    let publish = |payload: &'static [u8]| {
        publisher.publish(
            "t1",
            "default",
            QUEUE,
            payload.to_vec(),
            felix_wire::AckMode::PerMessage,
        )
    };
    publish(b"before").await?;

    let created = client
        .group_create(
            "t1",
            "default",
            QUEUE,
            0,
            "workers",
            felix_wire::StartPosition::Latest,
        )
        .await?;
    assert_eq!(
        created,
        felix_client::GroupPosition {
            offset: 1,
            moved: true
        }
    );
    publish(b"after").await?;
    let claimed = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].payload.as_ref(), b"after");

    let info = client
        .group_describe("t1", "default", QUEUE, 0, "workers")
        .await?;
    assert_eq!(info.committed, Some(1));
    assert_eq!((info.tail, info.in_flight, info.lag()), (2, 1, 1));

    // Created again on a restart of the consumer: left where it is.
    let again = client
        .group_create(
            "t1",
            "default",
            QUEUE,
            0,
            "workers",
            felix_wire::StartPosition::Earliest,
        )
        .await?;
    assert!(!again.moved);

    client
        .group_seek(
            "t1",
            "default",
            QUEUE,
            0,
            "workers",
            felix_wire::StartPosition::Earliest,
        )
        .await?;
    let replayed = client
        .group_poll("t1", "default", QUEUE, 0, "workers", 10)
        .await?;
    assert_eq!(replayed.len(), 2, "a seek to the start did not replay");

    assert!(
        client
            .group_delete("t1", "default", QUEUE, 0, "workers")
            .await?
    );
    let info = client
        .group_describe("t1", "default", QUEUE, 0, "workers")
        .await?;
    assert_eq!((info.committed, info.in_flight), (None, 0));
    running.stop().await;
    Ok(())
}

/// **A lease taken with a conditional put holds across a restart.** The client
/// reads a version, swaps on it, and the version it was given is still the
/// one the broker compares against after the log is replayed.
#[tokio::test]
async fn a_conditional_put_and_its_version_survive_a_restart() -> Result<()> {
    use felix_client::CacheCondition;

    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;
    let taken = client
        .cache_put_if(
            "t1",
            "default",
            CACHE,
            "lease:7",
            b"worker-1".to_vec().into(),
            None,
            CacheCondition::Absent,
        )
        .await?;
    assert!(taken.applied);
    let version = taken.version.expect("an applied put has a version");

    let contended = client
        .cache_put_if(
            "t1",
            "default",
            CACHE,
            "lease:7",
            b"worker-2".to_vec().into(),
            None,
            CacheCondition::Absent,
        )
        .await?;
    assert!(!contended.applied, "a second taker won the same lease");
    assert_eq!(contended.version, Some(version));
    drop(client);
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let client = restarted.client().await?;
    let found = client
        .cache_get_versioned("t1", "default", CACHE, "lease:7")
        .await?
        .expect("the lease is still held");
    assert_eq!(found.value.as_ref(), b"worker-1");
    assert_eq!(found.version, version);

    let released = client
        .cache_delete_if("t1", "default", CACHE, "lease:7", version)
        .await?;
    assert!(released.applied);
    assert_eq!(
        client.cache_get("t1", "default", CACHE, "lease:7").await?,
        None
    );
    drop(client);
    restarted.stop().await;
    Ok(())
}
