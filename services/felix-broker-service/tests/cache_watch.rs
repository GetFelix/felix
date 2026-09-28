//! A cache you can subscribe to, end to end.
//!
//! The hub and storage tests prove the pieces; this proves the wiring: a
//! client watch over QUIC sees exactly the changes its filter names, in the
//! shard's write order, with the log offsets that make resume and duplicate
//! detection real. The headline is `a_resumed_watch_is_gapless_under_
//! concurrent_writes` — the register-before-read join under real concurrency.
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
use felix_client::{CacheWatchFilter, CacheWatchItem, Client, ClientConfig};
use felix_storage::LogCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];
const CACHE: &str = "sessions";
// A liveness bound, not a performance assertion: generous, because this
// binary's join tests run under the whole workspace's parallel load in CI.
const RECV_TIMEOUT: Duration = Duration::from_secs(30);

struct DemoAuthBundle {
    auth: Arc<BrokerAuth>,
    tokens: HashMap<String, String>,
}

struct Running {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    tokens: HashMap<String, String>,
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    task: tokio::task::JoinHandle<Result<()>>,
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
    start_with(Box::new(cache)).await
}

/// A broker whose cache has no log, for proving watches are refused rather
/// than faked over it.
async fn start_ephemeral() -> Result<Running> {
    start_with(Box::new(felix_storage::EphemeralCache::new())).await
}

async fn start_with(cache: Box<dyn felix_storage::StorageApi + Send>) -> Result<Running> {
    let broker = Arc::new(Broker::new(cache));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", CACHE, CacheMetadata::default())
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
        broker,
        task,
    })
}

impl Running {
    async fn client(&self) -> Result<Client> {
        let mut config = build_client_config(self.cert.clone())?;
        config.auth_tenant_id = Some("t1".to_string());
        config.auth_token = self.tokens.get("t1").cloned();
        Client::connect(self.addr, "localhost", config).await
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        drop(self.server);
    }
}

async fn next_item(watch: &mut felix_client::CacheWatch) -> CacheWatchItem {
    tokio::time::timeout(RECV_TIMEOUT, watch.recv())
        .await
        .expect("the watch went quiet")
        .expect("the watch ended")
}

fn change(item: CacheWatchItem) -> felix_client::CacheChange {
    match item {
        CacheWatchItem::Change(change) => change,
        CacheWatchItem::Lagged { resume_from } => {
            panic!("the watch lagged unexpectedly (resume_from {resume_from})")
        }
        CacheWatchItem::ShardMoved(moved) => panic!("the shard moved unexpectedly: {moved:?}"),
    }
}

/// **A watch on a key receives that key's updates and no others.** The
/// acceptance line of #348, over the real wire: the filtered key's put and
/// delete arrive in order with their offsets, and the neighbour's write never
/// does — proven by a sentinel write on the watched key *after* it.
#[tokio::test]
async fn a_key_watch_sees_its_key_and_no_others() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let mut watch = client
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("user:1".to_string()),
            None,
        )
        .await?;
    assert!(!watch.resnapshot(), "a live watch was resnapshotted");

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
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:2",
            b"bob".to_vec().into(),
            None,
        )
        .await?;
    client
        .cache_delete("t1", "default", CACHE, "user:1")
        .await?;
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:1",
            b"sentinel".to_vec().into(),
            None,
        )
        .await?;

    let first = change(next_item(&mut watch).await);
    assert_eq!(first.key, "user:1");
    assert_eq!(first.value.as_deref(), Some(&b"alice"[..]));

    let second = change(next_item(&mut watch).await);
    assert_eq!(second.key, "user:1");
    assert_eq!(second.value, None, "a delete is a change with no value");
    assert!(
        second.offset > first.offset,
        "offsets must carry the shard's order",
    );

    // The sentinel arriving third proves user:2's write was filtered out, not
    // merely still in flight.
    let third = change(next_item(&mut watch).await);
    assert_eq!(third.value.as_deref(), Some(&b"sentinel"[..]));

    running.stop().await;
    Ok(())
}

/// A prefix watch receives exactly the prefix.
#[tokio::test]
async fn a_prefix_watch_sees_exactly_the_prefix() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let mut watch = client
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Prefix("user:".to_string()),
            None,
        )
        .await?;

    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "order:9",
            b"x".to_vec().into(),
            None,
        )
        .await?;
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
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "user:2",
            b"bob".to_vec().into(),
            None,
        )
        .await?;

    let first = change(next_item(&mut watch).await);
    assert_eq!(
        (first.key.as_str(), first.value.as_deref()),
        ("user:1", Some(&b"alice"[..])),
        "the non-prefix key must be filtered, and order preserved",
    );
    let second = change(next_item(&mut watch).await);
    assert_eq!(second.key, "user:2");

    running.stop().await;
    Ok(())
}

/// **Resume by offset is gapless under concurrent writes at join time.** A
/// writer streams numbered values into the watched key (with a neighbour
/// interleaved, so the key's offsets are sparse) while the test joins from
/// offset 0 over and over — each join is a fresh race between the replay read
/// and the live registration. Every join must deliver the numbered prefix
/// exactly once, in order: a gap means the join lost the write that landed
/// mid-establishment, a repeat means it doubled it.
///
/// Verified against the inverted order (catch-up read before registration —
/// the natural-looking implementation and a real past stream defect): these
/// joins then lose mid-replay writes and this test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_watch_is_gapless_under_concurrent_writes() -> Result<()> {
    const WRITES: u32 = 200;
    const JOINS: u32 = 6;

    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let progress = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let writer_client = running.client().await?;
    let writer_progress = Arc::clone(&progress);
    let writer = tokio::spawn(async move {
        for i in 0..WRITES {
            writer_client
                .cache_put(
                    "t1",
                    "default",
                    CACHE,
                    "seq",
                    i.to_string().into_bytes().into(),
                    None,
                )
                .await?;
            writer_progress.store(i, std::sync::atomic::Ordering::Release);
            writer_client
                .cache_put("t1", "default", CACHE, "noise", b"n".to_vec().into(), None)
                .await?;
        }
        Ok::<(), anyhow::Error>(())
    });

    for _ in 0..JOINS {
        // Read past the value that was current when this join started, so the
        // establishment window is inside the range being verified.
        let target = (progress.load(std::sync::atomic::Ordering::Acquire) + 3).min(WRITES - 1);
        let mut watch = client
            .watch_cache(
                "t1",
                "default",
                CACHE,
                CacheWatchFilter::Key("seq".to_string()),
                Some(0),
            )
            .await?;
        assert!(!watch.resnapshot(), "offset 0 is still retained");

        let mut seen = Vec::new();
        let mut last_offset = None;
        loop {
            let change = change(next_item(&mut watch).await);
            assert_eq!(change.key, "seq");
            if let Some(last) = last_offset {
                assert!(
                    change.offset > last,
                    "offset {} arrived after {last}: a duplicate or reorder",
                    change.offset,
                );
            }
            last_offset = Some(change.offset);
            let value: u32 = String::from_utf8(change.value.expect("a put").to_vec())?.parse()?;
            seen.push(value);
            if value >= target {
                break;
            }
        }
        let expected: Vec<u32> = (0..=*seen.last().expect("nonempty")).collect();
        assert_eq!(
            seen, expected,
            "the join lost or doubled writes landing while it was established",
        );
    }
    writer.await??;

    running.stop().await;
    Ok(())
}

/// A watch resuming from an offset compaction has collapsed gets the defined
/// signal — resnapshot, then each matching key's current value, then live —
/// never a silent gap.
#[tokio::test]
async fn a_watch_from_a_compacted_offset_resnapshots() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    // Overwrite one key until compaction runs (the store compacts past 1MiB of
    // log when it is 4x the live bytes), so offset 0 is collapsed history.
    let big = vec![b'x'; 64 * 1024];
    for _ in 0..40 {
        client
            .cache_put("t1", "default", CACHE, "hot", big.clone().into(), None)
            .await?;
    }
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "hot",
            b"current".to_vec().into(),
            None,
        )
        .await?;
    // Compaction runs in the background, so wait for it to trim offset 0.
    tokio::time::timeout(RECV_TIMEOUT, async {
        loop {
            let base = running
                .broker
                .shard_log(felix_broker::LogKind::Cache, "t1", "default", CACHE, 0)
                .await
                .map(|log| log.base_offset());
            if base.is_some_and(|base| base > 0) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the cache was never compacted")?;

    let mut watch = client
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("hot".to_string()),
            Some(0),
        )
        .await?;
    assert!(
        watch.resnapshot(),
        "a collapsed history must be signalled, not silently skipped",
    );

    let snapshot = change(next_item(&mut watch).await);
    assert_eq!(
        snapshot.value.as_deref(),
        Some(&b"current"[..]),
        "the resnapshot is the key's current value",
    );

    // And the watch is live after it.
    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "hot",
            b"after".to_vec().into(),
            None,
        )
        .await?;
    let live = change(next_item(&mut watch).await);
    assert_eq!(live.value.as_deref(), Some(&b"after"[..]));
    assert!(live.offset > snapshot.offset);

    running.stop().await;
    Ok(())
}

/// An offset past the tail is refused with the typed error, exactly as a
/// stream resume is: silently reinterpreting it would deliver history below
/// the requested position.
#[tokio::test]
async fn a_watch_from_a_future_offset_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let err = client
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("k".to_string()),
            Some(1_000),
        )
        .await
        .expect_err("an offset past the tail must be refused");
    let cursor = err
        .downcast_ref::<felix_client::SubscribeCursorError>()
        .expect("a typed cursor error");
    assert_eq!(cursor.requested, 1_000);

    running.stop().await;
    Ok(())
}

/// A prefix watch addressed to a shard the cache does not have is refused.
/// Accepting it would register a watch on a log nothing writes to — quiet
/// forever, and indistinguishable from a quiet prefix.
#[tokio::test]
async fn a_watch_on_a_nonexistent_shard_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;

    let err = running
        .client()
        .await?
        .watch_cache_shard(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Prefix("user:".to_string()),
            Some(5),
            None,
        )
        .await
        .expect_err("a shard the cache does not have must be refused");
    assert!(err.to_string().contains("does not exist"), "{err}");

    running.stop().await;
    Ok(())
}

/// A broker whose cache has no log does not advertise the feature, and the
/// client refuses before sending anything — probing would cost the connection.
#[tokio::test]
async fn a_broker_without_a_cache_log_does_not_offer_watches() -> Result<()> {
    let running = start_ephemeral().await?;
    let client = running.client().await?;

    let err = client
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("k".to_string()),
            None,
        )
        .await
        .expect_err("a watch over an ephemeral cache must be refused client-side");
    assert!(
        err.to_string().contains("does not support cache watch"),
        "{err}",
    );

    running.stop().await;
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
        ];
        tokens.insert(
            (*tenant).to_string(),
            issuer.mint(&TenantId::new(*tenant), "p:demo", perms)?,
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

/// **Retained delivery: current state first, then live — under concurrent
/// writes at join time.** A writer streams numbered values into one key of a
/// 200-key roster while retained prefix watches join over and over. Each join
/// must deliver exactly `retained_count` state entries (the full quiescent
/// roster among them, no key twice), and the watched key's events — retained
/// value, then live — must be consecutively numbered with ascending offsets:
/// a gap means the join lost a write landing mid-establishment, a repeat
/// means it doubled one.
///
/// Verified against the inverted order (snapshot read before registration):
/// writes landing during the snapshot read are then in neither half, and this
/// test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retained_watch_delivers_current_state_then_live_under_concurrent_writes() -> Result<()> {
    const ROSTER: u32 = 400;
    const WRITES: u32 = 400;
    const JOINS: u32 = 8;

    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    // A quiescent roster with values big enough to make the snapshot read
    // slow: the shard lock is held across it, so concurrent writes pile up
    // behind it and burst out the moment it releases — which is exactly the
    // window a wrongly-ordered join loses.
    let member = vec![b'm'; 4096];
    for i in 0..ROSTER {
        client
            .cache_put(
                "t1",
                "default",
                CACHE,
                &format!("user:r{i:03}"),
                member.clone().into(),
                None,
            )
            .await?;
    }

    let progress = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let writer_client = running.client().await?;
    let writer_progress = Arc::clone(&progress);
    let writer = tokio::spawn(async move {
        for i in 1..=WRITES {
            writer_client
                .cache_put(
                    "t1",
                    "default",
                    CACHE,
                    "user:seq",
                    i.to_string().into_bytes().into(),
                    None,
                )
                .await?;
            writer_progress.store(i, std::sync::atomic::Ordering::Release);
            // A non-matching neighbour keeps the watched offsets sparse.
            writer_client
                .cache_put(
                    "t1",
                    "default",
                    CACHE,
                    "order:9",
                    b"n".to_vec().into(),
                    None,
                )
                .await?;
        }
        Ok::<(), anyhow::Error>(())
    });

    for _ in 0..JOINS {
        let target = (progress.load(std::sync::atomic::Ordering::Acquire) + 3).min(WRITES);
        let mut watch = client
            .watch_cache_retained(
                "t1",
                "default",
                CACHE,
                CacheWatchFilter::Prefix("user:".to_string()),
            )
            .await?;
        let count = watch
            .retained_count()
            .expect("a retained watch reports its count") as usize;
        let resume = watch.resume_offset();

        // State phase: exactly `count` entries, all below the live edge, no
        // key twice.
        let mut state = std::collections::HashMap::new();
        for _ in 0..count {
            let change = change(next_item(&mut watch).await);
            assert!(
                change.offset < resume,
                "a retained value must sit below the live edge",
            );
            let value = change.value.expect("retained values are puts");
            assert!(
                state
                    .insert(change.key.clone(), (change.offset, value))
                    .is_none(),
                "key {} delivered twice in one snapshot",
                change.key,
            );
        }
        for i in 0..ROSTER {
            let key = format!("user:r{i:03}");
            assert_eq!(
                state.get(&key).map(|(_, value)| value.len()),
                Some(member.len()),
                "the roster is missing {key}",
            );
        }

        // The watched key's events must be consecutively numbered across the
        // retained/live boundary. Its current value can race past the live
        // edge mid-join — then it is absent here and its change arrives live,
        // which folds to the same state.
        let state_value = match state.get("user:seq") {
            Some((_, value)) => Some(String::from_utf8(value.to_vec())?.parse::<u32>()?),
            None => None,
        };
        if progress.load(std::sync::atomic::Ordering::Acquire) == WRITES && state_value.is_none() {
            panic!("a quiescent key is missing from the retained state");
        }
        if state_value.is_some_and(|value| value >= target) {
            // The retained state already covers everything this round would
            // wait for live; nothing further is owed.
            continue;
        }
        let mut expect = state_value.map_or(0, |value| value + 1);
        let mut last_offset = None;
        loop {
            let change = change(next_item(&mut watch).await);
            assert!(
                change.offset >= resume,
                "a live change below the live edge is a duplicate",
            );
            assert_eq!(change.key, "user:seq", "a quiescent key changed");
            if let Some(last) = last_offset {
                assert!(change.offset > last, "live offsets must ascend");
            }
            last_offset = Some(change.offset);
            let value: u32 = String::from_utf8(change.value.expect("a put").to_vec())?.parse()?;
            if expect == 0 {
                expect = value; // the raced current value arrived live
            }
            assert_eq!(
                value, expect,
                "the join lost or doubled a write landing while it was established",
            );
            expect += 1;
            if value >= target {
                break;
            }
        }
    }
    writer.await??;

    running.stop().await;
    Ok(())
}

/// Joining an empty key is an answer, not a silence: the confirmation says
/// zero retained values, and the watch is live from there.
#[tokio::test]
async fn a_retained_watch_on_an_empty_key_reports_no_value() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    let mut watch = client
        .watch_cache_retained(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("never-written".to_string()),
        )
        .await?;
    assert_eq!(
        watch.retained_count(),
        Some(0),
        "an empty key must be a definite zero, not silence",
    );

    client
        .cache_put(
            "t1",
            "default",
            CACHE,
            "never-written",
            b"first".to_vec().into(),
            None,
        )
        .await?;
    let live = change(next_item(&mut watch).await);
    assert_eq!(live.value.as_deref(), Some(&b"first"[..]));
    assert!(live.offset >= watch.resume_offset());

    running.stop().await;
    Ok(())
}

/// An unretained watch reports no count at all — absent, not zero — so a
/// client cannot mistake a live-only watch for a retained one that found
/// nothing.
#[tokio::test]
async fn an_unretained_watch_reports_no_count() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;

    let watch = running
        .client()
        .await?
        .watch_cache(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("k".to_string()),
            None,
        )
        .await?;
    assert_eq!(watch.retained_count(), None);

    running.stop().await;
    Ok(())
}

/// A retained watch may name its shard explicitly, like any other.
#[tokio::test]
async fn a_retained_watch_takes_an_explicit_shard() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let running = start(dir.path()).await?;
    let client = running.client().await?;

    client
        .cache_put("t1", "default", CACHE, "user:1", b"v".to_vec().into(), None)
        .await?;
    let mut watch = client
        .watch_cache_shard_retained(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Prefix("user:".to_string()),
            Some(0),
        )
        .await?;
    assert_eq!(watch.retained_count(), Some(1));
    assert_eq!(
        change(next_item(&mut watch).await).value.as_deref(),
        Some(&b"v"[..])
    );

    running.stop().await;
    Ok(())
}

/// **The retained value outlives the broker that stored it.** A restart wipes
/// every index; the promoted-or-restarted broker rebuilds it from the log,
/// and a new retained watch is served from that rebuilt index at the same
/// offset the write originally took.
#[tokio::test]
async fn a_retained_value_survives_a_restart() -> Result<()> {
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
    let mut before = client
        .watch_cache_retained(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("user:1".to_string()),
        )
        .await?;
    assert_eq!(before.retained_count(), Some(1));
    let original = change(next_item(&mut before).await);
    drop(before);
    drop(client);
    running.stop().await;

    let restarted = start(dir.path()).await?;
    let mut after = restarted
        .client()
        .await?
        .watch_cache_retained(
            "t1",
            "default",
            CACHE,
            CacheWatchFilter::Key("user:1".to_string()),
        )
        .await?;
    assert_eq!(after.retained_count(), Some(1));
    let rebuilt = change(next_item(&mut after).await);
    assert_eq!(rebuilt.value.as_deref(), Some(&b"alice"[..]));
    assert_eq!(
        rebuilt.offset, original.offset,
        "the rebuilt index must name the same record the write took",
    );

    restarted.stop().await;
    Ok(())
}
