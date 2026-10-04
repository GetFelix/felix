//! What a forwarded publish owes the client on the other end of it.
//!
//! A forwarded publish is still a publish to the stream, so it owes the
//! guarantee the *stream* asks for — not whichever one this path happens to
//! provide. Acknowledging on local durability alone made `Quorum` depend on
//! which broker a client reached: honoured when it talked to the leader,
//! silently downgraded through any other, which is where a failover then lost
//! the record.
use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{ConsistencyLevel, DurableStorage, StreamMetadata};
use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{AckMode, ShardRef};
use tempfile::TempDir;

use super::*;
use felix_replication::quorum::QuorumMarks;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const CACHE: &str = "sessions";
const LOCAL: &str = "broker-a";
const GENERATION: u64 = 4;
const TEST_PRIVATE_KEY: [u8; 32] = [10u8; 32];

/// The owner's view of the tenant's keys, and tokens minted against them.
struct Credentials {
    issuer: FelixTokenIssuer,
    auth: Arc<BrokerAuth>,
}

impl Credentials {
    fn new() -> Self {
        let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
        let public_key = signing_key.verifying_key().to_bytes();
        let jwks = Jwks {
            keys: vec![Jwk {
                kty: "OKP".to_string(),
                kid: "k1".to_string(),
                alg: "EdDSA".to_string(),
                use_field: KeyUse::Sig,
                crv: Some("Ed25519".to_string()),
                x: Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public_key)),
            }],
        };
        let mut materials = HashMap::new();
        materials.insert(
            TENANT.to_string(),
            TenantKeyMaterial {
                kid: "k1".to_string(),
                alg: jsonwebtoken::Algorithm::EdDSA,
                private_key: TEST_PRIVATE_KEY,
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
        // JWKS injected rather than fetched: the owner verifies with what it
        // holds, and no control plane is involved in these tests.
        let key_store = Arc::new(crate::serving::auth::ControlPlaneKeyStore::new(
            "http://localhost".to_string(),
            Arc::new(TenantKeyCache::default()),
        ));
        key_store.insert_jwks(&TenantId::new(TENANT), jwks);
        Self {
            issuer,
            auth: Arc::new(BrokerAuth::with_key_store(key_store)),
        }
    }

    fn token(&self, perms: &[&str]) -> String {
        self.issuer
            .mint(
                &TenantId::new(TENANT),
                "p:test",
                perms.iter().map(|perm| perm.to_string()).collect(),
            )
            .expect("mint")
    }

    fn publisher(&self) -> String {
        self.token(&[&format!(
            "stream.publish:stream:{TENANT}/{NAMESPACE}/{STREAM}"
        )])
    }
}

fn key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

fn cache_key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Cache,
    }
}

/// A router in which this broker leads the shard with one follower.
fn router() -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [LOCAL, "broker-b", "broker-c"]
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            (
                id.to_string(),
                NodeRef {
                    node_id: id.to_string(),
                    advertise_addr: format!("10.0.0.1:{}", 7001 + i as u16)
                        .parse()
                        .expect("addr"),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    let table = RoutingTable::build(
        [
            (
                key(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                GENERATION,
            ),
            (
                cache_key(),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                GENERATION,
            ),
        ],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

async fn broker_with(consistency: ConsistencyLevel) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: 1,
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (Arc::new(broker), dir)
}

fn handler(
    broker: Arc<Broker>,
    marks: Option<Arc<QuorumMarks>>,
    timeout: Duration,
) -> ForwardingHandler {
    handler_with(broker, marks, timeout, Credentials::new().auth)
}

fn handler_with(
    broker: Arc<Broker>,
    marks: Option<Arc<QuorumMarks>>,
    timeout: Duration,
    auth: Arc<BrokerAuth>,
) -> ForwardingHandler {
    let router = router();
    let ingress = Arc::new(crate::shards::routing::IngressRouter::new(
        Arc::clone(&router),
        Arc::default(),
    ));
    let servable: crate::shards::routing::ServableShards = [
        (
            crate::shards::ShardKey {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
                shard: 0,
                kind: crate::shards::ShardKind::Stream,
            },
            GENERATION,
        ),
        (
            crate::shards::ShardKey {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: CACHE.to_string(),
                shard: 0,
                kind: crate::shards::ShardKind::Cache,
            },
            GENERATION,
        ),
    ]
    .into_iter()
    .collect();
    // What the lifecycle does when it activates a shard.
    for (key, generation) in &servable {
        ingress.fence().open(key, *generation);
    }
    ingress.publish_servable(servable);
    ForwardingHandler::new(
        broker,
        ingress,
        router,
        "10.0.0.1:7001".to_string(),
        marks,
        timeout,
        auth,
    )
}

/// A forward carrying a credential that allows the publish.
fn forwarded() -> ForwardPublish {
    forwarded_with(Credentials::new().publisher())
}

fn forwarded_with(credential: String) -> ForwardPublish {
    ForwardPublish {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation: GENERATION,
        },
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"forwarded")],
        credential,
    }
}

fn forwarded_cache_op(op: CacheOpKind, credential: String) -> ForwardCacheOp {
    ForwardCacheOp {
        correlation_id: 2,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: CACHE.to_string(),
            shard: 0,
            generation: GENERATION,
        },
        op,
        key: "session:abc".to_string(),
        value: Bytes::from_static(b"v"),
        ttl_ms: 0,
        credential,
        condition: None,
    }
}

fn unauthorized_publish(answer: InternalMessage) -> String {
    match answer {
        InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::Unauthorized => {
            err.detail
        }
        other => panic!("expected an Unauthorized refusal, got {other:?}"),
    }
}

/// **The owner checks the client's credential itself.** The forwarder said it
/// checked; the owner cannot know that, so a forward carrying no credential,
/// or one that does not allow the publish, is refused before anything is
/// written -- and nothing lands in the log.
#[tokio::test]
async fn a_forward_without_a_credential_is_refused_and_writes_nothing() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let handler = handler(Arc::clone(&broker), None, Duration::from_secs(1));

    let detail = unauthorized_publish(handler.apply(forwarded_with(String::new())).await);
    assert!(detail.contains("no credential"), "{detail}");

    let tail = broker
        .cursor_tail(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("tail");
    assert_eq!(tail.next_seq(), 0, "a refused forward reached the log");
}

/// A credential that can only subscribe is exactly what the ingress broker
/// would have refused -- the case a compromised forwarder skips.
#[tokio::test]
async fn a_forward_whose_credential_cannot_publish_is_refused() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let credentials = Credentials::new();
    let handler = handler_with(
        broker,
        None,
        Duration::from_secs(1),
        Arc::clone(&credentials.auth),
    );

    let subscriber = credentials.token(&[&format!(
        "stream.subscribe:stream:{TENANT}/{NAMESPACE}/{STREAM}"
    )]);
    let detail = unauthorized_publish(handler.apply(forwarded_with(subscriber)).await);
    assert!(detail.contains("does not allow"), "{detail}");

    // The right action, on a different stream.
    let elsewhere =
        credentials.token(&[&format!("stream.publish:stream:{TENANT}/{NAMESPACE}/other")]);
    unauthorized_publish(handler.apply(forwarded_with(elsewhere)).await);

    // Not signed by the tenant's keys at all.
    let forged = "eyJhbGciOiJFZERTQSJ9.eyJ0aWQiOiJ0MSJ9.bm9wZQ".to_string();
    let detail = unauthorized_publish(handler.apply(forwarded_with(forged)).await);
    assert!(detail.contains("refused"), "{detail}");
}

/// A cache write needs `cache.write`; a read-only credential is refused for
/// it and accepted for a `Get`.
#[tokio::test]
async fn a_forwarded_cache_op_is_checked_against_its_own_action() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    broker
        .register_cache(
            TENANT,
            NAMESPACE,
            CACHE,
            felix_broker::CacheMetadata::default(),
        )
        .await
        .expect("cache");
    let credentials = Credentials::new();
    let handler = handler_with(
        broker,
        None,
        Duration::from_secs(1),
        Arc::clone(&credentials.auth),
    );
    let reader = credentials.token(&[&format!("cache.read:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);

    match handler
        .apply_cache_op(forwarded_cache_op(CacheOpKind::Put, reader.clone()))
        .await
    {
        InternalMessage::ForwardCacheError(err) => assert_eq!(err.code, ErrorCode::Unauthorized),
        other => panic!("a read-only credential wrote a cache key: {other:?}"),
    }
    assert!(
        matches!(
            handler
                .apply_cache_op(forwarded_cache_op(CacheOpKind::Get, reader))
                .await,
            InternalMessage::ForwardCacheOk(_)
        ),
        "a read-only credential could not read",
    );
    match handler
        .apply_cache_op(forwarded_cache_op(CacheOpKind::Get, String::new()))
        .await
    {
        InternalMessage::ForwardCacheError(err) => assert_eq!(err.code, ErrorCode::Unauthorized),
        other => panic!("a cache op with no credential was served: {other:?}"),
    }
}

/// A forwarded cache op the owner's store refused is answered as an error,
/// never as a success the store never applied.
#[tokio::test]
async fn a_forwarded_cache_op_the_store_refused_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("caches");
    let cache = felix_storage::LogCache::open(&root, LogConfig::default()).expect("open cache");
    // The shard can never open once its root is a plain file.
    std::fs::remove_dir(&root).expect("remove root");
    std::fs::write(&root, b"not a directory").expect("replace root");
    let broker = Broker::new(Box::new(cache));
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_cache(
            TENANT,
            NAMESPACE,
            CACHE,
            felix_broker::CacheMetadata::default(),
        )
        .await
        .expect("cache");
    let credentials = Credentials::new();
    let handler = handler_with(
        Arc::new(broker),
        None,
        Duration::from_secs(1),
        Arc::clone(&credentials.auth),
    );
    let token = credentials.token(&[
        &format!("cache.read:cache:{TENANT}/{NAMESPACE}/{CACHE}"),
        &format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}"),
    ]);
    for (op, code) in [
        (CacheOpKind::Put, ErrorCode::StorageFailed),
        (CacheOpKind::Delete, ErrorCode::StorageFailed),
        (CacheOpKind::Get, ErrorCode::Unavailable),
    ] {
        match handler
            .apply_cache_op(forwarded_cache_op(op, token.clone()))
            .await
        {
            InternalMessage::ForwardCacheError(err) => assert_eq!(err.code, code, "{op:?}"),
            other => panic!("a {op:?} the store failed was answered {other:?}"),
        }
    }
}

/// **A forwarded publish to a `Quorum` stream is not acknowledged until a
/// majority holds it.** Answering on local durability made the guarantee depend
/// on which broker the client happened to reach.
#[tokio::test]
async fn a_forwarded_quorum_publish_waits_for_the_majority() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    // A mark that never reaches the record: no follower ever stores it.
    marks.publish(
        &crate::shards::ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            kind: crate::shards::ShardKind::Stream,
        },
        GENERATION,
        0,
    );
    let handler = handler(broker, Some(marks), Duration::from_millis(300));

    let answer = handler.apply(forwarded()).await;

    match answer {
        InternalMessage::ForwardPublishError(err) => {
            assert_eq!(err.code, ErrorCode::StorageFailed);
        }
        other => panic!(
            "a forwarded quorum publish was acknowledged without a majority: {:?}",
            other.kind()
        ),
    }
}

/// And it *is* acknowledged once the majority holds it.
#[tokio::test]
async fn a_forwarded_quorum_publish_is_acknowledged_once_the_majority_holds_it() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(
        &crate::shards::ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            kind: crate::shards::ShardKind::Stream,
        },
        GENERATION,
        // Past the record this publish writes.
        1_000,
    );
    let handler = handler(broker, Some(marks), Duration::from_secs(5));

    let answer = handler.apply(forwarded()).await;

    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "expected an acknowledgement, got {:?}",
        answer.kind(),
    );
}

/// The lease is checked again when a `Quorum` ack is released. A broker whose
/// lease lapsed while it waited may already have been replaced, so it answers
/// "unknown" even once the mark passes the record.
#[tokio::test]
async fn a_quorum_ack_is_withheld_when_the_lease_lapses_while_it_waits() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let shard = crate::shards::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    };
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&shard, GENERATION, 0);
    let handler = Arc::new(handler(
        Arc::clone(&broker),
        Some(Arc::clone(&marks)),
        Duration::from_secs(5),
    ));
    let lease = Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
        3600,
    )));
    lease.renew();
    handler.ingress.fence().bind_lease(Arc::clone(&lease));

    let waiting = tokio::spawn({
        let handler = Arc::clone(&handler);
        async move { handler.apply(forwarded()).await }
    });
    for _ in 0..200 {
        if broker
            .cursor_tail(TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("tail")
            .next_seq()
            > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    lease.surrender();
    marks.publish(&shard, GENERATION, 1_000);

    match waiting.await.expect("task") {
        InternalMessage::ForwardPublishError(err) => {
            assert!(err.detail.contains("lease"), "{}", err.detail)
        }
        other => panic!(
            "a quorum ack went out after the lease lapsed: {:?}",
            other.kind()
        ),
    }
}

/// A `Leader` stream is unaffected: local durability is the guarantee it offers,
/// so a forwarded publish is acknowledged without waiting for anyone.
#[tokio::test]
async fn a_forwarded_leader_publish_does_not_wait() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let marks = Arc::new(QuorumMarks::new());
    let handler = handler(broker, Some(marks), Duration::from_millis(300));

    let answer = handler.apply(forwarded()).await;

    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "a leader-acknowledged stream waited for a quorum: {:?}",
        answer.kind(),
    );
}

/// The answer names where each forwarded batch landed in the owner's log.
#[tokio::test]
async fn a_forwarded_publish_is_answered_with_its_offsets() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let handler = handler(broker, None, Duration::from_millis(300));

    for expected in [(0, 0), (1, 1)] {
        match handler.apply(forwarded()).await {
            InternalMessage::ForwardPublishOk(ok) => assert_eq!(ok.offsets(), Some(expected)),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
    }
}

/// A stream with no log has no offsets, and says so with an empty range
/// rather than `0..=0`, which a requester would take for a real one.
#[tokio::test]
async fn a_forwarded_publish_to_a_stream_without_a_log_has_no_offsets() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: false,
                shards: 1,
                consistency: ConsistencyLevel::Leader,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let handler = handler(Arc::new(broker), None, Duration::from_millis(300));

    match handler.apply(forwarded()).await {
        InternalMessage::ForwardPublishOk(ok) => {
            assert_eq!((ok.first_offset, ok.last_offset), (1, 0));
            assert_eq!(ok.offsets(), None);
        }
        other => panic!("expected an acknowledgement, got {:?}", other.kind()),
    }
}

/// The write fence, on the owner. Ownership and the generation check pass --
/// the servable set still says the shard is served here -- but the lifecycle
/// has already closed the fence for a move, so every forwarded write is
/// refused and nothing lands.
mod fence {
    use super::*;
    use crate::test_support::leader::{
        self, CACHE, DURABLE, GENERATION, Leader, NAMESPACE, TENANT,
    };

    fn owner(leader: &Leader, credentials: &Credentials) -> ForwardingHandler {
        ForwardingHandler::new(
            Arc::clone(&leader.broker),
            Arc::clone(&leader.ingress),
            Arc::clone(&leader.router),
            "10.0.0.1:7001".to_string(),
            None,
            Duration::from_secs(1),
            Arc::clone(&credentials.auth),
        )
    }

    fn shard_ref(name: &str) -> ShardRef {
        ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: name.to_string(),
            shard: 0,
            generation: GENERATION,
        }
    }

    fn cache_op(credential: &str, op: CacheOpKind, value: Bytes) -> ForwardCacheOp {
        ForwardCacheOp {
            correlation_id: 2,
            shard: shard_ref(CACHE),
            op,
            key: "session:abc".to_string(),
            value,
            ttl_ms: 0,
            credential: credential.to_string(),
            condition: None,
        }
    }

    fn refused_cache_op(answer: InternalMessage) {
        match answer {
            InternalMessage::ForwardCacheError(err) => {
                assert_eq!(err.code, ErrorCode::Unavailable, "{}", err.detail)
            }
            other => panic!("a cache write landed after the fence closed: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_forwarded_publish_after_the_fence_is_refused() {
        let mut leader = Leader::start().await;
        let credentials = Credentials::new();
        let handler = owner(&leader, &credentials);
        let publish = |payload: &'static [u8]| ForwardPublish {
            correlation_id: 1,
            shard: shard_ref(DURABLE),
            ack: AckMode::OnCommit,
            payloads: vec![Bytes::from_static(payload)],
            credential: credentials.token(&[&format!(
                "stream.publish:stream:{TENANT}/{NAMESPACE}/{DURABLE}"
            )]),
        };

        assert!(matches!(
            handler.apply(publish(b"before")).await,
            InternalMessage::ForwardPublishOk(_)
        ));
        leader.fence_move(&leader::stream_key(DURABLE));
        match handler.apply(publish(b"after")).await {
            InternalMessage::ForwardPublishError(err) => {
                assert_eq!(err.code, ErrorCode::Unavailable, "{}", err.detail)
            }
            other => panic!("a forwarded publish landed after the fence closed: {other:?}"),
        }
        assert_eq!(
            leader.tail(DURABLE).await,
            1,
            "the refused publish was written"
        );
    }

    /// The owner's lease is checked on a forwarded write exactly as on a
    /// direct one: the generation matching is not enough once the lease has
    /// lapsed, because the control plane may have moved the shard already.
    #[tokio::test]
    async fn forwarded_writes_after_the_lease_lapses_are_refused() {
        let leader = Leader::start().await;
        let lease = leader.hold_lease();
        let credentials = Credentials::new();
        let handler = owner(&leader, &credentials);
        let publish = ForwardPublish {
            correlation_id: 1,
            shard: shard_ref(DURABLE),
            ack: AckMode::OnCommit,
            payloads: vec![Bytes::from_static(b"late")],
            credential: credentials.token(&[&format!(
                "stream.publish:stream:{TENANT}/{NAMESPACE}/{DURABLE}"
            )]),
        };
        let writer =
            credentials.token(&[&format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);

        lease.surrender();
        let started = std::time::Instant::now();
        match handler.apply(publish.clone()).await {
            InternalMessage::ForwardPublishError(err) => {
                assert_eq!(err.code, ErrorCode::Unavailable, "{}", err.detail);
                assert!(err.detail.contains("lease"), "{}", err.detail);
            }
            other => panic!("a forwarded publish landed without a lease: {other:?}"),
        }
        assert_eq!(
            leader.tail(DURABLE).await,
            0,
            "the refused publish was written"
        );
        refused_cache_op(
            handler
                .apply_cache_op(cache_op(
                    &writer,
                    CacheOpKind::Put,
                    Bytes::from_static(b"v"),
                ))
                .await,
        );
        refused_cache_op(
            handler
                .apply_cache_op(cache_op(
                    &writer,
                    CacheOpKind::CounterAdd,
                    felix_storage::counter_log::encode_sum(1),
                ))
                .await,
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a lapsed lease is not a move to wait out"
        );

        lease.renew();
        assert!(matches!(
            handler.apply(publish).await,
            InternalMessage::ForwardPublishOk(_)
        ));
    }

    #[tokio::test]
    async fn forwarded_cache_writes_after_the_fence_are_refused() {
        let mut leader = Leader::start().await;
        let credentials = Credentials::new();
        let handler = owner(&leader, &credentials);
        let writer =
            credentials.token(&[&format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);
        let reader =
            credentials.token(&[&format!("cache.read:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);
        let add = || {
            cache_op(
                &writer,
                CacheOpKind::CounterAdd,
                felix_storage::counter_log::encode_sum(5),
            )
        };

        assert!(matches!(
            handler
                .apply_cache_op(cache_op(
                    &writer,
                    CacheOpKind::Put,
                    Bytes::from_static(b"v1")
                ))
                .await,
            InternalMessage::ForwardCacheOk(_)
        ));
        assert!(matches!(
            handler.apply_cache_op(add()).await,
            InternalMessage::ForwardCacheOk(_)
        ));
        leader.fence_move(&leader::cache_key());

        refused_cache_op(
            handler
                .apply_cache_op(cache_op(
                    &writer,
                    CacheOpKind::Put,
                    Bytes::from_static(b"v2"),
                ))
                .await,
        );
        refused_cache_op(
            handler
                .apply_cache_op(cache_op(&writer, CacheOpKind::Delete, Bytes::new()))
                .await,
        );
        refused_cache_op(handler.apply_cache_op(add()).await);

        // Reads are not fenced, and show nothing changed.
        match handler
            .apply_cache_op(cache_op(&reader, CacheOpKind::Get, Bytes::new()))
            .await
        {
            InternalMessage::ForwardCacheOk(ok) => {
                assert_eq!(ok.value, Some(Bytes::from_static(b"v1")))
            }
            other => panic!("read failed: {other:?}"),
        }
        match handler
            .apply_cache_op(cache_op(&reader, CacheOpKind::CounterGet, Bytes::new()))
            .await
        {
            InternalMessage::ForwardCacheOk(ok) => {
                assert_eq!(ok.value, Some(felix_storage::counter_log::encode_sum(5)))
            }
            other => panic!("read failed: {other:?}"),
        }
    }

    /// A forwarded counter add that meets the fence is held until the move
    /// cuts over, then sends the requester to the new owner rather than being
    /// refused. Nothing is counted here.
    #[tokio::test]
    async fn a_forwarded_write_during_a_move_is_sent_to_the_new_owner() {
        let hold = crate::shards::routing::hold::MoveHold::new(Duration::from_secs(5), 16);
        let mut leader = Leader::start_holding(hold).await;
        let credentials = Credentials::new();
        let handler = Arc::new(owner(&leader, &credentials));
        let writer =
            credentials.token(&[&format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);
        leader.fence_move(&leader::cache_key());

        let add = tokio::spawn({
            let handler = Arc::clone(&handler);
            async move {
                handler
                    .apply_cache_op(cache_op(
                        &writer,
                        CacheOpKind::CounterAdd,
                        felix_storage::counter_log::encode_sum(5),
                    ))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!add.is_finished(), "the add is held while the shard moves");

        leader.cut_over(&leader::cache_key());
        match add.await.expect("add") {
            InternalMessage::NotLeader(moved) => {
                assert_eq!(moved.node_id, leader::SUCCESSOR);
                assert_eq!(moved.generation, GENERATION + 1);
            }
            other => panic!("expected the new owner, got {other:?}"),
        }
        let counted = leader
            .broker
            .counters()
            .expect("counters")
            .get(TENANT, NAMESPACE, CACHE, 0, "session:abc")
            .await
            .expect("read");
        assert_eq!(counted, None, "the held add was applied on the old owner");
    }
}

/// A forwarded conditional put is checked on the owner and answered with
/// whether it applied and the version, as a local one is.
#[tokio::test]
async fn a_forwarded_conditional_put_answers_with_its_outcome() {
    use felix_wire::internal::{ForwardCacheCondition, ForwardCacheOutcome};

    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    broker
        .register_cache(
            TENANT,
            NAMESPACE,
            CACHE,
            felix_broker::CacheMetadata::default(),
        )
        .await
        .expect("cache");
    let credentials = Credentials::new();
    let handler = handler_with(
        broker,
        None,
        Duration::from_secs(1),
        Arc::clone(&credentials.auth),
    );
    let writer = credentials.token(&[&format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}")]);
    let put_if = || ForwardCacheOp {
        condition: Some(ForwardCacheCondition::Absent),
        ..forwarded_cache_op(CacheOpKind::Put, writer.clone())
    };

    let first = match handler.apply_cache_op(put_if()).await {
        InternalMessage::ForwardCacheOk(ok) => ok.outcome.expect("an outcome"),
        other => panic!("not served: {other:?}"),
    };
    assert!(first.applied);
    match handler.apply_cache_op(put_if()).await {
        InternalMessage::ForwardCacheOk(ok) => assert_eq!(
            ok.outcome,
            Some(ForwardCacheOutcome {
                applied: false,
                version: first.version,
            })
        ),
        other => panic!("not served: {other:?}"),
    }
}
