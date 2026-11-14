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
use felix_wire::internal::{AckMode, ForwardProducer, ShardRef};
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
    router_at(0)
}

/// [`router`], with both assignments for `incarnation` of their stream and
/// cache.
fn router_at(incarnation: u64) -> Arc<ShardRouter> {
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
    let table = RoutingTable::build_with(
        [key(), cache_key()].map(|key| felix_router::Placed {
            key,
            leader: LOCAL.to_string(),
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            generation: GENERATION,
            draining: false,
            successor: None,
            routing: felix_wire::routing::ShardRouting::Modulo,
            incarnation,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            pending: None,
            readmitted: Default::default(),
        }),
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
    handler_routed(broker, marks, timeout, auth, router())
}

fn handler_routed(
    broker: Arc<Broker>,
    marks: Option<Arc<QuorumMarks>>,
    timeout: Duration,
    auth: Arc<BrokerAuth>,
    router: Arc<ShardRouter>,
) -> ForwardingHandler {
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
        Arc::new(crate::shards::routing::StreamEngine::new(
            marks,
            Some(ingress),
        )),
        router,
        "10.0.0.1:7001".to_string(),
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
            incarnation: None,
        },
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"forwarded")],
        credential,
        headers: None,
        producer: None,
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
            incarnation: None,
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

/// A forward that carries record headers is stored with them, so the owner's
/// log holds what the client sent through the ingress broker.
#[tokio::test]
async fn a_forward_with_headers_is_stored_with_them() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    broker.record_headers_when(|| true);
    let handler = handler(Arc::clone(&broker), None, Duration::from_secs(1));
    let blocks = vec![Some(Bytes::from_static(b"trace")), None];
    let mut publish = forwarded();
    publish.payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    publish.headers = Some(blocks.clone());

    let answer = handler.apply(publish).await;
    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "{answer:?}"
    );

    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let stored = log.read_replicated_from(0, 1 << 20).await.expect("read");
    assert_eq!(
        felix_broker::replication::headers_to_wire(&stored),
        Some(blocks)
    );
}

/// A forward that names its producer and sequence.
fn sequenced(payload: &'static [u8]) -> ForwardPublish {
    ForwardPublish {
        payloads: vec![Bytes::from_static(payload)],
        headers: Some(vec![Some(Bytes::from_static(b"block"))]),
        producer: Some(ForwardProducer {
            producer_id: 77,
            sequence: 0,
        }),
        ..forwarded()
    }
}

fn shard_key() -> crate::shards::ShardKey {
    crate::shards::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    }
}

async fn next_offset(broker: &Broker) -> u64 {
    broker
        .cursor_tail(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("tail")
        .next_seq()
}

/// **A sequenced batch sent again is answered from the log, not written
/// again.** That is what makes it safe for the forwarder to re-send a batch
/// whose answer it lost.
#[tokio::test]
async fn a_sequenced_forward_sent_twice_is_written_once() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    broker.record_headers_when(|| true);
    let handler = handler(Arc::clone(&broker), None, Duration::from_secs(1));

    let first = handler.apply(sequenced(b"reading")).await;
    let again = handler.apply(sequenced(b"reading")).await;

    let (InternalMessage::ForwardPublishOk(first), InternalMessage::ForwardPublishOk(again)) =
        (&first, &again)
    else {
        panic!("both sends are answered: {first:?} {again:?}");
    };
    assert_eq!(first.offsets(), Some((0, 0)));
    assert_eq!(again.offsets(), first.offsets());
    assert_eq!(
        next_offset(&broker).await,
        1,
        "the re-send was written again"
    );
}

/// Another batch under a sequence already held is refused and not written: a
/// reused sequence is not taken as the batch it replaced.
#[tokio::test]
async fn a_sequence_reused_for_another_batch_is_refused() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    broker.record_headers_when(|| true);
    let handler = handler(Arc::clone(&broker), None, Duration::from_secs(1));

    let first = handler.apply(sequenced(b"reading")).await;
    assert!(
        matches!(first, InternalMessage::ForwardPublishOk(_)),
        "{first:?}"
    );
    let other = handler.apply(sequenced(b"another reading")).await;

    assert!(
        matches!(&other, InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::StorageFailed),
        "{other:?}"
    );
    assert_eq!(next_offset(&broker).await, 1);
}

/// **A re-sent batch on a `Quorum` stream waits for the majority like the
/// first send did.** The first send's answer is exactly what may have been
/// lost before a majority held it, so finding the batch in the log is not
/// enough to acknowledge it.
#[tokio::test]
async fn a_resent_quorum_batch_waits_for_the_majority_again() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    broker.record_headers_when(|| true);
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&shard_key(), GENERATION, 0);
    let handler = handler(
        Arc::clone(&broker),
        Some(Arc::clone(&marks)),
        Duration::from_millis(300),
    );

    for send in ["first", "re-send"] {
        let answer = handler.apply(sequenced(b"reading")).await;
        assert!(
            matches!(&answer, InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::StorageFailed),
            "the {send} was acknowledged without a majority: {answer:?}"
        );
    }

    marks.publish(&shard_key(), GENERATION, 1_000);
    let answer = handler.apply(sequenced(b"reading")).await;
    assert!(
        matches!(&answer, InternalMessage::ForwardPublishOk(ok) if ok.offsets() == Some((0, 0))),
        "{answer:?}"
    );
    assert_eq!(next_offset(&broker).await, 1);
}

/// An owner that does not accept headers refuses a forward that has them, and
/// writes nothing, rather than store the records without.
#[tokio::test]
async fn a_forward_with_headers_is_refused_while_the_owner_does_not_accept_them() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let handler = handler(Arc::clone(&broker), None, Duration::from_secs(1));
    let mut publish = forwarded();
    publish.headers = Some(vec![Some(Bytes::from_static(b"trace"))]);

    let answer = handler.apply(publish).await;
    assert!(
        matches!(&answer, InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::UnsupportedKind),
        "{answer:?}"
    );
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

/// **The owner re-checks a forwarded cache op with its key.** A key grant
/// covers the keys it names and no others, for every forwarded op, and a
/// whole-cache grant still covers them all.
#[tokio::test]
async fn a_forwarded_cache_op_is_checked_against_its_key() {
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
    let token = |object: &str| {
        credentials.token(&[
            &format!("cache.read:{object}"),
            &format!("cache.write:{object}"),
        ])
    };
    let cache = format!("cache:{TENANT}/{NAMESPACE}/{CACHE}");
    // `forwarded_cache_op` uses the key `session:abc`.
    let cases = [
        (cache.clone(), true),
        (format!("{cache}/session:abc"), true),
        (format!("{cache}/session:*"), true),
        (format!("{cache}/session:ab"), false),
        (format!("{cache}/session:abcd*"), false),
        (format!("{cache}/other*"), false),
    ];
    for (object, allowed) in cases {
        let credential = token(&object);
        for op in [
            CacheOpKind::Get,
            CacheOpKind::Put,
            CacheOpKind::Delete,
            CacheOpKind::CounterAdd,
            CacheOpKind::CounterGet,
        ] {
            let answer = handler
                .apply_cache_op(forwarded_cache_op(op, credential.clone()))
                .await;
            let refused = matches!(
                &answer,
                InternalMessage::ForwardCacheError(err) if err.code == ErrorCode::Unauthorized
            );
            assert_eq!(refused, !allowed, "{object} {op:?}: {answer:?}");
        }
    }
}

/// The owner of a lease cache key mints for a forwarded claim from the
/// credential it verified itself, and refuses before writing when it may not.
#[tokio::test]
async fn a_forwarded_lease_claim_is_minted_by_the_owner() {
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
    let lease = crate::serving::lease::LeaseTokens::from_keys(
        &base64::engine::general_purpose::STANDARD.encode([3u8; 32]),
        crate::serving::lease::DEFAULT_MAX_TTL,
    )
    .expect("lease keys");
    let auth = Arc::new(
        (*credentials.auth)
            .clone()
            .with_lease_tokens(Some(Arc::new(lease))),
    );
    let handler = handler_with(broker, None, Duration::from_secs(1), auth);
    let write = format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}");
    let template = format!("stream.publish:stream:{TENANT}/{NAMESPACE}/wl.{{{CACHE}}}.out");
    let claim = |credential: String, condition| ForwardCacheOp {
        key: "w7".to_string(),
        ttl_ms: 30_000,
        condition: Some(condition),
        ..forwarded_cache_op(CacheOpKind::Put, credential)
    };

    let plain = credentials.token(&[&write]);
    let answer = handler
        .apply_lease_cache_op(claim(plain, ForwardCacheCondition::Absent))
        .await;
    assert!(
        matches!(&answer, InternalMessage::ForwardCacheError(err) if err.code == ErrorCode::Unauthorized),
        "{answer:?}"
    );

    let node = credentials.token(&[&write, &template]);
    let answer = handler
        .apply_lease_cache_op(claim(node.clone(), ForwardCacheCondition::Absent))
        .await;
    let InternalMessage::LeaseForwardCacheOk(ok) = answer else {
        panic!("not minted: {answer:?}");
    };
    assert!(ok.outcome.applied, "the refused claim wrote nothing");
    assert!(matches!(ok.lease, ForwardLease::Minted { .. }), "{ok:?}");

    let answer = handler
        .apply_lease_cache_op(claim(node, ForwardCacheCondition::Absent))
        .await;
    let InternalMessage::LeaseForwardCacheOk(lost) = answer else {
        panic!("not answered: {answer:?}");
    };
    assert!(!lost.outcome.applied);
    assert_eq!(lost.lease, ForwardLease::NotMinted);
}

/// A forwarded cache op the owner's store refused is answered as an error,
/// never as a success the store never applied.
#[tokio::test]
async fn a_forwarded_cache_op_the_store_refused_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("caches");
    let cache = felix_storage::LogCache::open(&root, LogConfig::default()).expect("open cache");
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
    // The shard can never open once its root is a plain file. Replaced after
    // the registration, which reads the cache's marker under that root.
    std::fs::remove_dir_all(&root).expect("remove root");
    std::fs::write(&root, b"not a directory").expect("replace root");
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
    let err = lease_lapses_while_waiting(forwarded()).await;
    assert_eq!(err.code, ErrorCode::StorageFailed);
}

/// A sequenced forward is told the leadership moved, which its sender reads
/// as an unknown outcome and sends again to whoever leads now.
#[tokio::test]
async fn a_sequenced_forward_deposed_while_it_waits_is_told_so() {
    let err = lease_lapses_while_waiting(sequenced(b"reading")).await;
    assert_eq!(err.code, ErrorCode::LeadershipLost);
}

async fn lease_lapses_while_waiting(publish: ForwardPublish) -> ForwardPublishError {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    broker.record_headers_when(|| true);
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
        async move { handler.apply(publish).await }
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
            assert!(err.detail.contains("lease"), "{}", err.detail);
            err
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
            Arc::new(crate::shards::routing::StreamEngine::new(
                None,
                Some(Arc::clone(&leader.ingress)),
            )),
            Arc::clone(&leader.router),
            "10.0.0.1:7001".to_string(),
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
            incarnation: None,
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
            headers: None,
            producer: None,
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
            headers: None,
            producer: None,
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

/// **A forward routed by another incarnation's assignment is refused**, even
/// at the owner's own generation: a stream deleted and created again can
/// start at the generation the deleted one had (after a control-plane restore
/// rewinds the change sequence), and generation alone would put the deleted
/// stream's write in the new one. A forward that names the owner's
/// incarnation, or none, is served as before.
#[tokio::test]
async fn a_forward_for_another_incarnation_is_refused_and_writes_nothing() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Leader).await;
    let credentials = Credentials::new();
    let handler = handler_routed(
        Arc::clone(&broker),
        None,
        Duration::from_secs(1),
        Arc::clone(&credentials.auth),
        router_at(5),
    );
    let tail = || async {
        broker
            .cursor_tail(TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("tail")
            .next_seq()
    };

    for sent in [4, 6] {
        let mut publish = forwarded_with(credentials.publisher());
        publish.shard.incarnation = Some(sent);
        match handler.apply(publish).await {
            InternalMessage::ForwardPublishError(err) => {
                assert_eq!(err.code, ErrorCode::StaleRoute, "{}", err.detail);
                assert!(err.detail.contains("incarnation 5"), "{}", err.detail);
            }
            other => panic!("incarnation {sent} was not refused: {other:?}"),
        }
        assert_eq!(tail().await, 0, "incarnation {sent} reached the log");

        let mut op = forwarded_cache_op(
            CacheOpKind::Put,
            credentials.token(&[&format!("cache.write:cache:{TENANT}/{NAMESPACE}/{CACHE}")]),
        );
        op.shard.incarnation = Some(sent);
        match handler.apply_cache_op(op).await {
            InternalMessage::ForwardCacheError(err) => {
                assert_eq!(err.code, ErrorCode::StaleRoute, "{}", err.detail);
            }
            other => panic!("cache incarnation {sent} was not refused: {other:?}"),
        }
    }

    for sent in [Some(5), None] {
        let mut publish = forwarded_with(credentials.publisher());
        publish.shard.incarnation = sent;
        assert!(
            matches!(
                handler.apply(publish).await,
                InternalMessage::ForwardPublishOk(_)
            ),
            "a forward naming {sent:?} was refused",
        );
    }
    assert_eq!(tail().await, 2);
}

/// The forwarded wait lasts the handler's timeout when the mark is below the
/// batch, and ends at once when the mark passes it. A handler built without
/// marks would answer both at once.
#[tokio::test]
async fn a_forwarded_quorum_publish_waits_its_timeout_then_is_released_by_the_mark() {
    let (broker, _dir) = broker_with(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    let key = crate::shards::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    };
    marks.publish(&key, GENERATION, 0);
    let timeout = Duration::from_millis(300);
    let handler = handler(broker, Some(Arc::clone(&marks)), timeout);

    let started = std::time::Instant::now();
    let answer = handler.apply(forwarded()).await;
    let waited = started.elapsed();
    assert!(
        matches!(answer, InternalMessage::ForwardPublishError(_)),
        "{:?}",
        answer.kind()
    );
    assert!(waited >= timeout, "ended early after {waited:?}");
    assert!(waited < timeout * 6, "ended late after {waited:?}");

    marks.publish(&key, GENERATION, 1_000);
    let started = std::time::Instant::now();
    let answer = handler.apply(forwarded()).await;
    assert!(
        matches!(answer, InternalMessage::ForwardPublishOk(_)),
        "{:?}",
        answer.kind()
    );
    assert!(started.elapsed() < timeout, "waited though the mark passed");
}

/// A forwarded cache or counter op makes the same quorum waits as a local
/// one, on the marks and timeout the owner holds. Without marks each of these
/// would be answered at once.
mod quorum_waits {
    use super::*;
    use crate::test_support::leader::{self, GENERATION, Leader, NAMESPACE, QUORUM_CACHE, TENANT};

    const TIMEOUT: Duration = Duration::from_millis(300);

    fn owner(
        leader: &Leader,
        credentials: &Credentials,
        marks: &Arc<QuorumMarks>,
    ) -> ForwardingHandler {
        ForwardingHandler::new(
            Arc::clone(&leader.broker),
            Arc::new(crate::shards::routing::StreamEngine::new(
                Some(Arc::clone(marks)),
                Some(Arc::clone(&leader.ingress)),
            )),
            Arc::clone(&leader.router),
            "10.0.0.1:7001".to_string(),
            TIMEOUT,
            Arc::clone(&credentials.auth),
        )
    }

    fn op(credentials: &Credentials, op: CacheOpKind, value: Bytes) -> ForwardCacheOp {
        ForwardCacheOp {
            correlation_id: 2,
            shard: ShardRef {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: QUORUM_CACHE.to_string(),
                shard: 0,
                generation: GENERATION,
                incarnation: None,
            },
            op,
            key: "session:abc".to_string(),
            value,
            ttl_ms: 0,
            credential: credentials.token(&[
                &format!("cache.write:cache:{TENANT}/{NAMESPACE}/{QUORUM_CACHE}"),
                &format!("cache.read:cache:{TENANT}/{NAMESPACE}/{QUORUM_CACHE}"),
            ]),
            condition: None,
        }
    }

    fn ops(credentials: &Credentials) -> Vec<(&'static str, ForwardCacheOp)> {
        let add = felix_storage::counter_log::encode_sum(2);
        vec![
            (
                "put",
                op(credentials, CacheOpKind::Put, Bytes::from_static(b"v")),
            ),
            ("delete", op(credentials, CacheOpKind::Delete, Bytes::new())),
            ("get", op(credentials, CacheOpKind::Get, Bytes::new())),
            ("counter add", op(credentials, CacheOpKind::CounterAdd, add)),
            (
                "counter get",
                op(credentials, CacheOpKind::CounterGet, Bytes::new()),
            ),
        ]
    }

    #[tokio::test]
    async fn a_forwarded_quorum_cache_op_waits_its_timeout_then_is_released_by_the_mark() {
        let leader = Leader::start().await;
        let _lease = leader.hold_lease();
        let credentials = Credentials::new();
        let marks = Arc::new(QuorumMarks::new());
        let handler = owner(&leader, &credentials, &marks);

        for (what, forwarded) in ops(&credentials) {
            let started = std::time::Instant::now();
            let answer = handler.apply_cache_op(forwarded).await;
            let waited = started.elapsed();
            assert!(
                matches!(answer, InternalMessage::ForwardCacheError(_)),
                "{what}: {:?}",
                answer.kind()
            );
            assert!(waited >= TIMEOUT, "{what} ended early after {waited:?}");
            assert!(waited < TIMEOUT * 6, "{what} ended late after {waited:?}");
        }

        let cache = leader::quorum_cache_key();
        marks.publish(&cache, GENERATION, 1 << 20);
        marks.counters().publish(&cache, GENERATION, 1 << 20);
        for (what, forwarded) in ops(&credentials) {
            let started = std::time::Instant::now();
            let answer = handler.apply_cache_op(forwarded).await;
            assert!(
                matches!(answer, InternalMessage::ForwardCacheOk(_)),
                "{what}: {:?}",
                answer.kind()
            );
            assert!(
                started.elapsed() < TIMEOUT,
                "{what} waited though the mark passed"
            );
        }
    }

    /// Where the fleet reads by round, a read does not need the lease before
    /// its value; the round confirms leadership after it.
    #[tokio::test]
    async fn a_forwarded_read_by_round_is_not_refused_for_a_lapsed_lease() {
        use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
        struct Confirms;
        #[async_trait::async_trait]
        impl felix_replication::quorum::LeadershipCheck for Confirms {
            async fn confirm(
                &self,
                _key: &crate::shards::ShardKey,
                _generation: u64,
            ) -> Result<(), felix_replication::quorum::QuorumError> {
                Ok(())
            }
        }

        let leader = Leader::start().await;
        let lease = leader.hold_lease();
        let credentials = Credentials::new();
        let names = [
            GENERATION_START.name(),
            MAJORITY_ACK.name(),
            LEASE_FREE_READS.name(),
        ];
        let fleet = Arc::new(FleetGate::new(names));
        let marks = Arc::new(QuorumMarks::with_fleet(Arc::clone(&fleet)));
        marks.set_read_check(Arc::new(Confirms), false);
        fleet.observe(names);
        let cache = leader::quorum_cache_key();
        marks.publish(&cache, GENERATION, 1 << 20);
        marks.counters().publish(&cache, GENERATION, 1 << 20);
        let handler = owner(&leader, &credentials, &marks);

        lease.surrender();
        for (what, forwarded) in ops(&credentials)
            .into_iter()
            .filter(|(what, _)| what.ends_with("get"))
        {
            let answer = handler.apply_cache_op(forwarded).await;
            assert!(
                matches!(answer, InternalMessage::ForwardCacheOk(_)),
                "{what}: {:?}",
                answer.kind()
            );
        }
    }
}

#[cfg(feature = "raft")]
mod on_raft {
    use super::*;
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    /// **A forward to a Raft shard's leader is proposed to its group and
    /// answered with its offsets at the commit**, whatever generation the
    /// requester saw. The hybrid here has no route for the shard and its
    /// fence is closed, so a forward that reached either is refused.
    #[tokio::test]
    async fn a_forward_to_a_raft_leader_commits_through_its_group() {
        let root = tempfile::tempdir().expect("tempdir");
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let orders = fixture::key(STREAM, 0);
        let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
        shards.take(&mut fixture::assignments([assigned])).await;
        fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
        let router = Arc::new(ShardRouter::new(
            LOCAL,
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        let ingress = Arc::new(IngressRouter::new(Arc::clone(&router), Arc::default()));
        ingress.set_engine_shards(Arc::clone(&shards) as Arc<dyn EngineShards>);
        let engine = Arc::new(StreamEngine::new(None, Some(ingress)));
        engine.set_raft(Arc::clone(&shards));
        let handler = ForwardingHandler::new(
            broker,
            engine,
            router,
            "10.0.0.1:7001".to_string(),
            Duration::from_secs(10),
            Credentials::new().auth,
        );

        let first = match handler.apply(forwarded()).await {
            InternalMessage::ForwardPublishOk(ok) => ok.offsets().expect("offsets"),
            other => panic!("expected an acknowledgement, got {other:?}"),
        };
        assert_eq!(first.0, first.1);
        match handler.apply(forwarded()).await {
            InternalMessage::ForwardPublishOk(ok) => {
                assert_eq!(ok.offsets(), Some((first.0 + 1, first.0 + 1)));
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }

        // A sequenced forward is written once however often it arrives.
        let plain = |payload| ForwardPublish {
            headers: None,
            ..sequenced(payload)
        };
        let third = match handler.apply(plain(b"sequenced")).await {
            InternalMessage::ForwardPublishOk(ok) => ok.offsets().expect("offsets"),
            other => panic!("expected an acknowledgement, got {other:?}"),
        };
        assert_eq!(third, (first.0 + 2, first.0 + 2));
        match handler.apply(plain(b"sequenced")).await {
            InternalMessage::ForwardPublishOk(ok) => assert_eq!(ok.offsets(), Some(third)),
            other => panic!("expected the first send's answer, got {other:?}"),
        }
        // A different batch under the sequence is refused, as on the hybrid.
        match handler.apply(plain(b"another")).await {
            InternalMessage::ForwardPublishError(err) => {
                assert_eq!(err.code, ErrorCode::StorageFailed);
                assert!(err.detail.contains("sequence"), "{}", err.detail);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        match handler.apply(forwarded()).await {
            InternalMessage::ForwardPublishOk(ok) => {
                assert_eq!(ok.offsets(), Some((third.0 + 1, third.0 + 1)));
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }

        // What a Raft stream cannot carry is refused by name.
        match handler.apply(sequenced(b"headered")).await {
            InternalMessage::ForwardPublishError(err) => {
                assert_eq!(err.code, ErrorCode::UnsupportedKind);
                assert!(err.detail.contains("record headers"), "{}", err.detail);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        // So is a sequenced batch of several records, with nothing written.
        let mut several = plain(b"several");
        several.payloads.push(Bytes::from_static(b"records"));
        if let Some(producer) = several.producer.as_mut() {
            producer.sequence += 1;
        }
        match handler.apply(several).await {
            InternalMessage::ForwardPublishError(err) => {
                // Not `UnsupportedKind`, which the requester answers by
                // sending the batch again in the older form.
                assert_eq!(err.code, ErrorCode::StorageFailed);
                assert!(err.detail.contains("more than one"), "{}", err.detail);
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        match handler.apply(forwarded()).await {
            InternalMessage::ForwardPublishOk(ok) => {
                assert_eq!(ok.offsets(), Some((third.0 + 2, third.0 + 2)));
            }
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
        EngineShards::shutdown(&*shards).await;
    }

    use felix_replication::peer::pool::{PeerError, PeerRequester};
    use felix_wire::internal::PeerCapabilities;

    /// A trio's forwarding handlers, as the requester's network.
    struct Handlers {
        by_node: HashMap<String, ForwardingHandler>,
        asked: std::sync::Mutex<Vec<String>>,
    }

    impl PeerRequester for Handlers {
        async fn request(
            &self,
            node_id: &str,
            _addr: std::net::SocketAddr,
            message: InternalMessage,
        ) -> std::result::Result<InternalMessage, PeerError> {
            self.asked.lock().expect("lock").push(node_id.to_string());
            Ok(self.by_node[node_id].handle(message).await)
        }

        fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
            Some(PeerCapabilities::FORWARD_OFFSETS)
        }

        async fn capabilities(
            &self,
            _node_id: &str,
            _addr: std::net::SocketAddr,
        ) -> std::result::Result<PeerCapabilities, PeerError> {
            Ok(PeerCapabilities::NONE)
        }
    }

    /// **A forward sent to a member that does not lead is answered with the
    /// leader, and the requester follows it there**, where the batch commits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_forward_to_a_member_that_does_not_lead_is_followed_to_the_leader() {
        use crate::serving::forward::{ForwardKey, ForwardTarget, forward_publish};

        let (_net, members, leader) = fixture::trio().await;
        let orders = fixture::key(STREAM, 0);
        let credentials = Credentials::new();
        let by_node = members
            .iter()
            .map(|member| {
                let (router, ingress) = fixture::routes(&member.shards);
                let engine = Arc::new(StreamEngine::new(None, Some(ingress)));
                engine.set_raft(Arc::clone(&member.shards));
                let handler = ForwardingHandler::new(
                    Arc::clone(&member.broker),
                    engine,
                    router,
                    fixture::addr_of(member.name).expect("addr").to_string(),
                    Duration::from_secs(10),
                    Arc::clone(&credentials.auth),
                );
                (member.name.to_string(), handler)
            })
            .collect();
        let handlers = Handlers {
            by_node,
            asked: std::sync::Mutex::default(),
        };
        let follower = &members[(leader + 1) % members.len()];
        fixture::until("the follower knows who leads", || {
            matches!(
                EngineShards::dispatch(&*follower.shards, &orders, &fixture::addr_of),
                Some(Dispatch::Forward { .. })
            )
        })
        .await;
        // A requester whose routes are behind: it still names the follower.
        let stale = ForwardTarget {
            node_id: follower.name.to_string(),
            advertise_addr: fixture::addr_of(follower.name).expect("addr"),
            generation: 0,
            incarnation: 0,
        };
        let key = ForwardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
        };

        let offsets = forward_publish(
            &handlers,
            &stale,
            &key,
            AckMode::OnCommit,
            &credentials.publisher(),
            vec![Bytes::from_static(b"forwarded")],
            None,
            None,
            Duration::from_secs(30),
        )
        .await
        .expect("forwarded")
        .expect("offsets");

        assert_eq!(
            *handlers.asked.lock().expect("lock"),
            [follower.name, members[leader].name]
        );
        let read = members[leader]
            .broker
            .read_durable(TENANT, NAMESPACE, STREAM, 0, offsets.0, 1 << 20)
            .await
            .expect("read");
        assert_eq!(&read[0].payload[..], b"forwarded");
        fixture::stop(&members).await;
    }
}
