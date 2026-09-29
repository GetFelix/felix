//! A publish that arrives at the wrong broker, end to end.
//!
//! Real transports and a real broker on the owning side, so the two halves
//! cannot drift: the ingress forwarder and the owner's handler are the pair
//! under test, and a mismatch between what one sends and the other reads fails
//! here rather than in a cluster.
//!
//! Run with `cargo test -p felix-broker-service --test cross_broker_forwarding`.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_broker_service::serving::forward::{
    ForwardError, ForwardKey, ForwardTarget, ForwardingHandler, forward_publish,
};
use felix_broker_service::shards::routing::{IngressRouter, routing_table_from};
use felix_broker_service::shards::{ShardKey, watch::ShardAssignment};
use felix_replication::peer::{PeerPool, PeerServer, PeerTransportConfig};
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_storage::EphemeralCache;
use felix_wire::internal::{AckMode, ErrorCode, InternalMessage};
use tokio_util::sync::CancellationToken;

/// Long enough that nothing here is bounded by it: these cases are about what
/// the owner answered, not about running out of time.
const FORWARD_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const OWNER: &str = "broker-b";
const TEST_PRIVATE_KEY: [u8; 32] = [10u8; 32];

/// Tokens the owner will verify: minted against a fixed key whose JWKS every
/// owner here is handed directly, so no control plane is involved.
fn issuer() -> (FelixTokenIssuer, Jwks) {
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
    (
        FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            Duration::from_secs(900),
            Arc::new(materials),
        ),
        jwks,
    )
}

fn token(perms: &[&str]) -> String {
    issuer()
        .0
        .mint(
            &TenantId::new(TENANT),
            "p:test",
            perms.iter().map(|perm| perm.to_string()).collect(),
        )
        .expect("mint")
}

/// A credential that may publish to the stream every case here forwards to.
fn publisher_token() -> String {
    token(&[&format!(
        "stream.publish:stream:{TENANT}/{NAMESPACE}/{STREAM}"
    )])
}

fn owner_auth() -> Arc<BrokerAuth> {
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(TENANT), issuer().1);
    Arc::new(BrokerAuth::with_key_store(key_store))
}

fn shard_key() -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_broker_service::shards::ShardKind::Stream,
    }
}

fn forward_key() -> ForwardKey {
    ForwardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
    }
}

fn peer_config() -> PeerTransportConfig {
    PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        request_timeout: Duration::from_millis(500),
        handshake_timeout: Duration::from_millis(500),
        reconnect_base: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
        ..Default::default()
    }
}

fn catalog(entries: &[(&str, SocketAddr)]) -> HashMap<String, NodeRef> {
    entries
        .iter()
        .map(|(node_id, addr)| {
            (
                node_id.to_string(),
                NodeRef {
                    node_id: node_id.to_string(),
                    advertise_addr: *addr,
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect()
}

/// A broker that owns the shard, with the router and ingress view to match.
struct Owner {
    broker: Arc<Broker>,
    ingress: Arc<IngressRouter>,
    router: Arc<ShardRouter>,
}

impl Owner {
    async fn new(node_id: &str, generation: u64, servable: bool) -> Self {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant(TENANT).await.expect("tenant");
        broker
            .register_namespace(TENANT, NAMESPACE)
            .await
            .expect("namespace");
        broker
            .register_stream(TENANT, NAMESPACE, STREAM, StreamMetadata::default())
            .await
            .expect("stream");

        let router = Arc::new(ShardRouter::new(
            node_id,
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        let ingress = Arc::new(IngressRouter::new(Arc::clone(&router), Arc::default()));
        let owner = Self {
            broker,
            ingress,
            router,
        };
        owner.assign(node_id, generation, servable, &[]);
        owner
    }

    /// Point the router at `leader` for this shard, and say whether this broker
    /// has the shard open.
    fn assign(&self, leader: &str, generation: u64, servable: bool, nodes: &[(&str, SocketAddr)]) {
        let assignments: HashMap<ShardKey, ShardAssignment> = [(
            shard_key(),
            ShardAssignment {
                key: shard_key(),
                leader: leader.to_string(),
                replicas: Vec::new(),
                generation,
                state: "active".to_string(),
                successor: None,
                routing: Default::default(),
            },
        )]
        .into_iter()
        .collect();
        let nodes = catalog(nodes);
        self.router
            .publish(routing_table_from(&assignments, &nodes), &nodes);
        // What the shard lifecycle does when it activates or leaves a shard.
        if servable {
            self.ingress.fence().open(&shard_key(), generation);
        } else {
            self.ingress.fence().close(&shard_key());
        }
        self.ingress.publish_servable(if servable {
            [(shard_key(), generation)].into_iter().collect()
        } else {
            HashMap::new()
        });
    }

    fn handler(&self, advertise_addr: &str) -> Arc<ForwardingHandler> {
        Arc::new(ForwardingHandler::new(
            Arc::clone(&self.broker),
            Arc::clone(&self.ingress),
            Arc::clone(&self.router),
            advertise_addr.to_string(),
            // No quorum marks: these exercise forwarding itself, and every
            // stream here is leader-acknowledged.
            None,
            std::time::Duration::from_secs(5),
            owner_auth(),
        ))
    }
}

/// A running internal listener in front of a handler.
struct Listener {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Listener {
    fn start(node_id: &str, handler: Arc<dyn felix_replication::peer::PeerRequestHandler>) -> Self {
        let server = PeerServer::bind(node_id.to_string(), &peer_config(), handler).expect("bind");
        let addr = server.local_addr().expect("addr");
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));
        Self {
            addr,
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.task).await;
    }
}

fn pool() -> Arc<PeerPool> {
    PeerPool::new(
        "broker-a".to_string(),
        peer_config(),
        CancellationToken::new(),
    )
    .expect("pool")
}

fn target(addr: SocketAddr, generation: u64) -> ForwardTarget {
    ForwardTarget {
        node_id: OWNER.to_string(),
        advertise_addr: addr,
        generation,
    }
}

/// The acceptance criterion: a publish sent to a non-owner is persisted by the
/// owner and acknowledged back through the ingress broker.
#[tokio::test]
async fn a_forwarded_publish_is_written_by_the_owner() {
    let owner = Owner::new(OWNER, 1, true).await;
    // Subscribed before the forward, so a delivery proves the owner applied it
    // rather than merely answering.
    let mut subscription = owner
        .broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let listener = Listener::start(OWNER, owner.handler("10.0.0.5:7000"));
    let pool = pool();

    let offsets = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"hello")],
        FORWARD_BUDGET,
    )
    .await
    .expect("the owner must accept a publish for a shard it owns");

    // Ephemeral stream: no log, so no offsets to report.
    assert_eq!(offsets, Some((0, 0)));

    let delivered = tokio::time::timeout(Duration::from_secs(2), subscription.recv())
        .await
        .expect("the owner never delivered the forwarded record")
        .expect("subscription closed");
    assert_eq!(delivered, Bytes::from_static(b"hello"));

    pool.shutdown().await;
    listener.stop().await;
}

/// The requester resolved against a newer assignment than the owner has seen.
/// The owner must refuse: it may already have lost the shard.
#[tokio::test]
async fn an_owner_behind_the_requester_refuses_rather_than_writing() {
    let owner = Owner::new(OWNER, 1, true).await;
    let mut subscription = owner
        .broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let listener = Listener::start(OWNER, owner.handler("10.0.0.5:7000"));
    let pool = pool();

    // Generation 5 against an owner still at 1.
    let err = forward_publish(
        &pool,
        &target(listener.addr, 5),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"too-new")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("an owner behind the requester must not accept the write");

    assert!(
        matches!(err, ForwardError::Refused { .. }),
        "nothing was written, so this is a refusal: {err:?}",
    );
    assert!(
        subscription.try_recv().is_err(),
        "a refused forward must not have been applied",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// The shard moved. The owner answers with where it went rather than writing.
#[tokio::test]
async fn a_broker_that_no_longer_owns_the_shard_redirects() {
    let owner = Owner::new(OWNER, 1, true).await;
    let listener = Listener::start(OWNER, owner.handler("10.0.0.5:7000"));
    let pool = pool();

    // The shard is reassigned to a broker this one knows the address of.
    owner.assign(
        "broker-c",
        2,
        false,
        &[("broker-c", "127.0.0.1:9".parse().expect("addr"))],
    );

    let err = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"moved")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("broker-c is not reachable, so this cannot succeed");

    // The failure has to name broker-c: that is the difference between
    // following the redirect and simply being refused by broker-b.
    assert!(
        err.to_string().contains("broker-c"),
        "the redirect was not followed: {err}",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A redirect back to the broker the batch was just sent to, at the generation
/// it was already sent at, is a loop. Bounded or not, following it would bounce
/// the batch between two brokers that disagree.
///
/// A redirect to a broker *not* yet tried is a different thing entirely — a
/// correction — and is followed even at the same generation, because on a
/// freshly formed cluster every shard is at generation 0 and the correct owner
/// has nothing higher to offer. That case is covered in the `forward` unit
/// tests.
#[tokio::test]
async fn a_redirect_that_loops_back_to_a_tried_owner_is_refused() {
    use async_trait::async_trait;
    use felix_wire::internal::NotLeader;

    // Redirects to the very node the batch was just sent to, at the same
    // generation: the loop closes immediately.
    struct AlwaysRedirectsBackwards;

    #[async_trait]
    impl felix_replication::peer::PeerRequestHandler for AlwaysRedirectsBackwards {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            InternalMessage::NotLeader(NotLeader {
                correlation_id: request.correlation_id(),
                node_id: "broker-b".to_string(),
                advertise_addr: "127.0.0.1:1".to_string(),
                // The same generation the requester already had.
                generation: 1,
            })
        }
    }

    let listener = Listener::start(OWNER, Arc::new(AlwaysRedirectsBackwards));
    let pool = pool();

    let err = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"loop")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("a backwards redirect must be refused, not followed");
    assert!(
        err.to_string().contains("loop"),
        "the error should say why the redirect was rejected: {err}",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A retryable refusal is retried, and converges when the owner becomes ready.
#[tokio::test]
async fn a_retryable_refusal_converges() {
    use async_trait::async_trait;
    use felix_wire::internal::{ForwardPublishError, ForwardPublishOk};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Refuses once with a retryable code, then accepts.
    struct UnavailableOnce {
        seen: AtomicUsize,
    }

    #[async_trait]
    impl felix_replication::peer::PeerRequestHandler for UnavailableOnce {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            if self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
                return InternalMessage::ForwardPublishError(ForwardPublishError {
                    correlation_id: request.correlation_id(),
                    code: ErrorCode::Unavailable,
                    detail: "still opening the shard".to_string(),
                });
            }
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id: request.correlation_id(),
                first_offset: 7,
                last_offset: 7,
            })
        }
    }

    let listener = Listener::start(
        OWNER,
        Arc::new(UnavailableOnce {
            seen: AtomicUsize::new(0),
        }),
    );
    let pool = pool();

    let offsets = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"retry-me")],
        FORWARD_BUDGET,
    )
    .await
    .expect("a retryable refusal must be retried");
    assert_eq!(offsets, Some((7, 7)));

    pool.shutdown().await;
    listener.stop().await;
}

/// An owner that keeps refusing must fail explicitly within the attempt budget,
/// not retry forever.
#[tokio::test]
async fn a_permanently_refusing_owner_fails_within_the_budget() {
    use async_trait::async_trait;
    use felix_wire::internal::ForwardPublishError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct AlwaysUnavailable {
        seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl felix_replication::peer::PeerRequestHandler for AlwaysUnavailable {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            self.seen.fetch_add(1, Ordering::SeqCst);
            InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: request.correlation_id(),
                code: ErrorCode::Unavailable,
                detail: "never ready".to_string(),
            })
        }
    }

    let seen = Arc::new(AtomicUsize::new(0));
    let listener = Listener::start(
        OWNER,
        Arc::new(AlwaysUnavailable {
            seen: Arc::clone(&seen),
        }),
    );
    let pool = pool();

    let started = std::time::Instant::now();
    let err = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"never")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("a permanent refusal must fail, not loop");

    assert!(matches!(err, ForwardError::Refused { .. }), "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the attempt budget did not bound this",
    );
    assert!(
        seen.load(Ordering::SeqCst) <= 3,
        "attempts must be bounded, saw {}",
        seen.load(Ordering::SeqCst),
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// The duplicate-delivery case, and the reason the retry rule is what it is.
///
/// The owner applies the batch and the answer never arrives. Retrying would
/// write it twice, and the ingress broker has no way to tell that from a batch
/// that never landed — so it must not retry, and must say the outcome is
/// unknown rather than claim failure.
#[tokio::test]
async fn a_lost_answer_is_reported_as_indeterminate_and_never_retried() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Applies the write, then never answers.
    struct WritesThenGoesSilent {
        broker: Arc<Broker>,
        applied: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl felix_replication::peer::PeerRequestHandler for WritesThenGoesSilent {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            if let InternalMessage::ForwardPublish(publish) = &request {
                self.broker
                    .publish_batch(TENANT, NAMESPACE, STREAM, 0, &publish.payloads)
                    .await
                    .expect("publish");
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            // The answer is lost. This is the moment the requester cannot
            // reason about.
            std::future::pending::<()>().await;
            unreachable!()
        }
    }

    let owner = Owner::new(OWNER, 1, true).await;
    let mut subscription = owner
        .broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let applied = Arc::new(AtomicUsize::new(0));
    let listener = Listener::start(
        OWNER,
        Arc::new(WritesThenGoesSilent {
            broker: Arc::clone(&owner.broker),
            applied: Arc::clone(&applied),
        }),
    );
    let pool = pool();

    let err = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"exactly-once")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("a lost answer cannot be reported as success");

    assert!(
        matches!(err, ForwardError::Indeterminate { .. }),
        "the outcome is unknown, and saying 'refused' would be a claim this \
         broker cannot make: {err:?}",
    );
    assert_eq!(
        applied.load(Ordering::SeqCst),
        1,
        "the batch must not have been sent a second time",
    );

    let delivered = tokio::time::timeout(Duration::from_secs(2), subscription.recv())
        .await
        .expect("nothing delivered")
        .expect("subscription closed");
    assert_eq!(delivered, Bytes::from_static(b"exactly-once"));
    assert!(
        subscription.try_recv().is_err(),
        "the record must appear exactly once",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// A peer that is not there fails without the batch ever being sent, so the
/// caller is free to retry elsewhere.
#[tokio::test]
async fn an_unreachable_owner_fails_without_sending() {
    let dead: SocketAddr = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
        socket.local_addr().expect("addr")
    };
    let pool = pool();

    let err = forward_publish(
        &pool,
        &target(dead, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"nowhere")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("an unreachable owner must fail");
    assert!(
        matches!(err, ForwardError::Refused { .. }),
        "nothing was sent, so this is not indeterminate: {err:?}",
    );

    pool.shutdown().await;
}

/// A broker that owns the shard but has not opened it must refuse rather than
/// acknowledge a write against a log it has not recovered.
#[tokio::test]
async fn an_owner_that_has_not_opened_the_shard_refuses() {
    let owner = Owner::new(OWNER, 1, false).await;
    let listener = Listener::start(OWNER, owner.handler("10.0.0.5:7000"));
    let pool = pool();

    let err = forward_publish(
        &pool,
        &target(listener.addr, 1),
        &forward_key(),
        AckMode::OnCommit,
        &publisher_token(),
        vec![Bytes::from_static(b"not-yet")],
        FORWARD_BUDGET,
    )
    .await
    .expect_err("an unopened shard must not accept writes");
    assert!(matches!(err, ForwardError::Refused { .. }), "{err:?}");

    pool.shutdown().await;
    listener.stop().await;
}

/// **The owner refuses a forward whose credential cannot publish, over the
/// real transport, and writes nothing.** The ingress broker would have refused
/// this before forwarding; the owner cannot know whether it did, which is the
/// gap #503 named. Also the credential-less legacy frame, which is what a
/// forwarder from before this build -- or something that is not a broker at
/// all -- sends.
#[tokio::test]
async fn a_forward_the_client_was_not_entitled_to_is_refused_by_the_owner() {
    let owner = Owner::new(OWNER, 1, true).await;
    let mut subscription = owner
        .broker
        .subscribe(TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("subscribe");
    let listener = Listener::start(OWNER, owner.handler("10.0.0.5:7000"));
    let pool = pool();

    let subscriber = token(&[&format!(
        "stream.subscribe:stream:{TENANT}/{NAMESPACE}/{STREAM}"
    )]);
    for credential in [subscriber.as_str(), ""] {
        let err = forward_publish(
            &pool,
            &target(listener.addr, 1),
            &forward_key(),
            AckMode::OnCommit,
            credential,
            vec![Bytes::from_static(b"not yours")],
            FORWARD_BUDGET,
        )
        .await
        .expect_err("the owner accepted a publish the client could not make");
        match err {
            ForwardError::Refused { detail, .. } => {
                assert!(detail.contains("Unauthorized"), "{detail}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    // Nothing reached the stream.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.recv())
            .await
            .is_err(),
        "a refused forward was delivered",
    );

    pool.shutdown().await;
    listener.stop().await;
}
