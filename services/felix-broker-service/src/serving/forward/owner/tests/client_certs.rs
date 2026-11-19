//! **An owner that binds token subjects to client certificates checks a
//! forward's binding itself.** The forwarder checked the client's certificate
//! at login, but the owner cannot know that: a forward must carry a chain
//! that this broker's client CA issued, to the token's subject, or it is
//! refused before anything is written. Every forwarded request takes this
//! path: publishes, cache ops, counter ops and lease claims.

use felix_wire::internal::ClientCertified;

use super::*;
use crate::serving::tls::PRINCIPAL_URI_PREFIX;

/// The subject `Credentials::token` mints for.
const SUBJECT: &str = "p:test";

/// A client CA, and the chains it issues.
struct Ca {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    root: rustls::pki_types::CertificateDer<'static>,
}

impl Ca {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let root = params.self_signed(&key).expect("ca cert").der().clone();
        Self {
            issuer: rcgen::Issuer::new(params, key),
            root,
        }
    }

    /// A one-certificate chain issued to `principal`.
    fn chain_for(&self, principal: &str) -> Vec<Bytes> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names.push(rcgen::SanType::URI(
            format!("{PRINCIPAL_URI_PREFIX}{principal}")
                .try_into()
                .expect("uri"),
        ));
        let key = rcgen::KeyPair::generate().expect("key");
        let cert = params.signed_by(&key, &self.issuer).expect("sign");
        vec![Bytes::copy_from_slice(cert.der())]
    }

    fn verifier(&self) -> Arc<dyn rustls::server::danger::ClientCertVerifier> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.root.clone()).expect("root");
        rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            felix_replication::peer::tls::provider(),
        )
        .build()
        .expect("verifier")
    }
}

/// An owner with a stream, a cache and counters, binding subjects or not.
struct Owner {
    handler: ForwardingHandler,
    broker: Arc<Broker>,
    credentials: Credentials,
    ca: Ca,
    _dir: TempDir,
}

impl Owner {
    async fn start(bind: bool) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        let storage =
            DurableStorage::open(dir.path().join("streams"), config.clone()).expect("storage");
        let counters = felix_storage::CounterStore::open(dir.path().join("counters"), config)
            .expect("counters");
        let broker = Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_counters(Arc::new(counters));
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
                    consistency: ConsistencyLevel::Leader,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
        broker
            .register_cache(
                TENANT,
                NAMESPACE,
                CACHE,
                felix_broker::CacheMetadata::default(),
            )
            .await
            .expect("cache");
        let broker = Arc::new(broker);
        let credentials = Credentials::new();
        let ca = Ca::new();
        let lease = crate::serving::lease::LeaseTokens::from_keys(
            &base64::engine::general_purpose::STANDARD.encode([3u8; 32]),
            crate::serving::lease::DEFAULT_MAX_TTL,
        )
        .expect("lease keys");
        let auth = Arc::new(
            (*credentials.auth)
                .clone()
                .with_subject_binding(bind)
                .with_client_verifier(Some(ca.verifier()))
                .with_lease_tokens(Some(Arc::new(lease))),
        );
        let handler = handler_with(Arc::clone(&broker), None, Duration::from_secs(1), auth);
        Self {
            handler,
            broker,
            credentials,
            ca,
            _dir: dir,
        }
    }

    /// Every kind of forwarded request, each allowed by its token.
    fn requests(&self) -> Vec<(&'static str, InternalMessage)> {
        let cache = format!("cache:{TENANT}/{NAMESPACE}/{CACHE}");
        let writer = self.credentials.token(&[
            &format!("cache.read:{cache}"),
            &format!("cache.write:{cache}"),
            &format!("stream.publish:stream:{TENANT}/{NAMESPACE}/wl.{{{CACHE}}}.out"),
        ]);
        let op = |op, value| ForwardCacheOp {
            value,
            ..forwarded_cache_op(op, writer.clone())
        };
        vec![
            (
                "publish",
                InternalMessage::ForwardPublish(forwarded_with(self.credentials.publisher())),
            ),
            (
                "cache put",
                InternalMessage::ForwardCacheOp(op(CacheOpKind::Put, Bytes::from_static(b"v"))),
            ),
            (
                "cache get",
                InternalMessage::ForwardCacheOp(op(CacheOpKind::Get, Bytes::new())),
            ),
            (
                "counter add",
                InternalMessage::ForwardCacheOp(op(
                    CacheOpKind::CounterAdd,
                    felix_storage::counter_log::encode_sum(1),
                )),
            ),
            (
                "counter get",
                InternalMessage::ForwardCacheOp(op(CacheOpKind::CounterGet, Bytes::new())),
            ),
            (
                "lease claim",
                InternalMessage::LeaseForwardCacheOp(ForwardCacheOp {
                    key: "w7".to_string(),
                    ttl_ms: 30_000,
                    condition: Some(ForwardCacheCondition::Absent),
                    ..op(CacheOpKind::Put, Bytes::from_static(b"node-a"))
                }),
            ),
        ]
    }

    /// Send `request` as the internal listener hands it over, with
    /// `client_chain` around it when there is one.
    async fn send(&self, request: InternalMessage, client_chain: Option<Vec<Bytes>>) -> Answer {
        let request = match client_chain {
            Some(client_chain) => InternalMessage::ClientCertified(ClientCertified {
                client_chain,
                message: Box::new(request),
            }),
            None => request,
        };
        Answer::of(self.handler.handle(request).await)
    }

    async fn stream_tail(&self) -> u64 {
        self.broker
            .cursor_tail(TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("tail")
            .next_seq()
    }
}

#[derive(Debug)]
enum Answer {
    Served,
    Unauthorized(String),
    Other(Box<InternalMessage>),
}

impl Answer {
    fn of(message: InternalMessage) -> Self {
        match message {
            InternalMessage::ForwardPublishOk(_)
            | InternalMessage::ForwardCacheOk(_)
            | InternalMessage::LeaseForwardCacheOk(_) => Self::Served,
            InternalMessage::ForwardPublishError(err) if err.code == ErrorCode::Unauthorized => {
                Self::Unauthorized(err.detail)
            }
            InternalMessage::ForwardCacheError(err) if err.code == ErrorCode::Unauthorized => {
                Self::Unauthorized(err.detail)
            }
            other => Self::Other(Box::new(other)),
        }
    }

    fn served(self, name: &str) {
        match self {
            Self::Served => {}
            Self::Unauthorized(detail) => panic!("{name} was refused: {detail}"),
            Self::Other(answer) => panic!("{name} was not served: {answer:?}"),
        }
    }

    fn refused(self, name: &str, why: &str) {
        match self {
            Self::Unauthorized(detail) => assert!(detail.contains(why), "{name}: {detail}"),
            other => panic!("{name} was not refused: {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_binding_owner_serves_a_forward_whose_chain_is_issued_to_the_subject() {
    let owner = Owner::start(true).await;
    for (name, request) in owner.requests() {
        owner
            .send(request, Some(owner.ca.chain_for(SUBJECT)))
            .await
            .served(name);
    }
    assert_eq!(owner.stream_tail().await, 1);
}

#[tokio::test]
async fn a_binding_owner_refuses_a_chain_issued_to_someone_else() {
    let owner = Owner::start(true).await;
    for (name, request) in owner.requests() {
        owner
            .send(request, Some(owner.ca.chain_for("p:someone-else")))
            .await
            .refused(name, "not issued to the token's subject");
    }
    assert_eq!(
        owner.stream_tail().await,
        0,
        "a refused forward was written"
    );
}

#[tokio::test]
async fn a_binding_owner_refuses_a_forward_with_no_chain() {
    let owner = Owner::start(true).await;
    for (name, request) in owner.requests() {
        owner
            .send(request, None)
            .await
            .refused(name, "sent no client certificate");
    }
    assert_eq!(
        owner.stream_tail().await,
        0,
        "a refused forward was written"
    );
}

/// The chain is verified against the owner's own client CA, so a forwarder
/// cannot make up a certificate that names the subject.
#[tokio::test]
async fn a_binding_owner_refuses_a_chain_its_client_ca_did_not_issue() {
    let owner = Owner::start(true).await;
    let elsewhere = Ca::new();
    for (name, request) in owner.requests() {
        owner
            .send(request, Some(elsewhere.chain_for(SUBJECT)))
            .await
            .refused(name, "forwarded client certificate");
    }
    // Not a certificate at all.
    let (name, request) = owner.requests().remove(0);
    owner
        .send(
            request,
            Some(vec![Bytes::from_static(b"not a certificate")]),
        )
        .await
        .refused(name, "forwarded client certificate");
    assert_eq!(
        owner.stream_tail().await,
        0,
        "a refused forward was written"
    );
}

/// Without binding, a forward is served as it always was, with or without
/// a chain, and whoever the chain names.
#[tokio::test]
async fn an_owner_that_does_not_bind_ignores_the_chain() {
    let owner = Owner::start(false).await;
    for (name, request) in owner.requests() {
        owner.send(request, None).await.served(name);
    }
    let (name, request) = owner.requests().remove(0);
    owner
        .send(
            request,
            Some(vec![Bytes::from_static(b"not a certificate")]),
        )
        .await
        .served(name);
    assert_eq!(owner.stream_tail().await, 2);
}

/// The forwarding side keeps the chain a client presented, whether or not it
/// binds itself, since the owner of a shard the client reaches may.
#[tokio::test]
async fn the_ingress_keeps_the_chain_its_client_presented() {
    let credentials = Credentials::new();
    let token = credentials.publisher();
    let chain = Ca::new().chain_for(SUBJECT);
    let presented: Vec<_> = chain
        .iter()
        .map(|cert| rustls::pki_types::CertificateDer::from(cert.as_ref()))
        .collect();
    let context = credentials
        .auth
        .authenticate_peer(TENANT, &token, Some(&presented))
        .await
        .expect("authenticated");
    assert_eq!(context.client_chain.as_deref(), Some(chain.as_slice()));
    assert_eq!(
        context.forward_credential().client_chain.as_deref(),
        Some(chain.as_slice())
    );
    let context = credentials
        .auth
        .authenticate_peer(TENANT, &token, None)
        .await
        .expect("authenticated");
    assert!(context.client_chain.is_none());
}
