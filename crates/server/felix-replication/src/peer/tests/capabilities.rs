//! Capability negotiation in both directions, and the fence never reaching a
//! peer that did not say it answers it.

use std::sync::atomic::AtomicUsize;

use bytes::{Bytes, BytesMut};
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::internal::{
    Fence, FrameEnvelope, Hello, HelloOk, InternalHeader, Kind, PeerCapabilities, ReplicaLog,
    correlation_id_in,
};

use super::*;
use crate::peer::codec::{Incoming, read_frame, write_frame};

fn fence() -> InternalMessage {
    InternalMessage::Fence(Fence {
        correlation_id: 0,
        shard: shard(),
        log: ReplicaLog::Stream,
    })
}

/// A listener as a build from before capabilities: it knows kinds up to
/// `ReplicateCommittedRecords` and refuses the rest as unknown, which is all
/// an older broker can do with them. Counts every frame it refused.
struct OldBroker {
    addr: SocketAddr,
    refused: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl OldBroker {
    const LAST_KNOWN_KIND: u16 = Kind::ReplicateCommittedRecords as u16;

    async fn start(node_id: &'static str) -> Self {
        let server = QuicServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            crate::peer::tls::server_config(None).expect("server config"),
            config().quic_transport(),
        )
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let refused = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&refused);
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                        let counted = Arc::clone(&counted);
                        tokio::spawn(async move {
                            while let Some((kind, frame)) = read_raw(&mut recv).await {
                                let answer = Self::answer(node_id, kind, frame, &counted);
                                if write_frame(&mut send, &answer).await.is_err() {
                                    return;
                                }
                            }
                        });
                    }
                });
            }
        });
        Self {
            addr,
            refused,
            task,
        }
    }

    fn answer(node_id: &str, kind: u16, frame: Bytes, refused: &AtomicUsize) -> InternalMessage {
        let correlation_id = correlation_id_in(&frame[InternalHeader::LEN..]).unwrap_or_default();
        if kind > Self::LAST_KNOWN_KIND {
            refused.fetch_add(1, Ordering::SeqCst);
            return InternalMessage::ForwardPublishError(
                felix_wire::internal::ForwardPublishError {
                    correlation_id,
                    code: ErrorCode::UnsupportedKind,
                    detail: format!("this broker does not know frame kind {kind}"),
                },
            );
        }
        match InternalMessage::decode(frame).expect("an old kind decodes") {
            InternalMessage::Hello(hello) => {
                assert!(hello.capabilities.is_none());
                InternalMessage::HelloOk(HelloOk {
                    correlation_id,
                    node_id: node_id.to_string(),
                    capabilities: None,
                })
            }
            _ => InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id,
                first_offset: 1,
                last_offset: 1,
            }),
        }
    }

    fn stop(self) {
        self.task.abort();
    }
}

/// One frame, whatever its kind, or `None` once the stream ends.
async fn read_raw(recv: &mut quinn::RecvStream) -> Option<(u16, Bytes)> {
    let mut head = [0u8; InternalHeader::LEN];
    recv.read_exact(&mut head).await.ok()?;
    let envelope = FrameEnvelope::decode(&Bytes::copy_from_slice(&head)).ok()?;
    let mut frame = BytesMut::from(&head[..]);
    frame.resize(InternalHeader::LEN + envelope.length as usize, 0);
    recv.read_exact(&mut frame[InternalHeader::LEN..])
        .await
        .ok()?;
    Some((envelope.kind, frame.freeze()))
}

/// A listener that notes what its callers offer, and whether the fence is on.
async fn listener_noting(fence: bool) -> (Listener, KnownCapabilities) {
    let known = KnownCapabilities::default();
    let config = PeerTransportConfig { fence, ..config() };
    let server = Listener::bind_with_retry(PEER, Arc::new(CountingHandler::default()), &config)
        .await
        .with_known_capabilities(known.clone());
    let addr = server.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(server.serve(shutdown.clone()));
    (
        Listener {
            addr,
            shutdown,
            task,
        },
        known,
    )
}

/// **Two brokers with the fence each learn the other has it.** The dialler
/// from the answer, the listener from the offer.
#[tokio::test]
async fn two_current_brokers_learn_each_others_capabilities() {
    let (listener, noted) = listener_noting(true).await;
    let pool = pool(config());

    let theirs = pool
        .capabilities(PEER, listener.addr)
        .await
        .expect("handshake");

    let offered = PeerCapabilities::FENCE
        .union(PeerCapabilities::TAIL_FETCH)
        .union(PeerCapabilities::GENERATION_LABELS)
        .union(PeerCapabilities::FORWARD_OFFSETS);
    assert_eq!(theirs, offered);
    assert_eq!(pool.known_capabilities().get(PEER), Some(offered));
    assert_eq!(noted.get("broker-a"), Some(offered));

    pool.shutdown().await;
    listener.stop().await;
}

/// **A broker that predates the bits is greeted the old way and never sent a
/// fence.** It refuses the capable `Hello` as an unknown kind; the pool says
/// `Hello` again, gets on with every request it always made, and refuses to
/// send the fence itself.
#[tokio::test]
async fn an_older_broker_is_never_sent_the_fence() {
    let old = OldBroker::start(PEER).await;
    let pool = pool(config());

    assert_eq!(
        pool.capabilities(PEER, old.addr).await.expect("handshake"),
        PeerCapabilities::NONE
    );
    assert!(
        matches!(
            pool.request(PEER, old.addr, forward()).await,
            Ok(InternalMessage::ForwardPublishOk(_))
        ),
        "the older broker must still be spoken to",
    );
    let refused_after_handshake = old.refused.load(Ordering::SeqCst);
    assert_eq!(
        refused_after_handshake, 1,
        "only the capable Hello should have been refused"
    );

    let err = pool
        .request(PEER, old.addr, fence())
        .await
        .expect_err("the fence must not be sent");
    assert!(matches!(err, PeerError::Unsupported { .. }), "{err:?}");
    assert_eq!(
        old.refused.load(Ordering::SeqCst),
        refused_after_handshake,
        "the fence reached a broker that never said it answers it",
    );

    pool.shutdown().await;
    old.stop();
}

/// **An older broker dialling this one is answered as it expects** -- the
/// original `HelloOk`, which is all it can read -- and noted as having none
/// of the capabilities.
#[tokio::test]
async fn an_older_broker_dialling_in_is_answered_the_old_way() {
    let (listener, noted) = listener_noting(true).await;
    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");
    let connection = client
        .connect(listener.addr, crate::peer::tls::INTERNAL_SERVER_NAME)
        .await
        .expect("connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("open");

    write_frame(
        &mut send,
        &InternalMessage::Hello(Hello {
            correlation_id: 5,
            node_id: "broker-old".to_string(),
            capabilities: None,
        }),
    )
    .await
    .expect("write");
    let (kind, _) = read_raw(&mut recv).await.expect("an answer");

    assert_eq!(
        kind,
        Kind::HelloOk as u16,
        "an older broker cannot read any other answer",
    );
    assert_eq!(noted.get("broker-old"), Some(PeerCapabilities::NONE));

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **A broker with the fence turned off is an older broker to whoever sends
/// one**: it offers nothing of the fence, the pool will not send it, and one
/// sent by hand is refused as an unknown kind. Labels and forwarded offsets
/// are not the fence's, so they are still offered.
#[tokio::test]
async fn a_broker_with_the_fence_off_neither_offers_nor_answers_it() {
    let (listener, _noted) = listener_noting(false).await;
    let pool = pool(config());

    assert_eq!(
        pool.capabilities(PEER, listener.addr)
            .await
            .expect("handshake"),
        PeerCapabilities::GENERATION_LABELS.union(PeerCapabilities::FORWARD_OFFSETS)
    );
    let err = pool
        .request(PEER, listener.addr, fence())
        .await
        .expect_err("the fence must not be sent");
    assert!(matches!(err, PeerError::Unsupported { .. }), "{err:?}");

    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");
    let connection = client
        .connect(listener.addr, crate::peer::tls::INTERNAL_SERVER_NAME)
        .await
        .expect("connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("open");
    write_frame(&mut send, &fence()).await.expect("write");
    let Incoming::Message(InternalMessage::ForwardPublishError(refusal)) =
        read_frame(&mut recv).await.expect("read")
    else {
        panic!("expected the fence to be refused");
    };
    assert_eq!(refusal.code, ErrorCode::UnsupportedKind);

    connection.close(0u32.into(), b"done");
    pool.shutdown().await;
    listener.stop().await;
}

/// Holds every forwarded publish for a long time and answers the fence at
/// once, as a broker waiting on a quorum for the forward would.
struct SlowForwards;

#[async_trait::async_trait]
impl PeerRequestHandler for SlowForwards {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::Fence(fence) => {
                InternalMessage::FenceOk(felix_wire::internal::FenceOk {
                    correlation_id: fence.correlation_id,
                    log_end: 0,
                    commit_offset: 0,
                    last_generation: 0,
                })
            }
            other => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                InternalMessage::ForwardPublishOk(ForwardPublishOk {
                    correlation_id: other.correlation_id(),
                    first_offset: 1,
                    last_offset: 1,
                })
            }
        }
    }
}

/// **The fence does not wait behind slow requests.** A peer answers each
/// stream's requests in order, and a forwarded `Quorum` publish can hold one
/// for its whole timeout; a promoted leader queued behind it does not serve.
/// The fence has a stream of its own.
#[tokio::test(flavor = "multi_thread")]
async fn the_fence_is_not_queued_behind_slow_requests() {
    let config = PeerTransportConfig {
        request_timeout: Duration::from_secs(10),
        ..config()
    };
    let listener = Listener::start_on(PEER, Arc::new(SlowForwards), config.clone()).await;
    let pool = pool(config.clone());
    pool.capabilities(PEER, listener.addr)
        .await
        .expect("handshake");

    // One slow forward on every data stream, and one more behind them.
    let mut forwards = Vec::new();
    for _ in 0..=config.streams_per_conn {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        forwards.push(tokio::spawn(async move {
            pool.request(PEER, addr, forward()).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = std::time::Instant::now();
    let answer = pool
        .request(PEER, listener.addr, fence())
        .await
        .expect("fence");
    assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the fence waited {:?} behind forwarded publishes",
        started.elapsed()
    );

    for forward in forwards {
        let _ = forward.await;
    }
    pool.shutdown().await;
    listener.stop().await;
}
