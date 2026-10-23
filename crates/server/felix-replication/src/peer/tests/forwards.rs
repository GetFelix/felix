//! Forwards that wait on a quorum must never hold up the replication that
//! ends the wait (#1458).

use std::sync::OnceLock;

use felix_wire::internal::{ForwardPublishError, ReplicateOk, ReplicateRecords};

use super::*;

fn replicate() -> InternalMessage {
    InternalMessage::ReplicateRecords(ReplicateRecords {
        times: None,
        correlation_id: 0,
        shard: shard(),
        first_offset: 0,
        checksum: 0,
        payloads: vec![bytes::Bytes::from_static(b"record")],
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
    })
}

/// A broker in miniature: a forward is answered only once the other broker
/// has acknowledged the record, the way an owner's quorum wait is.
#[derive(Default)]
struct QuorumOwner {
    other: OnceLock<(Arc<PeerPool>, String, SocketAddr)>,
    replicated: AtomicUsize,
}

#[async_trait::async_trait]
impl PeerRequestHandler for QuorumOwner {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        let correlation_id = request.correlation_id();
        match request {
            InternalMessage::ForwardPublish(_) => {
                let (pool, node, addr) = self.other.get().expect("wired");
                match pool.request(node, *addr, replicate()).await {
                    Ok(InternalMessage::ReplicateOk(_)) => {
                        InternalMessage::ForwardPublishOk(ForwardPublishOk {
                            correlation_id,
                            first_offset: 0,
                            last_offset: 0,
                        })
                    }
                    other => InternalMessage::ForwardPublishError(ForwardPublishError {
                        correlation_id,
                        code: ErrorCode::StorageFailed,
                        detail: format!("no quorum: {other:?}"),
                    }),
                }
            }
            _ => {
                self.replicated.fetch_add(1, Ordering::SeqCst);
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id,
                    durable_offset: 1,
                })
            }
        }
    }
}

/// Two brokers forwarding to each other, many forwards at once, more than
/// either has streams. Each forward's answer needs a replication round trip
/// the other way, so replication queued behind forwards deadlocks both until
/// every request times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_forwarding_to_each_other_still_replicate() {
    let config = PeerTransportConfig {
        streams_per_conn: 2,
        request_timeout: Duration::from_secs(2),
        forward_timeout: Duration::from_secs(4),
        ..config()
    };
    let a = Arc::new(QuorumOwner::default());
    let b = Arc::new(QuorumOwner::default());
    let listener_a = Listener::start_on("broker-a", a.clone(), config.clone()).await;
    let listener_b = Listener::start_on("broker-b", b.clone(), config.clone()).await;
    let pool_a = PeerPool::new(
        "broker-a".to_string(),
        config.clone(),
        CancellationToken::new(),
    )
    .unwrap();
    let pool_b = PeerPool::new(
        "broker-b".to_string(),
        config.clone(),
        CancellationToken::new(),
    )
    .unwrap();
    let _ = a
        .other
        .set((Arc::clone(&pool_a), "broker-b".to_string(), listener_b.addr));
    let _ = b
        .other
        .set((Arc::clone(&pool_b), "broker-a".to_string(), listener_a.addr));

    const EACH_WAY: usize = 32;
    let mut forwards = Vec::new();
    for i in 0..EACH_WAY * 2 {
        let (pool, node, addr) = if i % 2 == 0 {
            (Arc::clone(&pool_a), "broker-b", listener_b.addr)
        } else {
            (Arc::clone(&pool_b), "broker-a", listener_a.addr)
        };
        forwards.push(tokio::spawn(async move {
            pool.request(node, addr, forward()).await
        }));
    }

    // Well inside one replication timeout: a forward that waited one out
    // has already lost.
    let all = tokio::time::timeout(Duration::from_millis(1_500), async {
        let mut answers = Vec::new();
        for forward in forwards {
            answers.push(forward.await.expect("join"));
        }
        answers
    })
    .await
    .expect("forwards stalled: replication queued behind them");
    for answer in all {
        assert!(
            matches!(answer, Ok(InternalMessage::ForwardPublishOk(_))),
            "{answer:?}"
        );
    }
    assert_eq!(a.replicated.load(Ordering::SeqCst), EACH_WAY);
    assert_eq!(b.replicated.load(Ordering::SeqCst), EACH_WAY);

    pool_a.shutdown().await;
    pool_b.shutdown().await;
    listener_a.stop().await;
    listener_b.stop().await;
}

/// Forwards at the in-flight limit must not shed replication: it has its own
/// allowance, and it is what lets those forwards finish.
#[tokio::test]
async fn forwards_at_their_limit_do_not_shed_replication() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        max_inflight_per_peer: 4,
        streams_per_conn: 1,
        request_timeout: Duration::from_secs(10),
        forward_timeout: Duration::from_secs(10),
        ..config()
    });

    let mut held = Vec::new();
    for _ in 0..4 {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        held.push(tokio::spawn(async move {
            pool.request(PEER, addr, forward()).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // `CountingHandler` delays everything, so this stub is answered by a
    // forward-shaped reply after 30 s. The point is only that it is admitted
    // and sent rather than shed at once.
    let sent = tokio::time::timeout(
        Duration::from_millis(300),
        pool.request(PEER, listener.addr, replicate()),
    )
    .await;
    assert!(
        sent.is_err(),
        "replication was refused instead of sent: {sent:?}"
    );

    pool.shutdown().await;
    for task in held {
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    listener.stop().await;
}

/// A forward that waits until a record replicates, and the record.
#[derive(Default)]
struct WaitsForRecord {
    replicated: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl PeerRequestHandler for WaitsForRecord {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        let correlation_id = request.correlation_id();
        match request {
            InternalMessage::ForwardPublish(_) => {
                let _ =
                    tokio::time::timeout(Duration::from_secs(5), self.replicated.notified()).await;
                InternalMessage::ForwardPublishOk(ForwardPublishOk {
                    correlation_id,
                    first_offset: 0,
                    last_offset: 0,
                })
            }
            _ => {
                self.replicated.notify_one();
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id,
                    durable_offset: 1,
                })
            }
        }
    }
}

/// A requester that predates lane classes puts replication on the same
/// stream as its forwards. The listener answers forwards as they finish, so a
/// record behind a forward that waits on it is still served, and each answer
/// carries its own correlation id.
#[tokio::test]
async fn a_record_behind_a_waiting_forward_on_one_stream_is_served() {
    use felix_transport::{QuicClient, TransportConfig};
    use felix_wire::internal::Hello;

    use crate::peer::codec::{Incoming, read_frame, write_frame};

    let listener = Listener::start(PEER, Arc::new(WaitsForRecord::default())).await;
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
    let hello = InternalMessage::Hello(Hello {
        correlation_id: 1,
        node_id: "broker-old".to_string(),
        capabilities: None,
    });
    write_frame(&mut send, &hello).await.expect("hello");
    assert!(matches!(
        read_frame(&mut recv).await,
        Ok(Incoming::Message(InternalMessage::HelloOk(_)))
    ));

    let forward = match forward() {
        InternalMessage::ForwardPublish(publish) => {
            InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 2,
                ..publish
            })
        }
        _ => unreachable!(),
    };
    let record = match replicate() {
        InternalMessage::ReplicateRecords(records) => {
            InternalMessage::ReplicateRecords(ReplicateRecords {
                correlation_id: 3,
                ..records
            })
        }
        _ => unreachable!(),
    };
    write_frame(&mut send, &forward).await.expect("forward");
    write_frame(&mut send, &record).await.expect("record");

    let mut answered = Vec::new();
    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
            .await
            .expect("the record queued behind the forward waiting on it");
        match frame {
            Ok(Incoming::Message(message)) => answered.push(message),
            _ => panic!("stream ended"),
        }
    }
    // The forward is answered only once the record was, so either may be
    // written first.
    answered.sort_by_key(InternalMessage::correlation_id);
    assert!(
        matches!(&answered[0], InternalMessage::ForwardPublishOk(ok) if ok.correlation_id == 2),
        "{answered:?}"
    );
    assert!(
        matches!(&answered[1], InternalMessage::ReplicateOk(ok) if ok.correlation_id == 3),
        "{answered:?}"
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}
