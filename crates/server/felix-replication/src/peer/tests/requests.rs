//! Requests over a pooled connection: reuse, interleaving, timeouts and shedding.

use super::*;

/// Repeated requests to one peer must not redial it. One connection, and the
/// handshake happens once.
#[tokio::test]
async fn repeated_requests_reuse_one_connection() {
    let handler = Arc::new(CountingHandler::default());
    let listener = Listener::start(PEER, handler.clone()).await;
    let pool = pool(config());

    for _ in 0..20 {
        let response = pool
            .request(PEER, listener.addr, forward())
            .await
            .expect("request");
        assert!(matches!(response, InternalMessage::ForwardPublishOk(_)));
    }

    assert_eq!(pool.connection_count().await, 1, "one peer, one connection");
    assert_eq!(
        handler.seen.load(Ordering::SeqCst),
        20,
        "the handshake must not be re-run per request",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// Responses are matched by correlation id, so concurrent requests on shared
/// streams must not cross.
#[tokio::test]
async fn concurrent_requests_do_not_cross_responses() {
    /// Echoes the shard's `generation` back as `first_offset`, so a crossed
    /// response is detectable rather than merely plausible.
    struct EchoHandler;

    #[async_trait::async_trait]
    impl PeerRequestHandler for EchoHandler {
        async fn handle(&self, request: InternalMessage) -> InternalMessage {
            let generation = match &request {
                InternalMessage::ForwardPublish(publish) => publish.shard.generation,
                _ => 0,
            };
            // A staggered delay guarantees responses are produced out of the
            // order the requests arrived in.
            tokio::time::sleep(Duration::from_millis(generation % 7)).await;
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id: request.correlation_id(),
                first_offset: generation,
                last_offset: generation,
            })
        }
    }

    let listener = Listener::start(PEER, Arc::new(EchoHandler)).await;
    let pool = pool(config());

    let mut tasks = Vec::new();
    for generation in 0..64u64 {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        tasks.push(tokio::spawn(async move {
            let mut message = shard();
            message.generation = generation;
            let request = InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 0,
                shard: message,
                ack: AckMode::None,
                payloads: vec![],
                credential: String::new(),
                headers: None,
                producer: None,
            });
            let response = pool.request(PEER, addr, request).await.expect("request");
            match response {
                InternalMessage::ForwardPublishOk(ok) => (generation, ok.first_offset),
                other => panic!("unexpected {other:?}"),
            }
        }));
    }

    for task in tasks {
        let (sent, echoed) = task.await.expect("join");
        assert_eq!(sent, echoed, "response landed on the wrong request");
    }

    pool.shutdown().await;
    listener.stop().await;
}

/// An unhealthy peer must not be able to consume this broker. At the in-flight
/// limit, requests are shed rather than queued.
#[tokio::test]
async fn a_peer_at_its_inflight_limit_sheds_rather_than_queues() {
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
        request_timeout: Duration::from_secs(10),
        ..config()
    });

    let mut inflight = Vec::new();
    for _ in 0..4 {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        inflight.push(tokio::spawn(async move {
            pool.request(PEER, addr, forward()).await
        }));
    }
    // Let the four occupy their permits before the fifth arrives.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let shed = tokio::time::timeout(
        Duration::from_millis(500),
        pool.request(PEER, listener.addr, forward()),
    )
    .await
    .expect("the request over the limit blocked instead of shedding")
    .expect_err("it must be refused");

    assert!(
        matches!(shed, PeerError::Unavailable { .. }),
        "shedding is not the same as failing: {shed:?}",
    );
    assert!(
        shed.is_retryable(),
        "nothing was sent, so this is safe to retry elsewhere",
    );

    pool.shutdown().await;
    for task in inflight {
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    listener.stop().await;
}

/// A peer that accepts a request and never answers must not hold a waiter open
/// past the request timeout.
#[tokio::test]
async fn a_silent_peer_times_out() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        request_timeout: Duration::from_millis(200),
        forward_timeout: Duration::from_millis(200),
        ..config()
    });

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("a silent peer must time out");
    assert!(matches!(err, PeerError::Timeout { .. }), "{err:?}");
    assert!(
        !err.is_retryable(),
        "the peer may have applied the write, so a retry is a duplicate",
    );

    pool.shutdown().await;
    listener.stop().await;
}

/// Raft calls have a budget of their own: groups waiting on a slow peer
/// cannot take the permits its pings need.
#[tokio::test]
async fn raft_calls_do_not_take_the_budget_pings_need() {
    struct Slow;
    #[async_trait::async_trait]
    impl crate::peer::RaftHandler for Slow {
        async fn answer(&self, body: bytes::Bytes) -> bytes::Bytes {
            tokio::time::sleep(Duration::from_secs(30)).await;
            body
        }
    }
    let config = PeerTransportConfig {
        max_inflight_per_peer: 2,
        request_timeout: Duration::from_secs(10),
        ..config()
    };
    let server = Listener::bind_with_retry(PEER, Arc::new(CountingHandler::default()), &config)
        .await
        .with_raft(Arc::new(Slow));
    let addr = server.local_addr().expect("addr");
    let stop = CancellationToken::new();
    tokio::spawn(server.serve(stop.clone()));
    let pool = pool(config);

    let mut inflight = Vec::new();
    for _ in 0..2 {
        let pool = Arc::clone(&pool);
        inflight.push(tokio::spawn(async move {
            let call = InternalMessage::Raft(felix_wire::internal::RaftCall {
                correlation_id: 0,
                body: bytes::Bytes::from_static(b"\x01\x03"),
            });
            pool.request(PEER, addr, call).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let ping = InternalMessage::Ping(felix_wire::internal::Ping { correlation_id: 0 });
    let answer = tokio::time::timeout(Duration::from_secs(2), pool.request(PEER, addr, ping))
        .await
        .expect("the ping waited")
        .expect("the ping was shed");
    assert!(matches!(answer, InternalMessage::Pong(_)), "{answer:?}");

    pool.shutdown().await;
    stop.cancel();
    for task in inflight {
        task.abort();
    }
}

/// A Raft body as far as the transport reads it: layout, message byte,
/// the sender's deadline in milliseconds, and one byte naming a group.
fn raft_body(message: u8, deadline_ms: u32, group: u8) -> bytes::Bytes {
    let mut body = vec![1, message];
    body.extend_from_slice(&deadline_ms.to_be_bytes());
    body.push(group);
    bytes::Bytes::from(body)
}

fn raft_call(body: bytes::Bytes) -> InternalMessage {
    InternalMessage::Raft(felix_wire::internal::RaftCall {
        correlation_id: 0,
        body,
    })
}

/// Never answers for group 0 or for an append that carries entries; echoes
/// everything else at once.
struct StuckGroup;

#[async_trait::async_trait]
impl crate::peer::RaftHandler for StuckGroup {
    async fn answer(&self, body: bytes::Bytes) -> bytes::Bytes {
        if body[6] == 0 || body[1] == crate::peer::RAFT_APPEND_ENTRIES {
            std::future::pending::<()>().await;
        }
        body
    }
}

async fn raft_listener(config: &PeerTransportConfig) -> (SocketAddr, CancellationToken) {
    let server = Listener::bind_with_retry(PEER, Arc::new(CountingHandler::default()), config)
        .await
        .with_raft(Arc::new(StuckGroup));
    let addr = server.local_addr().expect("addr");
    let stop = CancellationToken::new();
    tokio::spawn(server.serve(stop.clone()));
    (addr, stop)
}

/// A member stuck on storage must not take the control stream. Its sender
/// gives up at its deadline, as openraft does, which the pool never sees;
/// the listener has to let go at the same deadline, or every retry keeps a
/// permit and the stream's heartbeats are all refused as overload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_member_does_not_take_the_stream_from_other_groups() {
    let config = PeerTransportConfig {
        max_inflight_per_peer: 4096,
        request_timeout: Duration::from_secs(10),
        ..config()
    };
    let (addr, stop) = raft_listener(&config).await;
    let pool = pool(config);

    // More than the listener runs at once on a stream, every one cut off by
    // its sender the way the Raft router cuts off a call.
    let calls = (0..1100).map(|_| {
        let pool = Arc::clone(&pool);
        async move {
            let call = raft_call(raft_body(1, 100, 0));
            tokio::time::timeout(Duration::from_millis(100), pool.request(PEER, addr, call)).await
        }
    });
    let cut_off = futures::future::join_all(calls)
        .await
        .into_iter()
        .filter(Result::is_err)
        .count();
    assert!(
        cut_off >= 1000,
        "only {cut_off} calls waited on the stuck group"
    );
    // The listener's own deadlines run out just after the senders'.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let heartbeat = raft_body(1, 200, 1);
    let answer = pool
        .request(PEER, addr, raft_call(heartbeat.clone()))
        .await
        .expect("the heartbeat");
    match answer {
        InternalMessage::RaftAnswer(answer) => assert_eq!(answer.body, heartbeat),
        other => panic!("another group's heartbeat was refused: {other:?}"),
    }

    pool.shutdown().await;
    stop.cancel();
}

/// Appends stalled on a slow follower must not shed other groups'
/// heartbeats and votes: those keep leaders in place, and losing them
/// starts elections everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_appends_do_not_take_the_budget_heartbeats_need() {
    let config = PeerTransportConfig {
        max_inflight_per_peer: 4,
        request_timeout: Duration::from_secs(10),
        ..config()
    };
    let (addr, stop) = raft_listener(&config).await;
    let pool = pool(config);

    let mut stalled = Vec::new();
    for group in 1..=4 {
        let pool = Arc::clone(&pool);
        stalled.push(tokio::spawn(async move {
            let append = raft_call(raft_body(crate::peer::RAFT_APPEND_ENTRIES, 10_000, group));
            pool.request(PEER, addr, append).await
        }));
    }
    // Let the four take the budget before the heartbeat and vote arrive.
    tokio::time::sleep(Duration::from_millis(200)).await;

    for message in [1, 3] {
        let body = raft_body(message, 200, 5);
        let answer = tokio::time::timeout(
            Duration::from_secs(2),
            pool.request(PEER, addr, raft_call(body.clone())),
        )
        .await
        .expect("it waited")
        .unwrap_or_else(|err| panic!("message {message} was shed: {err}"));
        match answer {
            InternalMessage::RaftAnswer(answer) => assert_eq!(answer.body, body),
            other => panic!("message {message} was refused: {other:?}"),
        }
    }

    pool.shutdown().await;
    stop.cancel();
    for task in stalled {
        task.abort();
    }
}
