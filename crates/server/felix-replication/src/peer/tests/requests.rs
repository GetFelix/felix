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
