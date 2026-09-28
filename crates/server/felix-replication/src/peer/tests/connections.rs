//! A pooled connection's life: reconnects, backoff, keepalive and shutdown.

use super::*;

/// A peer that restarts at the same address must be usable again, without the
/// pool needing to be told.
#[tokio::test]
async fn a_restarted_peer_is_reconnected_to() {
    // A fixed port, because the restarted listener has to come back at the
    // address the pool already holds a dead connection to.
    let first = Listener::start_on(
        PEER,
        Arc::new(CountingHandler::default()),
        PeerTransportConfig {
            bind: "127.0.0.1:0".parse().expect("addr"),
            ..config()
        },
    )
    .await;
    let addr = first.addr;
    let pool = pool(config());

    pool.request(PEER, addr, forward()).await.expect("first");
    first.stop().await;

    // The pool cannot know the peer is gone until it tries, so the request that
    // spans the restart is allowed to fail. What must not happen is failing
    // forever.
    let second = Listener::start_on(
        PEER,
        Arc::new(CountingHandler::default()),
        PeerTransportConfig {
            bind: addr,
            ..config()
        },
    )
    .await;

    let mut recovered = false;
    for _ in 0..50 {
        if pool.request(PEER, addr, forward()).await.is_ok() {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        recovered,
        "the pool never reconnected to the restarted peer"
    );

    pool.shutdown().await;
    second.stop().await;
}

/// A connection that drops with requests in flight must fail them at that
/// moment. `docs/internal-protocol.md` requires no forwarded publish is left
/// pending, and waiting out the request timeout is not the same thing.
#[tokio::test]
async fn connection_loss_fails_in_flight_requests_immediately() {
    let listener = Listener::start(
        PEER,
        Arc::new(CountingHandler {
            seen: AtomicUsize::new(0),
            // Longer than the test waits, so nothing answers on its own.
            delay: Duration::from_secs(30),
        }),
    )
    .await;
    let pool = pool(PeerTransportConfig {
        // Far longer than the connection will live: if the request only fails
        // when this expires, the test times out instead of passing.
        request_timeout: Duration::from_secs(30),
        ..config()
    });

    let addr = listener.addr;
    let requester = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.request(PEER, addr, forward()).await })
    };

    // Let the request reach the peer before taking the listener away. The pool
    // is deliberately *not* shut down here: cancelling it would end the request
    // for its own reason and prove nothing about the connection dropping.
    tokio::time::sleep(Duration::from_millis(100)).await;
    listener.stop().await;

    let result = tokio::time::timeout(Duration::from_secs(5), requester)
        .await
        .expect("the in-flight request was left pending")
        .expect("join");
    let err = result.expect_err("a lost connection must fail its requests");
    assert!(
        matches!(err, PeerError::Disconnected { .. }),
        "the loss must be reported as a loss, not as a timeout: {err:?}",
    );

    pool.shutdown().await;
}

/// A peer that cannot be reached at all must back off rather than dial in a
/// loop, and must say so without sending anything.
#[tokio::test]
async fn an_unreachable_peer_backs_off() {
    // Nothing is listening here: a bound-then-dropped socket leaves an address
    // that is routable and refuses.
    let dead: SocketAddr = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
        socket.local_addr().expect("addr")
    };
    // A window no CI stall outlives, so the second request below is inside it
    // however slowly this test is scheduled. The proof of "no dial" is the
    // error variant, not the clock: a refusal inside the window is
    // `Unavailable`, while an actual dial to this address fails as a connect
    // error. Asserting on elapsed time was a proxy for the same thing and
    // flaked whenever the runner stalled past the deadline.
    let pool = pool(PeerTransportConfig {
        handshake_timeout: Duration::from_millis(100),
        reconnect_base: Duration::from_secs(60),
        reconnect_max: Duration::from_secs(60),
        ..config()
    });

    let first = pool.request(PEER, dead, forward()).await;
    assert!(first.is_err(), "an unreachable peer must fail");

    // Inside the backoff window the next request must be refused without a dial,
    // which is what makes a dead peer cheap rather than a dial loop.
    let second = pool
        .request(PEER, dead, forward())
        .await
        .expect_err("still unreachable");
    assert!(
        matches!(second, PeerError::Unavailable { .. }),
        "the backoff window was not honoured; it dialled again: {second:?}"
    );
    assert!(
        second.to_string().contains("reconnecting in"),
        "the refusal should say it is backing off: {second}"
    );

    pool.shutdown().await;
}

/// Shutdown must close connections and leave nothing running. A leaked pump
/// task holds a stream handle, which keeps the connection alive after the pool
/// has forgotten it.
#[tokio::test]
async fn shutdown_closes_connections_and_refuses_new_requests() {
    let listener = Listener::start(PEER, Arc::new(CountingHandler::default())).await;
    let pool = pool(config());
    pool.request(PEER, listener.addr, forward())
        .await
        .expect("request");
    assert_eq!(pool.connection_count().await, 1);

    pool.shutdown().await;
    assert_eq!(
        pool.connection_count().await,
        0,
        "connections were not closed"
    );

    let err = pool
        .request(PEER, listener.addr, forward())
        .await
        .expect_err("a shut-down pool must not dial");
    assert!(matches!(err, PeerError::ShuttingDown), "{err:?}");

    listener.stop().await;
}

/// Backoff is jittered so that every broker noticing the same peer restart does
/// not redial it in step.
#[test]
fn reconnect_backoff_grows_and_is_jittered() {
    let config = PeerTransportConfig {
        reconnect_base: Duration::from_millis(100),
        reconnect_max: Duration::from_millis(5_000),
        ..Default::default()
    };

    for attempt in 0..6 {
        let ceiling = config
            .reconnect_base
            .saturating_mul(1 << attempt)
            .min(config.reconnect_max);
        for _ in 0..32 {
            assert!(
                config.reconnect_delay(attempt) <= ceiling,
                "attempt {attempt} exceeded its ceiling",
            );
        }
    }

    // Capped, and never longer than the configured maximum however many
    // attempts have failed.
    assert!(config.reconnect_delay(40) <= config.reconnect_max);

    let samples: std::collections::HashSet<_> =
        (0..64).map(|_| config.reconnect_delay(4)).collect();
    assert!(
        samples.len() > 1,
        "an unjittered backoff has every broker redial in step",
    );
}

/// **A peer connection gives up before a request on it does.** A broker that
/// was killed leaves its peers holding connections nothing will tear down; if
/// QUIC outlasts the request timeout, every forwarded publish and every
/// replication pass sent over one waits the request timeout out in full.
#[test]
fn a_peer_connection_fails_before_the_request_on_it_does() {
    let config = PeerTransportConfig::default();
    assert!(
        config.peer_idle_timeout() < config.request_timeout,
        "idle window {:?} does not close before a request gives up at {:?}",
        config.peer_idle_timeout(),
        config.request_timeout,
    );
    let transport = config.quic_transport();
    assert_eq!(
        transport.max_idle_timeout,
        Some(config.peer_idle_timeout()),
        "the transport does not use the derived idle window",
    );
}

/// The keep-alive has to fit inside the idle window, or a healthy but quiet
/// peer connection is closed on the idle timer for having nothing to say.
#[test]
fn a_quiet_peer_connection_is_kept_alive_inside_its_idle_window() {
    let transport = PeerTransportConfig::default().quic_transport();
    let keep_alive = transport.keep_alive_interval.expect("a keep-alive");
    let idle = transport.max_idle_timeout.expect("an idle window");
    assert!(
        keep_alive * 3 <= idle,
        "keep-alive {keep_alive:?} leaves no margin inside idle window {idle:?}",
    );
}
