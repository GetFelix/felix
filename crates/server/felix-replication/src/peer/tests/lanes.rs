//! A stream whose handler never returns must not stay wedged: the requester
//! replaces it, and the listener gives it up once the requester has. That
//! holds for a request answered in turn, such as a bootstrap, and for record
//! batches, which are stored side by side.

use felix_transport::{QuicClient, TransportConfig};
use felix_wire::internal::{
    Hello, PeerCapabilities, ReplicateBootstrap, ReplicateOk, ReplicateRecords,
};

use super::*;
use crate::peer::codec::{Incoming, read_frame, write_frame};

/// The `first_offset` that makes [`Stalls`] never answer.
const STALL: u64 = 999;

pub(super) fn record(correlation_id: u64, first_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateRecords(ReplicateRecords {
        times: None,
        correlation_id,
        shard: shard(),
        first_offset,
        checksum: 0,
        payloads: vec![bytes::Bytes::from_static(b"record")],
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
        headers: None,
        set_generation: None,
    })
}

/// A bootstrap at `base_offset`: answered in turn on its stream.
fn bootstrap(correlation_id: u64, base_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
        correlation_id,
        shard: shard(),
        base_offset,
    })
}

/// Answers everything at once, except a bootstrap or record batch at
/// [`STALL`], which it never answers.
struct Stalls;

#[async_trait::async_trait]
impl PeerRequestHandler for Stalls {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        let stalls = match &request {
            InternalMessage::ReplicateBootstrap(offer) => offer.base_offset == STALL,
            InternalMessage::ReplicateRecords(batch) => batch.first_offset == STALL,
            _ => false,
        };
        if stalls {
            std::future::pending::<()>().await;
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: false,
        })
    }
}

/// The requester side alone, against a listener that never gives up on a
/// handler, as one that predates the bound does not: after a stalled record,
/// replication on the connection recovers within a couple of timeouts.
#[tokio::test]
async fn a_stalled_handler_does_not_wedge_replication_on_its_connection() {
    let listener = Listener::start_on(
        PEER,
        Arc::new(Stalls),
        PeerTransportConfig {
            request_timeout: Duration::from_secs(60),
            ..config()
        },
    )
    .await;
    let pool = pool(PeerTransportConfig {
        // One replication stream, so the stalled one is the only one.
        streams_per_conn: 1,
        request_timeout: Duration::from_millis(300),
        ..config()
    });

    let stalled = pool.request(PEER, listener.addr, bootstrap(0, STALL)).await;
    assert!(
        matches!(stalled, Err(PeerError::Timeout { .. })),
        "{stalled:?}"
    );

    let started = std::time::Instant::now();
    let recovered = loop {
        match pool.request(PEER, listener.addr, record(0, 1)).await {
            Ok(InternalMessage::ReplicateOk(_)) => break started.elapsed(),
            _ if started.elapsed() > Duration::from_secs(3) => {
                panic!("replication stayed wedged behind the stalled handler")
            }
            _ => {}
        }
    };
    assert!(recovered < Duration::from_secs(2), "took {recovered:?}");
    for _ in 0..5 {
        let answer = pool.request(PEER, listener.addr, record(0, 1)).await;
        assert!(
            matches!(answer, Ok(InternalMessage::ReplicateOk(_))),
            "{answer:?}"
        );
    }
    assert_eq!(pool.connection_count().await, 1, "the connection was kept");

    pool.shutdown().await;
    listener.stop().await;
}

/// Connect to `listener` and say hello offering `capabilities`, the way a
/// requester of either age does.
pub(super) async fn greet(
    connection: &felix_transport::QuicConnection,
    capabilities: Option<PeerCapabilities>,
) -> (quinn::SendStream, quinn::RecvStream) {
    greet_as(connection, "broker-a", capabilities).await
}

async fn greet_as(
    connection: &felix_transport::QuicConnection,
    node_id: &str,
    capabilities: Option<PeerCapabilities>,
) -> (quinn::SendStream, quinn::RecvStream) {
    let (mut send, mut recv) = connection.open_bi().await.expect("open");
    let hello = InternalMessage::Hello(Hello {
        correlation_id: 1,
        node_id: node_id.to_string(),
        capabilities,
    });
    write_frame(&mut send, &hello).await.expect("hello");
    assert!(matches!(
        read_frame(&mut recv).await,
        Ok(Incoming::Message(InternalMessage::HelloOk(_)))
    ));
    (send, recv)
}

pub(super) async fn connect(listener: &Listener) -> felix_transport::QuicConnection {
    let client = QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        crate::peer::tls::client_config(None).expect("client config"),
        TransportConfig::default(),
    )
    .expect("bind");
    client
        .connect(listener.addr, crate::peer::tls::INTERNAL_SERVER_NAME)
        .await
        .expect("connect")
}

/// The `first_offset` that makes [`Slow`] take its time.
const SLOW: u64 = 7;

/// Answers a bootstrap at [`SLOW`] only after `delay`, longer than the
/// listener's request timeout; everything else at once.
struct Slow {
    delay: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Slow {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        if let InternalMessage::ReplicateBootstrap(offer) = &request
            && offer.base_offset == SLOW
        {
            tokio::time::sleep(self.delay).await;
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: false,
        })
    }
}

/// A requester that predates stream replacement never opens another one, so
/// a stream dropped under it is lost for the life of the connection. A
/// handler that is slow and then finishes still answers it, and the stream
/// goes on serving.
#[tokio::test]
async fn an_older_requester_keeps_its_stream_through_a_slow_handler() {
    let listener = Listener::start_on(
        PEER,
        Arc::new(Slow {
            delay: Duration::from_millis(2500),
        }),
        // The QUIC idle window is three quarters of this. Much shorter, and a
        // test thread starved by the rest of the suite loses the connection
        // while the handler sleeps.
        PeerTransportConfig {
            request_timeout: Duration::from_secs(1),
            ..config()
        },
    )
    .await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, None).await;

    write_frame(&mut send, &bootstrap(2, SLOW))
        .await
        .expect("slow");
    write_frame(&mut send, &record(3, 1)).await.expect("record");
    for expected in [2, 3] {
        let answer = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut recv))
            .await
            .expect("an answer");
        assert!(
            matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == expected),
            "the stream was dropped under an older requester"
        );
    }

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// The listener side alone: a handler that outlives the requester's timeout
/// gives up its stream under a requester that replaces streams, so the
/// requests behind it are not held forever, and
/// the connection goes on serving other streams.
#[tokio::test]
async fn the_listener_gives_up_a_stream_whose_handler_overran() {
    let listener = Listener::start_on(
        PEER,
        Arc::new(Stalls),
        PeerTransportConfig {
            request_timeout: Duration::from_millis(300),
            ..config()
        },
    )
    .await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, Some(PeerCapabilities::LANE_RENEWAL)).await;

    write_frame(&mut send, &bootstrap(2, STALL))
        .await
        .expect("stall");
    write_frame(&mut send, &record(3, 1)).await.expect("record");
    let ended = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("the stream stayed held by a handler nobody waits for");
    assert!(
        !matches!(ended, Ok(Incoming::Message(_))),
        "nothing is answered behind a handler that overran"
    );

    let (mut send, mut recv) = connection.open_bi().await.expect("open another");
    write_frame(&mut send, &record(4, 1)).await.expect("record");
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("another stream is served");
    assert!(
        matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == 4)
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// Record batches that never finish fill their stream's limit, and the
/// stream then stops reading. Under a requester that replaces streams the
/// listener still gives that stream up once the handlers overrun, rather than
/// holding it and its stream credit until they return.
#[tokio::test]
async fn the_listener_gives_up_a_stream_whose_record_batches_overran() {
    let listener = Listener::start_on(
        PEER,
        Arc::new(Stalls),
        PeerTransportConfig {
            request_timeout: Duration::from_millis(300),
            ..config()
        },
    )
    .await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, Some(PeerCapabilities::LANE_RENEWAL)).await;

    // One past the limit, so the stream is also waiting for room to read on.
    for id in 0..=crate::peer::server::MAX_CONCURRENT_BATCHES_PER_STREAM as u64 {
        write_frame(&mut send, &record(10 + id, STALL))
            .await
            .expect("stall");
    }
    let ended = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("the stream stayed held by batches nobody waits for");
    assert!(
        !matches!(ended, Ok(Incoming::Message(_))),
        "nothing is answered for a batch that overran"
    );

    let (mut send, mut recv) = connection.open_bi().await.expect("open another");
    write_frame(&mut send, &record(4, 1)).await.expect("record");
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("another stream is served");
    assert!(
        matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == 4)
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// Holds every record batch until `want` of them are being stored at once.
struct Together {
    want: usize,
    arrived: AtomicUsize,
    all_in: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Together {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        let all_in = self.all_in.notified();
        if self.arrived.fetch_add(1, Ordering::SeqCst) + 1 >= self.want {
            self.all_in.notify_waiters();
        } else {
            let _ = tokio::time::timeout(Duration::from_secs(5), all_in).await;
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: false,
        })
    }
}

/// **Record batches on one stream are stored side by side.** A leader ships
/// many shards at once over a few streams, and a follower storing one batch
/// at a time per stream made every pass wait behind the batches queued ahead
/// of it (#1968). Here each batch waits until all of them are in, which only
/// happens if the listener reads past the first before answering it.
#[tokio::test]
async fn record_batches_on_one_stream_are_stored_side_by_side() {
    const BATCHES: u64 = 8;
    let listener = Listener::start(
        PEER,
        Arc::new(Together {
            want: BATCHES as usize,
            arrived: AtomicUsize::new(0),
            all_in: tokio::sync::Notify::new(),
        }),
    )
    .await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, Some(PeerCapabilities::LANE_RENEWAL)).await;
    for id in 0..BATCHES {
        write_frame(&mut send, &record(10 + id, id))
            .await
            .expect("record");
    }
    let mut answered = Vec::new();
    for _ in 0..BATCHES {
        let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
            .await
            .expect("the batches were stored one at a time");
        match answer {
            Ok(Incoming::Message(InternalMessage::ReplicateOk(ok))) => {
                answered.push(ok.correlation_id);
            }
            _ => panic!("a record batch was not acknowledged"),
        }
    }
    answered.sort_unstable();
    assert_eq!(answered, (10..10 + BATCHES).collect::<Vec<_>>());

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// Takes `delay` to store every record batch, as a follower waiting on its
/// disk does.
struct Stores {
    delay: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Stores {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        tokio::time::sleep(self.delay).await;
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: false,
        })
    }
}

/// **A leader's round trip is about the follower's store time, however many
/// shards it ships at once.** Batches stored in turn made the round trip grow
/// with the batches queued on the stream: 32 at 50 ms each on one stream took
/// 1.6 s for the last one (#1968).
#[tokio::test]
async fn a_round_trip_does_not_wait_behind_other_shards_batches() {
    const BATCHES: u64 = 32;
    const STORE: Duration = Duration::from_millis(50);
    let listener = Listener::start(PEER, Arc::new(Stores { delay: STORE })).await;
    let pool = pool(PeerTransportConfig {
        streams_per_conn: 1,
        request_timeout: Duration::from_secs(10),
        ..config()
    });
    // Connected first, so the dial is not in the timing.
    let warm = pool.request(PEER, listener.addr, record(0, 0)).await;
    assert!(
        matches!(warm, Ok(InternalMessage::ReplicateOk(_))),
        "{warm:?}"
    );

    let started = std::time::Instant::now();
    let answers = futures::future::join_all((0..BATCHES).map(|offset| {
        let pool = Arc::clone(&pool);
        let addr = listener.addr;
        async move { pool.request(PEER, addr, record(0, offset)).await }
    }))
    .await;
    let took = started.elapsed();
    for answer in &answers {
        assert!(
            matches!(answer, Ok(InternalMessage::ReplicateOk(_))),
            "{answer:?}"
        );
    }
    // Generous: in turn this is BATCHES * STORE = 1.6 s.
    // Two waves at the stream's limit is 2 x STORE; in turn it would be 32 x.
    assert!(took < 16 * STORE, "{BATCHES} batches took {took:?}");

    pool.shutdown().await;
    listener.stop().await;
}

/// Holds a record batch at [`STALL`] until `open` gets permits; answers
/// everything else at once.
struct Gated {
    open: Arc<tokio::sync::Semaphore>,
    held: AtomicUsize,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Gated {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        if let InternalMessage::ReplicateRecords(batch) = &request
            && batch.first_offset == STALL
        {
            self.held.fetch_add(1, Ordering::SeqCst);
            let _ = self.open.acquire().await.expect("never closed");
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: false,
        })
    }
}

/// Stalls batches from enough peers to hold every broker-wide permit, each
/// peer at its own limit. Returns the first peer's connection and every stream.
async fn fill_broker(
    listener: &Listener,
    capabilities: Option<PeerCapabilities>,
) -> (
    felix_transport::QuicConnection,
    Vec<(quinn::SendStream, quinn::RecvStream)>,
) {
    use crate::peer::server::{
        MAX_CONCURRENT_BATCHES_PER_STREAM, MAX_SERVED_BATCHES, MAX_SERVED_BATCHES_PER_PEER,
    };
    let mut first = None;
    let mut streams = Vec::new();
    let mut id = 10;
    for peer in 0..MAX_SERVED_BATCHES / MAX_SERVED_BATCHES_PER_PEER {
        let connection = connect(listener).await;
        let mut mine = vec![greet_as(&connection, &format!("peer-{peer}"), capabilities).await];
        while mine.len() < MAX_SERVED_BATCHES_PER_PEER / MAX_CONCURRENT_BATCHES_PER_STREAM {
            mine.push(connection.open_bi().await.expect("open"));
        }
        for (send, _recv) in &mut mine {
            for _ in 0..MAX_CONCURRENT_BATCHES_PER_STREAM {
                write_frame(send, &record(id, STALL)).await.expect("stall");
                id += 1;
            }
        }
        streams.extend(mine);
        first.get_or_insert(connection);
    }
    (first.expect("a peer"), streams)
}

/// **The batches a follower stores at once are bounded across streams.**
/// Each stream holds up to its own limit, so without a broker-wide one the
/// batches in memory grow with every stream a peer opens. Once the
/// broker-wide limit is held, a batch on yet another stream waits for one
/// of them to finish.
#[tokio::test]
async fn record_batches_stored_at_once_are_bounded_across_streams() {
    use crate::peer::server::MAX_SERVED_BATCHES;

    let open = Arc::new(tokio::sync::Semaphore::new(0));
    let gated = Arc::new(Gated {
        open: Arc::clone(&open),
        held: AtomicUsize::new(0),
    });
    let listener = Listener::start_on(PEER, Arc::clone(&gated) as _, config()).await;
    // An older requester, so nothing is given up while the batches wait.
    let (_peer0, _streams) = fill_broker(&listener, None).await;
    let connection = connect(&listener).await;
    let _ = greet_as(&connection, "probe", None).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while gated.held.load(Ordering::SeqCst) < MAX_SERVED_BATCHES {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("every stalled batch is being stored");

    let (mut send, mut recv) = connection.open_bi().await.expect("open another");
    write_frame(&mut send, &record(4, 1)).await.expect("record");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), read_frame(&mut recv))
            .await
            .is_err(),
        "a batch was stored past the broker-wide limit"
    );

    open.add_permits(MAX_SERVED_BATCHES);
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("answered once the held batches finish");
    assert!(
        matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == 4)
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **A batch that overran still counts against the broker-wide limit.** Its
/// stream is given up, but the handler runs on with the batch in memory, so
/// the batches a requester sends on fresh streams wait for it to finish.
#[tokio::test]
async fn an_overrun_batch_keeps_its_place_in_the_broker_wide_limit() {
    use crate::peer::server::MAX_SERVED_BATCHES;

    let open = Arc::new(tokio::sync::Semaphore::new(0));
    let gated = Arc::new(Gated {
        open: Arc::clone(&open),
        held: AtomicUsize::new(0),
    });
    let listener = Listener::start_on(
        PEER,
        Arc::clone(&gated) as _,
        PeerTransportConfig {
            request_timeout: Duration::from_millis(300),
            ..config()
        },
    )
    .await;
    let (_peer0, mut streams) = fill_broker(&listener, Some(PeerCapabilities::LANE_RENEWAL)).await;
    let connection = connect(&listener).await;
    let _ = greet_as(&connection, "probe", None).await;
    // Every one of those streams is given up once its batches overrun.
    for (_send, recv) in &mut streams {
        let ended = tokio::time::timeout(Duration::from_secs(5), read_frame(recv))
            .await
            .expect("the stream was given up");
        assert!(!matches!(ended, Ok(Incoming::Message(_))));
    }
    assert_eq!(gated.held.load(Ordering::SeqCst), MAX_SERVED_BATCHES);

    let (mut send, mut recv) = connection.open_bi().await.expect("open another");
    write_frame(&mut send, &record(4, 1)).await.expect("record");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), read_frame(&mut recv))
            .await
            .is_err(),
        "a batch was stored while the overrun ones still held their batches"
    );

    open.add_permits(MAX_SERVED_BATCHES);
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("answered once the overrun batches finish");
    assert!(
        matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == 4)
    );

    connection.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **One peer cannot take the broker-wide limit.** A peer whose stores stall
/// holds at most its own share, across connections and reconnects, and
/// another peer's batch is still stored.
#[tokio::test]
async fn one_stalled_peer_holds_only_its_share_of_the_broker() {
    use crate::peer::server::{
        MAX_CONCURRENT_BATCHES_PER_STREAM, MAX_SERVED_BATCHES, MAX_SERVED_BATCHES_PER_PEER,
    };

    let open = Arc::new(tokio::sync::Semaphore::new(0));
    let gated = Arc::new(Gated {
        open: Arc::clone(&open),
        held: AtomicUsize::new(0),
    });
    let listener = Listener::start_on(PEER, Arc::clone(&gated) as _, config()).await;
    // Enough connections from one node to take the whole broker if only the
    // broker-wide limit applied: the limit follows the node.
    let mut stalled = Vec::new();
    let mut id = 10;
    for _ in 0..MAX_SERVED_BATCHES / MAX_SERVED_BATCHES_PER_PEER {
        let connection = connect(&listener).await;
        let mut streams = vec![greet_as(&connection, "stalled", None).await];
        while streams.len() < MAX_SERVED_BATCHES_PER_PEER / MAX_CONCURRENT_BATCHES_PER_STREAM {
            streams.push(connection.open_bi().await.expect("open"));
        }
        for (send, _recv) in &mut streams {
            for _ in 0..MAX_CONCURRENT_BATCHES_PER_STREAM {
                write_frame(send, &record(id, STALL)).await.expect("stall");
                id += 1;
            }
        }
        stalled.push((connection, streams));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while gated.held.load(Ordering::SeqCst) < MAX_SERVED_BATCHES_PER_PEER {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the peer's batches are being stored");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        gated.held.load(Ordering::SeqCst),
        MAX_SERVED_BATCHES_PER_PEER,
        "one peer stored past its share"
    );

    let other = connect(&listener).await;
    let (mut send, mut recv) = greet_as(&other, "healthy", None).await;
    write_frame(&mut send, &record(4, 1)).await.expect("record");
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("another peer's batch is stored");
    assert!(
        matches!(answer, Ok(Incoming::Message(InternalMessage::ReplicateOk(ref ok))) if ok.correlation_id == 4)
    );

    open.add_permits(MAX_SERVED_BATCHES);
    other.close(0u32.into(), b"done");
    listener.stop().await;
}

/// **Limits for node ids that are gone do not pile up.** An entry is dropped
/// once nothing holds it and every permit is back.
#[test]
fn a_peer_limit_nobody_holds_is_dropped() {
    use crate::peer::server::{PeerBatches, peer_limit};

    let peers = PeerBatches::default();
    let held = peer_limit(&peers, Arc::from("held"));
    let _permit = Arc::clone(&held).try_acquire_owned().expect("a permit");
    drop(peer_limit(&peers, Arc::from("gone")));
    assert_eq!(peers.lock().len(), 2);

    // Looking up another node sweeps: "gone" is idle, "held" is in use.
    drop(peer_limit(&peers, Arc::from("next")));
    let nodes: Vec<_> = peers.lock().keys().map(|k| k.to_string()).collect();
    assert!(nodes.contains(&"held".to_string()), "{nodes:?}");
    assert!(!nodes.contains(&"gone".to_string()), "{nodes:?}");
}

/// **A limit someone holds but has not acquired from survives the sweep.**
/// Dropping it would hand the next lookup a second semaphore for the same node.
#[test]
fn a_peer_limit_held_without_permits_survives_the_sweep() {
    use crate::peer::server::{PeerBatches, peer_limit};

    let peers = PeerBatches::default();
    let held = peer_limit(&peers, Arc::from("a"));
    drop(peer_limit(&peers, Arc::from("b")));
    assert!(Arc::ptr_eq(&held, &peer_limit(&peers, Arc::from("a"))));
}
