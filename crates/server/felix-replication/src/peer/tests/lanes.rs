//! A stream whose handler never returns must not stay wedged: the requester
//! replaces it, and the listener gives it up once the requester has.

use felix_transport::{QuicClient, TransportConfig};
use felix_wire::internal::{Hello, PeerCapabilities, ReplicateOk, ReplicateRecords};

use super::*;
use crate::peer::codec::{Incoming, read_frame, write_frame};

/// The `first_offset` that makes [`Stalls`] never answer.
const STALL: u64 = 999;

fn record(correlation_id: u64, first_offset: u64) -> InternalMessage {
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
    })
}

/// Answers every record at once, except one at [`STALL`], which it never
/// answers.
struct Stalls;

#[async_trait::async_trait]
impl PeerRequestHandler for Stalls {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        if let InternalMessage::ReplicateRecords(records) = &request
            && records.first_offset == STALL
        {
            std::future::pending::<()>().await;
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
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

    let stalled = pool.request(PEER, listener.addr, record(0, STALL)).await;
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
async fn greet(
    connection: &felix_transport::QuicConnection,
    capabilities: Option<PeerCapabilities>,
) -> (quinn::SendStream, quinn::RecvStream) {
    let (mut send, mut recv) = connection.open_bi().await.expect("open");
    let hello = InternalMessage::Hello(Hello {
        correlation_id: 1,
        node_id: "broker-a".to_string(),
        capabilities,
    });
    write_frame(&mut send, &hello).await.expect("hello");
    assert!(matches!(
        read_frame(&mut recv).await,
        Ok(Incoming::Message(InternalMessage::HelloOk(_)))
    ));
    (send, recv)
}

async fn connect(listener: &Listener) -> felix_transport::QuicConnection {
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

/// Answers a record at [`SLOW`] only after `delay`, longer than the
/// listener's request timeout; everything else at once.
struct Slow {
    delay: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Slow {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        if let InternalMessage::ReplicateRecords(records) = &request
            && records.first_offset == SLOW
        {
            tokio::time::sleep(self.delay).await;
        }
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
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
            delay: Duration::from_millis(600),
        }),
        PeerTransportConfig {
            request_timeout: Duration::from_millis(200),
            ..config()
        },
    )
    .await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, None).await;

    write_frame(&mut send, &record(2, SLOW))
        .await
        .expect("slow");
    write_frame(&mut send, &record(3, 1)).await.expect("record");
    for expected in [2, 3] {
        let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
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

    write_frame(&mut send, &record(2, STALL))
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
