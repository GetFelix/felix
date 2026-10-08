use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::Message;

use super::{NodeConnections, NodeLimits, OpenedStream};
use crate::auth::StaticToken;
use crate::connection::Credentials;
use crate::frame_io::{read_message_with_limit, write_message};
use crate::test_support::{build_server_config, quinn_client_config};

const MAX_FRAME: usize = 1024 * 1024;

/// A broker that authenticates every stream and answers anything after that
/// with `Ok`, granting `max_streams` concurrent streams per connection.
struct EchoBroker {
    addr: SocketAddr,
    accepted: Arc<Mutex<Vec<QuicConnection>>>,
    task: tokio::task::JoinHandle<()>,
}

impl EchoBroker {
    fn start(max_streams: u16) -> Result<(Self, QuicClient)> {
        Self::granting(max_streams, 0, None)
    }

    /// An echo broker whose `AuthOk` advertises `server_features` and grants
    /// `publish_window`.
    fn granting(
        max_streams: u16,
        server_features: u32,
        publish_window: Option<u32>,
    ) -> Result<(Self, QuicClient)> {
        let (server_config, cert) = build_server_config()?;
        let transport = TransportConfig {
            max_streams,
            ..TransportConfig::default()
        };
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport)?;
        let addr = server.local_addr()?;
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let accepted = Arc::clone(&accepted);
            async move {
                while let Ok(connection) = server.accept().await {
                    accepted.lock().unwrap().push(connection.clone());
                    tokio::spawn(async move {
                        while let Ok((send, recv)) = connection.accept_bi().await {
                            tokio::spawn(answer(send, recv, server_features, publish_window));
                        }
                    });
                }
            }
        });
        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            quinn_client_config(cert)?,
            TransportConfig::default(),
        )?;
        Ok((
            Self {
                addr,
                accepted,
                task,
            },
            client,
        ))
    }

    fn accepted(&self) -> usize {
        self.accepted.lock().unwrap().len()
    }

    /// Close the `index`th connection this broker accepted.
    fn kill(&self, index: usize) {
        self.accepted.lock().unwrap()[index].close(7u32.into(), b"killed");
    }
}

impl Drop for EchoBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn answer(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    server_features: u32,
    publish_window: Option<u32>,
) -> Result<()> {
    let mut scratch = BytesMut::new();
    while let Some(message) = read_message_with_limit(&mut recv, &mut scratch, MAX_FRAME).await? {
        let reply = match message {
            Message::Auth { .. } => Message::AuthOk {
                server_flags: felix_wire::KNOWN_FLAGS,
                server_features: Some(server_features),
                listener_ports: None,
                publish_window,
            },
            _ => Message::Ok,
        };
        write_message(&mut send, reply).await?;
    }
    send.finish()?;
    Ok(())
}

fn node(broker: &EchoBroker, endpoint: QuicClient, limits: NodeLimits) -> Arc<NodeConnections> {
    let credentials = Arc::new(Credentials::new(
        "t1".to_string(),
        Arc::new(StaticToken("token".to_string())),
    ));
    Arc::new(NodeConnections::new(
        endpoint,
        broker.addr,
        "localhost",
        credentials,
        limits,
    ))
}

fn limits(ceiling: usize, streams_per_conn: usize) -> NodeLimits {
    NodeLimits {
        role: "test",
        ceiling,
        streams_per_conn,
        max_frame_bytes: MAX_FRAME,
        event_router_max_pending: 16,
    }
}

async fn round_trip(stream: &mut OpenedStream) -> Result<()> {
    write_message(&mut stream.send, Message::Topology).await?;
    let mut scratch = BytesMut::new();
    match read_message_with_limit(&mut stream.recv, &mut scratch, MAX_FRAME).await? {
        Some(Message::Ok) => Ok(()),
        other => anyhow::bail!("unexpected answer {other:?}"),
    }
}

/// Open `count` streams at once and keep them.
async fn open_concurrently(node: &Arc<NodeConnections>, count: usize) -> Vec<OpenedStream> {
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..count {
        let node = Arc::clone(node);
        tasks.spawn(async move { node.open().await });
    }
    let mut opened = Vec::with_capacity(count);
    while let Some(result) = tasks.join_next().await {
        opened.push(result.expect("join").expect("open a stream"));
    }
    opened
}

/// Light use never opens a second connection: streams multiplex on the first
/// for as long as it has room.
#[tokio::test]
async fn streams_share_one_connection_while_it_has_room() -> Result<()> {
    let (broker, endpoint) = EchoBroker::start(1024)?;
    let node = node(&broker, endpoint, limits(8, 64));

    let mut streams = open_concurrently(&node, 20).await;
    for stream in &mut streams {
        round_trip(stream).await?;
    }
    assert_eq!(node.connection_count(), 1);
    assert_eq!(broker.accepted(), 1);
    Ok(())
}

/// Once the broker's stream credit runs out, the set grows -- and stops at
/// the ceiling, where the next stream waits for credit instead.
#[tokio::test]
async fn grows_to_the_ceiling_when_stream_credit_runs_out() -> Result<()> {
    let (broker, endpoint) = EchoBroker::start(4)?;
    let node = node(&broker, endpoint, limits(3, 1024));

    let mut streams = open_concurrently(&node, 12).await;
    assert_eq!(node.connection_count(), 3);
    assert_eq!(broker.accepted(), 3);

    let queued = tokio::spawn({
        let node = Arc::clone(&node);
        async move { node.open().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !queued.is_finished(),
        "a stream past the ceiling's credit should wait"
    );

    // Finishing one stream returns its credit, and the queued one gets it.
    let mut done = streams.pop().expect("a stream");
    done.send.finish()?;
    let mut scratch = BytesMut::new();
    while read_message_with_limit(&mut done.recv, &mut scratch, MAX_FRAME)
        .await?
        .is_some()
    {}
    drop(done);
    let mut late = tokio::time::timeout(Duration::from_secs(5), queued).await???;
    round_trip(&mut late).await?;
    assert_eq!(broker.accepted(), 3, "the ceiling holds");
    Ok(())
}

/// Streams opened after the pool go to the least-loaded connection, so a
/// stream's per-shard publish streams land on different connections, and
/// so on different listeners when the broker has several.
#[tokio::test]
async fn later_streams_spread_over_a_filled_pool() -> Result<()> {
    let (broker, endpoint) = EchoBroker::start(1024)?;
    let node = node(&broker, endpoint, limits(4, 1024));
    node.fill(4, false).await?;
    let mut pool = Vec::new();
    for _ in 0..8 {
        pool.push(node.open().await?);
    }
    let mut later = Vec::new();
    for _ in 0..4 {
        later.push(node.open().await?);
    }
    let mut slots: Vec<usize> = later.iter().map(|stream| stream.lease.slot()).collect();
    slots.sort_unstable();
    slots.dedup();
    assert_eq!(slots.len(), 4, "four later streams share connections");
    assert_eq!(broker.accepted(), 4);
    Ok(())
}

/// The stream budget is the other trigger: a connection carrying its share
/// gets a neighbour even when the broker would grant more.
#[tokio::test]
async fn grows_when_the_stream_budget_is_used() -> Result<()> {
    let (broker, endpoint) = EchoBroker::start(1024)?;
    let node = node(&broker, endpoint, limits(4, 2));

    let _streams = open_concurrently(&node, 6).await;
    assert_eq!(node.connection_count(), 3);
    assert_eq!(broker.accepted(), 3);
    Ok(())
}

/// A connection that dies fails its own streams and nobody else's, and the
/// next stream that needs the room gets a fresh connection in its place.
#[tokio::test]
async fn a_dead_connection_is_replaced_and_only_its_streams_fail() -> Result<()> {
    let (broker, endpoint) = EchoBroker::start(2)?;
    let node = node(&broker, endpoint, limits(2, 1024));

    let mut streams = Vec::new();
    for _ in 0..4 {
        streams.push(node.open().await?);
    }
    assert_eq!(broker.accepted(), 2);
    let (mut doomed, mut survivors): (Vec<_>, Vec<_>) = streams
        .into_iter()
        .partition(|stream| stream.lease.slot() == 1);
    assert_eq!(doomed.len(), 2);

    broker.kill(1);
    for stream in &mut doomed {
        assert!(
            round_trip(stream).await.is_err(),
            "a stream on the dead connection should fail"
        );
    }
    for stream in &mut survivors {
        round_trip(stream).await?;
    }
    assert_eq!(node.connection_count(), 1);

    // The survivor is out of credit, so these need the dead one's slot.
    let mut replacements = vec![node.open().await?, node.open().await?];
    for stream in &mut replacements {
        assert_eq!(stream.lease.slot(), 1);
        round_trip(stream).await?;
    }
    assert_eq!(node.connection_count(), 2);
    assert_eq!(broker.accepted(), 3);
    Ok(())
}

/// Two streams on one connection, both granted a window of two by a broker
/// advertising `server_features`. Returns how many slots the second stream
/// can still take once the first has taken all of its own.
async fn slots_left_beside_a_full_window(server_features: u32) -> Result<usize> {
    let (broker, endpoint) = EchoBroker::granting(
        1024,
        felix_wire::FEATURE_PUBLISH_PIPELINE | server_features,
        Some(2),
    )?;
    let node = node(&broker, endpoint, limits(1, 1024));
    let (first, second) = (node.open().await?, node.open().await?);
    assert_eq!(node.connection_count(), 1);
    let full = first
        .lease
        .publish_window(&first.negotiated)
        .expect("a window");
    let _taken = full.try_acquire_many_owned(2)?;
    let beside = second
        .lease
        .publish_window(&second.negotiated)
        .expect("a window");
    Ok(beside.available_permits())
}

/// **A broker that counts the window per stream gets one per stream**, so a
/// stream whose publishes are stuck behind a stalled shard leaves its
/// neighbours on the same connection their whole window.
#[tokio::test]
async fn a_per_stream_window_is_not_shared_with_the_connection() -> Result<()> {
    assert_eq!(
        slots_left_beside_a_full_window(felix_wire::FEATURE_STREAM_PUBLISH_WINDOW).await?,
        2
    );
    Ok(())
}

/// Any other broker counts the window per connection, so its streams share
/// one and the client never sends more than that broker reads.
#[tokio::test]
async fn a_connection_window_is_shared_by_its_streams() -> Result<()> {
    assert_eq!(slots_left_beside_a_full_window(0).await?, 0);
    Ok(())
}
