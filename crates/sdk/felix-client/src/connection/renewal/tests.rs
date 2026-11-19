//! A stream that outlives its token moves onto the provider's newer one, so a
//! write forwarded on it carries a token the owner still accepts.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::sync::{mpsc, oneshot};

use crate::TokenFuture;
use crate::auth::TokenProvider;
use crate::cache::{CacheRequest, run_cache_worker_with_limit};
use crate::connection::{Credentials, NodeConnections, NodeLimits};
use crate::frame_io::{read_message_with_limit, write_message};
use crate::publish::TestPublishRequest as PublishRequest;
use crate::publish::run_publisher_writer_with_limit;
use crate::test_support::{build_server_config, quinn_client_config};

const MAX_FRAME: usize = 1024 * 1024;

/// A token the test replaces by hand.
struct Swappable(Mutex<String>);

impl Swappable {
    fn set(&self, token: &str) {
        *self.0.lock().unwrap() = token.to_string();
    }
}

impl TokenProvider for Swappable {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.lock().unwrap().clone();
        Box::pin(async move { Ok(token) })
    }
}

/// What the broker saw after authentication: the token of the stream a
/// request arrived on, and the request's kind.
type Seen = Arc<Mutex<Vec<(String, &'static str)>>>;

struct Broker {
    addr: SocketAddr,
    seen: Seen,
    task: tokio::task::JoinHandle<()>,
}

impl Broker {
    fn start() -> Result<(Self, QuicClient)> {
        let (server_config, cert) = build_server_config()?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let seen: Seen = Arc::default();
        let task = tokio::spawn({
            let seen = Arc::clone(&seen);
            async move {
                while let Ok(connection) = server.accept().await {
                    let seen = Arc::clone(&seen);
                    tokio::spawn(async move {
                        while let Ok((send, recv)) = connection.accept_bi().await {
                            tokio::spawn(serve(send, recv, Arc::clone(&seen)));
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
        Ok((Self { addr, seen, task }, client))
    }

    async fn seen_len(&self, at_least: usize) -> Vec<(String, &'static str)> {
        for _ in 0..200 {
            let seen = self.seen.lock().unwrap().clone();
            if seen.len() >= at_least {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the broker saw {:?}", self.seen.lock().unwrap());
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut send: quinn::SendStream, mut recv: quinn::RecvStream, seen: Seen) -> Result<()> {
    let mut scratch = BytesMut::new();
    let Some(Message::Auth { token, .. }) =
        read_message_with_limit(&mut recv, &mut scratch, MAX_FRAME).await?
    else {
        return Ok(());
    };
    write_message(&mut send, Message::Ok).await?;
    while let Some(message) = read_message_with_limit(&mut recv, &mut scratch, MAX_FRAME).await? {
        let (kind, answer) = match message {
            Message::Publish { .. } => ("publish", None),
            Message::CachePut {
                request_id: Some(request_id),
                ..
            } => ("cache_put", Some(Message::CacheOk { request_id })),
            _ => ("other", Some(Message::Ok)),
        };
        seen.lock().unwrap().push((token.clone(), kind));
        if let Some(answer) = answer {
            write_message(&mut send, answer).await?;
        }
    }
    Ok(())
}

async fn open(
    broker: &Broker,
    endpoint: QuicClient,
    tokens: &Arc<Swappable>,
) -> Result<(Arc<NodeConnections>, crate::connection::OpenedStream)> {
    let credentials = Arc::new(Credentials::new("t1".to_string(), tokens.clone()));
    let node = Arc::new(NodeConnections::new(
        endpoint,
        broker.addr,
        "localhost",
        credentials,
        NodeLimits {
            role: "test",
            ceiling: 1,
            streams_per_conn: 8,
            spread: false,
            max_frame_bytes: MAX_FRAME,
            event_router_max_pending: 16,
        },
    ));
    let opened = node.open().await?;
    Ok((node, opened))
}

fn publish() -> Message {
    Message::Publish {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "s".into(),
        payload: b"x".to_vec(),
        key: None,
        shard: None,
        request_id: None,
        ack: None,
    }
}

async fn send_publish(tx: &mpsc::Sender<PublishRequest>) -> Result<()> {
    let (response, answer) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: publish(),
        ack: AckMode::None,
        request_id: None,
        _permit: Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap(),
        response,
    })
    .await?;
    answer.await??;
    Ok(())
}

/// The provider's renewed token reaches a publish stream before its next
/// write, and a token that did not change leaves the stream alone.
#[tokio::test]
async fn a_publish_stream_writes_with_the_renewed_token() -> Result<()> {
    let (broker, endpoint) = Broker::start()?;
    let tokens = Arc::new(Swappable(Mutex::new("old".to_string())));
    let (_node, opened) = open(&broker, endpoint, &tokens).await?;
    let renewal = opened.renewal(MAX_FRAME).checking_every(Duration::ZERO);
    let (tx, rx) = mpsc::channel(8);
    let (send, recv) = (opened.send, opened.recv);
    let writer = tokio::spawn(async move {
        let _lease = opened.lease;
        run_publisher_writer_with_limit(send, recv, rx, MAX_FRAME, None, false, Some(renewal)).await
    });

    send_publish(&tx).await?;
    send_publish(&tx).await?;
    tokens.set("new");
    send_publish(&tx).await?;
    send_publish(&tx).await?;

    let seen = broker.seen_len(4).await;
    let tokens_seen: Vec<&str> = seen.iter().map(|(token, _)| token.as_str()).collect();
    assert_eq!(tokens_seen, ["old", "old", "new", "new"], "{seen:?}");
    drop(tx);
    writer.await??;
    Ok(())
}

/// Same for a cache stream, which carries cache writes the broker forwards.
#[tokio::test]
async fn a_cache_stream_writes_with_the_renewed_token() -> Result<()> {
    let (broker, endpoint) = Broker::start()?;
    let tokens = Arc::new(Swappable(Mutex::new("old".to_string())));
    let (_node, opened) = open(&broker, endpoint, &tokens).await?;
    let renewal = opened.renewal(MAX_FRAME).checking_every(Duration::ZERO);
    let (tx, rx) = mpsc::channel(8);
    let (send, recv) = (opened.send, opened.recv);
    let counts = Arc::new(vec![std::sync::atomic::AtomicUsize::new(1)]);
    let worker = tokio::spawn(async move {
        let _lease = opened.lease;
        run_cache_worker_with_limit(0, send, recv, rx, counts, MAX_FRAME, Some(renewal)).await
    });

    for (request_id, token) in [(1, "old"), (2, "new")] {
        tokens.set(token);
        let (response, answer) = oneshot::channel();
        tx.send(CacheRequest::Put {
            request_id,
            message: Message::CachePut {
                tenant_id: "t1".into(),
                namespace: "default".into(),
                cache: "c".into(),
                key: "k".into(),
                value: Bytes::from_static(b"v"),
                request_id: Some(request_id),
                ttl_ms: None,
            },
            response,
        })
        .await?;
        answer.await??;
    }
    let seen = broker.seen_len(2).await;
    let tokens_seen: Vec<&str> = seen.iter().map(|(token, _)| token.as_str()).collect();
    assert_eq!(tokens_seen, ["old", "new"], "{seen:?}");
    drop(tx);
    worker.await?;
    Ok(())
}
