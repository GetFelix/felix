//! A broker that answers every publish and subscribe with one scripted
//! message, and counts what it was sent.
//!
//! It advertises every frame flag and only `FEATURE_ERROR_CODES`,
//! `FEATURE_IDEMPOTENT_PRODUCER`, `FEATURE_STREAM_SHARDS` (every stream has
//! [`STREAM_SHARDS`]) and `FEATURE_SHARD_OWNERS` (answered for caches from
//! [`StubBroker::set_cache_owners`]), so a
//! [`ClusterClient`](crate::ClusterClient) skips discovery and sends its
//! publishes as acked binary batches, answered here with coded binary acks.
//! A subscribe the script answers with `Subscribed` gets an event stream,
//! held open for the life of the connection unless
//! [`StubBroker::set_events`] ends it.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicConnection, QuicServer, TransportConfig};
use felix_wire::{Message, StartPosition};
use rustls::pki_types::CertificateDer;

use crate::frame_io::{read_frame_into, write_message};
use crate::test_support::build_server_config;

/// The answer to one request, given its request id (0 for a subscribe).
type Script = Arc<dyn Fn(u64) -> Message + Send + Sync>;

/// What a subscription's event stream carries, given the shard and start it
/// asked for. A stream that ends with `subscription_lagged` is finished after
/// it, as a broker does.
type EventScript = Arc<dyn Fn(Option<u32>, Option<StartPosition>) -> Vec<Message> + Send + Sync>;

/// The shard and start position a subscribe asked for.
pub(super) type SubscribeStart = (Option<u32>, Option<StartPosition>);

/// An idempotent batch's routing key and sequence.
pub(super) type Sequenced = (Option<bytes::Bytes>, u64);

/// How many shards every stream has here, by modulo.
pub(super) const STREAM_SHARDS: u32 = 4;

pub(super) struct StubBroker {
    pub(super) addr: SocketAddr,
    seen: Seen,
    connections: Arc<Mutex<Vec<QuicConnection>>>,
    task: tokio::task::JoinHandle<()>,
}

impl StubBroker {
    /// A stub with its own certificate, returned for the client to trust.
    pub(super) fn start(
        script: impl Fn(u64) -> Message + Send + Sync + 'static,
    ) -> Result<(Self, CertificateDer<'static>)> {
        let (server_config, cert) = build_server_config()?;
        Ok((Self::start_with(server_config, script)?, cert))
    }

    /// A stub sharing a certificate with another, so one client trusts both.
    pub(super) fn start_with(
        server_config: quinn::ServerConfig,
        script: impl Fn(u64) -> Message + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::start_limited(
            server_config,
            TransportConfig::default().max_streams,
            script,
        )
    }

    /// A stub granting each connection `max_streams` concurrent streams.
    pub(super) fn start_limited(
        server_config: quinn::ServerConfig,
        max_streams: u16,
        script: impl Fn(u64) -> Message + Send + Sync + 'static,
    ) -> Result<Self> {
        let transport = TransportConfig {
            max_streams,
            ..TransportConfig::default()
        };
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport)?;
        let addr = server.local_addr()?;
        let seen = Seen::default();
        let script: Script = Arc::new(script);
        let connections = Arc::new(Mutex::new(Vec::new()));
        let counts = seen.clone();
        let accepted = Arc::clone(&connections);
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                accepted.lock().unwrap().push(connection.clone());
                let script = Arc::clone(&script);
                let counts = counts.clone();
                tokio::spawn(async move {
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        let script = Arc::clone(&script);
                        let counts = counts.clone();
                        tokio::spawn(serve_stream(connection.clone(), send, recv, script, counts));
                    }
                });
            }
        });
        Ok(Self {
            addr,
            seen,
            connections,
            task,
        })
    }

    pub(super) fn publishes(&self) -> usize {
        self.seen.publishes.load(Ordering::SeqCst)
    }

    /// Script what each subscription's event stream sends after its hello.
    pub(super) fn set_events(
        &self,
        events: impl Fn(Option<u32>, Option<StartPosition>) -> Vec<Message> + Send + Sync + 'static,
    ) {
        *self.seen.events.lock().unwrap() = Some(Arc::new(events));
    }

    /// The shard and start position of each subscribe, in arrival order.
    pub(super) fn subscribe_starts(&self) -> Vec<SubscribeStart> {
        self.seen.starts.lock().unwrap().clone()
    }

    pub(super) fn subscribes(&self) -> usize {
        self.seen.subscribes.load(Ordering::SeqCst)
    }

    /// The shard each subscribe asked for, in order; `None` is unset.
    pub(super) fn subscribed_shards(&self) -> Vec<Option<u32>> {
        self.seen.shards.lock().unwrap().clone()
    }

    /// The routing key of each idempotent batch and the id of the QUIC stream
    /// it arrived on, in order.
    pub(super) fn batch_streams(&self) -> Vec<Sequenced> {
        self.seen.batch_streams.lock().unwrap().clone()
    }

    /// The routing key and sequence of each idempotent batch, in order.
    pub(super) fn sequences(&self) -> Vec<Sequenced> {
        self.seen.sequences.lock().unwrap().clone()
    }

    /// Cache requests it was sent.
    pub(super) fn cache_requests(&self) -> usize {
        self.seen.cache_requests.load(Ordering::SeqCst)
    }

    /// What it answers `shard_owners` with, for any cache.
    pub(super) fn set_cache_owners(&self, owners: Vec<felix_wire::ShardOwner>) {
        *self.seen.cache_owners.lock().unwrap() = owners;
    }

    /// Client connections still open, however many clients hold them.
    pub(super) fn live_connections(&self) -> usize {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .filter(|connection| connection.close_reason().is_none())
            .count()
    }

    /// Client connections ever accepted.
    pub(super) fn accepted_connections(&self) -> usize {
        self.connections.lock().unwrap().len()
    }

    /// Hold every publish answer until [`Self::release_acks`], so a client
    /// is left waiting with its batch already received.
    pub(super) fn hold_acks(&self) {
        self.seen.acks_held.send_replace(true);
    }

    pub(super) fn release_acks(&self) {
        self.seen.acks_held.send_replace(false);
    }

    /// Close every client connection, as a broker that lost its network
    /// would, while still accepting new ones.
    pub(super) fn drop_connections(&self) {
        for connection in self.connections.lock().unwrap().iter() {
            connection.close(9u32.into(), b"dropped");
        }
    }
}

impl Drop for StubBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What the stub was sent, shared by every stream it serves.
#[derive(Clone)]
struct Seen {
    publishes: Arc<AtomicUsize>,
    subscribes: Arc<AtomicUsize>,
    /// The shard each subscribe asked for.
    shards: Arc<Mutex<Vec<Option<u32>>>>,
    /// The shard and start of each subscribe.
    starts: Arc<Mutex<Vec<SubscribeStart>>>,
    events: Arc<Mutex<Option<EventScript>>>,
    /// The routing key of each idempotent batch and the QUIC stream it came on.
    batch_streams: Arc<Mutex<Vec<Sequenced>>>,
    sequences: Arc<Mutex<Vec<Sequenced>>>,
    cache_requests: Arc<AtomicUsize>,
    cache_owners: Arc<Mutex<Vec<felix_wire::ShardOwner>>>,
    /// True while publish answers are held back.
    acks_held: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Default for Seen {
    fn default() -> Self {
        Self {
            publishes: Arc::default(),
            subscribes: Arc::default(),
            shards: Arc::default(),
            starts: Arc::default(),
            events: Arc::default(),
            batch_streams: Arc::default(),
            sequences: Arc::default(),
            cache_requests: Arc::default(),
            cache_owners: Arc::default(),
            acks_held: Arc::new(tokio::sync::watch::channel(false).0),
        }
    }
}

async fn serve_stream(
    connection: QuicConnection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    script: Script,
    seen: Seen,
) -> Result<()> {
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(frame) = read_frame_into(&mut recv, &mut scratch, false).await? {
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0 {
            let batch = felix_wire::binary::decode_acked_publish_batch(&frame)?;
            let id = batch.request_id;
            seen.publishes.fetch_add(1, Ordering::SeqCst);
            if let Some(producer) = batch.producer {
                seen.batch_streams
                    .lock()
                    .unwrap()
                    .push((batch.batch.key.clone(), u64::from(recv.id())));
                seen.sequences
                    .lock()
                    .unwrap()
                    .push((batch.batch.key, producer.sequence));
            }
            let mut held = seen.acks_held.subscribe();
            held.wait_for(|held| !held).await?;
            send.write_all(&binary_ack(id, script(id))?).await?;
            continue;
        }
        match Message::decode(frame)? {
            Message::Auth { .. } => {
                let answer = Message::AuthOk {
                    server_flags: felix_wire::KNOWN_FLAGS,
                    server_features: Some(
                        felix_wire::FEATURE_ERROR_CODES
                            | felix_wire::FEATURE_IDEMPOTENT_PRODUCER
                            | felix_wire::FEATURE_STREAM_SHARDS
                            | felix_wire::FEATURE_SHARD_OWNERS,
                    ),
                    server_features_hi: None,
                    listener_ports: None,
                    publish_window: None,
                };
                write_message(&mut send, answer).await?;
            }
            Message::ProducerInit { request_id } => {
                let answer = Message::ProducerInitOk {
                    request_id,
                    producer_id: 1,
                };
                write_message(&mut send, answer).await?;
            }
            Message::StreamShards { request_id, .. } => {
                let answer = Message::StreamShardsView {
                    shards: STREAM_SHARDS,
                    request_id,
                    routing: None,
                };
                write_message(&mut send, answer).await?;
            }
            Message::ShardOwners { request_id, .. } => {
                let owners = seen.cache_owners.lock().unwrap().clone();
                write_message(&mut send, Message::ShardOwnersView { owners, request_id }).await?;
            }
            // The script's error is the answer; anything else is success.
            Message::CachePut { request_id, .. } => {
                let id = request_id.unwrap_or_default();
                seen.cache_requests.fetch_add(1, Ordering::SeqCst);
                let answer = match script(id) {
                    error @ Message::Error { .. } => error,
                    _ => Message::CacheOk { request_id: id },
                };
                write_message(&mut send, answer).await?;
            }
            Message::CacheGet {
                tenant_id,
                namespace,
                cache,
                key,
                request_id,
            } => {
                seen.cache_requests.fetch_add(1, Ordering::SeqCst);
                let answer = match script(request_id.unwrap_or_default()) {
                    error @ Message::Error { .. } => error,
                    _ => Message::CacheValue {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        value: Some(bytes::Bytes::from_static(b"stub")),
                        request_id,
                        version: None,
                    },
                };
                write_message(&mut send, answer).await?;
            }
            Message::Subscribe { shard, start, .. } => {
                seen.subscribes.fetch_add(1, Ordering::SeqCst);
                seen.shards.lock().unwrap().push(shard);
                seen.starts.lock().unwrap().push((shard, start));
                let scripted = seen
                    .events
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|events| events(shard, start));
                let answer = match script(0) {
                    Message::Subscribed {
                        start_offset,
                        live_offset,
                        ..
                    } => Message::Subscribed {
                        subscription_id: next_subscription_id(),
                        start_offset,
                        live_offset,
                    },
                    other => other,
                };
                let opened = match &answer {
                    Message::Subscribed {
                        subscription_id, ..
                    } => Some(*subscription_id),
                    _ => None,
                };
                write_message(&mut send, answer).await?;
                if let Some(subscription_id) = opened {
                    let mut events = connection.open_uni().await?;
                    write_message(&mut events, Message::EventStreamHello { subscription_id })
                        .await?;
                    let mut ended = false;
                    for message in scripted.unwrap_or_default() {
                        ended = matches!(message, Message::SubscriptionLagged { .. });
                        write_message(&mut events, message).await?;
                    }
                    if ended {
                        events.finish()?;
                        continue;
                    }
                    // Never finished: the subscription stays open until the
                    // connection goes.
                    let held = connection.clone();
                    tokio::spawn(async move {
                        let _events = events;
                        held.closed().await;
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn next_subscription_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::SeqCst)
}

/// A scripted publish answer as the binary ack the client expects.
fn binary_ack(request_id: u64, answer: Message) -> Result<bytes::Bytes> {
    let bytes = match answer {
        Message::PublishError {
            message,
            code,
            retry,
            ..
        } => {
            let code = code.map(|code| {
                let retry = retry.unwrap_or_else(|| code.default_retry());
                (code, retry)
            });
            felix_wire::binary::encode_publish_ack_bytes_coded(
                request_id,
                Some(&message),
                code.as_ref().map(|(code, retry)| (code, *retry)),
                None,
            )?
        }
        Message::PublishOk { offset, .. } => felix_wire::binary::encode_publish_ack_bytes_at(
            request_id, None, None, None, offset, None,
        )?,
        _ => felix_wire::binary::encode_publish_ack_bytes(request_id, None)?,
    };
    Ok(bytes)
}
