//! A broker that answers every publish and subscribe with one scripted
//! message, and counts what it was sent.
//!
//! It advertises every frame flag and only `FEATURE_ERROR_CODES`, so a
//! [`ClusterClient`](crate::ClusterClient) skips discovery and sends its
//! publishes as acked binary batches, answered here with coded binary acks.
//! A subscribe the script answers with `Subscribed` gets an event stream,
//! held open for the life of the connection.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicConnection, QuicServer, TransportConfig};
use felix_wire::Message;
use rustls::pki_types::CertificateDer;

use crate::frame_io::{read_frame_into, write_message};
use crate::test_support::build_server_config;

/// The answer to one request, given its request id (0 for a subscribe).
type Script = Arc<dyn Fn(u64) -> Message + Send + Sync>;

pub(super) struct StubBroker {
    pub(super) addr: SocketAddr,
    publishes: Arc<AtomicUsize>,
    subscribes: Arc<AtomicUsize>,
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
        let publishes = Arc::new(AtomicUsize::new(0));
        let subscribes = Arc::new(AtomicUsize::new(0));
        let script: Script = Arc::new(script);
        let connections = Arc::new(Mutex::new(Vec::new()));
        let counts = (Arc::clone(&publishes), Arc::clone(&subscribes));
        let accepted = Arc::clone(&connections);
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                accepted.lock().unwrap().push(connection.clone());
                let script = Arc::clone(&script);
                let counts = (Arc::clone(&counts.0), Arc::clone(&counts.1));
                tokio::spawn(async move {
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        let script = Arc::clone(&script);
                        let counts = (Arc::clone(&counts.0), Arc::clone(&counts.1));
                        tokio::spawn(serve_stream(connection.clone(), send, recv, script, counts));
                    }
                });
            }
        });
        Ok(Self {
            addr,
            publishes,
            subscribes,
            connections,
            task,
        })
    }

    pub(super) fn publishes(&self) -> usize {
        self.publishes.load(Ordering::SeqCst)
    }

    pub(super) fn subscribes(&self) -> usize {
        self.subscribes.load(Ordering::SeqCst)
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

async fn serve_stream(
    connection: QuicConnection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    script: Script,
    (publishes, subscribes): (Arc<AtomicUsize>, Arc<AtomicUsize>),
) -> Result<()> {
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(frame) = read_frame_into(&mut recv, &mut scratch, false).await? {
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0 {
            let id = felix_wire::binary::decode_acked_publish_batch(&frame)?.request_id;
            publishes.fetch_add(1, Ordering::SeqCst);
            send.write_all(&binary_ack(id, script(id))?).await?;
            continue;
        }
        match Message::decode(frame)? {
            Message::Auth { .. } => {
                let answer = Message::AuthOk {
                    server_flags: felix_wire::KNOWN_FLAGS,
                    server_features: Some(felix_wire::FEATURE_ERROR_CODES),
                    listener_ports: None,
                    publish_window: None,
                };
                write_message(&mut send, answer).await?;
            }
            Message::Subscribe { .. } => {
                subscribes.fetch_add(1, Ordering::SeqCst);
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
        _ => felix_wire::binary::encode_publish_ack_bytes(request_id, None)?,
    };
    Ok(bytes)
}
