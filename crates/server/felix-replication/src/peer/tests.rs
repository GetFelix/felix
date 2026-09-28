//! Transport behaviour under reuse, restart, loss, overload, and shutdown.
//!
//! Everything here drives the real endpoints over loopback. A stub would let the
//! two halves drift: the pool could send a frame the server never reads, or wait
//! on a response shape the server never sends, and every test would still pass.

mod connections;
mod listener;
mod mtls;
mod requests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use felix_wire::internal::{
    AckMode, ErrorCode, ForwardPublish, ForwardPublishOk, InternalMessage, ShardRef,
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::peer::server::PeerRequestHandler;

const PEER: &str = "broker-b";

fn config() -> PeerTransportConfig {
    PeerTransportConfig {
        // Port 0: every test binds its own listener, so nothing here depends on
        // a fixed port being free.
        bind: "127.0.0.1:0".parse().expect("addr"),
        request_timeout: Duration::from_millis(500),
        handshake_timeout: Duration::from_millis(500),
        reconnect_base: Duration::from_millis(20),
        reconnect_max: Duration::from_millis(40),
        ..Default::default()
    }
}

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        generation: 1,
    }
}

fn forward() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 0,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: vec![bytes::Bytes::from_static(b"payload")],
        credential: String::new(),
    })
}

/// Answers every forward with `Ok`, and counts what it saw.
#[derive(Default)]
struct CountingHandler {
    seen: AtomicUsize,
    /// Delay before answering, for the overload and timeout cases.
    delay: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for CountingHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.seen.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        InternalMessage::ForwardPublishOk(ForwardPublishOk {
            correlation_id: request.correlation_id(),
            first_offset: 10,
            last_offset: 10,
        })
    }
}

/// A running internal listener, with the handle needed to stop it.
struct Listener {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Listener {
    async fn start(node_id: &str, handler: Arc<dyn PeerRequestHandler>) -> Self {
        Self::start_on(node_id, handler, config()).await
    }

    async fn start_on(
        node_id: &str,
        handler: Arc<dyn PeerRequestHandler>,
        config: PeerTransportConfig,
    ) -> Self {
        let server = Self::bind_with_retry(node_id, handler, &config).await;
        let addr = server.local_addr().expect("addr");
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));
        Self {
            addr,
            shutdown,
            task,
        }
    }

    /// The restart case rebinds the port the previous listener held, and quinn
    /// releases it only once that endpoint's connections have finished draining.
    ///
    /// The wait must yield to the runtime. `#[tokio::test]` is single-threaded,
    /// so a blocking sleep here would stop the very tasks that close those
    /// connections, and the port would never come free however long we waited.
    async fn bind_with_retry(
        node_id: &str,
        handler: Arc<dyn PeerRequestHandler>,
        config: &PeerTransportConfig,
    ) -> PeerServer {
        let mut last = None;
        for _ in 0..100 {
            match PeerServer::bind(node_id.to_string(), config, Arc::clone(&handler)) {
                Ok(server) => return server,
                Err(err) => {
                    last = Some(err);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!("bind never succeeded: {:#}", last.expect("an error"));
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.task).await;
    }
}

fn pool(config: PeerTransportConfig) -> Arc<PeerPool> {
    PeerPool::new("broker-a".to_string(), config, CancellationToken::new()).expect("pool")
}
