//! Peer request handlers run on the broker's runtime, not the QUIC I/O one.
//!
//! Its own test binary because the I/O runtime pool is process-wide and sized
//! from `FELIX_IO_RUNTIME_THREADS` when the first endpoint is built; sharing a
//! binary with tests that build endpoints first would decide that for us.
//!
//! Run with `cargo test -p felix-broker-service --test peer_handler_runtime`.
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use felix_replication::peer::{PeerPool, PeerRequestHandler, PeerServer, PeerTransportConfig};
use felix_wire::internal::{AckMode, ForwardPublish, ForwardPublishOk, InternalMessage, ShardRef};
use tokio_util::sync::CancellationToken;

const PEER: &str = "broker-b";
const APP_THREAD: &str = "felix-app-worker";
/// A request with this generation makes the handler block its thread.
const SLOW: u64 = 1;
const SLOW_FOR: Duration = Duration::from_secs(3);

/// Turn the I/O runtime on, as it is by default on macOS, before anything in
/// this binary builds an endpoint.
fn enable_io_runtime() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: runs before either test starts a runtime or reads the
        // environment, and `Once` holds the other test until it is done.
        unsafe { std::env::set_var("FELIX_IO_RUNTIME_THREADS", "2") };
    });
}

fn app_runtime() -> tokio::runtime::Runtime {
    enable_io_runtime();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(APP_THREAD)
        .enable_all()
        .build()
        .expect("runtime")
}

/// Records the thread each request ran on and echoes the generation back.
struct RecordingHandler {
    threads: parking_lot::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl PeerRequestHandler for RecordingHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        let generation = match &request {
            InternalMessage::ForwardPublish(publish) => publish.shard.generation,
            _ => 0,
        };
        self.threads.lock().push(
            std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_string(),
        );
        if generation == SLOW {
            // Blocking on purpose: stands in for CPU-bound auth or anything
            // else that holds its thread. On the I/O runtime that thread is
            // also the endpoint's driver.
            std::thread::sleep(SLOW_FOR);
        }
        InternalMessage::ForwardPublishOk(ForwardPublishOk {
            correlation_id: request.correlation_id(),
            first_offset: generation,
            last_offset: generation,
        })
    }
}

fn config() -> PeerTransportConfig {
    PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        request_timeout: Duration::from_secs(10),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

fn forward(generation: u64) -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 0,
        shard: ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation,
        },
        ack: AckMode::None,
        payloads: vec![],
        credential: String::new(),
    })
}

struct Harness {
    handler: Arc<RecordingHandler>,
    pool: Arc<PeerPool>,
    addr: std::net::SocketAddr,
    shutdown: CancellationToken,
}

impl Harness {
    fn start() -> Self {
        let handler = Arc::new(RecordingHandler {
            threads: parking_lot::Mutex::new(Vec::new()),
        });
        let server = PeerServer::bind(
            PEER.to_string(),
            &config(),
            Arc::clone(&handler) as Arc<dyn PeerRequestHandler>,
        )
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let shutdown = CancellationToken::new();
        tokio::spawn(server.serve(shutdown.clone()));
        let pool = PeerPool::new("broker-a".to_string(), config(), CancellationToken::new())
            .expect("pool");
        Self {
            handler,
            pool,
            addr,
            shutdown,
        }
    }

    async fn request(&self, generation: u64) -> u64 {
        match self
            .pool
            .request(PEER, self.addr, forward(generation))
            .await
            .expect("request")
        {
            InternalMessage::ForwardPublishOk(ok) => ok.first_offset,
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn stop(self) {
        self.pool.shutdown().await;
        self.shutdown.cancel();
    }
}

#[test]
fn peer_handlers_run_on_the_app_runtime() {
    app_runtime().block_on(async {
        let harness = Harness::start();
        for generation in 10..14 {
            assert_eq!(harness.request(generation).await, generation);
        }
        let threads = harness.handler.threads.lock().clone();
        assert_eq!(threads.len(), 4);
        for name in &threads {
            assert_eq!(
                name, APP_THREAD,
                "a peer handler ran on {name}, not a worker of the broker's runtime",
            );
        }
        harness.stop().await;
    });
}

/// A handler that holds its thread must not hold up requests on other streams.
/// On the single-threaded I/O runtime it would stall the endpoint driver too,
/// so nothing else on that listener could even be read.
#[test]
fn a_slow_handler_does_not_block_other_peer_requests() {
    app_runtime().block_on(async {
        let harness = Arc::new(Harness::start());
        // Connect and handshake first, so the slow request is not also
        // waiting on the dial.
        assert_eq!(harness.request(0).await, 0);

        let slow = {
            let harness = Arc::clone(&harness);
            tokio::spawn(async move { harness.request(SLOW).await })
        };
        // Give the slow request time to reach the handler.
        while harness.handler.threads.lock().len() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The pool spreads requests across streams round-robin, so this one is
        // on a different stream from the slow one.
        let started = Instant::now();
        let fast = tokio::time::timeout(SLOW_FOR / 2, harness.request(2)).await;
        assert_eq!(
            fast.ok(),
            Some(2),
            "a fast peer request waited on a slow handler ({:?})",
            started.elapsed(),
        );

        assert_eq!(slow.await.expect("slow task"), SLOW);
        Arc::into_inner(harness).expect("only owner").stop().await;
    });
}
