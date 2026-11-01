//! The replication hold in a real group: a group with a join floor whose
//! members are all at it keeps electing, committing and heartbeating when
//! the members' standing cannot be read.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};

// Diagnostics for #1705 (loop branch only).
static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn ms() -> f64 {
    T0.get_or_init(Instant::now).elapsed().as_secs_f64() * 1000.0
}
fn ev(line: String) {
    EVENTS.lock().unwrap().push(format!("{:9.1} {line}", ms()));
}
fn floor() -> u16 {
    std::env::var("DIAG_FLOOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

/// A counter app at metadata level 10 whose applied state needs level 10.
#[derive(Default)]
struct FloorApp {
    applied: Mutex<u64>,
}

#[async_trait::async_trait]
impl AppStateMachine for FloorApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        *self.applied.lock().expect("applied lock") += 1;
        Vec::new()
    }

    async fn snapshot(&self) -> Vec<u8> {
        self.applied
            .lock()
            .expect("applied lock")
            .to_string()
            .into_bytes()
    }

    async fn restore(&self, snapshot: &[u8]) {
        *self.applied.lock().expect("applied lock") =
            String::from_utf8_lossy(snapshot).parse().expect("snapshot");
    }

    fn version(&self) -> u16 {
        10
    }

    fn join_floor(&self) -> u16 {
        floor()
    }
}

/// How the `standing` route behaves.
const ANSWER: u8 = 0;
const FAIL: u8 = 1;
const HANG: u8 = 2;

struct Node {
    handle: RaftHandle,
    addr: String,
    server: tokio::task::JoinHandle<()>,
}

async fn start(id: NodeId, dir: &std::path::Path, standing: Arc<AtomicU8>) -> Node {
    let mut settings = RaftSettings::new(
        id,
        dir.into(),
        felix_controlplane_service::raft::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(300), Duration::from_millis(600));
    let handle = RaftHandle::start(settings, Arc::new(FloorApp::default()))
        .await
        .expect("start raft node");
    let router = handle.rpc_router().layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let standing = Arc::clone(&standing);
            async move {
                let path = request.uri().path().to_string();
                let started = ms();
                if path == "/internal/raft/standing" {
                    ev(format!(
                        "n{id} standing asked, mode {}",
                        standing.load(Ordering::SeqCst)
                    ));
                    match standing.load(Ordering::SeqCst) {
                        FAIL => {
                            return axum::response::IntoResponse::into_response(
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            );
                        }
                        HANG => tokio::time::sleep(Duration::from_secs(5)).await,
                        _ => {}
                    }
                }
                let response = next.run(request).await;
                if path != "/internal/raft/append-entries" || std::env::var("DIAG_ALL").is_ok() {
                    ev(format!(
                        "n{id} rpc {path} took {:.1} -> {}",
                        ms() - started,
                        response.status()
                    ));
                } else {
                    ev(format!("n{id} ae took {:.1}", ms() - started));
                }
                response
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Node {
        handle,
        addr,
        server,
    }
}

async fn leader_of(nodes: &[&Node]) -> RaftHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for node in nodes {
            if let Some(leader) = node.handle.status().leader
                && let Some(found) = nodes.iter().find(|n| n.handle.status().id == leader)
                && found.handle.status().leader == Some(leader)
            {
                return found.handle.clone();
            }
        }
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// After a leader change the new leader knows no member's level. With the
/// members' standing failing or hanging, it must still feed them: no member
/// is held, writes commit, and no further election follows.
#[tokio::test]
async fn a_new_leader_feeds_members_whose_standing_cannot_be_read() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(std::env::var("DIAG_LOG").unwrap_or_else(|_| {
            "openraft::core::raft_core=info,felix_controlplane_service=info".into()
        }))
        .try_init();
    ev(format!("start floor={}", floor()));
    tokio::spawn(async {
        loop {
            let t = Instant::now();
            tokio::time::sleep(Duration::from_millis(10)).await;
            let lag = t.elapsed().saturating_sub(Duration::from_millis(10));
            if lag > Duration::from_millis(40) {
                ev(format!("STALL {:.1} ms", lag.as_secs_f64() * 1000.0));
            }
        }
    });
    let result = std::panic::AssertUnwindSafe(body())
        .catch_unwind_compat()
        .await;
    if result.is_err() || std::env::var("DIAG_DUMP").is_ok() {
        for line in EVENTS.lock().unwrap().iter() {
            println!("{line}");
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

trait CatchUnwindCompat: std::future::Future + Sized {
    fn catch_unwind_compat(self) -> CatchUnwind<Self> {
        CatchUnwind(Box::pin(self))
    }
}
impl<F: std::future::Future> CatchUnwindCompat for F {}
struct CatchUnwind<F>(std::pin::Pin<Box<F>>);
impl<F: std::future::Future> std::future::Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let fut = self.0.as_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(v)) => std::task::Poll::Ready(Ok(v)),
            Err(e) => std::task::Poll::Ready(Err(e)),
        }
    }
}

async fn body() {
    for mode in [FAIL, HANG] {
        ev(format!("mode {mode} begin"));
        let dirs: Vec<tempfile::TempDir> = (0..3)
            .map(|_| tempfile::tempdir().expect("tempdir"))
            .collect();
        let standing = Arc::new(AtomicU8::new(ANSWER));
        let mut nodes = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            nodes.push(start((i + 1) as NodeId, dir.path(), Arc::clone(&standing)).await);
        }
        let members: BTreeMap<NodeId, String> = nodes
            .iter()
            .map(|node| (node.handle.status().id, node.addr.clone()))
            .collect();
        nodes[0]
            .handle
            .initialize(members)
            .await
            .expect("initialize");
        let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;
        ev(format!(
            "first leader n{} term {}",
            leader.status().id,
            leader.status().term
        ));
        leader.write(b"before".to_vec()).await.expect("write");

        standing.store(mode, Ordering::SeqCst);
        let old = leader.status().id;
        let index = nodes
            .iter()
            .position(|node| node.handle.status().id == old)
            .expect("leader");
        let stopped = nodes.remove(index);
        ev(format!("stopping n{old}"));
        stopped.handle.shutdown().await.expect("shutdown");
        stopped.server.abort();
        ev(format!("stopped n{old}"));

        let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;
        ev(format!(
            "new leader n{} term {}",
            leader.status().id,
            leader.status().term
        ));
        // Only two members are left, so every commit needs the follower.
        leader
            .write(b"after".to_vec())
            .await
            .unwrap_or_else(|err| panic!("mode {mode}: no commit after the leader change: {err}"));
        let term = leader.status().term;
        ev(format!("after committed, term {term}"));
        for i in 0..10 {
            leader
                .write(format!("w{i}").into_bytes())
                .await
                .unwrap_or_else(|err| panic!("mode {mode}: write {i}: {err}"));
            ev(format!(
                "w{i} committed; terms {:?}",
                nodes
                    .iter()
                    .map(|n| (
                        n.handle.status().id,
                        n.handle.status().term,
                        n.handle.status().leader
                    ))
                    .collect::<Vec<_>>()
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for node in &nodes {
            assert_eq!(
                node.handle.status().term,
                term,
                "mode {mode}: an election followed the leader change"
            );
        }
        for node in nodes {
            let _ = node.handle.shutdown().await;
            node.server.abort();
        }
    }
}
