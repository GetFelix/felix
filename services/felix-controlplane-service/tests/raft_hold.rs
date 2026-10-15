//! The replication hold in a real group: a group with a join floor whose
//! members are all at it keeps electing, committing and heartbeating when
//! the members' standing cannot be read.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};

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
        10
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
                if request.uri().path() == "/internal/raft/standing" {
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
                next.run(request).await
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
    for mode in [FAIL, HANG] {
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
        leader.write(b"before".to_vec()).await.expect("write");

        standing.store(mode, Ordering::SeqCst);
        let old = leader.status().id;
        let index = nodes
            .iter()
            .position(|node| node.handle.status().id == old)
            .expect("leader");
        let stopped = nodes.remove(index);
        stopped.handle.shutdown().await.expect("shutdown");
        stopped.server.abort();

        let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).await;
        // Only two members are left, so every commit needs the follower.
        leader
            .write(b"after".to_vec())
            .await
            .unwrap_or_else(|err| panic!("mode {mode}: no commit after the leader change: {err}"));
        let term = leader.status().term;
        for i in 0..10 {
            leader
                .write(format!("w{i}").into_bytes())
                .await
                .unwrap_or_else(|err| panic!("mode {mode}: write {i}: {err}"));
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
