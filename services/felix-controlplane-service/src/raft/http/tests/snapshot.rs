//! The streamed snapshot route of a member on the segment store.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use felix_storage::raft_log::transfer;
use futures_util::StreamExt;

use super::*;
use crate::raft::wire;

/// Keeps the size of the last snapshot it restored.
#[derive(Default)]
struct Restored(AtomicUsize);

#[async_trait::async_trait]
impl AppStateMachine for Restored {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, snapshot: &[u8]) {
        self.0.store(snapshot.len(), Ordering::SeqCst);
    }
}

async fn member(dir: &std::path::Path, cap: u64, app: Arc<Restored>) -> RaftHandle {
    let mut settings = RaftSettings::new(
        2,
        dir.into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some(TOKEN.to_string()),
            tls: None,
        },
    );
    settings.write_timeout = Duration::from_millis(200);
    settings.snapshot_max_bytes = cap;
    RaftHandle::start(settings, app).await.expect("start")
}

fn head(term: u64, index: u64) -> Vec<u8> {
    let nodes: BTreeMap<u64, wire::Node> = [1, 2]
        .into_iter()
        .map(|id| {
            let addr = format!("127.0.0.1:{}", 1 + id);
            (id, wire::Node { addr })
        })
        .collect();
    wire::SnapshotHead {
        vote: wire::Vote {
            term,
            voted_for: 1,
            committed: true,
        },
        meta: wire::SnapshotMeta {
            last_log_id: Some(wire::LogId { term, index }),
            last_membership: wire::StoredMembership {
                log_id: Some(wire::LogId { term: 1, index: 1 }),
                membership: wire::Membership {
                    configs: vec![[1, 2].into()],
                    nodes,
                },
            },
        },
    }
    .encode()
}

/// A whole body in 64 KiB pieces.
fn whole(term: u64, index: u64, data: &[u8]) -> Vec<Bytes> {
    let mut crc = transfer::DataCrc::default();
    crc.update(data);
    let mut pieces = vec![transfer::encode_start(
        &head(term, index),
        data.len() as u64,
    )];
    pieces.extend(data.chunks(64 * 1024).map(Bytes::copy_from_slice));
    pieces.push(crc.trailer());
    pieces
}

/// `pieces`, then a body that never ends, counting the pieces read.
fn body_then_hang(pieces: Vec<Bytes>, read: Arc<AtomicUsize>) -> Body {
    let stream = futures_util::stream::iter(pieces)
        .chain(futures_util::stream::pending())
        .map(move |piece| {
            read.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(piece)
        });
    Body::from_stream(stream)
}

async fn send(router: &axum::Router, body: Body) -> (StatusCode, Bytes) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("{PEER_PREFIX}/install-snapshot"))
        .header(CLUSTER_ID_HEADER, "test-cluster")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(body)
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, body)
}

fn answered_vote(body: &[u8]) -> wire::Vote {
    match serde_json::from_slice(body).expect("reply") {
        wire::Reply::<wire::InstallSnapshotResponse>::Ok(response) => response.vote,
        wire::Reply::Err(err) => panic!("refused: {err}"),
    }
}

/// A snapshot larger than the router's 16 MiB body limit installs: the
/// route streams it to a file instead of buffering it.
#[tokio::test]
async fn a_snapshot_over_the_router_limit_installs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = Arc::new(Restored::default());
    let handle = member(dir.path(), u64::MAX, Arc::clone(&app)).await;
    let data = vec![7u8; 20 << 20];
    let body = Body::from_stream(
        futures_util::stream::iter(whole(5, 10, &data)).map(Ok::<_, std::io::Error>),
    );
    let (status, reply) = send(&handle.rpc_router(), body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(answered_vote(&reply).term, 5);
    assert_eq!(app.0.load(Ordering::SeqCst), data.len());
    handle.shutdown().await.expect("shutdown");
}

/// The data goes to the receive file as it arrives, not into memory: with
/// half a snapshot sent, half of it is on disk.
#[tokio::test]
async fn a_snapshot_is_written_to_its_file_as_it_arrives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = member(dir.path(), u64::MAX, Arc::default()).await;
    let data = vec![3u8; 8 << 20];
    let mut pieces = whole(5, 10, &data);
    pieces.truncate(pieces.len() / 2);
    let sent: usize = pieces.iter().skip(1).map(Bytes::len).sum();
    let router = handle.rpc_router();
    let request =
        tokio::spawn(async move { send(&router, body_then_hang(pieces, Arc::default())).await });
    let file = dir.path().join("store").join("raft-snapshot.recv");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let len = std::fs::metadata(&file).map(|meta| meta.len()).unwrap_or(0);
        if len as usize >= sent {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{len} of {sent} bytes on disk"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    request.abort();
    handle.shutdown().await.expect("shutdown");
}

/// A declared length over the member's cap is refused from the head, with
/// the data never read.
#[tokio::test]
async fn a_snapshot_over_the_cap_is_refused_before_its_data_is_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = member(dir.path(), 1024, Arc::default()).await;
    let read = Arc::new(AtomicUsize::new(0));
    let pieces = whole(5, 10, &[1u8; 4096])[..1].to_vec();
    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        send(
            &handle.rpc_router(),
            body_then_hang(pieces, Arc::clone(&read)),
        ),
    )
    .await
    .expect("the route waited for the data");
    assert_eq!(answer.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(read.load(Ordering::SeqCst), 1);
    handle.shutdown().await.expect("shutdown");
}

/// A sender behind this member's vote is answered with it from the head,
/// before its data is read.
#[tokio::test]
async fn a_stale_sender_is_answered_before_its_data_is_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = member(dir.path(), u64::MAX, Arc::default()).await;
    let router = handle.rpc_router();
    let body = Body::from_stream(
        futures_util::stream::iter(whole(5, 10, b"state")).map(Ok::<_, std::io::Error>),
    );
    assert_eq!(send(&router, body).await.0, StatusCode::OK);

    let read = Arc::new(AtomicUsize::new(0));
    let pieces = whole(3, 20, b"stale")[..1].to_vec();
    let (status, reply) = tokio::time::timeout(
        Duration::from_secs(5),
        send(&router, body_then_hang(pieces, Arc::clone(&read))),
    )
    .await
    .expect("the route waited for a stale sender's data");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answered_vote(&reply).term, 5);
    assert_eq!(read.load(Ordering::SeqCst), 1);
    handle.shutdown().await.expect("shutdown");
}

/// One install at a time: a second sender with the same vote is refused at
/// once, and a sender with a newer vote stops the first and installs.
#[tokio::test]
async fn the_install_slot_refuses_a_second_sender_and_yields_to_a_newer_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = Arc::new(Restored::default());
    let handle = member(dir.path(), u64::MAX, Arc::clone(&app)).await;
    let router = handle.rpc_router();
    let mut first = whole(5, 10, &[1u8; 200 << 10]);
    first.truncate(2);
    let held = {
        let router = router.clone();
        tokio::spawn(async move { send(&router, body_then_hang(first, Arc::default())).await })
    };
    let file = dir.path().join("store").join("raft-snapshot.recv");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !file.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let second = whole(5, 10, b"same vote")[..1].to_vec();
    let (status, _) = tokio::time::timeout(
        Duration::from_secs(5),
        send(&router, body_then_hang(second, Arc::default())),
    )
    .await
    .expect("the second sender was queued");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let newer = Body::from_stream(
        futures_util::stream::iter(whole(6, 12, b"newer")).map(Ok::<_, std::io::Error>),
    );
    let (status, reply) = tokio::time::timeout(Duration::from_secs(10), send(&router, newer))
        .await
        .expect("the newer sender waited on the first");
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(app.0.load(Ordering::SeqCst), b"newer".len());
    let (status, _) = held.await.expect("join");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    handle.shutdown().await.expect("shutdown");
}
