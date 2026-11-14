use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use super::*;
use crate::raft::types::{Entry, EntryPayload, Membership, Vote, test_log_id};
use crate::raft::{CLUSTER_ID_HEADER, peer_route};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct NullApp;

#[async_trait::async_trait]
impl AppStateMachine for NullApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
}

fn settings(dir: &std::path::Path) -> RaftSettings {
    let mut settings = RaftSettings::new(
        1,
        dir.into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some(TOKEN.to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    settings
}

async fn call(member: &RaftMember, method: &Method, path: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(CLUSTER_ID_HEADER, "test-cluster")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request");
    let response = member
        .rpc_router()
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn standing(member: &RaftMember) -> (StatusCode, serde_json::Value) {
    let (status, body) = call(member, &Method::GET, &peer_route("standing")).await;
    (status, serde_json::from_str(&body).unwrap_or_default())
}

/// Every route but `standing`.
const NODE_ROUTES: [(Method, &str); 7] = [
    (Method::POST, "append-entries"),
    (Method::POST, "vote"),
    (Method::POST, "install-snapshot"),
    (Method::POST, "propose"),
    (Method::POST, "leader"),
    (Method::POST, "forming"),
    (Method::GET, "catch-up-target"),
];

async fn assert_node_routes_closed(member: &RaftMember) {
    for (method, name) in &NODE_ROUTES {
        let (status, body) = call(member, method, &peer_route(name)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{name}");
        assert!(body.contains("no raft node"), "{name}: {body}");
    }
}

/// An empty member with no Raft node says so, and says which member it is.
/// It answers nothing else, and neither does a node that is running but
/// not attached yet: the caller has still to settle whether it may vote.
#[tokio::test]
async fn a_member_without_a_node_answers_standing_and_nothing_else() {
    let dir = tempfile::tempdir().expect("tempdir");
    let member = RaftMember::open(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("open");

    let (status, answer) = standing(&member).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        answer,
        serde_json::json!({ "last_log_index": null, "version": 0, "raft": false, "member": 1 })
    );
    assert_node_routes_closed(&member).await;

    let handle = member.start_node().await.expect("start node");
    let (_, answer) = standing(&member).await;
    assert_eq!(answer["raft"], false, "a node not attached yet");
    assert_node_routes_closed(&member).await;

    member.attach(&handle);
    let (status, answer) = standing(&member).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["raft"], true);
    assert_eq!(answer["node_id"], 1);
    assert_eq!(answer["member"], 1);
    for (method, name) in &NODE_ROUTES {
        let (_, body) = call(&member, method, &peer_route(name)).await;
        assert!(!body.contains("no raft node"), "{name}: {body}");
    }
    handle.shutdown().await.expect("shutdown");
}

/// A member that holds a log and has no node yet cannot say how far the
/// log goes. Answering without it would read as an empty member, and a
/// peer on its first start could form a group over this one's data.
#[tokio::test]
async fn a_member_holding_state_gives_no_standing_before_its_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let nodes: BTreeMap<NodeId, openraft::impls::BasicNode> = (1..=3)
        .map(|id| {
            let addr = "127.0.0.1:1".to_string();
            (id, openraft::impls::BasicNode { addr })
        })
        .collect();
    let membership = Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes).expect("membership");
    let entry = Entry {
        log_id: test_log_id(5, 0),
        payload: EntryPayload::Membership(membership),
    };
    let vote = Vote::new_committed(5, 2);
    store::seed(dir.path(), "test-cluster", vote, vec![entry], false, None).await;

    let member = RaftMember::open(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("open");
    let (status, body) = call(&member, &Method::GET, &peer_route("standing")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");

    let handle = member.start_node().await.expect("start node");
    member.attach(&handle);
    let (status, answer) = standing(&member).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["last_log_index"], 0);
    handle.shutdown().await.expect("shutdown");
}

/// The level a peer reports while this member has no node is the node's to
/// use once it runs, or a new leader would count that peer as unknown.
#[tokio::test]
async fn what_a_peer_said_before_the_node_is_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let member = RaftMember::open(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("open");
    let asked = format!("{}?from=2&version=7", peer_route("standing"));
    let (status, _) = call(&member, &Method::GET, &asked).await;
    assert_eq!(status, StatusCode::OK);

    let handle = member.start_node().await.expect("start node");
    let reported = handle.versions.lock().expect("versions").reported(2);
    assert_eq!(reported, Some(7));
    handle.shutdown().await.expect("shutdown");
}

/// A member whose node never started gives its store back, so the same
/// process can open it again.
#[tokio::test]
async fn a_member_closed_without_a_node_releases_the_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let member = RaftMember::open(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("open");
    member.close().await.expect("close");
    assert!(member.start_node().await.is_err(), "a node after the close");

    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("open again");
    handle.shutdown().await.expect("shutdown");
}
