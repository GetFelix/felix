use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use crate::raft::{
    AppStateMachine, CLUSTER_ID_HEADER, PEER_PREFIX, PeerSecurity, RaftHandle, RaftSettings,
};

mod snapshot;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The prefix a member of a release that kept its store in redb serves and
/// calls.
const OTHER_PREFIX: &str = "/internal/raft";

const ROUTES: [(Method, &str); 8] = [
    (Method::POST, "append-entries"),
    (Method::POST, "vote"),
    (Method::POST, "install-snapshot"),
    (Method::POST, "propose"),
    (Method::POST, "leader"),
    (Method::GET, "standing"),
    (Method::POST, "forming"),
    (Method::GET, "catch-up-target"),
];

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

async fn status(router: &axum::Router, method: &Method, path: String) -> StatusCode {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(CLUSTER_ID_HEADER, "test-cluster")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request");
    router
        .clone()
        .oneshot(request)
        .await
        .expect("response")
        .status()
}

/// A member of a release that kept its store in redb and this one must
/// never exchange Raft messages. Each serves every peer route under its own
/// prefix only, so a request from the other kind, credentials and all, is a
/// 404.
#[tokio::test]
async fn every_peer_route_is_served_under_this_stores_prefix_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = RaftSettings::new(
        1,
        dir.path().into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some(TOKEN.to_string()),
            tls: None,
        },
    );
    settings.write_timeout = Duration::from_millis(200);
    let handle = RaftHandle::start(settings, Arc::new(NullApp))
        .await
        .expect("start");
    let router = handle.rpc_router();

    assert_ne!(PEER_PREFIX, OTHER_PREFIX);
    for (method, name) in &ROUTES {
        let own = status(&router, method, format!("{PEER_PREFIX}/{name}")).await;
        assert_ne!(own, StatusCode::NOT_FOUND, "{name} under {PEER_PREFIX}");
        assert_ne!(own, StatusCode::METHOD_NOT_ALLOWED, "{name}");
        let other = status(&router, method, format!("{OTHER_PREFIX}/{name}")).await;
        assert_eq!(other, StatusCode::NOT_FOUND, "{name} under {OTHER_PREFIX}");
    }
    handle.shutdown().await.expect("shutdown");
}
