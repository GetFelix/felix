use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use crate::raft::{
    AppStateMachine, CLUSTER_ID_HEADER, PEER_PREFIX, PeerSecurity, RaftHandle, RaftSettings,
};

#[cfg(felix_segment_store)]
mod snapshot;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// The prefix the other kind of member serves and calls.
const OTHER_PREFIX: &str = if cfg!(felix_segment_store) {
    "/internal/raft"
} else {
    "/internal/raft/v2"
};

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

/// A member on the segment store and a member on redb must never exchange
/// Raft messages. Each serves every peer route under its own prefix only,
/// so a request from the other kind, credentials and all, is a 404.
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

/// The member's router refuses a request meant for another member before
/// any route sees it, and serves one that names it or names nobody.
#[tokio::test]
async fn a_request_for_another_member_reaches_no_route() {
    let dir = tempfile::tempdir().expect("tempdir");
    let security = PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some(TOKEN.to_string()),
        tls: None,
    };
    let handle = RaftHandle::start(
        RaftSettings::new(1, dir.path().into(), security),
        Arc::new(NullApp),
    )
    .await
    .expect("start");
    let router = handle.rpc_router();
    let ask = async |target: Option<&str>| {
        let mut request = Request::get(format!("{PEER_PREFIX}/standing"))
            .header(CLUSTER_ID_HEADER, "test-cluster")
            .header("authorization", format!("Bearer {TOKEN}"));
        if let Some(target) = target {
            request = request.header(crate::raft::TARGET_HEADER, target);
        }
        let request = request.body(Body::empty()).expect("request");
        router
            .clone()
            .oneshot(request)
            .await
            .expect("response")
            .status()
    };
    assert_eq!(ask(Some("1")).await, StatusCode::OK);
    assert_eq!(ask(None).await, StatusCode::OK);
    assert_eq!(ask(Some("2")).await, StatusCode::SERVICE_UNAVAILABLE);

    // Without peer credentials nobody learns which member this is.
    let request = Request::get(format!("{PEER_PREFIX}/standing"))
        .header(crate::raft::TARGET_HEADER, "2")
        .body(Body::empty())
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    handle.shutdown().await.expect("shutdown");
}
