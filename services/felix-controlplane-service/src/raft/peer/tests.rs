use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use tower::ServiceExt;

use super::*;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn security(token: Option<&str>) -> PeerSecurity {
    PeerSecurity {
        cluster_id: "cluster-a".to_string(),
        token: token.map(str::to_string),
        tls: None,
    }
}

fn guarded(security: PeerSecurity) -> Router {
    Router::new()
        .route(
            &crate::raft::peer_route("propose"),
            post(|| async { "applied" }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(security),
            require_peer,
        ))
}

async fn call(router: Router, cluster: Option<&str>, bearer: Option<&str>) -> StatusCode {
    let mut request = Request::post(crate::raft::peer_route("propose"));
    if let Some(cluster) = cluster {
        request = request.header(CLUSTER_ID_HEADER, cluster);
    }
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    router
        .oneshot(request.body(Body::empty()).expect("request"))
        .await
        .expect("response")
        .status()
}

#[tokio::test]
async fn a_request_without_peer_credentials_never_reaches_the_route() {
    let router = guarded(security(Some(TOKEN)));
    assert_eq!(
        call(router.clone(), None, None).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(router.clone(), Some("cluster-a"), None).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            Some("cluster-a"),
            Some("0123456789abcdef0123456789abcdeX")
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(router, Some("cluster-a"), Some(TOKEN)).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_peer_of_another_cluster_is_refused_even_with_the_right_token() {
    let router = guarded(security(Some(TOKEN)));
    assert_eq!(
        call(router, Some("cluster-b"), Some(TOKEN)).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn the_insecure_opt_out_still_checks_the_cluster_id() {
    let router = guarded(security(None));
    assert_eq!(
        call(router.clone(), Some("cluster-a"), None).await,
        StatusCode::OK
    );
    assert_eq!(
        call(router, Some("cluster-b"), None).await,
        StatusCode::FORBIDDEN
    );
}

async fn call_as(target: Option<&str>) -> StatusCode {
    let router = Router::new()
        .route(&crate::raft::peer_route("vote"), post(|| async { "voted" }))
        .layer(axum::middleware::from_fn_with_state(7, require_target));
    let mut request = Request::post(crate::raft::peer_route("vote"));
    if let Some(target) = target {
        request = request.header(TARGET_HEADER, target);
    }
    router
        .oneshot(request.body(Body::empty()).expect("request"))
        .await
        .expect("response")
        .status()
}

/// A member answers a request that names it, or names nobody as a sender
/// on a build without the header does. One that names another member, or
/// nothing that reads as an id, gets what an absent member would give.
#[tokio::test]
async fn a_request_meant_for_another_member_is_refused() {
    assert_eq!(call_as(Some("7")).await, StatusCode::OK);
    assert_eq!(call_as(None).await, StatusCode::OK);
    for other in ["8", "4294967303", "seven", ""] {
        assert_eq!(
            call_as(Some(other)).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "{other:?}"
        );
    }
}

#[test]
fn the_token_stays_out_of_debug_output() {
    let rendered = format!("{:?}", security(Some(TOKEN)));
    assert!(!rendered.contains(TOKEN), "{rendered}");
    assert!(rendered.contains("cluster-a"), "{rendered}");
}
