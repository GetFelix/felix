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
        previous_token: None,
        next_token: None,
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

const OLD: &str = "0123456789abcdef0123456789abcdef";
const NEW: &str = "fedcba9876543210fedcba9876543210";

fn rotating(token: &str, previous: Option<&str>, next: Option<&str>) -> PeerSecurity {
    PeerSecurity {
        previous_token: previous.map(str::to_string),
        next_token: next.map(str::to_string),
        ..security(Some(token))
    }
}

/// The bearer a member sends: always and only the current token.
fn sent_bearer(security: &PeerSecurity) -> Option<String> {
    security
        .peer_headers()
        .expect("headers")
        .get("authorization")
        .map(|value| value.to_str().expect("text").to_string())
}

#[tokio::test]
async fn a_member_mid_rotation_accepts_both_tokens() {
    // Step 1 of a rotation: still sending the old token, also accepting the new one.
    let adding = guarded(rotating(OLD, None, Some(NEW)));
    // Step 2: sending the new token, still accepting the old one.
    let switched = guarded(rotating(NEW, Some(OLD), None));
    for router in [adding, switched] {
        for bearer in [OLD, NEW] {
            assert_eq!(
                call(router.clone(), Some("cluster-a"), Some(bearer)).await,
                StatusCode::OK
            );
        }
        assert_eq!(
            call(router, Some("cluster-a"), Some(&"x".repeat(32))).await,
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn rotation_tokens_are_accepted_but_never_sent() {
    let member = rotating(NEW, Some(OLD), None);
    assert_eq!(sent_bearer(&member), Some(format!("Bearer {NEW}")));
    // A member that switched to the new token reaches one still on step 1.
    let step_one = guarded(rotating(OLD, None, Some(NEW)));
    assert_eq!(
        call(step_one, Some("cluster-a"), Some(NEW)).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_member_that_only_knows_the_old_token_refuses_the_new_one() {
    let router = guarded(security(Some(OLD)));
    assert_eq!(
        call(router, Some("cluster-a"), Some(NEW)).await,
        StatusCode::UNAUTHORIZED
    );
}

#[test]
fn match_token_names_the_slot_that_matched() {
    let member = rotating(NEW, Some(OLD), None);
    assert_eq!(member.match_token(NEW), Some("current"));
    assert_eq!(member.match_token(OLD), Some("previous"));
    assert_eq!(
        rotating(OLD, None, Some(NEW)).match_token(NEW),
        Some("next")
    );
    assert_eq!(member.match_token(""), None);
}

#[test]
fn an_empty_slot_never_matches_an_empty_bearer() {
    let member = PeerSecurity {
        previous_token: Some(String::new()),
        next_token: Some(String::new()),
        ..security(Some(""))
    };
    assert_eq!(member.match_token(""), None);
}

#[test]
fn accepted_requests_are_counted_by_the_token_they_carried() {
    let recorder = crate::test_support::CountingRecorder::default();
    let router = guarded(rotating(NEW, Some(OLD), None));
    recorder.run(async {
        for bearer in [NEW, OLD, OLD] {
            assert_eq!(
                call(router.clone(), Some("cluster-a"), Some(bearer)).await,
                StatusCode::OK
            );
        }
    });
    let count = |slot: &str| {
        recorder.count(&format!(
            "felix_meta_raft_peer_token_accepted_total{{token={slot}}}"
        ))
    };
    assert_eq!(count("current"), 1);
    assert_eq!(count("previous"), 2);
    assert_eq!(count("next"), 0);
}

mod rotation;
