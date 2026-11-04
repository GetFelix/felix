use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::store::ControlPlaneStore;
use crate::store::contract::nodes::{node, with_features};
use crate::test_support::{app_state_ready, one_shard_cluster, token};

async fn finalize(
    store: std::sync::Arc<crate::store::memory::InMemoryStore>,
    keys: &crate::auth::felix_token::TenantSigningKeys,
    feature: &str,
    dry_run: bool,
) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "/v1/fleet/features/{feature}/finalize?dry_run={dry_run}"
        ))
        .header(
            "authorization",
            format!("Bearer {}", token(keys, &["node.manage:cluster:*"])),
        )
        .body(Body::empty())
        .expect("request");
    crate::api::build_router(app_state_ready(store))
        .oneshot(request)
        .await
        .expect("response")
        .status()
}

/// Every serving broker reports the markers, so only the name keeps an
/// operator from enabling them. Enabled, `admit` would refuse every broker
/// that predates them.
#[tokio::test]
async fn the_reported_markers_cannot_be_finalized_by_name() {
    let (store, keys) = one_shard_cluster().await;
    for (i, id) in ["broker-x", "broker-y"].into_iter().enumerate() {
        store
            .register_node(with_features(
                node(id, 7600 + i as u16),
                &["peer_election", "start_holders", "non_voting"],
            ))
            .await
            .expect("node");
    }
    for marker in ["start_holders", "peer_election", "non_voting"] {
        for dry_run in [true, false] {
            assert_eq!(
                finalize(store.clone(), &keys, marker, dry_run).await,
                StatusCode::CONFLICT,
                "{marker} dry_run={dry_run}"
            );
        }
        assert!(
            !store
                .enabled_fleet_features()
                .await
                .expect("enabled")
                .contains(marker),
            "{marker} was enabled"
        );
    }
}
