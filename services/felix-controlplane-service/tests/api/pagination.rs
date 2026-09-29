//! `limit` and `cursor` on the list endpoints, through the router.
use std::sync::Arc;

use axum::body::Body;
use axum::http::StatusCode;
use base64::Engine as _;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::rbac::policy_store::PolicyRule;
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, Stream, StreamKind, Tenant,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use tower::ServiceExt;

use crate::common::{Credentials, read_json, request_as, seed_credentials};

const STREAMS: [&str; 5] = ["s1", "s2", "s3", "s4", "s5"];

struct Harness {
    app: axum::routing::RouterIntoService<Body, ()>,
    store: Arc<InMemoryStore>,
    credentials: Credentials,
}

/// Tenant `t1` with namespace `payments` holding streams `s1`..`s5`.
async fn harness() -> Harness {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(
            felix_controlplane_service::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS,
        ),
    }));
    let credentials = seed_credentials(store.as_ref()).await;
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    credentials.adopt(store.as_ref(), "t1").await;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            display_name: "payments".to_string(),
        })
        .await
        .expect("namespace");
    for name in STREAMS {
        store
            .create_stream(Stream {
                tenant_id: "t1".to_string(),
                namespace: "payments".to_string(),
                stream: name.to_string(),
                kind: StreamKind::Stream,
                shards: 1,
                replication_factor: 1,
                retention: RetentionPolicy {
                    max_age_seconds: None,
                    max_size_bytes: None,
                },
                consistency: ConsistencyLevel::Leader,
                delivery: DeliveryGuarantee::AtLeastOnce,
                durable: false,
                region: None,
                routing: Default::default(),
            })
            .await
            .expect("stream");
    }
    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::clone(&store)
            as Arc<dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync>,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    Harness {
        app: build_router(state).into_service(),
        store,
        credentials,
    }
}

async fn get(h: &Harness, uri: &str, token: &str) -> (StatusCode, serde_json::Value) {
    let response = h
        .app
        .clone()
        .oneshot(request_as("GET", uri, token))
        .await
        .expect("response");
    let status = response.status();
    (status, read_json(response).await)
}

/// Follow `next_cursor` from `uri` to the end, returning each page's names.
async fn pages(h: &Harness, uri: &str, token: &str, name: &str) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut next = uri.to_string();
    loop {
        let (status, body) = get(h, &next, token).await;
        assert_eq!(status, StatusCode::OK, "{next}: {body}");
        pages.push(
            body["items"]
                .as_array()
                .expect("items")
                .iter()
                .map(|item| item[name].as_str().expect("name").to_string())
                .collect(),
        );
        match body.get("next_cursor").and_then(|cursor| cursor.as_str()) {
            Some(cursor) => next = format!("{uri}&cursor={cursor}"),
            None => return pages,
        }
    }
}

#[tokio::test]
async fn a_listing_pages_through_every_entry_once() {
    let h = harness().await;
    let admin = h.credentials.tenant_admin("t1");
    let pages = pages(
        &h,
        "/v1/tenants/t1/namespaces/payments/streams?limit=2",
        &admin,
        "stream",
    )
    .await;
    assert_eq!(
        pages,
        vec![vec!["s1", "s2"], vec!["s3", "s4"], vec!["s5"]],
        "pages in name order, the last short and without a cursor",
    );
}

/// No parameters is the response every existing caller parses: the same
/// object, everything in it, and no cursor.
#[tokio::test]
async fn without_parameters_the_response_is_unchanged() {
    let h = harness().await;
    let admin = h.credentials.tenant_admin("t1");
    let (status, body) = get(&h, "/v1/tenants/t1/namespaces/payments/streams", &admin).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["items"].as_array().expect("items").len(),
        STREAMS.len()
    );
    assert!(body.get("next_cursor").is_none(), "{body}");
}

/// A caller who may see only some streams gets full pages of those, and no
/// cursor ever names one of the others.
#[tokio::test]
async fn a_cursor_never_names_an_entry_the_caller_cannot_see() {
    let h = harness().await;
    let narrow = h.credentials.token(
        "t1",
        &[
            "stream.manage:stream:t1/payments/s2",
            "stream.manage:stream:t1/payments/s5",
        ],
    );
    let uri = "/v1/tenants/t1/namespaces/payments/streams?limit=1";
    let (_, first) = get(&h, uri, &narrow).await;
    let cursor = first["next_cursor"].as_str().expect("a second page");
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .expect("base64");
    assert_eq!(decoded, br#""s2""#, "the cursor is the last entry returned");

    assert_eq!(
        pages(&h, uri, &narrow, "stream").await,
        vec![vec!["s2"], vec!["s5"]]
    );
}

#[tokio::test]
async fn a_bad_limit_or_cursor_is_a_validation_error() {
    let h = harness().await;
    let admin = h.credentials.tenant_admin("t1");
    for uri in [
        "/v1/tenants/t1/namespaces/payments/streams?limit=0",
        "/v1/tenants/t1/namespaces/payments/streams?limit=10001",
        "/v1/tenants/t1/namespaces/payments/streams?limit=many",
        "/v1/tenants/t1/namespaces/payments/streams?cursor=%21%21",
    ] {
        let (status, body) = get(&h, uri, &admin).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["code"], "validation_error", "{uri}");
    }
}

#[tokio::test]
async fn tenants_and_nodes_page_too() {
    let h = harness().await;
    for id in ["t2", "t3"] {
        h.store
            .create_tenant(Tenant {
                tenant_id: id.to_string(),
                display_name: id.to_string(),
            })
            .await
            .expect("tenant");
    }
    let operator = h.credentials.operator();
    let tenants: Vec<String> = pages(&h, "/v1/tenants?limit=1", &operator, "tenant_id")
        .await
        .concat();
    let mut expected: Vec<String> = h
        .store
        .list_tenants()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.tenant_id)
        .collect();
    expected.sort();
    assert_eq!(tenants, expected);

    // No nodes: one empty page, no cursor.
    let (status, body) = get(&h, "/v1/nodes?limit=5", &operator).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"], serde_json::json!([]));
    assert!(body.get("next_cursor").is_none());
}

/// RBAC listings answered with a bare array before paging existed; that stays
/// the answer when no paging is asked for, and a page is an object.
#[tokio::test]
async fn rbac_listings_keep_their_array_unless_paged() {
    let h = harness().await;
    for action in ["stream.publish", "stream.subscribe", "stream.manage"] {
        h.store
            .add_rbac_policy(
                "t1",
                PolicyRule {
                    subject: "role:reader".to_string(),
                    object: "stream:t1/payments/*".to_string(),
                    action: action.to_string(),
                },
            )
            .await
            .expect("policy");
    }
    let viewer = h.credentials.token("t1", &["rbac.view:tenant:t1"]);

    let (status, body) = get(&h, "/v1/tenants/t1/rbac/policies", &viewer).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().expect("a bare array").len(), 3);

    let (status, first) = get(&h, "/v1/tenants/t1/rbac/policies?limit=2", &viewer).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"].as_array().expect("a page").len(), 2);
    let cursor = first["next_cursor"].as_str().expect("more");
    let (_, second) = get(
        &h,
        &format!("/v1/tenants/t1/rbac/policies?limit=2&cursor={cursor}"),
        &viewer,
    )
    .await;
    assert_eq!(second["items"].as_array().expect("a page").len(), 1);
    assert!(second.get("next_cursor").is_none());

    let (status, groupings) = get(&h, "/v1/tenants/t1/rbac/groupings?limit=10", &viewer).await;
    assert_eq!(status, StatusCode::OK);
    assert!(groupings["items"].is_array(), "{groupings}");
}
