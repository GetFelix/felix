//! Authorization on the membership write path.
//!
//! Registration, heartbeat, drain, and deregistration were reachable by any
//! caller. These are the tests that say they are not, one per endpoint and one
//! per way of getting it wrong.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::{TenantSigningKeys, mint_token_for};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig,
};
use tower::ServiceExt;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 5_000,
    expiry_timeout_ms: 15_000,
    sweep_interval_ms: 2_000,
    shard_reconcile_interval_ms: 5_000,
};

type App = axum::routing::RouterIntoService<Body, ()>;

async fn setup() -> (App, Arc<InMemoryStore>, TenantSigningKeys) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    }));
    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::clone(&store) as Arc<dyn ControlPlaneAuthStore + Send + Sync>,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: LIVENESS,
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    (build_router(state).into_service(), store, keys)
}

fn token(keys: &TenantSigningKeys, perms: Vec<&str>) -> String {
    mint_token_for(
        keys,
        "t1",
        "p:broker",
        perms.into_iter().map(str::to_string).collect(),
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

fn post(path: &str, bearer: Option<&str>, body: serde_json::Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", format!("Bearer {bearer}"));
    }
    builder.body(Body::from(body.to_string())).expect("request")
}

fn registration(node_id: &str, port: u16) -> serde_json::Value {
    serde_json::json!({
        "node_id": node_id,
        "advertise_addr": format!("10.0.0.4:{port}"),
        "region": "us-west-2",
    })
}

async fn seed_node(store: &InMemoryStore, node_id: &str, port: u16) {
    store
        .register_node(Node {
            node_id: node_id.to_string(),
            spec: NodeSpec {
                advertise_addr: format!("10.0.0.4:{port}"),
                client_addr: None,
                kafka_addr: None,
                region: "us-west-2".to_string(),
                labels: BTreeMap::new(),
                capacity: NodeCapacity::default(),
            },
            status: NodeStatus {
                lifecycle: NodeLifecycle::Live,
                last_heartbeat_at_millis: 1,
                registered_at_millis: 1,
                incarnation: 0,
            },
        })
        .await
        .expect("seed");
}

/// Every write endpoint, with no credential at all. This is the hole being
/// closed, and there is one case per endpoint so a regression names itself.
#[tokio::test]
async fn every_membership_write_requires_a_token() {
    let (app, store, _keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;

    for (path, body) in [
        ("/v1/nodes", registration("broker-a", 7001)),
        (
            "/v1/nodes/broker-a/heartbeat",
            serde_json::json!({"incarnation": 0}),
        ),
        ("/v1/nodes/broker-a/drain", serde_json::json!({})),
        ("/v1/nodes/broker-a/deregister", serde_json::json!({})),
    ] {
        let response = app
            .clone()
            .oneshot(post(path, None, body))
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{path} must not be reachable without a token",
        );
    }
}

/// The property the node-scoped object exists for: broker-a's own credential
/// cannot be used to speak for broker-b.
#[tokio::test]
async fn a_broker_cannot_act_for_another_broker() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    seed_node(&store, "broker-b", 7002).await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    for (path, body) in [
        ("/v1/nodes", registration("broker-b", 7002)),
        (
            "/v1/nodes/broker-b/heartbeat",
            serde_json::json!({"incarnation": 0}),
        ),
        ("/v1/nodes/broker-b/drain", serde_json::json!({})),
        ("/v1/nodes/broker-b/deregister", serde_json::json!({})),
    ] {
        let response = app
            .clone()
            .oneshot(post(path, Some(&bearer), body))
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{path} must reject a credential scoped to a different node",
        );
    }

    // And broker-b is untouched by the attempt.
    assert_eq!(
        store
            .get_node("broker-b")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );
}

#[tokio::test]
async fn a_broker_can_manage_itself() {
    let (app, store, keys) = setup().await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    let registered = app
        .clone()
        .oneshot(post(
            "/v1/nodes",
            Some(&bearer),
            registration("broker-a", 7001),
        ))
        .await
        .expect("request");
    assert_eq!(registered.status(), StatusCode::OK);

    for path in [
        "/v1/nodes/broker-a/heartbeat",
        "/v1/nodes/broker-a/drain",
        "/v1/nodes/broker-a/deregister",
    ] {
        let body = if path.ends_with("heartbeat") {
            serde_json::json!({"incarnation": 0})
        } else {
            serde_json::json!({})
        };
        let response = app
            .clone()
            .oneshot(post(path, Some(&bearer), body))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Left,
    );
}

/// An operator managing the fleet holds cluster scope, which covers every node.
#[tokio::test]
async fn cluster_scope_can_manage_any_node() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-b", 7002).await;
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-b/drain",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store
            .get_node("broker-b")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Draining,
    );
}

/// Reading the fleet is not permission to change it.
#[tokio::test]
async fn node_view_does_not_grant_node_manage() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .oneshot(post(
            "/v1/nodes/broker-a/deregister",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

/// A tenant admin's own scope must not open the membership write path.
#[tokio::test]
async fn tenant_scoped_permissions_do_not_grant_node_manage() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;

    for perm in [
        "tenant.manage:tenant:t1",
        "ns.manage:namespace:t1/*",
        "rbac.policy.manage:tenant:t1",
    ] {
        let response = app
            .clone()
            .oneshot(post(
                "/v1/nodes/broker-a/drain",
                Some(&token(&keys, vec![perm])),
                serde_json::json!({}),
            ))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{perm}");
    }
}

/// A correctly shaped token signed by the wrong key is still no token.
#[tokio::test]
async fn a_token_signed_by_another_key_is_rejected() {
    let (app, store, _keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    let foreign = generate_signing_keys().expect("keys");
    let bearer = mint_token_for(
        &foreign,
        "t1",
        "p:attacker",
        vec!["node.manage:cluster:*".to_string()],
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token");

    let response = app
        .oneshot(post(
            "/v1/nodes/broker-a/drain",
            Some(&bearer),
            serde_json::json!({}),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Registration takes its identity from the body, so that is what must be
/// authorised -- otherwise a broker could claim any name it liked.
#[tokio::test]
async fn registration_authorises_the_identity_in_the_body() {
    let (app, _store, keys) = setup().await;
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes",
            Some(&bearer),
            registration("broker-impostor", 7009),
        ))
        .await
        .expect("request");
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "a broker must not register under another identity",
    );
}

/// Seed a tenant, namespace, stream and a shard assignment naming `leader`.
async fn seed_shard(store: &InMemoryStore, leader: &str, generation: u64) {
    use felix_controlplane_service::model::{
        ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, ShardAssignment, ShardKey,
        ShardKind, ShardState, Stream, StreamKind, Tenant,
    };

    let _ = store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T1".to_string(),
        })
        .await;
    let _ = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "NS".to_string(),
        })
        .await;
    let _ = store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
        })
        .await;
    for _ in 0..=generation {
        store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "orders".to_string(),
                    shard: 0,
                    kind: ShardKind::Stream,
                },
                leader: leader.to_string(),
                replicas: Vec::new(),
                generation: 0,
                state: ShardState::Assigning,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
            })
            .await
            .expect("assign");
    }
}

fn replica_report(generation: u64, caught_up: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "incarnation": 0,
        "shards": [{
            "tenant_id": "t1",
            "namespace": "ns",
            "stream": "orders",
            "shard": 0,
            "generation": generation,
            "caught_up": caught_up,
            "replica_offsets": [],
        }],
    })
}

/// A broker may only report positions for shards it leads.
///
/// Speaking for yourself about someone else's shard is still nominating
/// yourself for promotion, so the identity check on its own is not enough.
#[tokio::test]
async fn a_broker_cannot_report_positions_for_a_shard_it_does_not_lead() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    seed_node(&store, "broker-b", 7002).await;
    seed_shard(&store, "broker-a", 0).await;

    // broker-b is properly authenticated as itself, and leads nothing.
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);
    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-b/replica-status",
            Some(&bearer),
            replica_report(0, &["broker-b"]),
        ))
        .await
        .expect("report");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let key = felix_controlplane_service::model::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: felix_controlplane_service::model::ShardKind::Stream,
    };
    assert!(
        !reported_caught_up(store.as_ref(), &key, "broker-b").await,
        "a broker that leads nothing had its report recorded, so it can \
         nominate itself for promotion",
    );
}

/// A generation past the assignment's is refused, rather than wedging the shard.
///
/// Reports older than the newest held are dropped, so accepting a claim of
/// `u64::MAX` would block every genuine report for that shard from then on.
#[tokio::test]
async fn a_generation_ahead_of_the_assignment_is_refused() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    seed_shard(&store, "broker-a", 0).await;
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-a/replica-status",
            Some(&bearer),
            replica_report(u64::MAX, &["broker-zzz"]),
        ))
        .await
        .expect("report");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // The genuine report that follows must still land.
    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-a/replica-status",
            Some(&bearer),
            replica_report(0, &["broker-a"]),
        ))
        .await
        .expect("report");
    assert_eq!(response.status(), StatusCode::OK);

    let key = felix_controlplane_service::model::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: felix_controlplane_service::model::ShardKind::Stream,
    };
    assert!(
        reported_caught_up(store.as_ref(), &key, "broker-a").await,
        "the genuine report was dropped as older than the bogus one, so a \
         single claim of u64::MAX wedges the shard's positions for good",
    );
}

/// The leader's own report is recorded, so the checks above are refusing the
/// right thing rather than everything.
#[tokio::test]
async fn the_leader_of_a_shard_can_report_it() {
    let (app, store, keys) = setup().await;
    seed_node(&store, "broker-a", 7001).await;
    seed_shard(&store, "broker-a", 0).await;
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .clone()
        .oneshot(post(
            "/v1/nodes/broker-a/replica-status",
            Some(&bearer),
            replica_report(0, &["broker-a"]),
        ))
        .await
        .expect("report");
    assert_eq!(response.status(), StatusCode::OK);

    let key = felix_controlplane_service::model::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: felix_controlplane_service::model::ShardKind::Stream,
    };
    assert!(reported_caught_up(store.as_ref(), &key, "broker-a").await);
}

/// What promotion would conclude: is this node a candidate for this shard?
///
/// Read back from the store, as placement does -- the report never lives
/// anywhere else.
async fn reported_caught_up(
    store: &InMemoryStore,
    key: &felix_controlplane_service::model::ShardKey,
    node_id: &str,
) -> bool {
    use felix_controlplane_service::cluster::placement::CaughtUp;
    felix_controlplane_service::cluster::placement::ReplicaPositions::load(store, &LIVENESS)
        .await
        .expect("load reports")
        .is_caught_up(key, node_id)
}
