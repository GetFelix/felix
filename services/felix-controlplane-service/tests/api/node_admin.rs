//! Operator-facing node listing and detail.
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
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardState, Stream, StreamKind, Tenant,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig,
};
use tower::ServiceExt;

use crate::common::read_json;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 5_000,
    expiry_timeout_ms: 15_000,
    regrant_margin_ms: None,
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
        .expect("set keys");

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
        "p:ops",
        perms.into_iter().map(str::to_string).collect(),
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

fn get(path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(path);
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", format!("Bearer {bearer}"));
    }
    builder.body(Body::empty()).expect("request")
}

fn node(node_id: &str, port: u16, region: &str, rack: &str) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.4:{port}"),
            client_addr: None,
            kafka_addr: None,
            region: region.to_string(),
            zone: None,
            labels: BTreeMap::from([("rack".to_string(), rack.to_string())]),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: felix_controlplane_service::clock::now_millis(),
            registered_at_millis: 1,
            incarnation: 0,
            features: Default::default(),
        },
    }
}

#[tokio::test]
async fn listing_requires_a_token() {
    let (app, _store, _keys) = setup().await;
    let response = app.oneshot(get("/v1/nodes", None)).await.expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// A tenant admin's own scope must not open the cluster listing. This is the
/// HTTP half of the property `authorize.rs` proves about the object grammar.
#[tokio::test]
async fn tenant_scoped_permissions_do_not_grant_the_cluster() {
    let (app, _store, keys) = setup().await;
    for perm in [
        "tenant.manage:tenant:t1",
        "ns.manage:namespace:t1/*",
        "rbac.policy.manage:tenant:t1",
    ] {
        let response = app
            .clone()
            .oneshot(get("/v1/nodes", Some(&token(&keys, vec![perm]))))
            .await
            .expect("request");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{perm} must not grant node.view",
        );
    }
}

#[tokio::test]
async fn an_invalid_token_is_rejected() {
    let (app, _store, _keys) = setup().await;
    let response = app
        .oneshot(get("/v1/nodes", Some("not.a.token")))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// A token signed by a different tenant's keys must not pass, even carrying the
/// right permission string.
#[tokio::test]
async fn a_token_signed_by_another_key_is_rejected() {
    let (app, _store, _keys) = setup().await;
    let foreign = generate_signing_keys().expect("keys");
    let bearer = mint_token_for(
        &foreign,
        "t1",
        "p:attacker",
        vec!["node.view:cluster:*".to_string()],
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token");

    let response = app
        .oneshot(get("/v1/nodes", Some(&bearer)))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cluster_node_view_lists_every_broker() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7001, "us-west-2", "a1"))
        .await
        .expect("register");
    store
        .register_node(node("broker-b", 7002, "eu-central-1", "b2"))
        .await
        .expect("register");

    let bearer = token(&keys, vec!["node.view:cluster:*"]);
    let response = app
        .oneshot(get("/v1/nodes", Some(&bearer)))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = read_json(response).await;
    let items = body["items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["node"]["node_id"], "broker-a");
    assert_eq!(items[0]["placement"]["eligible"], true);
    assert_eq!(items[0]["placement"]["routable"], true);
    // The advertised address is what an operator needs to check reachability.
    assert_eq!(items[0]["node"]["spec"]["advertise_addr"], "10.0.0.4:7001");
}

#[tokio::test]
async fn filters_intersect() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7001, "us-west-2", "a1"))
        .await
        .expect("register");
    store
        .register_node(node("broker-b", 7002, "us-west-2", "b2"))
        .await
        .expect("register");
    store
        .register_node(node("broker-c", 7003, "eu-central-1", "a1"))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-b", NodeLifecycle::Draining)
        .await
        .expect("drain");
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    for (query, expected) in [
        ("?region=us-west-2", vec!["broker-a", "broker-b"]),
        ("?lifecycle=live", vec!["broker-a", "broker-c"]),
        ("?lifecycle=draining", vec!["broker-b"]),
        ("?label=rack%3Da1", vec!["broker-a", "broker-c"]),
        ("?region=us-west-2&label=rack%3Da1", vec!["broker-a"]),
        ("?region=us-west-2&lifecycle=live", vec!["broker-a"]),
        ("?region=nowhere", vec![]),
    ] {
        let response = app
            .clone()
            .oneshot(get(&format!("/v1/nodes{query}"), Some(&bearer)))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let body: serde_json::Value = read_json(response).await;
        let ids: Vec<String> = body["items"]
            .as_array()
            .expect("items")
            .iter()
            .map(|item| {
                item["node"]["node_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        assert_eq!(ids, expected, "{query}");
    }
}

#[tokio::test]
async fn a_missing_node_is_not_found() {
    let (app, _store, keys) = setup().await;
    let bearer = token(&keys, vec!["node.view:cluster:*"]);
    let response = app
        .oneshot(get("/v1/nodes/absent", Some(&bearer)))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// The acceptance criterion: the output has to say *why* a node is not a
/// placement candidate, not just that it is not one.
#[tokio::test]
async fn placement_explains_each_exclusion() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-down", 7001, "us-west-2", "a1"))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-down", NodeLifecycle::Down)
        .await
        .expect("down");
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .clone()
        .oneshot(get("/v1/nodes/broker-down", Some(&bearer)))
        .await
        .expect("request");
    let body: serde_json::Value = read_json(response).await;
    assert_eq!(body["placement"]["eligible"], false);
    assert_eq!(body["placement"]["routable"], false);
    let reasons = body["placement"]["reasons"].as_array().expect("reasons");
    assert!(
        reasons
            .iter()
            .any(|r| r.as_str().unwrap_or_default().contains("heartbeat window")),
        "{reasons:?}",
    );
}

/// A draining broker takes no new placement but still serves the shards it
/// has not handed off, so other brokers may still send it their requests.
#[tokio::test]
async fn a_draining_node_is_not_eligible_but_is_routable() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7001, "us-west-2", "a1"))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .oneshot(get("/v1/nodes/broker-a", Some(&bearer)))
        .await
        .expect("request");
    let body: serde_json::Value = read_json(response).await;
    assert_eq!(body["placement"]["eligible"], false);
    assert_eq!(body["placement"]["routable"], true);
}

/// The window between a heartbeat lapsing and the sweep noticing: the node
/// still reads `live`, and that gap is exactly what an operator is chasing.
#[tokio::test]
async fn a_stale_heartbeat_is_reported_before_expiry_runs() {
    let (app, store, keys) = setup().await;
    let mut stale = node("broker-stale", 7001, "us-west-2", "a1");
    stale.status.last_heartbeat_at_millis =
        felix_controlplane_service::clock::now_millis() - LIVENESS.expiry_timeout_ms * 3;
    store.register_node(stale).await.expect("register");

    let bearer = token(&keys, vec!["node.view:cluster:*"]);
    let response = app
        .oneshot(get("/v1/nodes/broker-stale", Some(&bearer)))
        .await
        .expect("request");
    let body: serde_json::Value = read_json(response).await;

    assert_eq!(
        body["node"]["status"]["lifecycle"], "live",
        "expiry has not run"
    );
    assert_eq!(body["placement"]["eligible"], false);
    assert_eq!(body["placement"]["routable"], false, "its heartbeat lapsed");
    let reasons = body["placement"]["reasons"].as_array().expect("reasons");
    assert!(
        reasons
            .iter()
            .any(|r| r.as_str().unwrap_or_default().contains("past the")),
        "a stale heartbeat should be called out even while the node reads live: {reasons:?}",
    );
    assert!(
        body["placement"]["heartbeat_age_ms"].as_u64().unwrap_or(0) > LIVENESS.expiry_timeout_ms
    );
}

/// Nothing in the operator view should carry key material or tokens.
#[tokio::test]
async fn the_node_view_exposes_no_secrets() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7001, "us-west-2", "a1"))
        .await
        .expect("register");
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .oneshot(get("/v1/nodes", Some(&bearer)))
        .await
        .expect("request");
    let body: serde_json::Value = read_json(response).await;
    let rendered = body.to_string().to_lowercase();
    for forbidden in ["secret", "private", "token", "signing", "password"] {
        assert!(
            !rendered.contains(forbidden),
            "{forbidden} leaked: {rendered}"
        );
    }
}

async fn seed_shards(store: &InMemoryStore) {
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "NS".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 3,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: Default::default(),
        })
        .await
        .expect("stream");

    for (i, id) in ["broker-a", "broker-b"].iter().enumerate() {
        store
            .register_node(node(id, 7100 + i as u16, "us-west-2", "a1"))
            .await
            .expect("node");
    }
    for (shard, leader) in [(0u32, "broker-a"), (1, "broker-b"), (2, "broker-a")] {
        store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "orders".to_string(),
                    shard,
                    kind: felix_controlplane_service::model::ShardKind::Stream,
                },
                leader: leader.to_string(),
                replicas: Vec::new(),
                generation: 0,
                state: ShardState::Active,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
            })
            .await
            .expect("assign");
    }
}

#[tokio::test]
async fn listing_shard_assignments_requires_cluster_scope() {
    let (app, _store, _keys) = setup().await;
    let response = app
        .oneshot(get("/v1/shard-assignments", None))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Ownership and membership are the same view of the cluster, so a tenant
/// scope must not open either.
#[tokio::test]
async fn a_tenant_scope_does_not_open_shard_assignments() {
    let (app, _store, keys) = setup().await;
    let response = app
        .oneshot(get(
            "/v1/shard-assignments",
            Some(&token(&keys, vec!["tenant.manage:tenant:t1"])),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn shard_assignments_list_and_filter_by_leader() {
    let (app, store, keys) = setup().await;
    seed_shards(&store).await;
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    let response = app
        .clone()
        .oneshot(get("/v1/shard-assignments", Some(&bearer)))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = read_json(response).await;
    let items = body["items"].as_array().expect("items");
    assert_eq!(items.len(), 3);
    // Ordered by stream then shard, so a listing reads the same way twice.
    assert_eq!(items[0]["shard"], 0);
    assert_eq!(items[0]["leader"], "broker-a");
    assert_eq!(items[0]["state"], "active");

    // The question an operator asks when a broker misbehaves.
    let response = app
        .oneshot(get("/v1/shard-assignments?leader=broker-a", Some(&bearer)))
        .await
        .expect("request");
    let body: serde_json::Value = read_json(response).await;
    let shards: Vec<u64> = body["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["shard"].as_u64().unwrap_or_default())
        .collect();
    assert_eq!(shards, vec![0, 2]);
}

/// Every assignment feed a broker reads carries its stream's routing, and a
/// modulo stream's assignments omit it so an older broker reads the same bytes.
#[tokio::test]
async fn shard_assignments_carry_the_streams_routing() {
    let (app, store, keys) = setup().await;
    seed_shards(&store).await;
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "clicks".to_string(),
            kind: StreamKind::Stream,
            shards: 2,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: felix_controlplane_service::model::StreamRouting::JumpHash,
        })
        .await
        .expect("stream");
    for shard in 0..2 {
        store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "clicks".to_string(),
                    shard,
                    kind: felix_controlplane_service::model::ShardKind::Stream,
                },
                leader: "broker-b".to_string(),
                replicas: Vec::new(),
                generation: 0,
                state: ShardState::Active,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
            })
            .await
            .expect("assign");
    }
    let bearer = token(&keys, vec!["node.view:cluster:*"]);

    for (path, items) in [
        ("/v1/shard-assignments", "/items"),
        ("/v1/shard-assignments/snapshot", "/items"),
        ("/v1/shard-assignments/changes?since=0", "/items"),
    ] {
        let response = app
            .clone()
            .oneshot(get(path, Some(&bearer)))
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let body: serde_json::Value = read_json(response).await;
        let items = body
            .pointer(items)
            .and_then(|i| i.as_array())
            .expect("items");
        let mut seen = 0;
        for item in items {
            // A change nests the assignment; a listing is the assignment.
            let assignment = item.get("assignment").unwrap_or(item);
            match assignment["stream"].as_str() {
                Some("clicks") => {
                    assert_eq!(assignment["routing"], "jump_hash", "{path}");
                    seen += 1;
                }
                Some("orders") => {
                    assert!(assignment.get("routing").is_none(), "{path}: {assignment}");
                    seen += 1;
                }
                other => panic!("{path}: unexpected stream {other:?}"),
            }
        }
        assert_eq!(seen, 5, "{path}");
    }
}

fn send(method: &str, path: &str, bearer: &str, body: Option<serde_json::Value>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {bearer}"))
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .expect("request")
}

async fn lifecycle(store: &InMemoryStore, node_id: &str) -> NodeLifecycle {
    store.get_node(node_id).await.expect("get").status.lifecycle
}

/// A drain is cancelled by patching the node back to `live`.
#[tokio::test]
async fn an_operator_can_cancel_a_drain() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7100, "us-west-2", "a1"))
        .await
        .expect("node");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .oneshot(send(
            "PATCH",
            "/v1/nodes/broker-a",
            &bearer,
            Some(serde_json::json!({"lifecycle": "live"})),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(lifecycle(&store, "broker-a").await, NodeLifecycle::Live);
}

/// Only registration revives a node: a patch cannot claim a silent broker is
/// alive.
#[tokio::test]
async fn a_patch_cannot_revive_a_down_node() {
    let (app, store, keys) = setup().await;
    store
        .register_node(node("broker-a", 7100, "us-west-2", "a1"))
        .await
        .expect("node");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("down");
    let bearer = token(&keys, vec!["node.manage:cluster:*"]);

    let response = app
        .oneshot(send(
            "PATCH",
            "/v1/nodes/broker-a",
            &bearer,
            Some(serde_json::json!({"lifecycle": "live"})),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(lifecycle(&store, "broker-a").await, NodeLifecycle::Down);
}

#[tokio::test]
async fn a_broker_cannot_patch_another_broker() {
    let (app, store, keys) = setup().await;
    for (i, id) in ["broker-a", "broker-b"].iter().enumerate() {
        store
            .register_node(node(id, 7100 + i as u16, "us-west-2", "a1"))
            .await
            .expect("node");
    }
    let bearer = token(&keys, vec!["node.manage:node:broker-a"]);

    let response = app
        .oneshot(send(
            "PATCH",
            "/v1/nodes/broker-b",
            &bearer,
            Some(serde_json::json!({"lifecycle": "draining"})),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(lifecycle(&store, "broker-b").await, NodeLifecycle::Live);
}

/// Removal is an operator's act on a stopped broker that no shard names. Each
/// refusal is checked before the one that finally succeeds.
#[tokio::test]
async fn a_node_is_removed_only_once_stopped_and_unnamed() {
    let (app, store, keys) = setup().await;
    seed_shards(&store).await;
    let operator = token(&keys, vec!["node.manage:cluster:*"]);
    let delete = |bearer: &str| send("DELETE", "/v1/nodes/broker-b", bearer, None);

    // Its own node-scoped credential is not enough.
    let own = token(&keys, vec!["node.manage:node:broker-b"]);
    let response = app.clone().oneshot(delete(&own)).await.expect("request");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Still running.
    let response = app
        .clone()
        .oneshot(delete(&operator))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Stopped, but still leads shard 1.
    store
        .set_node_lifecycle("broker-b", NodeLifecycle::Left)
        .await
        .expect("left");
    let response = app
        .clone()
        .oneshot(delete(&operator))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Leadership moved, but it is still a follower there. A follower slot
    // naming a deleted node would never be replaced.
    let mut moved = store
        .list_shard_assignments_for_node("broker-b")
        .await
        .expect("list")
        .pop()
        .expect("shard 1");
    moved.leader = "broker-a".to_string();
    moved.replicas = vec!["broker-b".to_string()];
    store
        .put_shard_assignment(moved.clone())
        .await
        .expect("move");
    let response = app
        .clone()
        .oneshot(delete(&operator))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    moved.replicas.clear();
    store.put_shard_assignment(moved).await.expect("reseat");
    let response = app
        .clone()
        .oneshot(delete(&operator))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .oneshot(get(
            "/v1/nodes/broker-b",
            Some(&token(&keys, vec!["node.view:cluster:*"])),
        ))
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
