//! The seal routes: who may order, report and give up a seal, and what each
//! answers.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::model::{ShardAssignment, ShardState};
use crate::store::ControlPlaneStore;
use crate::store::memory::InMemoryStore;
use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

const COPY_X: &str = "000102030405060708090a0b0c0d0e0f";
const COPY_Y: &str = "101112131415161718191a1b1c1d1e1f";
const SEAL: &str = "/v1/placement/seal/t1/ns/orders/0";

/// `t1/ns/orders` shard 0, active on broker-x with broker-y as its replica.
async fn active(store: &InMemoryStore) -> ShardAssignment {
    let mut assignment = ShardAssignment {
        key: shard_zero(),
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
    };
    store
        .put_shard_assignment(assignment.clone())
        .await
        .expect("assign");
    assignment.state = ShardState::Active;
    store
        .put_shard_assignment(assignment)
        .await
        .expect("activate")
}

/// Every broker reports the Raft engine and the fleet finalizes it.
async fn finalize_shard_raft(store: &InMemoryStore) {
    let feature = felix_common::fleet::SHARD_RAFT.name();
    for mut node in store.list_nodes().await.expect("nodes") {
        node.status.features.insert(feature.to_string());
        store.register_node(node).await.expect("register");
    }
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");
}

async fn post(
    store: &Arc<InMemoryStore>,
    keys: &crate::auth::felix_token::TenantSigningKeys,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(
            "authorization",
            format!("Bearer {}", token(keys, &["node.manage:cluster:*"])),
        )
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = crate::api::build_router(app_state_ready(Arc::clone(store) as _))
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, serde_json::from_slice(&body).expect("json"))
}

fn report(generation: u64) -> serde_json::Value {
    let side = serde_json::json!({ "start": 0, "end": 2, "digest": "cd".repeat(32) });
    serde_json::json!({
        "key": shard_zero(),
        "report": {
            "generation": generation,
            "offset": 9,
            "start": 1,
            "digest": "ab".repeat(32),
            "dead_letters": side,
            "cursors": side,
            "voters": { "broker-x": COPY_X, "broker-y": COPY_Y },
        },
    })
}

async fn read(store: &InMemoryStore) -> ShardAssignment {
    store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("assignment")
}

/// Ordered through the route, so the test also covers what it answers.
async fn sealed(
    store: &Arc<InMemoryStore>,
    keys: &crate::auth::felix_token::TenantSigningKeys,
) -> u64 {
    active(store).await;
    finalize_shard_raft(store).await;
    let (status, body) = post(store, keys, SEAL, serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let generation = body["generation"].as_u64().expect("generation");
    assert_eq!(body["switch"]["generation"].as_u64(), Some(generation));
    assert_eq!(body["switch"]["sealer"], "broker-x");
    assert!(body["switch"].get("report").is_none(), "{body}");
    generation
}

/// No build reports `shard_raft`, so until one does no shard is sealed.
#[tokio::test]
async fn no_seal_is_ordered_before_the_fleet_runs_the_raft_engine() {
    let (store, keys) = one_shard_cluster().await;
    let before = active(&store).await;
    let (status, body) = post(&store, &keys, SEAL, serde_json::json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "raft_unavailable");
    assert_eq!(read(&store).await, before);
}

#[tokio::test]
async fn a_shard_that_cannot_be_sealed_is_refused() {
    let (store, keys) = one_shard_cluster().await;
    finalize_shard_raft(&store).await;
    let (status, body) = post(&store, &keys, SEAL, serde_json::json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let mut moving = active(&store).await;
    moving.joining = Some("broker-y".to_string());
    moving.move_reason = Some(crate::model::MoveReason::Replace);
    let moving = store.put_shard_assignment(moving).await.expect("joining");
    let (status, body) = post(&store, &keys, SEAL, serde_json::json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "not_sealable");
    assert_eq!(read(&store).await, moving);
}

#[tokio::test]
async fn one_report_switches_the_shard_and_a_second_does_not() {
    let (store, keys) = one_shard_cluster().await;
    let generation = sealed(&store, &keys).await;

    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-x/seal-report",
        report(generation),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], true);
    let switched = read(&store).await;
    assert!(switched.switched());
    assert_eq!(switched.generation, generation);

    let mut second = report(generation);
    second["report"]["offset"] = 10.into();
    let (status, body) = post(&store, &keys, "/v1/nodes/broker-x/seal-report", second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], false);

    let give_up = serde_json::json!({ "key": shard_zero(), "generation": generation });
    let (status, body) = post(&store, &keys, "/v1/nodes/broker-x/seal-give-up", give_up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["given_up"], false);
    assert_eq!(read(&store).await, switched);

    // The recorded report again: its answer was lost on the way.
    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-x/seal-report",
        report(generation),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], true);

    // And no second seal.
    let (status, body) = post(&store, &keys, SEAL, serde_json::json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "not_sealable");
}

#[tokio::test]
async fn only_the_sealer_reports_or_gives_up_its_seal() {
    let (store, keys) = one_shard_cluster().await;
    let generation = sealed(&store, &keys).await;
    let order = read(&store).await;

    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-y/seal-report",
        report(generation),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "not_sealer");
    let give_up = serde_json::json!({
        "key": shard_zero(),
        "generation": generation,
        "reason": "a follower's digest differs",
    });
    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-y/seal-give-up",
        give_up.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "not_sealer");
    assert_eq!(read(&store).await, order);

    let (status, body) = post(&store, &keys, "/v1/nodes/broker-x/seal-give-up", give_up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["given_up"], true);
    let reopened = read(&store).await;
    assert!(reopened.generation > generation);
    assert!(reopened.switch.is_none());

    // The seal's report, late.
    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-x/seal-report",
        report(generation),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], false);
    assert_eq!(read(&store).await, reopened);
}

#[tokio::test]
async fn a_report_for_a_shard_that_was_never_sealed_changes_nothing() {
    let (store, keys) = one_shard_cluster().await;
    let before = active(&store).await;
    let (status, body) = post(
        &store,
        &keys,
        "/v1/nodes/broker-x/seal-report",
        report(before.generation),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], false);
    assert_eq!(read(&store).await, before);
}

#[tokio::test]
async fn a_report_without_a_majority_is_a_conflict() {
    let (store, keys) = one_shard_cluster().await;
    let generation = sealed(&store, &keys).await;
    let order = read(&store).await;
    let mut alone = report(generation);
    alone["report"]["voters"] = serde_json::json!({ "broker-x": COPY_X });
    let (status, body) = post(&store, &keys, "/v1/nodes/broker-x/seal-report", alone).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(read(&store).await, order);
}
