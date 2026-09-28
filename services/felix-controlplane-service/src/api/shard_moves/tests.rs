//! The operator's move controls over HTTP.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::api::build_router;
use crate::model::{ShardAssignment, ShardState};
use crate::store::ControlPlaneStore;
use crate::store::memory::InMemoryStore;
use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

struct Fixture {
    app: axum::Router,
    store: Arc<InMemoryStore>,
    viewer: String,
    operator: String,
}

async fn fixture() -> Fixture {
    let (store, keys) = one_shard_cluster().await;
    let state = app_state_ready(Arc::clone(&store) as _);
    Fixture {
        app: build_router(state),
        store,
        viewer: token(&keys, &["node.view:cluster:*"]),
        operator: token(&keys, &["node.view:cluster:*", "node.manage:cluster:*"]),
    }
}

const CANCEL: &str = "/v1/shard-moves/t1/ns/orders/0";
const ABANDON: &str = "/v1/placement/abandon/t1/ns/orders/0";

impl Fixture {
    async fn call(
        &self,
        method: &str,
        uri: &str,
        bearer: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request");
        let response = self.app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("json")
        };
        (status, value)
    }

    async fn operator(&self, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
        self.call(method, uri, &self.operator, body).await
    }

    async fn assign(&self, state: ShardState, successor: Option<&str>) -> ShardAssignment {
        self.store
            .put_shard_assignment(ShardAssignment {
                key: shard_zero(),
                leader: "broker-x".to_string(),
                replicas: successor.iter().map(|s| s.to_string()).collect(),
                generation: 0,
                state,
                successor: successor.map(str::to_string),
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
            })
            .await
            .expect("assign")
    }
}

fn start_body(destination: &str) -> Value {
    json!({
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": 0,
        "destination": destination,
    })
}

/// Reading moves takes the same permission as reading assignments; changing
/// them takes `node.manage` on the cluster, like a drain.
#[tokio::test]
async fn reading_moves_takes_view_and_changing_them_takes_manage() {
    let fx = fixture().await;
    fx.assign(ShardState::Active, None).await;

    for uri in ["/v1/shard-moves", "/v1/placement/plan"] {
        let (status, _) = fx.call("GET", uri, &fx.viewer, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        let (status, _) = fx.call("GET", uri, "not-a-token", Value::Null).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
    }
    for (method, uri, body) in [
        ("POST", "/v1/shard-moves", start_body("broker-y")),
        ("DELETE", CANCEL, Value::Null),
        ("POST", "/v1/placement/pause", Value::Null),
        ("POST", "/v1/placement/resume", Value::Null),
        ("POST", ABANDON, Value::Null),
    ] {
        let (status, _) = fx.call(method, uri, &fx.viewer, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}");
    }
    let unchanged = fx
        .store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(unchanged.successor, None);
    assert!(!fx.store.moves_paused().await.expect("read"));
}

/// Start a move, see it listed with who asked, cancel it, and it is gone.
#[tokio::test]
async fn an_operator_starts_lists_and_cancels_a_move() {
    let fx = fixture().await;
    fx.assign(ShardState::Active, None).await;

    let (status, started) = fx
        .operator("POST", "/v1/shard-moves", start_body("broker-y"))
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    assert_eq!(started["step"], "stage");
    assert_eq!(started["assignment"]["successor"], "broker-y");
    assert_eq!(started["assignment"]["move_reason"], "operator");
    assert!(started.get("zones_before").is_none(), "no zones: {started}");
    assert!(started.get("dry_run").is_none(), "{started}");

    let (_, listed) = fx.operator("GET", "/v1/shard-moves", Value::Null).await;
    assert_eq!(listed["paused"], false);
    let items = listed["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{listed}");
    assert_eq!(items[0]["stream"], "orders");
    assert_eq!(items[0]["leader"], "broker-x");
    assert_eq!(items[0]["destination"], "broker-y");
    assert_eq!(items[0]["step"], "staged");
    assert_eq!(items[0]["reason"], "operator");
    assert_eq!(items[0]["caught_up"], false);
    assert_eq!(items[0]["drained"], false);

    let (status, again) = fx
        .operator("POST", "/v1/shard-moves", start_body("broker-y"))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(again["code"], "already_moving");

    let (status, cancelled) = fx.operator("DELETE", CANCEL, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["step"], "cancel");
    assert!(cancelled["assignment"].get("successor").is_none());

    let (_, listed) = fx.operator("GET", "/v1/shard-moves", Value::Null).await;
    assert_eq!(listed["items"], json!([]));
}

/// With zones reported, a move says how many the shard spans before and
/// after, and a dry run says the same without writing anything.
#[tokio::test]
async fn a_move_reports_its_zone_impact_and_a_dry_run_writes_nothing() {
    let fx = fixture().await;
    for (port, id, zone) in [
        (7100, "broker-x", "a"),
        (7101, "broker-y", "a"),
        (7102, "broker-z", "b"),
    ] {
        let mut node = fx.store.get_node("broker-x").await.expect("broker-x");
        node.node_id = id.to_string();
        node.spec.advertise_addr = format!("10.0.0.4:{port}");
        node.spec.zone = Some(zone.to_string());
        fx.store.register_node(node).await.expect("register");
    }
    fx.assign(ShardState::Active, None).await;

    let mut body = start_body("broker-z");
    body["dry_run"] = json!(true);
    let (status, preview) = fx.operator("POST", "/v1/shard-moves", body).await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview["dry_run"], true);
    assert_eq!(preview["step"], "stage");
    assert_eq!(preview["assignment"]["successor"], "broker-z");
    assert_eq!(
        (
            preview["zones_before"].clone(),
            preview["zones_after"].clone()
        ),
        (json!(1), json!(2))
    );
    let untouched = fx
        .store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(untouched.successor, None, "a dry run writes nothing");

    let (status, started) = fx
        .operator("POST", "/v1/shard-moves", start_body("broker-z"))
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    assert!(started.get("dry_run").is_none(), "{started}");
    assert_eq!(
        (
            started["zones_before"].clone(),
            started["zones_after"].clone()
        ),
        (json!(1), json!(2))
    );
}

/// A fenced move is handed back to its leader at a new generation; with
/// nothing left in progress, a second cancel is a conflict.
#[tokio::test]
async fn a_fenced_move_is_taken_back_and_then_there_is_nothing_to_cancel() {
    let fx = fixture().await;
    fx.assign(ShardState::Active, Some("broker-y")).await;
    let fenced = fx.assign(ShardState::Draining, Some("broker-y")).await;

    let (_, listed) = fx.operator("GET", "/v1/shard-moves", Value::Null).await;
    assert_eq!(listed["items"][0]["step"], "fenced", "{listed}");

    let (status, cancelled) = fx.operator("DELETE", CANCEL, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{cancelled}");
    assert_eq!(cancelled["step"], "retake");
    assert_eq!(cancelled["assignment"]["leader"], "broker-x");
    assert_eq!(cancelled["assignment"]["state"], "assigning");
    assert_eq!(cancelled["assignment"]["generation"], fenced.generation + 1);

    let (status, refused) = fx.operator("DELETE", CANCEL, Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(refused["code"], "not_moving");
}

/// A request placement would refuse is refused with the reason.
#[tokio::test]
async fn a_move_placement_would_not_make_is_refused_with_the_reason() {
    let fx = fixture().await;
    fx.assign(ShardState::Active, None).await;

    let (status, body) = fx
        .operator("POST", "/v1/shard-moves", start_body("broker-z"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "unknown_node");

    let (status, body) = fx
        .operator("POST", "/v1/shard-moves", start_body("broker-x"))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "already_leader");

    let mut other_shard = start_body("broker-y");
    other_shard["shard"] = json!(7);
    let (status, body) = fx.operator("POST", "/v1/shard-moves", other_shard).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "unknown_shard");

    let (status, body) = fx
        .operator(
            "DELETE",
            "/v1/shard-moves/t1/ns/orders/0?kind=cache",
            Value::Null,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "unknown_shard");
}

/// Pausing is stored, and both the listing and the plan say so.
#[tokio::test]
async fn pausing_and_resuming_are_stored_and_reported() {
    let fx = fixture().await;
    let (status, paused) = fx
        .operator("POST", "/v1/placement/pause", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(paused, json!({ "paused": true }));
    assert!(fx.store.moves_paused().await.expect("read"));

    let (_, listed) = fx.operator("GET", "/v1/shard-moves", Value::Null).await;
    assert_eq!(listed["paused"], true);
    let (_, plan) = fx.operator("GET", "/v1/placement/plan", Value::Null).await;
    assert_eq!(plan["paused"], true);

    let (status, resumed) = fx
        .operator("POST", "/v1/placement/resume", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resumed, json!({ "paused": false }));
    assert!(!fx.store.moves_paused().await.expect("read"));
}

/// The plan says what the next pass would write, and writes nothing.
#[tokio::test]
async fn the_plan_previews_a_pass_without_writing_it() {
    let fx = fixture().await;
    let (status, plan) = fx.operator("GET", "/v1/placement/plan", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let items = plan["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{plan}");
    assert_eq!(items[0]["action"], "place");
    assert_eq!(items[0]["stream"], "orders");
    assert!(items[0]["assignment"]["leader"].is_string());
    assert!(
        fx.store.get_shard_assignment(&shard_zero()).await.is_err(),
        "the preview wrote an assignment"
    );
}

/// Abandoning a log is cluster-wide operator work: a broker's own
/// `node.manage` does not reach it. It is refused while the leader serves,
/// and once the leader is down it places the shard on a live node.
#[tokio::test]
async fn abandoning_a_log_takes_the_cluster_operator_and_a_lost_leader() {
    let fx = fixture().await;
    let (_, keys) = one_shard_cluster().await;
    let before = fx.assign(ShardState::Assigning, None).await;
    let broker = token(&keys, &["node.manage:node:broker-x"]);
    let (status, _) = fx.call("POST", ABANDON, &broker, Value::Null).await;
    assert!(
        matches!(status, StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED),
        "{status}"
    );

    let (status, body) = fx.operator("POST", ABANDON, Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "not_stranded");

    fx.store
        .set_node_lifecycle("broker-x", crate::model::NodeLifecycle::Down)
        .await
        .expect("down");
    let (status, body) = fx.operator("POST", ABANDON, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["step"], "discard");
    assert_eq!(body["assignment"]["leader"], "broker-y");
    let after = fx
        .store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(after.leader, "broker-y");
    assert!(after.generation > before.generation);
}
