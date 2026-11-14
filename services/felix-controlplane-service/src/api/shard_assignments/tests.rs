//! The changes feed's long-poll.
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::MAX_CHANGES_WAIT_MS;
use crate::api::build_router;
use crate::model::{ShardAssignment, ShardState};
use crate::store::ControlPlaneStore;
use crate::store::memory::InMemoryStore;
use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

struct Fixture {
    app: axum::Router,
    store: Arc<InMemoryStore>,
    state: crate::api::AppState,
    bearer: String,
}

async fn fixture() -> Fixture {
    let (store, keys) = one_shard_cluster().await;
    let state = app_state_ready(Arc::clone(&store) as _);
    Fixture {
        app: build_router(state.clone()),
        store,
        state,
        bearer: token(&keys, &["node.view:cluster:*"]),
    }
}

impl Fixture {
    async fn changes(&self, query: String) -> serde_json::Value {
        let request = Request::builder()
            .uri(format!("/v1/shard-assignments/changes?{query}"))
            .header("authorization", format!("Bearer {}", self.bearer))
            .body(Body::empty())
            .expect("request");
        let response = self.app.clone().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }

    async fn next_seq(&self) -> u64 {
        self.store
            .shard_assignment_snapshot()
            .await
            .expect("snapshot")
            .next_seq
    }

    async fn assign(&self, leader: &str) {
        self.store
            .put_shard_assignment(ShardAssignment {
                key: shard_zero(),
                leader: leader.to_string(),
                replicas: vec![],
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
            })
            .await
            .expect("assign");
    }
}

/// A caught-up caller with nothing new waits out `wait_ms` and gets the same
/// empty page it would have got at once.
#[tokio::test(start_paused = true)]
async fn waits_then_answers_empty_on_timeout() {
    let fx = fixture().await;
    let since = fx.next_seq().await;

    let started = tokio::time::Instant::now();
    let body = fx.changes(format!("since={since}&wait_ms=3000")).await;
    let waited = started.elapsed();

    assert!(waited >= Duration::from_millis(3000), "waited {waited:?}");
    assert!(waited < Duration::from_millis(3100), "waited {waited:?}");
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["next_seq"], since);
}

/// An absurd `wait_ms` is capped rather than holding a connection open for
/// as long as the caller asked.
#[tokio::test(start_paused = true)]
async fn a_long_wait_is_capped() {
    let fx = fixture().await;
    let since = fx.next_seq().await;

    let started = tokio::time::Instant::now();
    fx.changes(format!("since={since}&wait_ms={}", u64::MAX))
        .await;
    let waited = started.elapsed();

    assert!(waited >= Duration::from_millis(MAX_CHANGES_WAIT_MS));
    assert!(waited < Duration::from_millis(MAX_CHANGES_WAIT_MS + 100));
}

/// Without `wait_ms` nothing waits: an empty page comes straight back.
#[tokio::test(start_paused = true)]
async fn without_wait_ms_it_answers_at_once() {
    let fx = fixture().await;
    let since = fx.next_seq().await;

    let started = tokio::time::Instant::now();
    let body = fx.changes(format!("since={since}")).await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(body["items"], serde_json::json!([]));
}

/// Anything other than "nothing new" answers at once, even with `wait_ms`:
/// a change to apply, or a sign the caller has to re-snapshot.
#[tokio::test(start_paused = true)]
async fn a_page_with_news_is_not_held() {
    let fx = fixture().await;
    fx.assign("broker-x").await;
    let head = fx.next_seq().await;

    for (since, why) in [
        (0, "a change at or after since"),
        (head + 5, "a reset sequence"),
    ] {
        let started = tokio::time::Instant::now();
        fx.changes(format!("since={since}&wait_ms=10000")).await;
        assert_eq!(started.elapsed(), Duration::ZERO, "{why}");
    }
}

/// A write by another instance is seen at the next re-check of the store.
#[tokio::test]
async fn a_write_ends_the_wait_promptly() {
    let fx = fixture().await;
    let since = fx.next_seq().await;

    let started = std::time::Instant::now();
    let (body, ()) = tokio::join!(fx.changes(format!("since={since}&wait_ms=20000")), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        fx.assign("broker-x").await;
    });
    let waited = started.elapsed();

    assert_eq!(body["items"][0]["seq"], since);
    assert_eq!(body["items"][0]["assignment"]["leader"], "broker-x");
    // The write lands at 200 ms and the store is re-read every 50.
    assert!(waited < Duration::from_millis(1000), "waited {waited:?}");
}

/// This instance's own writes wake the wait directly, without waiting for
/// the next re-check. Paused time makes that exact: the re-check is 50 ms
/// out, and nothing advances the clock while the request has work to do.
#[tokio::test(start_paused = true)]
async fn an_own_write_wakes_the_wait_before_the_recheck() {
    let fx = fixture().await;
    let since = fx.next_seq().await;

    let started = tokio::time::Instant::now();
    let (body, ()) = tokio::join!(fx.changes(format!("since={since}&wait_ms=20000")), async {
        // The request has read the store and is waiting.
        tokio::time::sleep(Duration::from_millis(10)).await;
        fx.assign("broker-x").await;
        fx.state.placement_wakes.assignment_written();
    });

    assert_eq!(body["items"][0]["assignment"]["leader"], "broker-x");
    assert_eq!(started.elapsed(), Duration::from_millis(10));
}

/// A waiting request answers when the instance starts to drain, so it does
/// not hold the drain open.
#[tokio::test(start_paused = true)]
async fn a_drain_ends_the_wait() {
    let (store, keys) = one_shard_cluster().await;
    let closing = tokio_util::sync::CancellationToken::new();
    let mut state = app_state_ready(Arc::clone(&store) as _);
    state.placement_wakes = Arc::new(crate::cluster::placement::PlacementWakes::new(
        closing.clone(),
        crate::cluster::placement::DEFAULT_FENCE_MAX_LAG_RECORDS,
    ));
    let fx = Fixture {
        app: build_router(state.clone()),
        store,
        state,
        bearer: token(&keys, &["node.view:cluster:*"]),
    };
    let since = fx.next_seq().await;

    let started = tokio::time::Instant::now();
    let (body, ()) = tokio::join!(fx.changes(format!("since={since}&wait_ms=20000")), async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        closing.cancel();
    });

    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(started.elapsed(), Duration::from_millis(10));
}

/// The confirmed read answers one shard's assignment, and none for a shard
/// with no assignment.
#[tokio::test]
async fn the_confirmed_read_answers_one_shard() {
    let fixture = fixture().await;
    fixture.assign("broker-x").await;
    let read = |shard: u32| {
        let app = fixture.app.clone();
        let bearer = fixture.bearer.clone();
        async move {
            let request = Request::builder()
                .uri(format!(
                    "/v1/shard-assignments/confirmed?tenant_id=t1&namespace=ns&stream=orders&shard={shard}"
                ))
                .header("authorization", format!("Bearer {bearer}"))
                .body(Body::empty())
                .expect("request");
            let response = app.oneshot(request).await.expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            serde_json::from_slice::<serde_json::Value>(&bytes).expect("json")
        }
    };
    let assigned = read(0).await;
    assert_eq!(assigned["assignment"]["leader"], "broker-x");
    assert_eq!(read(7).await["assignment"], serde_json::Value::Null);
}

/// The batched confirmed read answers each shard in the request's order,
/// and refuses more shards than one request may read.
#[tokio::test]
async fn the_confirmed_read_answers_several_shards_in_order() {
    let fixture = fixture().await;
    fixture.assign("broker-x").await;
    let read = |shards: Vec<u32>| {
        let app = fixture.app.clone();
        let bearer = fixture.bearer.clone();
        async move {
            let keys: Vec<serde_json::Value> = shards
                .iter()
                .map(|shard| {
                    serde_json::json!({
                        "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": shard
                    })
                })
                .collect();
            let request = Request::builder()
                .method("POST")
                .uri("/v1/shard-assignments/confirmed")
                .header("authorization", format!("Bearer {bearer}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({ "keys": keys })).expect("body"),
                ))
                .expect("request");
            let response = app.oneshot(request).await.expect("response");
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).ok(),
            )
        }
    };
    let (status, body) = read(vec![7, 0]).await;
    assert_eq!(status, StatusCode::OK);
    let items = &body.expect("json")["items"];
    assert_eq!(items[0]["assignment"], serde_json::Value::Null);
    assert_eq!(items[1]["assignment"]["leader"], "broker-x");
    let (status, _) = read((0..257).collect()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// **The snapshot marks a shard's first assignment, and only that one**
/// (#1872). A broker that learns the shard from the snapshot records nothing
/// before it, as the `assigned` change tells a watching broker. A later
/// write is not first, so its item carries no mark.
#[tokio::test]
async fn the_snapshot_marks_only_a_first_assignment() {
    let fixture = fixture().await;
    let snapshot = || async {
        let request = Request::builder()
            .uri("/v1/shard-assignments/snapshot")
            .header("authorization", format!("Bearer {}", fixture.bearer))
            .body(Body::empty())
            .expect("request");
        let response = fixture
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        body["items"][0].clone()
    };

    // Only a shard with an incarnation is marked: recreate the stream once
    // the fleet finalized incarnations.
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        fixture
            .store
            .register_node(crate::store::contract::nodes::with_features(
                crate::store::contract::nodes::node(id, 7100 + i as u16),
                &[feature],
            ))
            .await
            .expect("report the feature");
    }
    fixture
        .store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");
    let key = crate::model::StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
    };
    let mut orders = fixture.store.get_stream(&key).await.expect("stream");
    fixture.store.delete_stream(&key).await.expect("delete");
    orders.incarnation = 0;
    fixture.store.create_stream(orders).await.expect("recreate");

    fixture.assign("broker-x").await;
    let first = snapshot().await;
    assert_eq!(first["leader"], "broker-x");
    assert_eq!(first["first_assignment"], true, "{first}");

    fixture.assign("broker-y").await;
    let later = snapshot().await;
    assert_eq!(later["leader"], "broker-y");
    assert_eq!(later.get("first_assignment"), None, "{later}");
}
