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
