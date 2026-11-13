//! A snapshot from another control-plane metadata group is never applied.

use tokio_util::sync::CancellationToken;

use super::*;
use crate::cluster::controlplane_group::{Guard, Mismatch};

/// A control plane in `group` whose catalog holds one stream and one cache.
fn control_plane(group: Option<&'static str>) -> Router {
    let empty = || async { Json(serde_json::json!({ "items": [], "next_seq": 1 })) };
    let claim = move |mut body: serde_json::Value| {
        if let Some(group) = group {
            body["group_id"] = group.into();
        }
        Json(body)
    };
    Router::new()
        .route(
            "/v1/tenants/snapshot",
            get(|| async {
                Json(serde_json::json!({ "items": [{ "tenant_id": "t1" }], "next_seq": 1 }))
            }),
        )
        .route(
            "/v1/namespaces/snapshot",
            get(|| async {
                Json(serde_json::json!({
                    "items": [{ "tenant_id": "t1", "namespace": "ns" }],
                    "next_seq": 1,
                }))
            }),
        )
        .route(
            "/v1/caches/snapshot",
            get(move || async move {
                claim(serde_json::json!({
                    "items": [{
                        "tenant_id": "t1", "namespace": "ns", "cache": "c",
                    }],
                    "next_seq": 1,
                }))
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(move || async move {
                claim(serde_json::json!({
                    "items": [{
                        "tenant_id": "t1", "namespace": "ns", "stream": "s",
                        "shards": 1, "durable": false,
                    }],
                    "next_seq": 1,
                }))
            }),
        )
        .route("/v1/tenants/changes", get(empty))
        .route("/v1/namespaces/changes", get(empty))
        .route("/v1/caches/changes", get(empty))
        .route("/v1/streams/changes", get(empty))
}

/// One pass against `group`, holding snapshots to `stored`.
async fn pass(
    group: Option<&'static str>,
    stored: &str,
) -> Result<(Result<()>, Arc<Broker>, SyncState, CancellationToken)> {
    let (addr, stop, handle) = serve_router(control_plane(group)).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let refused = CancellationToken::new();
    let mut state = SyncState::new();
    state.group = Some(Guard {
        stored: stored.to_owned(),
        refused: refused.clone(),
    });
    let source = HttpSource::new(build_test_client()?, format!("http://{addr}"));
    let result = sync_into(&broker, &source, None, &mut state).await;
    let _ = stop.send(());
    handle.await?;
    Ok((result, broker, state, refused))
}

#[tokio::test]
async fn a_snapshot_from_another_group_is_not_applied() -> Result<()> {
    let (result, broker, state, refused) = pass(Some("g-2"), "g-1").await?;
    let err = result.expect_err("refused");
    assert!(err.is::<Mismatch>(), "{err:#}");
    assert!(refused.is_cancelled(), "the broker is told to stop");
    assert!(!broker.cache_exists("t1", "ns", "c").await);
    assert!(!broker.stream_exists("t1", "ns", "s").await);
    assert!(
        !state.is_seeded(),
        "the broker must not become seeded from it"
    );
    Ok(())
}

#[tokio::test]
async fn a_snapshot_from_the_same_group_is_applied() -> Result<()> {
    let (result, broker, state, refused) = pass(Some("g-1"), "g-1").await?;
    result?;
    assert!(!refused.is_cancelled());
    assert!(broker.cache_exists("t1", "ns", "c").await);
    assert!(broker.stream_exists("t1", "ns", "s").await);
    assert!(state.is_seeded());
    Ok(())
}

#[tokio::test]
async fn a_snapshot_with_no_claim_is_applied() -> Result<()> {
    let (result, broker, state, refused) = pass(None, "g-1").await?;
    result?;
    assert!(!refused.is_cancelled());
    assert!(broker.stream_exists("t1", "ns", "s").await);
    assert!(state.is_seeded());
    Ok(())
}

#[tokio::test]
async fn the_sync_loop_ends_on_a_refusal_instead_of_retrying() -> Result<()> {
    let (addr, stop, handle) = serve_router(control_plane(Some("g-2"))).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let guard = Guard {
        stored: "g-1".to_owned(),
        refused: CancellationToken::new(),
    };
    let source = HttpSource::new(build_test_client()?, format!("http://{addr}"));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        sync_loop(
            broker,
            &source,
            Duration::from_millis(10),
            None,
            None,
            Default::default(),
            Some(guard),
        ),
    )
    .await?;
    assert!(result.expect_err("ends").is::<Mismatch>());
    let _ = stop.send(());
    handle.await?;
    Ok(())
}
