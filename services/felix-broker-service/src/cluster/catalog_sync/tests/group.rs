//! Nothing from another control-plane metadata group is applied, whether it
//! comes as a snapshot or as a change-feed page, and nothing that names no
//! group is applied while the directory is recorded under one.

use tokio_util::sync::CancellationToken;

use super::*;
use crate::cluster::controlplane_group::{Guard, Mismatch, Unclaimed};

/// What the control plane claims, per answer.
#[derive(Clone, Copy)]
struct Claims {
    tenants: Option<&'static str>,
    snapshots: Option<&'static str>,
    changes: Option<&'static str>,
}

impl Claims {
    fn all(group: Option<&'static str>) -> Self {
        Self {
            tenants: group,
            snapshots: group,
            changes: group,
        }
    }
}

fn claimed(group: Option<&'static str>, mut body: serde_json::Value) -> Json<serde_json::Value> {
    if let Some(group) = group {
        body["group_id"] = group.into();
    }
    Json(body)
}

type Since = axum::extract::Query<std::collections::HashMap<String, u64>>;

/// A control plane whose catalog holds one stream and one cache, and whose
/// stream feed creates stream `s2`.
fn control_plane(claims: Claims) -> Router {
    let empty = move || async move {
        claimed(
            claims.changes,
            serde_json::json!({ "items": [], "next_seq": 1 }),
        )
    };
    Router::new()
        .route(
            "/v1/tenants/snapshot",
            get(move || async move {
                claimed(
                    claims.tenants,
                    serde_json::json!({ "items": [{ "tenant_id": "t1" }], "next_seq": 1 }),
                )
            }),
        )
        .route(
            "/v1/namespaces/snapshot",
            get(move || async move {
                claimed(
                    claims.tenants,
                    serde_json::json!({
                        "items": [{ "tenant_id": "t1", "namespace": "ns" }],
                        "next_seq": 1,
                    }),
                )
            }),
        )
        .route(
            "/v1/caches/snapshot",
            get(move || async move {
                claimed(
                    claims.snapshots,
                    serde_json::json!({
                        "items": [{
                            "tenant_id": "t1", "namespace": "ns", "cache": "c",
                        }],
                        "next_seq": 1,
                    }),
                )
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(move || async move {
                claimed(
                    claims.snapshots,
                    serde_json::json!({
                        "items": [{
                            "tenant_id": "t1", "namespace": "ns", "stream": "s",
                            "shards": 1, "durable": false,
                        }],
                        "next_seq": 1,
                    }),
                )
            }),
        )
        .route("/v1/tenants/changes", get(empty))
        .route("/v1/namespaces/changes", get(empty))
        .route("/v1/caches/changes", get(empty))
        .route(
            "/v1/streams/changes",
            get(move |axum::extract::Query(query): Since| async move {
                let since = query.get("since").copied().unwrap_or(0);
                let items = if since <= 1 {
                    serde_json::json!([{
                        "seq": 1, "op": "created",
                        "key": { "tenant_id": "t1", "namespace": "ns", "stream": "s2" },
                        "stream": {
                            "tenant_id": "t1", "namespace": "ns", "stream": "s2",
                            "shards": 1, "durable": false,
                        },
                    }])
                } else {
                    serde_json::json!([])
                };
                claimed(
                    claims.changes,
                    serde_json::json!({ "items": items, "next_seq": 2 }),
                )
            }),
        )
}

/// One pass against `claims`, holding answers to `stored`.
async fn pass(
    claims: Claims,
    stored: Option<&str>,
) -> Result<(Result<()>, Arc<Broker>, SyncState, CancellationToken)> {
    let (addr, stop, handle) = serve_router(control_plane(claims)).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let refused = CancellationToken::new();
    let mut state = SyncState::new();
    state.group = stored.map(|stored| Guard {
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
    let claims = Claims {
        snapshots: Some("g-2"),
        ..Claims::all(Some("g-1"))
    };
    let (result, broker, state, refused) = pass(claims, Some("g-1")).await?;
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
async fn a_tenant_snapshot_from_another_group_is_not_applied() -> Result<()> {
    let claims = Claims {
        tenants: Some("g-2"),
        ..Claims::all(Some("g-1"))
    };
    let (result, broker, _, refused) = pass(claims, Some("g-1")).await?;
    assert!(result.expect_err("refused").is::<Mismatch>());
    assert!(refused.is_cancelled());
    assert!(!broker.tenant_exists("t1").await);
    Ok(())
}

#[tokio::test]
async fn everything_from_the_same_group_is_applied() -> Result<()> {
    let (result, broker, state, refused) = pass(Claims::all(Some("g-1")), Some("g-1")).await?;
    result?;
    assert!(!refused.is_cancelled());
    assert!(broker.cache_exists("t1", "ns", "c").await);
    assert!(broker.stream_exists("t1", "ns", "s").await);
    assert!(broker.stream_exists("t1", "ns", "s2").await);
    assert!(state.is_seeded());
    Ok(())
}

/// A control plane recreated with its sequence already past this broker's
/// cursor: the snapshots were this broker's group, the feed is not.
#[tokio::test]
async fn a_change_page_from_another_group_is_not_applied() -> Result<()> {
    let claims = Claims {
        changes: Some("g-2"),
        ..Claims::all(Some("g-1"))
    };
    let (result, broker, state, refused) = pass(claims, Some("g-1")).await?;
    assert!(result.expect_err("refused").is::<Mismatch>());
    assert!(refused.is_cancelled(), "the broker is told to stop");
    assert!(!broker.stream_exists("t1", "ns", "s2").await);
    assert_eq!(state.next_stream_seq, 1, "the cursor stays");
    Ok(())
}

#[tokio::test]
async fn a_snapshot_with_no_claim_is_skipped_not_applied() -> Result<()> {
    let claims = Claims {
        snapshots: None,
        ..Claims::all(Some("g-1"))
    };
    let (result, broker, state, refused) = pass(claims, Some("g-1")).await?;
    assert!(result.expect_err("skipped").is::<Unclaimed>());
    assert!(!refused.is_cancelled(), "the broker keeps running");
    assert!(!broker.stream_exists("t1", "ns", "s").await);
    assert!(!state.is_seeded());
    Ok(())
}

#[tokio::test]
async fn a_change_page_with_no_claim_is_skipped_not_applied() -> Result<()> {
    let claims = Claims {
        changes: None,
        ..Claims::all(Some("g-1"))
    };
    let (result, broker, _, refused) = pass(claims, Some("g-1")).await?;
    assert!(result.expect_err("skipped").is::<Unclaimed>());
    assert!(!refused.is_cancelled(), "the broker keeps running");
    assert!(!broker.stream_exists("t1", "ns", "s2").await);
    Ok(())
}

#[tokio::test]
async fn without_a_recorded_group_no_claim_is_applied() -> Result<()> {
    let (result, broker, state, _) = pass(Claims::all(None), None).await?;
    result?;
    assert!(broker.stream_exists("t1", "ns", "s2").await);
    assert!(state.is_seeded());
    Ok(())
}

#[tokio::test]
async fn the_sync_loop_ends_on_a_refusal_instead_of_retrying() -> Result<()> {
    let (addr, stop, handle) = serve_router(control_plane(Claims::all(Some("g-2")))).await?;
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
