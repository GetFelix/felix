//! When the catalog counts as seeded, which gates the broker's readiness.

use super::*;

#[test]
fn a_partially_seeded_state_is_not_seeded() {
    // Every cursor must be non-zero. A snapshot that failed leaves its
    // cursor at 0 and `sync_once` still returns Ok, so this is the only
    // thing standing between a failed cold start and a ready broker.
    let mut state = SyncState::new();
    assert!(!state.is_seeded(), "a fresh state is not seeded");

    state.seeded.tenants = true;
    state.seeded.namespaces = true;
    state.seeded.caches = true;
    assert!(!state.is_seeded(), "streams never arrived");

    state.seeded.streams = true;
    assert!(state.is_seeded());

    // A cursor advancing on its own proves nothing: a failed snapshot
    // leaves the cursor at 0 and the change feed in the same iteration
    // polls from zero, which can carry it straight to the global tail.
    let mut cursors_only = SyncState::new();
    cursors_only.next_tenant_seq = 9_000;
    cursors_only.next_namespace_seq = 9_000;
    cursors_only.next_cache_seq = 9_000;
    cursors_only.next_stream_seq = 9_000;
    assert!(
        !cursors_only.is_seeded(),
        "advanced cursors must not stand in for an applied snapshot"
    );
}

#[tokio::test]
async fn an_empty_catalog_still_becomes_seeded() -> Result<()> {
    // A successful snapshot of an empty catalog returns next_seq == 0. That
    // is a valid cursor, not a failure, and such a deployment has to be
    // able to reach ready.
    let router = Router::new()
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        );
    let (addr, shutdown_tx, handle) = serve_router(router).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let client = build_test_client()?;

    let state = sync_once(
        &broker,
        &HttpSource::new(client.clone(), format!("http://{addr}")),
        None,
        SyncState::new(),
    )
    .await?;
    assert!(
        state.is_seeded(),
        "an empty catalog that fetched cleanly is seeded"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    Ok(())
}

#[tokio::test]
async fn a_failed_snapshot_never_reports_the_catalog_as_seeded() -> Result<()> {
    // A control plane that answers every snapshot with 500. `sync_once`
    // logs and returns Ok, so without the seeded check the broker would
    // report ready having restored nothing.
    let router = Router::new()
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let (addr, shutdown_tx, handle) = serve_router(router).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let client = build_test_client()?;

    let state = sync_once(
        &broker,
        &HttpSource::new(client.clone(), format!("http://{addr}")),
        None,
        SyncState::new(),
    )
    .await?;
    assert!(
        !state.is_seeded(),
        "a sync where every snapshot failed must not count as seeded"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    Ok(())
}
