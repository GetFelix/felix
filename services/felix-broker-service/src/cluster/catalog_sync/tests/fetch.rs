//! Fetching snapshots and change feeds, and refusing bad answers.

use super::*;

#[tokio::test]
async fn fetch_cache_snapshot_rejects_non_200() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new().route(
            "/v1/caches/snapshot",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let result = fetch_cache_snapshot(&HttpSource::new(client.clone(), &base_url), None).await;
        assert!(result.is_err());

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn fetch_stream_changes_rejects_invalid_json() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new().route(
            "/v1/streams/changes",
            get(|| async { (StatusCode::OK, "not-json") }),
        );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let result = fetch_changes(&HttpSource::new(client.clone(), &base_url), None, 0).await;
        assert!(result.is_err());

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}
