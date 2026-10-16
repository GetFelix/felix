//! The sync task's retry around its first pass.

use super::*;

#[tokio::test]
async fn start_sync_retries_after_transient_parent_ordering_error() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let namespace_polls = StdArc::new(AtomicUsize::new(0));
        let namespace_polls_clone = StdArc::clone(&namespace_polls);
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                        tombstones: Vec::new(),
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                        tombstones: Vec::new(),
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(move || {
                    let namespace_polls = StdArc::clone(&namespace_polls_clone);
                    async move {
                        let poll = namespace_polls.fetch_add(1, Ordering::SeqCst);
                        let items = if poll == 0 {
                            vec![]
                        } else {
                            vec![TenantChange {
                                op: TenantChangeOp::Created,
                                tenant_id: "t1".to_string(),
                                tenant: Some(Tenant {
                                    tenant_id: "t1".to_string(),
                                }),
                                seq: None,
                            }]
                        };
                        Json(TenantChangesResponse { items, next_seq: 2 })
                    }
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![NamespaceChange {
                            op: NamespaceChangeOp::Created,
                            key: NamespaceKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            },
                            namespace: Some(Namespace {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            }),
                            seq: None,
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            );

        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let sync_task = tokio::spawn(start_sync_with_signal(
            Arc::clone(&broker),
            base_url,
            Duration::from_millis(10),
            None,
            None,
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if broker.namespace_exists("t1", "ns1").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(broker.namespace_exists("t1", "ns1").await);

        sync_task.abort();
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}
