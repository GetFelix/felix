//! One sync pass against a fake control plane.

use super::*;

#[tokio::test]
async fn sync_once_logs_and_skips_on_errors() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (addr, shutdown_tx, handle) = serve_router(error_router()).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 0);
        assert_eq!(state.next_namespace_seq, 0);
        assert_eq!(state.next_cache_seq, 0);
        assert_eq!(state.next_stream_seq, 0);
        assert!(!broker.namespace_exists("t1", "ns").await);
        assert!(!broker.stream_exists("t1", "ns", "s1").await);
        assert!(!broker.cache_exists("t1", "ns", "c1").await);

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
async fn sync_once_handles_tenant_deletion() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t1".to_string(),
                        }],
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
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![TenantChange {
                            op: TenantChangeOp::Deleted,
                            tenant_id: "t1".to_string(),
                            tenant: None,
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![],
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
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 2);
        let err = broker.register_namespace("t1", "ns").await;
        assert!(err.is_err());

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
async fn sync_once_handles_namespace_deletion() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t1".to_string(),
                        }],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![Namespace {
                            tenant_id: "t1".to_string(),
                            namespace: "ns".to_string(),
                        }],
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
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![NamespaceChange {
                            op: NamespaceChangeOp::Deleted,
                            key: NamespaceKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns".to_string(),
                            },
                            namespace: None,
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
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_namespace_seq, 2);
        assert!(!broker.namespace_exists("t1", "ns").await);

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
async fn sync_once_registers_created_and_updated_changes() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
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
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![TenantChange {
                            op: TenantChangeOp::Created,
                            tenant_id: "t1".to_string(),
                            tenant: Some(Tenant {
                                tenant_id: "t1".to_string(),
                            }),
                        }],
                        next_seq: 2,
                    })
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
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![CacheChange {
                            op: CacheChangeOp::Updated,
                            key: CacheKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                cache: "c1".to_string(),
                            },
                            cache: Some(Cache {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                cache: "c1".to_string(),
                                consistency: None,
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![StreamChange {
                            op: StreamChangeOp::Created,
                            key: StreamKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                stream: "orders".to_string(),
                            },
                            stream: Some(Stream {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                stream: "orders".to_string(),
                                shards: 2,
                                durable: true,
                                consistency: None,
                                retention: None,
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 2);
        assert_eq!(state.next_namespace_seq, 2);
        assert_eq!(state.next_cache_seq, 2);
        assert_eq!(state.next_stream_seq, 2);
        assert!(broker.namespace_exists("t1", "ns1").await);
        assert!(broker.cache_exists("t1", "ns1", "c1").await);
        // `orders` is marked durable and this broker has no durable storage,
        // so the sync skips it and keeps going rather than registering a
        // stream it could not persist. Everything else still applied.
        assert!(!broker.stream_exists("t1", "ns1", "orders").await);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}
