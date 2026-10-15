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

/// A feed more than one page behind is followed to its tail in one pass, not
/// one page per sync interval.
#[tokio::test]
async fn sync_once_follows_a_feed_across_pages() -> Result<()> {
    use std::collections::HashMap;

    use axum::extract::Query;

    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        // One change per page, the way a control plane answers when it is
        // capped at one change per page.
        let tenants = ["t0", "t1", "t2"];
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(
                    move |Query(query): Query<HashMap<String, u64>>| async move {
                        let since = query.get("since").copied().unwrap_or(0);
                        let page: Vec<TenantChange> = tenants
                            .get(since as usize)
                            .map(|tenant_id| TenantChange {
                                op: TenantChangeOp::Created,
                                tenant_id: tenant_id.to_string(),
                                tenant: Some(Tenant {
                                    tenant_id: tenant_id.to_string(),
                                }),
                            })
                            .into_iter()
                            .collect();
                        let next_seq = since + page.len() as u64;
                        Json(TenantChangesResponse {
                            items: page,
                            next_seq,
                        })
                    },
                ),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 0,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        for tenant_id in tenants {
            assert!(
                broker.tenant_exists(tenant_id).await,
                "{tenant_id} was past the first page and was not applied",
            );
        }
        assert_eq!(state.next_tenant_seq, tenants.len() as u64);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

/// A page from behind the cursor, as a lagging Raft member answers, is not
/// applied and does not move the cursor back. Taking it would replay a delete
/// the broker already applied before the stream was created again.
#[tokio::test]
async fn sync_once_ignores_a_page_from_behind_the_cursor() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1".to_string()).await?;
        let router = Router::new()
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
                        next_seq: 3,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 3,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 3,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let mut state = SyncState::new();
        state.seeded = SeededSnapshots {
            tenants: true,
            namespaces: true,
            caches: true,
            streams: true,
        };
        state.next_tenant_seq = 5;
        state.next_namespace_seq = 5;
        state.next_cache_seq = 5;
        state.next_stream_seq = 5;

        let state = sync_once(&broker, &client, &base_url, None, state).await?;
        assert!(
            broker.tenant_exists("t1").await,
            "a delete from behind the cursor was applied",
        );
        assert_eq!(state.next_tenant_seq, 5);
        assert_eq!(state.next_namespace_seq, 5);
        assert_eq!(state.next_cache_seq, 5);
        assert_eq!(state.next_stream_seq, 5);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

/// A feed that has answered only from behind the cursor for long enough is
/// taken as reset, and the broker takes a new snapshot rather than adopting
/// the new `next_seq`: the changes before it are a history this broker has
/// never seen, such as a tenant created again after a control plane restart.
#[tokio::test]
async fn a_feed_reset_is_followed_by_a_new_snapshot() -> Result<()> {
    use std::collections::HashMap;

    use axum::extract::Query;

    use crate::cluster::feed_cursor::{BehindCursor, RESET_AFTER};

    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t2".to_string(),
                        }],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|Query(_): Query<HashMap<String, u64>>| async {
                    // The restarted control plane's whole history is the
                    // one create, which only the snapshot shows.
                    Json(TenantChangesResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![],
                        next_seq: 50,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 50,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 50,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let mut state = SyncState::new();
        state.seeded = SeededSnapshots {
            tenants: true,
            namespaces: true,
            caches: true,
            streams: true,
        };
        state.next_tenant_seq = 50;
        state.next_namespace_seq = 50;
        state.next_cache_seq = 50;
        state.next_stream_seq = 50;
        let behind_since = tokio::time::Instant::now()
            .checked_sub(RESET_AFTER)
            .expect("clock far enough along");
        state.behind.tenants = BehindCursor::behind_since(behind_since);

        let state = sync_once(&broker, &client, &base_url, None, state).await?;
        let state = sync_once(&broker, &client, &base_url, None, state).await?;
        assert!(
            broker.tenant_exists("t2").await,
            "the reset feed's history before its next_seq was never applied",
        );
        assert!(state.seeded.tenants);
        assert_eq!(state.next_tenant_seq, 1);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}
