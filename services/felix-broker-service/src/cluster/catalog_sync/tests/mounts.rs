//! A stream's mount reaches the broker's table with the rest of its
//! metadata: from the snapshot, from a change that moves it, a change that
//! clears it, and a delete.

use std::collections::HashMap;

use axum::extract::Query;
use felix_broker::Broker;

use super::*;

fn stream(mount: Option<&str>) -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "plant".to_string(),
        shards: 1,
        durable: false,
        consistency: None,
        retention: None,
        incarnation: 0,
        mount: mount.map(str::to_string),
    }
}

fn key() -> StreamKey {
    StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "plant".to_string(),
    }
}

/// A control plane whose stream feed is `snapshot` and then `feed(since)`.
fn control_plane(
    snapshot: fn() -> StreamSnapshotResponse,
    feed: fn(u64) -> StreamChangesResponse,
) -> Router {
    Router::new()
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
                    tombstones: Vec::new(),
                })
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(move || async move { Json(snapshot()) }),
        )
        .route(
            "/v1/tenants/changes",
            get(|| async {
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
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/caches/changes",
            get(|| async {
                Json(CacheChangesResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
        .route(
            "/v1/streams/changes",
            get(
                move |Query(query): Query<HashMap<String, u64>>| async move {
                    Json(feed(query.get("since").copied().unwrap_or(0)))
                },
            ),
        )
}

fn change(op: StreamChangeOp, seq: u64, mount: Option<&str>) -> StreamChangesResponse {
    StreamChangesResponse {
        items: vec![StreamChange {
            seq: Some(seq),
            op,
            key: key(),
            stream: Some(stream(mount)),
            deleted_through: None,
        }],
        next_seq: seq + 1,
    }
}

fn mounted_snapshot() -> StreamSnapshotResponse {
    StreamSnapshotResponse {
        items: vec![stream(Some("plant/"))],
        next_seq: 1,
        tombstones: Vec::new(),
    }
}

fn nothing_new(since: u64) -> StreamChangesResponse {
    StreamChangesResponse {
        items: vec![],
        next_seq: since.max(1),
    }
}

fn is_mounted(broker: &Broker, prefix: &str) -> bool {
    broker
        .resolve_mount("t1", &format!("{prefix}line1"))
        .is_some()
}

async fn pass(
    broker: &Arc<Broker>,
    snapshot: fn() -> StreamSnapshotResponse,
    feed: fn(u64) -> StreamChangesResponse,
    state: SyncState,
) -> Result<SyncState> {
    let (addr, shutdown_tx, handle) = serve_router(control_plane(snapshot, feed)).await?;
    let client = build_test_client()?;
    let state = sync_once(broker, &client, &format!("http://{addr}"), None, state).await?;
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    Ok(state)
}

#[tokio::test]
async fn a_mount_arrives_moves_clears_and_goes_with_the_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));

    // The snapshot mounts it.
    let state = pass(&broker, mounted_snapshot, nothing_new, SyncState::new()).await?;
    assert!(is_mounted(&broker, "plant/"));
    let found = broker.resolve_mount("t1", "plant/line1").expect("mounted");
    assert_eq!((&*found.namespace, &*found.stream), ("ns", "plant"));

    // A change moves it.
    let state = pass(
        &broker,
        mounted_snapshot,
        |_| change(StreamChangeOp::Updated, 2, Some("works/")),
        state,
    )
    .await?;
    assert!(!is_mounted(&broker, "plant/"));
    assert!(is_mounted(&broker, "works/"));

    // A change without one clears it.
    let state = pass(
        &broker,
        mounted_snapshot,
        |_| change(StreamChangeOp::Updated, 3, None),
        state,
    )
    .await?;
    assert!(!is_mounted(&broker, "works/"));
    assert!(broker.stream_exists("t1", "ns", "plant").await);

    // Mounted again, then deleted.
    let state = pass(
        &broker,
        mounted_snapshot,
        |_| change(StreamChangeOp::Updated, 4, Some("plant/")),
        state,
    )
    .await?;
    assert!(is_mounted(&broker, "plant/"));
    pass(
        &broker,
        mounted_snapshot,
        |_| change(StreamChangeOp::Deleted, 5, None),
        state,
    )
    .await?;
    assert!(!broker.stream_exists("t1", "ns", "plant").await);
    assert!(!is_mounted(&broker, "plant/"));
    Ok(())
}

/// A control plane that predates mounts sends none, and a stream it describes
/// is not mounted.
#[test]
fn a_stream_from_an_older_control_plane_has_no_mount() {
    let old = serde_json::json!({
        "tenant_id": "t1", "namespace": "ns", "stream": "plant",
        "shards": 1, "durable": false,
    });
    let parsed: Stream = serde_json::from_value(old).expect("parse");
    assert_eq!(parsed.mount, None);
    assert_eq!(parsed.metadata().expect("metadata").mount, None);
}
