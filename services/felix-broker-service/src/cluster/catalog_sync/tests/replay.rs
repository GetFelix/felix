//! A change that fails every time must not make the sync apply the changes
//! before it again: replaying a delete after the create that followed it would
//! remove the new stream. Driven through `start_sync_with_signal`, the loop a
//! broker runs.

use std::collections::HashMap;

use axum::extract::Query;
use bytes::Bytes;
use felix_broker::{Broker, LogKind};
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn stream(consistency: Option<&str>) -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shards: 1,
        durable: true,
        consistency: consistency.map(str::to_string),
        retention: None,
        // Incarnation 0, as every stream is before the fleet finalizes
        // incarnations: the delete cannot say which stream it removed.
        incarnation: 0,
    }
}

fn key() -> StreamKey {
    StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
    }
}

/// The stream deleted, created again, and then a change no pass can apply.
fn feed(since: u64) -> StreamChangesResponse {
    let at = |incarnation, consistency| Stream {
        incarnation,
        ..stream(consistency)
    };
    let items = vec![
        StreamChange {
            seq: Some(1),
            op: StreamChangeOp::Deleted,
            key: key(),
            stream: Some(at(5, None)),
            deleted_through: Some(5),
        },
        StreamChange {
            seq: Some(2),
            op: StreamChangeOp::Created,
            key: key(),
            stream: Some(at(6, None)),
            deleted_through: None,
        },
        StreamChange {
            seq: Some(3),
            op: StreamChangeOp::Updated,
            key: key(),
            stream: Some(at(6, Some("Unheard-of"))),
            deleted_through: None,
        },
    ];
    StreamChangesResponse {
        items: items
            .into_iter()
            .filter(|change| change.seq.is_some_and(|seq| seq >= since))
            .collect(),
        next_seq: 4,
    }
}

fn router() -> Router {
    router_with(
        || StreamSnapshotResponse {
            items: vec![Stream {
                incarnation: 5,
                ..stream(None)
            }],
            next_seq: 1,
            tombstones: Vec::new(),
        },
        feed,
    )
}

fn router_with(
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
                })
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(move || {
                let snapshot = snapshot();
                async move { Json(snapshot) }
            }),
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

async fn tail(broker: &Broker) -> Option<u64> {
    let log = broker
        .shard_log(LogKind::Stream, "t1", "ns", "orders", 0)
        .await?;
    log.tail_offset().await.ok()
}

#[tokio::test]
async fn a_change_that_always_fails_does_not_replay_the_changes_before_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "ns").await?;
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation: 5,
                ..Default::default()
            },
        )
        .await?;
    for _ in 0..2 {
        broker
            .publish("t1", "ns", "orders", Bytes::from_static(b"old"))
            .await?;
    }

    let (addr, shutdown_tx, handle) = serve_router(router()).await?;
    let sync = tokio::spawn(start_sync_with_signal(
        Arc::clone(&broker),
        format!("http://{addr}"),
        Duration::from_millis(20),
        None,
        None,
    ));

    // The first pass deletes the old stream and creates the new one.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while broker.stream_incarnation("t1", "ns", "orders") != Some(6) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the delete was never applied"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for value in ["a", "b", "c"] {
        broker
            .publish(
                "t1",
                "ns",
                "orders",
                Bytes::copy_from_slice(value.as_bytes()),
            )
            .await?;
    }
    // Many more passes, each failing on the last change.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        tail(&broker).await,
        Some(3),
        "a later pass deleted the new stream"
    );

    sync.abort();
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    Ok(())
}

/// A delete that names no stream, from before the stream had an incarnation,
/// delivered after the name was created again with one.
fn legacy_delete(since: u64) -> StreamChangesResponse {
    StreamChangesResponse {
        items: [StreamChange {
            seq: Some(1),
            op: StreamChangeOp::Deleted,
            key: key(),
            stream: None,
            deleted_through: None,
        }]
        .into_iter()
        .filter(|change| change.seq.is_some_and(|seq| seq >= since))
        .collect(),
        next_seq: 2,
    }
}

/// A delete without a payload removes only a stream from before
/// incarnations: one that names none cannot be the delete of a stream that
/// has one, so it never removes the stream created since.
#[tokio::test]
async fn a_delete_naming_no_incarnation_keeps_an_incarnated_stream() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    let recreated = || StreamSnapshotResponse {
        items: vec![Stream {
            incarnation: 7,
            ..stream(None)
        }],
        next_seq: 1,
        tombstones: Vec::new(),
    };

    let (addr, shutdown_tx, handle) = serve_router(router_with(recreated, legacy_delete)).await?;
    let sync = tokio::spawn(start_sync_with_signal(
        Arc::clone(&broker),
        format!("http://{addr}"),
        Duration::from_millis(20),
        None,
        None,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while broker.stream_incarnation("t1", "ns", "orders").is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream was never registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Every pass delivers the delete again.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        broker.stream_incarnation("t1", "ns", "orders"),
        Some(7),
        "a delete naming no incarnation removed the stream created since"
    );

    sync.abort();
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    Ok(())
}

/// The create of an older incarnation, served again by a control-plane
/// member behind the one the snapshot came from: it has not applied the
/// delete and the create that followed.
fn lagging_member(_since: u64) -> StreamChangesResponse {
    StreamChangesResponse {
        items: vec![StreamChange {
            seq: Some(3),
            op: StreamChangeOp::Created,
            key: key(),
            stream: Some(Stream {
                incarnation: 7,
                ..stream(None)
            }),
            deleted_through: None,
        }],
        next_seq: 4,
    }
}

fn live() -> StreamSnapshotResponse {
    StreamSnapshotResponse {
        items: vec![Stream {
            incarnation: 8,
            ..stream(None)
        }],
        next_seq: 10,
        tombstones: Vec::new(),
    }
}

/// An older incarnation's create delivered again neither replaces the live
/// stream nor takes its data.
#[tokio::test]
async fn a_replayed_create_of_an_older_incarnation_keeps_the_live_stream() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    let mut state = SyncState::new();
    let client = reqwest::Client::new();

    let (addr, shutdown_tx, handle) = serve_router(router_with(live, |_| StreamChangesResponse {
        items: vec![],
        next_seq: 10,
    }))
    .await?;
    sync_into(
        &broker,
        &client,
        &format!("http://{addr}"),
        None,
        &mut state,
    )
    .await?;
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    for _ in 0..2 {
        broker
            .publish("t1", "ns", "orders", Bytes::from_static(b"live"))
            .await?;
    }

    // The next pass reads a member that is behind.
    state.next_stream_seq = 3;
    let (addr, shutdown_tx, handle) = serve_router(router_with(live, lagging_member)).await?;
    sync_into(
        &broker,
        &client,
        &format!("http://{addr}"),
        None,
        &mut state,
    )
    .await?;
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    assert_eq!(broker.stream_incarnation("t1", "ns", "orders"), Some(8));
    assert_eq!(tail(&broker).await, Some(2), "the live stream's data went");
    Ok(())
}

/// A broker that was down while the stream was deleted and created again
/// learns of the delete from the snapshot's tombstone, and deletes the old
/// incarnation's data rather than keeping it for good.
#[tokio::test]
async fn a_snapshot_tombstone_deletes_a_stream_missed_while_down() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config)?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "ns").await?;
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation: 5,
                ..Default::default()
            },
        )
        .await?;
    broker
        .publish("t1", "ns", "orders", Bytes::from_static(b"old"))
        .await?;
    let old = felix_storage::disk_log::layout::incarnation_dir(
        dir.path(),
        &felix_storage::log::ShardKey {
            tenant: "t1".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
            shard: 0,
        },
        5,
    );
    assert!(old.exists());

    let recreated = || StreamSnapshotResponse {
        items: vec![Stream {
            incarnation: 9,
            ..stream(None)
        }],
        next_seq: 12,
        tombstones: vec![StreamTombstone {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            incarnation: 5,
        }],
    };
    let (addr, shutdown_tx, handle) =
        serve_router(router_with(recreated, |_| StreamChangesResponse {
            items: vec![],
            next_seq: 12,
        }))
        .await?;
    let mut state = SyncState::new();
    sync_into(
        &broker,
        &reqwest::Client::new(),
        &format!("http://{addr}"),
        None,
        &mut state,
    )
    .await?;
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    assert_eq!(broker.stream_incarnation("t1", "ns", "orders"), Some(9));
    assert!(!old.exists(), "the deleted incarnation's data was kept");
    assert_eq!(tail(&broker).await, Some(0));
    Ok(())
}
