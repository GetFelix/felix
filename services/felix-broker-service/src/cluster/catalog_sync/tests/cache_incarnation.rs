//! A cache deleted and created again under the same name, as the catalog
//! sync applies it: the old incarnation's data goes only on the control
//! plane's word that it is deleted.

use std::collections::HashMap;

use axum::extract::Query;
use bytes::Bytes;
use felix_broker::Broker;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn cache(incarnation: u64) -> Cache {
    Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        consistency: None,
        incarnation,
    }
}

fn key() -> CacheKey {
    CacheKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
    }
}

/// The cache deleted, with what the delete names, then created again at
/// `recreated`.
fn delete_then_create(
    since: u64,
    payload: Option<Cache>,
    deleted_through: Option<u64>,
    recreated: u64,
) -> CacheChangesResponse {
    let items = vec![
        CacheChange {
            seq: Some(1),
            op: CacheChangeOp::Deleted,
            key: key(),
            cache: payload,
            deleted_through,
        },
        CacheChange {
            seq: Some(2),
            op: CacheChangeOp::Created,
            key: key(),
            cache: Some(cache(recreated)),
            deleted_through: None,
        },
    ];
    CacheChangesResponse {
        items: items
            .into_iter()
            .filter(|change| change.seq.is_some_and(|seq| seq >= since))
            .collect(),
        next_seq: 3,
    }
}

fn router_with(
    snapshot: fn() -> CacheSnapshotResponse,
    feed: fn(u64) -> CacheChangesResponse,
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
            get(move || {
                let snapshot = snapshot();
                async move { Json(snapshot) }
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
            get(
                move |Query(query): Query<HashMap<String, u64>>| async move {
                    Json(feed(query.get("since").copied().unwrap_or(0)))
                },
            ),
        )
        .route(
            "/v1/streams/changes",
            get(|| async {
                Json(StreamChangesResponse {
                    items: vec![],
                    next_seq: 1,
                })
            }),
        )
}

/// A broker with a log-backed cache and counters, holding one entry of the
/// cache at `incarnation`.
async fn broker(dir: &std::path::Path, incarnation: u64) -> Result<Arc<Broker>> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.join("streams"), config.clone())?;
    let counters = Arc::new(felix_storage::CounterStore::open(
        dir.join("counters"),
        config.clone(),
    )?);
    let broker = Arc::new(
        Broker::new(Box::new(felix_storage::LogCache::open(
            dir.join("caches"),
            config,
        )?))
        .with_durable_storage(storage)
        .with_counters(counters),
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "ns").await?;
    broker
        .register_cache(
            "t1",
            "ns",
            "sessions",
            CacheMetadata {
                incarnation,
                ..Default::default()
            },
        )
        .await?;
    broker
        .cache()
        .put(
            "t1",
            "ns",
            "sessions",
            0,
            "k",
            Bytes::from_static(b"old"),
            None,
        )
        .await?;
    Ok(broker)
}

fn incarnation_dir(dir: &std::path::Path, incarnation: u64) -> std::path::PathBuf {
    felix_storage::disk_log::layout::incarnation_dir(
        &dir.join("caches"),
        &felix_storage::log::ShardKey {
            tenant: "t1".into(),
            namespace: "ns".into(),
            stream: "sessions".into(),
            shard: 0,
        },
        incarnation,
    )
}

async fn sync(broker: &Arc<Broker>, router: Router) -> Result<()> {
    let (addr, shutdown_tx, handle) = serve_router(router).await?;
    let mut state = SyncState::new();
    let synced = sync_into(
        broker,
        &reqwest::Client::new(),
        &format!("http://{addr}"),
        None,
        &mut state,
    )
    .await;
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    synced
}

async fn value(broker: &Broker) -> Result<Option<Bytes>> {
    Ok(broker.cache().get("t1", "ns", "sessions", 0, "k").await?)
}

/// A broker that was down while the cache was deleted and created again
/// learns of the delete from the snapshot's tombstone, and deletes the old
/// incarnation's data rather than keeping it for good.
#[tokio::test]
async fn a_snapshot_tombstone_deletes_a_cache_missed_while_down() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let broker = broker(dir.path(), 5).await?;
    let old = incarnation_dir(dir.path(), 5);
    assert!(old.exists());

    let recreated = || CacheSnapshotResponse {
        items: vec![cache(9)],
        next_seq: 12,
        tombstones: vec![CacheTombstone {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: "sessions".to_string(),
            incarnation: 5,
        }],
    };
    sync(
        &broker,
        router_with(recreated, |_| CacheChangesResponse {
            items: vec![],
            next_seq: 12,
        }),
    )
    .await?;

    assert_eq!(broker.cache_incarnation("t1", "ns", "sessions"), Some(9));
    assert!(!old.exists(), "the deleted incarnation's data was kept");
    assert_eq!(value(&broker).await?, None);
    Ok(())
}

fn at_five() -> CacheSnapshotResponse {
    CacheSnapshotResponse {
        items: vec![cache(5)],
        next_seq: 1,
        tombstones: Vec::new(),
    }
}

/// A delete naming the incarnation it deletes takes that incarnation's data,
/// and the cache created again starts empty.
#[tokio::test]
async fn a_delete_naming_its_incarnation_deletes_the_caches_data() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let broker = broker(dir.path(), 5).await?;
    let old = incarnation_dir(dir.path(), 5);

    sync(
        &broker,
        router_with(at_five, |since| {
            delete_then_create(since, Some(cache(5)), Some(5), 6)
        }),
    )
    .await?;

    assert_eq!(broker.cache_incarnation("t1", "ns", "sessions"), Some(6));
    assert!(!old.exists(), "the deleted incarnation's data was kept");
    assert_eq!(value(&broker).await?, None);
    Ok(())
}

/// A delete with only its payload's incarnation, from a control plane that
/// sends no `deleted_through`, deletes that incarnation all the same.
#[tokio::test]
async fn a_delete_whose_payload_names_its_incarnation_deletes_the_caches_data() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let broker = broker(dir.path(), 5).await?;
    let old = incarnation_dir(dir.path(), 5);

    sync(
        &broker,
        router_with(at_five, |since| {
            delete_then_create(since, Some(cache(5)), None, 6)
        }),
    )
    .await?;

    assert_eq!(broker.cache_incarnation("t1", "ns", "sessions"), Some(6));
    assert!(!old.exists(), "the deleted incarnation's data was kept");
    Ok(())
}

/// Before the fleet finalizes cache incarnations a delete names nothing, and
/// keeps the cache's data for the name created again at 0, as a delete always
/// did.
#[tokio::test]
async fn a_delete_naming_no_incarnation_keeps_the_data_for_a_recreate_at_zero() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let broker = broker(dir.path(), 0).await?;

    sync(
        &broker,
        router_with(
            || CacheSnapshotResponse {
                items: vec![cache(0)],
                next_seq: 1,
                tombstones: Vec::new(),
            },
            |since| delete_then_create(since, None, None, 0),
        ),
    )
    .await?;

    assert_eq!(broker.cache_incarnation("t1", "ns", "sessions"), Some(0));
    assert_eq!(value(&broker).await?, Some(Bytes::from_static(b"old")));
    Ok(())
}
