//! Entries registered from the control-plane view before the first snapshot:
//! the snapshot keeps those it lists and unregisters the rest, and deletes
//! nothing on absence. A control plane restored from an older backup lacks
//! streams created after it, which the brokers that stayed up still hold.

use std::collections::BTreeSet;
use std::path::Path;

use super::*;
use crate::cluster::controlplane_view::Catalog;

fn stream(name: &str) -> Stream {
    serde_json::from_value(serde_json::json!({
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": name,
        "shards": 1,
        "durable": true,
        "incarnation": 2,
    }))
    .expect("stream")
}

fn cache(name: &str) -> Cache {
    serde_json::from_value(serde_json::json!({
        "tenant_id": "t1",
        "namespace": "ns",
        "cache": name,
        "incarnation": 2,
    }))
    .expect("cache")
}

/// Every path under `root`, so a test can tell nothing was deleted.
fn tree(root: &Path) -> BTreeSet<std::path::PathBuf> {
    let mut paths = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            paths.insert(path);
        }
    }
    paths
}

/// A control plane whose snapshots list only `kept` and `kept-cache`.
fn restored_control_plane() -> Router {
    let empty = || async { Json(serde_json::json!({ "items": [], "next_seq": 1 })) };
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
            get(|| async {
                Json(serde_json::json!({ "items": [cache("kept-cache")], "next_seq": 1 }))
            }),
        )
        .route(
            "/v1/streams/snapshot",
            get(|| async { Json(serde_json::json!({ "items": [stream("kept")], "next_seq": 1 })) }),
        )
        .route("/v1/tenants/changes", get(empty))
        .route("/v1/namespaces/changes", get(empty))
        .route("/v1/caches/changes", get(empty))
        .route("/v1/streams/changes", get(empty))
}

#[tokio::test]
async fn a_snapshot_unregisters_what_it_lacks_and_deletes_nothing() -> Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    let view = Catalog {
        streams: vec![stream("kept"), stream("gone")],
        caches: vec![cache("kept-cache"), cache("gone-cache")],
    };
    let provisional = seed_provisional(&broker, &view).await;
    assert_eq!(provisional.streams.len(), 2);
    assert_eq!(provisional.caches.len(), 2);
    assert!(broker.stream_exists("t1", "ns", "gone").await);
    let before = tree(dir.path());

    let (addr, stop, handle) = serve_router(restored_control_plane()).await?;
    let mut state = SyncState::new();
    state.provisional = provisional;
    let state = sync_once(
        &broker,
        &build_test_client()?,
        &format!("http://{addr}"),
        None,
        state,
    )
    .await?;
    let _ = stop.send(());
    let _ = handle.await;

    assert!(state.is_seeded());
    assert_eq!(state.provisional, Provisional::default());
    assert!(broker.stream_exists("t1", "ns", "kept").await);
    assert!(!broker.stream_exists("t1", "ns", "gone").await);
    assert!(
        broker
            .cache_metadata("t1", "ns", "kept-cache")
            .await
            .is_some(),
        "a listed cache stays"
    );
    assert!(
        broker
            .cache_metadata("t1", "ns", "gone-cache")
            .await
            .is_none(),
        "an unlisted cache is unregistered"
    );
    let after = tree(dir.path());
    let deleted: Vec<_> = before.difference(&after).collect();
    assert!(deleted.is_empty(), "absence deleted {deleted:?}");
    Ok(())
}

/// A broker that did not start from a view registers nothing before its
/// first snapshot, so there is nothing to unregister.
#[tokio::test]
async fn without_provisional_entries_a_snapshot_only_upserts() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns".to_string(),
        "local".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    let (addr, stop, handle) = serve_router(restored_control_plane()).await?;
    sync_once(
        &broker,
        &build_test_client()?,
        &format!("http://{addr}"),
        None,
        SyncState::new(),
    )
    .await?;
    let _ = stop.send(());
    let _ = handle.await;
    assert!(broker.stream_exists("t1", "ns", "local").await);
    Ok(())
}
