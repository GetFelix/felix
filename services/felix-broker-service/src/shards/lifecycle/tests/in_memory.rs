//! An in-memory stream's shard keeps nothing on disk, however often it is
//! placed: a log opened for it would be found by the next owner and the
//! stream served as durable (#3136).

use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

async fn broker(dir: &std::path::Path, durable: bool) -> std::sync::Arc<felix_broker::Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage =
        felix_broker::DurableStorage::open(dir.join("streams"), config.clone()).expect("storage");
    let broker = felix_broker::Broker::new(Box::new(
        felix_storage::LogCache::open(dir.join("caches"), config).expect("cache"),
    ))
    .with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    std::sync::Arc::new(broker)
}

fn store(broker: &std::sync::Arc<felix_broker::Broker>) -> DurableShardStore {
    DurableShardStore::new(std::sync::Arc::new(
        broker.durable_storage().expect("storage").clone(),
    ))
    .with_ballots("broker-a")
    .with_generation_starts(GenerationStarts {
        broker: std::sync::Arc::clone(broker),
        fleet: std::sync::Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
    })
}

/// Every path under `dir` that names the stream.
fn stream_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.to_string_lossy().contains("orders") {
                found.push(path.clone());
            }
            if path.is_dir() {
                pending.push(path);
            }
        }
    }
    found
}

async fn place_and_release(store: &DurableShardStore, generation: u64) {
    store
        .may_lead(&key(0), generation, 0, true)
        .await
        .expect("may lead");
    store
        .open(&key(0), generation, 0, true)
        .await
        .expect("open");
    store.release(&key(0)).await.expect("release");
}

#[tokio::test]
async fn an_in_memory_shard_opens_and_releases_with_nothing_on_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), false).await;
    let store = store(&broker);
    place_and_release(&store, 1).await;
    place_and_release(&store, 2).await;
    assert_eq!(stream_paths(dir.path()), Vec::<std::path::PathBuf>::new());
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("stream");
    assert!(!handle.is_durable(), "served as durable after a placement");
}

#[tokio::test]
async fn a_durable_shard_still_opens_its_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), true).await;
    place_and_release(&store(&broker), 1).await;
    assert!(
        !stream_paths(dir.path()).is_empty(),
        "no log for a durable shard"
    );
}
