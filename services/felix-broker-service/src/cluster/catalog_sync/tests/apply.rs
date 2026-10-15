//! Applying a catalog change to the broker.

use super::*;

#[tokio::test]
async fn apply_namespace_create_registers_missing_tenant() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_namespace_create(&broker, "t1".to_string(), "ns1".to_string()).await?;
    assert!(broker.namespace_exists("t1", "ns1").await);
    Ok(())
}

#[tokio::test]
async fn apply_cache_upsert_recovers_from_missing_parents() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_cache_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "c1".to_string(),
        CacheMetadata::default(),
    )
    .await?;
    assert!(broker.cache_exists("t1", "ns1", "c1").await);

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2".to_string()).await?;
    apply_cache_upsert(
        &broker,
        "t2".to_string(),
        "ns2".to_string(),
        "c2".to_string(),
        CacheMetadata::default(),
    )
    .await?;
    assert!(broker.cache_exists("t2", "ns2", "c2").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_recovers_from_missing_parents() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: false,
            shards: 2,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "orders").await);

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2".to_string()).await?;
    apply_stream_upsert(
        &broker,
        "t2".to_string(),
        "ns2".to_string(),
        "events".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t2", "ns2", "events").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_skips_durable_streams_without_storage() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));

    // A durable stream on a broker with no durable storage must not be
    // registered: a registered stream accepts publishes, and those would be
    // acknowledged against a guarantee that does not exist.
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(!broker.stream_exists("t1", "ns1", "orders").await);

    // The sync must keep going, so a later non-durable stream still applies.
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "telemetry".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "telemetry").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_registers_durable_streams_when_storage_exists() -> Result<()> {
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

    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "orders").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_propagates_storage_failures() -> Result<()> {
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

    // Corrupt the shard directory so opening the log fails. A file where
    // the segment directory belongs is the simplest way to make recovery
    // return an I/O error rather than a clean empty log.
    let shard_dir = felix_storage::disk_log::layout::shard_dir(
        dir.path(),
        &felix_storage::log::ShardKey {
            tenant: "t1".into(),
            namespace: "ns1".into(),
            stream: "orders".into(),
            shard: 0,
        },
    );
    std::fs::write(&shard_dir, b"not a directory").expect("block the shard dir");

    let err = apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await
    .expect_err("storage failure must not be skipped");

    // The sync fails rather than advancing its cursor past a durable stream
    // it could not create. Skipping here would leave the broker ready with
    // the stream permanently missing.
    assert!(err.to_string().contains("durable storage error"), "{err}");
    assert!(!broker.stream_exists("t1", "ns1", "orders").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_rejects_live_durability_changes() -> Result<()> {
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

    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    let err = apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await
    .expect_err("live durability change");

    assert!(
        err.to_string().contains("requires removal and recreation"),
        "{err}"
    );
    assert!(broker.stream_exists("t1", "ns1", "orders").await);
    assert_eq!(
        std::fs::read_dir(dir.path())
            .expect("read storage root")
            .filter_map(|entry| entry.ok())
            .count(),
        0,
        "a rejected control-plane update must not create durable state"
    );
    Ok(())
}
