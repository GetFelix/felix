//! An in-memory stream's shard keeps nothing on disk, however often it is
//! placed: a log opened for it would be found by the next owner and the
//! stream served as durable (#3136).

use felix_storage::log::{FsyncMode, LogConfig};

use super::*;

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// A broker with durable storage and consumer groups under `dir`, and no
/// stream yet.
async fn empty_broker(dir: &std::path::Path) -> std::sync::Arc<felix_broker::Broker> {
    let storage =
        felix_broker::DurableStorage::open(dir.join("streams"), config()).expect("storage");
    let broker = felix_broker::Broker::new(Box::new(
        felix_storage::LogCache::open(dir.join("caches"), config()).expect("cache"),
    ))
    .with_durable_storage(storage)
    .with_consumer_groups(
        std::sync::Arc::new(
            felix_broker::ConsumerGroups::open(dir.join("groups"), config()).expect("groups"),
        ),
        std::sync::Arc::new(
            felix_broker::DeadLetters::open(dir.join("dead-letters"), config())
                .expect("dead letters"),
        ),
        std::time::Duration::from_secs(30),
        3,
    );
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    std::sync::Arc::new(broker)
}

/// [`empty_broker`] with `orders` registered on two shards. Registering
/// opens a durable stream's shard 0 and leaves shard 1 to its placement.
async fn broker(dir: &std::path::Path, durable: bool) -> std::sync::Arc<felix_broker::Broker> {
    let broker = empty_broker(dir).await;
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable,
                shards: 2,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    broker
}

fn storage(broker: &felix_broker::Broker) -> std::sync::Arc<felix_broker::DurableStorage> {
    std::sync::Arc::new(broker.durable_storage().expect("storage").clone())
}

fn store(broker: &std::sync::Arc<felix_broker::Broker>) -> DurableShardStore {
    DurableShardStore::new(storage(broker))
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

async fn place(store: &DurableShardStore, shard: u32, generation: u64) {
    store
        .may_lead(&key(shard), generation, 0, true)
        .await
        .expect("may lead");
    store
        .open(&key(shard), generation, 0, true)
        .await
        .expect("open");
}

async fn place_and_release(store: &DurableShardStore, generation: u64) {
    place(store, 0, generation).await;
    store.release(&key(0)).await.expect("release");
}

/// That placing `shard` at generation 3 made its log: it was not on disk
/// before, and afterwards it holds the generation's term start and has
/// accepted the generation.
async fn placement_creates_the_log(
    store: &DurableShardStore,
    storage: &felix_broker::DurableStorage,
    shard: u32,
) {
    assert!(
        !storage.stream_on_disk("t1", "ns", "orders", shard),
        "the log was there before the placement, so the placement proves nothing"
    );
    place(store, shard, 3).await;
    assert!(
        storage.stream_on_disk("t1", "ns", "orders", shard),
        "the placement left no log for a durable shard"
    );
    let log = storage
        .opened_stream("t1", "ns", "orders", shard)
        .expect("the placement left the log open");
    assert_eq!(log.accepted_generation(), 3, "generation not accepted");
    assert!(
        log.generations()
            .iter()
            .any(|epoch| epoch.generation == 3 && epoch.start_offset == 0),
        "no term start for the generation: {:?}",
        log.generations()
    );
    store.release(&key(shard)).await.expect("release");
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

/// Shard 1, which only its placement opens: registering the stream opens
/// shard 0 itself, so shard 0's log says nothing about the lifecycle.
#[tokio::test]
async fn a_durable_shard_gets_its_log_from_the_placement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), true).await;
    placement_creates_the_log(&store(&broker), &storage(&broker), 1).await;
}

/// A stream this broker has not registered yet may be durable, so its shard
/// gets everything a durable one does.
#[tokio::test]
async fn an_unregistered_stream_counts_as_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = empty_broker(dir.path()).await;
    placement_creates_the_log(&store(&broker), &storage(&broker), 0).await;
}

/// A store with no broker cannot ask what the stream is, so it treats every
/// shard as durable, even one of a stream registered in memory elsewhere.
#[tokio::test]
async fn a_store_without_a_broker_counts_every_shard_as_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), false).await;
    let storage = storage(&broker);
    let store = DurableShardStore::new(std::sync::Arc::clone(&storage)).with_ballots("broker-a");
    placement_creates_the_log(&store, &storage, 0).await;
}

/// The stream's delete can reach this broker before the shard's release
/// does. By then nothing says the stream was in memory, and the release must
/// still create nothing.
#[tokio::test]
async fn a_release_after_the_delete_creates_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), false).await;
    let store = store(&broker);
    place(&store, 0, 1).await;
    assert!(
        broker
            .remove_stream("t1", "ns", "orders")
            .await
            .expect("remove"),
        "nothing removed"
    );
    store.release(&key(0)).await.expect("release");
    assert_eq!(stream_paths(dir.path()), Vec::<std::path::PathBuf>::new());
}

/// **A fenced promotion of an in-memory shard opens it with nothing on
/// disk.** Wherever replication offers the fence every new leadership opens
/// through the gate, not the store, and the gate wrote term starts to the
/// stream log and both group logs.
#[tokio::test]
async fn a_fenced_in_memory_shard_opens_with_nothing_on_disk() {
    use felix_replication::promotion::PromotionGate;

    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path(), false).await;
    let mut own = lifecycle();
    own.fence_promotions();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);
    let router = std::sync::Arc::new(felix_router::ShardRouter::new(
        "broker-a",
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = std::sync::Arc::new(crate::shards::routing::IngressRouter::new(
        router,
        std::sync::Arc::clone(own.fence()),
    ));
    let finalized = [felix_common::fleet::GENERATION_START.name()];
    let fleet = felix_common::fleet::FleetGate::new(finalized);
    fleet.observe(finalized);
    let own = std::sync::Arc::new(tokio::sync::Mutex::new(own));
    let gate = crate::shards::lifecycle::promotion::LifecycleGate::new(
        std::sync::Arc::clone(&own),
        ingress,
        storage(&broker),
        std::sync::Arc::clone(&broker),
        std::sync::Arc::new(fleet),
    );

    assert!(gate.open(&key(0), 3).await, "the shard stayed closed");

    assert_eq!(own.lock().await.phase(&key(0)), Phase::Active);
    assert_eq!(stream_paths(dir.path()), Vec::<std::path::PathBuf>::new());
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("stream");
    assert!(!handle.is_durable(), "served as durable after the fence");
}
