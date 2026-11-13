use std::path::Path;

use felix_broker::StreamMetadata;
use felix_replication::raft::{AgreeingPeers, Role, RuntimeConfig};

use super::*;

const INCARNATION: u64 = 3;

fn key(stream: &str, shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

fn assigned(key: ShardKey, incarnation: u64) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: "broker-a".to_string(),
        replicas: vec!["broker-a".to_string()],
        generation: 1,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
    }
}

fn assignments(
    all: impl IntoIterator<Item = ShardAssignment>,
) -> HashMap<ShardKey, ShardAssignment> {
    let mut map = HashMap::default();
    for assignment in all {
        map.insert(assignment.key.clone(), assignment);
    }
    map
}

/// A broker that leaves durable streams to their engines, with the durable
/// stream `orders` at [`INCARNATION`] and the in-memory stream `scratch`.
async fn broker(root: &Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(root, config).expect("storage");
    let broker =
        Broker::new(felix_storage::EphemeralCache::new().into()).with_durable_storage(storage);
    broker.require_registered_streams();
    broker.leave_durable_streams_to_engines();
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation: INCARNATION,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("orders");
    broker
        .register_stream(
            "t1",
            "ns",
            "scratch",
            StreamMetadata {
                durable: false,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("scratch");
    Arc::new(broker)
}

fn shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let timers = RuntimeConfig {
        election_min: Duration::from_millis(150),
        election_max: Duration::from_millis(300),
        heartbeat: Duration::from_millis(50),
        tick: Duration::from_millis(10),
        ..RuntimeConfig::default()
    };
    let runtime =
        GroupRuntime::start("broker-a", timers, Arc::new(AgreeingPeers)).expect("runtime");
    RaftShards::new(Arc::clone(broker), runtime, "broker-a".to_string())
}

async fn until(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **A durable stream shard is taken from the hybrid and opened as a Raft
/// member on its incarnation's directory**, which the storage provider then
/// refuses to open: one shard never has two engines on its log.
#[tokio::test]
async fn a_durable_stream_shard_opens_as_a_raft_member() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut cache = key("orders", 0);
    cache.kind = ShardKind::Cache;
    let mut left = assignments([
        assigned(orders.clone(), INCARNATION),
        assigned(key("scratch", 0), 0),
        assigned(key("unknown", 0), 0),
        assigned(cache.clone(), 0),
    ]);

    shards.take(&mut left).await;

    // The in-memory stream and the cache are the hybrid's. The stream not
    // registered yet is nobody's this pass.
    let mut kept: Vec<_> = left.into_keys().collect();
    kept.sort();
    let mut expected = vec![key("scratch", 0), cache];
    expected.sort();
    assert_eq!(kept, expected);
    assert!(shards.engine_for(&key("unknown", 0)).is_none());
    assert!(shards.engine_for(&key("scratch", 0)).is_none());

    until("the member to lead its group of one", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
    assert_eq!(shards.leading(&orders), shards.engine.open_term(&orders));
    assert!(shards.engine_for(&orders).is_some());

    let storage = broker.durable_storage().expect("storage");
    assert!(
        storage
            .open_stream_for("t1", "ns", "orders", 0, None, INCARNATION)
            .is_err(),
        "the provider opened a Raft member's directory"
    );
    assert!(
        broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .is_ok(),
        "the stream is not open on the member's log"
    );
    shards.runtime.shutdown().await;
}

/// **A deleted stream's Raft member is stopped and its log closed before
/// the directory is retired.** The provider never opened that log, so its
/// own close misses it, and the member would go on writing deleted files.
#[tokio::test]
async fn a_deleted_stream_closes_its_raft_log_before_the_directory_goes() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    assert!(!log.is_closed());

    let removed = broker
        .remove_stream_from_catalog("t1", "ns", "orders", Some(INCARNATION))
        .await
        .expect("delete");

    assert!(removed);
    assert!(log.is_closed(), "the member's log outlived its directory");
    assert!(shards.runtime.status(&orders).is_none());
    assert!(shards.log_of(&orders).await.is_none());
    // The assignment is still in the feed: nothing is opened for a stream
    // that is gone.
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.runtime.status(&orders).is_none());
    shards.runtime.shutdown().await;
}

/// **A shard no longer assigned here is closed, and opens again from its
/// log when it comes back.**
#[tokio::test]
async fn a_shard_no_longer_assigned_is_closed_and_reopens() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;

    // Assigned to another broker only.
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    let mut left = assignments([elsewhere]);
    shards.take(&mut left).await;

    assert!(
        left.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(log.is_closed());
    assert!(shards.runtime.status(&orders).is_none());
    assert_eq!(shards.leading(&orders), None);

    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.runtime.status(&orders).is_some());
    assert!(
        broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .is_ok(),
        "the stream did not reopen on the member's log"
    );
    shards.runtime.shutdown().await;
}
