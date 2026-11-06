//! A broker that leads a `Quorum` shard again after dropping what it held
//! answers state reads with the dropped commit once the mark covers it, even
//! when the fleet writes no generation-start record, on either way a
//! leadership opens.
use std::sync::Arc;

use bytes::Bytes;
use felix_replication::promotion::PromotionGate;
use felix_storage::log::{FsyncMode, LogConfig};
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
struct Bound(Mutex<felix_broker::ReadBound>);

impl felix_broker::ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> felix_broker::ReadBound {
        *self.0.lock()
    }
}

async fn commit(broker: &felix_broker::Broker, value: &'static [u8]) {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let ops = vec![felix_broker::StateOp::Put {
        key: "state".to_string(),
        value: Bytes::from_static(value),
    }];
    broker
        .commit_to_handle(&handle, Bytes::from_static(value), ops, None)
        .await
        .expect("commit");
}

async fn version(broker: &felix_broker::Broker) -> Option<u64> {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    broker
        .state_get(&handle, "state")
        .await
        .expect("state read")
        .version
}

/// A broker whose `Quorum` stream held a commit past the mark and dropped
/// it when it gave the shard up. `GENERATION_START` is not finalized, as in a
/// rolling upgrade, so leading again writes no start record.
async fn dropped_hold(dir: &std::path::Path) -> (Arc<felix_broker::Broker>, Arc<Bound>) {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir, config).expect("storage");
    let broker = Arc::new(
        felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
            .with_durable_storage(storage),
    );
    let bound = Arc::new(Bound(Mutex::new(felix_broker::ReadBound::Unbounded)));
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn felix_broker::ReadBounds>);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("register");

    commit(&broker, b"first").await;
    assert_eq!(version(&broker).await, Some(0));
    *bound.0.lock() = felix_broker::ReadBound::Committed(1);
    commit(&broker, b"held").await;
    assert_eq!(broker.discard_uncommitted("t1", "ns", "orders", 0).await, 1);
    (broker, bound)
}

fn no_start_records() -> Arc<felix_common::fleet::FleetGate> {
    Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new()))
}

/// Opened as leader without a fence.
#[tokio::test]
async fn opening_as_leader_without_a_start_record_rebuilds_the_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bound) = dropped_hold(dir.path()).await;
    let store =
        DurableShardStore::new(Arc::new(broker.durable_storage().expect("storage").clone()))
            .with_generation_starts(GenerationStarts {
                broker: Arc::clone(&broker),
                fleet: no_start_records(),
            });

    store.open(&key(0), 2, 0, true).await.expect("open");
    *bound.0.lock() = felix_broker::ReadBound::Unbounded;
    assert_eq!(version(&broker).await, Some(1));
}

/// Opened by the gate once its fence is done.
#[tokio::test]
async fn opening_after_a_fence_without_a_start_record_rebuilds_the_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bound) = dropped_hold(dir.path()).await;

    let mut own = lifecycle();
    own.fence_promotions();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);
    let router = Arc::new(felix_router::ShardRouter::new(
        "broker-a",
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = Arc::new(crate::shards::routing::IngressRouter::new(
        router,
        Arc::clone(own.fence()),
    ));
    let gate = crate::shards::lifecycle::promotion::LifecycleGate::new(
        Arc::new(tokio::sync::Mutex::new(own)),
        ingress,
        Arc::new(broker.durable_storage().expect("storage").clone()),
        Arc::clone(&broker),
        no_start_records(),
    );

    assert!(gate.open(&key(0), 3).await, "the shard stayed closed");
    *bound.0.lock() = felix_broker::ReadBound::Unbounded;
    assert_eq!(version(&broker).await, Some(1));
}

/// **A release stuck fanning out does not stall a promotion, and the shard
/// does not serve before it ends.** Under `Block` a released batch waits on a
/// full subscriber for as long as it stays full. The gate gives up on leading
/// again after a short wait, keeps the shard closed, and opens it on a later
/// pass once the fanout is through.
#[tokio::test]
async fn a_stuck_release_keeps_the_shard_closed_without_stalling_the_promotion() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path(), config).expect("storage");
    let broker = Arc::new(
        felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
            .with_topic_capacity(1)
            .expect("capacity")
            .with_subscriber_queue_policy(felix_broker::SubQueuePolicy::Block)
            .with_durable_storage(storage),
    );
    let bound = Arc::new(Bound(Mutex::new(felix_broker::ReadBound::Committed(0))));
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn felix_broker::ReadBounds>);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    let slow = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    for payload in [&b"a"[..], b"b", b"c"] {
        broker
            .publish_batch_with_outcome(&handle, &[Bytes::copy_from_slice(payload)], None, None)
            .await
            .expect("held publish");
    }
    // One pass takes all three; the second then waits on the full queue.
    *bound.0.lock() = felix_broker::ReadBound::Unbounded;
    handle.release_committed();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        broker.discard_uncommitted("t1", "ns", "orders", 0).await,
        0,
        "the release took the batches"
    );

    let mut own = lifecycle();
    own.fence_promotions();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);
    let router = Arc::new(felix_router::ShardRouter::new(
        "broker-a",
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = Arc::new(crate::shards::routing::IngressRouter::new(
        router,
        Arc::clone(own.fence()),
    ));
    let gate = crate::shards::lifecycle::promotion::LifecycleGate::new(
        Arc::new(tokio::sync::Mutex::new(own)),
        Arc::clone(&ingress),
        Arc::new(broker.durable_storage().expect("storage").clone()),
        Arc::clone(&broker),
        no_start_records(),
    );

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), gate.open(&key(0), 3))
        .await
        .expect("the promotion stalled on the release");
    assert!(!first, "opened while a release was still running");
    assert_eq!(
        gate.awaiting(&key(0)),
        Some(3),
        "the shard served before its release ended"
    );

    drop(slow);
    assert!(gate.open(&key(0), 3).await, "the shard stayed closed");
}
