use std::sync::Arc;
use std::time::Duration;

use felix_common::random::HashMap;
use felix_replication::driver::WriteFence;
use felix_router::{RegionRouter, ShardRouter};
use felix_storage::log::{FsyncMode, LogConfig};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::cluster::lease::LeaseState;
use crate::shards::lifecycle::fence::Fenced;
use crate::shards::lifecycle::{EphemeralShardStore, ShardLifecycle};
use crate::shards::routing::{
    CatalogSource, Dispatch, FeedState, IngressRouter, Reason, routing_table_from, spawn_feed,
};

const LOCAL: &str = "broker-a";

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "ticks".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

fn assigned(leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(0),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        target: None,
        targets_withheld: false,
        raft_formed: None,
        raft_voters: None,
        raft_bases: None,
        raft_seed: None,
        placed: None,
    }
}

fn assignments(of: &[ShardAssignment]) -> HashMap<ShardKey, ShardAssignment> {
    of.iter().map(|a| (a.key.clone(), a.clone())).collect()
}

/// A broker with durable storage under `root`, as a Raft broker has, and the
/// stream registered in memory.
async fn broker(root: &std::path::Path) -> Arc<felix_broker::Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(root, config).expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "ticks",
            felix_broker::StreamMetadata {
                durable: false,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    Arc::new(broker)
}

fn memory(broker: &Arc<felix_broker::Broker>, fence: &Arc<ShardFence>) -> MemoryShards {
    MemoryShards::new(
        LOCAL,
        Arc::clone(broker),
        Arc::clone(fence),
        ShardReaders::new(Arc::clone(broker)),
    )
}

/// A fence holding a lease on paused time, renewed now.
fn leased() -> (Arc<ShardFence>, Arc<LeaseState>) {
    let fence = Arc::new(ShardFence::default());
    let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(4)));
    lease.renew();
    fence.bind_lease(Arc::clone(&lease));
    (fence, lease)
}

/// Every path under `root` that names the stream.
fn paths_naming_stream(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.to_string_lossy().contains("ticks") {
                found.push(path.clone());
            }
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    found
}

/// The next thing a subscriber is handed, or `None` once it ended.
async fn ended(receiver: &mut felix_broker::SubscriptionReceiver) -> bool {
    tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await
        .expect("the subscriber ended in time")
        .is_none()
}

/// **A shard serves at the generation it is assigned, with no fence round,
/// and nothing for it lands under the durable root.**
#[tokio::test(start_paused = true)]
async fn opens_at_the_assigned_generation_without_touching_disk() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = memory(&broker, &fence);

    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    let taken = memory.take(&mut fed);
    assert!(fed.is_empty(), "a stream shard was left to the lifecycle");
    memory.apply(&taken).await;

    assert_eq!(memory.servable().get(&key(0)), Some(&5));
    assert!(fence.admit(&key(0), 5).is_ok());
    assert_eq!(
        fence.admit(&key(0), 4).err(),
        Some(Fenced::NotServing),
        "a write admitted at an older generation got in",
    );
    // Publish and subscribe as a client would, then look at the disk.
    let handle = broker
        .resolve_stream_handle("t1", "ns", "ticks", 0)
        .await
        .expect("handle");
    broker
        .publish_batch_with_outcome(&handle, &[bytes::Bytes::from_static(b"tick")], None, None)
        .await
        .expect("publish");
    assert_eq!(
        paths_naming_stream(root.path()),
        Vec::<std::path::PathBuf>::new()
    );
}

/// **Routed at a generation the shard does not serve at, it is not served
/// here**: the routes and the servable generation must agree.
#[tokio::test(start_paused = true)]
async fn refuses_when_the_route_names_another_generation() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = Arc::new(memory(&broker, &fence));
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = IngressRouter::new(router, Arc::clone(&fence));
    ingress.set_memory(Arc::clone(&memory));

    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    let taken = memory.take(&mut fed);
    memory.apply(&taken).await;
    let nodes = HashMap::default();
    ingress.publish(
        routing_table_from(&taken, &nodes),
        &nodes,
        HashMap::default(),
    );
    assert_eq!(ingress.dispatch(&key(0)), Dispatch::Local { generation: 5 });

    // The routes moved on before this broker opened the new generation.
    let newer = assignments(&[assigned(LOCAL, 6)]);
    ingress.publish(
        routing_table_from(&newer, &nodes),
        &nodes,
        HashMap::default(),
    );
    assert_eq!(
        ingress.dispatch(&key(0)),
        Dispatch::Unavailable(Reason::NotReady),
        "served at generation 5 under routes at 6",
    );
}

/// **A broker whose lease has lapsed refuses every write, however recently it
/// was admitted**: the control plane may have placed the shard elsewhere.
#[tokio::test(start_paused = true)]
async fn refuses_after_the_lease_lapses() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, lease) = leased();
    let memory = memory(&broker, &fence);
    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    let taken = memory.take(&mut fed);
    memory.apply(&taken).await;
    assert!(fence.admit(&key(0), 5).is_ok());

    tokio::time::advance(lease.usable() + Duration::from_millis(1)).await;
    assert_eq!(
        fence.admit(&key(0), 5).err(),
        Some(Fenced::LeaseLapsed),
        "a cut-off owner took a write after its lease lapsed",
    );
}

/// **A lapse ends the readers of an in-memory shard, naming no owner and the
/// generation it served.**
#[tokio::test(start_paused = true)]
async fn a_lapse_ends_its_readers() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, lease) = leased();
    let memory = Arc::new(memory(&broker, &fence));
    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    let taken = memory.take(&mut fed);
    memory.apply(&taken).await;
    let (mut receiver, _guard) = broker
        .subscribe("t1", "ns", "ticks", 0)
        .await
        .expect("subscribe")
        .into_parts();
    let shutdown = CancellationToken::new();
    tokio::spawn(ShardReaders::new(Arc::clone(&broker)).watch_leadership(
        Arc::clone(&fence),
        None,
        Arc::new(tokio::sync::Mutex::new(ShardLifecycle::new(LOCAL))),
        Some(Arc::clone(&memory)),
        shutdown.clone(),
    ));
    tokio::task::yield_now().await;

    lease.surrender();
    assert!(ended(&mut receiver).await);
    let moved = receiver.moved().expect("told the shard left");
    assert_eq!(moved.to.node_id, None);
    assert_eq!(moved.to.generation, 5);
    shutdown.cancel();
}

/// **Placed elsewhere, the shard closes, and each reader is told where it
/// went and at which generation. Unassigned, it is told there is no owner.**
#[tokio::test(start_paused = true)]
async fn closing_ends_readers_with_the_generation() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = memory(&broker, &fence);
    let subscribe = || async {
        broker
            .subscribe("t1", "ns", "ticks", 0)
            .await
            .expect("subscribe")
            .into_parts()
    };

    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    memory.apply(&memory.take(&mut fed)).await;
    let (mut receiver, _guard) = subscribe().await;
    let mut fed = assignments(&[assigned("broker-b", 6)]);
    memory.apply(&memory.take(&mut fed)).await;
    assert!(ended(&mut receiver).await);
    let moved = receiver.moved().expect("told the shard moved");
    assert_eq!(moved.to.node_id.as_deref(), Some("broker-b"));
    assert_eq!(moved.to.generation, 6);
    assert!(memory.servable().is_empty());
    assert_eq!(fence.admit(&key(0), 5).err(), Some(Fenced::NotServing));

    let mut fed = assignments(&[assigned(LOCAL, 7)]);
    memory.apply(&memory.take(&mut fed)).await;
    let (mut receiver, _guard) = subscribe().await;
    memory.apply(&HashMap::default()).await;
    assert!(ended(&mut receiver).await);
    let moved = receiver.moved().expect("told the shard has no owner");
    assert_eq!(moved.to.node_id, None);
    assert_eq!(moved.to.generation, 7);
}

/// **The generation right after the one served carries on with what the
/// shard holds; one that skips a generation starts empty and tells its
/// readers**, since another broker may have owned the shard in between.
#[tokio::test(start_paused = true)]
async fn only_the_next_generation_keeps_its_readers() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = memory(&broker, &fence);
    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    memory.apply(&memory.take(&mut fed)).await;
    let (mut receiver, _guard) = broker
        .subscribe("t1", "ns", "ticks", 0)
        .await
        .expect("subscribe")
        .into_parts();

    let mut fed = assignments(&[assigned(LOCAL, 6)]);
    memory.apply(&memory.take(&mut fed)).await;
    assert_eq!(memory.servable().get(&key(0)), Some(&6));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "a reader ended at the next generation on the same owner",
    );

    let mut fed = assignments(&[assigned(LOCAL, 9)]);
    memory.apply(&memory.take(&mut fed)).await;
    assert!(ended(&mut receiver).await);
    assert_eq!(receiver.moved().expect("moved").to.generation, 9);
    assert_eq!(memory.servable().get(&key(0)), Some(&9));
}

/// **Only a shard of a stream registered in memory at the assignment's
/// incarnation is served.** A stream recreated durable, or not registered
/// yet, is routed and not served, and never handed to the lifecycle.
#[tokio::test(start_paused = true)]
async fn serves_only_a_registered_in_memory_incarnation() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = memory(&broker, &fence);
    let at = |incarnation| ShardAssignment {
        incarnation,
        ..assigned(LOCAL, 5)
    };
    // The catalog still has incarnation 0.
    let mut fed = assignments(&[at(3)]);
    memory.apply(&memory.take(&mut fed)).await;
    assert!(memory.servable().is_empty());

    broker
        .register_stream(
            "t1",
            "ns",
            "ticks",
            felix_broker::StreamMetadata {
                durable: true,
                shards: 1,
                incarnation: 3,
                ..Default::default()
            },
        )
        .await
        .expect("recreated durable");
    let mut fed = assignments(&[at(3)]);
    let taken = memory.take(&mut fed);
    assert!(fed.is_empty());
    memory.apply(&taken).await;
    assert!(
        memory.servable().is_empty(),
        "served a durable stream's shard"
    );
}

/// **The hybrid never sees a shard this module takes**: replication passes
/// skip it, a cache shard is left alone, and a shard no longer assigned is
/// walked again.
#[tokio::test]
async fn replication_skips_what_it_takes() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let fence = Arc::new(ShardFence::default());
    let memory = memory(&broker, &fence);
    let cache = ShardKey {
        kind: ShardKind::Cache,
        ..key(0)
    };
    let mut fed = assignments(&[assigned(LOCAL, 5)]);
    fed.insert(
        cache.clone(),
        ShardAssignment {
            key: cache.clone(),
            ..assigned(LOCAL, 5)
        },
    );
    memory.take(&mut fed);
    assert_eq!(fed.keys().collect::<Vec<_>>(), vec![&cache]);
    assert!(WriteFence::withheld(fence.as_ref(), &key(0), 5));
    assert!(!WriteFence::withheld(fence.as_ref(), &cache, 5));

    memory.take(&mut HashMap::default());
    assert!(!WriteFence::withheld(fence.as_ref(), &key(0), 5));
}

/// **Through the routing feed, an in-memory shard on a Raft broker serves at
/// once and the hybrid lifecycle never holds it**, though that lifecycle
/// fences every promotion.
#[tokio::test]
async fn the_feed_serves_it_without_the_lifecycle() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let mut lifecycle = ShardLifecycle::new(LOCAL);
    lifecycle.fence_promotions();
    let fence = Arc::clone(lifecycle.fence());
    let memory = Arc::new(memory(&broker, &fence));
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = Arc::new(IngressRouter::new(router, Arc::clone(&fence)));
    ingress.set_memory(Arc::clone(&memory));
    let lifecycle = Arc::new(tokio::sync::Mutex::new(lifecycle));
    let shutdown = CancellationToken::new();
    let feed = spawn_feed(
        FeedState {
            client_endpoints: None,
            ownership: Arc::new(tokio::sync::RwLock::new({
                let mut ownership = crate::shards::watch::ShardOwnership::default();
                ownership.reset(vec![assigned(LOCAL, 5)]);
                ownership
            })),
            lifecycle: Arc::clone(&lifecycle),
            store: Arc::new(EphemeralShardStore::default()),
            ingress: Arc::clone(&ingress),
            assignments_changed: Arc::default(),
            routes_changed: Arc::default(),
            joint_fence: None,
            prefetch: None,
            elections: None,
            nodes: HashMap::default(),
            engine_shards: None,
            memory: Some(Arc::clone(&memory)),
        },
        None::<CatalogSource<felix_replication::controlplane_link::HttpControlPlaneLink>>,
        Duration::from_millis(20),
        shutdown.clone(),
    );
    for _ in 0..200 {
        if ingress.dispatch(&key(0)) == (Dispatch::Local { generation: 5 }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(ingress.dispatch(&key(0)), Dispatch::Local { generation: 5 });
    assert_eq!(
        lifecycle.lock().await.phase(&key(0)),
        crate::shards::lifecycle::Phase::Unassigned,
        "the hybrid lifecycle opened an in-memory shard",
    );
    assert!(fence.awaiting_promotion(&key(0)).is_none());
    assert!(WriteFence::withheld(fence.as_ref(), &key(0), 5));
    assert_eq!(
        paths_naming_stream(root.path()),
        Vec::<std::path::PathBuf>::new()
    );
    shutdown.cancel();
    let _ = feed.await;
}

/// **Closing many shards with a write in flight on each holds the pass up for
/// one bound, not one per shard**, and a shard opened in the same pass
/// serves by the time it returns. Every closing shard refuses writes at once.
#[tokio::test(start_paused = true)]
async fn closes_share_one_bound() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (fence, _lease) = leased();
    let memory = memory(&broker, &fence);
    let at = |shard, leader: &str, generation| ShardAssignment {
        key: key(shard),
        ..assigned(leader, generation)
    };
    let closing: Vec<u32> = (0..10).collect();
    let mut fed = assignments(&closing.iter().map(|s| at(*s, LOCAL, 5)).collect::<Vec<_>>());
    memory.apply(&memory.take(&mut fed)).await;
    let held: Vec<_> = closing
        .iter()
        .map(|s| fence.admit(&key(*s), 5).expect("a write in flight"))
        .collect();

    let mut next: Vec<ShardAssignment> = closing.iter().map(|s| at(*s, "broker-b", 6)).collect();
    next.push(at(10, LOCAL, 6));
    let mut fed = assignments(&next);
    let started = tokio::time::Instant::now();
    memory.apply(&memory.take(&mut fed)).await;
    let took = started.elapsed();
    assert!(
        took <= QUIESCE_BOUND + Duration::from_millis(100),
        "the pass took {took:?} closing {} shards",
        closing.len(),
    );
    assert_eq!(memory.servable().get(&key(10)), Some(&6));
    for shard in &closing {
        assert_eq!(fence.admit(&key(*shard), 5).err(), Some(Fenced::NotServing));
        assert!(memory.servable().get(&key(*shard)).is_none());
    }
    drop(held);
}
