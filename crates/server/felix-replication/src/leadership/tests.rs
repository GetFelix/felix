//! A read's leadership round against replicas that answer as the real
//! handler does: `ConfirmRead` and `EndRead` in
//! `docs/formal/FelixShardReads.tla`.

use std::collections::HashMap;
use std::net::SocketAddr;

use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::PeerCapabilities;
use tempfile::TempDir;

use super::*;
use crate::ReplicaHandler;
use crate::peer::PeerError;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LEADER: &str = "broker-a";
/// The generation the leader serves at.
const LEADING: u64 = 5;

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn key() -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn routing_key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

fn nodes() -> HashMap<String, NodeRef> {
    [
        (LEADER.to_string(), node(LEADER, 7001)),
        ("broker-b".to_string(), node("broker-b", 7002)),
        ("broker-c".to_string(), node("broker-c", 7003)),
    ]
    .into_iter()
    .collect()
}

/// A router for `local` that sees `LEADER` leading at `LEADING`.
fn router_for(local: &str) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let table = RoutingTable::build(
        [(
            routing_key(),
            LEADER.to_string(),
            vec!["broker-b".to_string(), "broker-c".to_string()],
            LEADING,
        )],
        &nodes(),
    );
    router.publish(table, &nodes());
    router
}

/// A broker whose copy of the shard's log has accepted `generation`.
async fn broker_at(generation: u64) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(generation, None)
        .await
        .expect("accept");
    (broker, dir)
}

/// One replica and the handler that answers for it.
struct Replica {
    handler: ReplicaHandler,
    reachable: bool,
    _dir: TempDir,
}

impl Replica {
    async fn new(node_id: &str) -> Self {
        let (broker, dir) = broker_at(LEADING).await;
        Self {
            handler: ReplicaHandler::new(broker, router_for(node_id)),
            reachable: true,
            _dir: dir,
        }
    }

    /// A leader at `generation` fences this replica, as a promotion does.
    async fn fenced_at(&self, generation: u64) {
        let answer = self
            .handler
            .fence(
                None,
                Fence {
                    correlation_id: 1,
                    shard: shard_at(generation),
                    log: ReplicaLog::Stream,
                },
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::FenceOk(_)),
            "the newer leader's fence was refused: {answer:?}"
        );
    }
}

fn shard_at(generation: u64) -> ShardRef {
    ShardRef {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        generation,
    }
}

/// Routes each fence to the replica it names. Each answer is decided when the
/// request arrives and handed back once `open` is true, which is how a round
/// can be answered in the past of a newer leader's fence.
struct Replicas {
    replicas: HashMap<String, Replica>,
    open: watch::Sender<bool>,
}

impl Replicas {
    async fn new() -> Self {
        let mut replicas = HashMap::new();
        for id in ["broker-b", "broker-c"] {
            replicas.insert(id.to_string(), Replica::new(id).await);
        }
        Self {
            replicas,
            open: watch::Sender::new(true),
        }
    }
}

impl PeerRequester for Replicas {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let replica = &self.replicas[node_id];
        if !replica.reachable {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "unreachable".to_string(),
            });
        }
        let answer = match message {
            InternalMessage::Fence(fence) => replica.handler.fence(None, fence).await,
            other => panic!("a read round sent {:?}", other.kind()),
        };
        let mut open = self.open.subscribe();
        let _ = open.wait_for(|open| *open).await;
        Ok(answer)
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(crate::promotion::REQUIRED)
    }
}

async fn read_index(
    replicas: &Arc<Replicas>,
    leader_accepted: u64,
) -> (ReadIndex<Arc<Replicas>>, TempDir) {
    let (broker, dir) = broker_at(leader_accepted).await;
    (
        ReadIndex::new(
            Arc::clone(replicas),
            broker,
            router_for(LEADER),
            Duration::from_secs(5),
        ),
        dir,
    )
}

#[tokio::test]
async fn a_majority_at_the_generation_confirms_the_read() {
    let replicas = Arc::new(Replicas::new().await);
    let (index, _dir) = read_index(&replicas, LEADING).await;
    index
        .confirm(&key(), LEADING)
        .await
        .expect("both replicas are at the leader's generation");
}

/// **A deposed leader cannot confirm a read.** A newer leader fenced both
/// replicas, so every majority this one could reach includes a replica that
/// refuses it, whatever its lease or its routing view says.
#[tokio::test]
async fn a_leader_whose_replicas_took_a_newer_fence_cannot_confirm() {
    let replicas = Arc::new(Replicas::new().await);
    for replica in replicas.replicas.values() {
        replica.fenced_at(LEADING + 1).await;
    }
    let (index, _dir) = read_index(&replicas, LEADING).await;
    let refused = index
        .confirm(&key(), LEADING)
        .await
        .expect_err("a read confirmed against replicas that follow a newer leader");
    assert!(
        matches!(refused, QuorumError::LeadershipLost { .. }),
        "{refused:?}"
    );
}

/// The leader counts itself only while its own log has taken no newer fence:
/// one follower still at its generation and itself, fenced, are not two.
#[tokio::test]
async fn a_fenced_leader_does_not_count_itself() {
    let mut replicas = Replicas::new().await;
    replicas
        .replicas
        .get_mut("broker-c")
        .expect("replica")
        .reachable = false;
    let replicas = Arc::new(replicas);
    let (index, _dir) = read_index(&replicas, LEADING + 1).await;
    index
        .confirm(&key(), LEADING)
        .await
        .expect_err("a leader fenced by its successor counted itself toward the round");
}

/// A read that arrives while a round is running waits for the next: the one
/// running may have been answered before the read took its value, and before
/// a newer leader fenced the replicas.
#[tokio::test]
async fn a_read_does_not_join_a_round_that_started_before_it() {
    let replicas = Arc::new(Replicas::new().await);
    let (index, _dir) = read_index(&replicas, LEADING).await;
    let index = Arc::new(index);

    // The first round is answered at the leader's generation, and held.
    replicas.open.send_replace(false);
    let first = tokio::spawn({
        let index = Arc::clone(&index);
        async move { index.confirm(&key(), LEADING).await }
    });
    // Let the first round reach both replicas before the newer leader does.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for replica in replicas.replicas.values() {
        replica.fenced_at(LEADING + 1).await;
    }
    let second = tokio::spawn({
        let index = Arc::clone(&index);
        async move { index.confirm(&key(), LEADING).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    replicas.open.send_replace(true);

    first
        .await
        .expect("join")
        .expect("the first read's round was answered at the generation");
    second
        .await
        .expect("join")
        .expect_err("a read joined a round that was answered before a newer fence");
}

/// A cache replica whose counter log took a newer leader refuses an older
/// leader's round, though its cache log never heard of that leader: counters
/// are all a new cache leader may have written.
#[tokio::test]
async fn a_replica_whose_counters_took_a_newer_leader_refuses_the_round() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Arc::new(
        Broker::new(Box::new(
            felix_storage::LogCache::open(dir.path().join("caches"), config.clone())
                .expect("cache"),
        ))
        .with_counters(Arc::new(
            felix_storage::CounterStore::open(dir.path().join("counters"), config)
                .expect("counters"),
        )),
    );
    let accept = |kind, generation| {
        let broker = Arc::clone(&broker);
        async move {
            broker
                .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
                .await
                .expect("log")
                .accept_generation(generation, None)
                .await
                .expect("accept");
        }
    };
    accept(felix_broker::LogKind::Cache, LEADING).await;
    accept(felix_broker::LogKind::Counters, LEADING + 1).await;

    let cache_key = felix_router::ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..routing_key()
    };
    let router = Arc::new(ShardRouter::new(
        "broker-b",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    router.publish(
        RoutingTable::build(
            [(
                cache_key,
                LEADER.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                LEADING,
            )],
            &nodes(),
        ),
        &nodes(),
    );
    let answer = ReplicaHandler::new(broker, router)
        .fence(
            None,
            Fence {
                correlation_id: 1,
                shard: shard_at(LEADING),
                log: ReplicaLog::Cache,
            },
        )
        .await;
    assert!(
        matches!(&answer, InternalMessage::ReplicateError(err) if err.code == felix_wire::internal::ErrorCode::FencedEpoch),
        "{answer:?}"
    );
}
