//! A promoted leader's fence and catch-up against replicas that answer as the
//! real handler does: `AnswerFence` and `OpenForWrites` in
//! `docs/formal/FelixShard.tla`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;

use bytes::Bytes;
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{Kind, ReplicateRecords, batch_checksum};
use tempfile::TempDir;

use super::*;
use crate::ReplicaHandler;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LEADER: &str = "broker-a";
/// The generation the leader was promoted at.
const PROMOTED: u64 = 5;

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

fn nodes() -> HashMap<String, NodeRef> {
    [
        (LEADER.to_string(), node(LEADER, 7001)),
        ("broker-b".to_string(), node("broker-b", 7002)),
        ("broker-c".to_string(), node("broker-c", 7003)),
    ]
    .into_iter()
    .collect()
}

/// The route the promoted leader sees.
fn route() -> Route {
    let table = RoutingTable::build(
        [(
            key(),
            LEADER.to_string(),
            vec!["broker-b".to_string(), "broker-c".to_string()],
            PROMOTED,
        )],
        &nodes(),
    );
    table.get(&key()).expect("route").clone()
}

/// A router for `local`, still at the generation before the promotion.
fn router_for(local: &str, generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let table = RoutingTable::build(
        [(
            key(),
            "broker-old".to_string(),
            vec![
                LEADER.to_string(),
                "broker-b".to_string(),
                "broker-c".to_string(),
            ],
            generation,
        )],
        &nodes(),
    );
    router.publish(table, &nodes());
    router
}

fn broker_on(dir: &std::path::Path) -> Arc<Broker> {
    let storage = felix_broker::DurableStorage::open(
        dir,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage))
}

fn batch(generation: u64, first_offset: u64, values: &[&str]) -> ReplicateRecords {
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect();
    ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation,
        },
        first_offset,
        checksum: batch_checksum(&payloads, &[]),
        payloads,
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
    }
}

/// One replica: its broker, and the handler that answers for it.
struct Replica {
    broker: Arc<Broker>,
    handler: ReplicaHandler,
    capabilities: PeerCapabilities,
    reachable: bool,
    _dir: TempDir,
}

impl Replica {
    fn new(node_id: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker = broker_on(dir.path());
        Self {
            handler: ReplicaHandler::new(Arc::clone(&broker), router_for(node_id, 3)),
            broker,
            capabilities: REQUIRED,
            reachable: true,
            _dir: dir,
        }
    }

    /// Store `values` at `first_offset` as a leader at `generation` shipped
    /// them. The router is republished at that generation first.
    async fn holds(&self, node_id: &str, generation: u64, first_offset: u64, values: &[&str]) {
        let handler =
            ReplicaHandler::new(Arc::clone(&self.broker), router_for(node_id, generation));
        let answer = handler
            .apply(
                batch(generation, first_offset, values),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{node_id} did not store the setup batch: {answer:?}"
        );
    }
}

/// Routes each request to the replica it names, as the pool would, and
/// remembers what was sent to whom.
struct Replicas {
    replicas: HashMap<String, Replica>,
    sent: Mutex<Vec<(String, Kind)>>,
}

impl Replicas {
    fn new() -> Self {
        Self {
            replicas: ["broker-b", "broker-c"]
                .into_iter()
                .map(|id| (id.to_string(), Replica::new(id)))
                .collect(),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn get(&self, node_id: &str) -> &Replica {
        &self.replicas[node_id]
    }

    fn get_mut(&mut self, node_id: &str) -> &mut Replica {
        self.replicas.get_mut(node_id).expect("replica")
    }

    fn sent(&self, kind: Kind) -> Vec<String> {
        self.sent
            .lock()
            .expect("lock")
            .iter()
            .filter(|(_, sent)| *sent == kind)
            .map(|(node, _)| node.clone())
            .collect()
    }
}

impl PeerRequester for Replicas {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let replica = self.get(node_id);
        if !replica.reachable {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "unreachable".to_string(),
            });
        }
        self.sent
            .lock()
            .expect("lock")
            .push((node_id.to_string(), message.kind()));
        match message {
            InternalMessage::Fence(fence) => Ok(replica.handler.fence(fence).await),
            InternalMessage::ReplicateFetch(fetch) => Ok(replica.handler.fetch(fetch).await),
            other => panic!("the promoted leader sent {:?}", other.kind()),
        }
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        Some(self.get(node_id).capabilities)
    }
}

/// The promoted leader's broker, holding `records` at `generation`.
async fn leader_holding(generation: u64, records: &[&str]) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_on(dir.path());
    let log = broker
        .durable_storage()
        .expect("storage")
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    log.record_generation(generation, 0).expect("generation");
    if !records.is_empty() {
        let payloads: Vec<Bytes> = records
            .iter()
            .map(|r| Bytes::copy_from_slice(r.as_bytes()))
            .collect();
        log.append(&payloads).await.expect("append");
    }
    (broker, dir)
}

async fn held(broker: &Broker) -> Vec<String> {
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.read_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}

async fn accepted(broker: &Broker) -> u64 {
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accepted_generation()
}

/// **The leader opens once a majority, itself included, has taken its
/// generation, and the old leader is refused by that majority from then on.**
#[tokio::test]
async fn a_promoted_leader_opens_once_a_majority_took_its_generation() {
    let replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 4, 0, &["a"]).await;
    }
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    let fenced = [
        accepted(&replicas.get("broker-b").broker).await,
        accepted(&replicas.get("broker-c").broker).await,
    ];
    assert!(
        fenced.iter().filter(|g| **g == PROMOTED).count() >= 1,
        "no replica took the fence: {fenced:?}"
    );
    // The deposed leader's next batch, to whichever replica took the fence.
    for id in ["broker-b", "broker-c"] {
        if accepted(&replicas.get(id).broker).await == PROMOTED {
            let answer =
                ReplicaHandler::new(Arc::clone(&replicas.get(id).broker), router_for(id, 4))
                    .apply(batch(4, 1, &["late"]), felix_broker::LogKind::Stream)
                    .await;
            assert!(
                matches!(&answer, InternalMessage::ReplicateError(err) if err.code == ErrorCode::FencedEpoch),
                "{id} took the deposed leader's batch after the fence: {answer:?}"
            );
        }
    }
}

/// **Without a majority the shard stays closed**, however long that takes:
/// opening on a minority is exactly what lets the old leader find a majority
/// of its own.
#[tokio::test]
async fn without_a_majority_the_shard_stays_closed() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-b").reachable = false;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert!(matches!(outcome, Outcome::Pending(_)), "{outcome:?}");
}

/// **A replica set with one broker that does not offer the fence keeps the
/// lease**, and nobody is sent a fence: what the fence buys later counts
/// every replica's promise.
#[tokio::test]
async fn a_replica_without_the_capability_keeps_the_lease() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-c").capabilities = PeerCapabilities::FENCE;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        outcome,
        Outcome::Lease {
            lacking: "broker-c".to_string()
        }
    );
    assert!(replicas.sent(Kind::Fence).is_empty());
}

/// **A tail a replica holds past the leader's own is taken before serving.**
/// It may be on a majority already; opening without it would let the leader
/// write over it.
#[tokio::test]
async fn a_tail_a_replica_holds_past_the_leader_is_taken() {
    let mut replicas = Replicas::new();
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a", "b", "c"])
        .await;
    // The majority is the leader and broker-b, so broker-b's answer is the
    // one the leader has when it opens.
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "b", "c"]);
}

/// **A newer generation's log wins over a longer older one**, and the
/// leader's own records it superseded go: (last generation, length), the
/// order `Ahead` uses.
#[tokio::test]
async fn a_newer_generations_log_replaces_the_leaders_own_suffix() {
    let replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 3, 0, &["a"]).await;
        replicas.get(id).holds(id, 4, 0, &["a", "y"]).await;
    }
    let (leader, _dir) = leader_holding(3, &["a", "stale-1", "stale-2"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert!(
        matches!(
            outcome,
            Outcome::Fenced {
                caught_up_from: Some(_)
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(held(&leader).await, vec!["a", "y"]);
}

/// And the other way round: a longer log from an older generation is not
/// ahead, so the leader keeps its own.
#[tokio::test]
async fn a_longer_log_from_an_older_generation_is_not_taken() {
    let replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 3, 0, &["a", "p", "q"]).await;
    }
    let (leader, _dir) = leader_holding(4, &["a", "x"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "x"]);
    assert!(replicas.sent(Kind::ReplicateFetch).is_empty());
}

/// **Records past the winning log's end go too.** They are older than its
/// last record, and the model replaces the leader's whole log with it. Here
/// the leader's own history under-reports what it holds, as it does when a
/// generation was never recorded, so nothing disagrees until the end.
#[tokio::test]
async fn a_suffix_past_the_winning_log_is_dropped() {
    let replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 3, 0, &["a"]).await;
        replicas.get(id).holds(id, 4, 0, &["a", "y"]).await;
    }
    let (leader, _dir) = leader_holding(3, &["a", "y", "older"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert!(
        matches!(
            outcome,
            Outcome::Fenced {
                caught_up_from: Some(_)
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(held(&leader).await, vec!["a", "y"]);
}

/// **The dropped suffix takes its labels with it, and the replica's labels
/// cover what is kept.** The records the leader already held matched, so no
/// batch appended and none labelled them as it arrived; after the suffix
/// goes, the leader's history must be the replica's, not its own.
#[tokio::test]
async fn a_dropped_suffix_leaves_the_replicas_labels() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 3, 0, &["a"]).await;
        replicas.get(id).holds(id, 4, 0, &["a", "y"]).await;
        replicas.get_mut(id).capabilities = REQUIRED.union(PeerCapabilities::GENERATION_LABELS);
    }
    let (leader, _dir) = leader_holding(3, &["a", "y", "older"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert!(
        matches!(
            outcome,
            Outcome::Fenced {
                caught_up_from: Some(_)
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(held(&leader).await, vec!["a", "y"]);
    let log = leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let history: Vec<(u64, u64)> = log
        .generations()
        .iter()
        .map(|epoch| (epoch.generation, epoch.start_offset))
        .collect();
    assert_eq!(history, vec![(3, 0), (4, 1)]);
    assert!(replicas.sent(Kind::ReplicateFetch).is_empty());
    assert!(!replicas.sent(Kind::ReplicateLabelledFetch).is_empty());
}

/// Delivers a leader's batches to one replica's handler, as the pool would.
struct ShipTo<'a> {
    handler: &'a ReplicaHandler,
}

impl PeerRequester for ShipTo<'_> {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => Ok(self
                .handler
                .apply(batch, felix_broker::LogKind::Stream)
                .await),
            other => panic!("the leader sent {:?}", other.kind()),
        }
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(REQUIRED.union(PeerCapabilities::GENERATION_LABELS))
    }
}

/// **A follower keeps the generation a record was written at**, not the one
/// of the leader that shipped it, so its fence answer does not claim a newer
/// last generation than it has. `FelixShardFollowerLabels.cfg` is the trace:
///
/// 1. At generation 1, x1 and x2 are acknowledged on broker-b and broker-c.
/// 2. broker-c is promoted at 2, ships x1 to this leader, and dies.
/// 3. This leader is promoted and fences broker-b.
///
/// Labelled with broker-c's generation, this leader's x1 looks like 2, ahead
/// of broker-b's (1, two records), so it would open without x2.
#[tokio::test]
async fn a_follower_keeps_the_generation_a_record_was_written_at() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get(id).holds(id, 1, 0, &["x1", "x2"]).await;
    }

    // broker-c leads at generation 2 from its tail, and ships this leader
    // one record before it dies.
    let (leader, _dir) = {
        let dir = tempfile::tempdir().expect("tempdir");
        (broker_on(dir.path()), dir)
    };
    let promoted = &replicas.get("broker-c").broker;
    let log = promoted
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.record_generation(2, 2).expect("term start");
    let follower = ReplicaHandler::new(Arc::clone(&leader), router_for(LEADER, 2));
    let mut cursor =
        crate::follower::FollowerCursor::new(LEADER, "10.0.0.1:7001".parse().expect("addr"), 0);
    let progress = crate::ship::ship_once(
        &ShipTo { handler: &follower },
        &log,
        &batch(2, 0, &[]).shard,
        felix_broker::LogKind::Stream,
        &mut cursor,
        1,
        &crate::rebuild::Rebuilds::disabled(),
    )
    .await;
    assert_eq!(
        progress,
        crate::ship::Progress::Stored { durable_offset: 1 }
    );
    assert_eq!(held(&leader).await, vec!["x1"]);
    replicas.get_mut("broker-c").reachable = false;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["x1", "x2"]);
}

/// **Raft's Figure 8, replayed: an inherited record acknowledged on a majority
/// survives the next fence**, because the majority that let it be
/// acknowledged also holds the generation-start record written after it.
/// `FelixShardFigure8.cfg` is the trace:
///
/// 1. This leader wrote x at generation 1 and shipped it nowhere.
/// 2. broker-b, at generation 2, wrote y and shipped it nowhere.
/// 3. broker-c is promoted at 3 holding x, writes its start record, and ships
///    both here. Its mark covers x only now, with a majority holding the
///    start record (`quorum::counted_offset`), and x is acknowledged.
/// 4. broker-c dies, and this leader is promoted and fences broker-b.
///
/// Without the start record this leader's log is (1, one record) and
/// broker-b's (2, one record) wins the fence: y replaces the acknowledged x.
#[tokio::test]
async fn an_inherited_record_acknowledged_on_a_majority_survives_the_next_fence() {
    let mut replicas = Replicas::new();
    let (leader, _dir) = leader_holding(1, &["x"]).await;
    replicas
        .get("broker-b")
        .holds("broker-b", 2, 0, &["y"])
        .await;
    replicas
        .get("broker-c")
        .holds("broker-c", 1, 0, &["x"])
        .await;

    let promoted = &replicas.get("broker-c").broker;
    let log = promoted
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.record_generation(3, 1).expect("term start");
    assert_eq!(
        log.append_generation_start(3).await.expect("start record"),
        1
    );
    let follower = ReplicaHandler::new(Arc::clone(&leader), router_for(LEADER, 3));
    let mut cursor =
        crate::follower::FollowerCursor::new(LEADER, "10.0.0.1:7001".parse().expect("addr"), 0);
    let progress = crate::ship::ship_once(
        &ShipTo { handler: &follower },
        &log,
        &batch(3, 0, &[]).shard,
        felix_broker::LogKind::Stream,
        &mut cursor,
        1 << 20,
        &crate::rebuild::Rebuilds::disabled(),
    )
    .await;
    assert!(matches!(progress, crate::ship::Progress::Stored { .. }));
    replicas.get_mut("broker-c").reachable = false;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route()).await;

    assert_eq!(
        held(&leader).await,
        vec!["x"],
        "the acknowledged x was replaced"
    );
    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
}
