//! Every change of leader is fenced before the shard serves, not only a
//! promotion: `FenceEveryChange` in `docs/formal/FelixShard.tla`.
//!
//! Each test takes this broker through one kind of change, runs the fence
//! the replication driver would run against replicas that answer as the real
//! handler does, and then has a leader from before the change send a batch to
//! the replica it can still reach. The new leader cannot reach that old
//! leader, so the replica is part of every majority the fence can get, and it
//! refuses the batch. Without the fence it would still take it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use felix_replication::ReplicaHandler;
use felix_replication::peer::{PeerError, PeerRequester};
use felix_replication::promotion::{self, Outcome};
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{
    ErrorCode, InternalMessage, PeerCapabilities, ReplicateRecords, ShardRef, batch_checksum,
};

use super::*;
use crate::shards::routing::{routing_table_from, to_router_key};

const LOCAL: &str = "broker-a";

fn nodes() -> HashMap<String, NodeRef> {
    ["broker-a", "broker-b", "broker-c"]
        .into_iter()
        .zip(7001u16..)
        .map(|(id, port)| {
            (
                id.to_string(),
                NodeRef {
                    node_id: id.to_string(),
                    advertise_addr: SocketAddr::from(([10, 0, 0, 1], port)),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect()
}

fn broker_on(dir: &std::path::Path) -> Arc<felix_broker::Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir, config).expect("storage");
    Arc::new(
        felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
            .with_durable_storage(storage),
    )
}

/// A replica's handler with its routing view at `generation` under `leader`.
fn handler(
    broker: &Arc<felix_broker::Broker>,
    local: &str,
    leader: &str,
    generation: u64,
) -> ReplicaHandler {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let table = RoutingTable::build(
        [(
            to_router_key(&key(0)),
            leader.to_string(),
            vec![local.to_string()],
            generation,
        )],
        &nodes(),
    );
    router.publish(table, &nodes());
    ReplicaHandler::new(Arc::clone(broker), router)
}

fn batch(generation: u64, first_offset: u64, values: &[&str]) -> ReplicateRecords {
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect();
    ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation,
        },
        first_offset,
        checksum: batch_checksum(&payloads, &[], &[]),
        payloads,
        marks: Vec::new(),
        commit_offset: None,
        generations: None,
        publishers: Vec::new(),
        times: None,
    }
}

/// broker-a, its lifecycle, and its replicas. broker-b is the leader from
/// before the change, and broker-a cannot reach it.
struct Change {
    own: ShardLifecycle,
    assignments: HashMap<ShardKey, ShardAssignment>,
    leader: Arc<felix_broker::Broker>,
    replicas: HashMap<String, Arc<felix_broker::Broker>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl PeerRequester for Change {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let Some(broker) = self.replicas.get(node_id).filter(|_| node_id != "broker-b") else {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "partitioned".to_string(),
            });
        };
        // The handler's routing view does not matter to a fence or a fetch.
        let handler = handler(broker, node_id, "broker-b", 1);
        match message {
            InternalMessage::Fence(fence) => Ok(handler.fence(Some(LOCAL), fence).await),
            InternalMessage::ReplicateFetch(fetch) => Ok(handler.fetch(Some(LOCAL), fetch).await),
            other => panic!("the new leader sent {:?}", other.kind()),
        }
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(promotion::REQUIRED.union(PeerCapabilities::BALLOTS))
    }
}

impl Change {
    fn new() -> Self {
        let mut dirs = Vec::new();
        let mut on_disk = || {
            let dir = tempfile::tempdir().expect("tempdir");
            let broker = broker_on(dir.path());
            dirs.push(dir);
            broker
        };
        let leader = on_disk();
        let replicas = ["broker-b", "broker-c"]
            .into_iter()
            .map(|id| (id.to_string(), on_disk()))
            .collect();
        let mut own = lifecycle();
        own.fence_promotions();
        Self {
            own,
            assignments: HashMap::new(),
            leader,
            replicas,
            _dirs: dirs,
        }
    }

    /// `values` written by `from` leading at `generation`, on broker-c.
    async fn shipped(&self, from: &str, generation: u64, first_offset: u64, values: &[&str]) {
        let answer = handler(&self.replicas["broker-c"], "broker-c", from, generation)
            .apply(
                Some(from),
                batch(generation, first_offset, values),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "broker-c did not take {from}'s batch: {answer:?}"
        );
    }

    /// Deliver `assignment` as the feed would and carry out what it asks:
    /// open the shard, and if it waits for the fence, run the fence the
    /// replication driver would run and open it on the answer.
    async fn deliver(&mut self, assignment: ShardAssignment) {
        let key = assignment.key.clone();
        let action = self.own.observe(&key, Some(&assignment));
        self.assignments.insert(key.clone(), assignment);
        let Action::Open { generation, .. } = action else {
            return;
        };
        if self.own.opened(&key, generation) != Opened::Fencing {
            return;
        }
        let route = routing_table_from(&self.assignments, &nodes())
            .get(&to_router_key(&key))
            .expect("route")
            .clone();
        let outcome = promotion::fence_shard(
            self,
            &self.leader,
            LOCAL,
            &to_router_key(&key),
            &route,
            true,
        )
        .await;
        assert!(
            matches!(outcome, Outcome::Fenced { .. }),
            "the fence did not settle: {outcome:?}"
        );
        assert!(self.own.fenced(&key, generation));
    }

    /// The old leader's next batch at `generation`, to broker-c.
    async fn late_batch(&self, generation: u64, first_offset: u64) -> InternalMessage {
        handler(
            &self.replicas["broker-c"],
            "broker-c",
            "broker-b",
            generation,
        )
        .apply(
            Some("broker-b"),
            batch(generation, first_offset, &["late"]),
            felix_broker::LogKind::Stream,
        )
        .await
    }

    async fn assert_refused(&self, generation: u64, first_offset: u64) {
        let answer = self.late_batch(generation, first_offset).await;
        assert!(
            matches!(&answer, InternalMessage::ReplicateError(err) if err.code == ErrorCode::FencedEpoch),
            "broker-c took a batch from broker-b's generation {generation} after the change: {answer:?}"
        );
    }
}

fn led(leader: &str, replicas: &[&str], generation: u64) -> ShardAssignment {
    ShardAssignment {
        replicas: replicas.iter().map(|r| r.to_string()).collect(),
        ..assigned_to(leader, generation)
    }
}

/// **A move's cut-over is fenced.** broker-b drained into this broker at
/// generation 3; once this broker serves at 4, broker-b's batch at 3 is
/// refused by the replica it can still reach.
#[tokio::test]
async fn after_a_cut_over_the_drained_leader_is_refused() {
    let mut change = Change::new();
    change.shipped("broker-b", 3, 0, &["x"]).await;
    change
        .deliver(ShardAssignment {
            replicas: vec!["broker-a".to_string(), "broker-c".to_string()],
            ..moving_to("broker-a", 3, "draining")
        })
        .await;

    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 4))
        .await;

    assert!(change.own.may_serve_at(&key(0), 4));
    assert!(!change.own.is_incoming(&key(0)), "the move arrived");
    change.assert_refused(3, 1).await;
}

/// **A failover that names a move's destination is fenced.** The broker
/// cannot tell it from the cut-over it was waiting for, so it is the same
/// path: here the destination was still copying when broker-b failed.
#[tokio::test]
async fn after_a_failover_to_the_destination_the_old_leader_is_refused() {
    let mut change = Change::new();
    change.shipped("broker-b", 3, 0, &["x"]).await;
    change
        .deliver(ShardAssignment {
            replicas: vec!["broker-a".to_string(), "broker-c".to_string()],
            ..moving_to("broker-a", 3, "active")
        })
        .await;

    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 4))
        .await;

    assert!(change.own.may_serve_at(&key(0), 4));
    change.assert_refused(3, 1).await;
}

/// **A cancelled move's hand-back is fenced.** This broker drained toward
/// broker-b at 3 and is handed the shard back at 4. broker-b, named at 3 by
/// a planner working from an older read, is refused once it serves.
#[tokio::test]
async fn after_a_hand_back_an_older_leader_is_refused() {
    let mut change = Change::new();
    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 2))
        .await;
    assert!(change.own.may_serve_at(&key(0), 2));
    change.shipped("broker-b", 3, 0, &["x"]).await;
    change
        .deliver(ShardAssignment {
            leader: "broker-a".to_string(),
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            state: "draining".to_string(),
            successor: Some("broker-b".to_string()),
            ..assigned_to("broker-a", 3)
        })
        .await;

    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 4))
        .await;

    assert!(change.own.may_serve_at(&key(0), 4));
    change.assert_refused(3, 1).await;
}

/// **A new generation of a shard this broker already serves is fenced**,
/// whether a follower replacement or a move's staging bumped it, or a
/// promotion of broker-b at 3 reached this broker coalesced away. broker-b
/// is refused, and what it wrote at 3 is taken before this broker serves.
#[tokio::test]
async fn after_a_new_generation_of_a_served_shard_the_leader_between_is_refused() {
    let mut change = Change::new();
    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 2))
        .await;
    assert!(change.own.may_serve_at(&key(0), 2));
    change.shipped("broker-b", 3, 0, &["x"]).await;

    change
        .deliver(led("broker-a", &["broker-b", "broker-c"], 4))
        .await;

    assert!(change.own.may_serve_at(&key(0), 4));
    change.assert_refused(3, 1).await;
    let log = change
        .leader
        .shard_log(felix_broker::LogKind::Stream, "t1", "ns", "orders", 0)
        .await
        .expect("log");
    let held: Vec<_> = log
        .read_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| record.payload.clone())
        .collect();
    assert_eq!(held, vec![Bytes::from_static(b"x")]);
}
