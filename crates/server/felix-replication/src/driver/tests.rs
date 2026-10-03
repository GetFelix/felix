//! Which shards get shipped, and how long a cursor is believed.
//!
//! The engine's rules are tested next door; these are about the decisions
//! around it — leading versus following, a replica set that changes, and a
//! generation that moves.

mod cursors;
mod drain;
mod halts;
mod learner;
mod quorum;
mod reports;
mod shipping;
mod stragglers;
mod throttle;

use std::collections::HashMap as Map;
use std::net::SocketAddr;
use std::sync::Mutex;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage};
use felix_common::membership::{
    ReplicaOffset, ReplicaStatusRequest, ShardKind as WireShardKind, ShardReplicaStatus,
};
use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{InternalMessage, ReplicateOk, ReplicateRecords};
use tempfile::TempDir;

use super::shard::*;
use super::*;
use crate::peer::PeerError;
use crate::reporter::ReportTo;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LOCAL: &str = "broker-a";

/// A follower that stores everything and records which shard it was for. A
/// `Quorum` cache's counter log is acknowledged but not recorded.
#[derive(Default)]
struct AcceptingFollower {
    sent: Mutex<Vec<(String, ReplicateRecords)>>,
}

impl AcceptingFollower {
    fn batches(&self) -> Vec<(String, u64, usize)> {
        self.sent
            .lock()
            .expect("lock")
            .iter()
            .map(|(node, batch)| (node.clone(), batch.first_offset, batch.payloads.len()))
            .collect()
    }
}

impl PeerRequester for AcceptingFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let batch = match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateCacheRecords(batch) => batch,
            InternalMessage::ReplicateCounterRecords(batch) => {
                return Ok(InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id: 0,
                    durable_offset: batch.first_offset + batch.payloads.len() as u64,
                }));
            }
            _ => panic!("the driver sent something other than a replication batch"),
        };
        let durable_offset = batch.first_offset + batch.payloads.len() as u64;
        self.sent
            .lock()
            .expect("lock")
            .push((node_id.to_string(), batch));
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset,
        }))
    }
}

/// A follower that is gone: it answers nothing, and the dial gives up on its
/// own after `handshake` rather than hanging forever, as the pool's handshake
/// timeout makes it.
struct UnreachableFollowers {
    handshake: std::time::Duration,
}

impl PeerRequester for UnreachableFollowers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        tokio::time::sleep(self.handshake).await;
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "no answer within the handshake timeout".to_string(),
        })
    }
}

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
        kind: felix_router::ShardKind::Stream,
    }
}

fn nodes() -> Map<String, NodeRef> {
    ["broker-a", "broker-b", "broker-c"]
        .into_iter()
        .enumerate()
        .map(|(i, id)| (id.to_string(), node(id, 7001 + i as u16)))
        .collect()
}

/// A router where `leader` leads the shard with `replicas` behind it.
fn router(leader: &str, replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    publish(&router, leader, replicas, generation);
    router
}

fn publish(router: &ShardRouter, leader: &str, replicas: &[&str], generation: u64) {
    let nodes = nodes();
    let table = RoutingTable::build(
        [(
            key(),
            leader.to_string(),
            replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
}

/// A router where `leader` leads the shard, and has been told to stop.
fn draining_router(leader: &str, replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: leader.to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
            draining: true,
            // A move names where it is going; the drained report waits on it.
            successor: replicas.first().map(|r| r.to_string()),
            routing: Default::default(),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// A router where `leader` leads `shards` shards, each with `replicas` behind
/// it.
fn router_over_shards(
    leader: &str,
    replicas: &[&str],
    generation: u64,
    shards: u32,
) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build(
        (0..shards).map(|shard| {
            (
                ShardKey { shard, ..key() },
                leader.to_string(),
                replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                generation,
            )
        }),
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// A broker leading `shards` shards, each with `per_shard` records on disk.
async fn leader_with_shards(shards: u32, per_shard: usize) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    for shard in 0..shards {
        let log = storage
            .open_stream(TENANT, NAMESPACE, STREAM, shard)
            .expect("open");
        for i in 0..per_shard {
            log.append(&[Bytes::from(format!("s{shard}-v{i}"))])
                .await
                .expect("append");
        }
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
}

/// A broker leading the shard, with `count` records already on disk.
async fn leader_with(count: usize) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for i in 0..count {
        log.append(&[Bytes::from(format!("v{i}"))])
            .await
            .expect("append");
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
}

/// A leader with `count` records whose log says generation `generation` began
/// at `began_at` — what `DurableShardStore::open` records when this broker
/// takes the shard.
async fn leader_led_from(count: usize, generation: u64, began_at: u64) -> (Arc<Broker>, TempDir) {
    let (broker, dir) = leader_with(count).await;
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .record_generation(generation, began_at)
        .expect("record");
    (broker, dir)
}

/// A follower whose log disagrees with the leader's, which is a halt.
struct DivergingFollower;

impl PeerRequester for DivergingFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        Ok(InternalMessage::ReplicateError(
            felix_wire::internal::ReplicateError {
                correlation_id: message.correlation_id(),
                code: felix_wire::internal::ErrorCode::LogConflict,
                expected_offset: 0,
                detail: "diverged".to_string(),
            },
        ))
    }
}
