//! Placement tests, by theme. The helpers here build the catalogs and
//! clusters every theme plans over.
mod abandon;
mod caches;
mod departed;
mod failover;
mod halted;
mod lease;
mod moves;
mod operator;
mod pacing;
mod reconciler;
mod regions;
mod rendezvous;
mod replicas;
mod restore;
mod stalled;
mod startup;
mod suspicion;
mod wakes;
mod zones;

use std::collections::{BTreeMap, BTreeSet};

use super::rendezvous::score;
use super::*;
use crate::model::{
    Cache, ConsistencyLevel, DeliveryGuarantee, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
    StreamKind,
};

fn stream(name: &str, shards: u32) -> Stream {
    replicated_stream(name, shards, 1)
}

/// A stream held only in memory: there is no log to lose, so a shard whose
/// broker is lost is simply placed again.
fn ephemeral_stream(name: &str, shards: u32) -> Stream {
    Stream {
        durable: false,
        ..stream(name, shards)
    }
}

/// A stream that keeps `replication_factor` copies of each shard.
fn replicated_stream(name: &str, shards: u32, replication_factor: u32) -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: name.to_string(),
        kind: StreamKind::Stream,
        shards,
        replication_factor,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: true,
        region: None,
        routing: Default::default(),
    }
}

fn node(id: &str, lifecycle: NodeLifecycle, max_shards: Option<u32>) -> Node {
    Node {
        node_id: id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.4:{}", 7000 + id.len() as u16),
            client_addr: None,
            kafka_addr: None,
            region: "us-west-2".to_string(),
            zone: None,
            labels: Default::default(),
            capacity: NodeCapacity {
                max_shards,
                weight: 1,
            },
        },
        status: NodeStatus {
            lifecycle,
            last_heartbeat_at_millis: 1,
            registered_at_millis: 1,
            incarnation: 0,
            features: Default::default(),
        },
    }
}

fn live(ids: &[&str]) -> Vec<Node> {
    ids.iter()
        .map(|id| node(id, NodeLifecycle::Live, None))
        .collect()
}

fn placements(plan: &Plan) -> BTreeMap<(String, u32), String> {
    plan.shards
        .iter()
        .filter_map(|p| match &p.decision {
            Decision::Place(leader, _) => {
                Some(((p.key.stream.clone(), p.key.shard), leader.clone()))
            }
            _ => None,
        })
        .collect()
}

fn pinned(stream: &str, shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: stream.to_string(),
            shard,
            kind: ShardKind::Stream,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 3,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    }
}

fn assigned(stream: &str, leader: &str, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: stream.to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        leader: leader.to_string(),
        replicas: replicas.iter().map(|r| r.to_string()).collect(),
        generation: 3,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    }
}
