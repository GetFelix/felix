//! Making a control plane's store read as one that served brokers before.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
    StreamKey, StreamKind,
};

use super::ControlPlane;

/// The namespace, stream and node of the shard [`ControlPlane::record_a_served_shard`]
/// places. Their own names, so nothing a test creates meets them.
const NAMESPACE: &str = "upgraded-fleet";
const STREAM: &str = "served-before";
const NODE: &str = "upgraded-fleet-broker";

impl ControlPlane {
    /// Leave in the store what a cluster that ran before this release has:
    /// a shard that was placed on a broker. A broker is registered, a stream
    /// placed on it, and both removed again, so only the history is left.
    ///
    /// The control plane then reads the cluster as not new, by its own rule
    /// (`cluster::fleet::never_placed`), and finalizes nothing at the first
    /// registration: the fleet comes up as an upgraded one does, with every
    /// feature left for the operator. Run before any broker registers.
    pub(crate) async fn record_a_served_shard(&self, tenant_id: &str) -> Result<()> {
        let store = self.live_store();
        store
            .create_namespace(Namespace {
                tenant_id: tenant_id.to_string(),
                namespace: NAMESPACE.to_string(),
                display_name: NAMESPACE.to_string(),
            })
            .await
            .context("create the upgraded fleet's namespace")?;
        store
            .register_node(Node {
                node_id: NODE.to_string(),
                spec: NodeSpec {
                    // Port 1 is never a broker's.
                    advertise_addr: "127.0.0.1:1".to_string(),
                    client_addr: None,
                    kafka_addr: None,
                    region: "local".to_string(),
                    zone: None,
                    labels: BTreeMap::new(),
                    capacity: NodeCapacity {
                        max_shards: None,
                        weight: 1,
                    },
                },
                status: NodeStatus {
                    lifecycle: NodeLifecycle::Live,
                    last_heartbeat_at_millis: 0,
                    registered_at_millis: 0,
                    incarnation: 0,
                    features: Default::default(),
                    storage_id: None,
                },
            })
            .await
            .context("register the upgraded fleet's broker")?;
        let stream = Stream {
            tenant_id: tenant_id.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: Default::default(),
            incarnation: 0,
            mount: None,
        };
        store
            .create_stream(stream)
            .await
            .context("create the upgraded fleet's stream")?;
        store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: tenant_id.to_string(),
                    namespace: NAMESPACE.to_string(),
                    stream: STREAM.to_string(),
                    shard: 0,
                    kind: ShardKind::Stream,
                },
                leader: NODE.to_string(),
                replicas: Vec::new(),
                generation: 0,
                state: ShardState::Assigning,
                incarnation: 0,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
                fence_storage_ids: Default::default(),
                member_storage_ids: Default::default(),
                learners: Default::default(),
                pending_set: None,
                previous_generation: None,
                readmitted: Default::default(),
                placed: None,
                switch: None,
                raft_copy_ids: Default::default(),
                raft_target: None,
                raft_target_counter: 0,
                raft_floor: None,
                raft_formed: None,
                raft_voters: None,
                raft_seed: None,
                raft_bases: None,
                raft_leader: None,
                raft_ordered: None,
            })
            .await
            .context("place the upgraded fleet's shard")?;
        // Deleting the stream drops its assignment, which lets the node go.
        store
            .delete_stream(&StreamKey {
                tenant_id: tenant_id.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
            })
            .await
            .context("delete the upgraded fleet's stream")?;
        store
            .delete_node(NODE)
            .await
            .context("remove the upgraded fleet's broker")?;
        Ok(())
    }
}
