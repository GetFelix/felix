//! Broker node model, patch payloads, and change-log events.
//!
//! A node is a broker process in the cluster. The record is split in two: a
//! `NodeSpec` an operator sets, and a `NodeStatus` the cluster observes. Only
//! the spec is reachable through [`NodePatchRequest`], so an admin call can
//! never claim a broker is alive.
//!
//! Shard placement reads this model but is not part of it: a node says where it
//! is and how much it can hold, never what it currently holds.
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// Longest accepted node identity, label key, or label value.
const MAX_IDENTIFIER_LEN: usize = 253;

/// Why a node record was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum NodeValidationError {
    #[error("node_id must not be empty")]
    EmptyNodeId,
    #[error("node_id must be at most {MAX_IDENTIFIER_LEN} characters")]
    NodeIdTooLong,
    #[error(
        "node_id must contain only lowercase alphanumerics, '-', '.', or '_': {0:?} is not allowed"
    )]
    NodeIdCharacter(char),
    #[error("region must not be empty")]
    EmptyRegion,
    #[error("zone must not be empty when set")]
    EmptyZone,
    #[error("zone must be at most {MAX_IDENTIFIER_LEN} characters")]
    ZoneTooLong,
    #[error("advertise_addr {0:?} is not a valid host:port address")]
    InvalidAdvertiseAddr(String),
    #[error("client_addr {0:?} is not a valid host:port address")]
    InvalidClientAddr(String),
    #[error("client_addr must specify a non-zero port")]
    ZeroClientPort,
    #[error("kafka_addr {0:?} is not a valid host:port address")]
    InvalidKafkaAddr(String),
    #[error("kafka_addr must specify a non-zero port")]
    ZeroKafkaPort,
    #[error("advertise_addr must specify a non-zero port")]
    ZeroAdvertisePort,
    #[error("label keys must not be empty")]
    EmptyLabelKey,
    #[error("label key {0:?} must be at most {MAX_IDENTIFIER_LEN} characters")]
    LabelKeyTooLong(String),
    #[error("label value for {0:?} must be at most {MAX_IDENTIFIER_LEN} characters")]
    LabelValueTooLong(String),
    #[error("capacity hint max_shards must be greater than zero when set")]
    ZeroMaxShards,
    #[error("cannot move a node from {from:?} to {to:?}")]
    UnsupportedTransition {
        from: NodeLifecycle,
        to: NodeLifecycle,
    },
}

/// Where a node is in its membership lifecycle.
///
/// No state is terminal: a broker keeps its identity across restarts, so a
/// record that reached `Left` or `Down` is revived by the same node
/// re-registering rather than replaced by a new one.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum NodeLifecycle {
    /// Registered and heartbeating. Eligible for new shard placement.
    Live,
    /// Still serving, but should not receive new placement. Operator-driven.
    Draining,
    /// Heartbeat expired. The process may or may not still be running.
    Down,
    /// Deregistered through a graceful shutdown.
    Left,
}

impl NodeLifecycle {
    /// Whether `self` may move to `next`.
    ///
    /// Two rules produce the whole table. A node that is not currently serving
    /// cannot begin draining, because there is nothing to drain. Everything
    /// else is reachable, including revival from `Down` or `Left`, which is
    /// what a broker restart looks like.
    pub fn can_transition_to(self, next: NodeLifecycle) -> bool {
        use NodeLifecycle::*;
        match (self, next) {
            (from, to) if from == to => true,
            (Down | Left, Draining) => false,
            _ => true,
        }
    }

    /// Whether an operator, rather than an observed signal, may move `self` to
    /// `next`.
    ///
    /// Only into and out of `Draining`, and only from a serving node. Reviving
    /// a `Down` or `Left` node is registration's to do: an operator setting one
    /// `Live` would be claiming a broker is alive that nothing has heard from.
    pub fn operator_can_move_to(self, next: NodeLifecycle) -> bool {
        use NodeLifecycle::*;
        self == next || matches!((self, next), (Live, Draining) | (Draining, Live))
    }

    /// Live or draining: the node is serving, and counts toward the fleet
    /// minimum ([`crate::cluster::fleet`]).
    pub fn is_serving(self) -> bool {
        matches!(self, NodeLifecycle::Live | NodeLifecycle::Draining)
    }
}

/// Capacity hints an operator provides for placement.
///
/// Hints, not limits: placement is free to weigh them against live signals.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct NodeCapacity {
    /// Upper bound on shards this node should hold. `None` means unbounded.
    pub max_shards: Option<u32>,
    /// Relative share of shards against other nodes. Larger takes more.
    #[serde(default = "default_weight")]
    pub weight: u32,
}

/// Hand-written so an omitted `capacity` and an omitted `weight` agree. A
/// derived `Default` would give weight 0, which is no share of placement at all.
impl Default for NodeCapacity {
    fn default() -> Self {
        Self {
            max_shards: None,
            weight: default_weight(),
        }
    }
}

impl NodeCapacity {
    pub fn validate(&self) -> Result<(), NodeValidationError> {
        if self.max_shards == Some(0) {
            return Err(NodeValidationError::ZeroMaxShards);
        }
        Ok(())
    }
}

fn default_weight() -> u32 {
    1
}

/// What an operator declares about a node.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct NodeSpec {
    /// `host:port` the node's broker-internal QUIC listener is reachable on.
    ///
    /// Unique across the cluster: two nodes advertising one address cannot both
    /// be reached, and the store rejects the second.
    pub advertise_addr: String,
    /// `host:port` the node's *client-facing* QUIC listener is reachable on.
    ///
    /// Distinct from `advertise_addr`, which is the broker-internal listener no
    /// application may connect to. Optional, and absent by default: a broker
    /// that does not advertise one is simply never offered to a client as a
    /// place to connect, which is what an older broker registering against a
    /// newer control plane looks like.
    ///
    /// Not required to be unique. Two brokers behind one address is a
    /// deployment this cannot distinguish from a misconfiguration, and refusing
    /// the second registration would take a broker out of the cluster over a
    /// field that only affects discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_addr: Option<String>,
    /// `host:port` Kafka clients are told to connect to for this broker.
    ///
    /// Present only when the broker runs its Kafka-protocol listener. May be a
    /// DNS name rather than an IP, since Kafka advertised listeners usually
    /// are. Not required to be unique, for the same reason as `client_addr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_addr: Option<String>,
    pub region: String,
    /// The failure domain within the region: an availability zone, a rack.
    /// Placement spreads each shard's copies across zones where it can.
    ///
    /// Optional. A node without one shares a zone with no other node, so a
    /// cluster that reports no zones is placed exactly as it was before zones
    /// existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub capacity: NodeCapacity,
}

impl NodeSpec {
    pub fn validate(&self) -> Result<(), NodeValidationError> {
        if self.region.trim().is_empty() {
            return Err(NodeValidationError::EmptyRegion);
        }
        if let Some(zone) = &self.zone {
            if zone.trim().is_empty() {
                return Err(NodeValidationError::EmptyZone);
            }
            if zone.len() > MAX_IDENTIFIER_LEN {
                return Err(NodeValidationError::ZoneTooLong);
            }
        }
        validate_advertise_addr(&self.advertise_addr)?;
        if let Some(client_addr) = &self.client_addr {
            validate_client_addr(client_addr)?;
        }
        if let Some(kafka_addr) = &self.kafka_addr {
            validate_kafka_addr(kafka_addr)?;
        }
        validate_labels(&self.labels)?;
        self.capacity.validate()
    }
}

/// What the cluster has observed about a node.
///
/// Every field here is derived from registration and heartbeats. None of it is
/// settable through [`NodePatchRequest`], except `lifecycle`, and then only for
/// the transitions an operator is allowed to drive.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    pub lifecycle: NodeLifecycle,
    /// Milliseconds since the Unix epoch at the last accepted heartbeat.
    pub last_heartbeat_at_millis: u64,
    /// Milliseconds since the Unix epoch at the node's first registration.
    ///
    /// Preserved across restarts, so it dates the identity rather than the
    /// current process.
    pub registered_at_millis: u64,
    /// Increments on every registration, so a restart is distinguishable from a
    /// node that never went away.
    #[serde(default)]
    pub incarnation: u64,
    /// The fleet features this incarnation reported at registration
    /// (`felix_common::fleet`). Empty for a broker that predates them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub features: BTreeSet<String>,
}

/// The leaders a broker last said it cannot reach, from its heartbeat.
///
/// Soft state, like a heartbeat: placement acts on it only while it is
/// recent, and a broker that suspects nobody sends nothing, so an old one
/// ages out.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct NodeSuspicion {
    pub node_id: String,
    pub incarnation: u64,
    pub suspects: BTreeSet<String>,
    /// The store's clock when it arrived.
    pub reported_at_millis: u64,
}

/// A broker process in the cluster.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct Node {
    pub node_id: String,
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

impl Node {
    /// Check identity, address, labels, and capacity hints.
    ///
    /// Does not check cluster-wide uniqueness of `node_id` or
    /// `spec.advertise_addr`; only a store sees enough to do that.
    pub fn validate(&self) -> Result<(), NodeValidationError> {
        validate_node_id(&self.node_id)?;
        self.spec.validate()
    }
}

/// Operator-settable changes to a node.
///
/// Deliberately has no field for `last_heartbeat_at_millis`,
/// `registered_at_millis`, or `incarnation`: liveness is observed, and an admin
/// call that could write it would let a dead broker be reported live.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
pub struct NodePatchRequest {
    pub region: Option<String>,
    pub labels: Option<BTreeMap<String, String>>,
    pub capacity: Option<NodeCapacity>,
    /// The only status field an operator may drive, and only between `live` and
    /// `draining` ([`NodeLifecycle::operator_can_move_to`]).
    pub lifecycle: Option<NodeLifecycle>,
}

impl NodePatchRequest {
    /// Apply this patch to `node`, leaving it untouched if anything is invalid.
    pub fn apply(&self, node: &Node) -> Result<Node, NodeValidationError> {
        let mut patched = node.clone();
        if let Some(region) = &self.region {
            patched.spec.region = region.clone();
        }
        if let Some(labels) = &self.labels {
            patched.spec.labels = labels.clone();
        }
        if let Some(capacity) = &self.capacity {
            patched.spec.capacity = capacity.clone();
        }
        if let Some(lifecycle) = self.lifecycle {
            if !node.status.lifecycle.operator_can_move_to(lifecycle) {
                return Err(NodeValidationError::UnsupportedTransition {
                    from: node.status.lifecycle,
                    to: lifecycle,
                });
            }
            patched.status.lifecycle = lifecycle;
        }
        patched.validate()?;
        Ok(patched)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeChange {
    pub seq: u64,
    pub op: NodeChangeOp,
    pub node_id: String,
    pub node: Option<Node>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NodeChangeOp {
    Registered,
    Updated,
    Deregistered,
}

pub(crate) fn validate_node_id(node_id: &str) -> Result<(), NodeValidationError> {
    if node_id.is_empty() {
        return Err(NodeValidationError::EmptyNodeId);
    }
    if node_id.len() > MAX_IDENTIFIER_LEN {
        return Err(NodeValidationError::NodeIdTooLong);
    }
    // Constrained because a node_id reaches directory names, metric labels, and
    // log lines; anything needing quoting there is a problem later.
    if let Some(bad) = node_id
        .chars()
        .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '.' | '_')))
    {
        return Err(NodeValidationError::NodeIdCharacter(bad));
    }
    Ok(())
}

fn validate_advertise_addr(addr: &str) -> Result<(), NodeValidationError> {
    let parsed: SocketAddr = addr
        .parse()
        .map_err(|_| NodeValidationError::InvalidAdvertiseAddr(addr.to_string()))?;
    // Port 0 means "any port" to a listener, so it can never be dialled.
    if parsed.port() == 0 {
        return Err(NodeValidationError::ZeroAdvertisePort);
    }
    Ok(())
}

/// Same rules as the internal address: a client has to dial it too.
fn validate_client_addr(addr: &str) -> Result<(), NodeValidationError> {
    let parsed: SocketAddr = addr
        .parse()
        .map_err(|_| NodeValidationError::InvalidClientAddr(addr.to_string()))?;
    if parsed.port() == 0 {
        return Err(NodeValidationError::ZeroClientPort);
    }
    Ok(())
}

/// Unlike the QUIC addresses this accepts a hostname: a Kafka client resolves
/// it itself, and containerised setups advertise names like
/// `host.docker.internal:9092`. An IPv6 host must be bracketed, otherwise the
/// port cannot be told apart from the address.
fn validate_kafka_addr(addr: &str) -> Result<(), NodeValidationError> {
    let invalid = || NodeValidationError::InvalidKafkaAddr(addr.to_string());
    let (host, port) = addr.rsplit_once(':').ok_or_else(invalid)?;
    let port: u16 = port.parse().map_err(|_| invalid())?;
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner.strip_suffix(']').ok_or_else(invalid)?;
        inner.parse::<std::net::Ipv6Addr>().map_err(|_| invalid())?;
    } else if !is_hostname_or_ipv4(host) {
        return Err(invalid());
    }
    if port == 0 {
        return Err(NodeValidationError::ZeroKafkaPort);
    }
    Ok(())
}

/// A DNS name (RFC 1123 labels) or a dotted IPv4 address, which is itself a
/// valid name syntactically.
fn is_hostname_or_ipv4(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= MAX_IDENTIFIER_LEN
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), NodeValidationError> {
    for (key, value) in labels {
        if key.is_empty() {
            return Err(NodeValidationError::EmptyLabelKey);
        }
        if key.len() > MAX_IDENTIFIER_LEN {
            return Err(NodeValidationError::LabelKeyTooLong(key.clone()));
        }
        if value.len() > MAX_IDENTIFIER_LEN {
            return Err(NodeValidationError::LabelValueTooLong(key.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
