//! Shard assignment: which broker leads a stream shard, and who replicates it.
//!
//! One assignment per `(stream, shard)`, and it is the authority on ownership.
//! Placement writes it; brokers read it and take or release shards to match.
//!
//! `generation` is what makes that safe. A broker reporting on an assignment it
//! read some time ago carries the generation it saw, and the control plane
//! rejects it if placement has moved on. Without that, a slow broker's status
//! could resurrect an ownership decision that was already replaced.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::model::node::validate_node_id;
use crate::model::{NodeValidationError, StreamKey};

/// Why a shard assignment was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShardValidationError {
    #[error("shard {shard} is out of bounds for a stream with {shards} shards")]
    ShardOutOfBounds { shard: u32, shards: u32 },
    #[error("leader node is invalid: {0}")]
    InvalidLeader(NodeValidationError),
    #[error("replica node is invalid: {0}")]
    InvalidReplica(NodeValidationError),
    #[error("node {0:?} is both the leader and a replica")]
    LeaderIsAlsoReplica(String),
    #[error("node {0:?} appears twice in the replica set")]
    DuplicateReplica(String),
    #[error("cannot move a shard from {from:?} to {to:?}")]
    UnsupportedTransition { from: ShardState, to: ShardState },
    #[error("successor node is invalid: {0}")]
    InvalidSuccessor(NodeValidationError),
    #[error("successor {0:?} is the current leader")]
    SuccessorIsLeader(String),
    #[error("successor {0:?} is not in the replica set")]
    SuccessorNotAReplica(String),
    #[error("joining follower {0:?} is not in the replica set")]
    JoiningNotAReplica(String),
}

/// Where an assignment is between being decided and being served.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ShardState {
    /// Placement has decided, and the leader has not confirmed it is serving.
    Assigning,
    /// The leader is serving this shard.
    Active,
    /// Ownership is moving. The leader stops serving at this generation and
    /// reports so; the next assignment names the successor.
    Draining,
}

impl ShardState {
    /// Whether `self` may move to `next`.
    ///
    /// `Draining` is reachable from `Assigning` too: nothing reports `Active`
    /// yet, and requiring it would cost every move an extra generation. A
    /// drained shard leaves only through a fresh `Assigning`.
    pub fn can_transition_to(self, next: ShardState) -> bool {
        use ShardState::*;
        matches!(
            (self, next),
            (Assigning, Assigning | Active | Draining)
                | (Active, Active | Draining)
                | (Draining, Draining | Assigning)
        )
    }
}

/// Whether a shard belongs to a stream or to a cache.
///
/// Both are placed by the same algorithm over the same kind of log — that is
/// the point of "one core log, many semantics". They are *not* the same
/// namespace: a cache and a stream may share a name within one namespace, and
/// when they do their shards are unrelated. Anything keyed by `ShardKey` must
/// therefore carry the kind, or the two silently collide on ownership.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "camelCase")]
pub enum ShardKind {
    /// A stream's shard. The default, so an assignment persisted before caches
    /// were placed reads back as exactly what it was.
    #[default]
    Stream,
    /// A cache's shard.
    Cache,
}

/// The wire shape a broker reports with, mapped to the model.
///
/// Two types on purpose: the wire form is shared with the broker and changes
/// only with the protocol, while this one is stored and carries the
/// persistence rules. Converting explicitly is what keeps a change to either
/// from silently becoming a change to the other.
impl From<felix_common::membership::ShardKind> for ShardKind {
    fn from(kind: felix_common::membership::ShardKind) -> Self {
        match kind {
            felix_common::membership::ShardKind::Stream => Self::Stream,
            felix_common::membership::ShardKind::Cache => Self::Cache,
        }
    }
}

impl ShardKind {
    /// The name this kind is stored and logged under.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Cache => "cache",
        }
    }
}

impl std::fmt::Display for ShardKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies one shard of one stream or cache.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    /// The stream or cache name, disambiguated by `kind`.
    pub stream: String,
    pub shard: u32,
    /// Absent on the wire means `Stream`, which is what every assignment
    /// written before caches were placed is.
    #[serde(default)]
    pub kind: ShardKind,
}

impl ShardKey {
    /// The stream this shard belongs to, or `None` if it belongs to a cache.
    pub fn stream_key(&self) -> Option<StreamKey> {
        (self.kind == ShardKind::Stream).then(|| StreamKey {
            tenant_id: self.tenant_id.clone(),
            namespace: self.namespace.clone(),
            stream: self.stream.clone(),
        })
    }

    /// The order assignments are paged in: Postgres's primary-key order, so a
    /// page there is an index range, with kind compared by its stored name.
    /// Every backend must page alike.
    pub(crate) fn page_order(&self) -> (String, String, &'static str, String, u32) {
        (
            self.tenant_id.clone(),
            self.namespace.clone(),
            self.kind.as_str(),
            self.stream.clone(),
            self.shard,
        )
    }
}

/// Who owns a shard right now.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardAssignment {
    #[serde(flatten)]
    pub key: ShardKey,
    /// The node that serves reads and writes for this shard.
    pub leader: String,
    /// Nodes holding a copy. Never contains the leader, never repeats.
    ///
    /// Empty when the replication factor is 1, which is the default.
    #[serde(default)]
    pub replicas: Vec<String>,
    /// Increments on every change to this assignment.
    ///
    /// A broker reports status against the generation it read; the control plane
    /// rejects anything older, so a slow broker cannot resurrect an ownership
    /// decision that placement has already replaced.
    pub generation: u64,
    pub state: ShardState,
    /// Where this shard is moving to, while a move is in progress. Always one
    /// of `replicas`; cleared by the assignment that makes it leader. In the
    /// store so any instance can resume the move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successor: Option<String>,
    /// A follower being copied in to replace one on a draining node. Always
    /// one of `replicas`; the one it replaces stays in the set until it is
    /// caught up, so the shard never has fewer copies than it asked for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub joining: Option<String>,
    /// The store's clock when the move or replacement in progress started,
    /// for the move timeout. With neither in progress, when the last one that
    /// timed out started: such a shard goes to the back of the queue for a
    /// move slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub move_started_at_millis: Option<u64>,
    /// Why the move or replacement in progress started. Cleared when it
    /// ends, so it is set exactly while `successor` or `joining` is, or the
    /// shard is `Draining`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub move_reason: Option<MoveReason>,
}

/// Why a shard is moving.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum MoveReason {
    /// Its leader's node is draining.
    Drain,
    /// Its leader's node leads more than its share.
    Balance,
    /// An operator asked for it (`POST /v1/shard-moves`).
    Operator,
    /// A follower on a draining node is being replaced.
    Replace,
}

impl MoveReason {
    /// The name this reason is stored and shown under.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Balance => "balance",
            Self::Operator => "operator",
            Self::Replace => "replace",
        }
    }

    /// The reverse of [`Self::as_str`].
    pub fn parse(value: &str) -> Option<Self> {
        [Self::Drain, Self::Balance, Self::Operator, Self::Replace]
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }
}

impl std::fmt::Display for MoveReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ShardAssignment {
    /// Check node identities and replica-set shape.
    ///
    /// Does not check the shard bound, which needs the stream's shard count, or
    /// that the nodes exist, which needs the catalog. Both are the store's.
    pub fn validate(&self) -> Result<(), ShardValidationError> {
        validate_node_id(&self.leader).map_err(ShardValidationError::InvalidLeader)?;

        let mut seen = std::collections::BTreeSet::new();
        for replica in &self.replicas {
            validate_node_id(replica).map_err(ShardValidationError::InvalidReplica)?;
            if replica == &self.leader {
                return Err(ShardValidationError::LeaderIsAlsoReplica(replica.clone()));
            }
            if !seen.insert(replica) {
                return Err(ShardValidationError::DuplicateReplica(replica.clone()));
            }
        }
        if let Some(successor) = &self.successor {
            validate_node_id(successor).map_err(ShardValidationError::InvalidSuccessor)?;
            if successor == &self.leader {
                return Err(ShardValidationError::SuccessorIsLeader(successor.clone()));
            }
            // The catch-up gate reads the replica report, so a successor that
            // is not a replica can never be found caught up.
            if !self.replicas.contains(successor) {
                return Err(ShardValidationError::SuccessorNotAReplica(
                    successor.clone(),
                ));
            }
        }
        if let Some(joining) = &self.joining
            && !self.replicas.contains(joining)
        {
            return Err(ShardValidationError::JoiningNotAReplica(joining.clone()));
        }
        Ok(())
    }

    /// Check this assignment's shard number against the stream that owns it.
    pub fn validate_within(&self, shards: u32) -> Result<(), ShardValidationError> {
        if self.key.shard >= shards {
            return Err(ShardValidationError::ShardOutOfBounds {
                shard: self.key.shard,
                shards,
            });
        }
        self.validate()
    }

    /// Every node this assignment refers to, leader first.
    pub fn nodes(&self) -> impl Iterator<Item = &String> {
        std::iter::once(&self.leader).chain(self.replicas.iter())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardAssignmentChange {
    pub seq: u64,
    pub op: ShardAssignmentChangeOp,
    pub key: ShardKey,
    pub assignment: Option<ShardAssignment>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ShardAssignmentChangeOp {
    Assigned,
    Updated,
    Unassigned,
}

/// What a shard's leader last reported about its replicas.
///
/// Promotion is gated on this. It lives in the store rather than in one
/// control-plane instance's memory because the instance a report reaches and
/// the instance that later promotes a replica need not be the same process --
/// under Postgres every instance serves, and a report only one of them had
/// seen was a position no promoter could use.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ReplicaReport {
    pub key: ShardKey,
    /// The assignment generation the leader held when it reported. A report
    /// from an older generation says nothing about this one: the replica set
    /// may be different.
    pub generation: u64,
    /// Replicas within the catch-up bound.
    pub caught_up: BTreeSet<String>,
    /// How far each replica had got when this was reported.
    pub offsets: BTreeMap<String, u64>,
    /// The store's clock when it was recorded. Never the broker's, which would
    /// let a leader keep its own report alive.
    pub reported_at_millis: u64,
    /// The leader has stopped serving at `generation` and its log will not
    /// grow: the handoff may proceed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub drained: bool,
    /// The leader's own tail when it reported, so placement can tell how far
    /// behind each replica in `offsets` is. Absent from brokers that predate
    /// it; placement then waits for an exact catch-up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_offset: Option<u64>,
}

impl ReplicaReport {
    /// Whether this report may replace `held` in the store.
    ///
    /// Reports only move forward: a later generation, or at the same one a
    /// leader tail at least as far along. Otherwise a slow request that lands
    /// after a newer one could put back a view in which a follower that has
    /// since fallen behind still looks caught up, and failover would promote
    /// it. A report without a tail (from a broker that predates it) is ordered
    /// by generation alone. Every backend applies this same rule.
    pub fn supersedes(&self, held: &ReplicaReport) -> bool {
        match self.generation.cmp(&held.generation) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => match (self.leader_offset, held.leader_offset) {
                (Some(new), Some(old)) => new >= old,
                _ => true,
            },
        }
    }
}

#[cfg(test)]
mod tests;
