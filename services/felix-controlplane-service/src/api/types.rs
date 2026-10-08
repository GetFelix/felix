//! HTTP API request/response types.
//!
//! Defines shared payload shapes for the control-plane REST API and OpenAPI
//! schema generation.
// A leader's account of which replicas hold a shard's log.
//
// The shapes a broker sends live in felix-common, so a field renamed here and
// not there cannot be a silent mismatch — both sides compile against one
// definition. Re-exported rather than mirrored, for the same reason.
//
// A report is sent by the shard's *leader*, because it is the only party that
// knows both ends of the comparison: its own tail, and how far each follower
// has acknowledged. A follower knows where it is, not whether that is caught up.
pub use felix_common::membership::{
    ReplicaOffset, ReplicaStatusRequest, ReplicaStatusResponse, ReportOutcome, ShardReplicaStatus,
    ShardReportOutcome,
};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::{
    Cache, CacheChange, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceChange,
    NodeLifecycle, RetentionPolicy, Stream, StreamChange, StreamKind, Tenant, TenantChange,
};

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FeatureFlags {
    pub durable_storage: bool,
    pub tiered_storage: bool,
    pub bridges: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct SystemInfo {
    pub region_id: String,
    pub api_version: String,
    pub features: FeatureFlags,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct HealthStatus {
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Region {
    pub region_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListRegionsResponse {
    pub items: Vec<Region>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantCreateRequest {
    pub tenant_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceCreateRequest {
    pub namespace: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamCreateRequest {
    pub stream: String,
    pub kind: StreamKind,
    pub shards: u32,
    /// Brokers holding each shard, leader included. Defaults to leader-only.
    #[serde(default = "crate::model::default_replication_factor")]
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub consistency: ConsistencyLevel,
    pub delivery: DeliveryGuarantee,
    pub durable: bool,
    /// The region the stream's data belongs to. Placement keeps every copy
    /// in it, or in a region it has a bridge to. Omitted means any region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// How routing keys map to shards. Omitted means `jump_hash` once the
    /// `jump_hash_routing` fleet feature is finalized and `modulo` before.
    /// Asking for `jump_hash` before then is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<crate::model::StreamRouting>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheCreateRequest {
    pub cache: String,
    pub display_name: String,
    /// How many shards to split the keyspace across. Omitted means one.
    #[serde(default = "crate::model::default_cache_shards")]
    pub shards: u32,
    /// How many brokers hold each shard, leader included. Omitted means one.
    #[serde(default = "crate::model::default_cache_replication_factor")]
    pub replication_factor: u32,
    /// `Quorum` holds each write's acknowledgement until a majority of the
    /// shard's replicas have it. Omitted means `Leader`.
    #[serde(default = "crate::model::default_cache_consistency")]
    pub consistency: crate::model::ConsistencyLevel,
}

/// Matches the serde defaults, so a caller that fills in `..Default::default()`
/// gets the same cache a caller that omits the fields from JSON gets. Deriving
/// this instead would default both counts to zero, which places nothing.
impl Default for CacheCreateRequest {
    fn default() -> Self {
        Self {
            cache: String::new(),
            display_name: String::new(),
            shards: crate::model::default_cache_shards(),
            replication_factor: crate::model::default_cache_replication_factor(),
            consistency: crate::model::default_cache_consistency(),
        }
    }
}

/// Streams and caches to create in one namespace, all or none.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
pub struct ResourceBatchRequest {
    #[serde(default)]
    pub streams: Vec<StreamCreateRequest>,
    #[serde(default)]
    pub caches: Vec<CacheCreateRequest>,
}

/// Every item of a batch, in request order, with what the batch did to it.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ResourceBatchResponse {
    pub streams: Vec<BatchStream>,
    pub caches: Vec<BatchCache>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct BatchStream {
    pub status: BatchItemStatus,
    pub stream: Stream,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct BatchCache {
    pub status: BatchItemStatus,
    pub cache: Cache,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchItemStatus {
    /// The batch created it.
    Created,
    /// It already existed with the same configuration and was left alone.
    Unchanged,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantListResponse {
    pub items: Vec<Tenant>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantSnapshotResponse {
    pub items: Vec<Tenant>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantChangesResponse {
    pub items: Vec<TenantChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceListResponse {
    pub items: Vec<Namespace>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceSnapshotResponse {
    pub items: Vec<Namespace>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceChangesResponse {
    pub items: Vec<NamespaceChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamListResponse {
    pub items: Vec<Stream>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamSnapshotResponse {
    pub items: Vec<Stream>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamChangesResponse {
    pub items: Vec<StreamChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheListResponse {
    pub items: Vec<Cache>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheSnapshotResponse {
    pub items: Vec<Cache>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheChangesResponse {
    pub items: Vec<CacheChange>,
    pub next_seq: u64,
}

/// A broker's report that it is alive.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeHeartbeatRequest {
    /// The reporting process's own incarnation, from its last registration.
    ///
    /// Carried so a heartbeat that was delayed past a restart is rejected
    /// rather than counted for the process that replaced it.
    pub incarnation: u64,
}

/// What the control plane tells a broker in return.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeHeartbeatResponse {
    pub node_id: String,
    /// The node's lifecycle as the cluster sees it. A broker that reads `down`
    /// here has been expired and must register again.
    pub lifecycle: NodeLifecycle,
    /// How soon the next heartbeat is expected, so the interval is configured
    /// in one place rather than on every broker.
    pub heartbeat_interval_ms: u64,
    /// Silence beyond this marks the node down.
    pub expiry_timeout_ms: u64,
    /// The fleet features an operator has enabled ([`crate::cluster::fleet`]).
    /// May lag a moment behind a finalize on a Raft follower, so a broker
    /// only ever adds to what it has from this.
    #[serde(default)]
    pub fleet_features: std::collections::BTreeSet<String>,
}

/// A broker claiming its identity on boot.
///
/// Carries only spec fields. Observed status is the control plane's to set: a
/// broker that could declare itself live could outlive its own expiry.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeRegistrationRequest {
    pub node_id: String,
    /// `host:port` the broker-internal QUIC listener is reachable on.
    pub advertise_addr: String,
    /// `host:port` the client-facing QUIC listener is reachable on.
    ///
    /// Optional so that a broker predating client discovery registers exactly
    /// as it did before, and is left out of what clients are told rather than
    /// being refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_addr: Option<String>,
    /// `host:port` Kafka clients are told to connect to. Set only by a broker
    /// running its Kafka listener; absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_addr: Option<String>,
    pub region: String,
    /// The broker's failure domain within its region. Absent from a broker
    /// that predates zones, which is then placed as it always was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub capacity: crate::model::NodeCapacity,
    /// The fleet features this broker implements (`felix_common::fleet`).
    /// Absent from a broker that predates them, which reads as none.
    ///
    /// A broker lacking a feature an operator has enabled is refused with
    /// 409, so an enabled feature is never withdrawn by a late joiner.
    #[serde(default)]
    pub features: std::collections::BTreeSet<String>,
}

/// What a broker learns from registering.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeRegistrationResponse {
    pub node: crate::model::Node,
    /// The cadence expected of this broker, so it is configured in one place.
    pub heartbeat_interval_ms: u64,
    pub expiry_timeout_ms: u64,
    /// The fleet features enabled as of this registration.
    #[serde(default)]
    pub fleet_features: std::collections::BTreeSet<String>,
}

/// Which fleet features the serving brokers support, and which are enabled.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FleetFeaturesResponse {
    /// Every live or draining broker reported it. Supported is not enabled:
    /// nothing changes until an operator finalizes it.
    pub supported: std::collections::BTreeSet<String>,
    /// Finalized by an operator. Brokers act on these, and a broker lacking
    /// one is refused at registration.
    pub enabled: std::collections::BTreeSet<String>,
    /// How many brokers are live or draining, and so counted.
    pub serving_nodes: usize,
}

/// The outcome, or with `dry_run` the preview, of finalizing a feature.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FleetFinalizeResponse {
    pub feature: String,
    pub dry_run: bool,
    /// Whether the feature is enabled now. With `dry_run`, whether it
    /// already was.
    pub enabled: bool,
    /// Whether a real finalize would be accepted now.
    pub would_enable: bool,
    /// Serving brokers that did not report the feature. Finalizing is
    /// refused while any are listed.
    pub lacking: Vec<String>,
    /// How many brokers are live or draining.
    pub serving_nodes: usize,
}

/// Why a node is or is not a placement candidate.
///
/// The point of the endpoint: "this broker is registered but shards are not
/// landing on it" is otherwise answered by reading a lifecycle string and doing
/// heartbeat arithmetic by hand.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodePlacement {
    pub eligible: bool,
    /// Whether other brokers may send it requests for the shards it leads:
    /// live or draining, with its heartbeat inside the window. A draining
    /// node takes no new placement but serves each shard until the shard is
    /// handed off.
    #[serde(default)]
    pub routable: bool,
    /// Empty when eligible. One entry per reason it is not.
    pub reasons: Vec<String>,
    /// How long since the last accepted heartbeat, against the control plane's
    /// clock at the time of the request.
    pub heartbeat_age_ms: u64,
}

/// A node as an operator sees it: the record, plus what it means.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeView {
    pub node: crate::model::Node,
    pub placement: NodePlacement,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NodeListResponse {
    pub items: Vec<NodeView>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// A shard assignment as the control plane serves it: the stored record with
/// its stream's key mapping alongside.
///
/// The mapping travels with the assignment, not only with the stream catalog,
/// so a broker learns a stream's width and mapping from the same record and
/// can never resolve a key against one without the other.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RoutedShardAssignment {
    #[serde(flatten)]
    pub assignment: crate::model::ShardAssignment,
    /// The stream's routing. Absent is `modulo`, and always absent for a cache.
    #[serde(
        default,
        skip_serializing_if = "crate::model::StreamRouting::is_modulo"
    )]
    pub routing: crate::model::StreamRouting,
}

/// [`crate::model::ShardAssignmentChange`] with [`RoutedShardAssignment`].
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RoutedShardAssignmentChange {
    pub seq: u64,
    pub op: crate::model::ShardAssignmentChangeOp,
    pub key: crate::model::ShardKey,
    pub assignment: Option<RoutedShardAssignment>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardAssignmentListResponse {
    pub items: Vec<RoutedShardAssignment>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardAssignmentSnapshotResponse {
    pub items: Vec<RoutedShardAssignment>,
    /// Where to start polling changes. A consumer that applies this snapshot and
    /// then polls from here sees every committed change exactly once.
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardAssignmentChangesResponse {
    pub items: Vec<RoutedShardAssignmentChange>,
    pub next_seq: u64,
}

/// Where a move in progress has got to.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShardMoveStep {
    /// The destination is copying the log; the leader still serves.
    Staged,
    /// The leader has stopped serving; the move waits for its drained report
    /// and then cuts over.
    Fenced,
    /// A follower on a draining node is being replaced; leadership stays.
    Replacing,
    /// A copy is being added to bring the shard back to its replication
    /// factor; leadership stays.
    Restoring,
}

/// One move or follower replacement in progress.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ShardMove {
    #[serde(flatten)]
    pub key: crate::model::ShardKey,
    pub leader: String,
    /// The node the shard is moving to, or the follower being copied in.
    /// Absent for a fenced move whose destination died: it cuts over to a
    /// follower that holds the log, or back to the leader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    /// For a replacement, the follower on the draining node it replaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacing: Option<String>,
    pub step: ShardMoveStep,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<crate::model::MoveReason>,
    /// The store's clock when the move started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_millis: Option<u64>,
    pub generation: u64,
    /// How many records the destination is behind the leader, from the
    /// leader's latest report at this generation. Absent without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_records: Option<u64>,
    /// The leader's latest report at this generation names the destination
    /// caught up.
    pub caught_up: bool,
    /// The fenced leader has reported that it stopped and its logs are level
    /// on the destination; the next pass cuts over.
    pub drained: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardMoveListResponse {
    /// Whether placement's own moves are paused.
    pub paused: bool,
    pub items: Vec<ShardMove>,
}

/// Move a shard's leadership to `destination`.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardMoveRequest {
    #[serde(flatten)]
    pub key: crate::model::ShardKey,
    pub destination: String,
    /// Decide the move and answer what it would write, without writing it.
    #[serde(default)]
    pub dry_run: bool,
}

/// The assignment an operator's request wrote, and which step it was.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardMoveResponse {
    /// `stage` or `fence` for a start; `cancel` or `retake` for a cancel;
    /// `discard` for an abandoned log.
    pub step: String,
    pub assignment: crate::model::ShardAssignment,
    /// Set when the request was a dry run: `assignment` is what it would
    /// have written, and nothing was.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    /// Failure domains the shard's live copies span now, for a started move,
    /// when any broker the shard may use reports a zone. A broker without a
    /// zone counts as one of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zones_before: Option<usize>,
    /// Failure domains placement expects them to span once the move cuts
    /// over. Below `zones_before`, the move narrows the shard's spread: it is
    /// still started, and logged as a warning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zones_after: Option<usize>,
}

/// One shard in a placement plan.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PlannedShard {
    #[serde(flatten)]
    pub key: crate::model::ShardKey,
    /// `place`, a move step (`stage`, `fence`, `cut_over`, `abandon`,
    /// `timed_out`, `reseat`, `seat`), `waiting` or `unplaceable`.
    pub action: String,
    /// What the step would write, for `place` and move steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<crate::model::ShardAssignment>,
    /// Why nothing can be done yet, for `waiting` and `unplaceable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// What the next placement pass would do, shard by shard. Shards it would
/// leave alone are not listed.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PlacementPlanResponse {
    pub paused: bool,
    pub items: Vec<PlannedShard>,
}

/// One shard's copies against its replication factor.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardReplication {
    #[serde(flatten)]
    pub key: crate::model::ShardKey,
    pub leader: String,
    /// The replication factor the stream or cache asks for.
    pub desired_replicas: u32,
    /// Copies on a serving broker, the leader's included. A copy still being
    /// added does not count until it is seated.
    pub current_replicas: u32,
    /// `current_replicas < desired_replicas`.
    pub under_replicated: bool,
    /// Members of the replica set whose broker is not serving.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable: Vec<String>,
    /// Members of the replica set whose broker is serving but whose leader
    /// has stopped shipping to them. Not counted in `current_replicas`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub halted: Vec<HaltedReplica>,
    /// The broker a copy is being added on, while a restore is under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restoring: Option<String>,
}

/// A copy of a shard its leader has stopped shipping to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HaltedReplica {
    pub node_id: String,
    /// `diverged` or `needs_bootstrap`; the broker's `/replication/halted`
    /// says what each means and what to do about it.
    pub reason: String,
    /// The generation the leader reported it at.
    pub generation: u64,
    /// When a report first named it halted, on the store's clock. Placement
    /// replaces the copy once it has been halted for the restore delay.
    pub since_millis: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardReplicationResponse {
    /// How many shards are under-replicated, whatever the filter.
    pub under_replicated: usize,
    pub items: Vec<ShardReplication>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PlacementStatusResponse {
    /// Whether placement's own moves are paused.
    pub paused: bool,
}

/// A page of RBAC policy rules, answered when the request names `limit` or
/// `cursor`.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct PolicyListResponse {
    pub items: Vec<crate::auth::rbac::policy_store::PolicyRule>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// A page of RBAC role assignments, answered when the request names `limit`
/// or `cursor`.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct GroupingListResponse {
    pub items: Vec<crate::auth::rbac::policy_store::GroupingRule>,
    /// Where the next page starts; absent on the last page. Pass it back as
    /// `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// The RBAC policy listing: every rule as a bare array when the request names
/// neither `limit` nor `cursor`, as it always has, and a page otherwise.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(untagged)]
pub enum PolicyListing {
    All(Vec<crate::auth::rbac::policy_store::PolicyRule>),
    Page(PolicyListResponse),
}

/// The RBAC role-assignment listing, shaped as [`PolicyListing`] is.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(untagged)]
pub enum GroupingListing {
    All(Vec<crate::auth::rbac::policy_store::GroupingRule>),
    Page(GroupingListResponse),
}
