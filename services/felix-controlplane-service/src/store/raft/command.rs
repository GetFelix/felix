//! The metadata command set: every mutation of the store, as data.
//!
//! One variant per mutating store-trait method, shaped like the API call
//! rather than the rows it touches — that is what makes multi-step
//! operations (a tenant bootstrap, a cascading delete) atomic under Raft for
//! free: the log totally orders whole operations, so there is no interleaved
//! half to protect against.
//!
//! Everything nondeterministic is **in** the command, decided before it is
//! proposed: heartbeat and expiry carry their timestamps, bootstrap carries
//! its candidate signing keys. Applying a command reads nothing but the
//! command and prior state, which is the property the determinism harness in
//! `state_machine/tests.rs` enforces.
//!
//! That leaves the question of *whose* clock a carried timestamp is. For the
//! ones that get compared against a reading taken elsewhere, the answer has
//! to be the leader's, and [`restamp`] is how it becomes so.
//!
//! The wire form is a versioned JSON envelope. A follower that does not
//! understand a command must not misparse it: it records an `Unsupported`
//! response instead, logs an error and counts
//! `felix_meta_raft_unsupported_commands_total`. That keeps members on the
//! same build identical, but a newer leader applied the command, so in a
//! mixed-version group the older member's state falls behind the leader's.
//! A release that adds a variant must therefore finish rolling every member
//! before anything proposes it; `docs/metadata-raft-design.md` ("Upgrading")
//! has the order and the recovery if the counter moves.
use serde::{Deserialize, Serialize};

use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::model::{
    Cache, CacheKey, CachePatchRequest, Namespace, NamespaceKey, Node, NodeLifecycle,
    NodePatchRequest, ReplicaReport, ShardAssignment, ShardKey, Stream, StreamKey,
    StreamPatchRequest, Tenant,
};
use crate::store::{StoreError, TenantAuthSeed};

/// The newest envelope version this build can apply.
///
/// Bump only when an existing variant's meaning changes; *adding* a variant
/// is not a version bump, because an old follower already rejects the
/// unknown `op` on deserialization (see the module docs for what that
/// costs in a mixed-version group).
pub const COMMAND_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetaCommand {
    CreateTenant {
        tenant: Tenant,
    },
    DeleteTenant {
        tenant_id: String,
    },
    CreateNamespace {
        namespace: Namespace,
    },
    DeleteNamespace {
        key: NamespaceKey,
    },
    CreateStream {
        stream: Stream,
    },
    PatchStream {
        key: StreamKey,
        patch: StreamPatchRequest,
    },
    DeleteStream {
        key: StreamKey,
    },
    CreateCache {
        cache: Cache,
    },
    PatchCache {
        key: CacheKey,
        patch: CachePatchRequest,
    },
    DeleteCache {
        key: CacheKey,
    },
    RegisterNode {
        node: Node,
    },
    PatchNode {
        node_id: String,
        patch: NodePatchRequest,
    },
    DeleteNode {
        node_id: String,
    },
    /// `at_millis` is the leader's clock, whichever instance the broker's
    /// heartbeat happened to land on: the proposer fills in its own reading
    /// and the leader overwrites it at acceptance (see [`restamp`]). Never
    /// the broker's own, which would let it postpone its own timeout.
    ///
    /// It has to be the leader's specifically, because expiry compares it
    /// against a cutoff the leader-gated sweep computes. Two instances'
    /// readings would make liveness depend on their wall clocks agreeing to
    /// within the timeout, rather than on the much weaker bound on drift
    /// *rate* the design assumes.
    RecordNodeHeartbeat {
        node_id: String,
        incarnation: u64,
        at_millis: u64,
    },
    /// The cutoff is computed by the proposer; every replica expires exactly
    /// the same set, in sorted order, whatever its own clock says.
    ExpireStaleNodes {
        expiry_before_millis: u64,
    },
    SetNodeLifecycle {
        node_id: String,
        lifecycle: NodeLifecycle,
    },
    PutShardAssignment {
        assignment: ShardAssignment,
    },
    /// Write only if the shard is at `expected_generation` (`None`: has no
    /// assignment), checked as the command applies.
    ///
    /// Its own variant rather than an optional field on `PutShardAssignment`:
    /// a follower that predates it must refuse it, not apply it
    /// unconditionally and diverge from the replicas that skipped it as stale.
    PutShardAssignmentIf {
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
    },
    /// `PutShardAssignmentIf`, also only at placement token `fence`
    /// (`ControlPlaneStore::put_shard_assignment_if`). Its own variant for the
    /// same reason: a follower that predates the token must refuse it rather
    /// than apply it unfenced. The older variant stays so a log written
    /// before it replays as it applied.
    PutShardAssignmentFenced {
        assignment: ShardAssignment,
        expected_generation: Option<u64>,
        fence: u64,
    },
    /// The placement lease goes to `holder`, advancing the token if it
    /// changes hands. Proposed only by a confirmed leader, so no expiry.
    TakePlacementLease {
        holder: String,
    },
    DeleteShardAssignment {
        key: ShardKey,
    },
    /// `report.reported_at_millis` is the leader's clock, for the same
    /// reason as a heartbeat's `at_millis`: placement judges its freshness on
    /// the leader, and the instance the broker's report happened to reach is
    /// not necessarily that one.
    RecordReplicaReport {
        report: ReplicaReport,
        /// The node the report must come from. Checked against the
        /// replicated assignment when the entry applies, not against the
        /// proposer's possibly lagging copy. Absent in entries from members
        /// that predate the check, which apply unchecked as they did then.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leader: Option<String>,
    },
    SetMovesPaused {
        paused: bool,
    },
    UpsertIdpIssuer {
        tenant_id: String,
        issuer: IdpIssuerConfig,
    },
    DeleteIdpIssuer {
        tenant_id: String,
        issuer: String,
    },
    AddRbacPolicy {
        tenant_id: String,
        policy: PolicyRule,
    },
    AddRbacGrouping {
        tenant_id: String,
        grouping: GroupingRule,
    },
    SetTenantSigningKeys {
        tenant_id: String,
        keys: TenantSigningKeys,
    },
    /// Rule removal and the signing-key lifecycle below are newer than the
    /// rest of this set. A member that predates them refuses them, so every
    /// control-plane member must run a build that has them before anyone uses
    /// the routes that propose them.
    RemoveRbacPolicy {
        tenant_id: String,
        policy: PolicyRule,
    },
    RemoveRbacGrouping {
        tenant_id: String,
        grouping: GroupingRule,
    },
    /// The key is generated by the proposer, like bootstrap's.
    StageSigningKey {
        tenant_id: String,
        key: SigningKey,
    },
    ActivateSigningKey {
        tenant_id: String,
        kid: String,
    },
    RetireSigningKey {
        tenant_id: String,
        kid: String,
    },
    /// Install `candidate` only if the tenant has no keys; existing keys win.
    ///
    /// Distinct from `SetTenantSigningKeys` (an unconditional rotation)
    /// because the proposer decides from a possibly-stale local read: an
    /// overwrite proposed off stale emptiness would clobber a rotation that
    /// committed in between. Install-if-absent makes the race harmless.
    EnsureSigningKeys {
        tenant_id: String,
        candidate: TenantSigningKeys,
    },
    SetTenantAuthBootstrapped {
        tenant_id: String,
        bootstrapped: bool,
    },
    SeedRbac {
        tenant_id: String,
        policies: Vec<PolicyRule>,
        groupings: Vec<GroupingRule>,
    },
    /// The whole day-0 seed as one command; the seed carries the candidate
    /// signing keys, so exactly-once needs nothing beyond log order.
    BootstrapTenantAuth {
        tenant_id: String,
        seed: TenantAuthSeed,
    },
    /// Replace the entire store with an exported state — the migration
    /// cutover from Postgres, and the beyond-quorum-loss restore.
    ///
    /// One command, so the whole import is one log entry applied atomically
    /// on every member; sequence high-water marks inside the state are what
    /// let broker watches resume with at most one resnapshot. Refused unless
    /// the store has never held anything, or `overwrite` says the operator
    /// really means to discard what is there.
    ImportState {
        // Boxed: this one variant is as big as the whole store, and every
        // command would otherwise pay its size.
        state: Box<crate::store::export::ExportedState>,
        overwrite: bool,
    },
    /// Refresh-token writes.
    ///
    /// Through the log like every other write, because the replicas must agree
    /// on which tokens exist and which are spent. `TakeRefreshToken` in
    /// particular: a single-use token whose spend was applied on one replica
    /// and not another would be usable twice, once per replica.
    InsertRefreshToken {
        token: RefreshToken,
    },
    TakeRefreshToken {
        tenant_id: String,
        token_id: String,
        /// The clock, decided by the proposer.
        ///
        /// Every replica applies this command, so anything read from the
        /// environment would differ between them — a token could expire on one
        /// replica and not another, and the state machines would diverge.
        now_secs: i64,
    },
    RevokeRefreshFamily {
        tenant_id: String,
        family_id: String,
    },
    RevokeRefreshTokensForPrincipal {
        tenant_id: String,
        principal_id: String,
    },
    PurgeExpiredRefreshTokens {
        before_secs: i64,
    },
    /// Mark these nodes down, each only while it is still serving at the
    /// incarnation named. Heartbeats are the leader's soft state rather than
    /// log entries, so the leader decides who is stale and the log carries
    /// the decision, not a cutoff for replicas to judge against.
    ExpireNodes {
        nodes: Vec<NodeIncarnation>,
    },
    /// The last heartbeat the leader saw from each node, recorded now and
    /// then so that node listings on followers do not freeze at registration.
    /// Never moves a stored time backwards, and never changes a lifecycle.
    CheckpointHeartbeats {
        beats: Vec<HeartbeatSeen>,
    },
}

/// A node, at the incarnation a decision about it was made against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIncarnation {
    pub node_id: String,
    pub incarnation: u64,
}

/// One node's last heartbeat as the leader saw it, by the leader's clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatSeen {
    pub node_id: String,
    pub incarnation: u64,
    pub at_millis: u64,
}

/// What a command returns, mirroring the store method it stands for.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetaResponse {
    Unit,
    Tenant {
        tenant: Tenant,
    },
    Namespace {
        namespace: Namespace,
    },
    Stream {
        stream: Stream,
    },
    Cache {
        cache: Cache,
    },
    Node {
        node: Node,
    },
    Nodes {
        nodes: Vec<Node>,
    },
    /// `set_node_lifecycle`'s "already there" answer must survive the trip.
    MaybeNode {
        node: Option<Node>,
    },
    Assignment {
        assignment: ShardAssignment,
    },
    /// `PutShardAssignmentIf` found another generation and wrote nothing.
    StaleAssignment {
        current_generation: Option<u64>,
    },
    /// `PutShardAssignmentFenced` found another placement token.
    FencedAssignment {
        token: u64,
    },
    PlacementLease {
        token: u64,
        taken: bool,
    },
    SigningKeys {
        keys: TenantSigningKeys,
    },
    /// What `TakeRefreshToken` found, carried back from the apply.
    RefreshTokenTake {
        take: RefreshTokenTake,
    },
    /// How many rows a revoke or purge affected.
    Count {
        count: u64,
    },
    /// `RecordReplicaReport` kept the report already held. A stored report
    /// answers `Unit`, so an older leader's answer still reads as stored.
    StaleReport,
    /// `RecordReplicaReport` came from a node that does not lead the shard.
    NotLeaderReport,
    /// The placement lease is held by someone else and has not expired.
    NoPlacementLease,
}

/// `StoreError`, flattened into something that serializes and compares.
///
/// `Unexpected` loses its structure deliberately: an anyhow chain is for
/// humans, and by the time an error crosses the log it is a string either way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "error", content = "detail", rename_all = "snake_case")]
pub enum MetaError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
    /// The command could not be decoded — a version or variant this build
    /// does not know. Fatal to serve from, never silently skipped.
    #[error("unsupported command: {0}")]
    Unsupported(String),
}

impl From<StoreError> for MetaError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::NotFound(what) => MetaError::NotFound(what),
            StoreError::Conflict(what) => MetaError::Conflict(what),
            StoreError::Unexpected(err) => MetaError::Internal(format!("{err:#}")),
        }
    }
}

impl From<MetaError> for StoreError {
    fn from(err: MetaError) -> Self {
        match err {
            MetaError::NotFound(what) => StoreError::NotFound(what),
            MetaError::Conflict(what) => StoreError::Conflict(what),
            MetaError::Internal(msg) | MetaError::Unsupported(msg) => {
                StoreError::Unexpected(anyhow::anyhow!(msg))
            }
        }
    }
}

pub type MetaResult = Result<MetaResponse, MetaError>;

#[derive(Serialize, Deserialize)]
struct Envelope {
    v: u16,
    /// Identifies one *logical* write across the attempts that carry it.
    ///
    /// `RaftStore::write` caps each attempt and retries within a larger
    /// budget, and a timed-out attempt does not mean the proposal failed -- it
    /// means no answer arrived in time. If the command committed as the cap
    /// expired, the retry proposes it again and the state machine answers from
    /// its post-commit state: `409 tenant already exists`, for a tenant the
    /// caller successfully created (#529).
    ///
    /// With an id the retry is recognised and answered with the original
    /// response, which makes it genuinely idempotent rather than merely
    /// repeated. Absent for a proposal from a peer that predates this, which
    /// is deduplicated by nothing and behaves exactly as it did before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rid: Option<String>,
    #[serde(flatten)]
    command: MetaCommand,
}

/// Serialize a command for proposal.
pub fn encode_command(command: &MetaCommand) -> Vec<u8> {
    serde_json::to_vec(&Envelope {
        v: COMMAND_VERSION,
        rid: None,
        command: command.clone(),
    })
    .expect("commands serialize by construction")
}

/// The shared skeleton behind every in-place command edit ([`restamp`],
/// [`stamp_request_id`]): decode to `Value` rather than round-tripping through
/// [`MetaCommand`], because a command from a newer build may carry fields this
/// one does not know, and the typed form would drop them on the way back out.
///
/// `edit` returns whether it changed anything; `false` means "nothing to do
/// here" (the wrong command, or a field already set), and this returns `None`
/// so the caller proposes the original bytes.
fn edit_command_json(
    command: &[u8],
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>) -> bool,
) -> Option<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(command).ok()?;
    let object = value.as_object_mut()?;
    if !edit(object) {
        return None;
    }
    serde_json::to_vec(&value).ok()
}

/// Stamp `command` with a request id, once, before it is first proposed.
///
/// Stamping must happen **once per logical write, not per attempt** -- a fresh
/// id on every retry is indistinguishable from a fresh command, which is the
/// situation this exists to fix.
pub fn stamp_request_id(command: &[u8], rid: &str) -> Option<Vec<u8>> {
    edit_command_json(command, |object| {
        // Never overwrite one. A forwarded proposal arrives already stamped by
        // the instance the client reached, and that is the id the leader must
        // deduplicate on -- restamping here would give the same logical write
        // two identities, one per hop.
        if object.contains_key("rid") {
            return false;
        }
        object.insert("rid".to_string(), rid.into());
        true
    })
}

/// Just the request id, ignoring every other field.
///
/// A dedicated struct rather than a `Value` parse of the whole payload:
/// serde skips fields it does not recognise instead of materialising them, so
/// this stays cheap even when the command is large -- a retried `ImportState`
/// carries the whole exported state, and `apply` calls this on every proposal
/// before deciding whether to decode the rest.
#[derive(Deserialize)]
struct RidPeek {
    #[serde(default)]
    rid: Option<String>,
}

/// The request id on a proposal, if it carries one.
pub fn request_id_of(command: &[u8]) -> Option<String> {
    serde_json::from_slice::<RidPeek>(command).ok()?.rid
}

/// The `op` of the one command whose clock the leader replaces.
///
/// A literal because [`restamp`] works on the JSON rather than on
/// [`MetaCommand`]; `a_heartbeat_is_restamped` is what keeps it honest if the
/// variant is ever renamed.
const HEARTBEAT_OP: &str = "record_node_heartbeat";
/// The other command carrying a clock reading the leader replaces.
const REPLICA_REPORT_OP: &str = "record_replica_report";

/// Replace the proposer's clock reading in `command` with `now_millis`.
///
/// `None` for every other command, so the caller proposes the original bytes.
///
/// Only a raw "what time is it" is rewritten, and right now that is only a
/// heartbeat's `at_millis`. Cutoffs are not: `ExpireStaleNodes` carries
/// `now - timeout`, and substituting `now` for it would expire the cluster.
/// The other proposer clock, `TakeRefreshToken`'s `now_secs`, is left alone
/// because the `expires_at` it is compared against was stamped by a proposer
/// too — fixing one half would not make that comparison single-clock.
pub fn restamp(command: &[u8], now_millis: u64) -> Option<Vec<u8>> {
    edit_command_json(command, |object| {
        match object.get("op").and_then(serde_json::Value::as_str) {
            Some(HEARTBEAT_OP) => {
                object.insert("at_millis".to_string(), now_millis.into());
                true
            }
            Some(REPLICA_REPORT_OP) => {
                let Some(report) = object.get_mut("report").and_then(|v| v.as_object_mut()) else {
                    return false;
                };
                report.insert("reported_at_millis".to_string(), now_millis.into());
                true
            }
            _ => false,
        }
    })
}

/// Decode a committed command. An unreadable command is an error *response*,
/// not a skip: every replica answers it identically, and the caller sees
/// exactly what the log holds that this build cannot honour.
pub fn decode_command(bytes: &[u8]) -> Result<MetaCommand, MetaError> {
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|err| MetaError::Unsupported(format!("undecodable command: {err}")))?;
    if envelope.v > COMMAND_VERSION {
        return Err(MetaError::Unsupported(format!(
            "command version {} is newer than this build's {}",
            envelope.v, COMMAND_VERSION
        )));
    }
    Ok(envelope.command)
}

/// Serialize a command's outcome for the Raft response channel.
pub fn encode_result(result: &MetaResult) -> Vec<u8> {
    serde_json::to_vec(result).expect("results serialize by construction")
}

pub fn decode_result(bytes: &[u8]) -> Result<MetaResult, MetaError> {
    serde_json::from_slice(bytes)
        .map_err(|err| MetaError::Internal(format!("undecodable result: {err}")))
}

#[cfg(test)]
mod tests;
