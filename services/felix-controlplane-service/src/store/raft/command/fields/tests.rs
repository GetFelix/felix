//! Keeps `fields.txt` in step with the replicated types.
//!
//! The samples below are written as full struct literals, never with
//! `..Default::default()`, so a new field does not compile until it has a
//! value here. Give every optional field a non-default value and key every
//! map by `"*"`, so that the field shows up in the serialized shape. The
//! shape is then compared with the table, and a path the table does not list
//! fails the test until it is given a level.
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::*;
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::auth::refresh_token::{Narrowing, RefreshToken};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, ConsistencyLevel,
    DeliveryGuarantee, MoveReason, Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey,
    Node, NodeCapacity, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest, NodeSpec,
    NodeStatus, ReplicaReport, RetentionPolicy, ShardAssignment, ShardAssignmentChange,
    ShardAssignmentChangeOp, ShardKey, ShardKind, ShardState, Stream, StreamChange, StreamChangeOp,
    StreamKey, StreamKind, StreamPatchRequest, StreamRouting, Tenant, TenantChange, TenantChangeOp,
};
use crate::store::TenantAuthSeed;
use crate::store::export::{ExportedLog, ExportedState};
use crate::store::raft::command::{HeartbeatSeen, METADATA_VERSION, MetaCommand, NodeIncarnation};
use crate::store::raft::state_machine::{AppliedIds, Snapshot};

fn s(value: &str) -> String {
    value.to_string()
}

fn any_map<V>(value: V) -> BTreeMap<String, V> {
    BTreeMap::from([(s(ANY_KEY), value)])
}

fn tenant() -> Tenant {
    Tenant {
        tenant_id: s("t"),
        display_name: s("T"),
    }
}

fn namespace_key() -> NamespaceKey {
    NamespaceKey {
        tenant_id: s("t"),
        namespace: s("n"),
    }
}

fn namespace() -> Namespace {
    Namespace {
        tenant_id: s("t"),
        namespace: s("n"),
        display_name: s("N"),
    }
}

fn stream_key() -> StreamKey {
    StreamKey {
        tenant_id: s("t"),
        namespace: s("n"),
        stream: s("s"),
    }
}

fn retention() -> RetentionPolicy {
    RetentionPolicy {
        max_age_seconds: Some(1),
        max_size_bytes: Some(1),
    }
}

fn stream() -> Stream {
    Stream {
        tenant_id: s("t"),
        namespace: s("n"),
        stream: s("s"),
        kind: StreamKind::Stream,
        shards: 2,
        replication_factor: 3,
        retention: retention(),
        consistency: ConsistencyLevel::Quorum,
        delivery: DeliveryGuarantee::AtLeastOnce,
        durable: true,
        region: Some(s("r")),
        routing: StreamRouting::JumpHash,
    }
}

fn stream_patch() -> StreamPatchRequest {
    StreamPatchRequest {
        retention: Some(retention()),
        consistency: Some(ConsistencyLevel::Leader),
        delivery: Some(DeliveryGuarantee::AtMostOnce),
        durable: Some(true),
    }
}

fn cache_key() -> CacheKey {
    CacheKey {
        tenant_id: s("t"),
        namespace: s("n"),
        cache: s("c"),
    }
}

fn cache() -> Cache {
    Cache {
        tenant_id: s("t"),
        namespace: s("n"),
        cache: s("c"),
        display_name: s("C"),
        shards: 2,
        replication_factor: 3,
        consistency: ConsistencyLevel::Quorum,
    }
}

fn capacity() -> NodeCapacity {
    NodeCapacity {
        max_shards: Some(1),
        weight: 2,
    }
}

fn node() -> Node {
    Node {
        node_id: s("b"),
        spec: NodeSpec {
            advertise_addr: s("a:1"),
            client_addr: Some(s("a:2")),
            kafka_addr: Some(s("a:3")),
            region: s("r"),
            zone: Some(s("z")),
            labels: any_map(s("v")),
            capacity: capacity(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1,
            registered_at_millis: 1,
            incarnation: 1,
            features: BTreeSet::from([s("f")]),
        },
    }
}

fn node_patch() -> NodePatchRequest {
    NodePatchRequest {
        region: Some(s("r")),
        labels: Some(any_map(s("v"))),
        capacity: Some(capacity()),
        lifecycle: Some(NodeLifecycle::Draining),
    }
}

fn shard_key() -> ShardKey {
    ShardKey {
        tenant_id: s("t"),
        namespace: s("n"),
        stream: s("s"),
        shard: 1,
        kind: ShardKind::Cache,
    }
}

fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: shard_key(),
        leader: s("b"),
        replicas: vec![s("c")],
        generation: 1,
        state: ShardState::Draining,
        successor: Some(s("c")),
        joining: Some(s("c")),
        move_started_at_millis: Some(1),
        move_reason: Some(MoveReason::Drain),
    }
}

fn report() -> ReplicaReport {
    ReplicaReport {
        key: shard_key(),
        generation: 1,
        caught_up: BTreeSet::from([s("c")]),
        offsets: any_map(1),
        reported_at_millis: 1,
        drained: true,
        leader_offset: Some(1),
        halted: any_map(crate::model::HaltedCopy {
            reason: s("diverged"),
            generation: 1,
            since_millis: 1,
        }),
    }
}

fn issuer() -> IdpIssuerConfig {
    IdpIssuerConfig {
        issuer: s("https://idp"),
        audiences: vec![s("a")],
        discovery_url: Some(s("https://idp/d")),
        jwks_url: Some(s("https://idp/j")),
        claim_mappings: ClaimMappings {
            subject_claim: s("sub"),
            groups_claim: Some(s("groups")),
        },
    }
}

fn policy() -> PolicyRule {
    PolicyRule {
        subject: s("role:r"),
        object: s("tenant:t"),
        action: s("stream.read"),
    }
}

fn grouping() -> GroupingRule {
    GroupingRule {
        user: s("u"),
        role: s("role:r"),
    }
}

fn signing_key() -> SigningKey {
    SigningKey {
        kid: s("k"),
        alg: jsonwebtoken::Algorithm::EdDSA,
        private_key: [1; 32],
        public_key: [1; 32],
    }
}

fn signing_keys() -> TenantSigningKeys {
    TenantSigningKeys {
        current: signing_key(),
        previous: vec![signing_key()],
    }
}

fn refresh_token() -> RefreshToken {
    RefreshToken {
        token_id: s("i"),
        tenant_id: s("t"),
        principal_id: s("p"),
        groups: vec![s("g")],
        secret_hash: s("h"),
        family_id: s("f"),
        issued_at_secs: 1,
        expires_at_secs: 1,
        used: true,
        revoked: true,
        narrowing: Some(Narrowing {
            requested: Some(vec![s("stream.read")]),
            resources: Some(vec![s("stream:t/n/s")]),
            permissions: Some(vec![s("stream.publish:stream:t/n/s")]),
            audience: s("felix-broker"),
        }),
    }
}

fn log<T>(item: T) -> ExportedLog<T> {
    ExportedLog {
        next_seq: 1,
        items: vec![item],
    }
}

fn exported() -> ExportedState {
    ExportedState {
        v: 1,
        tenants: vec![(s("t"), tenant())],
        namespaces: vec![(namespace_key(), namespace())],
        streams: vec![(stream_key(), stream())],
        caches: vec![(cache_key(), cache())],
        nodes: vec![(s("b"), node())],
        node_changes: log(NodeChange {
            seq: 1,
            op: NodeChangeOp::Registered,
            node_id: s("b"),
            node: Some(node()),
        }),
        shards: vec![(shard_key(), assignment())],
        shard_changes: log(ShardAssignmentChange {
            seq: 1,
            op: ShardAssignmentChangeOp::Assigned,
            key: shard_key(),
            assignment: Some(assignment()),
        }),
        tenant_changes: log(TenantChange {
            seq: 1,
            op: TenantChangeOp::Created,
            tenant_id: s("t"),
            tenant: Some(tenant()),
        }),
        namespace_changes: log(NamespaceChange {
            seq: 1,
            op: NamespaceChangeOp::Created,
            key: namespace_key(),
            namespace: Some(namespace()),
        }),
        stream_changes: log(StreamChange {
            seq: 1,
            op: StreamChangeOp::Created,
            key: stream_key(),
            stream: Some(stream()),
        }),
        cache_changes: log(CacheChange {
            seq: 1,
            op: CacheChangeOp::Created,
            key: cache_key(),
            cache: Some(cache()),
        }),
        idp_issuers: vec![(s("t"), vec![issuer()])],
        tenant_signing_keys: vec![(s("t"), signing_keys())],
        rbac_policies: vec![(s("t"), vec![policy()])],
        rbac_groupings: vec![(s("t"), vec![grouping()])],
        auth_bootstrapped: vec![(s("t"), true)],
        moves_paused: true,
        placement_token: 1,
        placement_holder: Some(s("h")),
        refresh_tokens: vec![refresh_token()],
        fleet_enabled: BTreeSet::from([s("f")]),
        replica_reports: vec![report()],
    }
}

fn snapshot() -> Snapshot {
    Snapshot {
        state: exported(),
        applied: AppliedIds {
            order: VecDeque::from([s("r")]),
            responses: any_map(vec![1]),
        },
    }
}

/// One of every variant. The match in `op_of` stops a new variant from
/// compiling until it has an arm; give it a sample here too.
fn commands() -> Vec<MetaCommand> {
    vec![
        MetaCommand::CreateTenant { tenant: tenant() },
        MetaCommand::DeleteTenant { tenant_id: s("t") },
        MetaCommand::CreateNamespace {
            namespace: namespace(),
        },
        MetaCommand::DeleteNamespace {
            key: namespace_key(),
        },
        MetaCommand::CreateStream { stream: stream() },
        MetaCommand::PatchStream {
            key: stream_key(),
            patch: stream_patch(),
        },
        MetaCommand::DeleteStream { key: stream_key() },
        MetaCommand::CreateCache { cache: cache() },
        MetaCommand::PatchCache {
            key: cache_key(),
            patch: CachePatchRequest {
                display_name: Some(s("C")),
            },
        },
        MetaCommand::DeleteCache { key: cache_key() },
        MetaCommand::RegisterNode { node: node() },
        MetaCommand::PatchNode {
            node_id: s("b"),
            patch: node_patch(),
        },
        MetaCommand::DeleteNode { node_id: s("b") },
        MetaCommand::RecordNodeHeartbeat {
            node_id: s("b"),
            incarnation: 1,
            at_millis: 1,
        },
        MetaCommand::ExpireStaleNodes {
            expiry_before_millis: 1,
        },
        MetaCommand::SetNodeLifecycle {
            node_id: s("b"),
            lifecycle: NodeLifecycle::Draining,
        },
        MetaCommand::PutShardAssignment {
            assignment: assignment(),
        },
        MetaCommand::PutShardAssignmentIf {
            assignment: assignment(),
            expected_generation: Some(1),
        },
        MetaCommand::PutShardAssignmentFenced {
            assignment: assignment(),
            expected_generation: Some(1),
            fence: 1,
        },
        MetaCommand::TakePlacementLease { holder: s("h") },
        MetaCommand::DeleteShardAssignment { key: shard_key() },
        MetaCommand::RecordReplicaReport {
            report: report(),
            leader: Some(s("b")),
        },
        MetaCommand::SetMovesPaused { paused: true },
        MetaCommand::UpsertIdpIssuer {
            tenant_id: s("t"),
            issuer: issuer(),
        },
        MetaCommand::DeleteIdpIssuer {
            tenant_id: s("t"),
            issuer: s("https://idp"),
        },
        MetaCommand::AddRbacPolicy {
            tenant_id: s("t"),
            policy: policy(),
        },
        MetaCommand::AddRbacGrouping {
            tenant_id: s("t"),
            grouping: grouping(),
        },
        MetaCommand::SetTenantSigningKeys {
            tenant_id: s("t"),
            keys: signing_keys(),
        },
        MetaCommand::RemoveRbacPolicy {
            tenant_id: s("t"),
            policy: policy(),
        },
        MetaCommand::RemoveRbacGrouping {
            tenant_id: s("t"),
            grouping: grouping(),
        },
        MetaCommand::StageSigningKey {
            tenant_id: s("t"),
            key: signing_key(),
        },
        MetaCommand::ActivateSigningKey {
            tenant_id: s("t"),
            kid: s("k"),
        },
        MetaCommand::RetireSigningKey {
            tenant_id: s("t"),
            kid: s("k"),
        },
        MetaCommand::EnsureSigningKeys {
            tenant_id: s("t"),
            candidate: signing_keys(),
        },
        MetaCommand::SetTenantAuthBootstrapped {
            tenant_id: s("t"),
            bootstrapped: true,
        },
        MetaCommand::SeedRbac {
            tenant_id: s("t"),
            policies: vec![policy()],
            groupings: vec![grouping()],
        },
        MetaCommand::BootstrapTenantAuth {
            tenant_id: s("t"),
            seed: TenantAuthSeed {
                issuers: vec![issuer()],
                policies: vec![policy()],
                groupings: vec![grouping()],
                signing_keys: signing_keys(),
            },
        },
        MetaCommand::ImportState {
            state: Box::new(exported()),
            overwrite: true,
        },
        MetaCommand::InsertRefreshToken {
            token: refresh_token(),
        },
        MetaCommand::TakeRefreshToken {
            tenant_id: s("t"),
            token_id: s("i"),
            now_secs: 1,
        },
        MetaCommand::RevokeRefreshFamily {
            tenant_id: s("t"),
            family_id: s("f"),
        },
        MetaCommand::RevokeRefreshTokensForPrincipal {
            tenant_id: s("t"),
            principal_id: s("p"),
        },
        MetaCommand::PurgeExpiredRefreshTokens { before_secs: 1 },
        MetaCommand::ExpireNodes {
            nodes: vec![NodeIncarnation {
                node_id: s("b"),
                incarnation: 1,
            }],
        },
        MetaCommand::CheckpointHeartbeats {
            beats: vec![HeartbeatSeen {
                node_id: s("b"),
                incarnation: 1,
                at_millis: 1,
            }],
        },
        MetaCommand::RegisterNodeInFleet { node: node() },
        MetaCommand::FinalizeFleetFeature { feature: s("f") },
    ]
}

fn op_of(command: &MetaCommand) -> &'static str {
    match command {
        MetaCommand::CreateTenant { .. } => "create_tenant",
        MetaCommand::DeleteTenant { .. } => "delete_tenant",
        MetaCommand::CreateNamespace { .. } => "create_namespace",
        MetaCommand::DeleteNamespace { .. } => "delete_namespace",
        MetaCommand::CreateStream { .. } => "create_stream",
        MetaCommand::PatchStream { .. } => "patch_stream",
        MetaCommand::DeleteStream { .. } => "delete_stream",
        MetaCommand::CreateCache { .. } => "create_cache",
        MetaCommand::PatchCache { .. } => "patch_cache",
        MetaCommand::DeleteCache { .. } => "delete_cache",
        MetaCommand::RegisterNode { .. } => "register_node",
        MetaCommand::PatchNode { .. } => "patch_node",
        MetaCommand::DeleteNode { .. } => "delete_node",
        MetaCommand::RecordNodeHeartbeat { .. } => "record_node_heartbeat",
        MetaCommand::ExpireStaleNodes { .. } => "expire_stale_nodes",
        MetaCommand::SetNodeLifecycle { .. } => "set_node_lifecycle",
        MetaCommand::PutShardAssignment { .. } => "put_shard_assignment",
        MetaCommand::PutShardAssignmentIf { .. } => "put_shard_assignment_if",
        MetaCommand::PutShardAssignmentFenced { .. } => "put_shard_assignment_fenced",
        MetaCommand::TakePlacementLease { .. } => "take_placement_lease",
        MetaCommand::DeleteShardAssignment { .. } => "delete_shard_assignment",
        MetaCommand::RecordReplicaReport { .. } => "record_replica_report",
        MetaCommand::SetMovesPaused { .. } => "set_moves_paused",
        MetaCommand::UpsertIdpIssuer { .. } => "upsert_idp_issuer",
        MetaCommand::DeleteIdpIssuer { .. } => "delete_idp_issuer",
        MetaCommand::AddRbacPolicy { .. } => "add_rbac_policy",
        MetaCommand::AddRbacGrouping { .. } => "add_rbac_grouping",
        MetaCommand::SetTenantSigningKeys { .. } => "set_tenant_signing_keys",
        MetaCommand::RemoveRbacPolicy { .. } => "remove_rbac_policy",
        MetaCommand::RemoveRbacGrouping { .. } => "remove_rbac_grouping",
        MetaCommand::StageSigningKey { .. } => "stage_signing_key",
        MetaCommand::ActivateSigningKey { .. } => "activate_signing_key",
        MetaCommand::RetireSigningKey { .. } => "retire_signing_key",
        MetaCommand::EnsureSigningKeys { .. } => "ensure_signing_keys",
        MetaCommand::SetTenantAuthBootstrapped { .. } => "set_tenant_auth_bootstrapped",
        MetaCommand::SeedRbac { .. } => "seed_rbac",
        MetaCommand::BootstrapTenantAuth { .. } => "bootstrap_tenant_auth",
        MetaCommand::ImportState { .. } => "import_state",
        MetaCommand::InsertRefreshToken { .. } => "insert_refresh_token",
        MetaCommand::TakeRefreshToken { .. } => "take_refresh_token",
        MetaCommand::RevokeRefreshFamily { .. } => "revoke_refresh_family",
        MetaCommand::RevokeRefreshTokensForPrincipal { .. } => {
            "revoke_refresh_tokens_for_principal"
        }
        MetaCommand::PurgeExpiredRefreshTokens { .. } => "purge_expired_refresh_tokens",
        MetaCommand::ExpireNodes { .. } => "expire_nodes",
        MetaCommand::CheckpointHeartbeats { .. } => "checkpoint_heartbeats",
        MetaCommand::RegisterNodeInFleet { .. } => "register_node_in_fleet",
        MetaCommand::FinalizeFleetFeature { .. } => "finalize_fleet_feature",
    }
}

/// Every path the samples serialize, rooted at the op for a command and at
/// `snapshot` for the snapshot. The op itself is a path too, carrying the
/// variant's level.
fn shape() -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for command in commands() {
        let value = serde_json::to_value(&command).expect("serializes");
        let op = value["op"].as_str().expect("op").to_string();
        assert_eq!(op, op_of(&command));
        paths.insert(op.clone());
        for (key, child) in value.as_object().expect("object") {
            if key == "op" {
                continue;
            }
            let mut path = format!("{op}.{key}");
            paths.insert(path.clone());
            walk(child, &mut path, &|_| false, &mut |p, _| {
                paths.insert(p.to_string());
            });
        }
    }
    let value = serde_json::to_value(snapshot()).expect("serializes");
    walk(&value, &mut s("snapshot"), &|_| false, &mut |p, _| {
        paths.insert(p.to_string());
    });
    paths
}

#[test]
fn the_table_lists_every_replicated_field() {
    let table = parse(TABLE);
    let shape = shape();
    let missing: Vec<_> = shape
        .iter()
        .filter(|path| !table.contains_key(path.as_str()))
        .map(|path| format!("{METADATA_VERSION} {path}"))
        .collect();
    let stale: Vec<_> = table
        .keys()
        .filter(|path| !shape.contains(**path))
        .collect();
    assert!(
        missing.is_empty(),
        "replicated fields missing from fields.txt. An older member drops a \
         field it does not know, so each new one needs a new metadata version: \
         raise METADATA_VERSION and add these at that level (a field the \
         proposer must never drop also needs a fallback or a refusal below it):\n{}",
        missing.join("\n")
    );
    assert!(
        stale.is_empty(),
        "fields.txt lists paths no replicated type has: {stale:?}"
    );
}

#[test]
fn no_level_is_newer_than_this_build() {
    for (path, level) in parse(TABLE) {
        assert!(
            level <= METADATA_VERSION,
            "{path} is at level {level}, above METADATA_VERSION {METADATA_VERSION}"
        );
    }
}

#[test]
fn each_variant_is_listed_at_its_level() {
    let table = parse(TABLE);
    for command in commands() {
        let op = op_of(&command);
        assert_eq!(
            table.get(op).copied(),
            Some(command.variant_level()),
            "{op} is listed at a different level than MetaCommand::variant_level gives"
        );
    }
}

/// The samples set every field, so each command's level is the highest of
/// anything it can carry.
#[test]
fn a_command_needs_the_level_of_its_newest_field() {
    let table = parse(TABLE);
    let shape = shape();
    for command in commands() {
        let op = op_of(&command);
        let newest = shape
            .iter()
            .filter(|path| path.as_str() == op || path.starts_with(&format!("{op}.")))
            .map(|path| table[path.as_str()])
            .max()
            .unwrap_or(0);
        assert_eq!(command.version(), newest, "{op}");
    }
}

#[test]
fn a_command_at_its_defaults_needs_only_its_variant() {
    let mut plain = stream();
    plain.routing = StreamRouting::Modulo;
    assert_eq!(MetaCommand::CreateStream { stream: plain }.version(), 0);
    assert_eq!(MetaCommand::CreateStream { stream: stream() }.version(), 3);

    let mut token = refresh_token();
    token.narrowing = None;
    assert_eq!(MetaCommand::InsertRefreshToken { token }.version(), 0);
}

#[test]
fn map_keys_match_the_wildcard() {
    let labelled = MetaCommand::RegisterNode {
        node: Node {
            spec: NodeSpec {
                labels: BTreeMap::from([(s("rack"), s("7"))]),
                zone: None,
                ..node().spec
            },
            status: NodeStatus {
                features: BTreeSet::new(),
                ..node().status
            },
            ..node()
        },
    };
    assert_eq!(labelled.version(), 0);
}

#[test]
fn a_dropped_field_is_named_and_an_empty_one_is_not() {
    let original = serde_json::json!({
        "stream": {"shards": 2, "routing": "jump_hash", "flag": false, "note": null},
        "list": [{"a": 1, "extra": {"deep": 1}}],
    });
    let decoded = serde_json::json!({
        "stream": {"shards": 2},
        "list": [{"a": 1}],
    });
    assert_eq!(
        dropped_fields(&original, &decoded),
        [s(".list[].extra"), s(".stream.routing")]
    );
    assert!(dropped_fields(&decoded, &decoded).is_empty());
}
