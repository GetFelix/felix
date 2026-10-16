//! The Raft backend against the same contracts every other backend passes.
//!
//! A single-member group needs no network, so these run as plain unit tests:
//! the node elects itself, and every write still travels the full path —
//! encode, propose, commit, apply, decode — that a three-member group uses.
use std::collections::BTreeMap;
use std::time::Duration;

use super::*;
use crate::raft::{AppStateMachine, RaftHandle, RaftSettings};
use crate::store::StoreConfig;

pub(super) async fn single_node_store(dir: &std::path::Path) -> Arc<RaftStore> {
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(
        1,
        dir.into(),
        crate::raft::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start raft");
    handle
        .initialize(BTreeMap::from([(1, "127.0.0.1:0".to_string())]))
        .await
        .expect("initialize single-member group");
    // A single member elects itself; writes block until then, so wait here
    // rather than in every contract step.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while handle.status().leader.is_none() {
        assert!(std::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Arc::new(RaftStore::new(handle, machine))
}

/// The same suite memory and Postgres run: the Raft backend is a fourth
/// implementation of the same contract, not a new contract.
#[tokio::test]
async fn satisfies_the_node_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::nodes::run_node_contract(store.clone()).await;
    crate::store::contract::nodes::run_node_concurrency_contract(store.clone()).await;
    crate::store::contract::nodes::run_fleet_contract(store.clone()).await;
    crate::store::contract::suspicions::run_suspicion_contract(store).await;
}

#[tokio::test]
async fn satisfies_the_shard_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::shards::run_shard_contract(store.clone()).await;
    crate::store::contract::shards::run_shard_concurrency_contract(store.clone()).await;
    crate::store::contract::shards::run_node_delete_race_contract(store, 20).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_signing_key_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::signing_keys::run_signing_key_contract(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_incarnation_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::incarnations::run_incarnation_contract(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_resources_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::resources::run_resources_contract(store.clone()).await;
    crate::store::contract::resources::run_resources_race_contract(store, 20).await;
}

#[tokio::test]
async fn satisfies_the_rbac_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::rbac::run_rbac_contract(store).await;
}

/// Pages are read from the local replica, with the leader's heartbeats
/// overlaid on nodes.
#[tokio::test]
async fn satisfies_the_pagination_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::pagination::run_pagination_contract(store).await;
}

#[tokio::test]
async fn satisfies_the_placement_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::placement::run_placement_contract(store.clone(), store).await;
}

/// The lease expires as it does on the other backends, even though its
/// renewals are the leader's soft state.
#[tokio::test]
async fn satisfies_the_expiring_lease_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::placement::run_expiring_lease_contract(store.clone(), store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_refresh_token_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::refresh_tokens::run_refresh_contract(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_revocation_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::revocations::run_revocation_contract(store).await;
}

/// A snapshot carries the token and holder, so a replica restored from one
/// decides a fenced write exactly as the replicas that applied the log did.
#[tokio::test]
async fn a_snapshot_carries_the_placement_token() {
    let source = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let lease = source.take_placement_lease("leader").await;
    let restored = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    restored
        .import_state(source.export_state().await)
        .await
        .expect("import");
    assert_eq!(
        restored.placement_token().await.expect("token"),
        lease.token
    );
    assert_eq!(restored.placement_holder().await.as_deref(), Some("leader"));
}

/// Writes travel the log; reads come from applied state — so a write
/// through the trait must be immediately visible to a read through the
/// trait on the same instance (the proposal only returns after apply).
#[tokio::test]
async fn a_write_is_readable_once_acknowledged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;

    let created = store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("create");
    assert_eq!(created.tenant_id, "t1");

    let listed = store.list_tenants().await.expect("list");
    assert_eq!(listed.len(), 1);

    let conflict = store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Again".to_string(),
        })
        .await;
    assert!(
        matches!(conflict, Err(StoreError::Conflict(_))),
        "store errors survive the encode/decode round trip"
    );
}

/// `ensure` proposes install-if-absent, so racing it against itself — or
/// against an already-committed rotation — never clobbers keys.
#[tokio::test]
async fn ensure_signing_keys_never_overwrites() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("create");

    let first = store
        .ensure_signing_key_current("t1")
        .await
        .expect("ensure");
    let second = store
        .ensure_signing_key_current("t1")
        .await
        .expect("ensure");
    assert_eq!(
        first.current.kid, second.current.kid,
        "a second ensure returns the keys the first installed"
    );
}

/// A peer that answers `standing`, reporting `version` — or, at 0,
/// nothing, as a build from before metadata versions does. It accepts every
/// append so the leader has nothing to retry.
/// A [`stub_member`] level that makes it answer no probe.
const SILENT: u16 = u16::MAX;

async fn stub_member(version: Arc<std::sync::atomic::AtomicU16>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let app = axum::Router::new()
        .route(
            "/internal/raft/append-entries",
            axum::routing::post(|| async { axum::Json(serde_json::json!({ "Ok": "Success" })) }),
        )
        .route(
            "/internal/raft/standing",
            axum::routing::get(move || {
                let version = version.load(std::sync::atomic::Ordering::SeqCst);
                async move {
                    // `SILENT` stands for a member that does not answer.
                    if version == SILENT {
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    Ok(axum::Json(if version == 0 {
                        serde_json::json!({ "last_log_index": null })
                    } else {
                        serde_json::json!({ "last_log_index": null, "version": version })
                    }))
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

/// A learner that answers every append with an error is retried with a
/// backoff, not in a loop: a dead member must not cost the leader a busy
/// stream of failed RPCs.
#[tokio::test]
async fn a_failing_learner_is_not_retried_in_a_loop() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let appends = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let counted = Arc::clone(&appends);
    let app = axum::Router::new().route(
        "/internal/raft/append-entries",
        axum::routing::post(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async { axum::http::StatusCode::NOT_FOUND }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await });
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("add learner");

    tokio::time::sleep(Duration::from_secs(1)).await;
    let sent = appends.load(Ordering::SeqCst);
    assert!(sent > 0, "the leader never tried the learner");
    // About six with a 50 ms heartbeat; a loop without backoff sends
    // hundreds.
    assert!(
        sent < 50,
        "{sent} appends to a failing learner in one second"
    );
}

/// With a member that predates the newer commands, the leader proposes
/// none of them: rule removal is refused before it reaches the log, and
/// liveness stays on the log commands that member can apply. Once it
/// reports the level, rule removal goes through.
#[tokio::test]
async fn nothing_newer_than_the_oldest_member_is_proposed() {
    use crate::auth::rbac::policy_store::PolicyRule;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let applied = || store.handle.status().last_applied_index;
    let version = Arc::new(std::sync::atomic::AtomicU16::new(0));
    let addr = stub_member(Arc::clone(&version)).await;
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("add old member");

    let mut quiet = crate::store::contract::nodes::node("broker-quiet", 7001);
    // Stale by any cutoff under the log's rules; soft state would instead
    // give it a full window from when the leader began judging.
    quiet.status.last_heartbeat_at_millis = 1;
    // Registered the way the old member can apply: without its features.
    quiet.status.features = ["x".to_string()].into();
    let (registered, fleet) = store.register_node_in_fleet(quiet).await.expect("register");
    assert!(registered.status.features.is_empty(), "{registered:?}");
    assert!(fleet.is_empty());
    assert!(
        store.finalize_fleet_feature("x").await.is_err(),
        "an old member cannot apply a finalize"
    );

    let policy = PolicyRule {
        subject: "role:reader".to_string(),
        object: "tenant:t1".to_string(),
        action: "stream.read".to_string(),
    };
    let before = applied();
    let refused = store.remove_rbac_policy("t1", policy.clone()).await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("metadata version")),
        "a command the old member cannot apply was proposed: {refused:?}"
    );
    assert_eq!(applied(), before, "the refused command reached the log");

    let now = store.now_millis().await.expect("now");
    let expired = store
        .expire_stale_nodes(now.saturating_sub(60_000))
        .await
        .expect("sweep");
    assert_eq!(
        expired
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        ["broker-quiet"],
        "expiry went through the leader's soft state (`ExpireNodes`)"
    );

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match store.remove_rbac_policy("t1", policy.clone()).await {
            Err(StoreError::NotFound(_)) => break,
            other => assert!(
                std::time::Instant::now() < deadline,
                "still refused after every member reported the level: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And now registration keeps them.
    let mut again = crate::store::contract::nodes::node("broker-quiet", 7001);
    again.status.features = ["x".to_string()].into();
    let (registered, fleet) = store
        .register_node_in_fleet(again)
        .await
        .expect("register again");
    assert!(registered.status.features.contains("x"), "{registered:?}");
    assert!(fleet.is_empty(), "supported, not yet enabled: {fleet:?}");
    let enabled = store.finalize_fleet_feature("x").await.expect("finalize");
    assert!(enabled.contains("x"), "{enabled:?}");
}

/// A group with one member behind, and a stream store to create into.
async fn store_with_member_at(
    dir: &std::path::Path,
    level: u16,
) -> (Arc<RaftStore>, Arc<std::sync::atomic::AtomicU16>) {
    let store = single_node_store(dir).await;
    let version = Arc::new(std::sync::atomic::AtomicU16::new(level));
    let addr = stub_member(Arc::clone(&version)).await;
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("add old member");
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(crate::model::Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "Ns".to_string(),
        })
        .await
        .expect("namespace");
    (store, version)
}

fn routed_stream(name: &str, routing: crate::model::StreamRouting) -> Stream {
    Stream {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: name.to_string(),
        kind: crate::model::StreamKind::Stream,
        shards: 4,
        replication_factor: 1,
        retention: crate::model::RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: crate::model::ConsistencyLevel::Leader,
        delivery: crate::model::DeliveryGuarantee::AtLeastOnce,
        durable: false,
        region: None,
        routing,
        incarnation: 0,
    }
}

/// A member before jump-hash routing would store a jump-hash stream as
/// modulo. So while one is in the group, the stream is refused before it
/// reaches the log, and so is finalizing the feature that makes it the
/// default; once every member has it, the routing is kept.
#[tokio::test]
async fn a_jump_hash_stream_waits_for_every_member() {
    use crate::model::StreamRouting;
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 2).await;
    let applied = || store.handle.status().last_applied_index;

    let before = applied();
    let refused = store
        .create_stream(routed_stream("jumpy", StreamRouting::JumpHash))
        .await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("metadata version 3")),
        "a jump-hash stream was proposed past a member that would drop its routing: {refused:?}"
    );
    assert_eq!(applied(), before, "the refused stream reached the log");
    let refused = store
        .finalize_fleet_feature(felix_common::fleet::JUMP_HASH_ROUTING.name())
        .await;
    assert!(refused.is_err(), "{refused:?}");

    // Modulo is what the old member stores anyway.
    store
        .create_stream(routed_stream("plain", StreamRouting::Modulo))
        .await
        .expect("a modulo stream needs nothing new");

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let created = loop {
        match store
            .create_stream(routed_stream("jumpy", StreamRouting::JumpHash))
            .await
        {
            Ok(stream) => break stream,
            other => assert!(
                std::time::Instant::now() < deadline,
                "still refused after every member reported the level: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(created.routing, StreamRouting::JumpHash);
    let key = crate::model::StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "jumpy".to_string(),
    };
    assert_eq!(
        store.get_stream(&key).await.expect("read back").routing,
        StreamRouting::JumpHash
    );
}

/// A member before level 10 would drop a stream's incarnation, so finalizing
/// the feature that gives one is refused until every member has it, and an
/// incarnated stream is refused before it reaches the log.
#[tokio::test]
async fn an_incarnated_stream_waits_for_every_member() {
    use crate::model::StreamRouting;
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 9).await;
    let refused = store
        .finalize_fleet_feature(felix_common::fleet::STREAM_INCARNATIONS.name())
        .await;
    assert!(refused.is_err(), "{refused:?}");
    let incarnated = Stream {
        incarnation: 7,
        ..routed_stream("again", StreamRouting::Modulo)
    };
    let refused = store.create_stream(incarnated.clone()).await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("metadata version 10")),
        "{refused:?}"
    );

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let created = loop {
        match store.create_stream(incarnated.clone()).await {
            Ok(stream) => break stream,
            other => assert!(
                std::time::Instant::now() < deadline,
                "still refused after every member reported the level: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // Not finalized yet, so the stream is at 0 whatever the command carried.
    assert_eq!(created.incarnation, 0);
    let key = crate::model::StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "again".to_string(),
    };
    store.delete_stream(&key).await.expect("delete");
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
    store
        .register_node(crate::store::contract::nodes::with_features(
            crate::store::contract::nodes::node("broker-i", 7301),
            &[feature],
        ))
        .await
        .expect("report the feature");
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");
    let created = store
        .create_stream(incarnated.clone())
        .await
        .expect("create");
    assert_eq!(created.incarnation, 7);
    assert_eq!(
        store.get_stream(&key).await.expect("read back").incarnation,
        7
    );
}

/// A member before level 10 leaves a deleted stream's replica reports, and
/// leaves every shard of a deleted namespace or tenant. While one is in the
/// group, deletes are proposed that way on every member; once every member
/// has the level, they take both.
#[tokio::test]
async fn deletes_take_shard_state_only_once_every_member_does() {
    use crate::model::{NamespaceKey, ReplicaReport, ShardAssignment, ShardKey, ShardKind};
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 8).await;
    store
        .register_node(crate::store::contract::nodes::node("broker-x", 7101))
        .await
        .expect("register");
    let key = |namespace: &str, stream: &str| ShardKey {
        tenant_id: "t1".to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    // A stream with its shard 0 assigned and reported on.
    let reported = |namespace: &'static str, name: &'static str| {
        let store = Arc::clone(&store);
        async move {
            let mut stream = routed_stream(name, Default::default());
            stream.namespace = namespace.to_string();
            store.create_stream(stream).await.expect("stream");
            let assigned = store
                .put_shard_assignment(ShardAssignment {
                    key: key(namespace, name),
                    leader: "broker-x".to_string(),
                    replicas: Vec::new(),
                    generation: 0,
                    state: crate::model::ShardState::Active,
                    successor: None,
                    joining: None,
                    move_started_at_millis: None,
                    move_reason: None,
                    incarnation: 0,
                })
                .await
                .expect("assign");
            store
                .record_replica_report(
                    ReplicaReport {
                        key: key(namespace, name),
                        generation: assigned.generation,
                        caught_up: Default::default(),
                        offsets: Default::default(),
                        reported_at_millis: 1,
                        drained: false,
                        leader_offset: Some(5),
                        halted: Default::default(),
                        counter_offsets: Default::default(),
                        stream_incarnation: None,
                        by_round: false,
                        leader_incarnation: None,
                        follower_storage: Default::default(),
                    },
                    "broker-x",
                )
                .await
                .expect("report");
        }
    };
    let left = |namespace: &'static str, name: &'static str| {
        let store = Arc::clone(&store);
        async move {
            let target = key(namespace, name);
            let assignments = store.list_shard_assignments().await.expect("list");
            let reports = store.list_replica_reports().await.expect("reports");
            (
                assignments.iter().any(|a| a.key == target),
                reports.iter().any(|r| r.key == target),
            )
        }
    };
    let delete_stream = |namespace: &str, name: &str| crate::model::StreamKey {
        tenant_id: "t1".to_string(),
        namespace: namespace.to_string(),
        stream: name.to_string(),
    };
    let doomed = |namespace: &str| {
        let store = Arc::clone(&store);
        let namespace = namespace.to_string();
        async move {
            store
                .create_namespace(crate::model::Namespace {
                    tenant_id: "t1".to_string(),
                    namespace: namespace.clone(),
                    display_name: "Doomed".to_string(),
                })
                .await
                .expect("namespace");
        }
    };

    assert!(!store.deletes_take_shards().await);
    reported("ns", "old-single").await;
    store
        .delete_stream(&delete_stream("ns", "old-single"))
        .await
        .expect("delete");
    assert_eq!(left("ns", "old-single").await, (false, true));
    doomed("old-ns").await;
    reported("old-ns", "cascaded").await;
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "old-ns".to_string(),
        })
        .await
        .expect("delete");
    assert_eq!(left("old-ns", "cascaded").await, (true, true));

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !store.deletes_take_shards().await {
        assert!(
            std::time::Instant::now() < deadline,
            "deletes still leave shard state after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    reported("ns", "new-single").await;
    store
        .delete_stream(&delete_stream("ns", "new-single"))
        .await
        .expect("delete");
    assert_eq!(left("ns", "new-single").await, (false, false));
    doomed("new-ns").await;
    reported("new-ns", "cascaded").await;
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "new-ns".to_string(),
        })
        .await
        .expect("delete");
    assert_eq!(left("new-ns", "cascaded").await, (false, false));
}

/// A member before zones would store a node without its zone. Below that
/// level the node is registered without one on every member, rather than
/// with one on some.
#[tokio::test]
async fn a_zone_is_kept_only_once_every_member_has_zones() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 1).await;

    let mut zoned = crate::store::contract::nodes::node("broker-z", 7101);
    zoned.spec.zone = Some("zone-a".to_string());
    let (registered, _) = store
        .register_node_in_fleet(zoned.clone())
        .await
        .expect("register");
    assert_eq!(registered.spec.zone, None, "{registered:?}");
    assert_eq!(
        store.get_node("broker-z").await.expect("node").spec.zone,
        None
    );

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (registered, _) = store
            .register_node_in_fleet(zoned.clone())
            .await
            .expect("register again");
        if registered.spec.zone.as_deref() == Some("zone-a") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "zone still dropped after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A member before halted copies would drop them from a report it applies,
/// and the same for counter positions. So while one is in the group the
/// report is recorded without them, and it is not refused: placement needs
/// the rest of it. Once every member has the level, both are kept.
#[tokio::test]
async fn halted_copies_wait_for_every_member_and_the_report_does_not() {
    use crate::model::{
        HaltedCopy, ReplicaReport, ShardAssignment, ShardKey, ShardKind, ShardState,
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 3).await;
    store
        .create_stream(routed_stream("orders", Default::default()))
        .await
        .expect("stream");
    for (id, port) in [("broker-x", 7101), ("broker-y", 7102)] {
        store
            .register_node(crate::store::contract::nodes::node(id, port))
            .await
            .expect("register");
    }
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let assigned = store
        .put_shard_assignment(ShardAssignment {
            key: key.clone(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Active,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            incarnation: 0,
        })
        .await
        .expect("assign");
    let report = ReplicaReport {
        counter_offsets: [("broker-y".to_string(), 7)].into(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: key.clone(),
        generation: assigned.generation,
        caught_up: Default::default(),
        offsets: Default::default(),
        reported_at_millis: 1,
        drained: false,
        leader_offset: None,
        halted: [(
            "broker-y".to_string(),
            HaltedCopy {
                reason: "diverged".to_string(),
                generation: assigned.generation,
                since_millis: 1,
            },
        )]
        .into(),
        follower_storage: Default::default(),
    };
    let held = || async {
        store
            .list_replica_reports()
            .await
            .expect("reports")
            .into_iter()
            .find(|held| held.key == key)
            .expect("the report was recorded")
    };

    assert_eq!(
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("recorded without the halts"),
        crate::store::ReportWrite::Stored
    );
    assert!(held().await.halted.is_empty());
    assert!(held().await.counter_offsets.is_empty());

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("record");
        let held = held().await;
        if held.halted.contains_key("broker-y") && held.counter_offsets.contains_key("broker-y") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "halts still dropped after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A member before `by_round` would drop it, and its placement would read the
/// leader as on its lease while the others did not. So until every member has
/// the level the report is recorded without it, which is the safe reading, and
/// the halts and counter positions a member at level 10 can apply are kept.
#[tokio::test]
async fn by_round_waits_for_every_member_and_the_rest_of_the_report_does_not() {
    use crate::model::{
        HaltedCopy, ReplicaReport, ShardAssignment, ShardKey, ShardKind, ShardState,
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 10).await;
    store
        .create_stream(routed_stream("orders", Default::default()))
        .await
        .expect("stream");
    for (id, port) in [("broker-x", 7101), ("broker-y", 7102)] {
        store
            .register_node(crate::store::contract::nodes::node(id, port))
            .await
            .expect("register");
    }
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let assigned = store
        .put_shard_assignment(ShardAssignment {
            key: key.clone(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Active,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            incarnation: 0,
        })
        .await
        .expect("assign");
    let report = ReplicaReport {
        counter_offsets: [("broker-y".to_string(), 7)].into(),
        stream_incarnation: None,
        by_round: true,
        leader_incarnation: Some(1),
        key: key.clone(),
        generation: assigned.generation,
        caught_up: Default::default(),
        offsets: Default::default(),
        reported_at_millis: 1,
        drained: false,
        leader_offset: None,
        halted: [(
            "broker-y".to_string(),
            HaltedCopy {
                reason: "diverged".to_string(),
                generation: assigned.generation,
                since_millis: 1,
            },
        )]
        .into(),
        follower_storage: Default::default(),
    };
    let held = || async {
        store
            .list_replica_reports()
            .await
            .expect("reports")
            .into_iter()
            .find(|held| held.key == key)
            .expect("the report was recorded")
    };

    assert_eq!(
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("recorded without by_round"),
        crate::store::ReportWrite::Stored
    );
    let first = held().await;
    assert!(!first.by_round);
    assert!(first.halted.contains_key("broker-y"));
    assert!(first.counter_offsets.contains_key("broker-y"));

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("record");
        if held().await.by_round {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "by_round still dropped after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A member before level 12 would drop a node's storage identity and a
/// report's follower identities, and its placement would then judge the
/// report differently from the rest. So until every member has the level both
/// are dropped, which placement reads as unverified, and the rest of the
/// registration and report (with `by_round`, level 11) is kept.
#[tokio::test]
async fn storage_ids_wait_for_every_member() {
    use crate::model::{ReplicaReport, ShardAssignment, ShardKey, ShardKind, ShardState};
    let id = "0123456789abcdef0123456789abcdef".to_string();
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 11).await;
    store
        .create_stream(routed_stream("orders", Default::default()))
        .await
        .expect("stream");
    let with_id = |node_id: &str, port: u16| {
        let mut node = crate::store::contract::nodes::node(node_id, port);
        node.status.storage_id = Some(id.clone());
        node
    };
    for (node_id, port) in [("broker-x", 7101), ("broker-y", 7102)] {
        let registered = store
            .register_node(crate::store::contract::nodes::with_features(
                with_id(node_id, port),
                &[],
            ))
            .await
            .expect("register");
        assert_eq!(registered.status.storage_id, None);
    }
    let mut zoned = with_id("broker-y", 7102);
    zoned.spec.zone = Some("z1".to_string());
    let registered = store.register_node(zoned).await.expect("register");
    assert_eq!(registered.status.storage_id, None);
    assert_eq!(
        registered.spec.zone.as_deref(),
        Some("z1"),
        "dropping the identity dropped the rest of the registration"
    );
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    let assigned = store
        .put_shard_assignment(ShardAssignment {
            key: key.clone(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Active,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            incarnation: 0,
        })
        .await
        .expect("assign");
    let report = ReplicaReport {
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: true,
        leader_incarnation: Some(1),
        key: key.clone(),
        generation: assigned.generation,
        caught_up: ["broker-y".to_string()].into(),
        offsets: [("broker-y".to_string(), 3)].into(),
        reported_at_millis: 1,
        drained: false,
        leader_offset: Some(3),
        halted: Default::default(),
        follower_storage: [("broker-y".to_string(), id.clone())].into(),
    };
    let held = || async {
        store
            .list_replica_reports()
            .await
            .expect("reports")
            .into_iter()
            .find(|held| held.key == key)
            .expect("the report was recorded")
    };

    assert_eq!(
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("recorded without identities"),
        crate::store::ReportWrite::Stored
    );
    let first = held().await;
    assert!(first.follower_storage.is_empty());
    assert!(first.by_round);

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        store
            .record_replica_report(report.clone(), "broker-x")
            .await
            .expect("record");
        if !held().await.follower_storage.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "follower identities still dropped after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(held().await.follower_storage, report.follower_storage);
    let registered = store
        .register_node(with_id("broker-y", 7102))
        .await
        .expect("register");
    assert_eq!(registered.status.storage_id, Some(id.clone()));
}

/// A member before pair narrowing would drop `permissions` from a refresh
/// record and refresh the chain by `requested: []` alone. The record is
/// refused until every member has the level; one without pairs is not held up.
#[tokio::test]
async fn pair_narrowing_waits_for_every_member() {
    use crate::auth::refresh_token::Narrowing;

    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 4).await;
    let record = |permissions: Option<Vec<String>>| {
        let (mut record, _) = crate::auth::refresh::issue(
            "t1",
            "p:dev",
            Vec::new(),
            None,
            1,
            Duration::from_secs(60),
        );
        record.narrowing = Some(Narrowing {
            requested: Some(Vec::new()),
            resources: None,
            permissions,
            audience: "felix-broker".to_string(),
            may_act: None,
        });
        record
    };
    let pairs = Some(vec!["stream.publish:stream:t1/rooms/a".to_string()]);

    let refused = store.insert_refresh_token(record(pairs.clone())).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict(_))),
        "{refused:?}"
    );
    store
        .insert_refresh_token(record(None))
        .await
        .expect("a record without pairs");

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store
        .insert_refresh_token(record(pairs.clone()))
        .await
        .is_err()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "pairs still refused after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A member before `may_act` would drop it from a refresh record, and the
/// chain's refreshed tokens could no longer be delegated by the gateway the
/// exchange named. Refused until every member has the level.
#[tokio::test]
async fn a_may_act_record_waits_for_every_member() {
    use crate::auth::refresh_token::Narrowing;

    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) = store_with_member_at(dir.path(), 5).await;
    let record = |may_act: Option<&str>| {
        let (mut record, _) = crate::auth::refresh::issue(
            "t1",
            "p:dev",
            Vec::new(),
            None,
            1,
            Duration::from_secs(60),
        );
        record.narrowing = Some(Narrowing {
            requested: None,
            resources: None,
            permissions: None,
            audience: "felix-broker".to_string(),
            may_act: may_act.map(str::to_string),
        });
        record
    };

    let refused = store.insert_refresh_token(record(Some("p:gateway"))).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict(_))),
        "{refused:?}"
    );
    store
        .insert_refresh_token(record(None))
        .await
        .expect("a record without may_act");

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while store
        .insert_refresh_token(record(Some("p:gateway")))
        .await
        .is_err()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "may_act still refused after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Revocation cutoffs are read on the leader, so a member that has not
/// applied a revocation yet still sees it. Modelled with a second store over
/// the same group whose own state is empty, as a lagging follower's would be.
#[tokio::test]
async fn revocation_cutoffs_are_read_on_the_leader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = single_node_store(dir.path()).await;
    leader
        .revoke_principals("t1", vec!["p:gateway".to_string()], 1_000)
        .await
        .expect("revoke");
    let empty = Arc::new(MetadataStateMachine::new(Arc::new(InMemoryStore::new(
        StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        },
    ))));
    let behind = RaftStore::new(leader.handle.clone(), empty);
    assert_eq!(
        behind
            .principal_revoked_at("t1", "p:gateway")
            .await
            .expect("read"),
        Some(1_000)
    );
}

/// With a member that predates the combined entries, an auth change and its
/// revocation, and a refresh rotation, still go through as the separate
/// entries that member can apply.
#[tokio::test]
async fn combined_writes_fall_back_below_their_level() {
    use crate::auth::rbac::policy_store::PolicyRule;
    use crate::auth::refresh_token::RefreshTokenTake;
    use crate::store::{AuthChange, Revocation};
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, _version) = store_with_member_at(dir.path(), 8).await;
    let policy = PolicyRule {
        subject: "p:alice".to_string(),
        object: "tenant:t1".to_string(),
        action: "stream.publish".to_string(),
    };
    store
        .add_rbac_policy("t1", policy.clone())
        .await
        .expect("policy");
    store
        .change_auth(
            "t1",
            AuthChange::RemovePolicy(policy),
            Some(Revocation {
                principal_ids: vec!["p:alice".to_string()],
                revoked_at: 100,
            }),
        )
        .await
        .expect("change");
    assert_eq!(
        store
            .principal_revoked_at("t1", "p:alice")
            .await
            .expect("read"),
        Some(100)
    );

    let (token, _) =
        crate::auth::refresh::issue("t1", "p:alice", vec![], None, 1, Duration::from_secs(60));
    let (next, _) = crate::auth::refresh::issue(
        "t1",
        "p:alice",
        vec![],
        Some(token.family_id.clone()),
        1,
        Duration::from_secs(60),
    );
    let next_id = next.token_id.clone();
    store
        .insert_refresh_token(token.clone())
        .await
        .expect("insert");
    let take = store
        .rotate_refresh_token("t1", &token.token_id, 1, next)
        .await
        .expect("rotate");
    assert!(matches!(take, RefreshTokenTake::Taken(_)), "{take:?}");
    assert!(
        store
            .peek_refresh_token("t1", &next_id)
            .await
            .expect("peek")
            .is_some()
    );
}

/// Once incarnations are finalized, a create proposes the clock even while
/// a peer has not answered since this member restarted. Proposing 0 then
/// gives the name tombstone + 1, which can repeat an incarnation after a
/// restore. A peer that answers with a lower level still holds level-10
/// commands back, as after a rollback. Goes through the API's create path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finalized_group_proposes_incarnations_past_a_peer_that_never_answered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, version) =
        store_with_member_at(dir.path(), crate::store::raft::command::METADATA_VERSION).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !store.takes_incarnation_proposals().await {
        assert!(
            std::time::Instant::now() < deadline,
            "the member's level was never read"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
    store
        .register_node(crate::store::contract::nodes::with_features(
            crate::store::contract::nodes::node("broker-i", 7301),
            &[feature],
        ))
        .await
        .expect("report the feature");
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");

    let state = crate::test_support::app_state_ready(store.clone());
    let key = crate::model::StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
    };
    let create = || async {
        let body = serde_json::from_value(serde_json::json!({
            "stream": "orders", "kind": "Stream", "shards": 1, "retention": {},
            "consistency": "Leader", "delivery": "AtLeastOnce", "durable": true
        }))
        .expect("request");
        let stream = crate::api::streams::stream_to_create(&state, "t1".into(), "ns".into(), body)
            .await
            .expect("stream");
        store.create_stream(stream).await
    };
    let first = create().await.expect("create");
    assert_ne!(first.incarnation, 0);
    store.delete_stream(&key).await.expect("delete");

    // As after a restart with the peer down: it has never answered.
    version.store(SILENT, std::sync::atomic::Ordering::SeqCst);
    store.handle.forget_reported_versions();
    assert!(!store.takes_incarnation_proposals().await);
    let again = create()
        .await
        .expect("create with a peer that never answered");
    assert!(
        again.incarnation > first.incarnation + 1,
        "proposed nothing: incarnation {} after {}",
        again.incarnation,
        first.incarnation
    );
    store.delete_stream(&key).await.expect("delete again");

    // A peer that says it is at level 0 holds the incarnated create back.
    version.store(0, std::sync::atomic::Ordering::SeqCst);
    store.handle.forget_reported_versions();
    let refused = create().await;
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("metadata version 10")),
        "a peer that reported level 0 was overridden: {refused:?}"
    );
}

/// Before anything level-gated is finalized, a member on an older build may
/// join: that is a rolling upgrade in progress, and the group's level drops
/// to the newcomer's.
#[tokio::test]
async fn an_older_member_joins_a_group_with_nothing_finalized() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    for (id, level) in [(2, 8), (3, 0)] {
        let addr = stub_member(Arc::new(std::sync::atomic::AtomicU16::new(level))).await;
        store
            .handle
            .add_learner_without_waiting(id, addr)
            .await
            .unwrap_or_else(|err| panic!("member at level {level} refused: {err:#}"));
    }
}

/// Once `stream_incarnations` is finalized, a member below level 10 would
/// refuse the incarnated entries the others apply, so it is refused before
/// the membership changes: at a lower level, without a reported level, and
/// without an answer. One at the group's level joins.
#[tokio::test]
async fn a_member_below_a_finalized_level_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
    store
        .register_node(crate::store::contract::nodes::with_features(
            crate::store::contract::nodes::node("broker-i", 7301),
            &[feature],
        ))
        .await
        .expect("report the feature");
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");

    let version = Arc::new(std::sync::atomic::AtomicU16::new(8));
    let addr = stub_member(Arc::clone(&version)).await;
    for (level, expected) in [
        (8, "supports metadata version 8, but the group is at 10"),
        (0, "supports metadata version 0, but the group is at 10"),
        (
            SILENT,
            "did not report its metadata version, and the group is at 10",
        ),
    ] {
        version.store(level, std::sync::atomic::Ordering::SeqCst);
        let before = store.handle.status().last_applied_index;
        let refused = store
            .handle
            .add_learner_without_waiting(2, addr.clone())
            .await;
        assert!(
            matches!(&refused, Err(err) if err.to_string().contains(expected)),
            "member at level {level}: {refused:?}"
        );
        assert_eq!(
            store.handle.status().last_applied_index,
            before,
            "the refused join reached the log"
        );
    }

    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("a member at the group's level joins");
}

/// With nothing finalized, a delete that takes shard state still carries a
/// level-10 field. A member below 10 would refuse it on replay, so once one
/// is in the log an older member is refused too.
#[tokio::test]
async fn an_older_member_is_refused_after_a_flagged_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T1".to_string(),
        })
        .await
        .expect("tenant");
    assert!(store.deletes_take_shards().await);
    store.delete_tenant("t1").await.expect("delete");
    assert!(
        store
            .enabled_fleet_features()
            .await
            .expect("features")
            .is_empty(),
        "nothing is finalized"
    );

    let version = Arc::new(std::sync::atomic::AtomicU16::new(8));
    let addr = stub_member(Arc::clone(&version)).await;
    let refused = store
        .handle
        .add_learner_without_waiting(2, addr.clone())
        .await;
    assert!(
        matches!(&refused, Err(err) if err.to_string().contains("the group is at 10")),
        "{refused:?}"
    );
    version.store(
        crate::store::raft::command::METADATA_VERSION,
        std::sync::atomic::Ordering::SeqCst,
    );
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("a member at the group's level joins");
}

/// A member already in the group that comes back on an older build (a wiped
/// volume, the same id) answers the next append with a conflict below what
/// it acknowledged. The leader asks its level again and sends it nothing
/// while it is below the floor, then feeds it again once it reports the
/// floor.
#[tokio::test]
async fn the_leader_holds_a_member_below_the_floor_until_it_reports_it() {
    use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .create_tenant(crate::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T1".to_string(),
        })
        .await
        .expect("tenant");
    store.delete_tenant("t1").await.expect("delete");

    let version = Arc::new(AtomicU16::new(
        crate::store::raft::command::METADATA_VERSION,
    ));
    let conflict = Arc::new(AtomicBool::new(false));
    let appends = Arc::new(AtomicUsize::new(0));
    let app = {
        let (version, conflict, appends) = (
            Arc::clone(&version),
            Arc::clone(&conflict),
            Arc::clone(&appends),
        );
        axum::Router::new()
            .route(
                "/internal/raft/append-entries",
                axum::routing::post(move || {
                    appends.fetch_add(1, Ordering::SeqCst);
                    let reply = if conflict.load(Ordering::SeqCst) {
                        "Conflict"
                    } else {
                        "Success"
                    };
                    async move { axum::Json(serde_json::json!({ "Ok": reply })) }
                }),
            )
            .route(
                "/internal/raft/standing",
                axum::routing::get(move || {
                    let version = version.load(Ordering::SeqCst);
                    async move {
                        axum::Json(
                            serde_json::json!({ "last_log_index": null, "version": version }),
                        )
                    }
                }),
            )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    store
        .handle
        .add_learner_without_waiting(2, addr)
        .await
        .expect("a member at the floor joins");

    let fed_within = |within: Duration| {
        let appends = Arc::clone(&appends);
        async move {
            let before = appends.load(Ordering::SeqCst);
            tokio::time::sleep(within).await;
            appends.load(Ordering::SeqCst) > before
        }
    };
    assert!(fed_within(Duration::from_secs(2)).await, "never fed");

    version.store(8, Ordering::SeqCst);
    conflict.store(true, Ordering::SeqCst);
    // The conflict lands, the member is asked again and held.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !fed_within(Duration::from_secs(2)).await,
        "the leader kept replicating to a member below the floor"
    );

    conflict.store(false, Ordering::SeqCst);
    version.store(
        crate::store::raft::command::METADATA_VERSION,
        Ordering::SeqCst,
    );
    assert!(
        fed_within(Duration::from_secs(3)).await,
        "the member was not fed again after reporting the floor"
    );
}
