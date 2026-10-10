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
                    axum::Json(if version == 0 {
                        serde_json::json!({ "last_log_index": null })
                    } else {
                        serde_json::json!({ "last_log_index": null, "version": version })
                    })
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

/// A member before halted copies would drop them from a report it applies.
/// So while one is in the group the report is recorded without them, and it
/// is not refused: placement needs the rest of it. Once every member has the
/// level, halts are kept.
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
        })
        .await
        .expect("assign");
    let report = ReplicaReport {
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
        if held().await.halted.contains_key("broker-y") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "halts still dropped after every member reported the level"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
