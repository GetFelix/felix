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

async fn single_node_store(dir: &std::path::Path) -> Arc<RaftStore> {
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
    crate::store::contract::nodes::run_node_concurrency_contract(store).await;
}

#[tokio::test]
async fn satisfies_the_shard_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::shards::run_shard_contract(store.clone()).await;
    crate::store::contract::shards::run_shard_concurrency_contract(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn satisfies_the_signing_key_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::signing_keys::run_signing_key_contract(store).await;
}

#[tokio::test]
async fn satisfies_the_placement_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::placement::run_placement_contract(store.clone(), store).await;
}

/// Under Raft the lease is leadership: whoever the gate confirmed takes it at
/// once, without waiting for the last leader's lease to run out, and a step
/// the last leader planned is fenced once it has.
#[tokio::test]
async fn the_confirmed_leader_takes_the_placement_lease_at_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    crate::store::contract::shards::seed(store.as_ref()).await;

    let old = store
        .acquire_placement_lease("old-leader", 60_000)
        .await
        .expect("old")
        .expect("granted");
    assert!(old.taken);
    let renewed = store
        .acquire_placement_lease("old-leader", 60_000)
        .await
        .expect("renew")
        .expect("granted");
    assert_eq!(
        renewed,
        crate::store::PlacementLease {
            token: old.token,
            taken: false
        }
    );

    let new = store
        .acquire_placement_lease("new-leader", 60_000)
        .await
        .expect("new")
        .expect("the leader is never refused");
    assert!(new.taken);
    assert_eq!(new.token, old.token + 1);
    assert_eq!(
        store
            .put_shard_assignment_if(
                crate::store::contract::shards::assignment(0, "broker-x"),
                None,
                old.token,
            )
            .await
            .expect("late write"),
        AssignmentWrite::Fenced { token: new.token },
    );
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
