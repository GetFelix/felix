//! The leader's soft state on a single-member group, which leads at once.
use std::time::Duration;

use crate::model::{Node, NodeLifecycle};
use crate::store::ControlPlaneStore;
use crate::store::contract::nodes::node;
use crate::store::raft::tests::single_node_store;

fn applied(store: &crate::store::raft::RaftStore) -> Option<u64> {
    store.handle.status().last_applied_index
}

fn registered(node_id: &str, port: u16) -> Node {
    let mut node = node(node_id, port);
    // Long before this leader began: a replicated time that alone would
    // make the node stale by any cutoff below.
    node.status.last_heartbeat_at_millis = 1;
    node
}

/// A heartbeat is the leader's to remember, not the log's: no entry per
/// heartbeat, and the leader still reports the latest one.
#[tokio::test]
async fn heartbeats_write_nothing_to_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    let before = applied(&store);

    let mut last = 0;
    for _ in 0..20 {
        last = store
            .record_node_heartbeat("broker-a", 0, 0)
            .await
            .expect("heartbeat")
            .status
            .last_heartbeat_at_millis;
    }

    assert_eq!(applied(&store), before, "a heartbeat became a log entry");
    assert!(last > 1, "the leader's clock stamps the heartbeat");
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .last_heartbeat_at_millis,
        last,
        "the leader serves the heartbeat it holds"
    );
}

/// A leader that has just started judging has heard from nobody, and must
/// not read that as everybody having gone quiet: the full window runs from
/// when it began, whatever the log says about older heartbeats.
#[tokio::test]
async fn a_new_leader_expires_nobody_until_a_full_window_has_passed() {
    const WINDOW: Duration = Duration::from_millis(600);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");

    let cutoff = |store_now: u64| store_now.saturating_sub(WINDOW.as_millis() as u64);
    let now = store.now_millis().await.expect("now");
    let early = store.expire_stale_nodes(cutoff(now)).await.expect("sweep");
    assert!(
        early.is_empty(),
        "expired before the new leader had waited a window: {early:?}"
    );

    tokio::time::sleep(WINDOW + Duration::from_millis(200)).await;
    let now = store.now_millis().await.expect("now");
    let late = store.expire_stale_nodes(cutoff(now)).await.expect("sweep");
    assert_eq!(
        late.iter().map(|n| n.node_id.as_str()).collect::<Vec<_>>(),
        vec!["broker-a"],
        "a node silent for a whole window under this leader is expired"
    );
    assert_eq!(late[0].status.lifecycle, NodeLifecycle::Down);
}

/// A heartbeat within the window keeps the node, even though the log never
/// saw it.
#[tokio::test]
async fn a_soft_heartbeat_keeps_a_node_alive() {
    const WINDOW: Duration = Duration::from_millis(400);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .register_node(registered("broker-b", 7002))
        .await
        .expect("register");

    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        store
            .record_node_heartbeat("broker-a", 0, 0)
            .await
            .expect("heartbeat");
    }
    let now = store.now_millis().await.expect("now");
    let expired = store
        .expire_stale_nodes(now - WINDOW.as_millis() as u64)
        .await
        .expect("sweep");
    assert_eq!(
        expired
            .iter()
            .map(|n| n.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["broker-b"]
    );
}

/// Renewing the placement lease is soft state too; only a change of holder
/// is written.
#[tokio::test]
async fn renewing_the_placement_lease_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let first = store
        .acquire_placement_lease("instance-a", 60_000)
        .await
        .expect("acquire")
        .expect("granted");
    assert!(first.taken);
    let before = applied(&store);
    for _ in 0..10 {
        let renewed = store
            .acquire_placement_lease("instance-a", 60_000)
            .await
            .expect("renew")
            .expect("granted");
        assert!(!renewed.taken);
        assert_eq!(renewed.token, first.token);
    }
    assert_eq!(applied(&store), before, "a renewal became a log entry");
}

/// A node deleted and registered again under the same id starts over: what
/// the leader heard from the old record does not keep the new one alive.
#[tokio::test]
async fn a_recreated_node_does_not_inherit_old_heartbeats() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .record_node_heartbeat("broker-a", 0, 0)
        .await
        .expect("heartbeat");
    store.delete_node("broker-a").await.expect("delete");
    let again = store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register again");

    assert_eq!(
        store.get_node("broker-a").await.expect("get"),
        again,
        "the old record's heartbeat leaked into the new one"
    );
}

/// The log's heartbeat for a node that left can trail a beat the previous
/// leader granted, and placement waits out the lease from it. So a new
/// leader reads such a node as heard from when it began judging, as expiry
/// does, and a heartbeat after leaving moves nothing.
#[tokio::test]
async fn a_departed_node_is_heard_from_no_earlier_than_this_leader_began() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Left)
        .await
        .expect("leave");
    // Starts this leader's judging, so the reading below holds still.
    store.expire_stale_nodes(0).await.expect("sweep");

    let left = store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .last_heartbeat_at_millis;
    assert!(left > 1, "read as last heard from before this leader began");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let answer = store
        .record_node_heartbeat("broker-a", 0, 0)
        .await
        .expect("heartbeat");
    assert_eq!(answer.status.lifecycle, NodeLifecycle::Left);
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .last_heartbeat_at_millis,
        left,
        "a heartbeat after leaving moved the stamp",
    );
}
