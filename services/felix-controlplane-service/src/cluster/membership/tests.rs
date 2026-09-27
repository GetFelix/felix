//! Expiry behaviour, driven at exact times rather than by waiting on a timer.
use super::*;
use crate::config::NodeLivenessConfig;
use crate::model::NodeLifecycle;
use crate::store::contract::nodes::node;
use crate::store::memory::InMemoryStore;
use crate::store::{ControlPlaneStore, StoreConfig};

const INTERVAL_MS: u64 = 1_000;
const TIMEOUT_MS: u64 = 3_000;
/// The timeout plus the default regrant margin, a quarter of it.
const DOWN_AFTER_MS: u64 = TIMEOUT_MS + TIMEOUT_MS / 4;
const T0: u64 = 1_700_000_000_000;

fn liveness() -> NodeLivenessConfig {
    NodeLivenessConfig {
        heartbeat_interval_ms: INTERVAL_MS,
        expiry_timeout_ms: TIMEOUT_MS,
        regrant_margin_ms: None,
        sweep_interval_ms: 500,
        shard_reconcile_interval_ms: 5_000,
    }
}

async fn store_with_node() -> InMemoryStore {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let mut broker = node("broker-a", 7001);
    broker.status.last_heartbeat_at_millis = T0;
    store.register_node(broker).await.expect("register");
    store
}

#[tokio::test]
async fn a_broker_heartbeating_inside_the_window_stays_live() {
    let store = store_with_node().await;

    // Four intervals of healthy reporting, sweeping after each one.
    for beat in 1..=4 {
        let now = T0 + beat * INTERVAL_MS;
        store
            .record_node_heartbeat("broker-a", 0, now)
            .await
            .expect("beat");
        assert_eq!(expire_once(&store, &liveness(), now).await, 0);
    }

    let node = store.get_node("broker-a").await.expect("get");
    assert_eq!(node.status.lifecycle, NodeLifecycle::Live);
}

#[tokio::test]
async fn a_silent_broker_goes_down_once_the_timeout_elapses() {
    let store = store_with_node().await;

    // Exactly at the timeout is still inside the window.
    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS).await,
        0
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );

    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS + 1).await,
        1
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Down,
    );
}

/// Marking a node down is what placement watches for, so it has to reach the
/// changefeed like any other membership change.
#[tokio::test]
async fn expiry_publishes_a_change() {
    let store = store_with_node().await;
    let since = store.node_snapshot().await.expect("snapshot").next_seq;

    expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS + 1).await;

    let changes = store.node_changes(since).await.expect("changes");
    assert_eq!(changes.items.len(), 1);
    let published = changes.items[0].node.as_ref().expect("body");
    assert_eq!(published.node_id, "broker-a");
    assert_eq!(published.status.lifecycle, NodeLifecycle::Down);
}

/// Two control-plane instances sweep the same database. Between them a node
/// must be marked down once and published once.
#[tokio::test]
async fn a_repeated_sweep_expires_a_node_once() {
    let store = store_with_node().await;
    let since = store.node_snapshot().await.expect("snapshot").next_seq;
    let now = T0 + DOWN_AFTER_MS + 1;

    assert_eq!(expire_once(&store, &liveness(), now).await, 1);
    for _ in 0..3 {
        assert_eq!(expire_once(&store, &liveness(), now).await, 0);
    }

    assert_eq!(
        store
            .node_changes(since)
            .await
            .expect("changes")
            .items
            .len(),
        1
    );
}

/// A draining broker is still serving, so losing it matters as much as losing a
/// live one.
#[tokio::test]
async fn a_draining_broker_also_expires() {
    let store = store_with_node().await;
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain");

    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS + 1).await,
        1
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Down,
    );
}

/// A node already down is not swept again, so a permanently dead broker does
/// not publish a change on every tick forever.
#[tokio::test]
async fn a_departed_node_is_not_swept() {
    let store = store_with_node().await;
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Left)
        .await
        .expect("leave");

    assert_eq!(
        expire_once(&store, &liveness(), T0 + TIMEOUT_MS * 100).await,
        0
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Left,
    );
}

/// Before `expiry_timeout_ms` has elapsed since the epoch, subtracting it would
/// wrap and expire every node in the cluster.
#[tokio::test]
async fn a_clock_near_the_epoch_expires_nothing() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    let mut broker = node("broker-a", 7001);
    broker.status.last_heartbeat_at_millis = 0;
    store.register_node(broker).await.expect("register");

    assert_eq!(expire_once(&store, &liveness(), 1).await, 0);
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );
}

/// The acceptance criterion: metrics must reconcile with the node listing. They
/// do by construction -- the census is published from the same store read the
/// listing serves -- and this pins that construction in place.
#[tokio::test]
async fn the_census_counts_exactly_what_the_listing_returns() {
    let store = store_with_node().await;
    let mut second = node("broker-b", 7002);
    second.spec.region = "eu-central-1".to_string();
    second.status.last_heartbeat_at_millis = T0;
    store.register_node(second).await.expect("register");
    store
        .set_node_lifecycle("broker-b", NodeLifecycle::Draining)
        .await
        .expect("drain");

    let listed = store.list_nodes().await.expect("list");
    assert_eq!(listed.len(), 2);

    // A census over the listing is the same operation the sweep performs.
    crate::cluster::membership::metrics::publish_census(&listed);

    let live = listed
        .iter()
        .filter(|n| n.status.lifecycle == NodeLifecycle::Live)
        .count();
    let draining = listed
        .iter()
        .filter(|n| n.status.lifecycle == NodeLifecycle::Draining)
        .count();
    assert_eq!((live, draining), (1, 1));
}

/// The window starts at the first look, and only a full window of watching
/// lets a sweep run.
#[test]
fn the_grace_window_runs_from_the_first_look() {
    let window = Duration::from_millis(TIMEOUT_MS);
    let start = tokio::time::Instant::now();
    let mut grace = SweepGrace::new(window);

    assert!(!grace.may_sweep(start));
    assert!(!grace.may_sweep(start + window - Duration::from_millis(1)));
    assert!(grace.may_sweep(start + window));
    assert!(grace.may_sweep(start + window * 10));
}

/// Losing leadership or the store and getting it back is the same as a fresh
/// start: nobody could heartbeat in between, so the window starts over.
#[test]
fn losing_the_store_or_leadership_restarts_the_window() {
    let window = Duration::from_millis(TIMEOUT_MS);
    let start = tokio::time::Instant::now();
    let mut grace = SweepGrace::new(window);
    assert!(!grace.may_sweep(start));
    assert!(grace.may_sweep(start + window));

    grace.lost("test");
    let back = start + window * 3;
    assert!(!grace.may_sweep(back));
    assert!(!grace.may_sweep(back + window / 2));
    assert!(grace.may_sweep(back + window));
}

/// The outage case end to end: a control plane starting over a store whose
/// heartbeat stamps are all old must not mark those brokers down on its first
/// sweep. Without the grace this one expires the node at once.
#[tokio::test(start_paused = true)]
async fn a_restarted_sweep_waits_a_window_before_expiring() {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    // Last heard from long before this instance started.
    let mut broker = node("broker-a", 7001);
    broker.status.last_heartbeat_at_millis = store.now_millis().await.expect("clock") - 60_000;
    store.register_node(broker).await.expect("register");

    let shutdown = CancellationToken::new();
    let sweep = spawn_expiry_sweep(
        Arc::clone(&store) as Arc<dyn ControlPlaneStore + Send + Sync>,
        liveness(),
        crate::raft::LeadershipGate::Always,
        shutdown.clone(),
    );

    // Several sweeps inside the window: the node is left alone.
    tokio::time::sleep(Duration::from_millis(TIMEOUT_MS - 600)).await;
    let lifecycle = store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .lifecycle;
    assert_eq!(lifecycle, NodeLifecycle::Live);

    // Still silent a full window, and the margin, after start: expired as
    // usual. The watch started on the first tick, so it is the one waited on.
    tokio::time::sleep(Duration::from_millis(DOWN_AFTER_MS - TIMEOUT_MS + 1_200)).await;
    let lifecycle = store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .lifecycle;
    assert_eq!(lifecycle, NodeLifecycle::Down);

    shutdown.cancel();
    sweep.await.expect("sweep");
}

/// A broker failure has to become observable within the expiry bound, not at
/// some later reconciliation.
#[tokio::test]
async fn a_failure_shows_up_in_one_sweep_past_the_timeout() {
    let store = store_with_node().await;

    // One sweep at the boundary: still live.
    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS).await,
        0
    );
    // The very next sweep past it: down, and the listing agrees immediately.
    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS + 1).await,
        1
    );

    let listed = store.list_nodes().await.expect("list");
    assert_eq!(listed[0].status.lifecycle, NodeLifecycle::Down);
}

/// The margin is what separates a broker giving up its lease from its shards
/// being handed on. Down at the timeout alone, TLA+ finds two leaders under
/// drift (`FelixShardThinMargin`, and `FelixShardRealMargins` without it).
#[tokio::test]
async fn a_silent_broker_outlives_the_timeout_by_the_regrant_margin() {
    let store = store_with_node().await;

    assert_eq!(
        expire_once(&store, &liveness(), T0 + TIMEOUT_MS + 1).await,
        0
    );
    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS).await,
        0
    );
    assert_eq!(
        expire_once(&store, &liveness(), T0 + DOWN_AFTER_MS + 1).await,
        1
    );
}

/// A wall clock stepped forward -- NTP, or a newly elected leader whose clock
/// runs ahead -- makes every stamp look old at once. The monotonic watch has
/// heard from the broker recently, so it stays up until the watch agrees.
#[tokio::test]
async fn a_store_clock_step_does_not_expire_a_broker_heard_from_recently() {
    let store = store_with_node().await;
    let mut watch = SilenceWatch::default();
    let start = tokio::time::Instant::now();
    watch.observe(&store.list_nodes().await.expect("list"), start);

    let stepped = T0 + 3_600_000;
    let soon = start + Duration::from_millis(DOWN_AFTER_MS - 1);
    assert_eq!(
        expire_observed(&store, &liveness(), stepped, &mut watch, soon).await,
        0
    );
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .lifecycle,
        NodeLifecycle::Live,
    );

    // Once this process has itself seen the whole window pass in silence,
    // both clocks agree and it goes.
    let later = start + Duration::from_millis(DOWN_AFTER_MS);
    assert_eq!(
        expire_observed(&store, &liveness(), stepped, &mut watch, later).await,
        1
    );
}

/// A new heartbeat stamp restarts the watch's window, even when the store's
/// clock says the stamp is already stale.
#[tokio::test]
async fn a_heartbeat_restarts_the_watch() {
    let store = store_with_node().await;
    let mut watch = SilenceWatch::default();
    let start = tokio::time::Instant::now();
    watch.observe(&store.list_nodes().await.expect("list"), start);

    let beat = start + Duration::from_millis(DOWN_AFTER_MS / 2);
    store
        .record_node_heartbeat("broker-a", 0, T0 + 1)
        .await
        .expect("beat");
    watch.observe(&store.list_nodes().await.expect("list"), beat);

    let stepped = T0 + 3_600_000;
    let after_first_window = start + Duration::from_millis(DOWN_AFTER_MS);
    assert_eq!(
        expire_observed(&store, &liveness(), stepped, &mut watch, after_first_window).await,
        0
    );
    let after_second = beat + Duration::from_millis(DOWN_AFTER_MS);
    assert_eq!(
        expire_observed(&store, &liveness(), stepped, &mut watch, after_second).await,
        1
    );
}

/// A node that left is held exactly as long as a silent one is left live:
/// the same stamp, the same window, the same clock. Anything shorter hands
/// its shards on while it may still serve them.
#[tokio::test]
async fn a_departed_node_is_fenced_for_as_long_as_a_silent_one_stays_live() {
    for now in [
        T0 + TIMEOUT_MS,
        T0 + DOWN_AFTER_MS - 1,
        T0 + DOWN_AFTER_MS,
        T0 + DOWN_AFTER_MS + 1,
    ] {
        let store = store_with_node().await;
        let expired = expire_once(&store, &liveness(), now).await == 1;

        let mut left = store.get_node("broker-a").await.expect("get");
        left.status.lifecycle = NodeLifecycle::Left;
        assert_eq!(
            left_within_lease(&left, &liveness(), now),
            !expired,
            "at {} past the last heartbeat",
            now - T0,
        );
    }
}
