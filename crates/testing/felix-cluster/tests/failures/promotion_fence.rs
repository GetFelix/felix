//! A promoted leader fences a majority before it serves, so a deposed leader
//! that still believes it leads finds that majority refusing it.
//!
//! The deposed leader here is the case the lease alone used to cover: cut off
//! from the control plane, or frozen, past its lease, with a clock slowed so
//! far that its own view says the lease is still good. The follower it can
//! still reach has not heard of the promotion from the control plane either.
//! Only the fence tells that follower to refuse it.
//!
//! Run with `cargo test -p felix-cluster --test failures promotion_fence::`.
use std::time::{Duration, Instant};

use felix_cluster::{ClockFault, Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const FENCED: &str = "felix_broker_promotions_opened_total{path=\"fenced\"}";
const ON_LEASE: &str = "felix_broker_promotions_opened_total{path=\"lease\"}";
const REFUSED_AS_FENCED: &str = "felix_broker_replicated_total{outcome=\"fenced\"}";

fn config(node_env: Vec<Vec<(String, String)>>) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        proxy_links: true,
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "1500".to_string(),
        )],
        node_env,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

async fn metric(cluster: &Cluster, node: &str, name: &str) -> f64 {
    cluster
        .metric(node, name)
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0)
}

async fn wait_for_metric(cluster: &Cluster, node: &str, name: &str, at_least: f64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while metric(cluster, node, name).await < at_least {
        assert!(
            Instant::now() < deadline,
            "{what}: {node} never reported {name} >= {at_least}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait for placement to hand the shard to someone other than `gone`.
async fn promoted_away_from(cluster: &Cluster, gone: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(STREAM).await
            && owner != gone
        {
            return owner;
        }
        assert!(Instant::now() < deadline, "the shard never left {gone}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// How the old leader is taken out of the picture before the promotion.
#[derive(Clone, Copy)]
enum Deposed {
    /// Cut off from the control plane and from the promoted follower.
    Partitioned,
    /// The same, and frozen while the promotion happens.
    Frozen,
}

/// The whole scenario, returning how many ships the stale follower refused
/// as fenced once the deposed leader could reach it again.
///
/// 1. Leader `old` writes a record only `promoted` gets (the link to `stale`
///    is cut), so the control plane's report names `promoted` alone.
/// 2. `stale` stops hearing the control plane: it stays live, and its view of
///    the shard stays at the old generation.
/// 3. `old` is cut off from the control plane and from `promoted`, its clock
///    slowed to a hundredth, and (in the frozen case) stopped.
/// 4. The control plane lets `old`'s lease lapse and promotes `promoted`,
///    which opens once it has fenced a majority: itself and `stale`.
/// 5. `old` can reach `stale` again and writes. Its lease still looks good
///    to it; `stale` must refuse what it ships.
async fn deposed_leader_meets_the_fenced_majority(deposed: Deposed) {
    let cluster = Cluster::start(config(Vec::new())).await.expect("start");
    let old = cluster.owner(STREAM).await.expect("owner");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old)
        .collect();
    let (promoted, stale) = (followers[0].clone(), followers[1].clone());

    cluster
        .publish_via(&old, STREAM, b"before-1".to_vec())
        .await
        .expect("publish while whole");
    let old_to_stale = Fault::Drop {
        from: Endpoint::node(&old),
        to: Endpoint::node(&stale),
    };
    cluster.inject(&old_to_stale).await.expect("inject");
    cluster
        .publish_via(&old, STREAM, b"before-2".to_vec())
        .await
        .expect("publish on the leader and one follower");

    cluster
        .inject(&Fault::Drop {
            from: Endpoint::ControlPlane,
            to: Endpoint::node(&stale),
        })
        .await
        .expect("inject");
    cluster
        .inject(&Fault::Clock {
            process: Endpoint::node(&old),
            fault: ClockFault::Rate(0.01),
        })
        .await
        .expect("inject");
    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane)
        .into_iter()
        .chain(Fault::partition(
            Endpoint::node(&old),
            Endpoint::node(&promoted),
        ))
    {
        cluster.inject(&fault).await.expect("inject");
    }
    if matches!(deposed, Deposed::Frozen) {
        cluster.pause_node(&old).expect("freeze the old leader");
    }

    let owner = promoted_away_from(&cluster, &old).await;
    assert_eq!(
        owner, promoted,
        "the report named only {promoted} as holding everything",
    );
    wait_for_metric(
        &cluster,
        &promoted,
        FENCED,
        1.0,
        "the promoted leader opening",
    )
    .await;
    assert_eq!(metric(&cluster, &promoted, ON_LEASE).await, 0.0);

    if matches!(deposed, Deposed::Frozen) {
        cluster.resume_node(&old).expect("resume the old leader");
    }
    assert_eq!(
        cluster
            .metric(&old, "felix_broker_lease_held")
            .await
            .expect("metrics"),
        Some(1.0),
        "the deposed leader should still believe it holds its lease",
    );
    let refused_before = metric(&cluster, &stale, REFUSED_AS_FENCED).await;
    cluster.heal(&old_to_stale).await.expect("heal");
    let acknowledged = cluster
        .publish_via(&old, STREAM, b"after-deposed".to_vec())
        .await
        .is_ok();
    // The refusal is what shows the fence at work: `stale`'s own view of the
    // shard is the old generation, which would have taken the batch.
    wait_for_metric(
        &cluster,
        &stale,
        REFUSED_AS_FENCED,
        refused_before + 1.0,
        "the stale follower refusing the deposed leader",
    )
    .await;

    cluster.heal_all().await.expect("heal");
    if acknowledged {
        let (_client, mut replay) = cluster
            .replay_on(&promoted, STREAM)
            .await
            .expect("replay on the new leader");
        let mut held = Vec::new();
        while let Ok(Ok(Some(event))) =
            tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
        {
            held.push(String::from_utf8_lossy(&event.payload).to_string());
        }
        panic!(
            "the deposed leader acknowledged a write after the new leader fenced; \
             the new leader holds {held:?}"
        );
    }
    cluster.shutdown().await;
}

/// **A deposed leader cut off past its lease, its clock slowed so it still
/// believes the lease, cannot get a write acknowledged once the new leader has
/// fenced, and the follower it reaches refuses what it ships.**
#[serial]
#[tokio::test]
async fn a_partitioned_leader_is_refused_by_the_majority_its_successor_fenced() {
    deposed_leader_meets_the_fenced_majority(Deposed::Partitioned).await;
}

/// The same with the old leader frozen through the promotion and woken after.
#[serial]
#[tokio::test]
async fn a_frozen_leader_is_refused_by_the_majority_its_successor_fenced() {
    deposed_leader_meets_the_fenced_majority(Deposed::Frozen).await;
}

/// **A fleet with one broker that does not offer the fence keeps the lease.**
/// The promoted leader opens without fencing, and the failover still keeps
/// what was acknowledged.
#[serial]
#[tokio::test]
async fn a_mixed_fleet_fails_over_on_the_lease() {
    let without_fence = vec![("FELIX_INTERNAL_FENCE".to_string(), "false".to_string())];
    let cluster = Cluster::start(config(vec![without_fence]))
        .await
        .expect("start");
    let nodes = cluster.node_ids();
    let old = cluster.owner(STREAM).await.expect("owner");
    // The broker without the fence has to outlive the old leader to be in the
    // new replica set; placement picks the same first leader every run.
    assert_ne!(
        old, nodes[0],
        "the broker without the fence leads; give another broker the setting"
    );
    cluster
        .publish_via(&old, STREAM, b"acknowledged".to_vec())
        .await
        .expect("publish");

    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("inject");
    }
    let promoted = promoted_away_from(&cluster, &old).await;
    wait_for_metric(
        &cluster,
        &promoted,
        ON_LEASE,
        1.0,
        "the promoted leader opening",
    )
    .await;
    assert_eq!(
        metric(&cluster, &promoted, FENCED).await,
        0.0,
        "{promoted} fenced a replica set in which {} does not offer it",
        nodes[0]
    );
    for node in &nodes {
        if *node != promoted {
            assert_eq!(
                metric(
                    &cluster,
                    node,
                    "felix_broker_replicated_total{outcome=\"fence_taken\"}"
                )
                .await,
                0.0,
                "{node} was sent a fence in a mixed fleet",
            );
        }
    }

    let (_client, mut replay) = cluster
        .replay_on(&promoted, STREAM)
        .await
        .expect("replay on the new leader");
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
    {
        held.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    assert!(
        held.contains(&"acknowledged".to_string()),
        "the new leader lost an acknowledged record: {held:?}"
    );
    cluster.shutdown().await;
}
