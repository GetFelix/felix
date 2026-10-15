//! Once the fleet finalizes `fenced_caches` (with `majority_ack` and
//! `generation_start`), a `Quorum` cache acknowledges a put or a counter add
//! when a majority of its replicas has answered that it holds it at the
//! leader's generation, and a promoted cache shard fences both its logs on a
//! majority before it serves. The lease is off the write's path, as it is for
//! a `Quorum` stream under `majority_ack` (see `majority_ack.rs`).
//!
//! Run with `cargo test -p felix-cluster --test failures fenced_caches::`.
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

const CACHE: &str = "sessions";
const COUNTER: &str = "hits";
const LEASE_HELD: &str = "felix_broker_lease_held";

/// Three brokers, a `Quorum` cache on all three, and every feature the
/// follower acks on a cache need finalized.
async fn start() -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new("orders", 1)],
        caches: vec![CacheSpec::quorum(CACHE, 1, 3)],
        proxy_links: true,
        broker_env: vec![(
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "2000".to_string(),
        )],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["generation_start", "majority_ack", "fenced_caches"],
            Duration::from_secs(20),
        )
        .await
        .expect("every broker enables the cache follower acks");
    cluster
}

async fn cache_owner(cluster: &Cluster) -> anyhow::Result<String> {
    cluster.shard_owner_of("cache", CACHE, 0).await
}

/// **A cache leader cut off from the control plane keeps writing past its
/// lease, and loses no put or counter add it acknowledged.** The control
/// plane promotes a follower; the new leader fences a majority on the cache
/// log and the counter log, and takes the furthest of each before it serves.
/// From the fence on, the old leader's followers refuse it.
#[serial]
#[tokio::test]
async fn a_cache_leader_cut_off_from_the_control_plane_loses_nothing_it_acknowledged() {
    let cluster = start().await;
    let old = cache_owner(&cluster).await.expect("cache shard owner");
    cluster
        .cache_put_via(&old, CACHE, "before", b"before")
        .await
        .expect("put while whole");
    let mut counted: i64 = cluster
        .counter_add_via(&old, CACHE, COUNTER, 1)
        .await
        .map(|_| 1)
        .expect("counter add while whole");
    for fault in Fault::partition(Endpoint::node(&old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }

    // The harness's control plane places only when asked, so nobody else
    // leads until the loop below: these are acknowledged on the followers
    // with the old leader's own lease lapsed.
    felix_cluster::wait::until(Duration::from_secs(10), "the lease to lapse", || async {
        cluster.metric(&old, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the old leader's lease lapses");
    cluster
        .cache_put_via(&old, CACHE, "past-the-lease", b"past-the-lease")
        .await
        .expect("a majority holds the put, so it is acknowledged without the lease");
    cluster
        .counter_add_via(&old, CACHE, COUNTER, 1)
        .await
        .expect("a majority holds the add, so it is acknowledged without the lease");
    counted += 1;

    // Then writing and promotion at once.
    let promoted = std::sync::atomic::AtomicBool::new(false);
    let writes = async {
        let mut acknowledged = vec!["before".to_string(), "past-the-lease".to_string()];
        let mut adds: i64 = 0;
        let mut attempts: i64 = 0;
        let mut after_promotion = 0;
        for i in 0.. {
            if after_promotion >= 10 {
                break;
            }
            if promoted.load(std::sync::atomic::Ordering::SeqCst) {
                after_promotion += 1;
            }
            let key = format!("cut-off-{i}");
            if cluster
                .cache_put_via(&old, CACHE, &key, key.as_bytes())
                .await
                .is_ok()
            {
                acknowledged.push(key);
            }
            attempts += 1;
            if cluster
                .counter_add_via(&old, CACHE, COUNTER, 1)
                .await
                .is_ok()
            {
                adds += 1;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        (acknowledged, adds, attempts)
    };
    let promotion = async {
        let promoted_in =
            felix_cluster::wait::until(Duration::from_secs(30), "a new cache leader", || async {
                cluster.place_shards().await;
                cache_owner(&cluster).await.is_ok_and(|owner| owner != old)
            })
            .await;
        promoted.store(true, std::sync::atomic::Ordering::SeqCst);
        promoted_in
    };
    let ((acknowledged, adds, attempts), promoted_in) = tokio::join!(writes, promotion);
    promoted_in.expect("the control plane promotes a follower");
    counted += adds;

    cluster.heal_all().await.expect("heal");
    let new = cache_owner(&cluster).await.expect("cache shard owner");
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "the new cache leader to serve",
        || async {
            cluster
                .cache_put_via(&new, CACHE, "after", b"after")
                .await
                .is_ok()
        },
    )
    .await
    .expect("the new cache leader serves");

    for key in &acknowledged {
        let value = cluster
            .cache_get_via(&new, CACHE, key)
            .await
            .unwrap_or_else(|err| panic!("get {key} on {new}: {err:#}"));
        assert_eq!(
            value.as_deref(),
            Some(key.as_bytes()),
            "{new} lost {key}, which {old} acknowledged",
        );
    }
    // An add that timed out may still have landed, so the count is at least
    // what was acknowledged and at most what was sent.
    let total = cluster
        .counter_get_via(&new, CACHE, COUNTER)
        .await
        .expect("counter get on the new leader")
        .unwrap_or(0);
    assert!(
        total >= counted && total <= 2 + attempts,
        "{new} counts {total}, but {old} acknowledged {counted} adds of {} sent",
        2 + attempts,
    );
    cluster.shutdown().await;
}
