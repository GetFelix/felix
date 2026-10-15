//! Once the fleet reads by round, subscriptions, cache watches and consumer
//! groups on a replicated `Quorum` shard do not depend on the lease. Readers
//! see only the committed mark, which a deposed leader cannot overstate, and
//! a group write is confirmed by a round of fences before it is acknowledged.
//! A leader cut off from the control plane but not from its replicas keeps
//! them all; one that has been replaced ends its readers and refuses group
//! writes.
//!
//! Run with `cargo test -p felix-cluster --test failures lease_free_sessions::`.
use std::sync::Arc;
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_controlplane_service::store::ControlPlaneStore;
use serial_test::serial;

const STREAM: &str = "orders";
const CACHE: &str = "profiles";
const GROUP: &str = "billing";
const LEASE_HELD: &str = "felix_broker_lease_held";
const ALL: &[&str] = &["generation_start", "majority_ack", "lease_free_reads"];

/// Three brokers, a `Quorum` stream and a `Quorum` cache on all three, and
/// every feature lease-free sessions need finalized.
async fn start() -> Cluster {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
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
    let store = &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store;
    for feature in ALL {
        store
            .finalize_fleet_feature(feature)
            .await
            .unwrap_or_else(|err| panic!("finalize {feature}: {err}"));
    }
    for id in cluster.node_ids() {
        felix_cluster::wait::until(Duration::from_secs(20), "the features to turn on", || {
            let id = id.clone();
            let cluster = &cluster;
            async move {
                cluster
                    .metric(&id, "felix_broker_fleet_feature_enabled")
                    .await
                    .ok()
                    .flatten()
                    == Some(ALL.len() as f64)
            }
        })
        .await
        .expect("every broker enables the finalized features");
    }
    cluster
}

async fn stream_owner(cluster: &Cluster) -> String {
    cluster
        .shard_owner_of("stream", STREAM, 0)
        .await
        .expect("stream shard owner")
}

async fn wait_for_lapse(cluster: &Cluster, node: &str) {
    felix_cluster::wait::until(Duration::from_secs(20), "the lease to lapse", || async {
        cluster.metric(node, LEASE_HELD).await.ok().flatten() == Some(0.0)
    })
    .await
    .expect("the lease lapses with the control plane gone");
}

/// Publish through `node` until one is acknowledged: the shard's first
/// publishes can wait on the mark while the replicas settle.
async fn publish_settled(cluster: &Cluster, node: &str, payload: &[u8]) {
    felix_cluster::wait::until(Duration::from_secs(20), "a publish to land", || async {
        cluster
            .publish_via(node, STREAM, payload.to_vec())
            .await
            .is_ok()
    })
    .await
    .unwrap_or_else(|err| panic!("{node} takes a publish: {err:#}"));
}

/// Cut `old` off from the control plane alone, and wait until the control
/// plane has promoted another broker and that one has taken a publish.
async fn depose(cluster: &Cluster, old: &str) -> String {
    for fault in Fault::partition(Endpoint::node(old), Endpoint::ControlPlane) {
        cluster.inject(&fault).await.expect("partition");
    }
    wait_for_lapse(cluster, old).await;
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        cluster
            .shard_owner_of("stream", STREAM, 0)
            .await
            .is_ok_and(|owner| owner != old)
    })
    .await
    .expect("the control plane promotes a replica");
    let new = stream_owner(cluster).await;
    publish_settled(cluster, &new, b"after").await;
    new
}

/// **A control-plane partition that lapses the lease ends no reader and no
/// group session.** The replicas still answer the leader, so its mark keeps
/// moving, its subscribers keep receiving, its cache watches stay open, new
/// readers are taken, and group acks are confirmed by a round.
#[serial]
#[tokio::test]
async fn readers_and_groups_survive_a_control_plane_partition() {
    let cluster = start().await;
    let leader = stream_owner(&cluster).await;
    publish_settled(&cluster, &leader, b"before").await;
    let claimed = cluster
        .group_poll_records_via(&leader, STREAM, 0, GROUP, 10)
        .await
        .expect("poll while whole");
    let claimed = claimed.first().expect("a record to claim").offset;
    let (_subscriber, mut subscription) = cluster
        .subscribe_on(&leader, STREAM)
        .await
        .expect("subscribe while whole");

    let cache_owner = cluster
        .shard_owner_of("cache", CACHE, 0)
        .await
        .expect("cache shard owner");
    cluster
        .cache_put_via(&cache_owner, CACHE, "k", b"v")
        .await
        .expect("put while whole");
    let (_watcher, mut watch) = cluster
        .cache_watch_retained_via(&cache_owner, CACHE, "k")
        .await
        .expect("watch while whole");
    tokio::time::timeout(Duration::from_secs(10), watch.recv())
        .await
        .expect("the retained value")
        .expect("the watch is open");

    for id in cluster.node_ids() {
        for fault in Fault::partition(Endpoint::node(&id), Endpoint::ControlPlane) {
            cluster.inject(&fault).await.expect("partition");
        }
    }
    wait_for_lapse(&cluster, &leader).await;
    wait_for_lapse(&cluster, &cache_owner).await;
    // Several lease durations, so a reader ended on a timer would be gone.
    tokio::time::sleep(Duration::from_secs(4)).await;

    publish_settled(&cluster, &leader, b"during").await;
    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .expect("the subscriber hears of the publish")
        .expect("the subscription is healthy")
        .expect("the subscription was ended at the lapse");
    assert_eq!(event.payload.as_ref(), b"during");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), watch.recv())
            .await
            .is_err(),
        "the cache watch was ended at the lapse"
    );
    cluster
        .subscribe_on(&leader, STREAM)
        .await
        .expect("a new subscriber is taken without the lease");
    cluster
        .cache_watch_retained_via(&cache_owner, CACHE, "k")
        .await
        .expect("a new watch is taken without the lease");
    cluster
        .group_ack_via(&leader, STREAM, 0, GROUP, claimed)
        .await
        .expect("a group ack its replicas confirm, lease or not");
    cluster.shutdown().await;
}

/// **A deposed coordinator does not acknowledge a group write.** It still
/// believes it leads and still holds the claim; what stops it is that the
/// replicas that took the new leader's fence refuse its round.
#[serial]
#[tokio::test]
async fn a_deposed_coordinator_refuses_a_group_ack() {
    let cluster = start().await;
    let old = stream_owner(&cluster).await;
    publish_settled(&cluster, &old, b"before").await;
    let claimed = cluster
        .group_poll_records_via(&old, STREAM, 0, GROUP, 10)
        .await
        .expect("poll while whole");
    let claimed = claimed.first().expect("a record to claim").offset;

    depose(&cluster, &old).await;

    let refused = cluster
        .group_ack_via(&old, STREAM, 0, GROUP, claimed)
        .await
        .expect_err("the deposed coordinator acknowledged a group ack");
    let why = format!("{refused:#}");
    assert!(
        !why.contains("lease"),
        "the ack was refused for the lease, which proves nothing here: {why}"
    );
    cluster.shutdown().await;
}

/// **A deposed leader ends its subscriptions, and they follow the shard to
/// the new leader.** The lease plays no part: the old leader's replicas
/// refused it, and its readers were told to find the shard again.
#[serial]
#[tokio::test]
async fn a_deposed_leader_ends_its_subscriptions_and_readers_follow() {
    let cluster = start().await;
    let old = stream_owner(&cluster).await;
    publish_settled(&cluster, &old, b"before").await;
    // Entering through another broker, which still hears from the control
    // plane, so the client can learn where the shard went.
    let entry: Vec<_> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old)
        .map(|id| cluster.node(&id).expect("node").client_addr)
        .collect();
    let reader = Arc::new(
        felix_cluster::client::connect_cluster(&entry, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe");
    read_until(&mut subscription, b"before", Duration::from_secs(10)).await;

    let new = depose(&cluster, &old).await;
    assert_ne!(new, old);
    read_until(&mut subscription, b"after", Duration::from_secs(30)).await;
    cluster.shutdown().await;
}

/// Read until `payload` arrives, past the harness's own probes.
async fn read_until(
    subscription: &mut felix_client::ClusterSubscription,
    payload: &[u8],
    within: Duration,
) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let event = tokio::time::timeout_at(deadline, subscription.next_event())
            .await
            .unwrap_or_else(|_| panic!("no {:?} in time", String::from_utf8_lossy(payload)))
            .expect("a healthy subscription")
            .expect("an open subscription");
        if event.payload.as_ref() == payload {
            return;
        }
    }
}
