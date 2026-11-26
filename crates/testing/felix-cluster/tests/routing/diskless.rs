//! A broker with a node id and no durable storage in a Raft cluster.
//!
//! It runs no Raft group. The control plane gives it no durable stream's or
//! cache's shard, and may give it an in-memory stream's, which it serves
//! (#3067, `docs/hybrid-retirement.md`, "In-memory shards").
//!
//! Run with `cargo test -p felix-cluster --test routing diskless::`.
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const PLACED_WITHIN: Duration = Duration::from_secs(60);

/// The fourth broker starts without `FELIX_DURABLE_STORAGE_DIR`.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        streams: vec![
            StreamSpec::replicated("orders", 4, 3),
            StreamSpec::new("ticks", 1).in_memory(),
        ],
        caches: vec![CacheSpec::new("sessions", 4)],
        node_env: vec![
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![("FELIX_DURABLE_STORAGE_DIR".to_string(), String::new())],
        ],
        ..Default::default()
    }
}

/// **A broker without a disk holds no durable or cache shard, serves an
/// in-memory stream it owns, and reaches a cache through its owner.**
#[serial]
#[tokio::test]
async fn a_diskless_broker_serves_in_memory_shards_only() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: a broker without durable storage joins only a Raft cluster");
        return;
    }
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let (diskless, disked) = (nodes[3].clone(), nodes[0].clone());
    let durable_shards = |key: &str| {
        ["orders", "sessions"]
            .iter()
            .any(|name| key.contains(&format!("/{name}/")))
    };
    felix_cluster::wait::until(PLACED_WITHIN, "every shard placed", || async {
        cluster
            .shard_assignments()
            .await
            .is_ok_and(|all| all.keys().filter(|key| durable_shards(key)).count() == 8)
    })
    .await
    .expect("placed");

    let assignments = cluster.shard_assignments().await.expect("assignments");
    for (key, assignment) in assignments.iter().filter(|(key, _)| durable_shards(key)) {
        assert_ne!(
            assignment.leader, diskless,
            "{key} led by the diskless broker"
        );
        assert!(
            !assignment.replicas.contains(&diskless),
            "{key} has a copy on the diskless broker: {:?}",
            assignment.replicas
        );
    }
    assert!(
        cluster.start_move("orders", 0, &diskless).await.is_err(),
        "a durable shard was moved onto the diskless broker"
    );

    // Put the in-memory shard on the diskless broker.
    let owner = cluster
        .shard_owner_of("stream", "ticks", 0)
        .await
        .expect("ticks owner");
    if owner != diskless {
        cluster
            .start_move("ticks", 0, &diskless)
            .await
            .expect("move ticks onto the diskless broker");
    }
    felix_cluster::wait::until(
        PLACED_WITHIN,
        "ticks owned by the diskless broker",
        || async {
            cluster
                .shard_owner_of("stream", "ticks", 0)
                .await
                .is_ok_and(|owner| owner == diskless)
        },
    )
    .await
    .expect("moved");
    // Until the new owner opens the shard a publish answers "not ready".
    felix_cluster::wait::until(PLACED_WITHIN, "the diskless owner to serve", || async {
        cluster
            .publish_via(&diskless, "ticks", b"warm-up".to_vec())
            .await
            .is_ok()
    })
    .await
    .expect("served");

    let (_client, mut subscription) = cluster
        .subscribe_on(&diskless, "ticks")
        .await
        .expect("subscribe on the diskless broker");
    for node in [&diskless, &disked] {
        let payload = format!("through-{node}").into_bytes();
        felix_cluster::wait::until(PLACED_WITHIN, "a publish to be acknowledged", || async {
            cluster
                .publish_via(node, "ticks", payload.clone())
                .await
                .is_ok()
        })
        .await
        .unwrap_or_else(|err| panic!("publish through {node}: {err:#}"));
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("an event within 10 s")
            .expect("subscription")
            .expect("not ended");
        assert_eq!(event.payload.to_vec(), payload);
        assert_eq!(event.offset, None, "an in-memory stream has no offsets");
    }

    cluster
        .cache_put_via(&diskless, "sessions", "user-1", b"v1")
        .await
        .expect("cache put through the diskless broker");
    for node in [&diskless, &disked] {
        let value = cluster
            .cache_get_via(node, "sessions", "user-1")
            .await
            .unwrap_or_else(|err| panic!("cache get via {node}: {err:#}"));
        assert_eq!(value.as_deref(), Some(&b"v1"[..]), "read through {node}");
    }
}
