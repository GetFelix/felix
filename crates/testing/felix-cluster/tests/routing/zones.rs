//! Brokers register the zone `FELIX_NODE_ZONE` gives them, and placement
//! spreads each shard's copies across zones. Four brokers, two of them in
//! `a`: every shard has one copy in each of `a`, `b` and `c`, and draining
//! one `a` broker hands its copies to the other rather than doubling up in
//! `b` or `c`.
//!
//! Run with `cargo test -p felix-cluster --test routing zones::`.
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const ZONES: [&str; 4] = ["a", "a", "b", "c"];

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: ZONES.len(),
        zones: ZONES.iter().map(|zone| zone.to_string()).collect(),
        streams: vec![StreamSpec::replicated("orders", 8, 3)],
        ..Default::default()
    }
}

fn zone_of(node_id: &str) -> &'static str {
    let index: usize = node_id
        .strip_prefix("broker-")
        .and_then(|index| index.parse().ok())
        .unwrap_or_else(|| panic!("unexpected node id {node_id}"));
    ZONES[index]
}

/// Every shard's copies, leader first.
async fn copies(cluster: &Cluster) -> HashMap<String, Vec<String>> {
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .map(|(key, assignment)| {
            let nodes = std::iter::once(assignment.leader)
                .chain(assignment.replicas)
                .collect();
            (key, nodes)
        })
        .collect()
}

fn assert_spread(copies: &HashMap<String, Vec<String>>) {
    assert_eq!(copies.len(), 8, "{copies:?}");
    for (key, nodes) in copies {
        let zones: BTreeSet<&str> = nodes.iter().map(|node| zone_of(node)).collect();
        assert_eq!(nodes.len(), 3, "{key}: {nodes:?}");
        assert_eq!(zones.len(), 3, "{key} has copies sharing a zone: {nodes:?}");
    }
}

#[serial]
#[tokio::test]
async fn a_shards_copies_span_every_zone_and_a_drain_keeps_them_spread() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    assert_spread(&copies(&cluster).await);

    // broker-0 is in `a`; only broker-1 can take its copies without leaving
    // a shard with two in one zone.
    let draining = "broker-0";
    cluster.drain_node(draining).await.expect("drain");
    let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(180));
    loop {
        let outcome = cluster.place_shards_moving(4).await;
        let now = copies(&cluster).await;
        let settled = now
            .values()
            .all(|nodes| nodes.len() == 3 && !nodes.iter().any(|node| node == draining));
        if settled {
            assert_spread(&now);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{draining} still holds copies: {now:?}; last pass: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
