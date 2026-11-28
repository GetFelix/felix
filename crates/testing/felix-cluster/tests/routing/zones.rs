//! Brokers register the zone `FELIX_NODE_ZONE` gives them, and placement
//! spreads each shard's copies across zones. Four brokers, two of them in
//! `a`: every shard has one copy in each of `a`, `b` and `c`, and draining
//! one `a` broker hands its copies to the other rather than doubling up in
//! `b` or `c`.
//!
//! On Raft a lost voter is replaced in its own zone, so the group keeps
//! spanning every zone.
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

/// Two brokers in each of `a`, `b` and `c`, so every zone has a spare.
const PAIRED: [&str; 6] = ["a", "a", "b", "b", "c", "c"];

fn paired() -> ClusterConfig {
    ClusterConfig {
        nodes: PAIRED.len(),
        zones: PAIRED.iter().map(|zone| zone.to_string()).collect(),
        streams: vec![StreamSpec::replicated("orders", 2, 3)],
        ..Default::default()
    }
}

/// The other broker in `node_id`'s zone.
fn partner(node_id: &str) -> String {
    let index: usize = node_id
        .strip_prefix("broker-")
        .and_then(|index| index.parse().ok())
        .unwrap_or_else(|| panic!("unexpected node id {node_id}"));
    format!("broker-{}", index ^ 1)
}

fn paired_zone(node_id: &str) -> &'static str {
    let index: usize = node_id
        .strip_prefix("broker-")
        .and_then(|index| index.parse().ok())
        .unwrap_or_else(|| panic!("unexpected node id {node_id}"));
    PAIRED[index]
}

/// The sets and reported voters of every `orders` shard.
async fn raft_sets(cluster: &Cluster) -> Vec<felix_controlplane_service::model::ShardAssignment> {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .filter(|a| a.key.stream == "orders")
        .collect()
}

fn voters_of(record: &felix_controlplane_service::model::ShardAssignment) -> BTreeSet<String> {
    record
        .raft_voters
        .as_ref()
        .map(|voters| voters.members.keys().cloned().collect())
        .unwrap_or_default()
}

/// **A lost voter is replaced in its own zone, so the group still spans
/// every zone and survives losing any one.** The broker that held shard
/// 0's copy in one zone is killed; each shard it held must take that zone's
/// other broker, never a spare in a zone the set already has. Checked on
/// every pass: a set that doubled up a zone even briefly could lose two
/// voters to one zone outage.
#[serial]
#[tokio::test]
async fn on_raft_a_lost_voter_is_replaced_in_its_own_zone() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: the replacement checked here is a Raft membership change");
        return;
    }
    let mut cluster = Cluster::start(paired()).await.expect("start cluster");
    let before = raft_sets(&cluster).await;
    assert_eq!(before.len(), 2, "{before:?}");
    let lost = before
        .iter()
        .find(|record| record.key.shard == 0)
        .expect("shard 0")
        .nodes()
        .find(|node| paired_zone(node) == "a")
        .expect("shard 0 has a copy in zone a")
        .clone();
    let replacement = partner(&lost);
    cluster.kill_node(&lost).expect("kill");

    let policy = felix_controlplane_service::cluster::placement::MovePolicy {
        restore_after_millis: Some(1_000),
        ..Default::default()
    };
    let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(180));
    loop {
        let outcome = cluster
            .control_plane
            .as_ref()
            .expect("control plane running")
            .place_shards_with(policy.clone())
            .await;
        let now = raft_sets(&cluster).await;
        for record in &now {
            let live: Vec<&String> = record.nodes().filter(|node| **node != lost).collect();
            let zones: BTreeSet<&str> = live.iter().map(|node| paired_zone(node)).collect();
            assert_eq!(
                zones.len(),
                live.len(),
                "shard {} doubles up a zone: {record:?}",
                record.key.shard
            );
        }
        let settled = before.iter().all(|was| {
            let now = now
                .iter()
                .find(|record| record.key == was.key)
                .expect("shard still placed");
            let set: BTreeSet<String> = now.nodes().cloned().collect();
            let want: BTreeSet<String> = was
                .nodes()
                .map(|node| {
                    if *node == lost {
                        replacement.clone()
                    } else {
                        node.clone()
                    }
                })
                .collect();
            set == want && voters_of(now) == want
        });
        if settled {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{lost}'s seats never went to {replacement}: {now:?}; last pass: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cluster.shutdown().await;
}
