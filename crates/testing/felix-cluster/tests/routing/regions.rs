//! A stream with a home region keeps its data there. Two brokers in `eu`, one
//! in `us`, and no bridge between the regions: placement puts no copy of an
//! `eu` stream in `us`, the operator API refuses to move one there, and the
//! `us` broker refuses to forward a publish into `eu` instead of serving it.
//!
//! On Raft a lost voter's seat follows the same rule: it goes to another
//! region only across a bridge from the stream's own.
//!
//! Run with `cargo test -p felix-cluster --test routing regions::`.
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use felix_client::BrokerError;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::cluster::placement::MovePolicy;
use felix_controlplane_service::model::ShardAssignment;
use felix_wire::{AckMode, ErrorCode};
use serial_test::serial;

const STREAM: &str = "orders";
const IN_EU: [&str; 2] = ["broker-0", "broker-1"];
const IN_US: &str = "broker-2";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        regions: vec!["eu".to_string(), "eu".to_string(), "us".to_string()],
        streams: vec![StreamSpec::replicated(STREAM, 4, 2).in_region("eu")],
        ..Default::default()
    }
}

#[serial]
#[tokio::test]
async fn a_stream_homed_in_one_region_is_refused_by_another() {
    let cluster = Cluster::start(config()).await.expect("start cluster");

    let assignments = cluster.shard_assignments().await.expect("assignments");
    assert_eq!(assignments.len(), 4, "{assignments:?}");
    for (key, assignment) in &assignments {
        for node in std::iter::once(&assignment.leader).chain(&assignment.replicas) {
            assert!(
                IN_EU.contains(&node.as_str()),
                "{key} has a copy on {node}, outside its region"
            );
        }
    }

    let refused = cluster
        .start_move(STREAM, 0, IN_US)
        .await
        .expect_err("a move out of the region must be refused");
    assert!(
        format!("{refused:#}").contains("region_not_allowed"),
        "{refused:#}"
    );

    // `Cluster::start` proves only shard 0 serves. The unkeyed publish below
    // can land on any shard, and one still opening on its eu owner refuses it.
    // Bounded by the harness's ready timeout; a refusal that never clears
    // fails here instead of being retried past the assertion.
    cluster
        .await_every_shard_serving(STREAM, 4)
        .await
        .expect("every eu shard serves");

    // Inside the region a publish is served, forwarded or not.
    let eu = cluster.node(IN_EU[0]).expect("eu broker");
    let client =
        felix_cluster::client::connect(eu.client_addr, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("connect to eu");
    client
        .publisher()
        .await
        .expect("publisher")
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"in-region".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("an eu broker serves an eu stream");

    let us = cluster.node(IN_US).expect("us broker");
    let client =
        felix_cluster::client::connect(us.client_addr, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("connect to us");
    let err = client
        .publisher()
        .await
        .expect("publisher")
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"cross-region".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("the us broker must not forward into eu");
    let broker = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<BrokerError>())
        .unwrap_or_else(|| panic!("not a typed refusal: {err:#}"));
    // The binary publish ack carries the code and retry class but not the
    // reason; `region_not_routable` itself is checked where dispatch decides
    // it, in the broker's routing tests.
    assert_eq!(broker.code, ErrorCode::ShardUnavailable, "{broker:?}");
}

const BRIDGES: &str = "eu>us,ap>eu";
const IN_AP: &str = "broker-4";

/// Three brokers in `eu`, one in `us` and one in `ap`. `eu>us` lets an
/// `eu` stream into `us`; `ap>eu` does not let it into `ap`.
fn bridged() -> ClusterConfig {
    ClusterConfig {
        nodes: 5,
        regions: ["eu", "eu", "eu", "us", "ap"]
            .iter()
            .map(|region| region.to_string())
            .collect(),
        broker_env: vec![("FELIX_REGION_BRIDGES".to_string(), BRIDGES.to_string())],
        streams: vec![StreamSpec::replicated(STREAM, 2, 3).in_region("eu")],
        ..Default::default()
    }
}

/// Placement as a control plane with `bridges` in `FELIX_REGION_BRIDGES`
/// plans it, a broker down a second lost.
fn refilling(bridges: &str) -> MovePolicy {
    MovePolicy {
        restore_after_millis: Some(1_000),
        regions: std::sync::Arc::new(felix_router::RegionRouter::with_bridges(
            String::new(),
            felix_router::parse_bridges(bridges).expect("bridges"),
        )),
        ..MovePolicy::default()
    }
}

async fn raft_sets(cluster: &Cluster) -> Vec<ShardAssignment> {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .filter(|a| a.key.stream == STREAM)
        .collect()
}

fn voters_of(record: &ShardAssignment) -> BTreeSet<String> {
    record
        .raft_voters
        .as_ref()
        .map(|voters| voters.members.keys().cloned().collect())
        .unwrap_or_default()
}

/// One pass under `policy`, failing if any record seats the `ap` broker.
async fn pass_keeping_out_of_ap(cluster: &Cluster, policy: &MovePolicy) -> Vec<ShardAssignment> {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .place_shards_with(policy.clone())
        .await;
    let now = raft_sets(cluster).await;
    for record in &now {
        let seated = record
            .nodes()
            .chain(voters_of(record).iter())
            .any(|node| node == IN_AP);
        assert!(!seated, "an eu shard seats the ap broker: {record:?}");
    }
    now
}

/// **A lost voter of an `eu` stream is replaced only where the stream may
/// be.** With `eu` short a broker, the seat waits while the control plane
/// has no bridge out of `eu`, goes to `us` once `eu>us` is configured, and
/// never to `ap`: a bridge into `eu` is not one out of it.
#[serial]
#[tokio::test]
async fn on_raft_a_lost_voter_is_replaced_only_across_a_bridge() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: the replacement checked here is a Raft membership change");
        return;
    }
    let mut cluster = Cluster::start(bridged()).await.expect("start cluster");
    let lost = "broker-0";
    let in_us = "broker-3";
    cluster.kill_node(lost).expect("kill");

    let unbridged = refilling("ap>eu");
    let held_until = Instant::now() + Duration::from_secs(10);
    while Instant::now() < held_until {
        for record in pass_keeping_out_of_ap(&cluster, &unbridged).await {
            assert!(
                record.nodes().any(|node| node == lost) && !record.nodes().any(|n| n == in_us),
                "with no bridge out of eu the seat must wait: {record:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let policy = refilling(BRIDGES);
    let want: BTreeSet<String> = ["broker-1", "broker-2", in_us]
        .iter()
        .map(|node| node.to_string())
        .collect();
    let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(180));
    loop {
        let now = pass_keeping_out_of_ap(&cluster, &policy).await;
        let settled = now.iter().all(|record| {
            record.nodes().cloned().collect::<BTreeSet<_>>() == want && voters_of(record) == want
        });
        if settled {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{lost}'s seats never went to {in_us}: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    cluster.shutdown().await;
}
