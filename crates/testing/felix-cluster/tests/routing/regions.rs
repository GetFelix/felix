//! A stream with a home region keeps its data there. Two brokers in `eu`, one
//! in `us`, and no bridge between the regions: placement puts no copy of an
//! `eu` stream in `us`, the operator API refuses to move one there, and the
//! `us` broker refuses to forward a publish into `eu` instead of serving it.
//!
//! Run with `cargo test -p felix-cluster --test routing regions::`.
use felix_client::BrokerError;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
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
