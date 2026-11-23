//! A restarted broker that loses a port to another process starts again
//! instead of failing the test (#2525), while a real failure still fails it.
//!
//! Run with `cargo test -p felix-cluster --test failures restart_ports::`.
use std::net::UdpSocket;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig};
use serial_test::serial;

async fn three_brokers() -> Cluster {
    Cluster::start(ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster")
}

/// Another process holds the internal port when the broker comes back on its
/// old ports, then lets it go.
#[serial]
#[tokio::test]
async fn a_broker_restarted_in_place_waits_out_a_port_held_for_a_moment() {
    let mut cluster = three_brokers().await;
    let node = "broker-1";
    let internal = cluster.node(node).expect("node").internal_addr;
    cluster.kill_node(node).expect("kill");
    let thief = UdpSocket::bind(internal).expect("take the internal port");
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        drop(thief);
    });

    cluster.start_node_unready_in_place(node).expect("start");
    cluster
        .await_node_ready(node)
        .await
        .expect("the broker starts once the port is free");
    release.join().expect("release the port");
    cluster.shutdown().await;
}

/// The same race on fresh ports, over and over: a kill followed at once by a
/// restart never fails on a bind.
#[serial]
#[tokio::test]
async fn fifty_kills_and_restarts_never_fail_on_a_bind() {
    let mut cluster = three_brokers().await;
    for round in 0..50 {
        let node = format!("broker-{}", round % 3);
        cluster.kill_node(&node).expect("kill");
        cluster
            .restart_node(&node)
            .await
            .unwrap_or_else(|e| panic!("restart {node}, round {round}: {e:#}"));
    }
    cluster.shutdown().await;
}
