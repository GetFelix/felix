//! What the tests of Kafka and MQTT on a Raft shard share.
//!
//! Every broker runs with `FELIX_SHARD_ENGINE=raft`, which needs a broker
//! built with the `raft` feature, so these tests are opt-in as in
//! `tests/raft_shards.rs`.
use std::time::Duration;

use felix_cluster::Cluster;

const OPT_IN: &str = "FELIX_TEST_RAFT_SHARDS";

/// Whether this run asked for the Raft tests. Says so when it did not.
pub(crate) fn opted_in() -> bool {
    let asked = std::env::var_os(OPT_IN).is_some();
    if !asked {
        eprintln!("skipped: set {OPT_IN}=1 and build felix-broker with --features raft");
    }
    asked
}

pub(crate) fn engine() -> (String, String) {
    ("FELIX_SHARD_ENGINE".to_string(), "raft".to_string())
}

/// The broker among `among` that leads the shard's Raft group, once
/// exactly one says so.
pub(crate) async fn leader_among(cluster: &Cluster, among: &[String]) -> String {
    felix_cluster::wait::until_some(Duration::from_secs(60), "a Raft leader", || async {
        let mut leading = Vec::new();
        for node in among {
            let led = cluster
                .metric(node, "felix_broker_raft_shards_led")
                .await
                .ok()
                .flatten();
            if led.is_some_and(|led| led >= 1.0) {
                leading.push(node.clone());
            }
        }
        (leading.len() == 1).then(|| leading.remove(0))
    })
    .await
    .expect("a leader")
}
