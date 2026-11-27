//! A Raft shard with one replica in a cluster, whose machine crashed under
//! the default `Periodic` fsync: the broker's storage id rotated, so its only
//! copy is a stranger to the group. It re-forms itself through the control
//! plane, with no operator, and serves again with what was synced.
//!
//! Run with `cargo test -p felix-cluster --test failures alone_reform::`
//! against brokers built with the `raft` feature.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const RECORDS: usize = 20;
/// Records acknowledged after the sync, so the crash can take them.
const TAIL: usize = 10;

/// How long the brokers' periodic sync gets to reach the disk before the
/// crash, so the records are synced ones.
const SYNCED_BY: Duration = Duration::from_secs(5);

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// **A one-replica shard recovers from a machine crash with no operator
/// re-form.** The synced records are all there, and the first offset after
/// the crash is above every one acknowledged before it, the unsynced tail's
/// too.
#[tokio::test]
#[serial]
async fn a_crashed_one_replica_shard_re_forms_itself() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: a one-replica Raft shard needs the Raft engine");
        return;
    }
    let mut cluster = Cluster::start(config()).await.expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");
    for i in 0..RECORDS {
        cluster
            .publish_via_at(&owner, STREAM, format!("record-{i}").into_bytes())
            .await
            .expect("publish");
    }
    tokio::time::sleep(SYNCED_BY).await;
    // An acknowledged tail the periodic sync has not reached yet: the crash
    // may take it, but its offsets were given out and must not be again.
    let mut acked = 0;
    for i in 0..TAIL {
        let offset = cluster
            .publish_via_at(&owner, STREAM, format!("tail-{i}").into_bytes())
            .await
            .expect("publish the tail")
            .expect("a durable stream's ack names its offset");
        acked = acked.max(offset);
    }

    cluster.kill_node(&owner).expect("kill the owner");
    cluster
        .reboot_node(&owner)
        .expect("reboot the owner's machine");
    cluster.restart_node(&owner).await.expect("restart");

    // No re-form command is sent: the shard serves again on its own.
    let deadline = Instant::now() + Duration::from_secs(120);
    let after = loop {
        let served = match cluster.serving_leader(STREAM, Duration::from_secs(5)).await {
            Ok(serving) => {
                cluster
                    .publish_via_at(&serving, STREAM, b"after-the-crash".to_vec())
                    .await
            }
            Err(err) => Err(err),
        };
        if let Ok(offset) = served {
            break offset.expect("a durable stream's ack names its offset");
        }
        assert!(
            Instant::now() < deadline,
            "the shard never served again without an operator"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(
        after > acked,
        "offset {after} was given out again: the crashed run acknowledged up to {acked}"
    );
    let kept = replay(&cluster, &owner).await;
    assert_eq!(kept, all_records(), "every synced record is kept");
    cluster.shutdown().await;
}

async fn replay(cluster: &Cluster, node: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut payloads = Vec::new();
        if let Ok((_client, mut subscription)) = cluster.replay_on(node, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                let payload = String::from_utf8_lossy(&event.payload).into_owned();
                if payload.starts_with("record-") {
                    payloads.push(payload);
                }
                if payloads.len() == RECORDS {
                    return payloads;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "{node} never served every synced record: {payloads:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn all_records() -> Vec<String> {
    (0..RECORDS).map(|i| format!("record-{i}")).collect()
}
