//! Disk faults on the write path: a broker whose segment writes fail with
//! `ENOSPC` or `EIO`.
//!
//! A failed write is not a failed fsync. The writer rewinds the segment to
//! its last good byte and hands the offsets back, so nothing of the refused
//! batch stays on disk and the log goes on taking appends once the disk
//! does. A failed fsync stops the log for good (see `fsync.rs`), because the
//! kernel may already have dropped the pages.
//!
//! Run with `cargo test -p felix-cluster --test failures writes::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec, WriteFault, scenarios};
use felix_wire::{ErrorCode, RetryClass};
use serial_test::serial;

const STREAM: &str = "orders";

/// As in `fsync.rs`: acknowledge only after the commit, or a `Leader`
/// publish is acknowledged before its write is even tried.
fn on_commit(streams: Vec<StreamSpec>) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams,
        broker_env: vec![
            (
                "FELIX_DURABLE_FSYNC_MODE".to_string(),
                "on_commit".to_string(),
            ),
            ("FELIX_ACK_ON_COMMIT".to_string(), "true".to_string()),
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "1500".to_string(),
            ),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// Publish through `node` and expect a refusal typed `code` and `retry`.
async fn expect_refused(
    cluster: &Cluster,
    node: &str,
    payload: &str,
    code: ErrorCode,
    retry: RetryClass,
) {
    let err = cluster
        .publish_via(node, STREAM, payload.as_bytes().to_vec())
        .await
        .expect_err("a publish was acknowledged although the write behind it failed");
    // Not formatted: it came over TLS, and the code is what a client acts on.
    let typed = scenarios::broker_error(&err).expect("the refusal had no code");
    assert_eq!(typed.code, code, "refusal of {payload}");
    assert_eq!(typed.retry, retry, "refusal of {payload}");
}

/// Publish through `node`, retrying for a while: a healed follower may need
/// a moment before it counts again.
async fn publish_eventually(cluster: &Cluster, node: &str, payload: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match cluster
            .publish_via(node, STREAM, payload.as_bytes().to_vec())
            .await
        {
            Ok(()) => return,
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("{payload} was never acknowledged after healing: {err:#}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

/// Every payload `node` replays from the start of the stream, less the
/// probes the harness publishes while it waits for the stream to be served.
async fn replay(cluster: &Cluster, node: &str) -> Vec<String> {
    // Retried: a restarted broker serves the shard only once it is placed
    // back on it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (_client, mut replay) = loop {
        match cluster.replay_on(node, STREAM).await {
            Ok(opened) => break opened,
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("replay on {node}: {err:#}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
    {
        if event.payload.as_ref() != b"harness-probe" {
            held.push(String::from_utf8_lossy(&event.payload).to_string());
        }
    }
    held
}

/// **A full disk fails the publish as `overloaded`, and healing it loses
/// nothing and adds nothing.** Half the refused batch reaches the file before `ENOSPC`; the
/// writer cuts it off again, so after healing the log holds exactly what was
/// acknowledged and the next append lands where the refused one would have.
/// The restart makes the replay come from the segment on disk, which
/// recovery would refuse to open with the debris still in it.
#[serial]
#[tokio::test]
async fn a_full_disk_fails_the_publish_and_leaves_no_trace() {
    let mut cluster = Cluster::start(on_commit(vec![StreamSpec::new(STREAM, 1)]))
        .await
        .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&owner, STREAM, b"before".to_vec())
        .await
        .expect("publish with room on the disk");

    let fault = Fault::Write {
        node: owner.clone(),
        fault: WriteFault::NoSpace,
    };
    cluster.inject(&fault).await.expect("inject");
    expect_refused(
        &cluster,
        &owner,
        "disk-full-1",
        ErrorCode::Overloaded,
        RetryClass::RetryAfter,
    )
    .await;
    expect_refused(
        &cluster,
        &owner,
        "disk-full-2",
        ErrorCode::Overloaded,
        RetryClass::RetryAfter,
    )
    .await;

    cluster.heal(&fault).await.expect("heal");
    cluster
        .publish_via(&owner, STREAM, b"after".to_vec())
        .await
        .expect("a failed write does not stop the log, so a healed disk takes appends again");
    assert_eq!(replay(&cluster, &owner).await, ["before", "after"]);

    cluster.stop_node(&owner).await.expect("stop the owner");
    cluster
        .restart_node(&owner)
        .await
        .expect("restart the owner");
    cluster.place_shards().await;
    assert_eq!(replay(&cluster, &owner).await, ["before", "after"]);
    cluster.shutdown().await;
}

/// **One refused write costs one publish.** Unlike a failed fsync, a failed
/// write proves nothing about records already written, so the log carries
/// on as soon as the disk does.
#[serial]
#[tokio::test]
async fn a_single_failed_write_does_not_stop_the_log() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::new(STREAM, 1)]))
        .await
        .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&owner, STREAM, b"before".to_vec())
        .await
        .expect("publish on a healthy disk");

    let fault = Fault::Write {
        node: owner.clone(),
        fault: WriteFault::IoOnce,
    };
    cluster.inject(&fault).await.expect("inject");
    // The only log on `owner` taking writes is this one, so the failure is
    // this publish's.
    expect_refused(
        &cluster,
        &owner,
        "refused",
        ErrorCode::Storage,
        RetryClass::OutcomeUnknown,
    )
    .await;
    cluster
        .publish_via(&owner, STREAM, b"after".to_vec())
        .await
        .expect("the write after a one-off failure is acknowledged");
    assert_eq!(replay(&cluster, &owner).await, ["before", "after"]);
    cluster.shutdown().await;
}

/// **A `Quorum` leader whose own write fails does not acknowledge.** The
/// batch never reached its log, so there was nothing to replicate, and after
/// healing the refused record appears nowhere.
#[serial]
#[tokio::test]
async fn a_quorum_leader_whose_write_fails_does_not_ack() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::quorum(STREAM, 1, 3)]))
        .await
        .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish on healthy disks");

    let fault = Fault::Write {
        node: leader.clone(),
        fault: WriteFault::Io,
    };
    cluster.inject(&fault).await.expect("inject");
    expect_refused(
        &cluster,
        &leader,
        "leader-disk-bad",
        ErrorCode::Storage,
        RetryClass::OutcomeUnknown,
    )
    .await;

    cluster.heal(&fault).await.expect("heal");
    publish_eventually(&cluster, &leader, "after").await;
    assert_eq!(replay(&cluster, &leader).await, ["before", "after"]);
    cluster.shutdown().await;
}

/// **A follower whose write fails is not part of the majority.** With one
/// follower refusing, the leader and the other follower still make two of
/// three. With both refusing, the leader holds the record alone and must not
/// acknowledge it.
#[serial]
#[tokio::test]
async fn a_follower_whose_write_fails_does_not_count_toward_the_majority() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::quorum(STREAM, 1, 3)]))
        .await
        .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish on healthy disks");

    let faults: Vec<Fault> = followers
        .iter()
        .map(|node| Fault::Write {
            node: node.clone(),
            fault: WriteFault::NoSpace,
        })
        .collect();
    cluster.inject(&faults[0]).await.expect("inject");
    cluster
        .publish_via(&leader, STREAM, b"one-follower-full".to_vec())
        .await
        .expect("the leader and the healthy follower are a majority");

    cluster.inject(&faults[1]).await.expect("inject");
    expect_refused(
        &cluster,
        &leader,
        "both-followers-full",
        ErrorCode::QuorumTimeout,
        RetryClass::OutcomeUnknown,
    )
    .await;

    for fault in &faults {
        cluster.heal(fault).await.expect("heal");
    }
    publish_eventually(&cluster, &leader, "after").await;
    cluster.shutdown().await;
}
