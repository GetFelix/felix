//! Disk faults: a broker whose fsyncs are slow or fail.
//!
//! The brokers here flush on every commit, so an acknowledgement is a claim
//! that the record reached the disk. A failed flush must therefore never be
//! acknowledged, and neither may a later flush that "succeeds" after one
//! failed: Linux reports a writeback error once, marks the pages clean, and
//! lets the retry through with the data gone ("fsyncgate").
//!
//! Run with `cargo test -p felix-cluster --test failures fsync::`.
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, Fault, FsyncFault, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn on_commit(streams: Vec<StreamSpec>) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams,
        // Flush on every commit, and acknowledge only after it. By default a
        // `Leader` publish is acknowledged once it is queued, which says
        // nothing about the disk and so could not fail on one.
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

/// The fastest of three acknowledged publishes through `node`, on one
/// connection so the time is the publish and not the handshake.
async fn publish_latency(cluster: &Cluster, node: &str) -> Duration {
    let client = cluster.client_on(node).await.expect("connect");
    let publisher = client.publisher().await.expect("publisher");
    let mut fastest = Duration::MAX;
    for _ in 0..3 {
        let started = Instant::now();
        publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                b"timed".to_vec(),
                felix_wire::AckMode::PerMessage,
            )
            .await
            .expect("a timed publish should succeed");
        fastest = fastest.min(started.elapsed());
    }
    fastest
}

/// **A failing disk fails the publish.** Every flush on the owner returns
/// `EIO`, so nothing it is asked to write can be made durable, and it must
/// say so rather than acknowledge.
#[serial]
#[tokio::test]
async fn a_failed_fsync_fails_the_publish() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::new(STREAM, 1)]))
        .await
        .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&owner, STREAM, b"before".to_vec())
        .await
        .expect("publish on a healthy disk");

    let fault = Fault::Fsync {
        node: owner.clone(),
        fault: FsyncFault::Fail,
    };
    cluster.inject(&fault).await.expect("inject");
    assert!(
        cluster
            .publish_via(&owner, STREAM, b"on-a-bad-disk".to_vec())
            .await
            .is_err(),
        "a publish was acknowledged although every fsync failed",
    );

    cluster.heal(&fault).await.expect("heal");
    cluster.shutdown().await;
}

/// **A flush that succeeds after one that failed proves nothing.** One flush
/// fails and the next would succeed, which is how Linux behaves after a lost
/// writeback. The log that saw the failure must stay stopped rather than
/// acknowledge on the strength of the retry.
#[serial]
#[tokio::test]
async fn a_retry_after_a_failed_fsync_is_not_trusted() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::new(STREAM, 1)]))
        .await
        .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");

    let fault = Fault::Fsync {
        node: owner.clone(),
        fault: FsyncFault::FailOnce,
    };
    cluster.inject(&fault).await.expect("inject");
    // The failure goes to whichever flush on `owner` comes first. Here that
    // is this publish's: one single-shard stream, and nothing else writing.
    assert!(
        cluster
            .publish_via(&owner, STREAM, b"lost-writeback".to_vec())
            .await
            .is_err(),
        "the publish whose fsync failed was acknowledged",
    );
    // The failure has been used up: the disk would now flush fine.
    for attempt in 0..3 {
        assert!(
            cluster
                .publish_via(&owner, STREAM, format!("retry-{attempt}").into_bytes())
                .await
                .is_err(),
            "a publish was acknowledged by a log whose earlier fsync failed",
        );
    }
    cluster.shutdown().await;
}

/// **A slow disk slows the acknowledgement by at least its delay**, and
/// healing it takes the delay away.
#[serial]
#[tokio::test]
async fn a_slow_fsync_slows_the_ack() {
    const DELAY: Duration = Duration::from_millis(300);
    let cluster = Cluster::start(on_commit(vec![StreamSpec::new(STREAM, 1)]))
        .await
        .expect("start");
    let owner = cluster.owner(STREAM).await.expect("owner");

    let fault = Fault::Fsync {
        node: owner.clone(),
        fault: FsyncFault::Delay(DELAY),
    };
    cluster.inject(&fault).await.expect("inject");
    let slow = publish_latency(&cluster, &owner).await;
    assert!(
        slow >= DELAY,
        "an acknowledgement on a disk delayed {DELAY:?} took {slow:?}",
    );

    cluster.heal(&fault).await.expect("heal");
    let fast = publish_latency(&cluster, &owner).await;
    assert!(fast < DELAY / 2, "healing left the disk slow: {fast:?}");
    cluster.shutdown().await;
}

/// **A `Quorum` leader whose own disk fails does not acknowledge.** The
/// followers may hold the record durably, but the leader has just learned
/// its log cannot be trusted, and the acknowledgement is its to give.
#[serial]
#[tokio::test]
async fn a_quorum_leader_with_a_failed_fsync_does_not_ack() {
    let cluster = Cluster::start(on_commit(vec![StreamSpec::quorum(STREAM, 1, 3)]))
        .await
        .expect("start");
    let leader = cluster.owner(STREAM).await.expect("owner");

    let fault = Fault::Fsync {
        node: leader.clone(),
        fault: FsyncFault::Fail,
    };
    cluster.inject(&fault).await.expect("inject");
    assert!(
        cluster
            .publish_via(&leader, STREAM, b"leader-disk-bad".to_vec())
            .await
            .is_err(),
        "a Quorum leader acknowledged a record its own fsync failed on",
    );
    cluster.heal(&fault).await.expect("heal");
    cluster.shutdown().await;
}
