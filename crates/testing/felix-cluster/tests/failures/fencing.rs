//! What happens to a leader that was never told it stopped being one.
//!
//! The central safety criterion of replication (#115): *no record is
//! acknowledged by two
//! conflicting leaders for the same logical epoch*. A killed leader cannot
//! violate it — it is gone. A **paused** one can: it is alive, it still
//! believes it holds the shard, and it comes back after the cluster has moved
//! on. That is the case worth injecting, and it is what `pause_node` exists
//! for.
//!
//! Run with `cargo test -p felix-cluster --test failures fencing::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn quorum_cluster(nodes: usize, replication_factor: u32) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![StreamSpec::quorum(STREAM, 1, replication_factor)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    }
}

/// Everything `node_id` will replay, retried until it yields something: being
/// named leader and being ready to serve are different moments.
async fn replay(cluster: &Cluster, node_id: &str) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok((_client, mut subscription)) = cluster.replay_on(node_id, STREAM).await {
            let mut payloads = Vec::new();
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
            {
                payloads.push(String::from_utf8_lossy(&event.payload).to_string());
            }
            if !payloads.is_empty() {
                return payloads;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Vec::new();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Publish through a broker that has just been promoted, retrying while it gets
/// ready.
///
/// Being named leader and being able to serve are different moments: the
/// promoted broker learns of its own promotion through the same watch as
/// everything else, and has to open the shard before it can accept a write.
/// Retrying is what the cluster asks of an application here; teaching the
/// client to do it for itself is tracked as #119.
async fn publish_when_ready(cluster: &Cluster, node_id: &str, payload: &[u8]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        match cluster.publish_via(node_id, STREAM, payload.to_vec()).await {
            Ok(()) => return,
            Err(err) => last = format!("{err:#}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!(
        "{} never accepted a publish within 30s of being promoted; last: {last}",
        node_id
    );
}

/// The leader's tail in the last replica report the control plane holds.
async fn reported_tail(cluster: &Cluster) -> u64 {
    let report = cluster
        .replica_report(STREAM, 0)
        .await
        .expect("read the replica report")
        .expect("start-up waits for a replica report");
    report.leader_offset.expect("the leader reports its tail")
}

/// Wait until the control plane holds a report made past `tail` that names a
/// follower caught up with the leader.
async fn wait_for_caught_up_past(cluster: &Cluster, tail: u64) {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(10));
    loop {
        let report = cluster.replica_report(STREAM, 0).await.ok().flatten();
        if let Some(report) = &report
            && report.leader_offset.is_some_and(|reported| reported > tail)
            && !report.caught_up.is_empty()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the control plane never heard of a follower holding the record: {report:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for a broker other than `gone` to own the shard.
async fn failover_from(cluster: &Cluster, gone: &str) -> Option<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        // The harness's control plane does not reconcile on a timer, so a
        // failure test has to step placement itself. Without this nothing
        // re-plans after the fault and the wait measures nothing.
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(STREAM).await
            && owner != gone
        {
            return Some(owner);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// What every broker in the cluster holds, and who the cluster thinks owns the
/// shard.
///
/// The replication work (#115) asks for a timeline when a scenario fails,
/// because "a record is
/// missing" is not something anyone can act on: the question is always whether
/// it is missing from the log or missing from the answer, and which brokers
/// have it.
async fn timeline(cluster: &Cluster, note: &str) -> String {
    let mut out = format!("\n--- {note} ---\n");
    match cluster.owner(STREAM).await {
        Ok(owner) => out.push_str(&format!("owner: {owner}\n")),
        Err(err) => out.push_str(&format!("owner: unresolved ({err})\n")),
    }
    for node in &cluster.nodes {
        let running = node.is_running();
        let held = if running {
            match tokio::time::timeout(Duration::from_secs(5), replay_once(cluster, &node.node_id))
                .await
            {
                Ok(payloads) => format!("{payloads:?}"),
                Err(_) => "replay timed out".to_string(),
            }
        } else {
            "not running".to_string()
        };
        out.push_str(&format!(
            "  {:<10} running={running:<5} {held}\n",
            node.node_id
        ));
    }
    out
}

/// One replay attempt, without the retry the assertions use.
async fn replay_once(cluster: &Cluster, node_id: &str) -> Vec<String> {
    let Ok((_client, mut subscription)) = cluster.replay_on(node_id, STREAM).await else {
        return Vec::new();
    };
    let mut payloads = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(2), subscription.next_event()).await
    {
        payloads.push(String::from_utf8_lossy(&event.payload).to_string());
    }
    payloads
}

/// **A leader that was paused past its lease does not acknowledge writes the
/// cluster never sees.** The one thing fencing exists to prevent.
///
/// A pause is the interesting fault precisely because the process survives it.
/// The broker wakes believing it still leads the shard at its old generation;
/// if nothing stopped it, it would accept a write, acknowledge it, and write it
/// to a log no promoted replica is reading. The client would be told the record
/// is durable and it would not exist.
///
/// The test does not require the write to *fail* — forwarding it to the broker
/// that now owns the shard is a perfectly good answer, and the ordinary one.
/// What it requires is that a write this broker acknowledged is really there.
#[serial]
#[tokio::test]
async fn a_resumed_leader_does_not_acknowledge_writes_the_cluster_loses() {
    let cluster = Cluster::start(quorum_cluster(3, 3))
        .await
        .expect("start cluster");
    let deposed = cluster.owner(STREAM).await.expect("owner");

    for index in 1..=3u32 {
        cluster
            .publish_via(&deposed, STREAM, format!("before-{index}").into_bytes())
            .await
            .expect("publish before the pause");
    }

    cluster.pause_node(&deposed).expect("pause the leader");
    let promoted = failover_from(&cluster, &deposed)
        .await
        .expect("a replica should have been promoted while the leader was frozen");

    // Awake, and still holding the beliefs it had when it was frozen.
    cluster
        .resume_node(&deposed)
        .expect("resume the old leader");

    let acknowledged = cluster
        .publish_via(&deposed, STREAM, b"after-resume".to_vec())
        .await
        .is_ok();

    let payloads = replay(&cluster, &promoted).await;

    for index in 1..=3u32 {
        let expected = format!("before-{index}");
        if !payloads.contains(&expected) {
            let timeline = timeline(&cluster, "after the resume").await;
            panic!(
                "{expected} was acknowledged before the pause and is not on the promoted \
                 leader {promoted} (deposed {deposed}); it replayed {payloads:?}{timeline}",
            );
        }
    }
    if acknowledged {
        assert!(
            payloads.contains(&"after-resume".to_string()),
            "the resumed leader acknowledged a record the cluster does not have: {payloads:?}",
        );
    }

    cluster.shutdown().await;
}

/// The same fault on a `Leader` stream acknowledged on enqueue, the default.
///
/// Here the ack goes out before the write, so a resumed broker whose cached
/// lease flag has not caught up could acknowledge a record its worker then
/// refuses to write. It may refuse, forward or write; it may not acknowledge
/// and drop. The unit tests beside the publish handler place the pause exactly;
/// this checks the same promise end to end.
#[serial]
#[tokio::test]
async fn a_resumed_leader_acking_on_enqueue_does_not_acknowledge_writes_the_cluster_loses() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 3)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let deposed = cluster.owner(STREAM).await.expect("owner");
    let tail_before = reported_tail(&cluster).await;

    cluster
        .publish_via(&deposed, STREAM, b"before".to_vec())
        .await
        .expect("publish before the pause");
    // A `Leader` ack does not wait for the followers, so wait until the control
    // plane has heard that one holds the record. Paused before that, the
    // leader's last report can name no follower at its tail, and placement
    // rightly refuses to promote any of them.
    wait_for_caught_up_past(&cluster, tail_before).await;

    cluster.pause_node(&deposed).expect("pause the leader");
    let promoted = failover_from(&cluster, &deposed)
        .await
        .expect("a replica should have been promoted while the leader was frozen");
    cluster
        .resume_node(&deposed)
        .expect("resume the old leader");

    let acknowledged = cluster
        .publish_via(&deposed, STREAM, b"after-resume".to_vec())
        .await
        .is_ok();

    let payloads = replay(&cluster, &promoted).await;
    if acknowledged && !payloads.contains(&"after-resume".to_string()) {
        let timeline = timeline(&cluster, "after the resume").await;
        panic!(
            "the resumed leader {deposed} acknowledged a record the promoted leader \
             {promoted} does not have: {payloads:?}{timeline}",
        );
    }

    cluster.shutdown().await;
}

/// **A paused follower does not stop a quorum the rest of the set can reach.**
/// Losing a minority is the case `Quorum` exists to tolerate; a pause is the
/// sharpest version of it, because the follower is neither answering nor
/// refusing.
#[serial]
#[tokio::test]
async fn a_frozen_follower_does_not_block_a_quorum() {
    let cluster = Cluster::start(quorum_cluster(3, 3))
        .await
        .expect("start cluster");
    let (leader, follower) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("resolve a follower");

    cluster.pause_node(&follower).expect("pause a follower");

    // The leader and the one remaining follower are two of three: a majority.
    cluster
        .publish_via(&leader, STREAM, b"despite-a-frozen-follower".to_vec())
        .await
        .expect("a majority was available and the publish should have been acknowledged");

    cluster.resume_node(&follower).expect("resume");
    cluster.shutdown().await;
}

/// **Records survive two failovers in a row.** One failover proves promotion
/// works; two prove the cluster is still correct afterwards rather than merely
/// still running.
///
/// Five replicas so a majority survives both losses: three of five is a quorum,
/// and three brokers are left.
#[serial]
#[tokio::test]
async fn records_survive_repeated_failovers() {
    let mut cluster = Cluster::start(quorum_cluster(5, 5))
        .await
        .expect("start cluster");

    let first = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&first, STREAM, b"epoch-1".to_vec())
        .await
        .expect("publish under the first leader");

    cluster.kill_node(&first).expect("kill the first leader");
    let second = failover_from(&cluster, &first)
        .await
        .expect("a replica should have been promoted");
    publish_when_ready(&cluster, &second, b"epoch-2").await;

    cluster.kill_node(&second).expect("kill the second leader");
    let third = failover_from(&cluster, &second)
        .await
        .expect("a replica should have been promoted a second time");

    let payloads = replay(&cluster, &third).await;

    for expected in ["epoch-1", "epoch-2"] {
        assert!(
            payloads.contains(&expected.to_string()),
            "{expected} was acknowledged under a quorum and did not survive two failovers: {payloads:?}",
        );
    }

    cluster.shutdown().await;
}

/// **A `Quorum` publish with no available majority is refused.** The guarantee
/// stated at its simplest: freeze every follower, and a majority of three
/// cannot exist, so the broker must not claim one.
///
/// This failed before #282. `Quorum` degraded to the `Leader` behaviour
/// whenever `ack_on_commit` was off -- the default -- because the enqueue-ack
/// path answered "accepted into the ingress queue" to a question about
/// majorities, and the quorum wait ran in a worker nobody was listening to.
#[serial]
#[tokio::test]
async fn a_quorum_publish_without_a_majority_is_refused() {
    let cluster = Cluster::start(quorum_cluster(3, 3))
        .await
        .expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    let followers: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|id| id != &leader)
        .collect();

    for follower in &followers {
        cluster.pause_node(follower).expect("pause");
    }

    let outcome = cluster
        .publish_via(&leader, STREAM, b"no-majority".to_vec())
        .await;

    for follower in &followers {
        let _ = cluster.resume_node(follower);
    }
    assert!(
        outcome.is_err(),
        "a Quorum publish was acknowledged with no majority available",
    );
    cluster.shutdown().await;
}
