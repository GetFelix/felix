//! Durable stream shards on Raft, across real broker processes.
//!
//! Every broker here runs with `FELIX_SHARD_ENGINE=raft`, which needs a
//! broker built with the `raft` feature. The harness runs whatever
//! `felix-broker` sits beside the test binary, so these are opt-in: without
//! `FELIX_TEST_RAFT_SHARDS` set they pass without starting anything.
//!
//! ```text
//! cargo build -p felix-broker-service --bin felix-broker --features raft
//! FELIX_TEST_RAFT_SHARDS=1 cargo test -p felix-cluster --test raft_shards
//! ```
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";
const OPT_IN: &str = "FELIX_TEST_RAFT_SHARDS";
const ELECTED_WITHIN: Duration = Duration::from_secs(60);

/// Whether this run asked for the Raft tests. Says so when it did not, so a
/// pass in the default suite does not read as coverage.
fn opted_in() -> bool {
    let asked = std::env::var_os(OPT_IN).is_some();
    if !asked {
        eprintln!("skipped: set {OPT_IN}=1 and build felix-broker with --features raft");
    }
    asked
}

fn engine(name: &str) -> (String, String) {
    ("FELIX_SHARD_ENGINE".to_string(), name.to_string())
}

/// Three brokers, one shard, each broker a member of its group.
fn raft_cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![engine("raft")],
        ..Default::default()
    }
}

/// The broker that serves the shard: the one whose subscribe is not answered
/// with a redirect. Only brokers in `among` are asked.
async fn leader_among(cluster: &Cluster, among: &[String]) -> String {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a broker to lead the shard", || async {
        for node in among {
            if cluster.subscribe_on(node, STREAM).await.is_ok() {
                return Some(node.clone());
            }
        }
        None
    })
    .await
    .expect("a leader")
}

/// Publish through `node` until the broker acknowledges, and return the
/// offset. A shard between leaders answers "not ready", which a client
/// retries.
async fn publish_acked(cluster: &Cluster, node: &str, payload: &[u8]) -> u64 {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a publish to be acknowledged", || async {
        cluster
            .publish_via_at(node, STREAM, payload.to_vec())
            .await
            .ok()
            .flatten()
    })
    .await
    .unwrap_or_else(|err| panic!("publish through {node}: {err:#}"))
}

/// Everything `node` replays from the start of the stream, read until every
/// payload in `owed` has arrived.
async fn replay_holding(cluster: &Cluster, node: &str, owed: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let (_client, mut subscription) = cluster.replay_on(node, STREAM).await.expect("replay");
    let mut payloads = Vec::new();
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await {
            Ok(Ok(Some(event))) => payloads.push(event.payload.to_vec()),
            other => panic!(
                "{node} stopped at {} records with {} owed: {:?}",
                payloads.len(),
                owed.len(),
                other.map(|event| event.map(|event| event.is_some()))
            ),
        }
    }
    payloads
}

/// **A record published through any broker reaches a subscriber.** The
/// subscriber connects to a follower, which names the leader, and the client
/// follows it there.
#[tokio::test]
#[serial]
async fn a_publish_through_any_broker_reaches_a_subscriber() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("follower");

    let reader = std::sync::Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(follower).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe through a follower");

    let mut owed = Vec::new();
    let mut acked = Vec::new();
    for node in &nodes {
        let payload = format!("through-{node}").into_bytes();
        acked.push(publish_acked(&cluster, node, &payload).await);
        owed.push(payload);
    }
    assert!(
        acked.windows(2).all(|pair| pair[0] < pair[1]),
        "acknowledged offsets are not in publish order: {acked:?}"
    );

    let mut seen = Vec::new();
    let mut offsets = Vec::new();
    while !owed.iter().all(|payload| seen.contains(payload)) {
        let event = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s")
            .expect("delivery")
            .expect("the subscription stays open");
        offsets.push(event.offset.expect("a durable stream carries offsets"));
        seen.push(event.payload.to_vec());
    }
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "delivered out of order: {offsets:?}"
    );
}

/// **The leader dies while the control plane is down, and nothing
/// acknowledged is lost.** The group elects among the survivors on its own:
/// no placement, no lease and no promotion from the control plane. Publishes
/// continue, and the new leader holds every acknowledged record.
#[tokio::test]
#[serial]
async fn a_leader_lost_with_the_control_plane_down_loses_nothing_acknowledged() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let mut owed = Vec::new();
    let mut last = None;
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        last = Some(publish_acked(&cluster, &leader, &payload).await);
        owed.push(payload);
    }

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;

    // Through the survivor that does not lead, so the forward is covered too.
    let other = survivors
        .iter()
        .find(|node| **node != elected)
        .expect("the other survivor");
    for i in 0..20 {
        let payload = format!("after-{i}").into_bytes();
        let offset = publish_acked(&cluster, other, &payload).await;
        assert!(
            Some(offset) > last,
            "{elected} acknowledged offset {offset} at or below {last:?}"
        );
        last = Some(offset);
        owed.push(payload);
    }

    let held = replay_holding(&cluster, &elected, &owed).await;
    for payload in &owed {
        assert_eq!(
            held.iter().filter(|have| *have == payload).count(),
            1,
            "{elected} holds {:?} other than once",
            String::from_utf8_lossy(payload)
        );
    }
}

/// **A consumer group on a Raft shard is refused by name**, on the leader
/// and on a follower. A group's state is not in the Raft log yet, so serving
/// one would hand out records from state a failover loses.
#[tokio::test]
#[serial]
async fn a_consumer_group_on_a_raft_shard_is_refused() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"work").await;

    let err = cluster
        .group_poll_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect_err("the leader served a group on a Raft shard");
    assert!(
        format!("{err:#}").contains("a stream on Raft does not take"),
        "the leader's refusal does not name the reason: {err:#}"
    );
    for node in nodes.iter().filter(|node| **node != leader) {
        assert!(
            cluster
                .group_poll_via(node, STREAM, 0, "workers", 10)
                .await
                .is_err(),
            "{node} served a group on a Raft shard it does not lead"
        );
    }
}

/// Raft messages `node` was sent and refused, read from its own metrics.
async fn raft_messages_refused_by(cluster: &Cluster, node: &str) -> f64 {
    let addr = cluster.node(node).expect("node").metrics_addr;
    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape metrics")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .filter(|line| {
            line.starts_with("felix_broker_peer_served_total{")
                && line.contains("outcome=\"refused\"")
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// **A Raft broker sends a hybrid broker no Raft message.** Two brokers run
/// the shard under Raft and the third, in its replica set, runs the hybrid
/// engine. The two form the group between them and acknowledge publishes;
/// the third is never asked to vote or to append, so it cannot be counted
/// into a majority for a log it does not keep.
#[tokio::test]
#[serial]
async fn a_hybrid_broker_is_left_out_of_a_raft_group() {
    if !opted_in() {
        return;
    }
    let hybrid = "broker-2".to_string();
    let cluster = Cluster::start(ClusterConfig {
        node_env: vec![Vec::new(), Vec::new(), vec![engine("hybrid")]],
        ..raft_cluster()
    })
    .await
    .expect("start cluster");
    let raft: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| *node != hybrid)
        .collect();

    let leader = leader_among(&cluster, &raft).await;
    let mut owed = Vec::new();
    for i in 0..10 {
        let payload = format!("without-the-hybrid-{i}").into_bytes();
        publish_acked(&cluster, &leader, &payload).await;
        owed.push(payload);
    }
    replay_holding(&cluster, &leader, &owed).await;

    // Elections and appends ran for the whole test; one message to the
    // hybrid broker would have been refused there and counted.
    assert_eq!(
        raft_messages_refused_by(&cluster, &hybrid).await,
        0.0,
        "{hybrid} was sent a Raft message"
    );
    // Raft marks a shard's directory before it opens its log there.
    let storage = cluster.node(&hybrid).expect("node").storage_dir();
    assert!(
        !find(&storage, "engine"),
        "{hybrid} holds a Raft log directory under {}",
        storage.display()
    );
}

/// Whether a file called `name` exists anywhere under `dir`.
fn find(dir: &std::path::Path, name: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            find(&path, name)
        } else {
            entry.file_name() == name
        }
    })
}
