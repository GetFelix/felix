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

/// **A shard placed on fewer brokers than its stream asks for forms no
/// group and acknowledges nothing.** The stream asks for three brokers and
/// the cluster has one, so placement names that one alone. A group of one
/// would acknowledge on its own, and brokers placed later would form the
/// full set again on empty logs and elect each other.
#[tokio::test]
#[serial]
async fn a_short_placement_acknowledges_nothing() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: Vec::new(),
        broker_env: vec![engine("raft")],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let only = cluster.node_ids().remove(0);
    cluster
        .create_stream(&StreamSpec::quorum(STREAM, 1, 3))
        .await
        .expect("create the stream");
    felix_cluster::wait::until(ELECTED_WITHIN, "the shard to be placed", || async {
        cluster.place_shards().await;
        cluster
            .shard_owners_for(STREAM)
            .await
            .is_ok_and(|owners| owners.get(&0) == Some(&only))
    })
    .await
    .expect("placed on the one broker");

    let mut acked = 0;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        cluster.place_shards().await;
        let publish = cluster.publish_via_at(&only, STREAM, b"alone".to_vec());
        if let Ok(Ok(Some(_))) = tokio::time::timeout(Duration::from_secs(2), publish).await {
            acked += 1;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        acked, 0,
        "one broker of three acknowledged records on its own"
    );
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

/// **An idempotent producer keeps its sequence when its Raft leader dies.**
/// The producer's id and sequences are in the entries, so the member
/// elected next answers a re-send of the last batch from its own log
/// without writing it, and takes the producer's next batch as the next in
/// sequence. Kept in the dead leader's memory, the re-send would be written
/// again and the producer told it is unknown.
#[tokio::test]
#[serial]
async fn a_producer_keeps_its_sequence_when_its_raft_leader_dies() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let addrs: Vec<std::net::SocketAddr> = survivors
        .iter()
        .map(|node| cluster.node(node).expect("node").client_addr)
        .collect();
    let client =
        felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("connect");
    let producer = std::sync::Arc::new(client)
        .idempotent_producer()
        .await
        .expect("a producer id");
    let record = |i: u32| format!("record-{i}").into_bytes();
    let publish = |i: u32| {
        let (producer, payload) = (&producer, record(i));
        let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
        async move { producer.publish(tenant, namespace, STREAM, payload).await }
    };
    // Acknowledged at the commit, so a majority holds each batch.
    for i in 0..5 {
        publish(i).await.expect("published before the failover");
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let elected = leader_among(&cluster, &survivors).await;

    // What a producer does when the answer to its last batch never came:
    // the same batch under the same sequence, to whoever leads now.
    let node = cluster.node(&elected).expect("node");
    let direct = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect to the new leader");
    let publisher = direct.publisher().await.expect("publisher");
    felix_cluster::wait::until(
        ELECTED_WITHIN,
        "the new leader to take the re-send",
        || async {
            publisher
                .publish_idempotent_batch(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    vec![record(4)],
                    producer.producer_id(),
                    4,
                )
                .await
                .is_ok()
        },
    )
    .await
    .expect("the re-send is answered");
    for i in 5..8 {
        publish(i).await.expect("published after the failover");
    }

    let owed: Vec<Vec<u8>> = (0..8).map(record).collect();
    let mut held = replay_holding(&cluster, &elected, &owed).await;
    // The harness's own readiness probe may be on the stream too.
    held.retain(|payload| payload != b"harness-probe");
    assert_eq!(held, owed, "a batch landed twice, or out of order");
    cluster.shutdown().await;
}

/// The sum of `node`'s samples of the metric whose line starts with
/// `series`, read from its own metrics.
async fn metric(cluster: &Cluster, node: &str, series: &str) -> f64 {
    let addr = cluster.node(node).expect("node").metrics_addr;
    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape metrics")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .filter(|line| line.starts_with(series))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// **Brokers on different engines refuse the shard they share, on both
/// sides.** One of three Raft brokers comes back on the hybrid engine with
/// an empty disk, as a replaced broker would. (On its old disk it would not
/// start: the Raft member's log there refuses the hybrid.) The two Raft
/// brokers stop serving the shard instead of running the group without it,
/// and the hybrid broker never appends to a log of its own, whatever is
/// published through it while the brokers find out. Every broker says why,
/// and counts the refusal.
#[tokio::test]
#[serial]
async fn brokers_on_different_engines_refuse_their_shared_shard() {
    if !opted_in() {
        return;
    }
    let hybrid = "broker-2".to_string();
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    publish_acked(&cluster, &leader_among(&cluster, &nodes).await, b"agreed").await;

    cluster
        .set_node_env(&hybrid, "FELIX_SHARD_ENGINE", "hybrid")
        .expect("set engine");
    cluster.terminate_node(&hybrid).expect("stop");
    cluster
        .wait_for_exit(&hybrid, ELECTED_WITHIN)
        .await
        .expect("exit");
    let storage = cluster.node(&hybrid).expect("node").storage_dir();
    std::fs::remove_dir_all(&storage).expect("empty the disk");
    cluster.restart_node(&hybrid).await.expect("restart");

    for node in &nodes {
        // Asked for while the brokers still learn each other's engine. An
        // acknowledgement here is the Raft pair's, reached by forwarding.
        let _ = cluster
            .publish_via_at(&hybrid, STREAM, b"meanwhile".to_vec())
            .await;
        let node = node.clone();
        let cluster = &cluster;
        felix_cluster::wait::until(
            ELECTED_WITHIN,
            &format!("{node} to refuse the shard"),
            || {
                let node = node.clone();
                async move {
                    metric(cluster, &node, "felix_broker_shard_engine_refused_total{").await >= 1.0
                }
            },
        )
        .await
        .expect("refused");
    }
    for node in &nodes {
        let err = cluster
            .publish_via_at(node, STREAM, b"disagreed".to_vec())
            .await
            .expect_err("a publish to a shard on two engines was acknowledged");
        assert!(
            format!("{err:#}").contains("different shard engines"),
            "{node} does not say why: {err:#}"
        );
    }
    assert_eq!(
        metric(&cluster, &hybrid, "felix_storage_append_records_total").await,
        0.0,
        "the hybrid broker appended to a log of its own"
    );
}
