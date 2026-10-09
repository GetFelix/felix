//! Retention on a `Quorum` leader whose followers are all down (#1094).
//!
//! Run with `cargo test -p felix-cluster --test failures retention_floor::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// **Retention never deletes a record a `Quorum` publish is still waiting
/// on.** Both followers stop, so the publishes made then wait for a majority
/// on the leader alone, while retention ticks every second over a bound they
/// exceed several times. If retention reached them, the followers would come
/// back below the leader's new base, be rebuilt there, and their answers
/// would acknowledge the waiting publishes for records no broker holds. Held
/// at the commit offset, the leader keeps its log from the last acknowledged
/// record, the followers catch up by ordinary shipping, and every waiting
/// publish is acknowledged.
#[serial]
#[tokio::test]
async fn retention_waits_for_the_followers_of_a_quorum_stream() {
    let env = |key: &str, value: &str| (key.to_string(), value.to_string());
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        broker_env: vec![
            env("FELIX_DURABLE_SEGMENT_BYTES", "4096"),
            env("FELIX_DURABLE_RETENTION_BYTES", "8192"),
            env("FELIX_DURABLE_RETENTION_INTERVAL_SECONDS", "1"),
            // Long enough to outlast the outage below, and inside the
            // client's own 30 s backstop.
            env("FELIX_PUBLISH_QUORUM_TIMEOUT_MS", "25000"),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    let placed = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed");
    let leader = placed.leader.clone();
    let followers = placed.replicas.clone();
    assert_eq!(followers.len(), 2, "{placed:?}");

    // The harness publishes a probe of its own first, so the log's start is
    // read from offsets rather than payloads.
    let mut early = None;
    for i in 0..5 {
        let offset = cluster
            .publish_via_at(&leader, STREAM, format!("early-{i}").into_bytes())
            .await
            .expect("publish while whole");
        early = early.or(offset);
    }
    let early = early.expect("a durable publish reports its offset");
    felix_cluster::wait::until(Duration::from_secs(20), "every copy level", || async {
        cluster
            .replica_report(STREAM, 0)
            .await
            .ok()
            .flatten()
            .is_some_and(|report| followers.iter().all(|f| report.caught_up.contains(f)))
    })
    .await
    .expect("both followers hold the early records");

    for follower in &followers {
        cluster.stop_node(follower).await.expect("stop a follower");
    }
    // Each on its own connection, so none waits behind another's ack.
    let filler = "x".repeat(1024);
    let mut waiting = tokio::task::JoinSet::new();
    for i in 0..24 {
        let client = cluster.client_on(&leader).await.expect("connect");
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        let payload = format!("{i}-{filler}").into_bytes();
        waiting.spawn(async move {
            let publisher = client.publisher().await?;
            publisher
                .publish(
                    &tenant,
                    &namespace,
                    STREAM,
                    payload,
                    felix_wire::AckMode::PerMessage,
                )
                .await
        });
    }
    // Three retention ticks over more than twice the bound.
    tokio::time::sleep(Duration::from_secs(4)).await;
    if let Some(answer) = waiting.try_join_next() {
        panic!("a publish was answered with no follower up: {answer:?}");
    }

    let first = {
        let (_client, mut subscription) = cluster.replay_on(&leader, STREAM).await.expect("replay");
        tokio::time::timeout(Duration::from_secs(5), subscription.next_event())
            .await
            .expect("a replayed record")
            .expect("replay")
            .expect("a record")
    };
    let start = first
        .offset
        .expect("a durable stream's events carry offsets");
    assert!(
        start <= early,
        "retention deleted records above the commit offset: the leader's log now \
         starts at {start}, past early-0 at {early}",
    );

    for follower in &followers {
        cluster
            .restart_node(follower)
            .await
            .expect("restart a follower");
    }
    let answers = tokio::time::timeout(Duration::from_secs(40), waiting.join_all())
        .await
        .expect("the waiting publishes were answered");
    for (i, answer) in answers.into_iter().enumerate() {
        answer.unwrap_or_else(|err| panic!("publish {i} was not acknowledged: {err:#}"));
    }
}
