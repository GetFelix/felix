//! Retention on a `Quorum` leader whose followers are all down (#1094).
//!
//! Run with `cargo test -p felix-cluster --test failures retention_floor::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// The last index in `leader`'s member of the stream's group: a Raft
/// leader reports no tail to the replica report.
async fn last_index(cluster: &Cluster, leader: &str) -> Option<u64> {
    cluster
        .raft_member(leader, STREAM)
        .await
        .and_then(|member| member.last_index)
}

/// **Retention never deletes a record a `Quorum` publish is still waiting
/// on.** Both followers stop, so the publishes made then wait for a majority
/// on the leader alone, while retention ticks every second over a bound they
/// exceed several times. If retention reached them, the followers would come
/// back below the leader's new base, be rebuilt there, and their answers
/// would acknowledge the waiting publishes for records no broker holds. Held
/// at the commit offset, the leader keeps its log from the last acknowledged
/// record, the followers catch up by ordinary shipping, and every waiting
/// publish is acknowledged. A Raft leader may instead refuse a publish it has
/// no majority for, writing nothing; none is acknowledged either way.
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
    // On Raft the member the group elected serves, not the one placement named.
    let leader = cluster
        .serving_leader(STREAM, Duration::from_secs(30))
        .await
        .expect("leader");
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    assert_eq!(followers.len(), 2, "{followers:?}");

    // The harness publishes a probe of its own first, so the log's start is
    // read from offsets rather than payloads.
    let mut early = None;
    let mut last_early = 0;
    for i in 0..5 {
        let offset = cluster
            .publish_via_at(&leader, STREAM, format!("early-{i}").into_bytes())
            .await
            .expect("publish while whole");
        early = early.or(offset);
        last_early = offset.unwrap_or(last_early);
    }
    let early = early.expect("a durable publish reports its offset");
    // The leader's tail once both followers hold every early record. A report
    // can lag the last ack, so the tail must also be past the last early
    // offset, or the first waiting record could not be told apart.
    let raft = felix_cluster::suite_runs_on_raft();
    if raft {
        cluster
            .wait_for_replication(STREAM, Duration::from_secs(20))
            .await
            .expect("both followers hold the early records");
    }
    let settled_tail = if raft {
        last_early
    } else {
        felix_cluster::wait::until_some(
            Duration::from_secs(20),
            "both followers at the leader's tail",
            || async {
                let report = cluster.replica_report(STREAM, 0).await.ok().flatten()?;
                let tail = report.leader_offset.filter(|tail| *tail > last_early)?;
                followers
                    .iter()
                    .all(|f| report.offsets.get(f) == Some(&tail))
                    .then_some(tail)
            },
        )
        .await
        .expect("both followers hold the early records")
    };

    let settled_index = if raft {
        last_index(&cluster, &leader).await
    } else {
        None
    };

    for follower in &followers {
        cluster.stop_node(follower).await.expect("stop a follower");
    }
    // Each on its own connection, so none waits behind another's ack. All
    // connect first, so the quorum timeout covers only the outage.
    let mut clients = Vec::new();
    for _ in 0..24 {
        clients.push(cluster.client_on(&leader).await.expect("connect"));
    }
    let filler = "x".repeat(1024);
    let mut waiting = tokio::task::JoinSet::new();
    let mut answered = Vec::new();
    for (i, client) in clients.into_iter().enumerate() {
        if i == 1 && raft {
            // A Raft leader with no majority in reach may refuse a record
            // outright and write nothing, rather than hold it. Either way the
            // first is settled before the rest are sent.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while answered.is_empty() && last_index(&cluster, &leader).await <= settled_index {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the first waiting record was neither appended nor refused",
                );
                if let Some(answer) = waiting.try_join_next() {
                    answered.push(answer.expect("publish task"));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } else if i == 1 {
            // The first waiting record lands alone, in the segment that holds
            // the early ones. Arriving together, the rest can start a new
            // segment there, leaving one wholly below the commit offset that
            // retention may delete, and a replay from the start then waits for
            // the commit offset to move. Wait for the leader to have appended
            // it before sending the rest.
            felix_cluster::wait::until(
                Duration::from_secs(20),
                "the leader's tail to pass the commit offset",
                || async {
                    cluster
                        .replica_report(STREAM, 0)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|report| report.leader_offset)
                        .is_some_and(|tail| tail > settled_tail)
                },
            )
            .await
            .expect("the first waiting record reached the leader");
        }
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        let payload = format!("{i}-{filler}").into_bytes();
        waiting.spawn(async move {
            let publisher = client.publisher().await?;
            let answer = publisher
                .publish(
                    &tenant,
                    &namespace,
                    STREAM,
                    payload,
                    felix_wire::AckMode::PerMessage,
                )
                .await;
            anyhow::Ok((i, answer))
        });
    }
    // Three retention ticks over more than twice the bound.
    tokio::time::sleep(Duration::from_secs(4)).await;
    while let Some(answer) = waiting.try_join_next() {
        answered.push(answer.expect("publish task"));
    }
    // No publish is acknowledged with no follower up. A Raft leader refuses
    // what it cannot reach a majority for, and that refusal wrote nothing.
    let mut refused = std::collections::BTreeSet::new();
    for answer in answered {
        match answer {
            Ok((i, Err(err))) if raft && format!("{err:#}").contains("nothing was written") => {
                refused.insert(i);
            }
            other => panic!("a publish was answered with no follower up: {other:?}"),
        }
    }

    if raft {
        // A Raft leader trims only by a committed entry, so with no majority
        // retention cannot move its floor. Nor can it confirm that floor: a
        // read from the start needs a read-index round, which no majority
        // answers, so the replay is refused rather than served.
        let refused = cluster.replay_on(&leader, STREAM).await;
        let err = refused
            .err()
            .expect("a leader with no majority served a replay");
        assert!(
            format!("{err:#}").contains("cannot be subscribed to right now"),
            "the replay was refused for another reason: {err:#}"
        );
        // The floor is the member's own, read with no majority: it must not
        // have passed the first record a down follower still needs.
        assert_raft_floor_held(&cluster, &[&leader], early).await;
    } else {
        assert_floor_held(&cluster, &leader, early).await;
    }

    for follower in &followers {
        cluster
            .restart_node(follower)
            .await
            .expect("restart a follower");
    }
    if raft {
        let all = cluster.node_ids();
        let all: Vec<&str> = all.iter().map(String::as_str).collect();
        assert_raft_floor_held(&cluster, &all, early).await;
    }
    let answers = tokio::time::timeout(Duration::from_secs(40), waiting.join_all())
        .await
        .expect("the waiting publishes were answered");
    assert_eq!(
        answers.len() + refused.len(),
        24,
        "every publish was answered"
    );
    // A refusal made during the outage can be answered only now: answers
    // keep the order the batches took, so it waits behind a held record
    // that commits once a follower is back. It still wrote nothing.
    for answer in answers {
        let (i, answer) = answer.expect("publish task");
        match answer {
            Err(err) if raft && format!("{err:#}").contains("nothing was written") => {}
            answer => {
                answer.unwrap_or_else(|err| panic!("publish {i} was not acknowledged: {err:#}"));
            }
        }
    }
    felix_cluster::wait::until(
        Duration::from_secs(30),
        "a publish acknowledged with the followers back",
        || async {
            cluster
                .publish_via(&leader, STREAM, b"after".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("the leader takes publishes once its followers are back");
}

/// The leader's log still starts at or below `early`, read by a replay.
async fn assert_floor_held(cluster: &Cluster, leader: &str, early: u64) {
    let first = {
        let (_client, mut subscription) = cluster.replay_on(leader, STREAM).await.expect("replay");
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
}

/// Every member in `nodes` still serves from `early` or below, by its own
/// readable base. Raft has no read that answers without a majority, so the
/// floor is read from the member rather than by a replay.
async fn assert_raft_floor_held(cluster: &Cluster, nodes: &[&str], early: u64) {
    for node in nodes {
        let base = felix_cluster::wait::until_some(
            Duration::from_secs(20),
            "a member to report its readable base",
            || async {
                cluster
                    .raft_member(node, STREAM)
                    .await
                    .and_then(|member| member.readable_base)
            },
        )
        .await
        .expect("readable base");
        assert!(
            base <= early,
            "retention moved {node}'s floor to {base}, past early-0 at {early}",
        );
    }
}
