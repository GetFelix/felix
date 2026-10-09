//! A consumer group across brokers.
//!
//! The property under test is the one a queue exists for: a record is held by
//! one consumer at a time. Across a cluster that means only the broker leading
//! a shard may serve its groups — two brokers each keeping their own in-flight
//! state would each hand out the same records, and neither would know.
//!
//! Run with `cargo test -p felix-cluster --test queues consumer_groups::`.
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "jobs";
const GROUP: &str = "workers";

fn cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    }
}

/// **The cross-broker gate.** Only the shard's leader serves its groups; every
/// other broker refuses rather than starting a second, independent view of what
/// the group has been handed.
#[tokio::test]
#[serial]
async fn only_the_shard_owner_serves_a_group() {
    let cluster = Cluster::start(cluster()).await.expect("start cluster");
    let (owner, other) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("owner and non-owner");

    cluster
        .publish_via(&owner, STREAM, b"work".to_vec())
        .await
        .expect("publish");

    let refused = cluster.group_poll_via(&other, STREAM, 0, GROUP, 10).await;
    assert!(
        refused.is_err(),
        "{other} served a group for a shard it does not lead",
    );

    let served = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("the owner should serve the group");
    assert!(!served.is_empty(), "the owner returned nothing to consume");
}

/// **A cluster client's group calls reach the leader from any broker.**
/// Connected only to a broker that does not lead the shard, it follows the
/// `NotLeader` there, for a poll and for the ack after it. The Python and
/// TypeScript clients' group calls go through this.
#[tokio::test]
#[serial]
async fn a_cluster_client_follows_a_group_redirect_to_the_leader() {
    let cluster = Cluster::start(cluster()).await.expect("start cluster");
    let (owner, other) = cluster
        .owner_and_non_owner(STREAM)
        .await
        .expect("owner and non-owner");
    cluster
        .publish_via(&owner, STREAM, b"work".to_vec())
        .await
        .expect("publish");

    let client = felix_cluster::client::connect_cluster(
        &[cluster.node(&other).expect("the non-owner").client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect a cluster client to the non-owner");
    let records = client
        .group_poll(&cluster.tenant_id, &cluster.namespace, STREAM, 0, GROUP, 10)
        .await
        .expect("a poll through a non-owner should follow the redirect");
    assert!(
        !records.is_empty(),
        "the leader returned nothing to consume"
    );
    for record in &records {
        client
            .group_ack(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                0,
                GROUP,
                record.offset,
            )
            .await
            .expect("ack on the leader");
    }
    cluster.shutdown().await;
}

/// A record claimed through the owner is not handed out again while the claim
/// stands, no matter which broker is asked.
#[tokio::test]
#[serial]
async fn a_claimed_record_is_not_reissued_by_the_cluster() {
    let cluster = Cluster::start(cluster()).await.expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");

    cluster
        .publish_via(&owner, STREAM, b"once".to_vec())
        .await
        .expect("publish");

    let first = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(!first.is_empty());

    let second = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(
        second.is_empty(),
        "a record held by one consumer was handed to another",
    );
}

/// **A group's position survives losing the broker that led its shard**
/// (#314).
///
/// Before the cursors were replicated, a promoted replica had no record of the
/// group and started it at zero — redelivering everything already finished.
/// Within at-least-once, and severe enough to be a bug: a group that had
/// consumed a million records would be handed all million again.
#[tokio::test]
#[serial]
async fn a_group_position_survives_a_leader_failover() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        // Replicated, so there is a caught-up replica to promote.
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    for job in ["one", "two", "three"] {
        cluster
            .publish_via(&owner, STREAM, job.as_bytes().to_vec())
            .await
            .expect("publish");
    }

    // Consume and finish everything.
    let claimed = cluster
        .group_poll_via(&owner, STREAM, 0, GROUP, 10)
        .await
        .expect("poll");
    assert!(!claimed.is_empty(), "nothing to consume");
    // However many the harness's own startup probes add, this group finished
    // all of them. That count is what a fresh group should see afterwards, and
    // what this one should not.
    let finished = claimed.len();
    for offset in 0..finished as u64 {
        cluster
            .group_ack_via(&owner, STREAM, 0, GROUP, offset)
            .await
            .expect("ack");
    }
    assert!(
        cluster
            .group_poll_via(&owner, STREAM, 0, GROUP, 10)
            .await
            .expect("poll")
            .is_empty(),
        "the group should have finished its work before the kill",
    );

    // Give the cursors a pass to reach the replicas, then take the leader out.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    cluster.kill_node(&owner).expect("kill the owner");

    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| node != &owner)
        .collect();
    let promoted = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "a new leader for the stream",
        || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(next) if next != owner)
        },
    )
    .await;
    assert!(promoted.is_ok(), "no replica was promoted");
    let leader = cluster.owner(STREAM).await.expect("new owner");
    assert!(survivors.contains(&leader));

    // Waiting on a *different* group, deliberately. Polling the group under
    // test here would claim the very redelivery the assertion is about, and the
    // check below would then see an empty second poll whether or not the
    // position survived. It did exactly that in an earlier draft.
    let ready = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "the promoted leader to serve consumer groups",
        || async {
            cluster
                .group_poll_via(&leader, STREAM, 0, "control-group", 10)
                .await
                .is_ok_and(|records| !records.is_empty())
        },
    )
    .await;
    assert!(ready.is_ok(), "the promoted leader never served a group");

    // And that control group proves the records themselves survived, so an
    // empty answer below means the position was kept rather than that there was
    // nothing to hand out.
    let fresh = cluster
        .group_poll_via(&leader, STREAM, 0, "control-group-2", 10)
        .await
        .expect("poll as a new group");
    assert_eq!(
        fresh.len(),
        finished,
        "the promoted leader is missing the records themselves, so this test \
         cannot say anything about the group's position",
    );

    let redelivered = cluster
        .group_poll_via(&leader, STREAM, 0, GROUP, 10)
        .await
        .expect("poll the promoted leader");
    assert!(
        redelivered.is_empty(),
        "the group lost its position and was handed {} finished record(s) again",
        redelivered.len(),
    );
}

/// **The dead-letter list survives losing the broker that led its shard.** The
/// gap the status table named for as long as queues have existed: the cursors
/// replicated but the list of what a group gave up on did not, so a promotion
/// silently forgot exactly the records an operator was told to look at — and a
/// redrive after failover had nothing to redrive.
#[tokio::test]
#[serial]
async fn a_dead_letter_survives_a_leader_failover() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await
    .expect("start cluster");

    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&owner, STREAM, b"poison".to_vec())
        .await
        .expect("publish");

    // Hand the record back until the broker gives up on it. The default
    // attempt bound is generous, so this loops rather than assuming a number;
    // the guard bounds a broker that never gives up.
    let mut rounds = 0;
    let offset = loop {
        rounds += 1;
        assert!(rounds <= 20, "the record was never dead-lettered");
        let claimed = cluster
            .group_poll_records_via(&owner, STREAM, 0, GROUP, 10)
            .await
            .expect("poll");
        let Some(record) = claimed.first() else {
            let dead = cluster
                .group_dead_letters_via(&owner, STREAM, 0, GROUP)
                .await
                .expect("dead letters");
            assert_eq!(dead.len(), 1, "given up, but not listed");
            break dead[0];
        };
        cluster
            .group_nack_via(&owner, STREAM, 0, GROUP, record)
            .await
            .expect("nack");
    };

    // A pass for the group state to reach the replicas, then take the leader
    // out.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    cluster.kill_node(&owner).expect("kill the owner");

    let promoted = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "a new leader for the stream",
        || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(next) if next != owner)
        },
    )
    .await;
    assert!(promoted.is_ok(), "no replica was promoted");
    let leader = cluster.owner(STREAM).await.expect("new owner");

    // Being named owner and serving groups are different moments.
    let listed = felix_cluster::wait::until(
        std::time::Duration::from_secs(30),
        "the promoted leader to list the dead letter",
        || async {
            cluster
                .group_dead_letters_via(&leader, STREAM, 0, GROUP)
                .await
                .is_ok_and(|dead| dead == vec![offset])
        },
    )
    .await;
    assert!(
        listed.is_ok(),
        "the promoted leader lost the dead-letter list",
    );

    // And the list is live state there, not a read-only relic: the operator's
    // redrive works on the promoted leader, and the record comes back with its
    // attempts reset.
    cluster
        .group_redrive_via(&leader, STREAM, 0, GROUP, offset)
        .await
        .expect("redrive on the promoted leader");
    let redriven = cluster
        .group_poll_records_via(&leader, STREAM, 0, GROUP, 10)
        .await
        .expect("poll after redrive");
    assert_eq!(
        redriven.first().map(|record| record.offset),
        Some(offset),
        "the redriven record was not delivered",
    );
    assert!(
        cluster
            .group_dead_letters_via(&leader, STREAM, 0, GROUP)
            .await
            .expect("dead letters after redrive")
            .is_empty(),
        "a redriven record is still listed as given up on",
    );
}
