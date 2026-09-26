//! A publisher that outlives the broker it was using.
//!
//! The cluster survives losing a leader; a client holding connection pools to
//! that broker has to survive it too. These cover the seam an application actually
//! sits on: not "did the cluster recover" but "could the program carry on".
//!
//! Run with `cargo test -p felix-cluster --test clients reconnect::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// **A publisher carries on after the broker it was using is killed.**
///
/// It resends, so the record that was in flight is not lost — at the cost the
/// method's name states.
#[serial]
#[tokio::test]
async fn a_publisher_survives_losing_its_broker() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"before".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("publish before the failure");

    let leader = cluster
        .wait_for_replication(STREAM, Duration::from_secs(20))
        .await
        .expect("the leader should ship and report");
    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");

    client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"after".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("the same client should carry on after its broker died");

    cluster.shutdown().await;
}

/// **A publish in flight to a leader that dies silently fails over in seconds.**
///
/// A killed broker sends no QUIC close, so nothing tells the client its
/// connection is gone except the transport's idle timeout. The publish waiting
/// on that connection has to end when the connection is declared dead, not
/// when the 30 s ack backstop fires, or every failover costs half a minute.
///
/// The leader is paused first so the publish is certainly sent to it and
/// unanswered when it dies. The clock covers promotion too, which the
/// harness's 1 s membership expiry keeps short.
#[serial]
#[tokio::test]
async fn a_publish_in_flight_to_a_killed_leader_fails_over_in_seconds() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    // The leader is the first seed, so it is the broker the client publishes
    // through and the connection that goes silent.
    let leader = cluster
        .wait_for_replication(STREAM, Duration::from_secs(20))
        .await
        .expect("the leader should ship and report");
    let leader_addr = cluster.node(&leader).expect("leader").client_addr;
    let mut seeds = cluster.broker_addrs();
    seeds.retain(|addr| *addr != leader_addr);
    seeds.insert(0, leader_addr);
    let client =
        felix_cluster::client::connect_cluster(&seeds, &cluster.tenant_id, &cluster.client_token)
            .await
            .expect("connect");
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    client
        .publish_at_least_once(
            &tenant,
            &namespace,
            STREAM,
            b"before".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("publish before the failure");

    cluster.pause_node(&leader).expect("pause the leader");
    let started = std::time::Instant::now();
    let publish = client.publish_at_least_once(
        &tenant,
        &namespace,
        STREAM,
        b"in flight".to_vec(),
        AckMode::PerMessage,
    );
    let fail_over = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cluster.kill_node(&leader).expect("kill the leader");
        felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
        })
        .await
        .expect("a replica should be promoted");
    };
    let (published, ()) = tokio::join!(publish, fail_over);
    let elapsed = started.elapsed();
    published.expect("the publish should land on the new leader");
    eprintln!("publish in flight to a killed leader completed in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(10),
        "failing over from a killed leader took {elapsed:?}",
    );

    cluster.shutdown().await;
}

/// **`publish` reconnects but does not resend.** The failed record is the
/// caller's to deal with; the next publish goes to a live broker rather than
/// repeating the failure against the dead one.
#[serial]
#[tokio::test]
async fn publish_reports_the_failure_and_leaves_a_usable_client() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");

    // Whether this particular publish fails depends on which broker the client
    // happened to be using, so it is not asserted either way. What is asserted
    // is that the client is usable afterwards.
    let _ = client
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"maybe".to_vec(),
            AckMode::PerMessage,
        )
        .await;

    let mut published = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !published && std::time::Instant::now() < deadline {
        if client
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                b"after".to_vec(),
                AckMode::PerMessage,
            )
            .await
            .is_ok()
        {
            published = true;
        } else {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    assert!(published, "the client never became usable again");

    cluster.shutdown().await;
}

/// Everything published through the failover is readable afterwards, in order.
#[serial]
#[tokio::test]
async fn records_published_across_a_failover_are_all_readable() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await
    .expect("connect");

    for i in 0..3 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("r{i}").into_bytes(),
                AckMode::PerMessage,
            )
            .await
            .expect("publish");
    }
    let leader = cluster
        .wait_for_replication(STREAM, Duration::from_secs(20))
        .await
        .expect("ship");
    cluster.kill_node(&leader).expect("kill");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("promotion");

    for i in 3..6 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("r{i}").into_bytes(),
                AckMode::PerMessage,
            )
            .await
            .expect("publish after failover");
    }

    let promoted = cluster.owner(STREAM).await.expect("owner");
    let (_c, mut sub) = cluster.replay_on(&promoted, STREAM).await.expect("replay");
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), sub.next_event()).await
    {
        seen.push(String::from_utf8_lossy(&event.payload).to_string());
    }

    for i in 0..6 {
        assert!(
            seen.contains(&format!("r{i}")),
            "r{i} is missing from {seen:?}",
        );
    }
    cluster.shutdown().await;
}

/// **A publish that cannot succeed is not retried.** The planned client-side
/// retry (#119) has to classify failures, and this is why: a credential without `stream.publish` fails
/// the same way on every broker and after every backoff, so retrying it only
/// delays the error the application needs to see.
///
/// The clock is the assertion. The policy is five attempts with a jittered
/// backoff that reaches seconds, so a retried failure takes a second or more; a
/// classified one comes back immediately.
#[serial]
#[tokio::test]
async fn a_forbidden_publish_fails_fast_instead_of_retrying() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let client = felix_cluster::client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        // Good enough to connect and subscribe, and not to publish.
        &cluster.subscribe_only_token,
    )
    .await
    .expect("connect with a subscribe-only credential");

    let started = std::time::Instant::now();
    let err = client
        .publish_at_least_once(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"denied".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("a token without stream.publish must not be accepted");
    let elapsed = started.elapsed();

    assert!(
        format!("{err:#}").contains("not retried"),
        "the failure was not classified as terminal: {err:#}",
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "a terminal failure took {elapsed:?}, so it went round the retry loop",
    );

    cluster.shutdown().await;
}

/// The next record that is not the harness's own probe.
async fn next_record(subscription: &mut felix_client::ClusterSubscription) -> felix_client::Event {
    let read = async {
        loop {
            let event = subscription
                .next_event()
                .await
                .expect("the subscription should carry on")
                .expect("the subscription ended");
            if event.payload.as_ref() != b"harness-probe" {
                return event;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .expect("no record within the timeout")
}

/// **A subscription outlives the broker it was reading from.**
///
/// The leader is killed under a `ClusterClient` subscription. The loss is not
/// a clean end: the subscription resubscribes on the promoted leader from the
/// offset after the last one it delivered, so a record published while it was
/// down arrives, and the one delivered before is not repeated.
#[serial]
#[tokio::test]
async fn a_subscription_resumes_after_its_broker_is_killed() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let leader = cluster
        .wait_for_replication(STREAM, Duration::from_secs(20))
        .await
        .expect("the leader should ship and report");
    // The leader first, so the subscription reads from the broker that dies.
    let leader_addr = cluster.node(&leader).expect("leader").client_addr;
    let mut seeds = cluster.broker_addrs();
    seeds.retain(|addr| *addr != leader_addr);
    seeds.insert(0, leader_addr);
    let client = std::sync::Arc::new(
        felix_cluster::client::connect_cluster(&seeds, &cluster.tenant_id, &cluster.client_token)
            .await
            .expect("connect"),
    );
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let mut subscription = client
        .subscribe(&tenant, &namespace, STREAM)
        .await
        .expect("subscribe");

    client
        .publish_at_least_once(
            &tenant,
            &namespace,
            STREAM,
            b"before".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("publish before the failure");
    assert_eq!(
        next_record(&mut subscription).await.payload.as_ref(),
        b"before"
    );

    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");
    client
        .publish_at_least_once(
            &tenant,
            &namespace,
            STREAM,
            b"after".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect("publish after the failover");

    assert_eq!(
        next_record(&mut subscription).await.payload.as_ref(),
        b"after"
    );
    assert_eq!(subscription.reconnects(), 1);

    cluster.shutdown().await;
}
