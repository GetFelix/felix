//! A client given several brokers rather than one.
//!
//! The cluster survives losing a leader. A client pointed at a single
//! address does not: the broker it was given can be the one that just died,
//! while every other broker sits there able to serve. These cover the seam
//! between "the cluster is fine" and "the application can tell".
//!
//! Run with `cargo test -p felix-cluster --test clients seed_endpoints::`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// The ordinary case: any broker in the seed list will do, because one that
/// does not own the shard forwards the publish to the one that does.
#[serial]
#[tokio::test]
async fn a_seed_list_publishes_through_whichever_broker_answers() {
    let cluster = Cluster::start(config()).await.expect("start cluster");

    cluster
        .publish_via_any(STREAM, b"seeded".to_vec())
        .await
        .expect("publish through a seed endpoint");

    cluster.shutdown().await;
}

/// **A dead broker in the list is skipped.** This is the whole point: the
/// address an application was configured with is exactly the one that may have
/// just failed over, and the cluster can still serve the write.
#[serial]
#[tokio::test]
async fn a_dead_broker_in_the_list_is_skipped() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let dead = cluster.nodes[0].node_id.clone();
    cluster.kill_node(&dead).expect("kill a broker");

    cluster
        .publish_via_any(STREAM, b"past-a-dead-broker".to_vec())
        .await
        .expect("a dead first endpoint should not stop the publish");

    cluster.shutdown().await;
}

/// **Publishing through a seed list survives losing the shard's leader.** The
/// cluster promotes a replica; the client reaches it because it was given more
/// than one way in.
#[serial]
#[tokio::test]
async fn a_seed_list_survives_losing_the_leader() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let leader = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&leader, STREAM, b"before".to_vec())
        .await
        .expect("publish");
    felix_cluster::wait::until(Duration::from_secs(20), "the leader to report", || async {
        matches!(
            cluster
                .metric(&leader, "felix_broker_replication_shipped_total")
                .await,
            Ok(Some(n)) if n > 0.0
        )
    })
    .await
    .expect("the leader should ship and report");

    cluster.kill_node(&leader).expect("kill the leader");
    felix_cluster::wait::until(Duration::from_secs(30), "a new leader", || async {
        cluster.place_shards().await;
        matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
    })
    .await
    .expect("a replica should be promoted");

    // Retried, and that is the finding rather than a convenience.
    //
    // A seed list gets the client to a live broker, but a broker that is not the
    // shard's owner forwards — and for a short window after a failover its
    // routing view still names the broker that just died, so the forward goes to
    // a corpse. It fails inside the ack budget and says what happened rather
    // than being replaced by "publish commit timeout", but it still fails: the
    // cluster recovers within a feed interval, and one publish call spans less
    // than that.
    //
    // `ClusterClient` reconnects on a failed publish so the *next* call lands on
    // a live broker, which is what makes this loop terminate quickly. It does
    // not retry the publish itself, and that is deliberate rather than missing:
    // a publish whose answer was lost may already be on the owner's disk, so
    // retrying it inside the client would manufacture a duplicate the client is
    // the only layer able to detect. `services/felix-broker-service/src/serving/forward.rs`
    // argues the same rule for the broker's own forwarding. Idempotent
    // producers (#422) are what would make an absorbing retry safe.
    //
    // So this loop is what an application writes today, and the assertion is
    // that it converges — not that a single publish survives a failover.
    let mut published = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut last = String::new();
    while !published && std::time::Instant::now() < deadline {
        match cluster.publish_via_any(STREAM, b"after".to_vec()).await {
            Ok(()) => published = true,
            Err(err) => {
                last = format!("{err:#}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    assert!(
        published,
        "a seed list never reached the promoted leader; last failure: {last}",
    );

    cluster.shutdown().await;
}

/// **Nothing answering is one error naming every endpoint**, not the last
/// one's. A credential that the first broker refused is invisible if only the
/// final "connection refused" survives.
#[serial]
#[tokio::test]
async fn nothing_answering_reports_every_endpoint() {
    let mut cluster = Cluster::start(config()).await.expect("start cluster");
    let addrs = cluster.broker_addrs();
    for node in cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<Vec<_>>()
    {
        cluster.kill_node(&node).expect("kill");
    }

    let message = match felix_cluster::client::connect_any(
        &addrs,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    {
        Ok(_) => panic!("a broker answered after every one was killed"),
        Err(err) => format!("{err:#}"),
    };
    for addr in &addrs {
        assert!(
            message.contains(&addr.to_string()),
            "{addr} is missing from: {message}",
        );
    }
    cluster.shutdown().await;
}

/// An empty seed list is a configuration error, said plainly rather than as a
/// connection failure to nowhere.
#[tokio::test]
async fn an_empty_seed_list_is_refused() {
    let message = match felix_cluster::client::connect_any(&[], "t1", "token").await {
        Ok(_) => panic!("an empty list should be refused"),
        Err(err) => format!("{err:#}"),
    };

    assert!(message.contains("no broker addresses"), "{message}");
}
