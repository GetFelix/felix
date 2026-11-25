//! `consistency` on the Raft shard engine.
//!
//! The fault is the same as on the hybrid, a leader cut off from its
//! followers and then lost, but the cost of `Leader` is different. A Raft
//! group's two followers are a majority, so they elect one of themselves and
//! the shard stays available. What `Leader` gives up is the records its old
//! leader acknowledged alone, on its lease.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};

use super::super::{beat, step};

const STREAM: &str = "orders";

/// How long a healthy group may take to elect a leader or catch a member up.
const SETTLES_WITHIN: Duration = Duration::from_secs(30);

/// A publish not answered this soon is counted as unanswered. Well under the
/// leader lease (1.5 s by default), so a publisher stops inside it.
const ANSWERED_WITHIN: Duration = Duration::from_secs(1);

/// What one run of the fault did to one stream.
struct Outcome {
    /// Records the publisher was told were written, in order.
    acknowledged: Vec<String>,
    /// The acknowledged records the new leader does not hold.
    missing: Vec<String>,
}

pub(super) async fn consistency(pace: Duration) -> Result<()> {
    step("Two streams, identical but for one field");
    println!("  Each gets its own three-broker cluster and is replicated three ways.");
    println!("  The only difference is `consistency`:");
    println!("    Quorum   acknowledged once a majority holds the record");
    println!("    Leader   acknowledged from the leader's own log while its lease");
    println!("             holds, which is the default");
    beat(pace).await;

    let quorum = run("Quorum", StreamSpec::quorum(STREAM, 1, 3), pace).await?;
    let leader = run("Leader", StreamSpec::replicated(STREAM, 1, 3), pace).await?;

    step("The trade, stated plainly");
    println!("  The same fault, a leader cut off from its followers and then lost:");
    println!();
    println!(
        "    Quorum   {} acknowledged, {} missing afterwards.",
        quorum.acknowledged.len(),
        quorum.missing.len()
    );
    println!(
        "    Leader   {} acknowledged, {} missing afterwards.",
        leader.acknowledged.len(),
        leader.missing.len()
    );
    println!();
    println!("  Both shards stayed available: the two followers are a majority, so");
    println!("  they elected a new leader. Quorum stopped answering the moment the");
    println!("  leader was alone, so nothing it acknowledged could be lost with it.");
    if leader.missing.is_empty() {
        println!("  Leader lost nothing this time: its leader was cut off between two");
        println!("  publishes and answered none alone. It can, for as long as the lease.");
    } else {
        println!("  Leader kept answering from its own log until its lease ran out, and");
        println!("  \x1b[1mthose acknowledged records went with the leader\x1b[0m.");
    }
    println!();
    println!("  `Leader` is the default because one round trip beats two and most");
    println!("  streams would rather have the latency than the guarantee. Set");
    println!("  `consistency: Quorum` on the ones that must keep every acknowledged");
    println!("  record across a failover.");

    if !quorum.missing.is_empty() {
        bail!(
            "a Quorum stream lost {} acknowledged record(s), the first {:?}",
            quorum.missing.len(),
            quorum.missing[0]
        );
    }
    Ok(())
}

/// Cut a stream's leader off while a publisher is publishing to it, kill the
/// leader, and compare what the publisher was told with what the next leader
/// holds.
async fn run(level: &str, stream: StreamSpec, pace: Duration) -> Result<Outcome> {
    step(&format!("{level}: a publisher on the leader"));
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![stream],
        ..Default::default()
    })
    .await?;
    let leader = cluster.serving_leader(STREAM, SETTLES_WITHIN).await?;
    println!("  {STREAM} is led by {leader}. Publishing a record every 20 ms.");

    // Publishing before the cut and on through it: the records answered just
    // after it are the ones the two levels treat differently.
    let client = cluster.client_on(&leader).await?;
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let prefix = level.to_string();
    let publishing = tokio::spawn(async move {
        let mut acknowledged = Vec::new();
        let Ok(publisher) = client.publisher().await else {
            return acknowledged;
        };
        for index in 1..=100_000u32 {
            let payload = format!("{prefix}-{index:04}");
            let publish = publisher.publish(
                &tenant,
                &namespace,
                STREAM,
                payload.clone().into_bytes(),
                felix_wire::AckMode::PerMessage,
            );
            match tokio::time::timeout(ANSWERED_WITHIN, publish).await {
                Ok(Ok(_)) => acknowledged.push(payload),
                _ => break,
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        acknowledged
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    beat(pace).await;

    step(&format!("{level}: cutting {leader} off from its followers"));
    cluster.partition_node(&leader)?;
    println!("  {leader} is running and reachable by clients. It cannot reach the");
    println!("  other two brokers, and they cannot reach it.");
    let acknowledged = tokio::time::timeout(wait::budget(SETTLES_WITHIN), publishing)
        .await
        .context("the publisher was still being answered long after the cut")?
        .context("the publisher task failed")?;
    println!(
        "  The publisher stopped at its first unanswered publish, with {} acknowledged.",
        acknowledged.len()
    );
    beat(pace).await;

    step(&format!("{level}: killing {leader}"));
    cluster.kill_node(&leader)?;
    cluster.heal_partitions()?;
    let elected = cluster
        .promoted_leader(STREAM, &leader, SETTLES_WITHIN)
        .await?;
    println!("  Gone. The two brokers left are a majority, and elected {elected}.");
    // Proof the new leader serves before its log is read back.
    let marker = format!("{level}-after-failover");
    wait::until(
        SETTLES_WITHIN,
        "the new leader to take a publish",
        || async {
            cluster
                .publish_via(&elected, STREAM, marker.clone().into_bytes())
                .await
                .is_ok()
        },
    )
    .await?;
    beat(pace).await;

    step(&format!("{level}: reading the stream back from {elected}"));
    // A replay on a broker that has just taken over can stop short of its
    // log's end (#3112), so read until the marker, the last record, shows.
    let held = wait::until_some(SETTLES_WITHIN, "a replay through the marker", || async {
        let held = replay(&cluster, &elected).await.ok()?;
        held.contains(&marker).then_some(held)
    })
    .await?;
    let missing: Vec<String> = acknowledged
        .iter()
        .filter(|payload| !held.contains(payload))
        .cloned()
        .collect();
    match (missing.first(), missing.last()) {
        (Some(first), Some(last)) => {
            println!(
                "  \x1b[1m{} of {} acknowledged records are gone\x1b[0m: {first} to {last}.",
                missing.len(),
                acknowledged.len()
            );
            println!("  {leader} answered them alone, and they were only ever on its disk.");
        }
        _ => println!(
            "  All {} acknowledged records are here.",
            acknowledged.len()
        ),
    }
    beat(pace).await;
    cluster.shutdown().await;
    Ok(Outcome {
        acknowledged,
        missing,
    })
}

/// Every payload `node_id` serves from the start of the stream.
async fn replay(cluster: &Cluster, node_id: &str) -> Result<Vec<String>> {
    let (_client, mut subscription) = cluster.replay_on(node_id, STREAM).await?;
    let mut held = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        held.push(String::from_utf8_lossy(&event.payload).into_owned());
    }
    Ok(held)
}
