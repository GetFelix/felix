//! `failover`: replication, quorum acknowledgement and failover, end to end.

use std::time::Duration;

use anyhow::{Result, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};

use super::{beat, step};
use crate::args::{flag_usize, pace_from};
use crate::{STREAM, init_tracing};

/// Replication, quorum acknowledgement and failover, end to end.
///
/// The point it is making: a record the cluster acknowledged under `Quorum` is
/// still there after the broker that acknowledged it is killed, and a client
/// given more than one way in carries on without being rebuilt.
pub(crate) async fn failover(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let pace = pace_from(args)?;

    step("A three-node cluster, replicated three ways");
    println!("  Every shard is held by three brokers, and a publish is acknowledged");
    println!("  only once a majority of them has it durably.");
    let mut cluster = Cluster::start(ClusterConfig {
        nodes,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await?;
    let leader = cluster.owner(STREAM).await?;
    for node in &cluster.nodes {
        let role = if node.node_id == leader {
            "leader"
        } else {
            "replica"
        };
        println!("  {:<10} {:<22} {role}", node.node_id, node.client_addr);
    }
    beat(pace).await;

    step("A client that knows the cluster, not one broker");
    // One address on purpose. Handing it all three would hide the thing worth
    // showing: an application is configured with one endpoint far more often
    // than with a correct list of every broker.
    let seed = cluster.broker_addrs()[0];
    let client = felix_cluster::client::connect_cluster(
        &[seed],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await?;
    println!("  Configured with exactly one address: {seed}");
    let known = client.endpoints().await;
    println!(
        "  It asked that broker who else was there, and now knows {}:",
        plural(known.len(), "broker", "brokers"),
    );
    for addr in &known {
        println!("      {addr}");
    }
    println!("  The address it was given stays in that list. A cluster that");
    println!("  answers wrongly can never leave a client worse off than before.");
    beat(pace).await;

    step("Publishing under Quorum");
    for index in 1..=3u32 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("before-{index}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: before-{index}   (a majority holds it, not just the leader)");
    }
    wait::until(Duration::from_secs(20), "replication to settle", || async {
        matches!(
            cluster
                .metric(&leader, "felix_broker_replication_lag_records")
                .await,
            Ok(Some(lag)) if lag == 0.0
        )
    })
    .await?;
    println!("\n  Replication lag on {leader}: 0 — every replica holds everything it does.");
    beat(pace).await;

    step(&format!(
        "Killing {leader}, the broker that acknowledged them"
    ));
    let killed_at = std::time::Instant::now();
    cluster.kill_node(&leader)?;
    println!("  Gone. Not a graceful shutdown — the process is dead.");
    wait::until(
        Duration::from_secs(30),
        "a replica to be promoted",
        || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
        },
    )
    .await?;
    let promoted = cluster.owner(STREAM).await?;
    println!(
        "  {promoted} promoted after {:?}.",
        Duration::from_millis(killed_at.elapsed().as_millis() as u64)
    );
    println!("  It was chosen because it holds the log — a broker that did not");
    println!("  would serve an empty shard, and the failover would be the data loss.");
    beat(pace).await;

    step("The same client, still publishing");
    for index in 1..=2u32 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("after-{index}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: after-{index}");
    }
    println!("\n  It reconnected to a surviving broker on its own. Nothing was rebuilt.");
    beat(pace).await;

    step("Reading the whole stream back from the broker that took over");
    let (_client, mut subscription) = cluster.replay_on(&promoted, STREAM).await?;
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).to_string();
        let offset = event
            .offset
            .map(|o| o.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("      ← offset {offset:>4}  {payload}");
        seen.push(payload);
    }

    let survived = (1..=3).all(|i| seen.iter().any(|p| p == &format!("before-{i}")));
    println!();
    if !survived {
        bail!("a record acknowledged under Quorum did not survive the failover");
    }
    println!("  Every record acknowledged before the kill is still here, read from a");
    println!("  broker that was a replica a moment ago.");

    // Two things in that listing look like bugs and are not, so say what they
    // are rather than leaving them to be noticed.
    let probes = seen
        .iter()
        .filter(|p| p.as_str() == "harness-probe")
        .count();
    if probes > 1 {
        println!(
            "\n  ({probes} \"harness-probe\" records: the harness publishes one per broker at"
        );
        println!("  startup to prove the cluster can serve before the demo begins.)");
    }
    let duplicated: Vec<&String> = seen
        .iter()
        .filter(|p| seen.iter().filter(|other| other == p).count() > 1)
        .filter(|p| p.starts_with("after-"))
        .collect();
    if !duplicated.is_empty() {
        println!(
            "\n  ({} appears twice. That is `publish_at_least_once` doing what its",
            duplicated[0]
        );
        println!("  name says: the publish failed after the broker had already written the");
        println!("  record, and sending it again is the only way not to lose it. Only the");
        println!("  application holds an identity that could tell the two apart.)");
    }

    cluster.shutdown().await;
    Ok(())
}

/// "1 broker" rather than "1 brokers", which reads as a bug in the demo.
fn plural(count: usize, one: &str, many: &str) -> String {
    if count == 1 {
        format!("{count} {one}")
    } else {
        format!("{count} {many}")
    }
}
