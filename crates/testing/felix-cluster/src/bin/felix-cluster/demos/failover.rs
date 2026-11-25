//! `failover`: replication, quorum acknowledgement and failover, end to end.

use std::time::Duration;

use anyhow::{Result, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};

use super::replay::{self, Record};
use super::{beat, step};
use crate::args::{flag_usize, pace_from};
use crate::{STREAM, init_tracing};

/// How long a healthy cluster may take to elect a leader or catch a replica up.
const SETTLES_WITHIN: Duration = Duration::from_secs(20);

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
    // The broker that serves the shard. On Raft that is the member the group
    // elected, which placement names but does not choose.
    let leader = cluster.serving_leader(STREAM, SETTLES_WITHIN).await?;
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
    let mut before = Vec::new();
    for index in 1..=3u32 {
        let payload = format!("before-{index}");
        let offset = client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.clone().into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: {payload}   (a majority holds it, not just the leader)");
        before.push(Record::new(offset, payload));
    }
    cluster
        .wait_for_every_member_level(STREAM, SETTLES_WITHIN)
        .await?;
    println!("\n  Replication has caught up: {leader} is not the only broker holding them.");
    beat(pace).await;

    step(&format!(
        "Killing {leader}, the broker that acknowledged them"
    ));
    let killed_at = std::time::Instant::now();
    cluster.kill_node(&leader)?;
    println!("  Gone. Not a graceful shutdown — the process is dead.");
    let promoted = cluster
        .promoted_leader(STREAM, &leader, Duration::from_secs(30))
        .await?;
    println!(
        "  {promoted} took over after {:?}.",
        Duration::from_millis(killed_at.elapsed().as_millis() as u64)
    );
    println!("  It was chosen because it holds the log — a broker that did not");
    println!("  would serve an empty shard, and the failover would be the data loss.");
    beat(pace).await;

    step("The same client, still publishing");
    let mut last = None;
    for index in 1..=2u32 {
        let payload = format!("after-{index}");
        let offset = client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.clone().into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: {payload}");
        last = Some(Record::new(offset, payload));
    }
    let last = last.expect("two records were just published");
    println!("\n  It reconnected to a surviving broker on its own. Nothing was rebuilt.");
    beat(pace).await;

    step("Reading the whole stream back from the broker that took over");
    // Read through the newest acknowledged record, so a replay that stops
    // short (#3112) is retried and named, not reported as lost records.
    let replayed = replay::through(&cluster, &promoted, STREAM, &last, SETTLES_WITHIN).await?;
    for record in &replayed {
        let offset = record
            .offset
            .map(|o| o.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("      ← offset {offset:>4}  {}", record.payload);
    }
    let seen: Vec<String> = replayed.iter().map(|r| r.payload.clone()).collect();

    // The replay ran past every one of these, so one it does not hold is gone.
    let lost = replay::missing(&before, &replayed);
    println!();
    if let Some(first) = lost.first() {
        bail!(
            "{} record(s) acknowledged under Quorum did not survive the failover, the first {}",
            lost.len(),
            first.place()
        );
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
