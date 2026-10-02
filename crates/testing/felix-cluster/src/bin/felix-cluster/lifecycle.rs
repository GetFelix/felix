//! Commands that start a cluster in this process: `up`, `status` and `smoke`.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_cluster::session;
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};

use crate::args::flag_usize;
use crate::signals::stop_signal;
use crate::{STREAM, init_tracing};

pub(crate) async fn up(args: &[String]) -> Result<()> {
    init_tracing(true);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    eprintln!("starting a {nodes}-node cluster...");
    let cluster = Cluster::start(cluster_config(nodes, true)).await?;

    let path = session::default_path();
    if let Some(existing) = session::live_session(&path).await {
        eprintln!(
            "\nwarning: a cluster is already running at {}.\n\
             \x20        it keeps its ports and processes, but `subscribe` and `publish`\n\
             \x20        will now talk to this one instead. stop it with Ctrl-C in its\n\
             \x20        own window.",
            existing.control_plane,
        );
    }
    let session = cluster.session();
    session.write(&path)?;

    eprintln!("\ncluster up.\n");
    print_topology(&cluster).await?;
    println!("\nsession   {}", path.display());
    println!("\n  felix-cluster subscribe        # in another window");
    println!("  felix-cluster publish hello    # in a third");
    eprintln!("\nholding the cluster. press Ctrl-C to tear it down.");

    stop_signal().await?;
    eprintln!("\ntearing down...");
    // Only if it still describes this cluster: a second cluster may have taken
    // the file over, and deleting that one leaves it running and unreachable.
    session.remove_if_ours(&path);
    cluster.shutdown().await;
    Ok(())
}

pub(crate) async fn status(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let cluster = Cluster::start(cluster_config(nodes, false)).await?;
    print_topology(&cluster).await?;
    cluster.shutdown().await;
    Ok(())
}

pub(crate) async fn smoke_command(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let mut cluster = Cluster::start(cluster_config(nodes, false)).await?;
    print_topology(&cluster).await?;
    let result = smoke(&mut cluster).await;
    cluster.shutdown().await;
    result
}

pub(crate) fn cluster_config(nodes: usize, inherit_output: bool) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![StreamSpec::new(STREAM, 1)],
        // Something for `felixctl cache` to try out.
        caches: vec![CacheSpec::new("users", 1)],
        inherit_output: inherit_output && std::env::var("FELIX_CLUSTER_VERBOSE").is_ok(),
        ..Default::default()
    }
}

async fn print_topology(cluster: &Cluster) -> Result<()> {
    println!("control plane   {}", cluster.control_plane_url());
    println!();
    println!("{:<10} {:<22} metrics", "node", "client");
    for node in &cluster.nodes {
        println!(
            "{:<10} {:<22} {}",
            node.node_id,
            node.client_addr.to_string(),
            node.metrics_addr,
        );
    }
    println!();
    println!("placeable: {}", cluster.placeable_nodes().await?.join(", "));
    println!("shard ownership:");
    let mut owners: Vec<_> = cluster.shard_owners().await?.into_iter().collect();
    owners.sort();
    for (shard, leader) in owners {
        println!("  {shard} -> {leader}");
    }
    Ok(())
}

/// The scenario the harness exists to make easy: a publish that arrives at a
/// broker which does not own the shard still reaches a subscriber on the one
/// that does.
async fn smoke(cluster: &mut Cluster) -> Result<()> {
    let (owner, non_owner) = cluster.owner_and_non_owner(STREAM).await?;
    println!("\nsmoke: publishing via {non_owner}, subscribing on {owner}");

    let (_client, mut subscription) = cluster.subscribe_on(&owner, STREAM).await?;
    let payload = b"cross-broker".to_vec();
    cluster
        .publish_via(&non_owner, STREAM, payload.clone())
        .await?;

    let forwarded = cluster
        .metric(&non_owner, "felix_broker_forwards_total")
        .await?;
    match forwarded {
        Some(count) if count > 0.0 => println!("smoke: {non_owner} forwarded {count} publish(es)"),
        _ => bail!(
            "{non_owner} did not forward anything: the publish was served locally by a broker \
             that does not own the shard"
        ),
    }

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .context("timed out waiting for the forwarded record")?
        .context("subscription failed")?
        .context("subscription closed before the record arrived")?;

    if event.payload != payload {
        bail!(
            "expected {:?}, received {:?}",
            String::from_utf8_lossy(&payload),
            String::from_utf8_lossy(&event.payload),
        );
    }
    println!("smoke: ok — the record crossed the node boundary");
    Ok(())
}
