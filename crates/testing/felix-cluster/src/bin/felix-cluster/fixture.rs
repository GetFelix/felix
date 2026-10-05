//! `client-fixture`: a cluster for a client conformance suite to run against.

mod control;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use felix_conformance::link::Interposer;

use crate::args::{flag_usize, flag_value};
use crate::init_tracing;
use crate::signals::stop_signal;

/// A cluster for a client conformance suite, in any language, to run against.
///
/// The suite needs more than an address: a durable stream to observe offsets
/// on, a cache, a credential that may publish and one that may not, a stream
/// name that is deliberately absent, and the broker's certificate so a client
/// can verify properly instead of skipping verification. This starts all of
/// that and writes it where the suite can read it, along with the URL of a
/// small control endpoint (see `control`) for the scenarios that need a fault.
///
/// Three nodes by default, because the semantics that matter most to a client
/// only exist in a cluster: a redirect needs a broker that does not own the
/// shard, and reconnection needs somewhere to reconnect *to*. Each broker
/// exports its own certificate and they are concatenated into one PEM bundle,
/// which is what a trust store is allowed to be.
pub(crate) async fn client_fixture(args: &[String]) -> Result<()> {
    init_tracing(false);
    let node_count = flag_usize(args, "--nodes")?.unwrap_or(3);
    let out = flag_value(args, "--out")?.unwrap_or_else(|| {
        std::env::temp_dir()
            .join("felix-client-fixture.json")
            .display()
            .to_string()
    });
    let ca_file = flag_value(args, "--ca-file")?.unwrap_or_else(|| {
        std::env::temp_dir()
            .join("felix-client-fixture-ca.pem")
            .display()
            .to_string()
    });

    const DURABLE_STREAM: &str = "conformance";
    const CACHE: &str = "conformance";
    // A prefix watch reads one shard, so prefix and retained scenarios need a
    // cache whose keys cannot be spread across several.
    const SINGLE_SHARD_CACHE: &str = "conformance-single";
    // Never registered. A client proving it reports "unknown stream"
    // distinguishably needs a name the broker will genuinely refuse.
    const MISSING_STREAM: &str = "conformance-absent";
    // For the faults a suite asks the control endpoint for: one to fence
    // mid-move, one whose writes need a majority.
    const MOVABLE_STREAM: &str = "conformance-movable";
    const QUORUM_STREAM: &str = "conformance-quorum";
    // One shard, so its owner is one broker the link interposer can sit in
    // front of.
    const LINK_STREAM: &str = "conformance-link";

    eprintln!("starting a {node_count}-node cluster for a client conformance suite...");
    let cluster = Cluster::start(ClusterConfig {
        nodes: node_count,
        // Several shards so the stream is spread across brokers: a redirect
        // scenario needs a shard whose owner is not the broker the client
        // reached, and one shard on one node cannot produce that.
        //
        // Replicated across the whole cluster because the reconnect scenario
        // asks whether the *client* survives losing a broker — and with one
        // copy per shard the answer would be confounded by the shard not
        // surviving either. A client cannot reconnect its way to data that is
        // gone.
        streams: vec![
            StreamSpec::replicated(DURABLE_STREAM, 4, node_count as u32),
            StreamSpec::replicated(MOVABLE_STREAM, 1, node_count as u32),
            StreamSpec::quorum(QUORUM_STREAM, 1, node_count as u32),
            StreamSpec::replicated(LINK_STREAM, 1, node_count as u32),
        ],
        caches: vec![
            CacheSpec::replicated(CACHE, 4, node_count as u32),
            CacheSpec::replicated(SINGLE_SHARD_CACHE, 1, node_count as u32),
        ],
        inherit_output: std::env::var("FELIX_CLUSTER_VERBOSE").is_ok(),
        ..Default::default()
    })
    .await?;
    // Commits are refused until the fleet finalizes the feature, as in any
    // real cluster.
    cluster
        .finalize_fleet_features(&["atomic_commit"], Duration::from_secs(30))
        .await?;

    // Each broker wrote its own certificate; a client needs to trust all of
    // them, and a PEM bundle holding several is exactly how that is spelled.
    let mut bundle = String::new();
    for node in &cluster.nodes {
        let path = node.data_dir.join("broker-cert.pem");
        let pem = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "read the certificate {} exported to {}",
                node.node_id,
                path.display()
            )
        })?;
        bundle.push_str(&pem);
        if !bundle.ends_with('\n') {
            bundle.push('\n');
        }
    }
    std::fs::write(&ca_file, &bundle)
        .with_context(|| format!("write the certificate bundle to {ca_file}"))?;

    let link_owner = cluster.owner(LINK_STREAM).await?;
    let link_upstream = cluster
        .node(&link_owner)
        .with_context(|| format!("no node {link_owner}"))?
        .client_addr;
    let link = Arc::new(Interposer::start(link_upstream).await?);

    let cluster = Arc::new(cluster);
    let hold = Arc::new(AtomicBool::new(false));
    let (control_url, control_task) = control::Control::new(
        Arc::clone(&cluster),
        Arc::clone(&hold),
        Arc::clone(&link),
        MOVABLE_STREAM,
        QUORUM_STREAM,
    )
    .serve()
    .await?;

    let fixture = felix_conformance::kit::Fixture {
        addrs: cluster
            .nodes
            .iter()
            .map(|node| node.client_addr.to_string())
            .collect(),
        tenant_id: cluster.tenant_id.clone(),
        namespace: cluster.namespace.clone(),
        token: cluster.client_token(),
        unauthorized_token: cluster.subscribe_only_token(),
        ca_file: ca_file.clone(),
        durable_stream: DURABLE_STREAM.to_string(),
        cache: CACHE.to_string(),
        single_shard_cache: SINGLE_SHARD_CACHE.to_string(),
        missing_stream: MISSING_STREAM.to_string(),
        movable_stream: Some(MOVABLE_STREAM.to_string()),
        quorum_stream: Some(QUORUM_STREAM.to_string()),
        control_url: Some(control_url.clone()),
        link_addr: Some(link.addr().to_string()),
        link_stream: Some(LINK_STREAM.to_string()),
        faults: felix_conformance::kit::catalogue()?.fault_scenarios(),
    };
    let body = serde_json::to_vec_pretty(&fixture).context("encode the fixture")?;
    std::fs::write(&out, body).with_context(|| format!("write the fixture to {out}"))?;

    println!("fixture   {out}");
    println!("broker    {}", fixture.addrs.join(", "));
    println!("ca        {ca_file}");
    println!("control   {control_url}");
    println!("link      {} -> {link_owner}", link.addr());
    eprintln!("\nholding the fixture. press Ctrl-C to tear it down.");

    // A placement pass on a timer, which is what makes the reconnect scenario
    // answerable from outside. When a broker dies its shards need a new
    // leader, and in this harness placement is driven rather than swept — the
    // Rust tests call `place_shards` themselves while they wait. A suite in
    // another language has no such handle, so the fixture does it here: an
    // unowned shard is reassigned within a second or so, exactly as a
    // control plane's own sweep would. Skipped while the control endpoint
    // holds a fault open.
    let placement = tokio::spawn({
        let held = Arc::clone(&cluster);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if !hold.load(Ordering::SeqCst) {
                    held.place_shards().await;
                }
            }
        }
    });

    stop_signal().await?;
    placement.abort();
    control_task.abort();
    let _ = control_task.await;
    let _ = placement.await;
    eprintln!("\ntearing down...");
    let _ = std::fs::remove_file(&out);
    drop(cluster);
    Ok(())
}
