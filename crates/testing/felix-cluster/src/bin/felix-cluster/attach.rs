//! Commands that talk to a cluster another `felix-cluster up` is holding,
//! found through its session file.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_cluster::session::{self, Session};

use crate::args::{flag, positionals};
use crate::{STREAM, init_tracing};

/// Who leads each shard, read from a running cluster.
pub(crate) async fn owners() -> Result<()> {
    let session = Session::read(&session::default_path())?;
    let owners = shard_owners(&session).await?;
    let mut rows: Vec<_> = owners.into_iter().collect();
    rows.sort();
    for (shard, leader) in rows {
        println!("{shard} -> {leader}");
    }
    Ok(())
}

/// Every broker in the cluster, one per line.
///
/// Distinct from `owners`, which lists shard leaders: a broker holding no shard
/// is still a broker you can publish through, and that is exactly the case the
/// demo is about.
pub(crate) fn nodes() -> Result<()> {
    let session = Session::read(&session::default_path())?;
    for node in &session.nodes {
        println!("{}", node.node_id);
    }
    Ok(())
}

/// Stream events until interrupted.
pub(crate) async fn subscribe(args: &[String]) -> Result<()> {
    init_tracing(false);
    let session = Session::read(&session::default_path())?;
    let stream = match positionals(args).first() {
        Some(stream) => stream.clone(),
        None => bail!("subscribe takes a stream: felix-cluster subscribe {STREAM}"),
    };

    // Defaults to the owner, because that is the only broker that serves a
    // subscription today -- a non-owner has no copy to read from. How a
    // subscribe is routed is in docs/subscribe-routing.md.
    let owner = owner_of(&session, &stream).await?;
    let node_id = flag(args, "--on")?.unwrap_or_else(|| owner.clone());
    let node = session
        .node(&node_id)
        .with_context(|| format!("unknown node {node_id}"))?;

    let role = if node_id == owner {
        "owner"
    } else {
        "NOT the owner"
    };
    println!("subscribing to {stream} on {node_id} ({role})");
    if node_id != owner {
        println!(
            "  note: {node_id} does not own this shard, so it has nothing to deliver.\n\
             \x20       see docs/subscribe-routing.md for how subscribes are routed."
        );
    }
    println!("waiting for events. Ctrl-C to stop.\n");

    let client =
        felix_cluster::client::connect(node.client_addr, &session.tenant_id, &session.client_token)
            .await?;
    let mut subscription = client
        .subscribe(&session.tenant_id, &session.namespace, &stream)
        .await
        .context("subscribe")?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nstopped.");
                return Ok(());
            }
            event = subscription.next_event() => {
                match event.context("subscription failed")? {
                    Some(event) => {
                        let offset = event
                            .offset
                            .map(|o| o.to_string())
                            .unwrap_or_else(|| "-".to_string());
                        println!(
                            "[{node_id}] offset {offset:>6}  {}",
                            String::from_utf8_lossy(&event.payload),
                        );
                    }
                    None => {
                        println!("\nsubscription closed by the broker.");
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Publish one message, and say whether it crossed a node boundary.
pub(crate) async fn publish(args: &[String]) -> Result<()> {
    init_tracing(false);
    let session = Session::read(&session::default_path())?;
    let words = positionals(args);
    let (stream, message) = match words.split_first() {
        // The rest is the message, so it can contain spaces without quoting --
        // `publish orders order placed` reads better on camera than escaping.
        Some((stream, rest)) if !rest.is_empty() => (stream.clone(), rest.join(" ")),
        _ => bail!(
            "publish takes a stream and a message: \
             felix-cluster publish {STREAM} \"order placed\""
        ),
    };

    let owner = owner_of(&session, &stream).await?;
    // Defaults to a broker that does *not* own the shard, because that is the
    // interesting path: it is the one a single-node broker cannot show.
    let node_id = match flag(args, "--via")? {
        Some(explicit) => explicit,
        None => session
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .find(|id| id != &owner)
            .unwrap_or_else(|| owner.clone()),
    };
    let node = session
        .node(&node_id)
        .with_context(|| format!("unknown node {node_id}"))?;

    let before = forwards(&session, &node_id).await;
    let client =
        felix_cluster::client::connect(node.client_addr, &session.tenant_id, &session.client_token)
            .await?;
    let publisher = client.publisher().await.context("open publisher")?;
    publisher
        .publish(
            &session.tenant_id,
            &session.namespace,
            &stream,
            message.clone().into_bytes(),
            felix_wire::AckMode::PerMessage,
        )
        .await
        .with_context(|| format!("publish via {node_id}"))?;
    let after = forwards(&session, &node_id).await;

    // The acknowledgement already proves it was written. What a cluster adds is
    // *where*, and the forward counter is how that is visible from outside.
    // The stream and the payload are named on both sides on purpose: a viewer
    // watching two terminals has nothing else linking what was published to what
    // arrived.
    if node_id == owner {
        println!(
            "published {message:?} to {stream} via {node_id} (the owner) — written locally, no hop"
        );
    } else if after > before {
        println!(
            "published {message:?} to {stream} via {node_id} → forwarded to {owner} → acknowledged"
        );
    } else {
        println!(
            "published {message:?} to {stream} via {node_id}, but it did not forward — \
             it served a shard owned by {owner} locally"
        );
    }
    Ok(())
}

async fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .no_proxy()
        .build()
        .expect("build HTTP client")
}

async fn shard_owners(session: &Session) -> Result<HashMap<String, String>> {
    #[derive(serde::Deserialize)]
    struct Response {
        items: Vec<Row>,
        #[serde(default)]
        next_cursor: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct Row {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        leader: String,
    }

    let base = format!("{}/v1/shard-assignments", session.control_plane);
    let mut url = base.clone();
    let mut rows = Vec::new();
    loop {
        let response = http()
            .await
            .get(&url)
            .bearer_auth(&session.admin_token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if !status.is_success() {
            bail!("GET {url}: {status}");
        }
        let page: Response = response.json().await.context("decode assignments")?;
        rows.extend(page.items);
        match page.next_cursor {
            Some(cursor) => url = format!("{base}?cursor={cursor}"),
            None => break,
        }
    }
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                format!(
                    "{}/{}/{}/{}",
                    row.tenant_id, row.namespace, row.stream, row.shard
                ),
                row.leader,
            )
        })
        .collect())
}

async fn owner_of(session: &Session, stream: &str) -> Result<String> {
    let key = format!("{}/{}/{}/0", session.tenant_id, session.namespace, stream);
    shard_owners(session)
        .await?
        .remove(&key)
        .with_context(|| format!("no owner for {key}"))
}

/// The broker's forward counter, or 0 if it has never forwarded.
async fn forwards(session: &Session, node_id: &str) -> f64 {
    let Some(node) = session.node(node_id) else {
        return 0.0;
    };
    let url = format!("http://{}/metrics", node.metrics_addr);
    let Ok(response) = http().await.get(&url).send().await else {
        return 0.0;
    };
    let Ok(body) = response.text().await else {
        return 0.0;
    };
    body.lines()
        .filter(|line| line.starts_with("felix_broker_forwards_total"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}
