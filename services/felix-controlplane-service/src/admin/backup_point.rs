//! `felix-controlplane admin backup-point`: one committed offset per shard
//! log, across every broker, written to a manifest a restore cuts back to.
//!
//! The order is the argument. The barrier instant is taken first; every
//! offset is read after it, from the shard's leader, as that leader's
//! committed offset. So anything acknowledged before the barrier is below
//! the offset recorded for its shard, and nothing past a committed offset is
//! in the point. The assignments are read again at the end: offsets from a
//! leader that has since been replaced are not a stable basis, so a change
//! starts the collection over.
//!
//! Brokers do not agree on an instant, so a record written *during* the
//! collection can be in the point on one shard and not on another. See the
//! runbook, `docs-site/src/content/docs/deployment/backup-and-restore.md`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::{Admin, table};

/// The manifest layout this writes. A restore refuses any other.
pub const FORMAT_VERSION: u32 = 1;

/// The broker's default metrics listener port, where `/backup/offsets` is.
pub const DEFAULT_METRICS_PORT: u16 = 8080;

/// How many times the whole collection is started over because a leader or
/// generation changed under it.
const COLLECTION_ATTEMPTS: usize = 5;

/// How many times one broker is asked again for shards it had no committed
/// answer for, and the pause before the first retry (doubling after).
const BROKER_ATTEMPTS: u32 = 5;
const BROKER_BACKOFF: Duration = Duration::from_millis(200);

const USAGE: &str = "\
usage: felix-controlplane admin backup-point <name> [--out FILE]
         [--broker NODE=URL]... [--metrics-port PORT]

--out defaults to <name>.backup-point.json. Each broker's committed offsets
are read from http://<its client or internal host>:<metrics port>/backup/offsets
unless --broker names the URL for it. --metrics-port defaults to 8080.";

/// What to take a backup point of, and how to reach the brokers.
#[derive(Debug, Clone)]
pub struct BackupPoint {
    pub control_plane_url: String,
    pub token: Option<String>,
    pub name: String,
    /// Base URL of a broker's metrics listener, by node id, where the address
    /// the control plane has for it is not the one to use.
    pub brokers: HashMap<String, String>,
    pub metrics_port: u16,
}

impl BackupPoint {
    /// Collect the point. Fails rather than returning a partial one: a
    /// point missing a shard would restore that shard to nothing.
    pub async fn take(&self) -> Result<Manifest> {
        // First, before any read: the barrier every offset is read after.
        let taken_at_millis = now_millis();
        let admin = Admin {
            http: super::http_client()?,
            url: self.control_plane_url.trim_end_matches('/').to_string(),
            token: self.token.clone(),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build the HTTP client")?;
        for _ in 0..COLLECTION_ATTEMPTS {
            let snapshot = assignment_snapshot(&admin).await?;
            let nodes = node_addresses(&admin).await?;
            let mut shards = Vec::new();
            let mut leaders: BTreeMap<&str, Vec<&Assignment>> = BTreeMap::new();
            for assignment in &snapshot.items {
                leaders
                    .entry(assignment.leader.as_str())
                    .or_default()
                    .push(assignment);
            }
            for (leader, assigned) in leaders {
                let url = broker_url(leader, &nodes, &self.brokers, self.metrics_port)?;
                shards.extend(collect_from(&http, leader, &url, &assigned).await?);
            }
            let after = assignment_snapshot(&admin).await?;
            if leadership_changed(&snapshot.items, &after.items) {
                continue;
            }
            shards.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
            return Ok(Manifest {
                format_version: FORMAT_VERSION,
                name: self.name.clone(),
                taken_at_millis,
                metadata_version: snapshot.next_seq,
                shards,
            });
        }
        bail!(
            "shard leadership kept changing while offsets were read ({COLLECTION_ATTEMPTS} \
             attempts); try again once placement is quiet"
        )
    }
}

/// A backup point: per shard, the committed offset of each of its logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub name: String,
    /// The barrier, in UTC milliseconds. Everything acknowledged before it is
    /// in the point.
    pub taken_at_millis: u64,
    /// The assignment change sequence the point was read against. Restore
    /// control-plane metadata from a copy taken at or after it.
    pub metadata_version: u64,
    pub shards: Vec<PointShard>,
}

/// One shard in a backup point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointShard {
    pub tenant_id: String,
    pub namespace: String,
    pub name: String,
    pub shard: u32,
    /// `stream` or `cache`.
    pub kind: String,
    /// The broker whose copy of the shard the point describes.
    pub leader: String,
    pub generation: u64,
    pub logs: PointLogs,
}

impl PointShard {
    fn sort_key(&self) -> (&str, &str, &str, &str, u32) {
        (
            &self.tenant_id,
            &self.namespace,
            &self.kind,
            &self.name,
            self.shard,
        )
    }

    fn label(&self) -> String {
        let name = format!(
            "{}/{}/{}/{}",
            self.tenant_id, self.namespace, self.name, self.shard
        );
        if self.kind == "stream" {
            name
        } else {
            format!("{name} ({})", self.kind)
        }
    }
}

/// One past the last committed record of each of a shard's logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointLogs {
    pub records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_cursors: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_dead_letters: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counters: Option<u64>,
}

/// Run `backup-point`; `args` starts after the word `backup-point`.
pub(super) async fn run(admin: &Admin, args: Vec<String>) -> Result<()> {
    let mut name = None;
    let mut out = None;
    let mut brokers = HashMap::new();
    let mut metrics_port = DEFAULT_METRICS_PORT;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = Some(PathBuf::from(args.next().context("--out needs a file")?)),
            "--broker" => {
                let value = args.next().context("--broker needs NODE=URL")?;
                let Some((node, url)) = value.split_once('=') else {
                    bail!("--broker takes NODE=URL, not {value:?}");
                };
                brokers.insert(node.to_string(), url.trim_end_matches('/').to_string());
            }
            "--metrics-port" => {
                metrics_port = args
                    .next()
                    .context("--metrics-port needs a port")?
                    .parse()
                    .context("--metrics-port")?;
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            flag if flag.starts_with("--") => bail!("unknown option {flag}\n\n{USAGE}"),
            _ if name.is_none() => name = Some(arg),
            _ => bail!("{USAGE}"),
        }
    }
    let Some(name) = name else {
        bail!("{USAGE}");
    };
    let point = BackupPoint {
        control_plane_url: admin.url.clone(),
        token: admin.token.clone(),
        name: name.clone(),
        brokers,
        metrics_port,
    };
    let manifest = point.take().await?;
    let out = out.unwrap_or_else(|| PathBuf::from(format!("{name}.backup-point.json")));
    std::fs::write(&out, serde_json::to_string_pretty(&manifest)?)
        .with_context(|| format!("write {}", out.display()))?;
    print!("{}", render_summary(&manifest));
    println!("wrote {}", out.display());
    Ok(())
}

/// An assignment as the snapshot lists it; only what a point needs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct Assignment {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    #[serde(default = "stream_kind")]
    kind: String,
    leader: String,
    generation: u64,
}

impl Assignment {
    fn key(&self) -> (&str, &str, &str, &str, u32) {
        (
            &self.tenant_id,
            &self.namespace,
            &self.kind,
            &self.stream,
            self.shard,
        )
    }
}

fn stream_kind() -> String {
    "stream".to_string()
}

#[derive(Debug, Deserialize)]
struct AssignmentSnapshot {
    items: Vec<Assignment>,
    next_seq: u64,
}

async fn assignment_snapshot(admin: &Admin) -> Result<AssignmentSnapshot> {
    serde_json::from_value(admin.get("/v1/shard-assignments/snapshot").await?)
        .context("read the shard assignment snapshot")
}

/// Where a node can be reached: its client address, else its internal one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeAddress {
    node_id: String,
    client_addr: Option<String>,
    advertise_addr: String,
}

async fn node_addresses(admin: &Admin) -> Result<Vec<NodeAddress>> {
    #[derive(Deserialize)]
    struct Page {
        items: Vec<Item>,
        #[serde(default)]
        next_cursor: Option<String>,
    }
    #[derive(Deserialize)]
    struct Item {
        node: Node,
    }
    #[derive(Deserialize)]
    struct Node {
        node_id: String,
        spec: Spec,
    }
    #[derive(Deserialize)]
    struct Spec {
        advertise_addr: String,
        #[serde(default)]
        client_addr: Option<String>,
    }
    let mut nodes = Vec::new();
    let mut path = "/v1/nodes".to_string();
    loop {
        let page: Page =
            serde_json::from_value(admin.get(&path).await?).context("read the node list")?;
        nodes.extend(page.items.into_iter().map(|item| NodeAddress {
            node_id: item.node.node_id,
            client_addr: item.node.spec.client_addr,
            advertise_addr: item.node.spec.advertise_addr,
        }));
        match page.next_cursor {
            Some(cursor) => path = format!("/v1/nodes?cursor={cursor}"),
            None => return Ok(nodes),
        }
    }
}

/// The base URL of `node`'s metrics listener.
fn broker_url(
    node: &str,
    nodes: &[NodeAddress],
    overrides: &HashMap<String, String>,
    metrics_port: u16,
) -> Result<String> {
    if let Some(url) = overrides.get(node) {
        return Ok(url.clone());
    }
    let Some(address) = nodes.iter().find(|n| n.node_id == node) else {
        bail!("{node} leads shards but is not a registered node; name its URL with --broker");
    };
    let addr = address
        .client_addr
        .as_deref()
        .unwrap_or(&address.advertise_addr);
    Ok(format!("http://{}:{metrics_port}", host_of(addr)))
}

/// The host of a `host:port`, keeping an IPv6 literal's brackets.
fn host_of(addr: &str) -> &str {
    addr.rsplit_once(':').map_or(addr, |(host, _)| host)
}

/// `GET /backup/offsets` as a broker answers it.
#[derive(Debug, Deserialize)]
struct BrokerOffsets {
    #[serde(default)]
    shards: Vec<BrokerShard>,
    #[serde(default)]
    skipped: Vec<BrokerSkipped>,
}

#[derive(Debug, Deserialize)]
struct BrokerShard {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: String,
    generation: u64,
    logs: PointLogs,
}

#[derive(Debug, Deserialize)]
struct BrokerSkipped {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: String,
    reason: String,
}

/// Ask `leader` until it has an answer for every shard assigned to it.
async fn collect_from(
    http: &reqwest::Client,
    leader: &str,
    url: &str,
    assigned: &[&Assignment],
) -> Result<Vec<PointShard>> {
    let mut backoff = BROKER_BACKOFF;
    let mut last = String::new();
    for attempt in 1..=BROKER_ATTEMPTS {
        match fetch_offsets(http, url).await {
            Ok(offsets) => match match_leader(leader, assigned, &offsets) {
                Ok(shards) => return Ok(shards),
                Err(missing) => last = missing.join(", "),
            },
            Err(err) => last = format!("{err:#}"),
        }
        if attempt < BROKER_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    bail!("{leader} ({url}) had no committed offsets to give for: {last}")
}

async fn fetch_offsets(http: &reqwest::Client, url: &str) -> Result<BrokerOffsets> {
    let response = http
        .get(format!("{url}/backup/offsets"))
        .send()
        .await
        .with_context(|| format!("reach {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url}/backup/offsets"))?;
    response
        .json()
        .await
        .with_context(|| format!("read {url}/backup/offsets"))
}

/// The point's entry for each shard `leader` was assigned, from its answer,
/// or the shards it could not vouch for at the assigned generation.
///
/// An in-memory stream has nothing on disk to restore, so it is left out of
/// the point rather than counted as missing.
fn match_leader(
    leader: &str,
    assigned: &[&Assignment],
    offsets: &BrokerOffsets,
) -> Result<Vec<PointShard>, Vec<String>> {
    let mut shards = Vec::new();
    let mut missing = Vec::new();
    for assignment in assigned {
        let same = |tenant: &str, namespace: &str, kind: &str, name: &str, shard: u32| {
            (tenant, namespace, kind, name, shard) == assignment.key()
        };
        let found = offsets
            .shards
            .iter()
            .find(|s| same(&s.tenant_id, &s.namespace, &s.kind, &s.name, s.shard));
        let skipped = offsets
            .skipped
            .iter()
            .find(|s| same(&s.tenant_id, &s.namespace, &s.kind, &s.name, s.shard));
        let label = format!(
            "{}/{}/{}/{} ({})",
            assignment.tenant_id,
            assignment.namespace,
            assignment.stream,
            assignment.shard,
            assignment.kind
        );
        match (found, skipped) {
            (Some(found), _) if found.generation == assignment.generation => {
                shards.push(PointShard {
                    tenant_id: assignment.tenant_id.clone(),
                    namespace: assignment.namespace.clone(),
                    name: assignment.stream.clone(),
                    shard: assignment.shard,
                    kind: assignment.kind.clone(),
                    leader: leader.to_string(),
                    generation: assignment.generation,
                    logs: found.logs,
                });
            }
            (Some(found), _) => missing.push(format!(
                "{label} at generation {} rather than {}",
                found.generation, assignment.generation
            )),
            (None, Some(skipped)) if skipped.reason == "not_durable" => {}
            (None, Some(skipped)) => missing.push(format!("{label}: {}", skipped.reason)),
            (None, None) => missing.push(format!("{label}: not led there")),
        }
    }
    if missing.is_empty() {
        Ok(shards)
    } else {
        Err(missing)
    }
}

/// Whether any shard's leader or generation differs between two reads, or
/// a shard came or went.
fn leadership_changed(before: &[Assignment], after: &[Assignment]) -> bool {
    type Key<'a> = (&'a str, &'a str, &'a str, &'a str, u32);
    fn index(items: &[Assignment]) -> HashMap<Key<'_>, (&str, u64)> {
        items
            .iter()
            .map(|a| (a.key(), (a.leader.as_str(), a.generation)))
            .collect()
    }
    index(before) != index(after)
}

fn render_summary(manifest: &Manifest) -> String {
    let optional = |value: Option<u64>| value.map_or("-".to_string(), |v| v.to_string());
    let rows = manifest
        .shards
        .iter()
        .map(|shard| {
            vec![
                shard.label(),
                shard.leader.clone(),
                shard.generation.to_string(),
                shard.logs.records.to_string(),
                optional(shard.logs.group_cursors),
                optional(shard.logs.group_dead_letters),
                optional(shard.logs.counters),
            ]
        })
        .collect();
    let mut out = format!(
        "backup point {:?} at {} ms, metadata version {}\n",
        manifest.name, manifest.taken_at_millis, manifest.metadata_version
    );
    out.push_str(&table(
        &[
            "SHARD",
            "LEADER",
            "GENERATION",
            "RECORDS",
            "CURSORS",
            "DEAD_LETTERS",
            "COUNTERS",
        ],
        rows,
    ));
    out
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests;
