//! `felixctl backup-point`: one committed offset per shard
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

use crate::cli::BackupPointArgs;
use crate::context::Settings;
use crate::controlplane::Api;
use crate::error::{Exit, MarkExit};
use crate::output::{Output, table};

/// The manifest layout this writes. A restore refuses any other.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// How many times the whole collection is started over because a leader or
/// generation changed under it.
const COLLECTION_ATTEMPTS: usize = 5;

/// How many times one broker is asked again for shards it had no committed
/// answer for, and the pause before the first retry (doubling after).
const BROKER_ATTEMPTS: u32 = 5;
const BROKER_BACKOFF: Duration = Duration::from_millis(200);

/// What to take a backup point of, and how to reach the brokers.
struct BackupPoint<'a> {
    api: &'a Api,
    name: &'a str,
    /// Base URL of a broker's metrics listener, by node id, where the address
    /// the control plane has for it is not the one to use.
    brokers: HashMap<String, String>,
    metrics_port: u16,
}

impl BackupPoint<'_> {
    /// Collect the point. Fails rather than returning a partial one: a
    /// point missing a shard would restore that shard to nothing.
    async fn take(&self) -> Result<Manifest> {
        // First, before any read: the barrier every offset is read after.
        let taken_at_millis = now_millis();
        let admin = self.api;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build the HTTP client")?;
        for _ in 0..COLLECTION_ATTEMPTS {
            let snapshot = assignment_snapshot(admin).await?;
            let nodes = node_addresses(admin).await?;
            // A Raft shard's leader is whichever member won its election, not
            // the broker placement names, so every broker is asked.
            let mut ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
            ids.extend(self.brokers.keys().map(String::as_str));
            ids.extend(snapshot.items.iter().map(|a| a.leader.as_str()));
            ids.sort_unstable();
            ids.dedup();
            let urls = ids
                .iter()
                .map(|id| {
                    Ok((
                        id.to_string(),
                        broker_url(id, &nodes, &self.brokers, self.metrics_port)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let (urls, http) = (&urls, &http);
            let round = move || async move {
                let mut answers = Vec::new();
                for (id, url) in urls {
                    answers.push((id.clone(), fetch_offsets(http, url).await));
                }
                answers
            };
            let mut shards = collect_rounds(round, &snapshot.items, BROKER_BACKOFF).await?;
            let after = assignment_snapshot(admin).await?;
            if leadership_changed(&snapshot.items, &after.items) {
                continue;
            }
            shards.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
            return Ok(Manifest {
                format_version: FORMAT_VERSION,
                name: self.name.to_string(),
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
pub(crate) struct Manifest {
    pub(crate) format_version: u32,
    pub(crate) name: String,
    /// The barrier, in UTC milliseconds. Everything acknowledged before it is
    /// in the point.
    pub(crate) taken_at_millis: u64,
    /// The assignment change sequence the point was read against. Restore
    /// control-plane metadata from a copy taken at or after it.
    pub(crate) metadata_version: u64,
    pub(crate) shards: Vec<PointShard>,
}

/// One shard in a backup point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PointShard {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) shard: u32,
    /// `stream` or `cache`.
    pub(crate) kind: String,
    /// The broker whose copy of the shard the point describes.
    pub(crate) leader: String,
    pub(crate) generation: u64,
    /// Which incarnation of the stream the leader's logs belong to, as it
    /// answered. Left out when 0, a stream from before incarnations.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) incarnation: u64,
    /// For a Raft shard, the term of the entry below the records offset.
    /// A restore refuses a copy whose entry there is of another term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) end_term: Option<u64>,
    pub(crate) logs: PointLogs,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
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
pub(crate) struct PointLogs {
    pub(crate) records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) group_cursors: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) group_dead_letters: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) counters: Option<u64>,
}

/// Run `backup-point`: collect the point and write the manifest.
pub(crate) async fn run(
    args: &BackupPointArgs,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let point = BackupPoint {
        api: &api,
        name: &args.name,
        brokers: args.broker.iter().cloned().collect(),
        metrics_port: args.metrics_port,
    };
    let manifest = point.take().await?;
    let file = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.backup-point.json", args.name)));
    std::fs::write(&file, serde_json::to_string_pretty(&manifest)?)
        .mark(Exit::Failure, format!("write {}", file.display()))?;
    if out.json {
        return out.json_value(&serde_json::json!({
            "manifest": manifest,
            "written": file.display().to_string(),
        }));
    }
    out.text(&render_summary(&manifest))?;
    out.text(&format!("wrote {}", file.display()))
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

async fn assignment_snapshot(admin: &Api) -> Result<AssignmentSnapshot> {
    serde_json::from_value(admin.get("/v1/shard-assignments/snapshot", &[]).await?)
        .context("read the shard assignment snapshot")
}

/// Where a node can be reached: its client address, else its internal one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeAddress {
    node_id: String,
    client_addr: Option<String>,
    advertise_addr: String,
}

async fn node_addresses(admin: &Api) -> Result<Vec<NodeAddress>> {
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
    let items = admin.list("/v1/nodes", &[]).await?;
    items
        .into_iter()
        .map(|item| {
            let item: Item = serde_json::from_value(item).context("read the node list")?;
            Ok(NodeAddress {
                node_id: item.node.node_id,
                client_addr: item.node.spec.client_addr,
                advertise_addr: item.node.spec.advertise_addr,
            })
        })
        .collect()
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
    /// Every Raft member the broker holds, with its term.
    #[serde(default)]
    raft_members: Vec<RaftMember>,
}

#[derive(Debug, Deserialize)]
struct RaftMember {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: String,
    term: u64,
    /// This member, as its group's membership names it. Absent from a
    /// broker that predates it, whose answer then counts toward no majority.
    #[serde(default)]
    member: Option<Voter>,
    /// The voter sets of the last membership in the member's log: one, or
    /// two while a change is joint.
    #[serde(default)]
    voters: Vec<Vec<Voter>>,
    /// `[term, index]` of the entry that membership is from.
    #[serde(default)]
    membership_at: Option<[u64; 2]>,
}

/// A group member: a broker and the copy of the shard it holds. A broker
/// that lost its copy comes back as another member, which is not a voter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Voter {
    broker: String,
    copy: u64,
}

#[derive(Debug, Deserialize)]
struct BrokerShard {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: String,
    generation: u64,
    /// Absent from a broker that predates incarnations.
    #[serde(default)]
    incarnation: u64,
    /// `raft` when the Raft engine runs the shard: `generation` is then the
    /// leader's term.
    #[serde(default)]
    engine: Option<String>,
    /// The term of the entry below `records`, from a Raft leader.
    #[serde(default)]
    end_term: Option<u64>,
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

type Key<'a> = (&'a str, &'a str, &'a str, &'a str, u32);

/// Every broker's answer in one round, or why it gave none.
type Round = Vec<(String, Result<BrokerOffsets>)>;

/// Ask every broker, round after round, until every assigned shard has an
/// entry. Which shards the Raft engine runs is decided again each round
/// from that round's answers: a shard no broker listed a Raft member of
/// yet would otherwise be sent to placement's leader for good, which never
/// leads it (#2995).
async fn collect_rounds<F, Fut>(
    mut round: F,
    assigned: &[Assignment],
    backoff: Duration,
) -> Result<Vec<PointShard>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Round>,
{
    let mut backoff = backoff;
    let mut last = String::new();
    for attempt in 1..=BROKER_ATTEMPTS {
        match match_round(assigned, &round().await) {
            Ok(shards) => return Ok(shards),
            Err(missing) => last = missing.join(", "),
        }
        if attempt < BROKER_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    bail!("no committed offsets to give for: {last}")
}

/// The point from one round of answers: each Raft shard from its leader at
/// the newest term, each other shard from placement's leader. A broker
/// that did not answer is left out: a Raft shard it leads has no leader
/// this round, and a hybrid shard it leads is missing.
fn match_round(assigned: &[Assignment], round: &Round) -> Result<Vec<PointShard>, Vec<String>> {
    let answers: Vec<(String, &BrokerOffsets)> = round
        .iter()
        .filter_map(|(id, answer)| answer.as_ref().ok().map(|offsets| (id.clone(), offsets)))
        .collect();
    let raft = raft_keys(assigned, &answers);
    let (on_raft, hybrid): (Vec<&Assignment>, Vec<&Assignment>) =
        assigned.iter().partition(|a| raft.contains(&a.key()));
    let mut missing = Vec::new();
    let mut shards = match_raft(&on_raft, &answers).unwrap_or_else(|gone| {
        missing.extend(gone);
        Vec::new()
    });
    let mut leaders: BTreeMap<&str, Vec<&Assignment>> = BTreeMap::new();
    for assignment in hybrid {
        leaders
            .entry(assignment.leader.as_str())
            .or_default()
            .push(assignment);
    }
    for (leader, assigned) in leaders {
        match round
            .iter()
            .find(|(id, _)| id == leader)
            .map(|(_, answer)| answer)
        {
            Some(Ok(offsets)) => match match_leader(leader, &assigned, offsets) {
                Ok(found) => shards.extend(found),
                Err(gone) => missing.extend(gone.into_iter().map(|m| format!("{m} on {leader}"))),
            },
            Some(Err(err)) => missing.push(format!("{leader}: {err:#}")),
            None => missing.push(format!("{leader}: not asked")),
        }
    }
    if missing.is_empty() {
        Ok(shards)
    } else {
        Err(missing)
    }
}

/// The assignments some broker holds a Raft member of.
fn raft_keys<'a>(
    assigned: &'a [Assignment],
    answers: &[(String, &BrokerOffsets)],
) -> std::collections::HashSet<Key<'a>> {
    assigned
        .iter()
        .map(Assignment::key)
        .filter(|key| {
            answers.iter().any(|(_, offsets)| {
                offsets
                    .raft_members
                    .iter()
                    .any(|m| (&*m.tenant_id, &*m.namespace, &*m.kind, &*m.name, m.shard) == *key)
            })
        })
        .collect()
}

/// Each Raft shard's entry, from the broker that leads it at the highest
/// term any member reported, once a majority of each of the shard's voter
/// sets answered (see [`majority_answered`]). A broker leading at a lower
/// term may be deposed without knowing it, and its commit index may then
/// miss a write the newer leader acknowledged.
fn match_raft(
    assigned: &[&Assignment],
    answers: &[(String, &BrokerOffsets)],
) -> Result<Vec<PointShard>, Vec<String>> {
    let mut shards = Vec::new();
    let mut missing = Vec::new();
    for assignment in assigned {
        let key = assignment.key();
        let members: Vec<&RaftMember> = answers
            .iter()
            .flat_map(|(_, offsets)| &offsets.raft_members)
            .filter(|m| (&*m.tenant_id, &*m.namespace, &*m.kind, &*m.name, m.shard) == key)
            .collect();
        if let Err(short) = majority_answered(&members) {
            missing.push(format!(
                "{}/{}/{}/{} ({}): {short}",
                assignment.tenant_id,
                assignment.namespace,
                assignment.stream,
                assignment.shard,
                assignment.kind
            ));
            continue;
        }
        let newest = members.iter().map(|m| m.term).max().unwrap_or(0);
        let led = answers.iter().find_map(|(broker, offsets)| {
            offsets
                .shards
                .iter()
                .find(|s| {
                    s.engine.as_deref() == Some("raft")
                        && s.generation == newest
                        && (&*s.tenant_id, &*s.namespace, &*s.kind, &*s.name, s.shard) == key
                })
                .map(|found| (broker, found))
        });
        match led {
            Some((broker, found)) => shards.push(PointShard {
                tenant_id: assignment.tenant_id.clone(),
                namespace: assignment.namespace.clone(),
                name: assignment.stream.clone(),
                shard: assignment.shard,
                kind: assignment.kind.clone(),
                leader: broker.clone(),
                generation: assignment.generation,
                incarnation: found.incarnation,
                end_term: found.end_term,
                logs: found.logs,
            }),
            None => missing.push(format!(
                "{}/{}/{}/{} ({}): no member serves term {newest} as leader",
                assignment.tenant_id,
                assignment.namespace,
                assignment.stream,
                assignment.shard,
                assignment.kind
            )),
        }
    }
    if missing.is_empty() {
        Ok(shards)
    } else {
        Err(missing)
    }
}

/// Whether the answering `members` of one shard include a majority of each
/// voter set of its newest membership: the one from the latest entry any of
/// them holds. Every answer is read after the barrier and terms only grow,
/// so a majority intersects the voters that elected any leader that
/// acknowledged a write before it, and the newest term is seen. A joint
/// membership needs both majorities. A learner, or a copy that came back
/// under a new id, is in no voter set and counts for nothing.
fn majority_answered(members: &[&RaftMember]) -> Result<(), String> {
    let Some(newest) = members
        .iter()
        .filter(|m| !m.voters.is_empty())
        .max_by_key(|m| m.membership_at)
    else {
        return Err("no answering member reports its voters".to_string());
    };
    let answered: std::collections::BTreeSet<&Voter> =
        members.iter().filter_map(|m| m.member.as_ref()).collect();
    for set in &newest.voters {
        let set: std::collections::BTreeSet<&Voter> = set.iter().collect();
        let heard = set.intersection(&answered).count();
        if heard * 2 <= set.len() {
            return Err(format!(
                "{heard} of its {} voters answered; a point needs a majority",
                set.len()
            ));
        }
    }
    Ok(())
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
                    incarnation: found.incarnation,
                    end_term: None,
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
