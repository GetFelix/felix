//! `felixctl inspect`: what the brokers themselves hold, read-only.
//!
//! Each broker answers only for itself, so felixctl asks every broker that has
//! a part in the answer and puts the views together here. A broker that cannot
//! be reached is listed as unreachable; nothing is guessed on its behalf.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use felix_client::{Client, InspectedAssignment, ShardInspection, ShardKind};
use serde_json::{Value, json};

use crate::cli::{InspectCommand, InspectShardArgs};
use crate::connect::{Broker, client_config, server_name};
use crate::context::Settings;
use crate::controlplane::{Api, assignments};
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, table};

pub(crate) async fn run(
    command: &InspectCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    match command {
        InspectCommand::Shard(args) => shard(args, settings, out).await,
    }
}

/// The stream or cache a command names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) cache: bool,
}

impl Target {
    /// `NAME` in the configured tenant and namespace, or `TENANT/NS/NAME`.
    pub(crate) fn parse(
        given: &str,
        cache: bool,
        tenant: impl FnOnce() -> anyhow::Result<String>,
        namespace: &str,
    ) -> anyhow::Result<Self> {
        let parts: Vec<&str> = given.split('/').collect();
        let (tenant, namespace, name) = match parts.as_slice() {
            [name] if !name.is_empty() => (tenant()?, namespace.to_string(), name.to_string()),
            [tenant, namespace, name]
                if !tenant.is_empty() && !namespace.is_empty() && !name.is_empty() =>
            {
                (tenant.to_string(), namespace.to_string(), name.to_string())
            }
            _ => {
                return Err(fail(
                    Exit::Usage,
                    format!("{given:?} is neither NAME nor TENANT/NAMESPACE/NAME"),
                ));
            }
        };
        Ok(Self {
            tenant,
            namespace,
            name,
            cache,
        })
    }

    fn kind(&self) -> ShardKind {
        if self.cache {
            ShardKind::Cache
        } else {
            ShardKind::Stream
        }
    }

    fn kind_name(&self) -> &'static str {
        if self.cache { "cache" } else { "stream" }
    }
}

async fn shard(args: &InspectShardArgs, settings: &Settings, out: &Output) -> anyhow::Result<()> {
    let target = Target::parse(
        &args.name,
        args.cache,
        || Ok(settings.tenant()?.to_string()),
        &settings.namespace,
    )?;
    let broker = Broker::connect(settings).await?;
    let client = broker.cluster.client().await;
    let dialled = broker
        .cluster
        .endpoints()
        .await
        .first()
        .map_or_else(|| settings.brokers.join(","), ToString::to_string);
    if !client.supports_inspect() {
        return Err(unsupported(&dialled));
    }
    let addresses: BTreeMap<String, String> = client
        .topology()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|endpoint| (endpoint.node_id, endpoint.addr))
        .collect();
    let planned = match &settings.controlplane_url {
        Some(_) => Some(assignments(&Api::new(settings)?, None).await?),
        None => None,
    };

    let first_shard = args.shard.unwrap_or(0);
    let first = ask(&client, &target, first_shard).await?;
    let shards: Vec<u32> = match args.shard {
        Some(shard) => vec![shard],
        None if first.shards == 0 => {
            return Err(fail(
                Exit::NotFound,
                format!(
                    "the broker knows no {} {:?} in {}/{}",
                    target.kind_name(),
                    target.name,
                    target.tenant,
                    target.namespace
                ),
            ));
        }
        None => (0..first.shards).collect(),
    };
    let mut peers = Peers {
        settings,
        addresses,
        connected: BTreeMap::new(),
    };
    for shard in shards {
        let local = if shard == first_shard {
            first.clone()
        } else {
            ask(&client, &target, shard).await?
        };
        let assignment = planned
            .as_ref()
            .and_then(|planned| planned_assignment(planned, &target, shard))
            .or_else(|| local.assignment.clone());
        let mut views = BTreeMap::new();
        let mut unreachable = Vec::new();
        for node in assignment.as_ref().map(members).unwrap_or_default() {
            if node == local.node_id {
                views.insert(node, local.clone());
                continue;
            }
            match peers.ask(&node, &target, shard).await {
                Ok(view) => {
                    views.insert(node, view);
                }
                Err(_) => unreachable.push(node),
            }
        }
        if assignment.is_none() {
            views.insert(local.node_id.clone(), local.clone());
        }
        let report = report(&target, shard, assignment.as_ref(), &views, &unreachable);
        if out.json {
            out.json_value(&report)?;
        } else {
            out.text(&render(&report))?;
        }
    }
    Ok(())
}

fn unsupported(broker: &str) -> anyhow::Error {
    fail(
        Exit::Server,
        format!("broker {broker} does not support inspect; upgrade it to use felixctl inspect"),
    )
}

async fn ask(client: &Client, target: &Target, shard: u32) -> anyhow::Result<ShardInspection> {
    client
        .inspect_shard(
            target.kind(),
            &target.tenant,
            &target.namespace,
            &target.name,
            shard,
        )
        .await
        .mark(
            Exit::Server,
            format!("inspect {} shard {shard}", target.name),
        )
}

/// Connections to the brokers the assignment names, opened as needed.
struct Peers<'a> {
    settings: &'a Settings,
    /// Each broker's client address, as the cluster advertises it.
    addresses: BTreeMap<String, String>,
    connected: BTreeMap<String, Client>,
}

impl Peers<'_> {
    async fn ask(
        &mut self,
        node: &str,
        target: &Target,
        shard: u32,
    ) -> anyhow::Result<ShardInspection> {
        if !self.connected.contains_key(node) {
            let Some(addr) = self.addresses.get(node) else {
                anyhow::bail!("{node} advertises no client address");
            };
            let addr: SocketAddr = tokio::net::lookup_host(addr.as_str())
                .await?
                .next()
                .ok_or_else(|| anyhow::anyhow!("{addr} resolves to nothing"))?;
            let mut config = client_config(self.settings)?;
            // One question at a time: the pools a publisher wants cost a
            // handshake each.
            config.publish_conn_pool = 1;
            config.publish_streams_per_conn = 1;
            config.cache_conn_pool = 1;
            config.cache_streams_per_conn = 1;
            config.event_conn_pool = 1;
            let client = Client::connect(addr, &server_name(self.settings), config).await?;
            if !client.supports_inspect() {
                return Err(unsupported(node));
            }
            self.connected.insert(node.to_string(), client);
        }
        let client = &self.connected[node];
        ask(client, target, shard).await
    }
}

/// The control plane's assignment for one shard, in the broker's terms.
fn planned_assignment(
    planned: &[Value],
    target: &Target,
    shard: u32,
) -> Option<InspectedAssignment> {
    let found = crate::topology::shard_owners(
        planned.to_vec(),
        &target.tenant,
        &target.namespace,
        &target.name,
        target.kind_name(),
    )
    .into_iter()
    .find(|assignment| assignment["shard"] == shard)?;
    Some(InspectedAssignment {
        generation: found["generation"].as_u64()?,
        leader: found["leader"].as_str()?.to_string(),
        replicas: found["replicas"]
            .as_array()
            .map(|replicas| {
                replicas
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        draining: found["state"] == "draining",
        successor: found["successor"].as_str().map(str::to_string),
    })
}

/// The leader first, then its replicas, each once.
fn members(assignment: &InspectedAssignment) -> Vec<String> {
    let mut nodes = vec![assignment.leader.clone()];
    for replica in &assignment.replicas {
        if !nodes.contains(replica) {
            nodes.push(replica.clone());
        }
    }
    nodes
}

/// One shard put together from every view gathered, as `--json` prints it.
pub(crate) fn report(
    target: &Target,
    shard: u32,
    assignment: Option<&InspectedAssignment>,
    views: &BTreeMap<String, ShardInspection>,
    unreachable: &[String],
) -> Value {
    let leader_id = assignment
        .map(|assignment| assignment.leader.clone())
        .or_else(|| views.keys().next().cloned());
    let leader = leader_id.as_ref().and_then(|id| views.get(id));
    let replicas: Vec<Value> = assignment
        .map(|assignment| {
            members(assignment)
                .into_iter()
                .filter(|node| Some(node) != leader_id.as_ref())
                .map(|node| replica_row(&node, assignment, leader, views.get(&node)))
                .collect()
        })
        .unwrap_or_default();
    json!({
        "kind": target.kind_name(),
        "tenant_id": target.tenant,
        "namespace": target.namespace,
        "name": target.name,
        "shard": shard,
        "assignment": assignment.map(|assignment| json!({
            "generation": assignment.generation,
            "leader": assignment.leader,
            "replicas": assignment.replicas,
            "move": assignment.successor.as_ref().map(|to| json!({
                "to": to,
                "step": if assignment.draining { "fenced" } else { "staged" },
            })),
        })),
        "leader": leader.map(|view| {
            let mut leader = json!({
                "node_id": view.node_id,
                "phase": view.phase,
                "serving": view.serving,
                "generation": view.generation,
            });
            let fields = [
                ("reason", json!(view.reason)),
                ("detail", json!(view.detail)),
                ("fence", json!(view.fence)),
                ("lease", json!(view.lease)),
                ("tail", json!(view.tail)),
                ("committed", json!(view.committed)),
            ];
            for (name, value) in fields {
                if !value.is_null() {
                    leader[name] = value;
                }
            }
            leader
        }),
        "replicas": replicas,
        "unreachable": unreachable,
    })
}

/// One replica: the leader's account of it, and its own view when it was
/// reached.
fn replica_row(
    node: &str,
    assignment: &InspectedAssignment,
    leader: Option<&ShardInspection>,
    own: Option<&ShardInspection>,
) -> Value {
    let seen = leader.and_then(|leader| leader.replicas.iter().find(|r| r.node_id == node));
    let role = match seen {
        Some(seen) => seen.role.clone(),
        None if assignment.successor.as_deref() == Some(node) => "learner".to_string(),
        None => "follower".to_string(),
    };
    let mut row = json!({ "node_id": node, "role": role });
    if let Some(seen) = seen {
        row["next_offset"] = json!(seen.next_offset);
        row["lag"] = json!(seen.lag);
        row["fence"] = json!(seen.fence);
        row["state"] = json!(seen.state);
        if let Some(halted) = &seen.halted {
            row["halted"] = json!(halted);
        }
    }
    if let Some(own) = own {
        row["own"] = json!({
            "phase": own.phase,
            "generation": own.generation,
            "tail": own.tail,
            "accepted_generation": own.accepted_generation,
        });
    }
    row
}

/// The human form of [`report`].
pub(crate) fn render(report: &Value) -> String {
    let mut head: Vec<(&str, String)> = vec![(
        "shard",
        format!(
            "{}/{}/{}/{} ({})",
            text(&report["tenant_id"]),
            text(&report["namespace"]),
            text(&report["name"]),
            report["shard"],
            text(&report["kind"])
        ),
    )];
    let assignment = &report["assignment"];
    if !assignment.is_null() {
        let mut line = format!("{}", assignment["generation"]);
        if !assignment["move"].is_null() {
            line.push_str(&format!(
                ", move to {} {}",
                text(&assignment["move"]["to"]),
                text(&assignment["move"]["step"])
            ));
        }
        head.push(("generation", line));
    }
    let leader = &report["leader"];
    let leader_id = text(&assignment["leader"]);
    if leader.is_null() {
        head.push(("leader", format!("{leader_id}  unreachable")));
    } else {
        head.push((
            "leader",
            format!("{}  {}", text(&leader["node_id"]), serving(leader)),
        ));
        if let Some(lease) = leader.get("lease") {
            let held = if lease["held"] == true {
                format!("held, {} left", seconds(&lease["remaining_ms"]))
            } else {
                "not held".to_string()
            };
            head.push(("lease", held));
        }
        let mut offsets = Vec::new();
        for field in ["tail", "committed"] {
            if let Some(value) = leader.get(field) {
                offsets.push(format!("{field} {value}"));
            }
        }
        if !offsets.is_empty() {
            head.push(("offsets", offsets.join("  ")));
        }
    }
    let unreachable = report["unreachable"]
        .as_array()
        .map(|nodes| nodes.iter().map(text).collect::<Vec<_>>())
        .unwrap_or_default();
    if !unreachable.is_empty() {
        head.push(("unreachable", unreachable.join(", ")));
    }
    let width = head.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    let mut out: Vec<String> = head
        .iter()
        .map(|(key, value)| format!("{key:width$}  {value}"))
        .collect();

    let mut rows = Vec::new();
    if !leader.is_null() {
        rows.push(vec![
            text(&leader["node_id"]),
            "leader".to_string(),
            dash(&leader["tail"]),
            "-".to_string(),
            "-".to_string(),
            text(&leader["phase"]),
        ]);
    }
    for replica in report["replicas"].as_array().into_iter().flatten() {
        let own = &replica["own"];
        let (next, state) = if replica.get("state").is_some() {
            (dash(&replica["next_offset"]), text(&replica["state"]))
        } else if !own.is_null() {
            // The leader gave no account of it: its own view is all there is.
            (dash(&own["tail"]), text(&own["phase"]))
        } else {
            ("-".to_string(), "unreachable".to_string())
        };
        let state = match replica.get("halted") {
            Some(halted) => format!("{state} ({})", text(halted)),
            None => state,
        };
        rows.push(vec![
            text(&replica["node_id"]),
            text(&replica["role"]),
            next,
            dash(&replica["lag"]),
            match replica.get("fence") {
                Some(Value::Bool(true)) => "yes".to_string(),
                Some(Value::Bool(false)) => "no".to_string(),
                _ => "-".to_string(),
            },
            state,
        ]);
    }
    if !rows.is_empty() {
        out.push(String::new());
        out.push(table(
            &["REPLICA", "ROLE", "NEXT OFFSET", "LAG", "FENCE", "STATE"],
            rows,
        ));
    }
    out.join("\n")
}

/// `serving`, or `not serving: REASON, DETAIL (fence attempts)`.
fn serving(leader: &Value) -> String {
    if leader["serving"] == true {
        return "serving".to_string();
    }
    let mut line = format!("not serving: {}", text(&leader["reason"]));
    if let Some(detail) = leader.get("detail") {
        line.push_str(&format!(", {}", text(detail)));
    }
    if let Some(fence) = leader.get("fence") {
        let mut extra = format!("{} attempts", fence["attempts"]);
        if let Some(retry) = fence.get("retry_in_ms") {
            extra.push_str(&format!(", next in {}", seconds(retry)));
        }
        line.push_str(&format!(" ({extra})"));
    }
    line
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn dash(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        other => text(other),
    }
}

/// Milliseconds as seconds: `2s`, `7.1s`.
fn seconds(millis: &Value) -> String {
    let millis = millis.as_u64().unwrap_or(0);
    if millis.is_multiple_of(1000) {
        format!("{}s", millis / 1000)
    } else {
        format!("{:.1}s", millis as f64 / 1000.0)
    }
}

#[cfg(test)]
mod tests;
