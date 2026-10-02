//! `felixctl topology`: a stream's or cache's shards, their owners, and the
//! brokers a client can reach.
//!
//! The shard count, the owners and the brokers come from a broker. Replicas
//! and assignment state are the control plane's, so they are added when a
//! control-plane URL is configured; its leader is used only for a broker too
//! old to name owners.

use felix_client::{ShardKind, ShardOwner};
use serde_json::Value;

use crate::cli::TopologyArgs;
use crate::connect::Broker;
use crate::context::Settings;
use crate::controlplane::{Api, assignments};
use crate::error::{Exit, fail};
use crate::output::{Output, cell, table};

pub(crate) async fn run(
    args: &TopologyArgs,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let broker = Broker::connect(settings).await?;
    let client = broker.cluster.client().await;
    let (tenant, namespace, name) = (
        broker.tenant.as_str(),
        broker.namespace.as_str(),
        args.name.as_str(),
    );
    let kind = if args.cache { "cache" } else { "stream" };
    let (shards, routing) = if args.cache {
        (client.cache_shards(tenant, namespace, name).await?, None)
    } else {
        let (shards, routing) = client.stream_routing(tenant, namespace, name).await?;
        (shards, Some(routing))
    };
    if shards == 0 {
        return Err(fail(
            Exit::NotFound,
            format!("the broker knows no {kind} {name:?} in {tenant}/{namespace}"),
        ));
    }
    let brokers: Vec<(String, String)> = client
        .topology()
        .await?
        .into_iter()
        .map(|endpoint| (endpoint.node_id, endpoint.addr))
        .collect();

    let owners = if client.supports_shard_owners() {
        let shard_kind = if args.cache {
            ShardKind::Cache
        } else {
            ShardKind::Stream
        };
        Some(
            client
                .shard_owners(tenant, namespace, name, shard_kind)
                .await?,
        )
    } else {
        None
    };
    let assignments = match &settings.controlplane_url {
        Some(_) => {
            let api = Api::new(settings)?;
            Some(shard_owners(
                assignments(&api, None).await?,
                tenant,
                namespace,
                name,
                kind,
            ))
        }
        None => None,
    };
    let owners_known = owners.is_some() || assignments.is_some();

    let shard_rows: Vec<Value> = (0..shards)
        .map(|shard| {
            let owner = owners
                .as_ref()
                .and_then(|owners| owners.iter().find(|o| o.shard == shard));
            let assignment = assignments
                .as_ref()
                .and_then(|assignments| assignments.iter().find(|a| a["shard"] == shard));
            shard_row(shard, owner, assignment, &brokers)
        })
        .collect();

    if out.json {
        return out.json_value(&serde_json::json!({
            "tenant": tenant,
            "namespace": namespace,
            "name": name,
            "kind": kind,
            "shards": shards,
            "routing": routing,
            "owners_known": owners_known,
            "shard_owners": shard_rows,
            "brokers": brokers
                .iter()
                .map(|(node, addr)| serde_json::json!({ "node_id": node, "addr": addr }))
                .collect::<Vec<_>>(),
            "connected_to": broker.cluster.endpoints().await,
        }));
    }

    let mut text = format!("{kind} {tenant}/{namespace}/{name}: {shards} shard(s)");
    if let Some(routing) = routing {
        text.push_str(&format!(
            ", {} routing",
            cell(&serde_json::to_value(routing)?)
        ));
    }
    text.push_str("\n\n");
    if owners_known {
        let rows = shard_rows
            .iter()
            .map(|row| {
                [
                    "shard",
                    "leader",
                    "leader_addr",
                    "replicas",
                    "generation",
                    "state",
                ]
                .iter()
                .map(|field| cell(&row[*field]))
                .collect()
            })
            .collect();
        text.push_str(&table(
            &["SHARD", "LEADER", "ADDR", "REPLICAS", "GENERATION", "STATE"],
            rows,
        ));
    } else {
        text.push_str(
            "owners: unknown; this broker does not report them, so set a control-plane URL (--controlplane-url)",
        );
    }
    text.push_str("\n\n");
    if brokers.is_empty() {
        text.push_str("brokers: none advertised; the cluster has not been told client addresses");
    } else {
        let rows = brokers
            .iter()
            .map(|(node, addr)| vec![node.clone(), addr.clone()])
            .collect();
        text.push_str(&table(&["BROKER", "ADDR"], rows));
    }
    out.text(&text)
}

/// One shard's row: the broker's account of its owner, with the control
/// plane's replicas and state when there are any.
fn shard_row(
    shard: u32,
    owner: Option<&ShardOwner>,
    assignment: Option<&Value>,
    brokers: &[(String, String)],
) -> Value {
    let leader = match owner {
        Some(owner) => owner.node_id.clone(),
        None => assignment
            .and_then(|a| a["leader"].as_str())
            .map(str::to_string),
    };
    let addr = owner.and_then(|owner| owner.addr.clone()).or_else(|| {
        leader.as_ref().and_then(|leader| {
            brokers
                .iter()
                .find(|(node, _)| node == leader)
                .map(|(_, addr)| addr.clone())
        })
    });
    let generation = match owner {
        Some(owner) => Value::from(owner.generation),
        None => assignment.map_or(Value::Null, |a| a["generation"].clone()),
    };
    serde_json::json!({
        "shard": shard,
        "leader": leader,
        "leader_addr": addr,
        "replicas": assignment.map(|a| a["replicas"].clone()),
        "generation": generation,
        "state": assignment.map(|a| a["state"].clone()),
        "unavailable": owner.and_then(|owner| owner.unavailable.clone()),
    })
}

/// The assignments for one stream's or cache's shards.
pub(crate) fn shard_owners(
    assignments: Vec<Value>,
    tenant: &str,
    namespace: &str,
    name: &str,
    kind: &str,
) -> Vec<Value> {
    assignments
        .into_iter()
        .filter(|a| {
            a["tenant_id"] == tenant
                && a["namespace"] == namespace
                && a["stream"] == name
                // An assignment written before caches were placed has no kind
                // and is a stream's.
                && a["kind"].as_str().unwrap_or("stream") == kind
        })
        .collect()
}

#[cfg(test)]
mod tests;
