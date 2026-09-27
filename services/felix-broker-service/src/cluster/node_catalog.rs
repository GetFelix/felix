//! The address book: which broker is where.
//!
//! Shard assignments name an owner by node id. Forwarding needs an address, and
//! only the control plane's node catalog has one. Without this the router
//! resolves every remote shard to "owner unavailable" — the assignment is known
//! and the node behind it is not.
//!
//! Read with the same credential as the assignment feed: `/v1/nodes` requires
//! `node.view:cluster:*`, which a broker already holds to read assignments at
//! all.
use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use felix_router::NodeRef;
use felix_wire::BrokerEndpoint;
use serde::Deserialize;

/// One fetch, read two ways.
///
/// The same listing answers "where does this broker forward to" and "where may
/// a client connect", and they are different addresses on different listeners.
/// Fetching once and splitting here keeps them from drifting apart.
#[derive(Debug, Default)]
pub struct NodeCatalog {
    pub nodes: HashMap<String, NodeRef>,
    /// Sorted by node id, so the answer a client gets does not reshuffle
    /// between refreshes that changed nothing.
    pub client_endpoints: Vec<BrokerEndpoint>,
    /// Client addresses of every routable broker, draining ones included.
    /// Not offered to new clients, but a redirect to a draining broker that
    /// still leads the shard needs its address.
    pub redirect_endpoints: Vec<BrokerEndpoint>,
    /// Kafka listener addresses of every routable broker that runs one, sorted
    /// by node id. Routable rather than eligible for the same reason as
    /// redirects: a Kafka client has to reach whoever leads the partition,
    /// draining or not.
    pub kafka_endpoints: Vec<BrokerEndpoint>,
}

#[derive(Debug, Deserialize)]
struct NodeListResponse {
    items: Vec<NodeView>,
    /// Absent on the last page, and from a control plane that predates paging.
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NodeView {
    node: Node,
    placement: Placement,
}

#[derive(Debug, Deserialize)]
struct Node {
    node_id: String,
    spec: NodeSpec,
}

#[derive(Debug, Deserialize)]
struct NodeSpec {
    advertise_addr: String,
    /// Absent on a broker that predates client discovery, or one whose operator
    /// has not said where clients reach it.
    #[serde(default)]
    client_addr: Option<String>,
    /// Present only on a broker running the Kafka listener.
    #[serde(default)]
    kafka_addr: Option<String>,
    region: String,
}

#[derive(Debug, Deserialize)]
struct Placement {
    eligible: bool,
    /// Absent from a control plane that predates it, which counts only
    /// eligible nodes as live.
    #[serde(default)]
    routable: Option<bool>,
}

/// Fetch the catalog, every page of it.
///
/// A node whose advertised address does not parse is skipped rather than
/// failing the fetch: one malformed registration must not cost this broker every
/// other route it knows.
pub(crate) async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<NodeCatalog> {
    let mut all = NodeListResponse {
        items: Vec::new(),
        next_cursor: None,
    };
    let mut cursor: Option<String> = None;
    loop {
        let mut request = client.get(format!("{base_url}/v1/nodes"));
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        if let Some(bearer) = bearer {
            request = request.bearer_auth(bearer);
        }
        let response = request.send().await.context("send node list request")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("{status}: {body}"));
        }
        let page: NodeListResponse = response.json().await.context("decode node list")?;
        all.items.extend(page.items);
        match page.next_cursor {
            // A cursor that does not move would loop forever, and half a
            // catalog would read as brokers that are gone.
            Some(next) if cursor.as_ref() == Some(&next) => {
                return Err(anyhow!("node list cursor did not advance"));
            }
            Some(next) => cursor = Some(next),
            None => return Ok(into_catalog(all)),
        }
    }
}

/// Turn the control plane's answer into routable entries.
///
/// Separate from the request so the skipping rule above is testable without a
/// server standing in for the control plane.
fn into_catalog(response: NodeListResponse) -> NodeCatalog {
    let mut catalog = HashMap::with_capacity(response.items.len());
    let mut client_endpoints = Vec::new();
    let mut redirect_endpoints = Vec::new();
    let mut kafka_endpoints = Vec::new();
    for item in response.items {
        // The control plane's own verdict, which folds in lifecycle and
        // heartbeat age together. A broker re-deriving that from the lifecycle
        // alone would keep forwarding to a node whose heartbeat has lapsed but
        // whose sweep has not yet run. Routable rather than eligible: a
        // draining broker still serves the shards it has not handed off yet,
        // and refusing writes for them would make every drain refuse writes.
        let routable = item.placement.routable.unwrap_or(item.placement.eligible);
        // Offered to clients only while the cluster considers this broker able
        // to serve, and only when it said where clients reach it. Sending a
        // client to a broker that is down, or to the internal listener that
        // would refuse it, is worse than sending it nowhere.
        if let Some(client_addr) = &item.node.spec.client_addr
            && client_addr.parse::<std::net::SocketAddr>().is_ok()
        {
            let endpoint = BrokerEndpoint {
                node_id: item.node.node_id.clone(),
                addr: client_addr.clone(),
            };
            if item.placement.eligible {
                client_endpoints.push(endpoint.clone());
            }
            if routable {
                redirect_endpoints.push(endpoint);
            }
        }
        // Not parsed as a socket address: Kafka advertised listeners are
        // usually hostnames, and the control plane has already validated it.
        if routable && let Some(kafka_addr) = &item.node.spec.kafka_addr {
            kafka_endpoints.push(BrokerEndpoint {
                node_id: item.node.node_id.clone(),
                addr: kafka_addr.clone(),
            });
        }
        let Ok(advertise_addr) = item.node.spec.advertise_addr.parse() else {
            tracing::warn!(
                node_id = %item.node.node_id,
                advertise_addr = %item.node.spec.advertise_addr,
                "skipping node: advertised address does not parse",
            );
            continue;
        };
        catalog.insert(
            item.node.node_id.clone(),
            NodeRef {
                node_id: item.node.node_id,
                advertise_addr,
                region: item.node.spec.region,
                live: routable,
            },
        );
    }
    client_endpoints.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    kafka_endpoints.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    NodeCatalog {
        nodes: catalog,
        client_endpoints,
        redirect_endpoints,
        kafka_endpoints,
    }
}

#[cfg(test)]
mod tests;
