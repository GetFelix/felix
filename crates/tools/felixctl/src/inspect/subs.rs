//! `felixctl inspect subs`: the subscriptions each broker serves.
//!
//! Every broker answers for its own subscribers, so felixctl asks each one
//! (or only `--node`) for a page and prints them together. A broker that
//! cannot be reached or is too old to answer is reported as such.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_client::{
    InspectedSubscription, SubscriptionCursor, SubscriptionFilter, SubscriptionsPage,
};
use serde_json::{Value, json};

use super::{Peers, Target, unsupported};
use crate::cli::InspectSubsArgs;
use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, table};

/// One broker's answer, or why there is none.
#[derive(Debug)]
pub(crate) enum NodePage {
    Answered(SubscriptionsPage),
    Failed { node_id: String, error: String },
}

pub(super) async fn run(
    args: &InspectSubsArgs,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let filter = filter(args, settings)?;
    let cursor = args.cursor.as_deref().map(decode_cursor).transpose()?;
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
    let nodes: Vec<String> = match &args.node {
        Some(node) => vec![node.clone()],
        None => addresses.keys().cloned().collect(),
    };

    let mut pages = Vec::new();
    let mut failures = Vec::new();
    if nodes.is_empty() {
        // A broker outside a cluster advertises no others: it is the only one.
        let page = client
            .list_subscriptions(filter, Some(args.limit), cursor)
            .await
            .mark(Exit::Server, format!("list subscriptions on {dialled}"))?;
        pages.push(NodePage::Answered(page));
    } else {
        let mut peers = Peers {
            settings,
            addresses,
            connected: BTreeMap::new(),
        };
        for node in nodes {
            let answer = match peers.client(&node).await {
                Ok(client) => {
                    client
                        .list_subscriptions(filter.clone(), Some(args.limit), cursor.clone())
                        .await
                }
                Err(err) => Err(err),
            };
            match answer {
                Ok(page) => pages.push(NodePage::Answered(page)),
                Err(err) => failures.push((node, err)),
            }
        }
    }
    // Nothing answered: that is the result, with its own exit status (a
    // refused token, an unknown --node), not an empty list.
    if pages.is_empty()
        && let Some((node, err)) = failures.drain(..).next()
    {
        return Err(err).mark(Exit::Server, format!("list subscriptions on {node}"));
    }
    pages.extend(failures.into_iter().map(|(node_id, err)| NodePage::Failed {
        node_id,
        error: format!("{err:#}"),
    }));
    if out.json {
        for page in &pages {
            out.json_value(&page_json(page))?;
        }
    } else {
        out.text(&render(&pages))?;
    }
    Ok(())
}

fn filter(args: &InspectSubsArgs, settings: &Settings) -> anyhow::Result<SubscriptionFilter> {
    let mut filter = SubscriptionFilter {
        shard: args.shard,
        principal: args.principal.clone(),
        dropping: args.dropping,
        ..SubscriptionFilter::default()
    };
    if let Some(stream) = &args.stream {
        let target = Target::parse(
            stream,
            false,
            || Ok(settings.tenant()?.to_string()),
            &settings.namespace,
        )?;
        filter.tenant_id = Some(target.tenant);
        filter.namespace = Some(target.namespace);
        filter.stream = Some(target.name);
    }
    Ok(filter)
}

/// A cursor as felixctl prints it: one word to paste back into `--cursor`.
pub(crate) fn encode_cursor(cursor: &SubscriptionCursor) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor).expect("a cursor serializes"))
}

pub(crate) fn decode_cursor(given: &str) -> anyhow::Result<SubscriptionCursor> {
    URL_SAFE_NO_PAD
        .decode(given)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .ok_or_else(|| {
            fail(
                Exit::Usage,
                format!("{given:?} is not a cursor felixctl printed"),
            )
        })
}

/// Records the subscriber has yet to be handed, when its position is known.
fn behind(subscription: &InspectedSubscription) -> Option<u64> {
    subscription
        .position
        .map(|position| subscription.tail.saturating_sub(position))
}

/// One broker's page as `--json` prints it.
pub(crate) fn page_json(page: &NodePage) -> Value {
    match page {
        NodePage::Answered(page) => json!({
            "node_id": page.node_id,
            "subscriptions": page.subscriptions.iter().map(|subscription| {
                let mut row = serde_json::to_value(subscription).expect("serializes");
                if let Some(behind) = behind(subscription) {
                    row["behind"] = json!(behind);
                }
                row
            }).collect::<Vec<_>>(),
            "next_cursor": page.next_cursor.as_ref().map(encode_cursor),
        }),
        NodePage::Failed { node_id, error } => json!({
            "node_id": node_id,
            "error": error,
        }),
    }
}

/// The human form: one table across every broker, then what is left out.
pub(crate) fn render(pages: &[NodePage]) -> String {
    let mut rows = Vec::new();
    let mut notes = Vec::new();
    for page in pages {
        match page {
            NodePage::Answered(page) => {
                let node = if page.node_id.is_empty() {
                    "-"
                } else {
                    page.node_id.as_str()
                };
                for s in &page.subscriptions {
                    rows.push(vec![
                        node.to_string(),
                        format!("{}/{}/{}/{}", s.tenant_id, s.namespace, s.stream, s.shard),
                        s.subscriber_id.to_string(),
                        match (&s.connection, &s.peer) {
                            (Some(id), Some(peer)) => format!("{id} {peer}"),
                            (Some(id), None) => id.to_string(),
                            _ => "-".to_string(),
                        },
                        s.principal.clone().unwrap_or_else(|| "-".to_string()),
                        s.policy.clone(),
                        format!("{}/{}", s.depth, s.capacity),
                        s.dropped.to_string(),
                        s.position
                            .map_or_else(|| "-".to_string(), |p| p.to_string()),
                        s.tail.to_string(),
                        behind(s).map_or_else(|| "-".to_string(), |b| b.to_string()),
                    ]);
                }
                if let Some(cursor) = &page.next_cursor {
                    notes.push(format!(
                        "more on {node}: --node {node} --cursor {}",
                        encode_cursor(cursor)
                    ));
                }
            }
            NodePage::Failed { node_id, error } => notes.push(format!("{node_id}: {error}")),
        }
    }
    let mut out = Vec::new();
    if rows.is_empty() {
        out.push("no subscriptions".to_string());
    } else {
        out.push(table(
            &[
                "NODE",
                "STREAM/SHARD",
                "SUB",
                "CONNECTION",
                "PRINCIPAL",
                "POLICY",
                "QUEUE",
                "DROPPED",
                "POSITION",
                "TAIL",
                "BEHIND",
            ],
            rows,
        ));
    }
    if !notes.is_empty() {
        out.push(String::new());
        out.extend(notes);
    }
    out.join("\n")
}

#[cfg(test)]
mod tests;
