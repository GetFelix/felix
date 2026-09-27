//! Reading the node catalog, with each node's placement standing explained.
use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;

use super::require_cluster_node_view;
use crate::api::AppState;
use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::api::pagination::{PageParams, list_visible};
use crate::api::types::{NodeListResponse, NodePlacement, NodeView};
use crate::clock::now_millis;
use crate::model::{Node, NodeLifecycle};
use crate::store::StoreError;

#[utoipa::path(
    get,
    path = "/v1/nodes",
    tag = "nodes",
    params(
        ("lifecycle" = Option<String>, Query, description = "live, draining, down, or left"),
        ("region" = Option<String>, Query, description = "Exact region match"),
        ("label" = Option<String>, Query, description = "key=value; repeat to require several"),
        PageParams
    ),
    responses(
        (status = 200, description = "A page of registered nodes, in id order", body = NodeListResponse),
        (status = 400, description = "Invalid limit or cursor", body = crate::api::types::ErrorResponse)
    )
)]
/// List registered brokers and why each is, or is not, placeable.
///
/// Paged like every listing, though a cluster rarely has more brokers than
/// fit on the default page.
///
/// # Errors
/// - 400 for an invalid `limit` or `cursor`.
/// - 500 when the store cannot be read.
pub(crate) async fn list_nodes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    raw_query: axum::extract::RawQuery,
    Query(page): Query<PageParams>,
) -> Result<Json<NodeListResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let filters = NodeFilters::from_query(&query, raw_query.0.as_deref().unwrap_or_default());
    let now = now_millis();
    let expiry = state.node_liveness.expiry_timeout_ms;

    let listed = list_visible(
        page.request()?,
        |page| state.store.list_nodes_page(page),
        |node| filters.matches(node),
        |node: &Node| node.node_id.clone(),
    )
    .await
    .map_err(|ref err| api_internal("failed to list nodes", err))?;
    let items = listed
        .items
        .into_iter()
        .map(|node| NodeView {
            placement: placement_for(&node, now, expiry),
            node,
        })
        .collect();

    Ok(Json(NodeListResponse {
        items,
        next_cursor: listed.next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node detail", body = NodeView),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse)
    )
)]
/// Fetch one broker, with the same placement explanation the listing gives.
///
/// # Errors
/// - 404 when the node is not registered.
pub(crate) async fn get_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<NodeView>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let node = state
        .store
        .get_node(&node_id)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            ref other => api_internal("get node", other),
        })?;

    let placement = placement_for(&node, now_millis(), state.node_liveness.expiry_timeout_ms);
    Ok(Json(NodeView { node, placement }))
}

/// Filters accepted by the node listing.
///
/// Every filter is an intersection, and an absent filter matches everything.
#[derive(Debug, Default)]
struct NodeFilters {
    lifecycle: Option<String>,
    region: Option<String>,
    /// `key=value` pairs a node must carry all of.
    labels: Vec<(String, String)>,
}

impl NodeFilters {
    /// `?lifecycle=live&region=us-west-2&label=rack%3Da1&label=tier%3Dhot`
    ///
    /// Repeating `label` intersects rather than replaces, which is what makes
    /// "the hot racks in this region" expressible.
    fn from_query(query: &HashMap<String, String>, raw: &str) -> Self {
        let labels = raw
            .split('&')
            .filter_map(|pair| pair.strip_prefix("label="))
            .filter_map(|value| {
                let decoded = percent_decode(value);
                decoded
                    .split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect();
        Self {
            lifecycle: query.get("lifecycle").cloned(),
            region: query.get("region").cloned(),
            labels,
        }
    }

    fn matches(&self, node: &crate::model::Node) -> bool {
        if let Some(lifecycle) = &self.lifecycle
            && !lifecycle_matches(node.status.lifecycle, lifecycle)
        {
            return false;
        }
        if let Some(region) = &self.region
            && &node.spec.region != region
        {
            return false;
        }
        self.labels
            .iter()
            .all(|(key, value)| node.spec.labels.get(key) == Some(value))
    }
}

/// Explain this node's placement standing at `now`.
fn placement_for(node: &Node, now_millis: u64, expiry_timeout_ms: u64) -> NodePlacement {
    // Saturating: a heartbeat recorded a moment ahead of this read (two control
    // plane instances, slightly different clocks) is an age of zero, not a
    // wrapped enormous one.
    let heartbeat_age_ms = now_millis.saturating_sub(node.status.last_heartbeat_at_millis);
    let mut reasons = Vec::new();
    let heartbeat_fresh = heartbeat_age_ms <= expiry_timeout_ms;
    let routable = heartbeat_fresh
        && matches!(
            node.status.lifecycle,
            NodeLifecycle::Live | NodeLifecycle::Draining
        );

    match node.status.lifecycle {
        NodeLifecycle::Live => {}
        NodeLifecycle::Draining => {
            reasons.push("node is draining and takes no new placement".into())
        }
        NodeLifecycle::Down => reasons.push("node missed its heartbeat window".into()),
        NodeLifecycle::Left => reasons.push("node deregistered and left the cluster".into()),
    }

    // Reported separately from the lifecycle: between a heartbeat lapsing and
    // the sweep noticing, a node still reads `live` while already being past
    // its window, and that gap is exactly what an operator is trying to see.
    if !heartbeat_fresh
        && matches!(
            node.status.lifecycle,
            NodeLifecycle::Live | NodeLifecycle::Draining
        )
    {
        reasons.push(format!(
            "last heartbeat was {heartbeat_age_ms}ms ago, past the {expiry_timeout_ms}ms timeout; expiry has not run yet"
        ));
    }

    if node.spec.capacity.max_shards == Some(0) {
        reasons.push("capacity hint max_shards is zero".into());
    }
    if node.spec.capacity.weight == 0 {
        reasons.push("capacity hint weight is zero, so placement never selects it".into());
    }

    NodePlacement {
        eligible: reasons.is_empty(),
        routable,
        reasons,
        heartbeat_age_ms,
    }
}

/// Minimal `%XX` decoding for label values, which routinely contain `=` and `/`.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&value[i + 1..i + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                }
                Err(_) => {
                    out.push(bytes[i]);
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

fn lifecycle_matches(lifecycle: NodeLifecycle, wanted: &str) -> bool {
    let name = match lifecycle {
        NodeLifecycle::Live => "live",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Down => "down",
        NodeLifecycle::Left => "left",
    };
    name.eq_ignore_ascii_case(wanted)
}
