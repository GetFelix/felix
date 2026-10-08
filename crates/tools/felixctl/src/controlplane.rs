//! Read-only control-plane commands over its REST API: `tenant`,
//! `namespace`, `stream`, `cache ls|info`, `node` and `shard`, and the HTTP
//! client the writing commands share.
//!
//! Resources are handled as JSON values rather than typed copies of the
//! control plane's models, so a field the control plane adds shows up in
//! `--json` output without a CLI release. The tables pick known fields by
//! JSON pointer.

use std::time::Duration;

use serde_json::Value;

use crate::cli::{NamespaceCommand, NodeCommand, ShardCommand, StreamCommand, TenantCommand};
use crate::context::Settings;
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, cell, fields, table};

/// Page size asked for when listing; the CLI follows `next_cursor` either way.
const PAGE_LIMIT: &str = "1000";

/// A column of a listing: its header and where its value is.
pub(crate) type Column = (&'static str, &'static str);

const TENANT_COLUMNS: &[Column] = &[("TENANT", "/tenant_id"), ("NAME", "/display_name")];
const NAMESPACE_COLUMNS: &[Column] = &[("NAMESPACE", "/namespace"), ("NAME", "/display_name")];
const STREAM_COLUMNS: &[Column] = &[
    ("STREAM", "/stream"),
    ("SHARDS", "/shards"),
    ("RF", "/replication_factor"),
    ("CONSISTENCY", "/consistency"),
    ("DELIVERY", "/delivery"),
    ("DURABLE", "/durable"),
];
const CACHE_COLUMNS: &[Column] = &[
    ("CACHE", "/cache"),
    ("SHARDS", "/shards"),
    ("RF", "/replication_factor"),
    ("CONSISTENCY", "/consistency"),
];
const NODE_COLUMNS: &[Column] = &[
    ("NODE", "/node/node_id"),
    ("CLIENT ADDR", "/node/spec/client_addr"),
    ("REGION", "/node/spec/region"),
    ("LIFECYCLE", "/node/status/lifecycle"),
    ("ELIGIBLE", "/placement/eligible"),
    ("HEARTBEAT AGE MS", "/placement/heartbeat_age_ms"),
];
pub(crate) const SHARD_COLUMNS: &[Column] = &[
    ("TENANT", "/tenant_id"),
    ("NAMESPACE", "/namespace"),
    ("NAME", "/stream"),
    ("KIND", "/kind"),
    ("SHARD", "/shard"),
    ("LEADER", "/leader"),
    ("REPLICAS", "/replicas"),
    ("GENERATION", "/generation"),
    ("STATE", "/state"),
];

/// The control plane's HTTP API, authenticated as the context says.
pub(crate) struct Api {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Api {
    pub(crate) fn new(settings: &Settings) -> anyhow::Result<Self> {
        let base = settings
            .controlplane_url()?
            .trim_end_matches('/')
            .to_string();
        let mut builder = reqwest::Client::builder().connect_timeout(Duration::from_secs(10));
        if let Some(path) = &settings.controlplane_ca_file {
            let pem = std::fs::read(path).mark(Exit::Usage, format!("read {}", path.display()))?;
            for cert in reqwest::Certificate::from_pem_bundle(&pem)
                .mark(Exit::Usage, format!("parse {}", path.display()))?
            {
                builder = builder.add_root_certificate(cert);
            }
        }
        Ok(Self {
            http: builder
                .build()
                .mark(Exit::Usage, "set up the HTTP client")?,
            base,
            token: settings.controlplane_token()?,
        })
    }

    /// GET one resource.
    pub(crate) async fn get(&self, path: &str, query: &[(&str, &str)]) -> anyhow::Result<Value> {
        let url = format!("{}{path}", self.base);
        let mut request = self.http.get(&url).query(query);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.mark(
            Exit::Connection,
            format!("reach the control plane at {}", self.base),
        )?;
        let status = response.status();
        let body = response
            .text()
            .await
            .mark(Exit::Connection, format!("read the answer to GET {path}"))?;
        if !status.is_success() {
            let exit = if status == reqwest::StatusCode::NOT_FOUND {
                Exit::NotFound
            } else {
                Exit::Server
            };
            return Err(fail(
                exit,
                format!("GET {path}: {status}: {}", error_message(&body)),
            ));
        }
        serde_json::from_str(&body).mark(Exit::Server, format!("GET {path} answered non-JSON"))
    }

    /// Send `body` as JSON with `method`, for writes answered with no body
    /// worth reading. 404 is [`Exit::NotFound`], any other refusal
    /// [`Exit::Server`] with the control plane's message.
    pub(crate) async fn send_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &Value,
    ) -> anyhow::Result<()> {
        let url = format!("{}{path}", self.base);
        let mut request = self.http.request(method.clone(), &url).json(body);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.mark(
            Exit::Connection,
            format!("reach the control plane at {}", self.base),
        )?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        let exit = if status == reqwest::StatusCode::NOT_FOUND {
            Exit::NotFound
        } else {
            Exit::Server
        };
        Err(fail(
            exit,
            format!("{method} {path}: {status}: {}", error_message(&text)),
        ))
    }

    /// GET every page of a listing, following `next_cursor`.
    pub(crate) async fn list(
        &self,
        path: &str,
        filter: &[(&str, &str)],
    ) -> anyhow::Result<Vec<Value>> {
        let mut items = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut query: Vec<(&str, &str)> = filter.to_vec();
            query.push(("limit", PAGE_LIMIT));
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor));
            }
            let page = self.get(path, &query).await?;
            let (page_items, next) = page_parts(page)?;
            items.extend(page_items);
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(items),
            }
        }
    }
}

pub(crate) async fn tenant(
    command: &TenantCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let tenants = api.list("/v1/tenants", &[]).await?;
    match command {
        TenantCommand::Ls => print_list(out, "tenants", TENANT_COLUMNS, tenants),
        // There is no GET for one tenant, so it is found in the listing.
        TenantCommand::Info { tenant } => {
            let found = find(tenants, "/tenant_id", tenant)
                .ok_or_else(|| fail(Exit::NotFound, format!("no tenant {tenant:?}")))?;
            print_one(out, &found)
        }
    }
}

pub(crate) async fn namespace(
    command: &NamespaceCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let tenant = settings.tenant()?;
    let namespaces = api
        .list(&format!("/v1/tenants/{}/namespaces", segment(tenant)), &[])
        .await?;
    match command {
        NamespaceCommand::Ls => print_list(out, "namespaces", NAMESPACE_COLUMNS, namespaces),
        NamespaceCommand::Info { namespace } => {
            let found = find(namespaces, "/namespace", namespace).ok_or_else(|| {
                fail(
                    Exit::NotFound,
                    format!("no namespace {namespace:?} in {tenant}"),
                )
            })?;
            print_one(out, &found)
        }
    }
}

pub(crate) async fn stream(
    command: &StreamCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let base = namespace_path(settings)?;
    match command {
        StreamCommand::Ls => {
            let streams = api.list(&format!("{base}/streams"), &[]).await?;
            print_list(out, "streams", STREAM_COLUMNS, streams)
        }
        StreamCommand::Info { stream } => {
            let found = api
                .get(&format!("{base}/streams/{}", segment(stream)), &[])
                .await?;
            print_one(out, &found)
        }
    }
}

pub(crate) async fn list_caches(settings: &Settings, out: &Output) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let caches = api
        .list(&format!("{}/caches", namespace_path(settings)?), &[])
        .await?;
    print_list(out, "caches", CACHE_COLUMNS, caches)
}

pub(crate) async fn cache_info(
    settings: &Settings,
    cache: &str,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let found = api
        .get(
            &format!("{}/caches/{}", namespace_path(settings)?, segment(cache)),
            &[],
        )
        .await?;
    print_one(out, &found)
}

pub(crate) async fn node(
    command: &NodeCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    match command {
        NodeCommand::Ls => {
            let nodes = api.list("/v1/nodes", &[]).await?;
            print_list(out, "nodes", NODE_COLUMNS, nodes)
        }
        NodeCommand::Info { node } => {
            let found = api
                .get(&format!("/v1/nodes/{}", segment(node)), &[])
                .await?;
            print_one(out, &found)
        }
    }
}

pub(crate) async fn shard(
    command: &ShardCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let ShardCommand::Ls { leader, name } = command;
    let api = Api::new(settings)?;
    let assignments = assignments(&api, leader.as_deref()).await?;
    let assignments = assignments
        .into_iter()
        .filter(|item| name.as_deref().is_none_or(|name| item["stream"] == name))
        .collect();
    print_list(out, "shards", SHARD_COLUMNS, assignments)
}

/// Every shard assignment, optionally only those `leader` leads.
pub(crate) async fn assignments(api: &Api, leader: Option<&str>) -> anyhow::Result<Vec<Value>> {
    let filter: Vec<(&str, &str)> = leader
        .map(|leader| ("leader", leader))
        .into_iter()
        .collect();
    api.list("/v1/shard-assignments", &filter).await
}

/// Split a page into its items and the cursor for the next one.
pub(crate) fn page_parts(page: Value) -> anyhow::Result<(Vec<Value>, Option<String>)> {
    let next = page
        .get("next_cursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
        .map(str::to_string);
    let items = match page.get("items") {
        Some(Value::Array(items)) => items.clone(),
        _ => {
            return Err(fail(
                Exit::Server,
                "a listing answered without an items array",
            ));
        }
    };
    Ok((items, next))
}

/// Listing rows: each column's value read by pointer, blank when absent.
pub(crate) fn rows(columns: &[Column], items: &[Value]) -> Vec<Vec<String>> {
    items
        .iter()
        .map(|item| {
            columns
                .iter()
                .map(|(_, pointer)| item.pointer(pointer).map(cell).unwrap_or_default())
                .collect()
        })
        .collect()
}

fn print_list(
    out: &Output,
    kind: &str,
    columns: &[Column],
    items: Vec<Value>,
) -> anyhow::Result<()> {
    if out.json {
        return out.json_value(&serde_json::json!({ kind: items }));
    }
    let headers: Vec<&str> = columns.iter().map(|(header, _)| *header).collect();
    out.text(&table(&headers, rows(columns, &items)))
}

fn print_one(out: &Output, value: &Value) -> anyhow::Result<()> {
    if out.json {
        out.json_value(value)
    } else {
        out.text(&fields(value))
    }
}

fn find(items: Vec<Value>, pointer: &str, wanted: &str) -> Option<Value> {
    items
        .into_iter()
        .find(|item| item.pointer(pointer).and_then(Value::as_str) == Some(wanted))
}

fn namespace_path(settings: &Settings) -> anyhow::Result<String> {
    Ok(format!(
        "/v1/tenants/{}/namespaces/{}",
        segment(settings.tenant()?),
        segment(&settings.namespace)
    ))
}

/// Percent-encode one path segment.
pub(crate) fn segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The `message` of an error answer, or the body itself.
fn error_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .or_else(|| value.get("error"))
                .map(cell)
        })
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| body.trim().to_string())
}

#[cfg(test)]
mod tests;
