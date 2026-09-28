//! `felix-controlplane admin`: the operator's shard move and fleet feature
//! controls from a shell, as a thin client of the HTTP API
//! (`/v1/shard-moves`, `/v1/placement/*`, `/v1/fleet/features`).
//!
//! A client rather than a direct store connection, so it works against any
//! backend, goes through the same authorization as every other caller, and
//! can run anywhere the API is reachable.

pub mod backup_point;

use anyhow::{Context, Result, bail};
use serde_json::Value;

const USAGE: &str = "\
usage: felix-controlplane admin [--url URL] [--token TOKEN] [--json] <command>

commands:
  moves                                    moves in progress
  plan                                     what placement would do next
  move <tenant>/<namespace>/<name>/<shard> <node> [--cache] [--dry-run]
                                           move a shard's leadership to <node>;
                                           --dry-run shows what it would do
  cancel <tenant>/<namespace>/<name>/<shard> [--cache]
                                           cancel a shard's move
  abandon <tenant>/<namespace>/<name>/<shard> [--cache]
                                           LOSE a shard's unreachable log and
                                           place the shard afresh
  pause                                    stop placement starting moves
  resume                                   let placement start moves again
  backup-point <name> [--out FILE] [--broker NODE=URL]... [--metrics-port PORT]
                                           record every shard's committed
                                           offsets as a backup point
  features                                 fleet features: supported by every
                                           serving broker, and enabled
  features finalize <feature> [--dry-run]  enable a feature fleet-wide; ONE-WAY:
                                           brokers without it are refused after

--url defaults to $FELIX_CONTROLPLANE_URL, then http://127.0.0.1:8443.
An https URL is verified against the public roots and, when set, the PEM
bundle at $FELIX_CONTROLPLANE_CA.
--token defaults to $FELIX_TOKEN. Reading, backup-point included, takes
node.view:cluster:*; everything else takes node.manage:cluster:*.";

/// The API client, trusting `FELIX_CONTROLPLANE_CA` when it is set, as the
/// brokers do.
fn http_client() -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(path) = std::env::var("FELIX_CONTROLPLANE_CA")
        .ok()
        .filter(|path| !path.trim().is_empty())
    {
        let pem =
            std::fs::read(&path).with_context(|| format!("read FELIX_CONTROLPLANE_CA {path}"))?;
        for root in reqwest::Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("parse FELIX_CONTROLPLANE_CA {path}"))?
        {
            builder = builder.add_root_certificate(root);
        }
    }
    builder.build().context("build the HTTP client")
}

/// Run one `admin` command; `args` starts after the word `admin`.
pub async fn run(args: Vec<String>) -> Result<()> {
    let mut url = std::env::var("FELIX_CONTROLPLANE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8443".to_string());
    let mut token = std::env::var("FELIX_TOKEN").ok();
    let mut json = false;
    let mut cache = false;
    let mut dry_run = false;
    let mut words = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = args.next().context("--url needs a value")?,
            "--token" => token = Some(args.next().context("--token needs a value")?),
            "--json" => json = true,
            "--cache" => cache = true,
            "--dry-run" => dry_run = true,
            // Takes options of its own, so the rest of the line is its.
            "backup-point" => {
                let admin = Admin {
                    http: http_client()?,
                    url: url.trim_end_matches('/').to_string(),
                    token,
                };
                return backup_point::run(&admin, args.collect()).await;
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            flag if flag.starts_with("--") => bail!("unknown option {flag}\n\n{USAGE}"),
            _ => words.push(arg),
        }
    }
    let admin = Admin {
        http: http_client()?,
        url: url.trim_end_matches('/').to_string(),
        token,
    };
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    let (response, render): (Value, fn(&Value) -> String) = match words.as_slice() {
        ["moves"] => (admin.get("/v1/shard-moves").await?, render_moves),
        ["plan"] => (admin.get("/v1/placement/plan").await?, render_plan),
        ["move", shard, destination] => {
            let key = ShardPath::parse(shard, cache)?;
            let body = serde_json::json!({
                "tenant_id": key.tenant_id,
                "namespace": key.namespace,
                "stream": key.name,
                "shard": key.shard,
                "kind": key.kind(),
                "destination": destination,
                "dry_run": dry_run,
            });
            (
                admin
                    .send(reqwest::Method::POST, "/v1/shard-moves", Some(body))
                    .await?,
                render_step,
            )
        }
        ["cancel", shard] => {
            let key = ShardPath::parse(shard, cache)?;
            let path = format!(
                "/v1/shard-moves/{}/{}/{}/{}?kind={}",
                key.tenant_id,
                key.namespace,
                key.name,
                key.shard,
                key.kind()
            );
            (
                admin.send(reqwest::Method::DELETE, &path, None).await?,
                render_step,
            )
        }
        ["abandon", shard] => {
            let key = ShardPath::parse(shard, cache)?;
            let path = format!(
                "/v1/placement/abandon/{}/{}/{}/{}?kind={}",
                key.tenant_id,
                key.namespace,
                key.name,
                key.shard,
                key.kind()
            );
            (
                admin.send(reqwest::Method::POST, &path, None).await?,
                render_step,
            )
        }
        ["pause"] => (
            admin
                .send(reqwest::Method::POST, "/v1/placement/pause", None)
                .await?,
            render_paused,
        ),
        ["resume"] => (
            admin
                .send(reqwest::Method::POST, "/v1/placement/resume", None)
                .await?,
            render_paused,
        ),
        ["features"] => (admin.get("/v1/fleet/features").await?, render_features),
        ["features", "finalize", feature] => {
            let path = format!("/v1/fleet/features/{feature}/finalize?dry_run={dry_run}");
            (
                admin.send(reqwest::Method::POST, &path, None).await?,
                render_finalize,
            )
        }
        _ => bail!("{USAGE}"),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        print!("{}", render(&response));
    }
    Ok(())
}

struct Admin {
    http: reqwest::Client,
    url: String,
    token: Option<String>,
}

impl Admin {
    async fn get(&self, path: &str) -> Result<Value> {
        self.send(reqwest::Method::GET, path, None).await
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        let mut request = self.http.request(method, format!("{}{path}", self.url));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("reach the control plane at {}", self.url))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            // The API's error body says why; show its message, not the JSON.
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|body| {
                    Some(format!(
                        "{}: {}",
                        body["code"].as_str()?,
                        body["message"].as_str()?
                    ))
                })
                .unwrap_or(text);
            bail!("{status}: {message}");
        }
        serde_json::from_str(&text).context("the control plane answered something other than JSON")
    }
}

/// `tenant/namespace/name/shard`, the way a shard is named on the command line.
#[derive(Debug, PartialEq, Eq)]
struct ShardPath {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    cache: bool,
}

impl ShardPath {
    fn parse(path: &str, cache: bool) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        let [tenant_id, namespace, name, shard] = parts.as_slice() else {
            bail!("name a shard as <tenant>/<namespace>/<name>/<shard>, not {path:?}");
        };
        Ok(Self {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            shard: shard
                .parse()
                .with_context(|| format!("shard number {shard:?}"))?,
            cache,
        })
    }

    fn kind(&self) -> &'static str {
        if self.cache { "cache" } else { "stream" }
    }
}

fn shard_name(item: &Value) -> String {
    let kind = item["kind"].as_str().unwrap_or("stream");
    let name = format!(
        "{}/{}/{}/{}",
        text(&item["tenant_id"]),
        text(&item["namespace"]),
        text(&item["stream"]),
        text(&item["shard"]),
    );
    if kind == "stream" {
        name
    } else {
        format!("{name} ({kind})")
    }
}

/// A JSON value as a table cell: strings bare, absent as `-`.
fn text(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Columns padded to their widest cell, two spaces apart.
fn table(header: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let line = |cells: Vec<String>| {
        let mut out = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect::<Vec<_>>()
            .join("  ");
        out.truncate(out.trim_end().len());
        out.push('\n');
        out
    };
    let mut out = line(header.iter().map(|h| h.to_string()).collect());
    for row in rows {
        out.push_str(&line(row));
    }
    out
}

fn paused_line(response: &Value) -> &'static str {
    if response["paused"].as_bool() == Some(true) {
        "placement is paused: it starts no moves of its own\n"
    } else {
        ""
    }
}

fn render_moves(response: &Value) -> String {
    let items = response["items"].as_array().cloned().unwrap_or_default();
    let mut out = paused_line(response).to_string();
    if items.is_empty() {
        out.push_str("no moves in progress\n");
        return out;
    }
    let rows = items
        .iter()
        .map(|item| {
            vec![
                shard_name(item),
                text(&item["step"]),
                text(&item["reason"]),
                text(&item["leader"]),
                text(&item["destination"]),
                text(&item["lag_records"]),
                text(&item["started_at_millis"]),
            ]
        })
        .collect();
    out.push_str(&table(
        &[
            "SHARD",
            "STEP",
            "REASON",
            "LEADER",
            "DESTINATION",
            "LAG",
            "STARTED_MS",
        ],
        rows,
    ));
    out
}

fn render_plan(response: &Value) -> String {
    let items = response["items"].as_array().cloned().unwrap_or_default();
    let mut out = paused_line(response).to_string();
    if items.is_empty() {
        out.push_str("nothing to do\n");
        return out;
    }
    let rows = items
        .iter()
        .map(|item| {
            let detail = match &item["assignment"] {
                Value::Null => text(&item["reason"]),
                assignment => format!(
                    "leader {}{}",
                    text(&assignment["leader"]),
                    assignment["successor"]
                        .as_str()
                        .map(|to| format!(", moving to {to}"))
                        .unwrap_or_default()
                ),
            };
            vec![shard_name(item), text(&item["action"]), detail]
        })
        .collect();
    out.push_str(&table(&["SHARD", "ACTION", "DETAIL"], rows));
    out
}

fn render_step(response: &Value) -> String {
    let assignment = &response["assignment"];
    let mut out = format!(
        "{}{}: {} leader {} generation {}{}\n",
        if response["dry_run"].as_bool() == Some(true) {
            "dry run, nothing written: "
        } else {
            ""
        },
        text(&response["step"]),
        shard_name(assignment),
        text(&assignment["leader"]),
        text(&assignment["generation"]),
        assignment["successor"]
            .as_str()
            .map(|to| format!(", moving to {to}"))
            .unwrap_or_default(),
    );
    if let (Some(before), Some(after)) = (
        response["zones_before"].as_u64(),
        response["zones_after"].as_u64(),
    ) {
        out.push_str(&format!("zones: {before} -> {after}"));
        if after < before {
            out.push_str(" (the shard's copies will span fewer zones)");
        }
        out.push('\n');
    }
    out
}

fn render_paused(response: &Value) -> String {
    if response["paused"].as_bool() == Some(true) {
        "placement paused\n".to_string()
    } else {
        "placement resumed\n".to_string()
    }
}

fn names(value: &Value) -> String {
    let names: Vec<String> = value
        .as_array()
        .map(|items| items.iter().map(text).collect())
        .unwrap_or_default();
    if names.is_empty() {
        "-".to_string()
    } else {
        names.join(", ")
    }
}

fn render_features(response: &Value) -> String {
    format!(
        "serving brokers: {}\nsupported: {}\nenabled:   {}\n",
        text(&response["serving_nodes"]),
        names(&response["supported"]),
        names(&response["enabled"]),
    )
}

fn render_finalize(response: &Value) -> String {
    let feature = text(&response["feature"]);
    let enabled = response["enabled"].as_bool() == Some(true);
    if response["dry_run"].as_bool() != Some(true) {
        return format!("{feature} enabled; brokers without it will be refused from now on\n");
    }
    if enabled {
        return format!("dry run: {feature} is already enabled\n");
    }
    if response["would_enable"].as_bool() == Some(true) {
        return format!(
            "dry run: {feature} can be finalized; all {} serving brokers support it\n",
            text(&response["serving_nodes"]),
        );
    }
    let lacking = names(&response["lacking"]);
    if lacking == "-" {
        format!("dry run: {feature} cannot be finalized: no broker is serving\n")
    } else {
        format!("dry run: {feature} cannot be finalized: {lacking} do not support it\n")
    }
}

#[cfg(test)]
mod tests;
