//! `felix-controlplane admin`: the operator's shard move controls from a
//! shell, as a thin client of the HTTP API (`/v1/shard-moves`,
//! `/v1/placement/*`).
//!
//! A client rather than a direct store connection, so it works against any
//! backend, goes through the same authorization as every other caller, and
//! can run anywhere the API is reachable.
use anyhow::{Context, Result, bail};
use serde_json::Value;

const USAGE: &str = "\
usage: felix-controlplane admin [--url URL] [--token TOKEN] [--json] <command>

commands:
  moves                                    moves in progress
  plan                                     what placement would do next
  move <tenant>/<namespace>/<name>/<shard> <node> [--cache]
                                           move a shard's leadership to <node>
  cancel <tenant>/<namespace>/<name>/<shard> [--cache]
                                           cancel a shard's move
  abandon <tenant>/<namespace>/<name>/<shard> [--cache]
                                           LOSE a shard's unreachable log and
                                           place the shard afresh
  pause                                    stop placement starting moves
  resume                                   let placement start moves again

--url defaults to $FELIX_CONTROLPLANE_URL, then http://127.0.0.1:8443.
--token defaults to $FELIX_TOKEN. Reading takes node.view:cluster:*;
everything else takes node.manage:cluster:*.";

/// Run one `admin` command; `args` starts after the word `admin`.
pub async fn run(args: Vec<String>) -> Result<()> {
    let mut url = std::env::var("FELIX_CONTROLPLANE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8443".to_string());
    let mut token = std::env::var("FELIX_TOKEN").ok();
    let mut json = false;
    let mut cache = false;
    let mut words = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = args.next().context("--url needs a value")?,
            "--token" => token = Some(args.next().context("--token needs a value")?),
            "--json" => json = true,
            "--cache" => cache = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            flag if flag.starts_with("--") => bail!("unknown option {flag}\n\n{USAGE}"),
            _ => words.push(arg),
        }
    }
    let admin = Admin {
        http: reqwest::Client::new(),
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
    format!(
        "{}: {} leader {} generation {}{}\n",
        text(&response["step"]),
        shard_name(assignment),
        text(&assignment["leader"]),
        text(&assignment["generation"]),
        assignment["successor"]
            .as_str()
            .map(|to| format!(", moving to {to}"))
            .unwrap_or_default(),
    )
}

fn render_paused(response: &Value) -> String {
    if response["paused"].as_bool() == Some(true) {
        "placement paused\n".to_string()
    } else {
        "placement resumed\n".to_string()
    }
}

#[cfg(test)]
mod tests;
