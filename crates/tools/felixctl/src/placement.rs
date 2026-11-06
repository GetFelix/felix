//! What placement is doing and will do: `shard moves`, `placement plan` and
//! `placement replication`. Pause, resume and abandon are in `manage`.

use serde_json::Value;

use crate::cli::PlacementCommand;
use crate::context::Settings;
use crate::controlplane::Api;
use crate::manage;
use crate::output::{Output, cell, table};

pub(crate) async fn run(
    command: &PlacementCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    match command {
        PlacementCommand::Plan => {
            let api = Api::new(settings)?;
            let plan = api.get("/v1/placement/plan", &[]).await?;
            show(out, &plan, render_plan)
        }
        PlacementCommand::Replication => {
            let api = Api::new(settings)?;
            let short = api
                .get("/v1/placement/replication", &[("under_replicated", "true")])
                .await?;
            show(out, &short, render_replication)
        }
        PlacementCommand::Pause | PlacementCommand::Resume | PlacementCommand::Abandon { .. } => {
            manage::placement(command, settings, out).await
        }
    }
}

/// `shard moves`.
pub(crate) async fn moves(api: &Api, out: &Output) -> anyhow::Result<()> {
    let moves = api.get("/v1/shard-moves", &[]).await?;
    show(out, &moves, render_moves)
}

/// The API's answer under `--json`, else `render` of it.
fn show(out: &Output, response: &Value, render: fn(&Value) -> String) -> anyhow::Result<()> {
    if out.json {
        out.json_value(response)
    } else {
        out.text(&render(response))
    }
}

fn items(response: &Value) -> &[Value] {
    response["items"].as_array().map_or(&[], Vec::as_slice)
}

/// `tenant/namespace/name/shard`, with the kind when it is not a stream.
pub(crate) fn shard_name(item: &Value) -> String {
    let name = format!(
        "{}/{}/{}/{}",
        cell(&item["tenant_id"]),
        cell(&item["namespace"]),
        cell(&item["stream"]),
        cell(&item["shard"]),
    );
    match item["kind"].as_str() {
        None | Some("stream") => name,
        Some(kind) => format!("{name} ({kind})"),
    }
}

/// A cell that is never empty, so a column is not mistaken for a missing one.
fn dash(text: String) -> String {
    if text.is_empty() {
        "-".to_string()
    } else {
        text
    }
}

fn paused_line(response: &Value) -> &'static str {
    if response["paused"] == true {
        "placement is paused: it starts no moves of its own\n"
    } else {
        ""
    }
}

pub(crate) fn render_moves(response: &Value) -> String {
    let mut out = paused_line(response).to_string();
    let items = items(response);
    if items.is_empty() {
        out.push_str("no moves in progress");
        return out;
    }
    let rows = items
        .iter()
        .map(|item| {
            vec![
                shard_name(item),
                cell(&item["step"]),
                cell(&item["reason"]),
                cell(&item["leader"]),
                cell(&item["destination"]),
                dash(cell(&item["lag_records"])),
                cell(&item["started_at_millis"]),
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

pub(crate) fn render_plan(response: &Value) -> String {
    let mut out = paused_line(response).to_string();
    let items = items(response);
    if items.is_empty() {
        out.push_str("nothing to do");
        return out;
    }
    let rows = items
        .iter()
        .map(|item| {
            let detail = match &item["assignment"] {
                Value::Null => cell(&item["reason"]),
                assignment => format!(
                    "leader {}{}",
                    cell(&assignment["leader"]),
                    assignment["successor"]
                        .as_str()
                        .map(|to| format!(", moving to {to}"))
                        .unwrap_or_default()
                ),
            };
            vec![shard_name(item), cell(&item["action"]), detail]
        })
        .collect();
    out.push_str(&table(&["SHARD", "ACTION", "DETAIL"], rows));
    out
}

pub(crate) fn render_replication(response: &Value) -> String {
    let items = items(response);
    if items.is_empty() {
        return "every shard has its replication factor".to_string();
    }
    let rows = items
        .iter()
        .map(|item| {
            let halted = item["halted"]
                .as_array()
                .map(|halts| {
                    halts
                        .iter()
                        .map(|halt| {
                            format!("{} ({})", cell(&halt["node_id"]), cell(&halt["reason"]))
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            vec![
                shard_name(item),
                cell(&item["leader"]),
                format!(
                    "{}/{}",
                    cell(&item["current_replicas"]),
                    cell(&item["desired_replicas"])
                ),
                dash(cell(&item["unavailable"])),
                dash(halted),
                dash(cell(&item["restoring"])),
            ]
        })
        .collect();
    table(
        &[
            "SHARD",
            "LEADER",
            "COPIES",
            "UNAVAILABLE",
            "HALTED",
            "RESTORING",
        ],
        rows,
    )
}

#[cfg(test)]
mod tests;
