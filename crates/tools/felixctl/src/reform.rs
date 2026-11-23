//! `raft reform plan` and `raft reform apply`: a read-only re-form plan for
//! Raft groups that lost their majority, and carrying one out
//! (`docs/per-shard-raft.md`, "Re-forming a group", "Applying a plan").

use std::time::Duration;

use serde_json::{Value, json};

use crate::cli::{RaftCommand, ReformApplyArgs, ReformCommand, ReformPlanArgs};
use crate::context::Settings;
use crate::controlplane::{Api, segment};
use crate::error::{Exit, fail};
use crate::output::{Output, cell, table};

/// How often a surveying plan is read again.
const POLL: Duration = Duration::from_secs(1);

/// How long to wait for the survey. A plan settles within its rounds, each
/// at most the control plane's 30 s round timeout, so this only bounds a
/// control plane that stopped answering.
const WAIT: Duration = Duration::from_secs(300);

pub(crate) async fn run(
    command: &RaftCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let RaftCommand::Reform(command) = command;
    let args = match command {
        ReformCommand::Plan(args) => args,
        ReformCommand::Apply(args) => return apply(args, settings, out).await,
    };
    let api = Api::new(settings)?;
    let body = plan_body(args)?;
    let created = api.post("/v1/raft/reform/plans", &[], Some(&body)).await?;
    let id = created
        .body
        .as_ref()
        .and_then(|body| body["plan_id"].as_str())
        .ok_or_else(|| fail(Exit::Server, "the control plane answered without a plan id"))?
        .to_string();
    let path = format!("/v1/raft/reform/plans/{}", segment(&id));
    let deadline = tokio::time::Instant::now() + WAIT;
    let plan = loop {
        // A 404 here means the control-plane leader changed and took the
        // plan with it; the error says to plan again.
        let plan = api.get(&path, &[]).await?;
        if plan["state"] != "surveying" || tokio::time::Instant::now() >= deadline {
            break plan;
        }
        tokio::time::sleep(POLL).await;
    };
    if out.json {
        out.json_value(&plan)?;
    } else {
        out.text(&render_plan(&plan))?;
    }
    // A script must not take an unfinished plan for a finished one.
    if plan["state"] == "surveying" {
        return Err(fail(
            Exit::Server,
            "the survey did not finish in time; read the plan again or plan again",
        ));
    }
    Ok(())
}

async fn apply(args: &ReformApplyArgs, settings: &Settings, out: &Output) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let body = apply_body(args)?;
    let path = format!("/v1/raft/reform/plans/{}", segment(&args.plan_id));
    // A 409 says why: no such plan on the leader, or it is still surveying.
    api.post(&format!("{path}/apply"), &[], Some(&body)).await?;
    let deadline = tokio::time::Instant::now() + WAIT;
    let plan = loop {
        let plan = api.get(&path, &[]).await?;
        if !applying(&plan) || tokio::time::Instant::now() >= deadline {
            break plan;
        }
        tokio::time::sleep(POLL).await;
    };
    if out.json {
        out.json_value(&plan)?;
    } else {
        out.text(&render_apply(&plan))?;
    }
    if applying(&plan) {
        return Err(fail(
            Exit::Server,
            "the apply did not finish in time; read the plan again with `raft reform plan` \
             or apply it again",
        ));
    }
    Ok(())
}

/// The apply request for `--accept-loss`.
pub(crate) fn apply_body(args: &ReformApplyArgs) -> anyhow::Result<Value> {
    let keys = args
        .accept_loss
        .iter()
        .map(|name| shard_key(name))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({ "accept_loss": keys }))
}

/// `TENANT/NAMESPACE/STREAM/SHARD`, with `cache:` in front for a cache shard.
fn shard_key(name: &str) -> anyhow::Result<Value> {
    let (kind, rest) = match name.strip_prefix("cache:") {
        Some(rest) => ("cache", rest),
        None => ("stream", name),
    };
    let bad = || {
        fail(
            Exit::Usage,
            format!("--accept-loss {name}: name a shard as TENANT/NAMESPACE/STREAM/SHARD"),
        )
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let [tenant, namespace, stream, shard] = parts[..] else {
        return Err(bad());
    };
    let shard: u32 = shard.parse().map_err(|_| bad())?;
    if [tenant, namespace, stream]
        .iter()
        .any(|part| part.is_empty())
    {
        return Err(bad());
    }
    Ok(json!({
        "tenant_id": tenant, "namespace": namespace, "stream": stream,
        "shard": shard, "kind": kind,
    }))
}

/// Whether some shard's apply is still in a round.
fn applying(plan: &Value) -> bool {
    shards(plan).iter().any(|shard| {
        matches!(
            shard["apply"]["state"].as_str(),
            Some("stopping" | "writing" | "starting")
        )
    })
}

fn shards(plan: &Value) -> &[Value] {
    plan["shards"].as_array().map(Vec::as_slice).unwrap_or(&[])
}

/// One row per shard the apply touched.
pub(crate) fn render_apply(plan: &Value) -> String {
    let applied: Vec<&Value> = shards(plan)
        .iter()
        .filter(|shard| shard["apply"].is_object())
        .collect();
    let mut out = format!(
        "plan {}: {} shards applied",
        cell(&plan["plan_id"]),
        applied.len()
    );
    if applied.is_empty() {
        out.push_str("\nno shard in the plan is marked `reform`");
        return out;
    }
    let rows = applied
        .iter()
        .map(|shard| {
            let apply = &shard["apply"];
            let waiting: Vec<String> = apply["waiting_on"]
                .as_array()
                .into_iter()
                .flatten()
                .map(cell)
                .collect();
            let note = match apply["reason"].as_str() {
                Some(reason) => reason.to_string(),
                None if !waiting.is_empty() => format!("waiting on {}", waiting.join(", ")),
                None => String::new(),
            };
            vec![
                row(shard)[0].clone(),
                cell(&apply["state"]),
                if apply["epoch"].is_null() {
                    "-".to_string()
                } else {
                    cell(&apply["epoch"])
                },
                note,
            ]
        })
        .collect();
    out.push('\n');
    out.push_str(&table(&["SHARD", "APPLY", "NEW EPOCH", "NOTE"], rows));
    out
}

/// The create request for `--scope`.
pub(crate) fn plan_body(args: &ReformPlanArgs) -> anyhow::Result<Value> {
    let scope = match args.scope.as_slice() {
        [scope] if scope == "cluster" => json!("cluster"),
        [scope, node] if scope == "node" => json!({ "node": node }),
        _ => {
            return Err(fail(Exit::Usage, "--scope is `cluster` or `node <ID>`"));
        }
    };
    Ok(json!({ "scope": scope }))
}

/// A summary line, one row per shard, then what each re-form would drop.
pub(crate) fn render_plan(plan: &Value) -> String {
    let shards = plan["shards"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let count = |state: &str| shards.iter().filter(|s| s["state"] == state).count();
    let scope = match &plan["scope"] {
        Value::Object(scope) => format!("node {}", cell(&scope["node"])),
        scope => cell(scope),
    };
    let mut out = format!(
        "plan {} ({scope}): {}; {} to re-form, {} blocked, {} left alone",
        cell(&plan["plan_id"]),
        if plan["state"] == "surveying" {
            "still surveying"
        } else {
            "done"
        },
        count("reform"),
        count("blocked"),
        count("left_alone"),
    );
    if shards.is_empty() {
        out.push_str("\nno Raft shard in scope");
        return out;
    }
    let rows = shards.iter().map(row).collect();
    out.push('\n');
    out.push_str(&table(
        &["SHARD", "EPOCH", "STATE", "SOURCE", "VOTERS", "NOTE"],
        rows,
    ));
    let marked: Vec<&Value> = shards
        .iter()
        .filter(|s| s["plan"]["needs_accept_loss"] == true)
        .collect();
    if !marked.is_empty() {
        out.push_str(&format!(
            "\n{} of these could lose acknowledged records; applying them will need --accept-loss",
            marked.len()
        ));
    }
    out
}

fn row(shard: &Value) -> Vec<String> {
    let key = &shard["key"];
    let mut name = format!(
        "{}/{}/{}/{}",
        cell(&key["tenant_id"]),
        cell(&key["namespace"]),
        cell(&key["stream"]),
        cell(&key["shard"]),
    );
    if key["kind"] == "cache" {
        name.push_str(" (cache)");
    }
    let plan = &shard["plan"];
    let (source, voters) = if plan.is_object() {
        let mut source = cell(&plan["source_broker"]);
        if let Some(kept) = plan["source_kept"].as_str() {
            source.push_str(&format!(" (kept copy {kept})"));
        }
        let voters: Vec<String> = plan["voters"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|voter| cell(&voter[0]))
            .collect();
        (source, voters.join(", "))
    } else {
        ("-".to_string(), "-".to_string())
    };
    vec![
        name,
        cell(&shard["epoch"]),
        cell(&shard["state"]),
        source,
        voters,
        note(shard),
    ]
}

/// Why a shard is blocked or left alone, who a survey waits on, or what a
/// re-form could lose.
fn note(shard: &Value) -> String {
    if let Some(reason) = shard["reason"].as_str() {
        return reason.to_string();
    }
    let plan = &shard["plan"];
    if !plan.is_object() {
        let waiting: Vec<String> = shard["waiting_on"]
            .as_array()
            .into_iter()
            .flatten()
            .map(cell)
            .collect();
        return if waiting.is_empty() {
            String::new()
        } else {
            format!("waiting on {}", waiting.join(", "))
        };
    }
    let loses = plan["needs_accept_loss"] == true;
    let mut notes = vec![if loses {
        let why = plan["marked"].as_str().unwrap_or(
            "a vote or the floor is above every entry read, which cuts a copy below what it holds",
        );
        format!("MAY LOSE ACKNOWLEDGED RECORDS: {why}")
    } else {
        "keeps every acknowledged record".to_string()
    }];
    // Unmarked, what a copy holds past the base was never committed.
    let what = if loses {
        "entries"
    } else {
        "uncommitted entries"
    };
    for dropped in plan["dropped"].as_array().into_iter().flatten() {
        notes.push(format!(
            "drops {}'s {what} {}-{}",
            cell(&dropped[0]),
            cell(&dropped[1]),
            cell(&dropped[2]),
        ));
    }
    notes.join("; ")
}

#[cfg(test)]
mod tests;
