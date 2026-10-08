//! Control-plane writes: `create`, `set` and `rm` for tenants, namespaces,
//! streams and caches, `node drain|deregister`, `shard move` and `placement`.
//!
//! Request bodies are built as JSON by pure functions, so the tests can check
//! them without a control plane. Each command prints what changed; with
//! `--json`, the control plane's answer.

use std::io::{BufRead, IsTerminal, Write};

use serde_json::{Value, json};

use crate::cli::{
    Bound, CacheCreateArgs, Confirm, ConsistencyArg, DeliveryArg, KindArg, PlacementCommand,
    RoutingArg, ShardMoveArgs, ShardMoveCommand, ShardRef, StreamCreateArgs, StreamSetArgs,
};
use crate::context::Settings;
use crate::controlplane::{Answer, Api, namespace_path, print_one, segment};
use crate::error::{Exit, fail};
use crate::output::{Output, cell};

pub(crate) async fn create_tenant(
    api: &Api,
    tenant: &str,
    display_name: Option<&str>,
    out: &Output,
) -> anyhow::Result<()> {
    let answer = api
        .post(
            "/v1/tenants",
            &[],
            Some(&named_body("tenant_id", tenant, display_name)),
        )
        .await?;
    print_created(out, &format!("tenant {tenant}"), answer)
}

pub(crate) async fn remove_tenant(
    api: &Api,
    tenant: &str,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    ask(
        &format!("Delete tenant {tenant} and everything in it?"),
        confirm,
    )?;
    api.delete(&format!("/v1/tenants/{}", segment(tenant)), &[])
        .await?;
    print_deleted(out, "tenant", tenant)
}

pub(crate) async fn create_namespace(
    api: &Api,
    settings: &Settings,
    namespace: &str,
    display_name: Option<&str>,
    out: &Output,
) -> anyhow::Result<()> {
    let tenant = settings.tenant()?;
    let answer = api
        .post(
            &format!("/v1/tenants/{}/namespaces", segment(tenant)),
            &[],
            Some(&named_body("namespace", namespace, display_name)),
        )
        .await?;
    print_created(out, &format!("namespace {tenant}/{namespace}"), answer)
}

pub(crate) async fn remove_namespace(
    api: &Api,
    settings: &Settings,
    namespace: &str,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    let tenant = settings.tenant()?;
    let name = format!("{tenant}/{namespace}");
    ask(
        &format!("Delete namespace {name} with its streams and caches?"),
        confirm,
    )?;
    api.delete(
        &format!(
            "/v1/tenants/{}/namespaces/{}",
            segment(tenant),
            segment(namespace)
        ),
        &[],
    )
    .await?;
    print_deleted(out, "namespace", &name)
}

pub(crate) async fn create_stream(
    api: &Api,
    settings: &Settings,
    args: &StreamCreateArgs,
    out: &Output,
) -> anyhow::Result<()> {
    let base = namespace_path(settings)?;
    let answer = api
        .post(&format!("{base}/streams"), &[], Some(&stream_body(args)))
        .await?;
    print_created(
        out,
        &format!("stream {}", qualified(settings, &args.stream)?),
        answer,
    )
}

pub(crate) async fn set_stream(
    api: &Api,
    settings: &Settings,
    args: &StreamSetArgs,
    out: &Output,
) -> anyhow::Result<()> {
    let path = format!(
        "{}/streams/{}",
        namespace_path(settings)?,
        segment(&args.stream)
    );
    // The patch replaces retention whole, so a bound not given is read first
    // and sent back as it was.
    let before = api.get(&path, &[]).await?;
    let answer = api.patch(&path, &stream_patch(args, &before)).await?;
    print_changed(
        out,
        &format!("stream {}", qualified(settings, &args.stream)?),
        &before,
        answer,
    )
}

pub(crate) async fn remove_stream(
    api: &Api,
    settings: &Settings,
    stream: &str,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    let name = qualified(settings, stream)?;
    ask(&format!("Delete stream {name} and its records?"), confirm)?;
    api.delete(
        &format!("{}/streams/{}", namespace_path(settings)?, segment(stream)),
        &[],
    )
    .await?;
    print_deleted(out, "stream", &name)
}

pub(crate) async fn create_cache(
    api: &Api,
    settings: &Settings,
    args: &CacheCreateArgs,
    out: &Output,
) -> anyhow::Result<()> {
    let base = namespace_path(settings)?;
    let answer = api
        .post(&format!("{base}/caches"), &[], Some(&cache_body(args)))
        .await?;
    print_created(
        out,
        &format!("cache {}", qualified(settings, &args.cache)?),
        answer,
    )
}

pub(crate) async fn set_cache(
    api: &Api,
    settings: &Settings,
    cache: &str,
    display_name: &str,
    out: &Output,
) -> anyhow::Result<()> {
    let path = format!("{}/caches/{}", namespace_path(settings)?, segment(cache));
    let before = api.get(&path, &[]).await?;
    let answer = api
        .patch(&path, &json!({ "display_name": display_name }))
        .await?;
    print_changed(
        out,
        &format!("cache {}", qualified(settings, cache)?),
        &before,
        answer,
    )
}

pub(crate) async fn remove_cache(
    api: &Api,
    settings: &Settings,
    cache: &str,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    let name = qualified(settings, cache)?;
    ask(
        &format!("Delete cache {name} and every key in it?"),
        confirm,
    )?;
    api.delete(
        &format!("{}/caches/{}", namespace_path(settings)?, segment(cache)),
        &[],
    )
    .await?;
    print_deleted(out, "cache", &name)
}

/// `node drain` or `node deregister`: `action` is the last path segment.
pub(crate) async fn node_lifecycle(
    api: &Api,
    node: &str,
    action: &str,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    let question = match action {
        "drain" => format!("Drain node {node}? Placement moves its shards away."),
        _ => format!("Deregister node {node}? Its shards fail over once its lease runs out."),
    };
    ask(&question, confirm)?;
    let answer = api
        .post(&format!("/v1/nodes/{}/{action}", segment(node)), &[], None)
        .await?;
    let body = answer.body.unwrap_or(Value::Null);
    if out.json {
        return out.json_value(&body);
    }
    let lifecycle = body
        .pointer("/status/lifecycle")
        .map(cell)
        .unwrap_or_default();
    out.text(&format!("node {node}: {lifecycle}"))
}

pub(crate) async fn shard_move(
    api: &Api,
    settings: &Settings,
    args: &ShardMoveArgs,
    out: &Output,
) -> anyhow::Result<()> {
    let answer = match &args.cancel {
        Some(ShardMoveCommand::Cancel(shard)) => {
            let (path, query) = shard_path("/v1/shard-moves", settings, shard)?;
            api.delete(&path, &query).await?
        }
        None => {
            let body = move_body(settings.tenant()?, &settings.namespace, args)?;
            api.post("/v1/shard-moves", &[], Some(&body)).await?
        }
    };
    print_move(out, answer)
}

pub(crate) async fn placement(
    command: &PlacementCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    if let PlacementCommand::Abandon { confirm, .. } = command
        && !confirm.yes
    {
        // Never a prompt: a keystroke is too easy a way to lose acknowledged
        // records.
        return Err(fail(
            Exit::Usage,
            "abandoning a shard's log loses its records; pass --yes to do it",
        ));
    }
    let api = Api::new(settings)?;
    let (path, done) = match command {
        PlacementCommand::Pause => ("/v1/placement/pause", "placement's moves are paused"),
        PlacementCommand::Resume => ("/v1/placement/resume", "placement's moves are running"),
        PlacementCommand::Abandon { shard, .. } => {
            let (path, query) = shard_path("/v1/placement/abandon", settings, shard)?;
            let answer = api.post(&path, &query, None).await?;
            return print_move(out, answer);
        }
    };
    let answer = api.post(path, &[], None).await?;
    out.done(done, answer.body.unwrap_or(Value::Null))
}

/// The body that creates a tenant or namespace: its id under `key`, and a
/// display name that defaults to the id.
pub(crate) fn named_body(key: &str, id: &str, display_name: Option<&str>) -> Value {
    json!({ key: id, "display_name": display_name.unwrap_or(id) })
}

pub(crate) fn stream_body(args: &StreamCreateArgs) -> Value {
    let mut body = json!({
        "stream": args.stream,
        "kind": kind(args.kind),
        "shards": args.shards,
        "replication_factor": args.replication,
        "retention": {
            "max_age_seconds": args.retention_secs,
            "max_size_bytes": args.retention_bytes,
        },
        "consistency": consistency(args.consistency),
        "delivery": delivery(args.delivery),
        "durable": args.durable,
    });
    if let Some(region) = &args.region {
        body["region"] = json!(region);
    }
    // Left out when it is the default, so a control plane that predates
    // routing choices still accepts the request.
    if args.routing != RoutingArg::Modulo {
        body["routing"] = json!(routing(args.routing));
    }
    body
}

/// The patch `stream set` sends, given the stream as it is now.
pub(crate) fn stream_patch(args: &StreamSetArgs, current: &Value) -> Value {
    let mut patch = json!({});
    if let Some(level) = args.consistency {
        patch["consistency"] = json!(consistency(level));
    }
    if let Some(guarantee) = args.delivery {
        patch["delivery"] = json!(delivery(guarantee));
    }
    if let Some(durable) = args.durable {
        patch["durable"] = json!(durable);
    }
    if args.retention_secs.is_some() || args.retention_bytes.is_some() {
        let mut retention = json!({
            "max_age_seconds": current.pointer("/retention/max_age_seconds").cloned().unwrap_or(Value::Null),
            "max_size_bytes": current.pointer("/retention/max_size_bytes").cloned().unwrap_or(Value::Null),
        });
        for (key, bound) in [
            ("max_age_seconds", args.retention_secs),
            ("max_size_bytes", args.retention_bytes),
        ] {
            match bound {
                Some(Bound::Value(value)) => retention[key] = json!(value),
                Some(Bound::BrokerDefault) => retention[key] = Value::Null,
                None => {}
            }
        }
        patch["retention"] = retention;
    }
    patch
}

pub(crate) fn cache_body(args: &CacheCreateArgs) -> Value {
    json!({
        "cache": args.cache,
        "display_name": args.display_name.as_deref().unwrap_or(&args.cache),
        "shards": args.shards,
        "replication_factor": args.replication,
        "consistency": consistency(args.consistency),
    })
}

/// The body that starts a move. clap requires NAME, SHARD and --to unless
/// `cancel` is given.
pub(crate) fn move_body(
    tenant: &str,
    namespace: &str,
    args: &ShardMoveArgs,
) -> anyhow::Result<Value> {
    let (Some(name), Some(shard), Some(to)) = (&args.name, args.shard, &args.to) else {
        return Err(fail(Exit::Usage, "shard move needs NAME, SHARD and --to"));
    };
    Ok(json!({
        "tenant_id": tenant,
        "namespace": namespace,
        "stream": name,
        "shard": shard,
        "kind": shard_kind(args.cache),
        "destination": to,
        "dry_run": args.dry_run,
    }))
}

/// `prefix/{tenant}/{namespace}/{name}/{shard}`, and the query naming its kind.
pub(crate) fn shard_path(
    prefix: &str,
    settings: &Settings,
    shard: &ShardRef,
) -> anyhow::Result<(String, Vec<(&'static str, &'static str)>)> {
    let path = format!(
        "{prefix}/{}/{}/{}/{}",
        segment(settings.tenant()?),
        segment(&settings.namespace),
        segment(&shard.name),
        shard.shard
    );
    Ok((path, vec![("kind", shard_kind(shard.cache))]))
}

/// `key: before -> after` for each top-level field that differs.
pub(crate) fn changes(before: &Value, after: &Value) -> Vec<String> {
    let Some(after) = after.as_object() else {
        return Vec::new();
    };
    after
        .iter()
        .filter(|(key, value)| before.get(key.as_str()) != Some(*value))
        .map(|(key, value)| {
            let old = before.get(key.as_str()).map(cell).unwrap_or_default();
            format!("{key}: {old} -> {}", cell(value))
        })
        .collect()
}

/// Go ahead only with `--yes`, or when someone at a terminal says yes.
fn ask(question: &str, confirm: Confirm) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let interactive = stdin.is_terminal();
    confirm_with(question, confirm, interactive, || {
        let mut stderr = std::io::stderr().lock();
        write!(stderr, "{question} [y/N] ")?;
        stderr.flush()?;
        let mut answer = String::new();
        stdin.lock().read_line(&mut answer)?;
        Ok(answer)
    })
}

/// [`ask`] with the terminal check and the prompt passed in.
pub(crate) fn confirm_with(
    question: &str,
    confirm: Confirm,
    interactive: bool,
    prompt: impl FnOnce() -> std::io::Result<String>,
) -> anyhow::Result<()> {
    if confirm.yes {
        return Ok(());
    }
    if !interactive {
        return Err(fail(
            Exit::Usage,
            format!("{question} Pass --yes to confirm; stdin is not a terminal to ask on"),
        ));
    }
    let answer =
        prompt().map_err(|err| fail(Exit::Failure, format!("read the confirmation: {err}")))?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(()),
        _ => Err(fail(Exit::Failure, "not confirmed; nothing was changed")),
    }
}

fn print_created(out: &Output, what: &str, answer: Answer) -> anyhow::Result<()> {
    let body = answer.body.unwrap_or(Value::Null);
    if out.json {
        return out.json_value(&body);
    }
    // A stream or cache that already exists with the same settings is
    // answered 200, not refused.
    let verb = if answer.created {
        format!("created {what}")
    } else {
        format!("{what} already exists with these settings")
    };
    out.text(&verb)?;
    print_one(out, &body)
}

fn print_changed(out: &Output, what: &str, before: &Value, answer: Answer) -> anyhow::Result<()> {
    let after = answer.body.unwrap_or(Value::Null);
    if out.json {
        return out.json_value(&after);
    }
    let changed = changes(before, &after);
    if changed.is_empty() {
        return out.text(&format!("{what}: nothing changed"));
    }
    out.text(&format!("updated {what}\n{}", changed.join("\n")))
}

fn print_deleted(out: &Output, kind: &str, name: &str) -> anyhow::Result<()> {
    out.done(
        &format!("deleted {kind} {name}"),
        json!({ "deleted": kind, "name": name }),
    )
}

fn print_move(out: &Output, answer: Answer) -> anyhow::Result<()> {
    let body = answer.body.unwrap_or(Value::Null);
    if out.json {
        return out.json_value(&body);
    }
    let field = |pointer: &str| body.pointer(pointer).map(cell).unwrap_or_default();
    let mut text = format!(
        "{}/{} shard {}: {}\nleader {}, replicas {}, generation {}",
        field("/assignment/kind"),
        field("/assignment/stream"),
        field("/assignment/shard"),
        field("/step"),
        field("/assignment/leader"),
        field("/assignment/replicas"),
        field("/assignment/generation"),
    );
    if let (Some(before), Some(after)) = (body.get("zones_before"), body.get("zones_after")) {
        text.push_str(&format!("\nzones {} -> {}", cell(before), cell(after)));
    }
    if body["dry_run"] == true {
        text.push_str("\n(dry run: nothing was written)");
    }
    out.text(&text)
}

fn qualified(settings: &Settings, name: &str) -> anyhow::Result<String> {
    Ok(format!(
        "{}/{}/{name}",
        settings.tenant()?,
        settings.namespace
    ))
}

fn shard_kind(cache: bool) -> &'static str {
    if cache { "cache" } else { "stream" }
}

fn kind(kind: KindArg) -> &'static str {
    match kind {
        KindArg::Stream => "Stream",
        KindArg::Queue => "Queue",
    }
}

fn consistency(level: ConsistencyArg) -> &'static str {
    match level {
        ConsistencyArg::Leader => "Leader",
        ConsistencyArg::Quorum => "Quorum",
    }
}

fn delivery(guarantee: DeliveryArg) -> &'static str {
    match guarantee {
        DeliveryArg::AtMostOnce => "AtMostOnce",
        DeliveryArg::AtLeastOnce => "AtLeastOnce",
    }
}

fn routing(routing: RoutingArg) -> &'static str {
    match routing {
        RoutingArg::Modulo => "modulo",
        RoutingArg::JumpHash => "jump_hash",
    }
}

#[cfg(test)]
mod tests;
