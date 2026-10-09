//! `felixctl inspect segments`: a broker's data directory, read from disk.
//!
//! The verdict is `felix_storage::inspect`'s, which is the plan startup
//! recovery makes before it writes anything. Nothing here opens a file for
//! writing.

use felix_storage::inspect::{
    Action, IndexState, SegmentReport, ShardDir, ShardReport, Startup, Store, find_shards,
    inspect_shard, shard_dir,
};
use felix_storage::log::{LogConfig, ShardKey};
use serde_json::{Value, json};

use crate::cli::{InspectSegmentsArgs, StoreKind};
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, table};

pub(crate) fn run(args: &InspectSegmentsArgs, out: &Output) -> anyhow::Result<()> {
    let config = log_config(args);
    let shards = select(args)?;
    let single = args.shard.is_some();

    let mut reports = Vec::with_capacity(shards.len());
    for shard in &shards {
        let report = inspect_shard(&shard.path, &config)
            .mark(Exit::Failure, format!("read {}", shard.path.display()))?;
        reports.push(shard_json(shard, &report));
    }

    if out.json {
        for report in &reports {
            out.json_value(report)?;
        }
    } else {
        out.text(&render(&reports, single || args.segments))?;
    }
    verdict(&reports)
}

/// The broker settings the verdict depends on, as given.
fn log_config(args: &InspectSegmentsArgs) -> LogConfig {
    let defaults = LogConfig::default();
    LogConfig {
        index_spacing_bytes: args.index_spacing.unwrap_or(defaults.index_spacing_bytes),
        repair_checksum_tail: args.repair_checksum_tail,
        verify_all_on_open: args.verify_all_on_open,
        ..defaults
    }
}

fn store(kind: StoreKind) -> Store {
    match kind {
        StoreKind::Stream => Store::Stream,
        StoreKind::Cache => Store::Cache,
        StoreKind::Groups => Store::Groups,
        StoreKind::DeadLetters => Store::DeadLetters,
        StoreKind::Counters => Store::Counters,
    }
}

/// The shard directories the arguments name.
fn select(args: &InspectSegmentsArgs) -> anyhow::Result<Vec<ShardDir>> {
    let data = &args.data_dir;
    if !data.is_dir() {
        return Err(fail(
            Exit::NotFound,
            format!("{} is not a directory", data.display()),
        ));
    }
    if let Some(given) = &args.shard {
        let key = parse_shard(given)?;
        let store = store(args.kind.unwrap_or(StoreKind::Stream));
        let path = shard_dir(data, store, &key);
        if !path.is_dir() {
            return Err(fail(
                Exit::NotFound,
                format!(
                    "no {} shard {given} in {} (looked for {})",
                    store.name(),
                    data.display(),
                    path.display()
                ),
            ));
        }
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        return Ok(vec![ShardDir { store, name, path }]);
    }
    let mut shards = find_shards(data).mark(Exit::Failure, format!("list {}", data.display()))?;
    if let Some(kind) = args.kind {
        shards.retain(|shard| shard.store == store(kind));
    }
    Ok(shards)
}

/// `TENANT/NAMESPACE/NAME/SHARD`.
pub(crate) fn parse_shard(given: &str) -> anyhow::Result<ShardKey> {
    let parts: Vec<&str> = given.split('/').collect();
    match parts.as_slice() {
        [tenant, namespace, name, shard]
            if !tenant.is_empty() && !namespace.is_empty() && !name.is_empty() =>
        {
            let shard = shard.parse().map_err(|_| {
                fail(
                    Exit::Usage,
                    format!("{shard:?} in {given:?} is not a shard number"),
                )
            })?;
            Ok(ShardKey {
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
                stream: name.to_string(),
                shard,
            })
        }
        _ => Err(fail(
            Exit::Usage,
            format!("{given:?} is not TENANT/NAMESPACE/NAME/SHARD"),
        )),
    }
}

/// One shard, as `--json` prints it.
pub(crate) fn shard_json(shard: &ShardDir, report: &ShardReport) -> Value {
    let startup = match &report.startup {
        Startup::Clean => json!({ "verdict": "clean" }),
        Startup::Repair => json!({ "verdict": "repair" }),
        Startup::Refuse(detail) => json!({
            "verdict": "refuse",
            "segment": detail.site.segment,
            "position": detail.site.position,
            "detail": detail.kind.to_string(),
        }),
    };
    json!({
        "store": shard.store.name(),
        "dir": shard.name,
        "path": shard.path.display().to_string(),
        "startup": startup,
        "actions": report.actions.iter().map(action_json).collect::<Vec<_>>(),
        "durable_mark": report.durable_mark.map(|(segment, synced_bytes)| json!({
            "segment": segment,
            "synced_bytes": synced_bytes,
        })),
        "segments": report.segments.iter().map(segment_json).collect::<Vec<_>>(),
    })
}

fn action_json(action: &Action) -> Value {
    match action {
        Action::RemoveSegment { segment, reason } => json!({
            "action": "remove_segment", "segment": segment, "reason": reason,
        }),
        Action::RebuildIndex { segment, reason } => json!({
            "action": "rebuild_index", "segment": segment, "reason": reason,
        }),
        Action::CutRetired {
            segment,
            valid_bytes,
            drops_next,
        } => json!({
            "action": "cut_retired_segment",
            "segment": segment,
            "to_bytes": valid_bytes,
            "removes_segment": drops_next,
        }),
        Action::TruncateTail {
            segment,
            position,
            discarded_bytes,
            cause,
        } => json!({
            "action": "truncate_tail",
            "segment": segment,
            "position": position,
            "discarded_bytes": discarded_bytes,
            "cause": cause.to_string(),
        }),
        Action::CreateFirstSegment => json!({ "action": "create_first_segment" }),
    }
}

fn segment_json(segment: &SegmentReport) -> Value {
    json!({
        "id": segment.id,
        "base_offset": segment.base_offset,
        "next_offset": segment.next_offset,
        "records": segment.records,
        "size_bytes": segment.size_bytes,
        "version": segment.version,
        "created_at_micros": segment.created_at_micros,
        "index": match segment.index {
            IndexState::Matches => "matches",
            IndexState::Behind => "behind",
            IndexState::Missing => "missing",
            IndexState::Stale => "stale",
            IndexState::Unknown => "unknown",
        },
        "damage": segment.damage.as_ref().map(|damage| json!({
            "position": damage.position,
            "cause": damage.cause.to_string(),
            "tail": damage.tail,
        })),
    })
}

/// Damage the startup verdict does not account for: a record that fails its
/// checksum where startup does not look. Segments startup removes are left
/// out; their damage is why they go.
fn unexplained_damage(report: &Value) -> bool {
    if report["startup"]["verdict"] == "refuse" {
        return false;
    }
    let removed: Vec<&Value> = report["actions"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|action| match text(&action["action"]).as_str() {
            "remove_segment" => vec![&action["segment"]],
            "cut_retired_segment" => vec![&action["removes_segment"]],
            _ => vec![],
        })
        .collect();
    report["segments"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|segment| segment["damage"]["tail"] == false && !removed.contains(&&segment["id"]))
}

fn has_findings(report: &Value) -> bool {
    report["startup"]["verdict"] != "clean"
        || unexplained_damage(report)
        || report["segments"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|segment| segment["index"] == "missing" || segment["index"] == "stale")
}

/// The human form: a line per shard, then the segments of those with
/// findings (or of all, with `all_segments`).
pub(crate) fn render(reports: &[Value], all_segments: bool) -> String {
    if reports.is_empty() {
        return "no shards found".to_string();
    }
    let rows = reports
        .iter()
        .map(|report| {
            let segments = report["segments"].as_array().cloned().unwrap_or_default();
            let records: u64 = segments
                .iter()
                .filter_map(|segment| segment["records"].as_u64())
                .sum();
            let bytes: u64 = segments
                .iter()
                .filter_map(|segment| segment["size_bytes"].as_u64())
                .sum();
            vec![
                text(&report["store"]),
                text(&report["dir"]),
                segments.len().to_string(),
                records.to_string(),
                bytes.to_string(),
                summary(report),
            ]
        })
        .collect();
    let mut out = vec![table(
        &["STORE", "SHARD", "SEGMENTS", "RECORDS", "BYTES", "STARTUP"],
        rows,
    )];
    for report in reports {
        if !(all_segments || has_findings(report)) {
            continue;
        }
        out.push(String::new());
        out.push(details(report));
    }
    let live_tail = reports.iter().any(|report| {
        report["actions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|action| action["action"] == "truncate_tail")
    });
    if live_tail {
        out.push(String::new());
        out.push(
            "note: if a broker is running on this directory, a torn tail in an active \
             segment may be an append in flight"
                .to_string(),
        );
    }
    out.join("\n")
}

/// The STARTUP column.
fn summary(report: &Value) -> String {
    let startup = &report["startup"];
    let verdict = match text(&startup["verdict"]).as_str() {
        "refuse" => format!(
            "refuse: segment {} at byte {}: {}",
            startup["segment"],
            startup["position"],
            text(&startup["detail"])
        ),
        "repair" => {
            let repairs: Vec<String> = report["actions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|action| action["action"] != "rebuild_index")
                .map(describe)
                .collect();
            format!("repair: {}", repairs.join("; "))
        }
        _ => "clean".to_string(),
    };
    if unexplained_damage(report) {
        format!("{verdict} (records fail their checksum, see below)")
    } else {
        verdict
    }
}

/// One planned write in words.
fn describe(action: &Value) -> String {
    let segment = &action["segment"];
    match text(&action["action"]).as_str() {
        "remove_segment" => format!("remove segment {segment}: {}", text(&action["reason"])),
        "rebuild_index" => format!(
            "rebuild the index of segment {segment}: {}",
            text(&action["reason"])
        ),
        "cut_retired_segment" => {
            let mut line = format!(
                "cut segment {segment} to {} bytes, a seal that never finished",
                action["to_bytes"]
            );
            if !action["removes_segment"].is_null() {
                line.push_str(&format!(
                    ", and remove segment {}, which no longer follows on",
                    action["removes_segment"]
                ));
            }
            line
        }
        "truncate_tail" => format!(
            "cut the torn tail of segment {segment}: {} bytes at byte {} ({})",
            action["discarded_bytes"],
            action["position"],
            text(&action["cause"])
        ),
        "create_first_segment" => "start an empty log".to_string(),
        other => other.to_string(),
    }
}

/// The segments of one shard and what startup would do to it.
fn details(report: &Value) -> String {
    let mut out = vec![format!(
        "{} ({})  {}",
        text(&report["dir"]),
        text(&report["store"]),
        text(&report["path"])
    )];
    let rows: Vec<Vec<String>> = report["segments"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|segment| {
            let check = match segment.get("damage") {
                Some(damage) if !damage.is_null() => format!(
                    "{} at byte {}: {}",
                    if damage["tail"] == true {
                        "torn tail"
                    } else {
                        "damaged"
                    },
                    damage["position"],
                    text(&damage["cause"])
                ),
                _ => "ok".to_string(),
            };
            vec![
                text(&segment["id"]),
                dash(&segment["base_offset"]),
                dash(&segment["next_offset"]),
                dash(&segment["records"]),
                text(&segment["size_bytes"]),
                text(&segment["index"]),
                check,
            ]
        })
        .collect();
    if rows.is_empty() {
        out.push("  no segments".to_string());
    } else {
        let table = table(
            &[
                "SEGMENT",
                "BASE",
                "NEXT",
                "RECORDS",
                "BYTES",
                "INDEX",
                "RECORDS CHECK",
            ],
            rows,
        );
        out.extend(table.lines().map(|line| format!("  {line}")));
    }
    let startup = &report["startup"];
    if startup["verdict"] == "refuse" {
        out.push(format!(
            "  startup refuses: segment {} at byte {}: {}",
            startup["segment"],
            startup["position"],
            text(&startup["detail"])
        ));
    }
    let actions: Vec<&Value> = report["actions"].as_array().into_iter().flatten().collect();
    if !actions.is_empty() {
        let lead = if startup["verdict"] == "refuse" {
            "  before refusing, startup would:"
        } else {
            "  startup would:"
        };
        out.push(lead.to_string());
        for action in actions {
            out.push(format!("    {}", describe(action)));
        }
    }
    if unexplained_damage(report) {
        out.push(
            "  startup does not check these records, so it opens the shard and reads of them \
             fail; --verify-all-on-open shows what FELIX_DURABLE_VERIFY_ALL_ON_OPEN would do"
                .to_string(),
        );
    }
    out.join("\n")
}

/// The exit status: 7 for anything damaged, else 6 for anything repaired.
fn verdict(reports: &[Value]) -> anyhow::Result<()> {
    let count = |pick: &dyn Fn(&Value) -> bool| reports.iter().filter(|r| pick(r)).count();
    let refused = count(&|r| r["startup"]["verdict"] == "refuse");
    let rotted = count(&unexplained_damage);
    let repaired = count(&|r| r["startup"]["verdict"] == "repair");
    if refused + rotted > 0 {
        let mut parts = Vec::new();
        if refused > 0 {
            parts.push(format!("startup would refuse {}", shards(refused)));
        }
        if rotted > 0 {
            parts.push(format!(
                "{} hold records that fail their checksum",
                shards(rotted)
            ));
        }
        return Err(fail(Exit::Damaged, parts.join("; ")));
    }
    if repaired > 0 {
        return Err(fail(
            Exit::WouldRepair,
            format!("startup would repair {}", shards(repaired)),
        ));
    }
    Ok(())
}

fn shards(count: usize) -> String {
    if count == 1 {
        "1 shard".to_string()
    } else {
        format!("{count} shards")
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn dash(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        other => text(other),
    }
}

#[cfg(test)]
mod tests;
