//! `felix-broker inspect segments`: a data directory, read from disk.
//!
//! Runs instead of the broker, never alongside it in this process: nothing is
//! bound, no configuration is read, and no file is opened for writing. The
//! verdict is `felix_storage::inspect`'s, which is the plan startup recovery
//! makes before it writes anything, so the two cannot disagree.

use std::path::PathBuf;

use felix_storage::inspect::{
    Action, IndexState, SegmentReport, ShardDir, ShardReport, Startup, Store, find_shards,
    inspect_shard, shard_dir,
};
use felix_storage::log::{LogConfig, ShardKey};
use serde_json::{Value, json};

/// Startup would repair a shard: cut a torn tail, or remove what an
/// interrupted rollover left.
pub const EXIT_WOULD_REPAIR: u8 = 6;
/// Startup would refuse a shard, or a record fails its checksum.
pub const EXIT_DAMAGED: u8 = 7;
/// Bad arguments.
pub const EXIT_USAGE: u8 = 2;
/// The data directory or the named shard is not there.
pub const EXIT_NOT_FOUND: u8 = 5;

const USAGE: &str = "\
usage: felix-broker inspect segments DATA_DIR [TENANT/NAMESPACE/NAME/SHARD]
           [--kind stream|cache|groups|dead-letters|counters] [--segments] [--json]
           [--repair-checksum-tail] [--index-spacing BYTES] [--verify-all-on-open]

Reads a broker's data directory (FELIX_DURABLE_STORAGE_DIR) and reports, for
every shard of every store, its segments, whether each one's records and index
verify, and what startup would do with it: open it as it is, repair it, or
refuse to start, and where. Strictly read-only, and it does not start the
broker. Next to a running broker the reads are safe, but an active segment may
be mid-write.

The verdict depends on three broker settings; pass the values the broker runs
with: --repair-checksum-tail (FELIX_DURABLE_REPAIR_CHECKSUM_TAIL),
--index-spacing (FELIX_DURABLE_INDEX_SPACING_BYTES) and --verify-all-on-open
(FELIX_DURABLE_VERIFY_ALL_ON_OPEN).

A shard you name, shards with findings, and with --segments every shard, get
their segments listed. --json prints one line per shard.

Exits 0 when every shard opens as it is, 6 when startup would repair one, 7
when it would refuse one or a record fails its checksum, 2 for bad arguments
and 5 when the directory or shard is not there.";

/// What `inspect segments` was asked.
#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    data_dir: PathBuf,
    shard: Option<String>,
    kind: Option<Store>,
    segments: bool,
    json: bool,
    repair_checksum_tail: bool,
    index_spacing: Option<u64>,
    verify_all_on_open: bool,
}

/// Run `inspect`; `args` starts after the word `inspect`. Returns the exit
/// status, having printed the report and any error.
pub fn run(args: Vec<String>) -> u8 {
    let args = match parse(args) {
        Ok(Parsed::Run(args)) => args,
        Ok(Parsed::Help) => {
            println!("{USAGE}");
            return 0;
        }
        Err(message) => {
            eprintln!("felix-broker inspect: {message}\n\n{USAGE}");
            return EXIT_USAGE;
        }
    };
    match inspect(&args) {
        Ok((reports, single)) => {
            if args.json {
                for report in &reports {
                    println!("{report}");
                }
            } else {
                println!("{}", render(&reports, single || args.segments));
            }
            let (code, why) = verdict(&reports);
            if let Some(why) = why {
                eprintln!("felix-broker inspect: {why}");
            }
            code
        }
        Err((code, message)) => {
            eprintln!("felix-broker inspect: {message}");
            code
        }
    }
}

enum Parsed {
    Run(Args),
    Help,
}

fn parse(args: Vec<String>) -> Result<Parsed, String> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("segments") => {}
        Some("-h" | "--help") | None => return Ok(Parsed::Help),
        Some(other) => return Err(format!("unknown inspect command {other}")),
    }
    let mut parsed = Args::default();
    let mut positional = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--json" => parsed.json = true,
            "--segments" => parsed.segments = true,
            "--repair-checksum-tail" => parsed.repair_checksum_tail = true,
            "--verify-all-on-open" => parsed.verify_all_on_open = true,
            "--index-spacing" => {
                let value = args.next().ok_or("--index-spacing needs a byte count")?;
                let bytes = value
                    .parse()
                    .map_err(|_| format!("--index-spacing {value:?} is not a byte count"))?;
                parsed.index_spacing = Some(bytes);
            }
            "--kind" => {
                let value = args.next().ok_or("--kind needs a store")?;
                parsed.kind = Some(
                    STORES
                        .iter()
                        .find(|store| store.name() == value)
                        .copied()
                        .ok_or_else(|| format!("--kind {value:?} is not a store"))?,
                );
            }
            flag if flag.starts_with('-') => return Err(format!("unknown argument {flag}")),
            _ => positional.push(arg),
        }
    }
    let mut positional = positional.into_iter();
    parsed.data_dir = positional.next().ok_or("missing DATA_DIR")?.into();
    parsed.shard = positional.next();
    if let Some(extra) = positional.next() {
        return Err(format!("unexpected argument {extra}"));
    }
    Ok(Parsed::Run(parsed))
}

const STORES: [Store; 5] = [
    Store::Stream,
    Store::Cache,
    Store::Groups,
    Store::DeadLetters,
    Store::Counters,
];

/// Every selected shard's report, and whether one shard was named.
fn inspect(args: &Args) -> Result<(Vec<Value>, bool), (u8, String)> {
    let config = log_config(args);
    let shards = select(args)?;
    let mut reports = Vec::with_capacity(shards.len());
    for shard in &shards {
        let report = inspect_shard(&shard.path, &config)
            .map_err(|err| (1, format!("read {}: {err}", shard.path.display())))?;
        reports.push(shard_json(shard, &report));
    }
    Ok((reports, args.shard.is_some()))
}

/// The broker settings the verdict depends on, as given.
fn log_config(args: &Args) -> LogConfig {
    let defaults = LogConfig::default();
    LogConfig {
        index_spacing_bytes: args.index_spacing.unwrap_or(defaults.index_spacing_bytes),
        repair_checksum_tail: args.repair_checksum_tail,
        verify_all_on_open: args.verify_all_on_open,
        ..defaults
    }
}

/// The shard directories the arguments name.
fn select(args: &Args) -> Result<Vec<ShardDir>, (u8, String)> {
    let data = &args.data_dir;
    if !data.is_dir() {
        return Err((
            EXIT_NOT_FOUND,
            format!("{} is not a directory", data.display()),
        ));
    }
    if let Some(given) = &args.shard {
        let key = parse_shard(given).map_err(|message| (EXIT_USAGE, message))?;
        let store = args.kind.unwrap_or(Store::Stream);
        let path = shard_dir(data, store, &key);
        if !path.is_dir() {
            return Err((
                EXIT_NOT_FOUND,
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
    let mut shards =
        find_shards(data).map_err(|err| (1, format!("list {}: {err}", data.display())))?;
    if let Some(kind) = args.kind {
        shards.retain(|shard| shard.store == kind);
    }
    Ok(shards)
}

/// `TENANT/NAMESPACE/NAME/SHARD`.
fn parse_shard(given: &str) -> Result<ShardKey, String> {
    let parts: Vec<&str> = given.split('/').collect();
    match parts.as_slice() {
        [tenant, namespace, name, shard]
            if !tenant.is_empty() && !namespace.is_empty() && !name.is_empty() =>
        {
            let shard = shard
                .parse()
                .map_err(|_| format!("{shard:?} in {given:?} is not a shard number"))?;
            Ok(ShardKey {
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
                stream: name.to_string(),
                shard,
            })
        }
        _ => Err(format!("{given:?} is not TENANT/NAMESPACE/NAME/SHARD")),
    }
}

/// One shard, as `--json` prints it.
fn shard_json(shard: &ShardDir, report: &ShardReport) -> Value {
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
fn render(reports: &[Value], all_segments: bool) -> String {
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

/// The exit status and why: 7 for anything damaged, else 6 for anything
/// repaired, else 0.
fn verdict(reports: &[Value]) -> (u8, Option<String>) {
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
        return (EXIT_DAMAGED, Some(parts.join("; ")));
    }
    if repaired > 0 {
        return (
            EXIT_WOULD_REPAIR,
            Some(format!("startup would repair {}", shards(repaired))),
        );
    }
    (0, None)
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

/// Columns padded to their widest cell. The last column is not padded.
fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }
    let line = |cells: Vec<&str>| -> String {
        let last = cells.len().saturating_sub(1);
        let mut out = String::new();
        for (i, cell) in cells.into_iter().enumerate() {
            out.push_str(cell);
            if i != last {
                out.extend(std::iter::repeat_n(
                    ' ',
                    widths[i] - cell.chars().count() + 2,
                ));
            }
        }
        out
    };
    let mut out = vec![line(headers.to_vec())];
    for row in &rows {
        out.push(line(row.iter().map(String::as_str).collect()));
    }
    out.join("\n")
}

#[cfg(test)]
mod tests;
