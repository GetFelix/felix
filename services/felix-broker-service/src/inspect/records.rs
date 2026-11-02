//! `felix-broker inspect records`: the records around one offset of one shard.
//!
//! The decoding is `felix_storage::inspect::read_records`. This side parses
//! the arguments, prints the rows and picks the exit status.

use std::fmt::Write as _;
use std::path::PathBuf;

use felix_storage::inspect::{
    MAX_AROUND, OffsetStatus, PAYLOAD_PREVIEW_BYTES, RecordView, RecordsQuery, RecordsReport, Stop,
    Store, read_records,
};
use felix_storage::log::RecordMark;
use serde_json::{Value, json};

use super::{
    EXIT_DAMAGED, EXIT_NOT_FOUND, EXIT_WOULD_REPAIR, Parsed, STORES, resolve_shard, table,
};

const DEFAULT_AROUND: u64 = 10;

/// What `inspect records` was asked.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Args {
    data_dir: PathBuf,
    shard: String,
    offset: u64,
    around: u64,
    payload: bool,
    kind: Option<Store>,
    incarnation: Option<u64>,
    json: bool,
}

/// `args` starts after the word `records`.
pub(super) fn parse(args: std::vec::IntoIter<String>) -> Result<Parsed, String> {
    let mut args = args;
    let mut offset = None;
    let mut around = DEFAULT_AROUND;
    let mut payload = false;
    let mut json = false;
    let mut kind = None;
    let mut incarnation = None;
    let mut positional = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--json" => json = true,
            "--payload" => payload = true,
            "--offset" => offset = Some(number(&mut args, "--offset")?),
            "--around" => around = number(&mut args, "--around")?,
            "--incarnation" => incarnation = Some(number(&mut args, "--incarnation")?),
            "--kind" => {
                let value = args.next().ok_or("--kind needs a store")?;
                kind = Some(
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
    let data_dir = positional.next().ok_or("missing DATA_DIR")?.into();
    let shard = positional
        .next()
        .ok_or("missing TENANT/NAMESPACE/NAME/SHARD")?;
    if let Some(extra) = positional.next() {
        return Err(format!("unexpected argument {extra}"));
    }
    Ok(Parsed::Records(Args {
        data_dir,
        shard,
        offset: offset.ok_or("missing --offset")?,
        around,
        payload,
        kind,
        incarnation,
        json,
    }))
}

fn number(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{flag} needs a number"))?;
    value
        .parse()
        .map_err(|_| format!("{flag} {value:?} is not a number"))
}

/// Print the records and return the exit status.
pub(super) fn run(args: &Args) -> u8 {
    match read(args) {
        Ok(report) => {
            if args.json {
                for line in json_lines(&report) {
                    println!("{line}");
                }
            } else {
                println!("{}", render(&report, args.offset, args.payload));
            }
            let (code, notes) = outcome(&report, args.offset);
            for note in notes {
                eprintln!("felix-broker inspect: {note}");
            }
            code
        }
        Err((code, message)) => {
            eprintln!("felix-broker inspect: {message}");
            code
        }
    }
}

fn read(args: &Args) -> Result<RecordsReport, (u8, String)> {
    let data = &args.data_dir;
    if !data.is_dir() {
        return Err((
            EXIT_NOT_FOUND,
            format!("{} is not a directory", data.display()),
        ));
    }
    let shard = resolve_shard(data, &args.shard, args.kind, args.incarnation)?;
    if args.around > MAX_AROUND {
        eprintln!("felix-broker inspect: --around is capped at {MAX_AROUND}");
    }
    let query = RecordsQuery {
        offset: args.offset,
        around: args.around,
        payload: args.payload,
    };
    read_records(&shard.path, &query)
        .map_err(|err| (1, format!("read {}: {err}", shard.path.display())))
}

/// The exit status, and what to tell the operator on stderr. Damage comes
/// first, then an offset that is not in the log, then a torn tail.
fn outcome(report: &RecordsReport, offset: u64) -> (u8, Vec<String>) {
    let mut notes = Vec::new();
    let mut code = 0;
    match report.offset {
        OffsetStatus::Found | OffsetStatus::Unreached => {}
        OffsetStatus::BelowStart { log_start } => {
            code = EXIT_NOT_FOUND;
            notes.push(format!(
                "offset {offset} is before the start of the log: the oldest segment on disk \
                 starts at offset {log_start}"
            ));
        }
        OffsetStatus::PastTail { log_end } => {
            code = EXIT_NOT_FOUND;
            notes.push(match report.log_start.filter(|start| *start < log_end) {
                Some(start) => format!(
                    "offset {offset} is past the end of the log, which holds offsets {start} \
                     to {} (the next record gets offset {log_end})",
                    log_end - 1
                ),
                None => format!("offset {offset} is past the end of the log, which is empty"),
            });
        }
        OffsetStatus::Empty => {
            code = EXIT_NOT_FOUND;
            notes.push("the shard has no segments".to_string());
        }
        OffsetStatus::Missing => {
            code = EXIT_DAMAGED;
            notes.push(format!(
                "no record carries offset {offset}, though it is inside the log's range"
            ));
        }
    }
    if let Some(stop) = &report.stop {
        notes.push(stop_note(stop));
        if !stop.tail {
            code = EXIT_DAMAGED;
        } else if code == 0 {
            code = EXIT_WOULD_REPAIR;
        }
    }
    (code, notes)
}

fn stop_note(stop: &Stop) -> String {
    let at = format!(
        "segment {} at byte {}, where offset {} should be",
        stop.segment, stop.position, stop.expected_offset
    );
    if stop.tail {
        format!(
            "reading stopped at a torn tail in {at}: {}. If a broker is running on this \
             directory this may be an append in flight",
            stop.cause
        )
    } else {
        format!(
            "a record fails to verify in {at}: {}. Reading stopped there",
            stop.cause
        )
    }
}

/// The table, then the payloads when they were asked for.
fn render(report: &RecordsReport, asked: u64, payloads: bool) -> String {
    let mut rows: Vec<Vec<String>> = report
        .records
        .iter()
        .map(|record| {
            vec![
                marker(record.offset, asked),
                record.offset.to_string(),
                time(record.timestamp_micros),
                kind(&record.mark),
                record.payload_len.to_string(),
                record.segment.to_string(),
                record.position.to_string(),
                "ok".to_string(),
            ]
        })
        .collect();
    if let Some(stop) = &report.stop {
        rows.push(vec![
            marker(stop.expected_offset, asked),
            stop.expected_offset.to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            stop.segment.to_string(),
            stop.position.to_string(),
            format!(
                "{}: {}",
                if stop.tail { "TORN" } else { "FAIL" },
                stop.cause
            ),
        ]);
    }
    if rows.is_empty() {
        return "no records in range".to_string();
    }
    let mut out = vec![table(
        &[
            " ",
            "OFFSET",
            "TIME",
            "KIND",
            "PAYLOAD BYTES",
            "SEGMENT",
            "BYTE",
            "CHECKSUM",
        ],
        rows,
    )];
    if payloads {
        for record in &report.records {
            if let Some(bytes) = &record.payload {
                out.push(String::new());
                out.push(payload_dump(record, bytes));
            }
        }
    }
    out.join("\n")
}

/// `*` on the row the operator asked for.
fn marker(row: u64, asked: u64) -> String {
    if row == asked { "*" } else { " " }.to_string()
}

/// A record's payload as hex and text, sixteen bytes a line.
fn payload_dump(record: &RecordView, bytes: &[u8]) -> String {
    let mut out = format!(
        "offset {} payload, {} bytes",
        record.offset, record.payload_len
    );
    if bytes.len() as u64 != record.payload_len {
        let _ = write!(out, " (first {PAYLOAD_PREVIEW_BYTES} shown)");
    }
    out.push(':');
    if bytes.is_empty() {
        out.push_str(" empty");
    }
    for (line, chunk) in bytes.chunks(16).enumerate() {
        let hex: String = (0..16)
            .map(|i| match chunk.get(i) {
                Some(byte) => format!("{byte:02x} "),
                None => "   ".to_string(),
            })
            .collect();
        let text: String = chunk
            .iter()
            .map(|byte| {
                if byte.is_ascii_graphic() || *byte == b' ' {
                    *byte as char
                } else {
                    '.'
                }
            })
            .collect();
        let _ = write!(out, "\n  {:08x}  {hex} |{text}|", line * 16);
    }
    out
}

fn json_lines(report: &RecordsReport) -> Vec<String> {
    let mut lines: Vec<String> = report
        .records
        .iter()
        .map(|r| record_json(r).to_string())
        .collect();
    if let Some(stop) = &report.stop {
        lines.push(
            json!({
                "offset": stop.expected_offset,
                "checksum_ok": false,
                "torn_tail": stop.tail,
                "segment": stop.segment,
                "byte": stop.position,
                "error": stop.cause.to_string(),
            })
            .to_string(),
        );
    }
    lines
}

fn record_json(record: &RecordView) -> Value {
    let mut line = json!({
        "offset": record.offset,
        "time": (record.timestamp_micros != 0).then(|| time(record.timestamp_micros)),
        "timestamp_micros": record.timestamp_micros,
        "kind": kind(&record.mark),
        "payload_bytes": record.payload_len,
        "checksum": format!("{:#010x}", record.checksum),
        "checksum_ok": true,
        "segment": record.segment,
        "byte": record.position,
        "publisher": record.publisher.as_ref().map(|p| String::from_utf8_lossy(p).into_owned()),
    });
    if let Some(bytes) = &record.payload {
        line["payload_hex"] = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            .into();
        line["payload_utf8"] = String::from_utf8_lossy(bytes).into_owned().into();
        line["payload_truncated"] = (bytes.len() as u64 != record.payload_len).into();
    }
    line
}

fn kind(mark: &RecordMark) -> String {
    match mark {
        RecordMark::None => "plain".to_string(),
        RecordMark::Opens(_) => "producer-batch-start".to_string(),
        RecordMark::Continues => "producer-batch-continue".to_string(),
        RecordMark::GenerationStart => "generation-start".to_string(),
        RecordMark::Commit => "commit".to_string(),
        RecordMark::Trim { floor } => format!("trim (floor {floor})"),
    }
}

/// A microsecond Unix time as `2023-11-14T22:13:20.000000Z`; `-` for zero,
/// which a record that carries no time stores.
fn time(micros: u64) -> String {
    if micros == 0 {
        return "-".to_string();
    }
    let secs = micros / 1_000_000;
    let (days, rest) = ((secs / 86_400) as i64, secs % 86_400);
    // Days since 1970-01-01 to a calendar date.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        micros % 1_000_000
    )
}

#[cfg(test)]
mod tests;
