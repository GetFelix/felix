//! `audit ls`: the control plane's record of who did what.

use std::time::Duration;

use serde_json::Value;

use crate::cli::AuditCommand;
use crate::context::Settings;
use crate::controlplane::Api;
use crate::output::{Output, cell, table};

/// How long `--follow` waits after a page that was not full.
const FOLLOW_POLL: Duration = Duration::from_secs(2);

pub(crate) async fn run(
    command: &AuditCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    let AuditCommand::Ls {
        after,
        limit,
        follow,
    } = command;
    let limit = (*limit).clamp(1, 1000);
    let mut after = *after;
    let mut printed = false;
    loop {
        let page = api
            .get(
                "/v1/audit",
                &[("after", &after.to_string()), ("limit", &limit.to_string())],
            )
            .await?;
        if let Some(warning) = trimmed(&page, after) {
            eprintln!("warning: {warning}");
        }
        let records = page["records"].as_array().cloned().unwrap_or_default();
        if out.json {
            for record in &records {
                out.json_value(record)?;
            }
        } else if !records.is_empty() {
            // A header once, then rows as they come.
            let text = render(&records);
            let text = if printed {
                text.split_once('\n')
                    .map_or("", |(_, rows)| rows)
                    .to_string()
            } else {
                text
            };
            out.text(&text)?;
            printed = true;
        }
        if let Some(last) = records.last().and_then(|record| record["seq"].as_u64()) {
            after = last;
        }
        let full = records.len() >= limit as usize;
        if full {
            continue;
        }
        if !follow {
            if !printed && !out.json {
                out.text("no audit records")?;
            }
            return Ok(());
        }
        tokio::time::sleep(FOLLOW_POLL).await;
    }
}

/// Why records between `after` and the oldest kept one cannot be read: the
/// control plane keeps a fixed number, and they were trimmed first.
fn trimmed(page: &Value, after: u64) -> Option<String> {
    let oldest = page["oldest_seq"].as_u64()?;
    let missed = oldest.saturating_sub(after + 1);
    (missed > 0).then(|| {
        format!(
            "{missed} records after {after} were trimmed before they were read; the oldest \
             kept is {oldest}"
        )
    })
}

pub(crate) fn render(records: &[Value]) -> String {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                cell(&record["seq"]),
                record["at_ms"].as_u64().map_or_else(String::new, utc),
                cell(&record["member"]),
                cell(&record["method"]),
                cell(&record["path"]),
                cell(&record["status"]),
                or_dash(&record["subject"]),
                cell(&record["credential"]),
                or_dash(&record["refusal"]),
            ]
        })
        .collect();
    table(
        &[
            "SEQ",
            "TIME (UTC)",
            "MEMBER",
            "METHOD",
            "PATH",
            "STATUS",
            "SUBJECT",
            "CREDENTIAL",
            "REFUSAL",
        ],
        rows,
    )
}

fn or_dash(value: &Value) -> String {
    let text = cell(value);
    if text.is_empty() {
        "-".to_string()
    } else {
        text
    }
}

/// Milliseconds since the epoch as `YYYY-MM-DD HH:MM:SS`.
fn utc(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

#[cfg(test)]
mod tests;
