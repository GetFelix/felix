//! Printing: human-readable text by default, one compact JSON document per
//! result (or per message, for `sub` and `watch`) with `--json`.

use std::io::Write;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::Value;

/// Where results go, and in which form.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Output {
    pub(crate) json: bool,
}

impl Output {
    /// Print `text` as is, adding a final newline if it lacks one.
    pub(crate) fn text(&self, text: &str) -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(text.as_bytes())?;
        if !text.ends_with('\n') {
            stdout.write_all(b"\n")?;
        }
        stdout.flush()?;
        Ok(())
    }

    /// Print `value` as one line of JSON.
    pub(crate) fn json_value(&self, value: &Value) -> anyhow::Result<()> {
        self.text(&value.to_string())
    }

    /// Print `text`, or `json` under `--json`.
    pub(crate) fn done(&self, text: &str, json: Value) -> anyhow::Result<()> {
        if self.json {
            self.json_value(&json)
        } else {
            self.text(text)
        }
    }

    /// Write raw bytes and a newline, for payloads printed as they are.
    pub(crate) fn raw_line(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(bytes)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
        Ok(())
    }
}

/// Columns padded to their widest cell. The last column is not padded.
pub(crate) fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
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
            if i == last {
                out.push_str(cell);
            } else {
                out.push_str(cell);
                let pad = widths[i] - cell.chars().count() + 2;
                out.extend(std::iter::repeat_n(' ', pad));
            }
        }
        out.trim_end().to_string()
    };
    let mut out = line(headers.to_vec());
    for row in &rows {
        out.push('\n');
        out.push_str(&line(row.iter().map(String::as_str).collect()));
    }
    out
}

/// A payload as a JSON field: `payload` holding the text when it is UTF-8,
/// `payload_base64` otherwise, so binary data survives the round trip.
pub(crate) fn payload_field(bytes: &[u8]) -> (&'static str, Value) {
    match std::str::from_utf8(bytes) {
        Ok(text) => ("payload", Value::String(text.to_string())),
        Err(_) => ("payload_base64", Value::String(STANDARD.encode(bytes))),
    }
}

/// An object's top-level fields as `key: value` lines, nested values as
/// compact JSON. How `info` commands print without `--json`.
pub(crate) fn fields(value: &Value) -> String {
    let Some(object) = value.as_object() else {
        return cell(value);
    };
    let width = object.keys().map(|k| k.chars().count()).max().unwrap_or(0);
    object
        .iter()
        .map(|(key, value)| format!("{key:width$}  {}", cell(value)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// One JSON value as a table cell: strings bare, `null` empty, arrays of
/// strings comma-joined, anything else compact JSON.
pub(crate) fn cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Array(items) if items.iter().all(Value::is_string) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(","),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
