//! Field-level versioning for replicated values.
//!
//! A command's level covers its variant, but an older member also silently
//! drops any field it does not know when it decodes an entry or a snapshot.
//! So every key path a replicated value can serialize is listed in
//! `fields.txt` with the metadata version that introduced it, and two rules
//! follow from that table:
//!
//! - A proposer takes a command's level as the highest level among the key
//!   paths it actually serializes ([`level_of`]). An optional field that is
//!   left at its default is not serialized, so it costs nothing.
//! - A member refuses an entry, or a snapshot, that carries a non-empty field
//!   it would drop ([`dropped_fields`]), rather than applying what is left.
//!
//! The table is checked against the types by `fields/tests.rs`, so a new
//! field cannot be added without choosing its level.
use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use serde_json::Value;

const TABLE: &str = include_str!("fields.txt");

/// Map values are keyed by data (a node id, a label), so their keys are
/// written as this in the table.
pub(crate) const ANY_KEY: &str = "*";

fn table() -> &'static HashMap<&'static str, u16> {
    static TABLE_MAP: OnceLock<HashMap<&'static str, u16>> = OnceLock::new();
    TABLE_MAP.get_or_init(|| parse(TABLE))
}

pub(crate) fn parse(table: &str) -> HashMap<&str, u16> {
    table
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (level, path) = line
                .split_once(' ')
                .unwrap_or_else(|| panic!("fields.txt: bad line {line:?}"));
            let level = level
                .parse()
                .unwrap_or_else(|_| panic!("fields.txt: bad level in {line:?}"));
            (path, level)
        })
        .collect()
}

/// Calls `visit` with every key path under `value`, starting from `root`.
///
/// Array elements share their array's path plus `[]`. A key whose map is
/// listed with [`ANY_KEY`] in `is_map` becomes that instead.
pub(crate) fn walk(
    value: &Value,
    path: &mut String,
    is_map: &dyn Fn(&str) -> bool,
    visit: &mut dyn FnMut(&str, &Value),
) {
    match value {
        Value::Object(object) => {
            let wildcard = is_map(&format!("{path}.{ANY_KEY}"));
            for (key, child) in object {
                let len = path.len();
                path.push('.');
                path.push_str(if wildcard { ANY_KEY } else { key });
                visit(path, child);
                walk(child, path, is_map, visit);
                path.truncate(len);
            }
        }
        Value::Array(items) => {
            let len = path.len();
            path.push_str("[]");
            for item in items {
                walk(item, path, is_map, visit);
            }
            path.truncate(len);
        }
        _ => {}
    }
}

/// The level a serialized command needs: the highest among the paths it
/// carries. A path missing from the table counts as 0, the level every
/// member applies; `fields/tests.rs` is what keeps the table complete.
pub(crate) fn level_of(command: &Value) -> u16 {
    let Some(object) = command.as_object() else {
        return 0;
    };
    let Some(op) = object.get("op").and_then(Value::as_str) else {
        return 0;
    };
    let table = table();
    let mut level = 0;
    for (key, child) in object {
        if key == "op" {
            continue;
        }
        let mut path = format!("{op}.{key}");
        level = level.max(table.get(path.as_str()).copied().unwrap_or(0));
        walk(child, &mut path, &|p| table.contains_key(p), &mut |p, _| {
            level = level.max(table.get(p).copied().unwrap_or(0));
        });
    }
    level
}

/// Key paths in `original` with a non-empty value that `decoded` (the same
/// bytes decoded into this build's types and serialized again) lacks: fields
/// from a newer build that this one would drop.
///
/// Empty values (`null`, `false`, `0`, `""`, `[]`, `{}`) are let through,
/// since that is what an absent optional field means, and a peer that writes
/// one out explicitly has not said anything this build misses.
pub(crate) fn dropped_fields(original: &Value, decoded: &Value) -> Vec<String> {
    let mut kept = BTreeSet::new();
    walk(decoded, &mut String::new(), &|_| false, &mut |path, _| {
        kept.insert(path.to_string());
    });
    let mut dropped: Vec<String> = Vec::new();
    walk(
        original,
        &mut String::new(),
        &|_| false,
        &mut |path, value| {
            if kept.contains(path) || is_empty(value) {
                return;
            }
            // Only the outermost unknown key; its children say nothing more.
            let under_last = dropped.last().is_some_and(|prefix| {
                path.strip_prefix(prefix.as_str())
                    .is_some_and(|rest| rest.starts_with(['.', '[']))
            });
            if !under_last {
                dropped.push(path.to_string());
            }
        },
    );
    dropped.sort();
    dropped.dedup();
    dropped
}

fn is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(b) => !b,
        Value::Number(n) => n.as_f64() == Some(0.0),
        Value::String(s) => s.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(object) => object.is_empty(),
    }
}

#[cfg(test)]
mod tests;
