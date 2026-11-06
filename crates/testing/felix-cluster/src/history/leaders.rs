//! Rule 12, one leader per generation: no two brokers open one shard for
//! writes at the same generation. This is the model's `OneLeaderPerGeneration`.
//!
//! What a broker opens is not visible to a client, so this rule reads the
//! brokers' logs rather than the history's operations. The harness keeps one
//! log per broker and appends each earlier run to `broker.log.earlier`, and
//! the campaign reads both after every fault.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::checker::{Rule, Violation};
use super::model::Opened;
use crate::Cluster;

/// What a broker logs once a majority took its fence, with the generation it
/// opened at.
const OPENED: &str = "a majority took the fence; opening the shard for writes";

/// Every run of a broker: the current one, and all before it, which the
/// harness appends to `broker.log.earlier` at each restart. A heal can
/// restart a broker twice, so `broker.log.previous` alone would miss a run.
pub(super) const EVERY_RUN: [&str; 2] = ["broker.log", "broker.log.earlier"];

/// Add every open in every run of every broker of `cluster` to `into`.
pub(super) fn read(cluster: &Cluster, into: &mut BTreeSet<Opened>) {
    scan(cluster, &EVERY_RUN, parse, into);
}

/// Add the opens and the candidacies in the brokers' current runs to `opens`
/// and `stood`. For polling while nothing restarts.
pub(super) fn read_current(
    cluster: &Cluster,
    opens: &mut BTreeSet<Opened>,
    stood: &mut BTreeSet<Opened>,
) {
    scan(cluster, &["broker.log"], parse, opens);
    scan(cluster, &["broker.log"], parse_stood, stood);
}

/// What a candidate logs once its ballot is on disk. The full phrase, since
/// "not standing for election" ends the same way.
const STOOD: &str = "standing for election: the leader";

/// Every `node at generation` that stood for shard `shard` of `name`, from
/// the broker logs. For a report on an election that never opened.
pub(super) fn stood(cluster: &Cluster, name: &str, shard: u32) -> Vec<String> {
    let mut stood = BTreeSet::new();
    scan(cluster, &EVERY_RUN, parse_stood, &mut stood);
    stood
        .into_iter()
        .filter(|s| s.name == name && s.shard == shard)
        .map(|s| format!("{} at {}", s.node, s.generation))
        .collect()
}

fn scan(
    cluster: &Cluster,
    logs: &[&str],
    parse: fn(&str, &str) -> Option<Opened>,
    into: &mut BTreeSet<Opened>,
) {
    for node in cluster.node_ids() {
        if let Some(dir) = cluster.node(&node).map(|n| n.data_dir.clone()) {
            scan_dir(&dir, &node, logs, parse, into);
        }
    }
}

/// What `parse` finds in the `logs` of broker `node`, whose data is in `dir`.
pub(super) fn scan_dir(
    dir: &Path,
    node: &str,
    logs: &[&str],
    parse: fn(&str, &str) -> Option<Opened>,
    into: &mut BTreeSet<Opened>,
) {
    for log in logs {
        let Ok(text) = std::fs::read_to_string(dir.join(log)) else {
            continue;
        };
        into.extend(text.lines().filter_map(|line| parse(line, node)));
    }
}

/// The open `line` records, if it records one.
pub(super) fn parse(line: &str, node: &str) -> Option<Opened> {
    parse_marked(line, node, OPENED)
}

/// The candidacy `line` records, if it records one: `node` stood for the
/// shard at the generation it would open.
pub(super) fn parse_stood(line: &str, node: &str) -> Option<Opened> {
    parse_marked(line, node, STOOD)
}

fn parse_marked(line: &str, node: &str, marker: &str) -> Option<Opened> {
    if !line.contains(marker) {
        return None;
    }
    // The log file keeps the formatter's colours, which split a field's name
    // from its value.
    let line = strip_ansi(line);
    Some(Opened {
        name: field(&line, "stream")?.to_string(),
        shard: field(&line, "shard")?.parse().ok()?,
        generation: field(&line, "generation")?.parse().ok()?,
        node: node.to_string(),
    })
}

/// The latest open of shard `shard` of `name` after generation `after` by a
/// broker other than `killed` that stood for it at that generation. An open
/// nobody stood for came from the control plane, by a move that landed
/// before it went down, so it is no election.
pub(super) fn elected<'a>(
    opens: &'a BTreeSet<Opened>,
    stood: &BTreeSet<Opened>,
    name: &str,
    shard: u32,
    after: u64,
    killed: &str,
) -> Option<&'a Opened> {
    opens
        .iter()
        .filter(|open| {
            open.name == name
                && open.shard == shard
                && open.generation > after
                && open.node != killed
                && stood.contains(*open)
        })
        .max_by_key(|open| open.generation)
}

/// A violation for every shard and generation more than one broker opened.
pub(super) fn check(opens: &BTreeSet<Opened>, out: &mut Vec<Violation>) {
    let mut by_generation: BTreeMap<(&str, u32, u64), BTreeSet<&str>> = BTreeMap::new();
    for open in opens {
        by_generation
            .entry((&open.name, open.shard, open.generation))
            .or_default()
            .insert(&open.node);
    }
    for ((name, shard, generation), nodes) in by_generation {
        if nodes.len() > 1 {
            out.push(Violation {
                rule: Rule::OneLeaderPerGeneration,
                list: name.to_string(),
                ops: Vec::new(),
                explanation: format!(
                    "shard {shard} was opened for writes at generation {generation} by {}",
                    nodes.into_iter().collect::<Vec<_>>().join(" and ")
                ),
            });
        }
    }
}

/// The value of the `name=value` field in `line`.
fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(name)?.strip_prefix('='))
}

fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests;
