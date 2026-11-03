//! Rule 12, one leader per generation: no two brokers open one shard for
//! writes at the same generation. This is the model's `OneLeaderPerGeneration`.
//!
//! What a broker opens is not visible to a client, so this rule reads the
//! brokers' logs rather than the history's operations. The harness keeps one
//! log per broker and the one from before its last restart, so the campaign
//! reads them after every fault, before a later restart can drop one.

use std::collections::{BTreeMap, BTreeSet};

use super::checker::{Rule, Violation};
use super::model::Opened;
use crate::Cluster;

/// What a broker logs once a majority took its fence, with the generation it
/// opened at.
const OPENED: &str = "a majority took the fence; opening the shard for writes";

/// The log files kept per broker: the current run's and the one before.
const LOGS: [&str; 2] = ["broker.log", "broker.log.previous"];

/// Add every open in the logs of every broker of `cluster` to `into`.
pub(super) fn read(cluster: &Cluster, into: &mut BTreeSet<Opened>) {
    for node in cluster.node_ids() {
        let Some(dir) = cluster.node(&node).map(|n| n.data_dir.clone()) else {
            continue;
        };
        for log in LOGS {
            let Ok(text) = std::fs::read_to_string(dir.join(log)) else {
                continue;
            };
            into.extend(text.lines().filter_map(|line| parse(line, &node)));
        }
    }
}

/// The open `line` records, if it records one.
pub(super) fn parse(line: &str, node: &str) -> Option<Opened> {
    if !line.contains(OPENED) {
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
