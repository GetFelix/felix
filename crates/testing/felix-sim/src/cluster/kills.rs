//! The sim's own record of unclean kills and of who was in each shard's
//! replica set, kept to name the shards the broker holds on purpose.
//!
//! When two members of one shard's set are killed uncleanly with overlapping
//! down windows, the broker cannot prove that what those two copies
//! acknowledged is covered, so it holds the shard until an operator runs
//! `felixctl placement abandon` (`docs/replication-design.md`, "A leader's own
//! crash"). [`ElectionSettles`](crate::check::ElectionSettles) leaves such
//! shards out.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// Replica sets by shard (`stream/3`), as the control plane held them.
pub(crate) type ReplicaSets = BTreeMap<String, BTreeSet<String>>;

/// One unclean kill: the broker is down from `from` until its restart.
#[derive(Debug, Clone)]
struct Window {
    node: String,
    from: Duration,
    until: Option<Duration>,
}

/// Unclean kills so far, and the replica sets sampled around them.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    windows: Vec<Window>,
    /// Replica sets in time order.
    sets: Vec<(Duration, ReplicaSets)>,
}

impl KillLog {
    pub(crate) fn kill(&mut self, node: &str, at: Duration) {
        self.windows.push(Window {
            node: node.to_string(),
            from: at,
            until: None,
        });
    }

    /// The broker started again. Returns whether it had an open kill.
    pub(crate) fn restart(&mut self, node: &str, at: Duration) -> bool {
        let mut closed = false;
        for window in &mut self.windows {
            if window.node == node && window.until.is_none() {
                window.until = Some(at);
                closed = true;
            }
        }
        closed
    }

    pub(crate) fn sample(&mut self, at: Duration, sets: ReplicaSets) {
        self.sets.push((at, sets));
    }

    /// Shards with two members killed uncleanly at overlapping times.
    ///
    /// A member counts if it was in the set at any sample taken from the last
    /// one before the overlap to the first one after it. Sets are sampled at
    /// every kill and restart, which is when they move.
    pub(crate) fn held(&self) -> BTreeSet<String> {
        let mut held = BTreeSet::new();
        for (i, a) in self.windows.iter().enumerate() {
            for b in &self.windows[i + 1..] {
                if a.node == b.node {
                    continue;
                }
                let start = a.from.max(b.from);
                let end = match (a.until, b.until) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (x, y) => x.or(y),
                };
                if end.is_some_and(|end| end <= start) {
                    continue;
                }
                for sets in self.sampled_between(start, end) {
                    for (shard, members) in sets {
                        if members.contains(&a.node) && members.contains(&b.node) {
                            held.insert(shard.clone());
                        }
                    }
                }
            }
        }
        held
    }

    fn sampled_between(&self, start: Duration, end: Option<Duration>) -> Vec<&ReplicaSets> {
        let first = self
            .sets
            .iter()
            .rposition(|(at, _)| *at <= start)
            .unwrap_or(0);
        let last = match end {
            Some(end) => self
                .sets
                .iter()
                .position(|(at, _)| *at >= end)
                .unwrap_or(self.sets.len().saturating_sub(1)),
            None => self.sets.len().saturating_sub(1),
        };
        self.sets
            .get(first..=last.max(first))
            .unwrap_or_default()
            .iter()
            .map(|(_, sets)| sets)
            .collect()
    }
}

#[cfg(test)]
mod tests;
