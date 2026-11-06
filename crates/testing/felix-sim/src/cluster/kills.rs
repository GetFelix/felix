//! The sim's own record of unclean kills, kept to name the shards the broker
//! holds on purpose.
//!
//! A leader killed uncleanly may have acknowledged writes that only copies
//! also killed or still catching up can vouch for. When the copies that cannot
//! be counted (the killed leader plus every member killed uncleanly before it
//! and still down when it died) reach a majority of the set, a successor
//! cannot prove it covers what was acknowledged, so the broker holds the shard
//! until an operator runs `felixctl placement abandon`
//! (`docs/replication-design.md`, "A leader's own crash").
//! [`ElectionSettles`](crate::check::ElectionSettles) leaves such shards out.
//!
//! A power loss under `FsyncMode::OnCommit` is not a kill: every record the
//! broker acknowledged was synced, so it keeps its storage id and starts
//! clean, and its copy is trusted. Should a restart after one run
//! `UncleanStart` anyway, [`KillLog::doubted`] counts it like a kill.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// A shard as the control plane held it at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardView {
    pub(crate) leader: String,
    pub(crate) generation: u64,
    /// The replica set, with a joining member.
    pub(crate) members: BTreeSet<String>,
}

/// Every shard (`stream/3`) at one instant.
pub(crate) type Views = BTreeMap<String, ShardView>;

/// One unclean kill: the broker is down from `from` until its restart, and
/// `view` is what the shards looked like just before it.
#[derive(Debug, Clone)]
struct Window {
    node: String,
    from: Duration,
    until: Option<Duration>,
    view: Views,
}

/// Unclean kills so far. A clean stop never enters it.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    windows: Vec<Window>,
}

impl KillLog {
    pub(crate) fn kill(&mut self, node: &str, at: Duration, view: Views) {
        self.windows.push(Window {
            node: node.to_string(),
            from: at,
            until: None,
            view,
        });
    }

    /// A stop the broker came back doubting, though it was not a kill: a
    /// power loss whose restart ran `UncleanStart`. It counts as a kill from
    /// `from` until the restart at `until`.
    pub(crate) fn doubted(&mut self, node: &str, from: Duration, until: Duration, view: Views) {
        self.windows.push(Window {
            node: node.to_string(),
            from,
            until: Some(until),
            view,
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

    /// Shards the broker holds. For each unclean kill of a shard's leader, the
    /// leader counts, and so does every other member killed uncleanly earlier,
    /// at the same leader and generation (no successor had opened), that was
    /// still down when the leader died. The shard is held when those reach
    /// `n / 2 + 1` of its `n` members.
    ///
    /// A restart counts as re-reported the moment it is asked for, so a member
    /// that came back and had not yet reported is treated as healthy: the rule
    /// errs towards holding fewer shards.
    pub(crate) fn held(&self) -> BTreeSet<String> {
        let mut held = BTreeSet::new();
        for leader in &self.windows {
            for (shard, view) in &leader.view {
                if view.leader != leader.node {
                    continue;
                }
                let mut doubted = BTreeSet::from([leader.node.as_str()]);
                for other in &self.windows {
                    let same_term = other.view.get(shard).is_some_and(|v| {
                        v.leader == view.leader && v.generation == view.generation
                    });
                    if other.node != leader.node
                        && view.members.contains(&other.node)
                        && other.from < leader.from
                        && other.until.is_none_or(|until| until > leader.from)
                        && same_term
                    {
                        doubted.insert(other.node.as_str());
                    }
                }
                if doubted.len() > view.members.len() / 2 {
                    held.insert(shard.clone());
                }
            }
        }
        held
    }
}

#[cfg(test)]
mod tests;
