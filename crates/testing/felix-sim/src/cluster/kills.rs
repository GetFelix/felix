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
//!
//! Placement faults move shards while kills land, so the rule reads each kill
//! against the set and term the shard had at that instant:
//!
//! - A broker named leader that had not opened the shard yet (killed between
//!   promotion and open) acknowledged nothing at that generation, so its kill
//!   is not a leader's kill for the shard ([`leave_out_unopened`]).
//! - A follower being copied in (`joining`) counts in the set, so a seat
//!   replacement in flight needs a majority of the wider set.
//! - An operator's `placement abandon` ends the hold: kills before it no
//!   longer count for that shard.

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

/// Leave out of `view` each shard `node` is named leader of but had not
/// opened (`unopened`): it acknowledged nothing there as leader.
pub(crate) fn leave_out_unopened(view: &mut Views, node: &str, unopened: &BTreeSet<String>) {
    view.retain(|shard, v| v.leader != node || !unopened.contains(shard));
}

/// The last set a leader opened a shard for, and which members an opened
/// member's broker has trusted its copy since the restart.
#[derive(Debug, Clone)]
struct Opened {
    generation: u64,
    members: BTreeSet<String>,
    at: Duration,
    /// When an opened leader was last seen with the member up.
    shipped: BTreeMap<String, Duration>,
}

/// Unclean kills so far. A clean stop never enters it.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    windows: Vec<Window>,
    /// When an operator last abandoned each shard's log.
    abandoned: BTreeMap<String, Duration>,
    opened: BTreeMap<String, Opened>,
}

impl KillLog {
    /// `shard` is open at `view.generation` under `view.leader` at `at`. A
    /// newer generation than the one recorded starts a new set to read the
    /// kills against.
    pub(crate) fn opened(&mut self, shard: &str, view: &ShardView, at: Duration) {
        if self
            .opened
            .get(shard)
            .is_some_and(|o| o.generation >= view.generation)
        {
            return;
        }
        let mut members = view.members.clone();
        members.insert(view.leader.clone());
        self.opened.insert(
            shard.to_string(),
            Opened {
                generation: view.generation,
                members,
                at,
                shipped: BTreeMap::new(),
            },
        );
    }

    /// `member`'s broker trusts its copy of `shard` at `at`: it is running and
    /// a leader has shipped the copy since its restart.
    pub(crate) fn shipped(&mut self, shard: &str, member: &str, at: Duration) {
        if let Some(opened) = self.opened.get_mut(shard) {
            opened.shipped.insert(member.to_string(), at);
        }
    }

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

    /// An operator abandoned `shard`'s log at `at`: kills before then no
    /// longer hold it.
    pub(crate) fn abandon(&mut self, shard: &str, at: Duration) {
        self.abandoned.insert(shard.to_string(), at);
    }

    /// Shards the broker holds. For each unclean kill of a shard's leader, the
    /// leader counts, and so does every other member killed uncleanly earlier,
    /// at the same leader and generation (no successor had opened), that was
    /// still down when the leader died. The shard is held when those reach
    /// `n / 2 + 1` of its `n` members, the leader and a joining follower
    /// included.
    ///
    /// A restart counts as re-reported the moment it is asked for, so a member
    /// that came back and had not yet reported is treated as healthy: the rule
    /// errs towards holding fewer shards.
    pub(crate) fn held(&self) -> BTreeSet<String> {
        let mut held = self.held_by_open_set();
        for leader in &self.windows {
            for (shard, view) in &leader.view {
                if view.leader != leader.node
                    || self
                        .abandoned
                        .get(shard)
                        .is_some_and(|at| *at > leader.from)
                {
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
                let mut set: BTreeSet<&str> = view.members.iter().map(String::as_str).collect();
                set.insert(view.leader.as_str());
                if doubted.len() > set.len() / 2 {
                    held.insert(shard.clone());
                }
            }
        }
        held
    }
}

impl KillLog {
    /// Shards whose last opened set has a majority of doubted copies. A copy
    /// is doubted when its broker was killed uncleanly after the open and no
    /// leader has shipped its copy since it restarted, as the broker records it. A named leader
    /// killed before it opened counts as a doubted follower, since its kill
    /// is in the log either way. Shards whose copies are trusted but whose
    /// fence cannot open are not held, so they still fail the check.
    fn held_by_open_set(&self) -> BTreeSet<String> {
        let mut held = BTreeSet::new();
        for (shard, open) in &self.opened {
            if self.abandoned.get(shard).is_some_and(|at| *at > open.at) {
                continue;
            }
            let doubted = open
                .members
                .iter()
                .filter(|member| {
                    self.windows.iter().any(|w| {
                        w.node == **member
                            && w.from >= open.at
                            && match (w.until, open.shipped.get(*member)) {
                                (None, _) | (Some(_), None) => true,
                                (Some(until), Some(shipped)) => *shipped < until,
                            }
                    })
                })
                .count();
            if doubted > open.members.len() / 2 {
                held.insert(shard.clone());
            }
        }
        held
    }
}

#[cfg(test)]
mod tests;
