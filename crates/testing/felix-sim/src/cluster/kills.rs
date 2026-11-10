//! The shards the broker holds on purpose, so
//! [`ElectionSettles`](crate::check::ElectionSettles) leaves them out.
//!
//! A leader counts a promotion fence's answers by what each copy attests:
//! coverage comes only from a copy that holds everything its seat
//! acknowledged (`trusted`), and the trusted copies must meet every majority
//! of the set. A copy stops being trusted when its broker restarts after an
//! unclean stop, and stays so until a leader vouches it. Once the untrusted
//! copies of a shard nobody serves are a majority, no successor can prove it
//! covers what was acknowledged, and the broker holds the shard until an
//! operator runs `felixctl placement abandon` (`docs/replication-design.md`,
//! "A leader's own crash").
//!
//! A copy counts as untrusted when its broker attests so, as it would answer
//! a fence, or when it is down after an unclean kill (its next start distrusts
//! it whatever happened before). That includes a copy a serving leader sat
//! beside: a leader vouches only by shipping the copy level, which a leader
//! cut off from it, or one that has not readmitted a learner, never does. A
//! broker down after a clean stop, or a copy that does not exist, counts as
//! trusted, and so does a shard with trusted copies and no leader: the rule
//! errs towards holding fewer shards.
//!
//! An operator's `placement abandon` ends the hold: the shard is not held
//! again until a member is stopped uncleanly after it.

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

/// What one copy of a shard attests, as the rule reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Copy {
    /// Up and attesting its copy trusted.
    Trusted,
    /// Up and attesting its copy untrusted.
    Untrusted,
    /// Down after an unclean kill.
    DownAfterKill,
}

/// Each copy's attestation, keyed by shard and node. A copy left out is not
/// known to be untrusted.
pub(crate) type Trust = BTreeMap<(String, String), Copy>;

/// One unclean stop: a kill, or a power loss the restart came back doubting.
#[derive(Debug, Clone)]
struct Unclean {
    node: String,
    at: Duration,
}

/// Unclean stops and abandons so far. A clean stop never enters it.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    stops: Vec<Unclean>,
    /// When an operator last abandoned each shard's log.
    abandoned: BTreeMap<String, Duration>,
}

impl KillLog {
    /// `node` stopped uncleanly at `at`.
    pub(crate) fn kill(&mut self, node: &str, at: Duration) {
        self.stops.push(Unclean {
            node: node.to_string(),
            at,
        });
    }

    /// An operator abandoned `shard`'s log at `at`.
    pub(crate) fn abandon(&mut self, shard: &str, at: Duration) {
        self.abandoned.insert(shard.to_string(), at);
    }

    /// Of the shards in `unserved`, those whose copies that count as
    /// untrusted are `n / 2 + 1` or more of the `n` members, the leader and a
    /// joining member included.
    pub(crate) fn held(&self, unserved: &Views, trust: &Trust) -> BTreeSet<String> {
        unserved
            .iter()
            .filter(|(shard, view)| {
                let set = members(view);
                if let Some(abandoned) = self.abandoned.get(*shard)
                    && !self
                        .stops
                        .iter()
                        .any(|stop| stop.at > *abandoned && set.contains(stop.node.as_str()))
                {
                    return false;
                }
                let untrusted = set
                    .iter()
                    .filter(|node| {
                        trust
                            .get(&(shard.to_string(), node.to_string()))
                            .is_some_and(|copy| *copy != Copy::Trusted)
                    })
                    .count();
                untrusted > set.len() / 2
            })
            .map(|(shard, _)| shard.clone())
            .collect()
    }
}

/// The set's members, the leader included.
fn members(view: &ShardView) -> BTreeSet<&str> {
    let mut set: BTreeSet<&str> = view.members.iter().map(String::as_str).collect();
    set.insert(view.leader.as_str());
    set
}

#[cfg(test)]
mod tests;
