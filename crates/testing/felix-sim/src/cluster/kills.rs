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
//! The attestation alone is not enough: a broker that fails to vouch a copy
//! it could have would make its own exemption. So an untrusted copy counts
//! only when no leader had the chance to vouch it:
//!
//! - it is down after an unclean kill, so its next start distrusts it
//!   whatever happened before, or
//! - it is up, and no broker served the shard while it was up since its last
//!   unclean restart.
//!
//! A chance to vouch needs a way to vouch: the sim records a broker as served
//! beside a copy only while the serving broker's link to it is up and the
//! control plane is healthy (not paused, down or slowed), the injected faults
//! that impede vouching and readmission, and only a run of at least five
//! seconds counts, since a shorter one is a blip. And a copy counts as untrusted
//! only when the sim's own record explains it: a kill or power loss it
//! injected, or an unclean start it saw. A copy untrusted with no
//! such record is not counted, so a broker that loses a vouch fails the check.
//!
//! An up copy that is still untrusted after a broker served the shard beside
//! it is vouchable: its shard stays a stall. A broker down after a clean stop,
//! or a copy that does not exist, counts as trusted, and so does a shard with
//! trusted copies and no leader: the rule errs towards holding fewer shards.
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

/// A run of samples in which a serving broker could reach a copy.
#[derive(Debug, Clone, Copy)]
struct Reach {
    since: Duration,
    last: Duration,
    /// The longest earlier run.
    best: Duration,
}

impl Reach {
    fn span(&self) -> Duration {
        self.best.max(self.last - self.since)
    }
}

/// Samples further apart than this are not one run.
const RUN_GAP: Duration = Duration::from_millis(2500);

/// How long a serving broker must reach a copy for a vouch to be expected: a
/// few shipping rounds and, for a learner, the control plane's readmission
/// step. A shorter run is a blip, not a chance.
const VOUCH_WINDOW: Duration = Duration::from_secs(5);

/// Unclean stops and abandons so far. A clean stop never enters it.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    stops: Vec<Unclean>,
    /// When an operator last abandoned each shard's log.
    abandoned: BTreeMap<String, Duration>,
    /// When each broker last started unclean.
    unclean_starts: BTreeMap<String, Duration>,
    /// For each copy (shard, node), how long a broker that could vouch it
    /// served the shard beside it since its last unclean start.
    served: BTreeMap<(String, String), Reach>,
}

impl KillLog {
    /// `node` stopped uncleanly at `at`.
    pub(crate) fn kill(&mut self, node: &str, at: Duration) {
        self.stops.push(Unclean {
            node: node.to_string(),
            at,
        });
    }

    /// `node`'s broker came up at `at` after a start that doubted its copies.
    pub(crate) fn started_unclean(&mut self, node: &str, at: Duration) {
        self.unclean_starts.insert(node.to_string(), at);
        self.served.retain(|(_, served), _| served != node);
    }

    /// `node` was up at `at` while some broker served `shard`.
    pub(crate) fn served(&mut self, shard: &str, node: &str, at: Duration) {
        let reach = self
            .served
            .entry((shard.to_string(), node.to_string()))
            .or_insert(Reach {
                since: at,
                last: at,
                best: Duration::ZERO,
            });
        if at.saturating_sub(reach.last) > RUN_GAP {
            reach.best = reach.span();
            reach.since = at;
        }
        reach.last = at;
    }

    /// Whether a leader had the chance to vouch `node`'s copy of `shard`:
    /// some broker served the shard while `node` was up, since its last
    /// unclean start.
    pub(crate) fn vouchable(&self, shard: &str, node: &str) -> bool {
        self.served
            .get(&(shard.to_string(), node.to_string()))
            .is_some_and(|reach| reach.span() >= VOUCH_WINDOW)
    }

    /// Whether `copy` counts as untrusted towards holding `shard`.
    fn counts_untrusted(&self, shard: &str, node: &str, copy: Copy) -> bool {
        match copy {
            Copy::Trusted => false,
            Copy::DownAfterKill => true,
            Copy::Untrusted => {
                self.unclean_starts.contains_key(node) && !self.vouchable(shard, node)
            }
        }
    }

    /// The shards in `unserved` with a member copy that attests untrusted
    /// although a leader had the chance to vouch it.
    pub(crate) fn vouchable_untrusted(&self, unserved: &Views, trust: &Trust) -> BTreeSet<String> {
        unserved
            .iter()
            .filter(|(shard, view)| {
                members(view).iter().any(|node| {
                    trust.get(&(shard.to_string(), node.to_string())) == Some(&Copy::Untrusted)
                        && self.vouchable(shard, node)
                })
            })
            .map(|(shard, _)| shard.clone())
            .collect()
    }

    /// What explains each copy of `shard` counting as untrusted, for the
    /// report: the injected fault, by broker and time.
    pub(crate) fn explain(&self, shard: &str, view: &ShardView, trust: &Trust) -> Vec<String> {
        members(view)
            .into_iter()
            .filter_map(|node| {
                let copy = *trust.get(&(shard.to_string(), node.to_string()))?;
                if !self.counts_untrusted(shard, node, copy) {
                    return None;
                }
                Some(match copy {
                    Copy::DownAfterKill => {
                        match self.stops.iter().rev().find(|s| s.node == node) {
                            Some(stop) => {
                                format!("{node} stopped unclean at {:.1?} and is down", stop.at)
                            }
                            None => format!("{node} stopped unclean (power loss) and is down"),
                        }
                    }
                    _ => {
                        let at = self.unclean_starts.get(node)?;
                        format!(
                            "{node} restarted unclean at {at:.1?}; no serving leader reached it with the control plane up since"
                        )
                    }
                })
            })
            .collect()
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
                            .is_some_and(|copy| self.counts_untrusted(shard, node, *copy))
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
