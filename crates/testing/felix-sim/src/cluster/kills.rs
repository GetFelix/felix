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
//!   unclean restart. A broker counts only while the copy still accepts its
//!   generation: a copy that took a newer leader's fence refuses an older
//!   leader, so that leader could not have vouched it.
//!
//! An up copy that is still untrusted after a current broker served the shard
//! beside it is vouchable: its shard stays a stall. A broker down after a clean stop,
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

/// Unclean stops and abandons so far. A clean stop never enters it.
#[derive(Debug, Default)]
pub(crate) struct KillLog {
    stops: Vec<Unclean>,
    /// When an operator last abandoned each shard's log.
    abandoned: BTreeMap<String, Duration>,
    /// When each broker last started unclean.
    unclean_starts: BTreeMap<String, Duration>,
    /// When each copy (shard, node) was last seen up while a broker served
    /// the shard.
    served: BTreeMap<(String, String), Duration>,
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
    }

    /// `node` was up at `at` while a broker served `shard` at `serving`, and
    /// its copy had accepted generation `accepted`. A copy that already
    /// accepted a newer leader refuses this one, so it could not vouch.
    pub(crate) fn served(
        &mut self,
        shard: &str,
        node: &str,
        at: Duration,
        serving: u64,
        accepted: u64,
    ) {
        if accepted <= serving {
            self.served
                .insert((shard.to_string(), node.to_string()), at);
        }
    }

    /// Whether a leader had the chance to vouch `node`'s copy of `shard`:
    /// some broker served the shard while `node` was up, since its last
    /// unclean start.
    pub(crate) fn vouchable(&self, shard: &str, node: &str) -> bool {
        let since = self.unclean_starts.get(node).copied().unwrap_or_default();
        self.served
            .get(&(shard.to_string(), node.to_string()))
            .is_some_and(|at| *at >= since)
    }

    /// Whether `copy` counts as untrusted towards holding `shard`.
    fn counts_untrusted(&self, shard: &str, node: &str, copy: Copy) -> bool {
        match copy {
            Copy::Trusted => false,
            Copy::DownAfterKill => true,
            Copy::Untrusted => !self.vouchable(shard, node),
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
