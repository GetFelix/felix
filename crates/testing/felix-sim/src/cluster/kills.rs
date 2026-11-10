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
//! A chance to vouch needs a way to vouch. The sim records a copy as reached
//! only by a serving broker the copy has not accepted a newer leader over, on a link that is not
//! partitioned. A voter is vouched by that shipping; a learner also needs the
//! control plane to answer (not paused or down) for the reconcile interval
//! plus slack plus three slow calls. And a copy counts as untrusted only when
//! the sim's own record explains it: an injected kill or power loss, or an
//! unclean start it saw that the copy has not been seen trusted since.
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
    /// The members still joining: learners the control plane has not readmitted.
    pub(crate) joining: BTreeSet<String>,
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
    /// How long this run must last to count.
    need: Duration,
    /// An earlier run was long enough.
    done: bool,
}

impl Reach {
    fn long_enough(&self) -> bool {
        self.done || self.last - self.since >= self.need
    }
}

/// Samples further apart than this are not one run.
const RUN_GAP: Duration = Duration::from_millis(2500);

/// How long a serving broker must reach a voting copy for a vouch: a few
/// shipping rounds. A shorter run is a blip, not a chance.
const SHIP_WINDOW: Duration = RUN_GAP;

/// A learner is readmitted by a control plane step on its reconcile cycle,
/// and each call under a slow control plane takes up to three delays (the
/// reads and the write), so a run counts after `reconcile + RUN_GAP + 3d`.
const READMIT_CALLS: u32 = 3;

/// The serving brokers (index and generation) that can still vouch a copy
/// that has accepted a leader at `accepted`: a follower refuses a leader
/// below the generation it accepted, so only that copy's own record says a
/// leader was superseded for it. A newer generation routed elsewhere that the
/// copy never accepted supersedes nothing.
pub(crate) fn unsuperseded(servers: &[(usize, u64)], accepted: Option<u64>) -> Vec<usize> {
    servers
        .iter()
        .filter(|(_, generation)| accepted.is_none_or(|accepted| accepted <= *generation))
        .map(|(index, _)| *index)
        .collect()
}

/// Whether a serving broker in `serving` reaches the broker `to` over a link
/// that is up: it is one of them, or no partition cuts it off from `to`.
pub(crate) fn reaches(
    serving: &[usize],
    to: usize,
    link_up: &dyn Fn(usize, usize) -> bool,
) -> bool {
    serving
        .iter()
        .any(|leader| *leader == to || link_up(*leader, to))
}

/// How long each control plane call takes under `fault`, or `None` when it
/// answers nothing (paused or down), which only a voter's vouch survives.
pub(crate) fn cp_delay(fault: crate::controlplane::CpFault) -> Option<Duration> {
    use crate::controlplane::CpFault;
    match fault {
        CpFault::Healthy => Some(Duration::ZERO),
        CpFault::Slow(delay) => Some(delay),
        CpFault::Paused | CpFault::Down => None,
    }
}

/// Unclean stops and abandons so far. A clean stop never enters it.
#[derive(Debug)]
pub(crate) struct KillLog {
    /// The control plane's reconcile interval.
    reconcile: Duration,
    stops: Vec<Unclean>,
    /// When an operator last abandoned each shard's log.
    abandoned: BTreeMap<String, Duration>,
    /// When each broker last started unclean.
    unclean_starts: BTreeMap<String, Duration>,
    /// For each copy (shard, node), how long a serving broker had a link up
    /// to it since its last unclean start.
    shipped: BTreeMap<(String, String), Reach>,
    /// The same, with a control plane that answers: what readmitting a
    /// learner takes.
    readmittable: BTreeMap<(String, String), Reach>,
    /// Copies seen trusted since their broker's last unclean start.
    vouched: BTreeSet<(String, String)>,
}

/// Add a sample at `at` to a run, opening a new one after a gap. A run
/// needs `need` at the most demanding delay it saw.
fn sample(
    entry: std::collections::btree_map::Entry<'_, (String, String), Reach>,
    at: Duration,
    need: Duration,
) {
    let reach = entry.or_insert(Reach {
        since: at,
        last: at,
        need,
        done: false,
    });
    if at.saturating_sub(reach.last) > RUN_GAP {
        reach.done |= reach.long_enough();
        reach.since = at;
        reach.need = need;
    }
    reach.need = reach.need.max(need);
    reach.last = at;
}

impl Default for KillLog {
    fn default() -> Self {
        Self::new(Duration::from_secs(5))
    }
}

impl KillLog {
    pub(crate) fn new(reconcile: Duration) -> Self {
        Self {
            reconcile,
            stops: Vec::new(),
            abandoned: BTreeMap::new(),
            unclean_starts: BTreeMap::new(),
            shipped: BTreeMap::new(),
            readmittable: BTreeMap::new(),
            vouched: BTreeSet::new(),
        }
    }

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
        self.shipped.retain(|(_, served), _| served != node);
        self.readmittable.retain(|(_, served), _| served != node);
        self.vouched.retain(|(_, vouched)| vouched != node);
    }

    /// `node` was up at `at` with a link from a broker serving `shard`.
    /// `delay` is how long a control plane call takes now, `None` when it
    /// answers nothing.
    pub(crate) fn served(
        &mut self,
        shard: &str,
        node: &str,
        at: Duration,
        delay: Option<Duration>,
    ) {
        let key = (shard.to_string(), node.to_string());
        sample(self.shipped.entry(key.clone()), at, SHIP_WINDOW);
        if let Some(delay) = delay {
            let need = self.reconcile + RUN_GAP + delay * READMIT_CALLS;
            sample(self.readmittable.entry(key), at, need);
        }
    }

    /// The brokers that started unclean so far.
    pub(crate) fn unclean_nodes(&self) -> BTreeSet<String> {
        self.unclean_starts.keys().cloned().collect()
    }

    /// `shard`'s copy on `node` was seen trusted at some point after the
    /// broker's last unclean start: it was vouched, so a later loss of trust
    /// has no injected unclean start behind it.
    pub(crate) fn trusted(&mut self, shard: &str, node: &str) {
        if self.unclean_starts.contains_key(node) {
            self.vouched.insert((shard.to_string(), node.to_string()));
        }
    }

    /// Whether a leader had the chance to vouch `node`'s copy of `shard`
    /// since its last unclean start. A learner needs the control plane to
    /// readmit it; a voter is vouched by a leader shipping over a link.
    pub(crate) fn vouchable(&self, shard: &str, node: &str, learner: bool) -> bool {
        let key = (shard.to_string(), node.to_string());
        let runs = if learner {
            &self.readmittable
        } else {
            &self.shipped
        };
        runs.get(&key).is_some_and(Reach::long_enough)
    }

    /// Whether `copy` counts as untrusted towards holding `shard`.
    fn counts_untrusted(&self, shard: &str, node: &str, copy: Copy, view: &ShardView) -> bool {
        match copy {
            Copy::Trusted => false,
            Copy::DownAfterKill => true,
            Copy::Untrusted => {
                let learner = view.joining.contains(node);
                self.unclean_starts.contains_key(node)
                    && !self
                        .vouched
                        .contains(&(shard.to_string(), node.to_string()))
                    && !self.vouchable(shard, node, learner)
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
                        && self.vouchable(shard, node, view.joining.contains(*node))
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
                if !self.counts_untrusted(shard, node, copy, view) {
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
                            .is_some_and(|copy| self.counts_untrusted(shard, node, *copy, view))
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
