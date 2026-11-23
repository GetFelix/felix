//! The terms each broker opened its shards at, as the engine told it, and
//! the check that each one was a term Raft elected that broker in, with its
//! blank entry committed. This is the check that a host opens only what Raft
//! elected (`docs/hybrid-retirement.md`, "Gaps", 4).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock, Weak};

use felix_replication::ShardKey;
use felix_replication::driver::WriteFence;
use felix_replication::quorum::ShardServing;
use parking_lot::Mutex;

use super::{Sim, key, list, name};

/// One open the engine reported: `member` leads `shard` at `term` and may
/// take writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Open {
    pub(super) member: usize,
    pub(super) shard: u32,
    pub(super) term: u64,
}

/// Every open of the run, from every start of every member.
#[derive(Default)]
pub(super) struct Opens {
    opens: Mutex<Vec<Open>>,
    /// Opens that too few members held an entry of the term for, when the
    /// engine reported them.
    early: Mutex<Vec<String>>,
    /// Set once the run has started. Opens before it are judged only at the
    /// end.
    sim: OnceLock<Weak<Sim>>,
}

impl Opens {
    pub(super) fn watch(&self, sim: &Arc<Sim>) {
        let _ = self.sim.set(Arc::downgrade(sim));
    }

    /// At the instant `open` is reported, a majority must have flushed an
    /// entry of its term: committing the blank takes that, and every entry
    /// of a term on any log follows that term's blank. A member flushed
    /// into a later term, or stopped, counts, since neither says the blank
    /// is missing. Read with no await in between, so nothing moves under it.
    fn held_when_opened(&self, open: Open) {
        let Some(sim) = self.sim.get().and_then(Weak::upgrade) else {
            return;
        };
        let key = key(open.shard);
        let count = sim.members.len();
        // The opener applied an entry of the term, which is how it opened.
        let held = 1
            + (0..count)
                .filter(|member| *member != open.member)
                .filter(|member| {
                    let at = sim.at(*member);
                    at.down.is_cancelled()
                        || at
                            .runtime
                            .status(&key)
                            .and_then(|status| status.flushed)
                            .is_some_and(|flushed| flushed.term >= open.term)
                })
                .count();
        if held < count / 2 + 1 {
            self.early.lock().push(format!(
                "{}: {} opened term {} with {held} of {count} members holding an entry of it \
                 flushed, so its blank entry was not committed",
                list(open.shard),
                name(open.member),
                open.term
            ));
        }
    }

    pub(super) fn early(&self) -> Vec<String> {
        self.early.lock().clone()
    }

    /// What member `member` reports to, for each of its starts.
    pub(super) fn reporter(self: &Arc<Self>, member: usize) -> Arc<Reporter> {
        Arc::new(Reporter {
            member,
            opens: self.clone(),
        })
    }

    pub(super) fn all(&self) -> Vec<Open> {
        self.opens.lock().clone()
    }
}

/// The broker side of one member: the engine's only calls into it on Raft
/// are `opened` and `deposed`, and only `opened` is checked.
pub(super) struct Reporter {
    member: usize,
    opens: Arc<Opens>,
}

impl WriteFence for Reporter {
    fn quiesced(&self, _key: &ShardKey) -> bool {
        true
    }

    fn opened(&self, key: &ShardKey, term: u64) {
        let shard = key
            .stream
            .strip_prefix('s')
            .and_then(|n| n.parse().ok())
            .expect("the simulation names its streams s<n>");
        let open = Open {
            member: self.member,
            shard,
            term,
        };
        self.opens.opens.lock().push(open);
        self.opens.held_when_opened(open);
    }
}

impl ShardServing for Reporter {
    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        None
    }

    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

/// What the run saw besides the opens.
pub(super) struct Seen<'a> {
    /// Per (shard, term): the member the watch saw lead it, sampled.
    pub(super) led: &'a BTreeMap<(u32, u64), usize>,
    /// Per shard: the terms of the entries in its final committed log.
    /// A shard with no final read is not here and is not judged on this.
    pub(super) committed: &'a BTreeMap<u32, BTreeSet<u64>>,
}

/// Every open that was not a term Raft elected its broker in with the blank
/// entry committed. Three things say so:
///
/// - Two brokers opened one term of a shard. Raft elects one per term.
/// - The watch saw another broker lead that term.
/// - The shard's final log holds no entry of that term. The blank is the
///   first entry a leader writes, so if it was committed, the log keeps an
///   entry of the term for good.
pub(super) fn judge(opens: &[Open], seen: &Seen<'_>) -> Vec<String> {
    let mut findings = Vec::new();
    let mut opener: BTreeMap<(u32, u64), usize> = BTreeMap::new();
    for open in opens {
        let Open {
            member,
            shard,
            term,
        } = *open;
        let first = *opener.entry((shard, term)).or_insert(member);
        if first != member {
            findings.push(format!(
                "{}: {} and {} both opened term {term}",
                list(shard),
                name(first),
                name(member)
            ));
        }
        if let Some(&leader) = seen.led.get(&(shard, term))
            && leader != member
        {
            findings.push(format!(
                "{}: {} opened term {term}, which Raft elected {} in",
                list(shard),
                name(member),
                name(leader)
            ));
        }
        if let Some(terms) = seen.committed.get(&shard)
            && !terms.contains(&term)
        {
            findings.push(format!(
                "{}: {} opened term {term}, and the final log holds no entry of it, so its \
                 blank entry was never committed",
                list(shard),
                name(member)
            ));
        }
    }
    findings
}

#[cfg(test)]
mod tests;
