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

    /// How many opens have been reported so far. The list only grows, so
    /// this marks a point in it.
    pub(super) fn len(&self) -> usize {
        self.opens.lock().len()
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
    /// Per shard: where that log was read and who else holds what.
    pub(super) read: &'a BTreeMap<u32, Read>,
    /// Per shard: how many of the run's opens, in the order reported, came
    /// before its final log was read. Only those are judged against that
    /// log, since a later open's entry cannot be in it. Absent, every open is.
    pub(super) before_read: &'a BTreeMap<u32, usize>,
}

/// Where a shard's final log was read, kept so a missing term can be told
/// apart: a term some member still holds was read past or cut off, and a
/// term nobody holds is gone from every disk.
#[derive(Debug, Clone, Default)]
pub(super) struct Read {
    /// The member whose log it is, and the term it led then.
    pub(super) from: usize,
    pub(super) term: u64,
    /// Per term: the members whose log holds an entry of it at any offset,
    /// committed or not.
    pub(super) holders: BTreeMap<u64, Vec<usize>>,
    /// Each member's log as read: its last entry's term and offset, and
    /// whether it led the shard then. A hand-over between the opens and
    /// this read shows here.
    pub(super) logs: Vec<MemberLog>,
}

#[derive(Debug, Clone)]
pub(super) struct MemberLog {
    pub(super) member: usize,
    /// Term and offset of the last entry, `None` for an empty log.
    pub(super) last: Option<(u64, u64)>,
    pub(super) leader: bool,
}

impl Read {
    fn missing(&self, term: u64) -> String {
        let holders = self.holders.get(&term).map_or(&[][..], Vec::as_slice);
        let on = if holders.is_empty() {
            "no member's log holds an entry of it".to_string()
        } else {
            let names: Vec<_> = holders.iter().map(|member| name(*member)).collect();
            format!("{} still hold an entry of it", names.join(" "))
        };
        let logs: Vec<_> = self
            .logs
            .iter()
            .map(|log| {
                let last = log.last.map_or("empty".to_string(), |(term, index)| {
                    format!("term {term} last index {index}")
                });
                let role = if log.leader { ", leader" } else { "" };
                format!("{} {last}{role}", name(log.member))
            })
            .collect();
        format!(
            "; the log was read from {} leading term {}, and {on}; judged against {}",
            name(self.from),
            self.term,
            logs.join("; ")
        )
    }
}

/// Every open that was not a term Raft elected its broker in with the blank
/// entry committed. Three things say so:
///
/// - Two brokers opened one term of a shard. Raft elects one per term.
/// - The watch saw another broker lead that term.
/// - The shard's final log holds no entry of that term, for an open before
///   that log was read. The blank is the first entry a leader writes, so if
///   it was committed, the log keeps an entry of the term for good.
pub(super) fn judge(opens: &[Open], seen: &Seen<'_>) -> Vec<String> {
    let mut findings = Vec::new();
    let mut opener: BTreeMap<(u32, u64), usize> = BTreeMap::new();
    for (index, open) in opens.iter().enumerate() {
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
        let before_read = seen
            .before_read
            .get(&shard)
            .is_none_or(|&count| index < count);
        if before_read
            && let Some(terms) = seen.committed.get(&shard)
            && !terms.contains(&term)
        {
            let read = seen.read.get(&shard).map(|read| read.missing(term));
            findings.push(format!(
                "{}: {} opened term {term}, and the final log holds no entry of it, so its \
                 blank entry was never committed{}",
                list(shard),
                name(member),
                read.unwrap_or_default()
            ));
        }
    }
    findings
}

#[cfg(test)]
mod tests;
