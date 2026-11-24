//! The lease allowance of a `Leader` stream: how far a leader may answer
//! writes on its own log, as a majority of its voters has agreed to hold
//! the group to. See `docs/per-shard-raft.md`, "A `Leader` leader's own
//! tail", and `docs/formal/FelixRaftLease.tla`.
//!
//! The leader names an allowance on its appends; a follower keeps the
//! newest one it was named, durably, and echoes it. A voter refuses to
//! elect any candidate whose log could be missing what the allowance let
//! the leader answer, so an answered tail no follower has is never cut.

use std::collections::{BTreeSet, VecDeque};

use felix_storage::raft_log::LogPoint;

use super::types::MemberId;

/// Entries a writing leader asks to be let answer past its log end. The
/// same budget the broker allows uncommitted batches, so a burst rarely
/// waits for a fresh allowance.
pub(crate) const BUDGET: u64 = felix_broker::ANSWERED_AHEAD as u64;

/// The most allowances a leader keeps that a follower may hold in place of
/// the one it echoed. An echo older than all of them counts for nothing.
const NAMED_KEPT: usize = 64;

/// "In term `term` I may answer on my lease entries with an index below
/// `end`." `seq` orders a leader's allowances within its term. `end` 0
/// allows nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Allowance {
    pub(crate) term: u64,
    pub(crate) seq: u64,
    pub(crate) end: u64,
}

impl Allowance {
    fn key(&self) -> (u64, u64) {
        (self.term, self.seq)
    }
}

/// The allowance a follower holds, and the leader that named it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) leader: MemberId,
    pub(crate) allowance: Allowance,
}

impl Held {
    /// Whether `named` from `leader` replaces what `held` holds: a newer
    /// term, or the same term and a later `seq`. A lower `end` in the same
    /// term is taken too: the leader names its own log end once nothing
    /// waits, and everything it answered is in that log.
    pub(crate) fn replaced_by(held: Option<&Held>, named: &Allowance) -> bool {
        held.is_none_or(|held| named.key() > held.allowance.key())
    }

    /// Whether this voter must refuse a vote or pre-vote for `candidate`,
    /// whose last log id is `last`, and must not stand itself if `last` is
    /// its own. The leader that named the allowance is let through under
    /// its own member id: it holds what it answered. Anyone else needs the
    /// allowance's last entry, `(term, end - 1)`, or a later log id.
    pub(crate) fn refuses(&self, candidate: &MemberId, last: Option<LogPoint>) -> bool {
        let Some(needed) = self.allowance.end.checked_sub(1) else {
            return false;
        };
        if *candidate == self.leader {
            return false;
        }
        last.is_none_or(|last| (last.term, last.index) < (self.allowance.term, needed))
    }
}

/// A leader's side: the allowances it named in its term and what its
/// followers echoed.
#[derive(Debug, Default)]
pub(crate) struct Granting {
    term: u64,
    /// The last `seq` named in `term`.
    seq: u64,
    /// Named allowances as `(seq, end)` with both rising front to back. A
    /// follower that echoed `seq` s may since hold any allowance named
    /// after s, so it counts for the lowest of those: the first entry past
    /// s. An allowance with an end at or above a later one never is that.
    named: VecDeque<(u64, u64)>,
    /// Echoes older than this `seq` count for nothing: the allowances
    /// named after them were dropped.
    forgotten: u64,
    echoes: Vec<(MemberId, Allowance)>,
}

impl Granting {
    /// The allowance to name on an append in `term`, for a log that ends at
    /// `log_end` (one past its last index). A leader with writes waiting
    /// asks for its budget past the end; an idle one names its own end,
    /// so an idle group owes nothing.
    pub(crate) fn name(&mut self, term: u64, log_end: u64, writing: bool) -> Allowance {
        if term != self.term {
            *self = Self {
                term,
                ..Self::default()
            };
        }
        self.seq += 1;
        let end = if writing {
            log_end.saturating_add(BUDGET)
        } else {
            log_end
        };
        while self.named.back().is_some_and(|&(_, had)| had >= end) {
            self.named.pop_back();
        }
        self.named.push_back((self.seq, end));
        if self.named.len() > NAMED_KEPT
            && let Some((seq, _)) = self.named.pop_front()
        {
            self.forgotten = seq + 1;
        }
        Allowance {
            term,
            seq: self.seq,
            end,
        }
    }

    /// `from` says it holds `echo`. Echoes of another term than this
    /// leader's, and older echoes than the one kept, are ignored.
    pub(crate) fn echoed(&mut self, from: &MemberId, echo: Allowance) {
        if echo.term != self.term || echo.seq > self.seq {
            return;
        }
        match self.echoes.iter_mut().find(|(member, _)| member == from) {
            Some((_, had)) if had.seq < echo.seq => *had = echo,
            Some(_) => {}
            None => self.echoes.push((from.clone(), echo)),
        }
    }

    /// What `echo` lets the leader count on its sender holding now.
    fn holds(&self, echo: &Allowance) -> u64 {
        if echo.seq < self.forgotten {
            return 0;
        }
        let later = self.named.iter().find(|&&(seq, _)| seq > echo.seq);
        later.map_or(echo.end, |&(_, end)| end.min(echo.end))
    }

    /// The end a majority of every voter set holds in `term`: the leader
    /// may answer on its lease the entries below it. The leader `me`
    /// counts as holding any end, since it holds what it answers. 0 when
    /// `term` is not the term named in.
    pub(crate) fn granted(&self, term: u64, voters: &[BTreeSet<MemberId>], me: &MemberId) -> u64 {
        if term != self.term || voters.is_empty() {
            return 0;
        }
        let per_set = voters.iter().map(|set| {
            let mut held: Vec<u64> = set
                .iter()
                .map(|member| match member == me {
                    true => u64::MAX,
                    false => self
                        .echoes
                        .iter()
                        .find(|(from, _)| from == member)
                        .map_or(0, |(_, echo)| self.holds(echo)),
                })
                .collect();
            held.sort_unstable_by(|a, b| b.cmp(a));
            // The majority's lowest: the (n/2 + 1)th highest.
            held.get(set.len() / 2).copied().unwrap_or(0)
        });
        per_set.min().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
