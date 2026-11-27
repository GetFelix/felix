//! The check a learner of a re-formed stream group runs on every entry it
//! pulls from a voter below the gap, before any of them is kept. The pull
//! fills a staging log with `floor..=base`; the staging log is adopted only
//! once [`BaseCheck::complete`] holds, so a learner never takes part of the
//! base, a hole in it, or an entry of another history.
//!
//! The pull and the install that use it are issue #3005.

use super::epoch_file::{EpochBase, EpochLogId};

/// Where a pull of the base stands. Fed every pulled entry in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseCheck {
    base_last: Option<EpochLogId>,
    /// The next index the staging log is to take.
    next: u64,
    /// The term of the last entry kept, since terms never fall along a log.
    last_term: u64,
}

/// Why a pulled entry was refused. Any of them discards the staging log: the
/// voter that sent it holds another history, or sent it out of order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mismatch {
    /// Not the next index: a hole or a repeat.
    Index { wanted: u64, got: u64 },
    /// Past the base's last index: in the gap, which holds nothing.
    PastBase { index: u64 },
    /// A term below the entry before it.
    TermFalls { index: u64, term: u64, before: u64 },
    /// A term above the base's last, which no entry of the base has.
    TermPastBase { index: u64, term: u64 },
    /// The base's last entry in another term than the base names.
    LastTerm { term: u64, wanted: u64 },
}

impl BaseCheck {
    /// A pull of `base` from `floor`, the lowest offset the voters serve.
    /// `None` when there is nothing to pull: an empty base, or a floor past
    /// its end.
    pub fn new(base: &EpochBase, floor: u64) -> Option<Self> {
        let last = base.base_last?;
        (floor <= last.index).then_some(Self {
            base_last: Some(last),
            next: floor,
            last_term: 0,
        })
    }

    /// The index the next pulled entry must have.
    pub fn next(&self) -> u64 {
        self.next
    }

    /// Take the entry at `index`, written in `term`.
    pub fn take(&mut self, index: u64, term: u64) -> Result<(), Mismatch> {
        let Some(last) = self.base_last else {
            return Err(Mismatch::PastBase { index });
        };
        if index > last.index {
            return Err(Mismatch::PastBase { index });
        }
        if index != self.next {
            return Err(Mismatch::Index {
                wanted: self.next,
                got: index,
            });
        }
        if term < self.last_term {
            return Err(Mismatch::TermFalls {
                index,
                term,
                before: self.last_term,
            });
        }
        if term > last.term {
            return Err(Mismatch::TermPastBase { index, term });
        }
        if index == last.index && term != last.term {
            return Err(Mismatch::LastTerm {
                term,
                wanted: last.term,
            });
        }
        self.next = index + 1;
        self.last_term = term;
        Ok(())
    }

    /// The voters' floor moved to `floor` under the pull. True when the
    /// staging log is to be emptied and the pull restarted at `floor`: what
    /// it holds is below the floor, so keeping it would leave a hole between
    /// it and the next entry a voter still serves.
    pub fn raise_floor(&mut self, floor: u64) -> bool {
        if floor <= self.next {
            return false;
        }
        self.next = floor;
        self.last_term = 0;
        true
    }

    /// Every entry of `floor..=base` was taken, in order, the last in the
    /// base's term. Only then is the staging log adopted.
    pub fn complete(&self) -> bool {
        self.base_last
            .is_some_and(|last| self.next == last.index + 1 && self.last_term == last.term)
    }
}

#[cfg(test)]
mod tests;
