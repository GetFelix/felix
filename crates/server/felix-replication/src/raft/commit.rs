//! What a shard's group has committed, as this member's state machine
//! applied it, and the wait of a write for its commit.
//!
//! The state machine publishes here once per run of applied entries. The
//! read bound and the commit wait read this, never openraft's metrics, which
//! change on every heartbeat answer and would wake every waiter. See
//! `docs/per-shard-raft.md`, "Reads" and "Acknowledging a write".

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use felix_storage::raft_log::LogPoint;
use felix_storage::waiters::Waiters;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::propose::{self, Leading, Lost};
use crate::engine::{CommitError, Written};

/// Changes of term remembered. A wait asks about a record written moments
/// ago, so only a group that changed leader this often meanwhile forgets
/// it, and that wait is answered as an unknown outcome.
const TERMS_KEPT: usize = 8;

/// Runs of entries that hold no record remembered, for [`Commits::records_end`].
/// One is written per term, per membership change and per group write.
const QUIET_KEPT: usize = 64;

const NOT_LEADING: &str = "this broker stopped leading the shard";
const DISCARDED: &str = "another leader's entry committed at the offset";
const FORGOTTEN: &str = "the term committed at the offset is no longer known here";
const NO_RECORD: &str = "the write names no record";

/// The committed entries this member has applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Commits {
    applied: Option<LogPoint>,
    /// The first applied entry of each term, oldest first. Terms never go
    /// down along a log, so the entry at an index has the term of the last
    /// start at or below it.
    starts: VecDeque<LogPoint>,
    /// Runs of applied entries that hold no record (a leader's blank entry,
    /// a membership change), as `start..end`, oldest first.
    quiet: VecDeque<std::ops::Range<u64>>,
    /// The conditional batch the applied entries end inside, undecided until
    /// its last piece or another entry is applied. See `log::conditional`.
    batch: Option<OpenBatch>,
}

/// A conditional batch whose pieces from `first` up to the last applied
/// entry are applied and whose last piece is not. `first` need not be its
/// first piece: a log that lost the batch's start (a purge, an install)
/// holds only the rest, and `resolve` reads that rest from where it begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenBatch {
    pub(crate) first: u64,
    /// The position the next piece must have.
    pub(crate) next: u32,
    pub(crate) len: u32,
}

/// What an applied entry holds, as [`Commits::apply`] counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Holds {
    Record,
    /// A record of a conditional batch, at `position` of `len`.
    Piece {
        position: u32,
        len: u32,
    },
    Nothing,
}

/// How many offsets just below each applied record hold no record, as the
/// apply counted them walking its run, kept until no publisher can ask.
///
/// A publisher claims before it proposes and asks after its commit, so
/// nothing it needs is dropped however many runs land in between: an
/// offset-counting subscriber reads a count that is short as a drop.
#[derive(Debug, Clone, Default)]
pub(crate) struct Skips(Arc<parking_lot::Mutex<SkipTable>>);

// Sorted vectors, which keep their memory between publishes: a node-based
// map would allocate on every one.
#[derive(Debug, Default)]
struct SkipTable {
    /// The read bound each live claim saw, with how many saw it.
    claims: Vec<(u64, usize)>,
    /// Offsets of records with entries holding no record just below them,
    /// and how many.
    below: Vec<(u64, u64)>,
}

/// A publisher's hold on [`Skips`] for the records it is about to propose.
#[derive(Debug)]
pub(crate) struct SkipClaim {
    skips: Skips,
    floor: u64,
    released: bool,
}

impl Skips {
    /// Hold every count from the read bound `commits` shows on. Taken
    /// before the proposal, whose records all land at or above it.
    pub(crate) fn claim(&self, commits: &watch::Receiver<Commits>) -> SkipClaim {
        let mut table = self.0.lock();
        // Read under the lock, so a run published meanwhile either sees
        // this claim or moved the bound past what it drops.
        let floor = commits.borrow().read_bound();
        match table.claims.binary_search_by_key(&floor, |claim| claim.0) {
            Ok(at) => table.claims[at].1 += 1,
            Err(at) => table.claims.insert(at, (floor, 1)),
        }
        SkipClaim {
            skips: self.clone(),
            floor,
            released: false,
        }
    }

    /// Add a run's counts and `publish` it, which says whether it moved
    /// the commits. `bound` is the read bound as published before the run:
    /// counts below it and below every claim can no longer be asked for.
    ///
    /// The table and the commits move under the lock; the caller wakes
    /// waiters on the result after it, so a wakeup never queues a claim
    /// and a woken publisher still finds its count.
    pub(crate) fn applied(
        &self,
        bound: u64,
        run: &[(u64, u64)],
        publish: impl FnOnce() -> bool,
    ) -> bool {
        let mut table = self.0.lock();
        let floor = table
            .claims
            .first()
            .map_or(bound, |(floor, _)| bound.min(*floor));
        let dead = table.below.partition_point(|(at, _)| *at < floor);
        table.below.drain(..dead);
        table.below.extend_from_slice(run);
        publish()
    }
}

impl SkipClaim {
    /// How many offsets just below the record at `first` hold no record,
    /// and the claim released. Asked once everything below `first` is
    /// applied.
    pub(crate) fn skipped_before(mut self, first: u64) -> u64 {
        let mut table = self.skips.0.lock();
        let count = table
            .below
            .binary_search_by_key(&first, |entry| entry.0)
            .map_or(0, |at| table.below[at].1);
        self.release(&mut table);
        self.released = true;
        count
    }

    fn release(&self, table: &mut SkipTable) {
        if self.released {
            return;
        }
        if let Ok(at) = table
            .claims
            .binary_search_by_key(&self.floor, |claim| claim.0)
        {
            table.claims[at].1 -= 1;
            if table.claims[at].1 == 0 {
                table.claims.remove(at);
            }
        }
    }
}

impl Drop for SkipClaim {
    fn drop(&mut self) {
        let mut table = self.skips.0.lock();
        self.release(&mut table);
    }
}

/// The term of the committed entry at an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TermAt {
    /// Nothing at the index has been applied here.
    Pending,
    Term(u64),
    /// Applied, but from before the terms this member remembers.
    Forgotten,
}

impl Commits {
    /// A member that starts with `applied` as its last applied entry.
    pub(crate) fn resumed(applied: Option<LogPoint>) -> Self {
        Self {
            applied,
            starts: applied.into_iter().collect(),
            quiet: VecDeque::new(),
            batch: None,
        }
    }

    /// `self` with the last applied entry inside `batch`, as a member that
    /// resumes there reads it back from its log.
    pub(crate) fn in_batch(mut self, batch: Option<OpenBatch>) -> Self {
        self.batch = batch;
        self
    }

    pub(crate) fn applied(&self) -> Option<LogPoint> {
        self.applied
    }

    /// How far a reader may go: one past the last applied entry, or the
    /// first piece of the conditional batch the applied entries end inside,
    /// so no reader sees part of a batch. This is `log::conditional::bound`
    /// over the applied log.
    pub(crate) fn read_bound(&self) -> u64 {
        self.batch
            .map_or_else(|| self.applied_end(), |batch| batch.first)
    }

    /// One past the last applied entry, for a wait on this member having
    /// applied an index. A batch holding the read bound back must not hold
    /// that wait.
    pub(crate) fn applied_end(&self) -> u64 {
        self.applied.map_or(0, |applied| applied.index + 1)
    }

    /// The entry at `at`, holding `holds`, was applied, after every entry
    /// below it. `quiet` is the run of entries holding no record that ends
    /// at the last applied record, kept by the caller across runs; a record
    /// with such a run just below it is pushed to `skipped` with its length.
    ///
    /// A piece counts as a record only once its batch is whole. A batch cut
    /// by another entry where its next piece belongs holds no record, so its
    /// pieces join the run below the next record. Both follow
    /// `log::conditional::resolve`, which disk reads apply to the same log.
    pub(crate) fn apply(
        &mut self,
        at: LogPoint,
        holds: Holds,
        quiet: &mut std::ops::Range<u64>,
        skipped: &mut Vec<(u64, u64)>,
    ) {
        let index = at.index;
        if self.applied.is_some_and(|applied| index <= applied.index) {
            return;
        }
        self.advance(at);
        if let Some(open) = self.batch.take() {
            match holds {
                Holds::Piece { position, len } if len == open.len && position == open.next => {
                    if position + 1 < len {
                        self.batch = Some(OpenBatch {
                            next: position + 1,
                            ..open
                        });
                        return;
                    }
                    // Whole: the run below the batch is skipped before its
                    // first record.
                    if quiet.end == open.first && !quiet.is_empty() {
                        skipped.push((open.first, open.first - quiet.start));
                    }
                    *quiet = index + 1..index + 1;
                    return;
                }
                _ => {
                    for piece in open.first..index {
                        self.holds_no_record(piece);
                    }
                    if quiet.end != open.first || quiet.is_empty() {
                        quiet.start = open.first;
                    }
                    quiet.end = index;
                }
            }
        }
        let record = match holds {
            Holds::Record => true,
            Holds::Piece { position, len } if position + 1 < len => {
                self.batch = Some(OpenBatch {
                    first: index,
                    next: position + 1,
                    len,
                });
                return;
            }
            // A last piece with no batch below it is kept, as `resolve`
            // keeps it. One past its batch's length is no batch's.
            Holds::Piece { position, len } => position < len,
            Holds::Nothing => false,
        };
        if record {
            if quiet.end == index && !quiet.is_empty() {
                skipped.push((index, index - quiet.start));
            }
            *quiet = index + 1..index + 1;
        } else {
            self.holds_no_record(index);
            if quiet.end != index {
                quiet.start = index;
            }
            quiet.end = index + 1;
        }
    }

    /// The entry at `at` was applied. Entries are applied in log order.
    pub(crate) fn advance(&mut self, at: LogPoint) {
        if self
            .applied
            .is_some_and(|applied| at.index <= applied.index)
        {
            return;
        }
        if self.starts.back().is_none_or(|start| start.term != at.term) {
            if self.starts.len() == TERMS_KEPT {
                self.starts.pop_front();
            }
            self.starts.push_back(at);
        }
        self.applied = Some(at);
    }

    /// The entry at `index`, just applied, holds no record.
    pub(crate) fn holds_no_record(&mut self, index: u64) {
        match self.quiet.back_mut() {
            Some(run) if run.end == index => run.end = index + 1,
            Some(run) if run.end > index => {}
            _ => {
                if self.quiet.len() == QUIET_KEPT {
                    self.quiet.pop_front();
                }
                self.quiet.push_back(index..index + 1);
            }
        }
    }

    /// `end` less the run of applied entries just below it that hold no
    /// record. `end` need not be the bound now: a run that grew since still
    /// covers it.
    pub(crate) fn records_end(&self, end: u64) -> u64 {
        self.quiet
            .iter()
            .rev()
            .find(|run| run.start < end && end <= run.end)
            .map_or(end, |run| run.start)
    }

    pub(crate) fn term_at(&self, index: u64) -> TermAt {
        if self.applied.is_none_or(|applied| index > applied.index) {
            return TermAt::Pending;
        }
        self.starts
            .iter()
            .rev()
            .find(|start| start.index <= index)
            .map_or(TermAt::Forgotten, |start| TermAt::Term(start.term))
    }
}

/// Wait until the record just below `written.end` is committed as the entry
/// `written.generation` wrote there.
///
/// The commit index passing the offset is not enough: a leader deposed
/// before its entry reached a majority has the offset taken by the next
/// leader's entry. An error is an unknown outcome, never "not written".
pub(crate) async fn committed(
    written: Written,
    commits: watch::Receiver<Commits>,
    applied: Waiters<u64>,
    mut leading: impl Leading,
    stop: CancellationToken,
    timeout: Duration,
) -> Result<(), CommitError> {
    let Some(index) = written.end.checked_sub(1) else {
        return Err(propose::lost(NO_RECORD));
    };
    let term = written.generation;
    let reached = async {
        let term_at =
            |commits: &Commits| Some(commits.term_at(index)).filter(|at| *at != TermAt::Pending);
        applied.wait_for(&commits, index, term_at).await.ok()
    };
    tokio::select! {
        biased;
        at = reached => match at {
            Some(TermAt::Term(at)) if at == term => Ok(()),
            Some(TermAt::Term(_)) => Err(propose::lost(DISCARDED)),
            Some(TermAt::Forgotten) => Err(propose::lost(FORGOTTEN)),
            Some(TermAt::Pending) | None => Err(propose::stopped()),
        },
        why = leading.lost(Some(term)) => Err(match why {
            Lost::Stopped => propose::stopped(),
            Lost::Deposed | Lost::Electing => propose::lost(NOT_LEADING),
        }),
        () = stop.cancelled() => Err(propose::stopped()),
        () = tokio::time::sleep(timeout) => Err(CommitError::TimedOut {
            what: "batch",
            timeout,
        }),
    }
}
