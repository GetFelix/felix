//! What a shard's group has committed, as this member's state machine
//! applied it, and the wait of a write for its commit.
//!
//! The state machine publishes here once per run of applied entries. The
//! read bound and the commit wait read this, never openraft's metrics, which
//! change on every heartbeat answer and would wake every waiter. See
//! `docs/per-shard-raft.md`, "Reads" and "Acknowledging a write".

use std::collections::VecDeque;
use std::time::Duration;

use felix_storage::raft_log::LogPoint;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::propose::{self, Leading};
use crate::engine::{CommitError, Written};

/// Changes of term remembered. A wait asks about a record written moments
/// ago, so only a group that changed leader this often meanwhile forgets
/// it, and that wait is answered as an unknown outcome.
const TERMS_KEPT: usize = 8;

/// Runs of entries that hold no record remembered. One is written per term
/// and per membership change; a batch committed past more than this many
/// runs back reports none skipped, which a subscriber reads as a drop.
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
        }
    }

    pub(crate) fn applied(&self) -> Option<LogPoint> {
        self.applied
    }

    /// One past the last committed offset this member knows of.
    pub(crate) fn read_bound(&self) -> u64 {
        self.applied.map_or(0, |applied| applied.index + 1)
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

    /// How many offsets just below `first` hold no record, so a subscriber
    /// whose next record is at `first` is told they were skipped, not
    /// dropped. Asked once everything below `first` is applied.
    pub(crate) fn skipped_before(&self, first: u64) -> u64 {
        self.quiet
            .iter()
            .rev()
            .find(|run| run.end == first)
            .map_or(0, |run| run.end - run.start)
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
    mut commits: watch::Receiver<Commits>,
    mut leading: impl Leading,
    stop: CancellationToken,
    timeout: Duration,
) -> Result<(), CommitError> {
    let Some(index) = written.end.checked_sub(1) else {
        return Err(propose::lost(NO_RECORD));
    };
    let term = written.generation;
    let reached = async {
        let commits = commits
            .wait_for(|commits| commits.term_at(index) != TermAt::Pending)
            .await
            .ok()?;
        Some(commits.term_at(index))
    };
    tokio::select! {
        biased;
        at = reached => match at {
            Some(TermAt::Term(at)) if at == term => Ok(()),
            Some(TermAt::Term(_)) => Err(propose::lost(DISCARDED)),
            Some(TermAt::Forgotten) => Err(propose::lost(FORGOTTEN)),
            Some(TermAt::Pending) | None => Err(propose::stopped()),
        },
        () = leading.lost(Some(term)) => Err(propose::lost(NOT_LEADING)),
        () = stop.cancelled() => Err(propose::stopped()),
        () = tokio::time::sleep(timeout) => Err(CommitError::TimedOut {
            what: "batch",
            timeout,
        }),
    }
}
