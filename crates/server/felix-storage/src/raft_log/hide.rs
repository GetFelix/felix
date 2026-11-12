//! The purge rule of a group that must never restart on a log that begins
//! after its snapshot ([`RaftLog::purge_hides_past`]).
//!
//! A purge removes entries from disk only up to the stored snapshot. Past
//! it, the id is *hidden*: kept in memory, and seen by the log state, reads,
//! truncation and the committed id exactly as a purged id is, while the
//! entries stay on disk. A crash forgets a hidden id and brings its entries
//! back, so what restarts is a log with no entry missing above its snapshot.
//!
//! The next purge or append removes a hidden id's entries for real once a
//! stored snapshot covers it. Until then an append may only continue the
//! entries on disk. One that would start past them is refused: on disk it
//! would leave a hole that nothing covers.
//!
//! Every change here waits first for every append's callback, as a
//! truncation does.

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use super::purged::{self, Purged};
use super::snapshot::SnapshotStore;
use super::writer::io_error;
use super::{EntryCodec, LogTypes, RaftLog, Shared, generation_of, invalid, term_at, term_of};
use crate::log::{AppendOnlyLog, Offset};
use openraft::type_config::alias::CommittedLeaderIdOf;

/// What a purge past the stored snapshot does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PastSnapshot {
    Refuse,
    Hide,
}

/// The snapshot that bounds a log's purges.
pub(super) struct Covering {
    pub(super) snapshots: SnapshotStore,
    pub(super) past: PastSnapshot,
}

/// The last purged entry as `raft-purged` holds it, and a later one that
/// only this process knows.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Bounds {
    pub(super) on_disk: Option<Purged>,
    /// Always past `on_disk`.
    pub(super) hidden: Option<Purged>,
}

impl Bounds {
    /// The purged id as every reader of the log sees it.
    pub(super) fn purged(&self) -> Option<Purged> {
        self.hidden.or(self.on_disk)
    }
}

impl<C, K> RaftLog<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    /// Hide any purge past the snapshot `snapshots` holds, and hold every
    /// append to what that leaves on disk. The rule is in the module docs
    /// of `raft_log::hide` and in `docs/storage-format.md`, "Purging".
    ///
    /// For a group whose state machine is rebuilt from the snapshot: the
    /// metadata store. Fails if the log is already purged past the
    /// snapshot, or a snapshot was already named.
    pub fn purge_hides_past(&self, snapshots: &SnapshotStore) -> io::Result<()> {
        self.bound_purges(snapshots, PastSnapshot::Hide)
    }

    /// Empty the log when the stored snapshot covers its last entry, so it
    /// starts right after the snapshot. A snapshot install ends with this
    /// purge; a crash before it leaves it for the next open.
    ///
    /// Refused when the log's last entry is at the snapshot's index with
    /// another term.
    pub async fn start_after_snapshot(&self, snapshots: &SnapshotStore) -> io::Result<()> {
        let shared = &self.shared;
        shared.flushes.drained().await;
        let Some(stored) = stored(snapshots)? else {
            return Ok(());
        };
        let on_disk = shared.bounds.lock().on_disk;
        let tail = shared.log.tail_offset().await.map_err(io_error)?;
        if tail > stored.index + 1 || on_disk.is_some_and(|purged| purged.index >= stored.index) {
            return Ok(());
        }
        shared.check_held(stored).await?;
        shared.remove_through(stored).await
    }
}

impl<K> Shared<K> {
    /// The snapshot store a hidden purge is measured against, when this log
    /// hides.
    pub(super) fn hides(&self) -> Option<&SnapshotStore> {
        self.covering
            .get()
            .filter(|covering| covering.past == PastSnapshot::Hide)
            .map(|covering| &covering.snapshots)
    }

    /// Where the entries on disk start: one past the id in `raft-purged`,
    /// whatever is hidden.
    fn first_on_disk(&self) -> Offset {
        let base = self.log.base_offset();
        match self.bounds.lock().on_disk {
            Some(purged) => base.max(purged.index + 1),
            None => base,
        }
    }

    /// Fails when the log holds an entry at `id`'s index with another term.
    pub(super) async fn check_held(&self, id: Purged) -> io::Result<()> {
        let log = &self.log;
        let tail = log.tail_offset().await.map_err(io_error)?;
        if id.index < self.first_on_disk() || id.index >= tail {
            return Ok(());
        }
        let held = term_at(log.writer_at(id.index).await.map_err(io_error)?, id.index)?;
        let asked = term_of(Some(id.generation), id.index)?;
        if held != asked {
            return Err(invalid(format!(
                "{}: asked to purge through index {} of term {asked}, but the entry there is of term {held}",
                log.label(),
                id.index
            )));
        }
        Ok(())
    }

    /// Purge through `purged` on disk: the id first, then the segments.
    /// The caller has waited for every append's callback.
    pub(super) async fn remove_through(&self, purged: Purged) -> io::Result<()> {
        let log = &self.log;
        let dir = log.dir().to_path_buf();
        // Refused once closed: the directory may already be another log's.
        let writer = log.clone();
        felix_common::blocking::spawn(move || {
            writer.write_while_open(|| purged::store(&dir, &purged))
        })
        .await
        .map_err(io::Error::other)?
        .map_err(io_error)?;
        {
            let mut bounds = self.bounds.lock();
            bounds.on_disk = Some(purged);
            if bounds
                .hidden
                .is_some_and(|hidden| hidden.index <= purged.index)
            {
                bounds.hidden = None;
            }
        }
        let first: Offset = purged.index + 1;
        if first >= log.tail_offset().await.map_err(io_error)? {
            log.reset_to(first).await.map_err(io_error)
        } else {
            log.trim_before(first).await.map(drop).map_err(io_error)
        }
    }

    /// Purge through `asked`: remove what the stored snapshot covers and
    /// hide the rest. An id at or below the purged id asks for nothing new,
    /// and still removes what a snapshot has come to cover since. The caller
    /// has waited for every append's callback.
    pub(super) async fn purge_hiding(
        &self,
        snapshots: &SnapshotStore,
        asked: Purged,
    ) -> io::Result<()> {
        let bounds = *self.bounds.lock();
        let target = match bounds.purged() {
            Some(purged) if asked.index <= purged.index => purged,
            _ => {
                self.check_held(asked).await?;
                asked
            }
        };
        // The snapshot's own id when it stops short of the target.
        let removable = match stored(snapshots)? {
            Some(stored) if stored.index < target.index => Some(stored),
            Some(_) => Some(target),
            None => None,
        };
        if let Some(removable) = removable
            && bounds
                .on_disk
                .is_none_or(|on_disk| on_disk.index < removable.index)
        {
            if removable != target {
                self.check_held(removable).await?;
            }
            self.remove_through(removable).await?;
        }
        let mut bounds = self.bounds.lock();
        bounds.hidden = bounds
            .on_disk
            .is_none_or(|on_disk| on_disk.index < target.index)
            .then_some(target);
        Ok(())
    }

    /// Let an append that begins at `first` in, or refuse it with the log
    /// untouched. The rule is [`admission`].
    pub(super) async fn admit(
        self: &Arc<Self>,
        snapshots: &SnapshotStore,
        first: Offset,
    ) -> io::Result<()> {
        let log = &self.log;
        let tail = log.tail_now();
        if self.bounds.lock().hidden.is_none() && first == tail && tail > self.first_on_disk() {
            return Ok(());
        }

        // What follows may empty the log, which a flush in flight must not
        // find gone. openraft sends one write at a time, so no append is
        // queued behind this one.
        self.flushes.drained().await;
        let tail = log.tail_offset().await.map_err(io_error)?;
        let bounds = *self.bounds.lock();
        let covered = stored(snapshots)?;
        // Decided before anything is removed, so a refusal changes nothing.
        let purge =
            admission(bounds, covered, self.first_on_disk(), tail, first).map_err(|why| {
                invalid(format!(
                    "{}: an append at index {first} {why} (entries on disk end at {tail}, purged \
                 through {:?} on disk and {:?} in memory, snapshot through {:?})",
                    log.label(),
                    bounds.on_disk.map(|purged| purged.index),
                    bounds.hidden.map(|purged| purged.index),
                    covered.map(|stored| stored.index),
                ))
            })?;
        if let Some(purge) = purge {
            self.check_held(purge).await?;
            self.remove_through(purge).await?;
        }
        if first > log.tail_offset().await.map_err(io_error)? {
            log.reset_to(first).await.map_err(io_error)?;
        }
        Ok(())
    }
}

/// Whether an append that begins at `first` may be written, and the id to
/// purge through on disk before it is. `Err` says why not.
///
/// `first_on_disk..tail` are the entries on disk. Let in:
/// - an append that continues them, unless a hidden id says they are
///   purged and no stored snapshot covers it;
/// - with no entry left above the purged id, one that starts right after
///   it or right after the stored snapshot;
/// - the first append to a log with no entry, no purged id and no stored
///   snapshot, which has nothing to leave a hole after.
///
/// A hidden id the stored snapshot covers counts as purged on disk, and is
/// the id returned.
fn admission(
    bounds: Bounds,
    covered: Option<Purged>,
    first_on_disk: Offset,
    tail: Offset,
    first: Offset,
) -> Result<Option<Purged>, &'static str> {
    const NO_HOLE: &str = "would leave a log that starts after its snapshot: the entries below it \
                           were purged past the stored snapshot";
    const NOT_NEXT: &str = "does not continue the log";
    let hidden = match bounds.hidden {
        Some(hidden) if first <= hidden.index => return Err("is at or below the purged id"),
        Some(hidden) if covered.is_none_or(|stored| stored.index < hidden.index) => {
            // Stays hidden, so the entries on disk must reach this append.
            return if first == tail {
                Ok(None)
            } else {
                Err(NO_HOLE)
            };
        }
        hidden => hidden,
    };
    let purged = hidden.or(bounds.on_disk);
    let first_kept = purged.map_or(first_on_disk, |purged| first_on_disk.max(purged.index + 1));
    if tail > first_kept {
        return if first == tail {
            Ok(hidden)
        } else {
            Err(NOT_NEXT)
        };
    }
    match (purged, covered) {
        (Some(purged), _) if purged.index + 1 == first => Ok(hidden),
        (purged, Some(stored))
            if stored.index + 1 == first
                && purged.is_none_or(|purged| purged.index < stored.index) =>
        {
            Ok(Some(stored))
        }
        (None, None) if first >= tail => Ok(None),
        _ => Err("follows neither the purged id nor the stored snapshot"),
    }
}

/// The last entry the stored snapshot covers, as a purged id.
fn stored(snapshots: &SnapshotStore) -> io::Result<Option<Purged>> {
    snapshots
        .covers()
        .map(|last| {
            Ok(Purged {
                index: last.index,
                generation: generation_of(last.term)?,
            })
        })
        .transpose()
}
