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
//! Every change here is made under the `flushing` write lock, as a
//! truncation is.

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use openraft::type_config::alias::CommittedLeaderIdOf;
use tokio::sync::OwnedRwLockReadGuard;

use super::purged::{self, Purged};
use super::snapshot::SnapshotStore;
use super::writer::io_error;
use super::{EntryCodec, LogTypes, RaftLog, Shared, generation_of, invalid, term_at, term_of};
use crate::log::{AppendOnlyLog, Offset};

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
        let _flushed = shared.flushing.write().await;
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
    /// The caller holds the `flushing` write lock.
    pub(super) async fn remove_through(&self, purged: Purged) -> io::Result<()> {
        let log = &self.log;
        let dir = log.dir().to_path_buf();
        felix_common::blocking::spawn(move || purged::store(&dir, &purged))
            .await
            .map_err(io::Error::other)??;
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

    /// Purge through `asked`, or with `None` only catch the disk up with
    /// what is hidden: remove what the stored snapshot covers and hide the
    /// rest. The caller holds the `flushing` write lock.
    pub(super) async fn purge_hiding(
        &self,
        snapshots: &SnapshotStore,
        asked: Option<Purged>,
    ) -> io::Result<()> {
        let bounds = *self.bounds.lock();
        let target = match (bounds.purged(), asked) {
            (Some(purged), Some(asked)) if asked.index <= purged.index => purged,
            (_, Some(asked)) => {
                self.check_held(asked).await?;
                asked
            }
            (Some(purged), None) => purged,
            (None, None) => return Ok(()),
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

    /// Let an append that begins at `first` in, or refuse it. Returns the
    /// `flushing` read lock the append holds until its callback has fired.
    ///
    /// Let in: an append that continues the entries on disk, one that
    /// starts right after the id in `raft-purged` or right after the stored
    /// snapshot when no entry is above them, and the first append to a log
    /// that has no entry, no purged id and no snapshot at all, which has
    /// nothing to leave a hole after.
    pub(super) async fn admit(
        self: &Arc<Self>,
        snapshots: &SnapshotStore,
        first: Offset,
    ) -> io::Result<OwnedRwLockReadGuard<()>> {
        let log = &self.log;
        let reading = Arc::clone(&self.flushing).read_owned().await;
        let tail = log.tail_offset().await.map_err(io_error)?;
        if self.bounds.lock().hidden.is_none() && first == tail && tail >= self.first_on_disk() {
            return Ok(reading);
        }
        drop(reading);

        // What follows may empty the log, which a flush in flight must not
        // find gone. openraft sends one write at a time, so nothing else
        // gets in between the two locks.
        let writing = Arc::clone(&self.flushing).write_owned().await;
        self.purge_hiding(snapshots, None).await?;
        let tail = log.tail_offset().await.map_err(io_error)?;
        let bounds = *self.bounds.lock();
        let covered = stored(snapshots)?;
        let refused = |why: &str| {
            Err(invalid(format!(
                "{}: an append at index {first} {why} (entries on disk end at {tail}, purged \
                 through {:?} on disk and {:?} in memory, snapshot through {:?})",
                log.label(),
                bounds.on_disk.map(|purged| purged.index),
                bounds.hidden.map(|purged| purged.index),
                covered.map(|stored| stored.index),
            )))
        };
        if let Some(hidden) = bounds.hidden {
            if first <= hidden.index {
                return refused("is at or below the purged id");
            }
            if first != tail {
                return refused(
                    "would leave a log that starts after its snapshot: the entries below it \
                     were purged past the stored snapshot",
                );
            }
        } else if tail > self.first_on_disk() {
            if first != tail {
                return refused("does not continue the log");
            }
        } else {
            match (bounds.on_disk, covered) {
                (Some(purged), _) if purged.index + 1 == first => {}
                (purged, Some(stored))
                    if stored.index + 1 == first
                        && purged.is_none_or(|purged| purged.index < stored.index) =>
                {
                    self.remove_through(stored).await?;
                }
                (None, None) if first >= tail => {}
                _ => return refused("follows neither the purged id nor the stored snapshot"),
            }
            if first > log.tail_offset().await.map_err(io_error)? {
                log.reset_to(first).await.map_err(io_error)?;
            }
        }
        Ok(writing.downgrade())
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
