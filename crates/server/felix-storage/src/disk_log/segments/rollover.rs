//! Rolling the active segment over to a new one.
//!
//! Two ways to do it. [`SegmentSet::roll`] seals and creates in one step and
//! holds the caller's lock across every fsync. The split
//! [`SegmentSet::roll_plan`] / [`RollPlan::build`] / [`SegmentSet::commit_roll`]
//! builds the replacement with no lock held and swaps it in under one held
//! only long enough to move a pointer.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::SegmentSet;
use crate::Result;
use crate::disk_log::now_micros;
use crate::disk_log::sealed::{SealedEntry, SealedHandle};
use crate::log::{AppendRecord, SegmentId};
use crate::segment::writer::BlankSegment;
use crate::segment::{SegmentReader, SegmentWriter, index_file_name};
use crate::{StorageError, metrics_names};

impl SegmentSet {
    /// Whether appending `records` requires an *inline* rollover — the blocking
    /// kind, which seals and fsyncs while holding the caller's lock.
    ///
    /// `roll_pending` says a background rollover is already building the
    /// replacement. When it is, the ceiling rises to `max_overshoot_percent`
    /// above the configured size, and this is the crux of why the background
    /// roll works at all:
    ///
    /// The gap between the soft threshold and the hard limit is spent in
    /// *appends*, which cost microseconds. Building a replacement segment costs
    /// two *fsyncs*, which cost milliseconds. Hold the configured size as a hard
    /// ceiling and the inline path always wins that race, so every roll blocks
    /// exactly as it did before and the preparation is discarded unused —
    /// measurably worse than not trying. Letting the segment overshoot while its
    /// replacement is built is what buys the preparation enough time to finish.
    ///
    /// The overshoot is bounded rather than open-ended, because
    /// `segment_size_bytes` is also what bounds recovery time: the active
    /// segment is the one scanned in full at startup. Past the bound, appends
    /// roll inline and take the latency hit, which is the correct trade when the
    /// alternative is an unboundedly large segment to re-scan after a crash.
    ///
    /// A record the active segment's version cannot hold also rolls it: a
    /// segment left over from an older build is never given a flag bit that
    /// build would misread.
    pub(crate) fn would_roll_within(&self, records: &[AppendRecord], roll_pending: bool) -> bool {
        let full = self.active.projected_size(records) > self.size_ceiling(roll_pending)
            && self.active.record_count() > 0;
        full || !self.active.holds(records)
    }

    /// Whether the active segment has crossed the point where a rollover should
    /// be started in the background.
    ///
    /// Deliberately below `segment_size_bytes`: the roll is prepared while
    /// there is still room, so appends keep landing in the current segment
    /// instead of waiting for one to be built. That makes the configured size a
    /// target rather than a hard ceiling — a segment can overshoot slightly
    /// while its replacement is being created.
    pub(crate) fn should_prepare_roll(&self) -> bool {
        // 100 means "never roll early", and it is the default. Without this the
        // comparison below still fires whenever the segment has *reached* its
        // configured size -- which an exactly fitting batch or a single
        // oversized record does -- so the documented way to disable the
        // background roll would quietly enter it anyway.
        if self.config.rollover_threshold_percent >= 100 {
            return false;
        }
        if self.active.record_count() == 0 {
            return false;
        }
        let threshold = self
            .config
            .segment_size_bytes
            .saturating_mul(self.config.rollover_threshold_percent.min(100) as u64)
            / 100;
        self.active.size_bytes() >= threshold.max(1)
    }

    /// Build a replacement segment for the current tail, touching no shared
    /// state.
    ///
    /// This is the expensive half of a rollover — creating the file, writing
    /// and syncing its header, syncing the directory entry — and it runs with
    /// no lock held. `commit_roll` then swaps it in under a lock held only long
    /// enough to move a pointer.
    ///
    /// This half is deliberately trivial: it copies a path and bumps a counter.
    /// Everything expensive belongs to [`RollPlan::build`], which must run with
    /// no lock held — creating a segment fsyncs its header *and* its parent
    /// directory, and holding even a read lock across that blocks every append
    /// waiting for the write lock, which is the stall this whole design exists
    /// to remove.
    pub(crate) fn roll_plan(&self) -> RollPlan {
        RollPlan {
            dir: self.dir.clone(),
            id: self.next_segment_id.fetch_add(1, Ordering::AcqRel),
            previous_active_id: self.active.id(),
            preallocate_bytes: self.config.preallocate_bytes(),
            index_spacing_bytes: self.config.index_spacing_bytes,
        }
    }

    /// Swap a prepared segment in, returning the retired writer to be sealed.
    ///
    /// Fast by construction: a pointer swap plus a descriptor built from
    /// in-memory state. The retired segment joins the sealed list immediately,
    /// so reads never see a gap while it is still being flushed — its
    /// descriptor already describes every record it holds, because
    /// preallocation reserves blocks without extending the file, so a segment's
    /// length always equals the bytes actually written to it.
    ///
    /// The replacement takes its base offset *here*, from the tail as it stands
    /// at the swap. That is what lets appends keep landing in the old segment
    /// for as long as the preparation takes: however far the tail has moved,
    /// the replacement still continues it exactly, so no gap is possible and no
    /// preparation is wasted for having been built a moment too early.
    ///
    /// Returns [`RollOutcome::Stale`] only when another roll already replaced
    /// the segment this one was built to retire.
    pub(crate) fn commit_roll(&mut self, prepared: PreparedSegment) -> Result<RollOutcome> {
        // Rolling an empty segment would push a `SealedEntry` with no
        // `last_offset` to describe, and gains nothing. A closed log installs
        // nothing, so the caller deletes the prepared files.
        if prepared.previous_active_id != self.active.id()
            || self.active.record_count() == 0
            || self.check_open().is_err()
        {
            return Ok(RollOutcome::Stale(prepared));
        }
        let descriptor = self.active.descriptor();
        // One page-cache write, no flush: see `BlankSegment::activate`.
        let replacement = prepared.blank.activate(
            self.active.next_offset(),
            now_micros(),
            self.active.successor_version(&[]),
        )?;
        self.bump_next_segment_id(replacement.id() + 1);
        let retired = std::mem::replace(&mut self.active, replacement);
        self.active_reader = Arc::new(SegmentReader::open(
            self.active.path(),
            self.active.id(),
            self.active.base_offset(),
        )?);

        // Guaranteed non-empty by the check above, so `last_offset` is real.
        self.push_sealed(descriptor, &retired)?;

        metrics::counter!(metrics_names::SEGMENT_ROLL_TOTAL).increment(1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(RollOutcome::Installed(retired))
    }

    /// Seal the active segment and start a new one at the current tail.
    ///
    /// The all-in-one path, kept for the hard-limit fallback and for callers
    /// that drive `SegmentSet` directly. It holds the caller's lock across
    /// several fsyncs; the split `roll_plan`/`commit_roll` pair is what the
    /// async log uses to keep that off the append path.
    pub(crate) fn roll(&mut self) -> Result<()> {
        self.roll_for(&[])
    }

    /// [`Self::roll`] to a segment that can hold `records`.
    pub(crate) fn roll_for(&mut self, records: &[AppendRecord]) -> Result<()> {
        self.check_open()?;
        let version = self.active.successor_version(records);
        let descriptor = self.active.seal()?;
        let base_offset = self.active.next_offset();
        let id = self.next_segment_id.fetch_add(1, Ordering::AcqRel);

        let replacement = SegmentWriter::create_at_version(
            &self.dir,
            id,
            base_offset,
            now_micros(),
            self.config.preallocate_bytes(),
            self.config.index_spacing_bytes,
            version,
        )?;
        let retired = std::mem::replace(&mut self.active, replacement);
        self.active_reader = Arc::new(SegmentReader::open(
            self.active.path(),
            self.active.id(),
            base_offset,
        )?);
        self.bump_next_segment_id(id + 1);

        // A sealed segment holding no records would break the `last_offset`
        // invariant `SealedEntry` relies on. Rather than list it, delete it:
        // leaving the file behind would put an empty segment in the middle of
        // the chain, where recovery reads its base offset as a break.
        if retired.record_count() > 0 {
            self.push_sealed(descriptor, &retired)?;
        } else {
            let path = retired.path().to_path_buf();
            let index = self.dir.join(index_file_name(retired.id()));
            drop(retired);
            for path in [path, index] {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(StorageError::Io(err)),
                }
            }
        }

        metrics::counter!(metrics_names::SEGMENT_ROLL_TOTAL).increment(1);
        metrics::gauge!(metrics_names::SEGMENT_COUNT).set((self.sealed.len() + 1) as f64);
        Ok(())
    }

    /// List a retired writer's segment as sealed. Its index is already in
    /// memory and its records are the likeliest to be read next, by a follower
    /// catching up, so the cache starts with it rather than reading it back.
    fn push_sealed(
        &mut self,
        descriptor: crate::log::SegmentDescriptor,
        retired: &SegmentWriter,
    ) -> Result<()> {
        let entry = SealedEntry::new(descriptor, retired.holds_marks());
        self.files.seed(
            entry.key(),
            SealedHandle {
                reader: SegmentReader::open(retired.path(), retired.id(), retired.base_offset())?,
                index: retired.index().clone(),
            },
        );
        self.sealed.push(entry);
        Ok(())
    }

    /// Largest the active segment may grow before an inline roll is forced.
    fn size_ceiling(&self, roll_pending: bool) -> u64 {
        if !roll_pending {
            return self.config.segment_size_bytes;
        }
        let overshoot = 100u64.saturating_add(self.config.max_overshoot_percent as u64);
        self.config
            .segment_size_bytes
            .saturating_mul(overshoot)
            .max(self.config.segment_size_bytes)
            / 100
    }
}

/// Everything needed to build a replacement segment, captured under the lock so
/// that the building itself can happen without one.
#[derive(Debug, Clone)]
pub(crate) struct RollPlan {
    dir: PathBuf,
    id: SegmentId,
    previous_active_id: SegmentId,
    preallocate_bytes: u64,
    index_spacing_bytes: u64,
}

impl RollPlan {
    /// Create the replacement segment. Blocking, and safe to call with no lock.
    ///
    /// The segment is left *headerless*: it claims no base offset yet, because
    /// the correct one is not known until the swap. See [`BlankSegment`].
    pub(crate) fn build(self) -> Result<PreparedSegment> {
        let blank = BlankSegment::create(
            &self.dir,
            self.id,
            self.preallocate_bytes,
            self.index_spacing_bytes,
        )?;
        Ok(PreparedSegment {
            blank,
            previous_active_id: self.previous_active_id,
        })
    }
}

/// A replacement segment built ahead of the swap that installs it.
#[derive(Debug)]
pub(crate) struct PreparedSegment {
    blank: BlankSegment,
    /// The segment this replacement was built to retire.
    ///
    /// The only thing that can invalidate a prepared segment now that it takes
    /// its base offset at the swap: another roll got there first, so the
    /// segment this one was built to retire is no longer the active one.
    /// Installing anyway would abandon that segment's file with no owner.
    previous_active_id: SegmentId,
}

impl PreparedSegment {
    /// Remove the files backing a replacement that was never installed.
    pub(crate) fn discard(self) -> Result<()> {
        self.blank.discard()
    }
}

/// What happened to a prepared segment offered to [`SegmentSet::commit_roll`].
#[derive(Debug)]
pub(crate) enum RollOutcome {
    /// Installed as the active segment. Carries the retired writer to be sealed.
    Installed(SegmentWriter),
    /// Rejected: the tail moved past the offset it was built for. Handed back
    /// so the caller can delete its files — dropping it would leave them on
    /// disk for recovery to clean up instead.
    Stale(PreparedSegment),
}

#[cfg(test)]
mod tests;
