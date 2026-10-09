//! Deciding what recovery will do to a shard directory, without doing it.
//!
//! Every read recovery makes goes through a [`View`] of the directory that
//! carries the writes planned so far: a segment planned for removal is gone
//! from it, one planned for a cut reads as cut, and a planned index rebuild is
//! the index a later look sees. That is what lets the plan follow recovery
//! past its own repairs (an unsealed retired segment is cut, then the chain is
//! checked again) and still reach the verdict startup reaches.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use super::discover_segment_ids;
use crate::disk_log::durable_mark::{self, DurableMark};
use crate::disk_log::offload::Manifest;
use crate::log::{LogConfig, Offset, SegmentDescriptor, SegmentId};
use crate::segment::format::{IndexEntry, SEGMENT_HEADER_LEN};
use crate::segment::scan::scan_segment_upto;
use crate::segment::{
    ScanOutcome, ScanStart, SparseIndex, TailRepair, index_file_name, read_segment_header,
    segment_file_name,
};
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// What recovering one shard directory will do, in the order it does it.
#[derive(Debug)]
pub(crate) struct RecoveryPlan {
    /// Writes to make before the log opens.
    pub(crate) steps: Vec<Step>,
    pub(crate) end: End,
    /// Segments discarded as leftovers of a rollover, for the metric.
    pub(crate) abandoned: usize,
}

/// One write recovery makes.
#[derive(Debug)]
pub(crate) enum Step {
    /// Unlink a segment and its index.
    Remove {
        id: SegmentId,
        why: Removal,
    },
    SyncDir,
    /// Write a sealed segment's index from a full scan of it.
    RebuildIndex {
        id: SegmentId,
        index: SparseIndex,
        why: IndexRebuild,
    },
    /// Cut a retired segment whose seal never finished back to its last
    /// intact record, removing the newer segment first when it does not
    /// start exactly there.
    CutRetired {
        id: SegmentId,
        valid_bytes: u64,
        drop_next: Option<SegmentId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Removal {
    /// Empty, between two segments that meet: a rollover that lost its race.
    LostRollRace,
    /// The newest segment, with no header: a rollover never installed.
    HeaderlessPreparation,
    /// The newest segment, empty, based away from where the log ends.
    EmptyPreparation {
        base_offset: Offset,
        log_tail: Offset,
    },
    /// The only segment, and blank: its creation never finished.
    BlankFirst,
    /// Below a gap the offload manifest covers.
    BelowOffloadedGap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexRebuild {
    /// No index file, or one that is empty or names another base offset.
    Missing,
    /// The index points somewhere that is not the record it claims.
    Mismatch,
    /// `verify_all_on_open` asked for every sealed segment to be walked.
    VerifyAll,
}

/// Where the log stands once the steps are made.
#[derive(Debug)]
pub(crate) enum End {
    /// No segments: the log starts at segment 0, offset 0.
    Fresh,
    Resume(Resume),
    /// Startup refuses: committed data is damaged.
    Refuse(Corruption),
}

#[derive(Debug)]
pub(crate) struct Resume {
    pub(crate) sealed: Vec<PlannedSealed>,
    pub(crate) active_id: SegmentId,
    /// The active segment's full scan. A torn tail in it is cut when the
    /// writer reopens it.
    pub(crate) active: ScanOutcome,
    /// Sealed indexes rebuilt by the pass that succeeded.
    pub(crate) sealed_index_rebuilds: usize,
}

#[derive(Debug)]
pub(crate) struct PlannedSealed {
    pub(crate) descriptor: SegmentDescriptor,
    pub(crate) holds_marks: bool,
}

/// Plan the recovery of the shard in `dir`. Reads only.
pub(crate) fn plan_recovery(
    dir: &Path,
    label: &str,
    config: &LogConfig,
    offloaded: &Manifest,
) -> Result<RecoveryPlan> {
    // A shard that is not here yet is a fresh log; startup creates the
    // directory.
    let mut ids = if dir.exists() {
        discover_segment_ids(dir)?
    } else {
        Vec::new()
    };
    // Read once, before anything is repaired: it describes the files as the
    // last run left them.
    let mut view = View::new(dir, label, config, durable_mark::load(dir));
    let mut abandoned = 0;
    let end = match decide(&mut view, offloaded, &mut ids, &mut abandoned) {
        Ok(end) => end,
        Err(StorageError::Corruption(detail)) => End::Refuse(detail),
        Err(err) => return Err(err),
    };
    Ok(RecoveryPlan {
        steps: view.steps,
        end,
        abandoned,
    })
}

fn decide(
    view: &mut View<'_>,
    offloaded: &Manifest,
    ids: &mut Vec<SegmentId>,
    abandoned: &mut usize,
) -> Result<End> {
    // Rollover prepares the replacement segment ahead of the swap that installs
    // it, so a crash — or a truncation — can leave one behind that was never
    // used. Drop them before recovery proper, or their base offset reads as a
    // break in the offset chain. See `discard_abandoned_preparations`.
    *abandoned += view.discard_blank_interior(ids)?;
    *abandoned += view.discard_abandoned_preparations(ids)?;
    view.discard_blank_first(ids)?;
    let Some((active_id, sealed_ids)) = ids.split_last() else {
        return Ok(End::Fresh);
    };
    match view.recover_existing(offloaded, sealed_ids, *active_id) {
        Err(StorageError::Corruption(detail)) => {
            let Some(retired) = view.unsealed_retired(sealed_ids, *active_id, &detail)? else {
                return Err(StorageError::Corruption(detail));
            };
            if !view.cut_unsealed_retired(&retired, *active_id)? {
                return Err(StorageError::Corruption(detail));
            }
            let ids = view.present(ids);
            let (active_id, sealed_ids) = ids.split_last().expect("the retired segment");
            Ok(End::Resume(
                view.recover_existing(offloaded, sealed_ids, *active_id)?,
            ))
        }
        resumed => Ok(End::Resume(resumed?)),
    }
}

/// The shard directory as it will be once the steps planned so far are made.
pub(crate) struct View<'a> {
    dir: &'a Path,
    label: &'a str,
    config: &'a LogConfig,
    mark: Option<DurableMark>,
    removed: BTreeSet<SegmentId>,
    /// Lengths of segments planned to be cut.
    cut: HashMap<SegmentId, u64>,
    /// Last entry of each index planned to be rewritten.
    indexes: HashMap<SegmentId, Option<IndexEntry>>,
    pub(crate) steps: Vec<Step>,
}

struct OpenedSealed {
    sealed: PlannedSealed,
    /// Not `last_offset + 1`: a retired segment a crash emptied holds no
    /// records, and its successor must then start at its base.
    next_offset: Offset,
    rebuilt_index: bool,
}

struct UnsealedRetired {
    id: SegmentId,
    valid_bytes: u64,
    next_offset: Offset,
}

impl<'a> View<'a> {
    pub(crate) fn new(
        dir: &'a Path,
        label: &'a str,
        config: &'a LogConfig,
        mark: Option<DurableMark>,
    ) -> Self {
        Self {
            dir,
            label,
            config,
            mark,
            removed: BTreeSet::new(),
            cut: HashMap::new(),
            indexes: HashMap::new(),
            steps: Vec::new(),
        }
    }

    fn path(&self, id: SegmentId) -> PathBuf {
        self.dir.join(segment_file_name(id))
    }

    fn len(&self, id: SegmentId) -> Result<u64> {
        let on_disk = std::fs::metadata(self.path(id))?.len();
        Ok(self.cut.get(&id).map_or(on_disk, |cut| (*cut).min(on_disk)))
    }

    fn scan(&self, id: SegmentId, start: ScanStart, repair: TailRepair) -> Result<ScanOutcome> {
        scan_segment_upto(
            &self.path(id),
            self.cut.get(&id).copied(),
            id,
            self.label,
            self.config.index_spacing_bytes,
            start,
            repair,
        )
    }

    /// What a scan of segment `id` may repair: the configured checksum rule,
    /// and anything past where the durable mark says its synced bytes end.
    fn tail_repair(&self, id: SegmentId) -> TailRepair {
        TailRepair {
            checksum_tail: self.config.repair_checksum_tail,
            unsynced_from: DurableMark::unsynced_from(self.mark, id),
        }
    }

    fn index_last(&self, id: SegmentId, base_offset: Offset) -> Option<IndexEntry> {
        match self.indexes.get(&id) {
            Some(planned) => *planned,
            None => SparseIndex::load_last(&self.dir.join(index_file_name(id)), base_offset),
        }
    }

    /// `ids` without the segments planned for removal.
    fn present(&self, ids: &[SegmentId]) -> Vec<SegmentId> {
        ids.iter()
            .copied()
            .filter(|id| !self.removed.contains(id))
            .collect()
    }

    fn remove(&mut self, id: SegmentId, why: Removal) {
        self.removed.insert(id);
        self.steps.push(Step::Remove { id, why });
    }

    /// Remove segments inside the chain that never got as far as a header.
    ///
    /// A background rollover that loses the race to an inline one deletes the
    /// blank segment it built. The blank's directory entry was synced when it
    /// was created, so if the unlink did not reach the disk a power loss brings
    /// it back, empty, between two installed segments.
    ///
    /// Shorter than a header, it cannot hold a record. It is dropped only when
    /// the segments either side of it still meet exactly, so a real segment
    /// that lost its bytes stays the offset gap that `recover_existing`
    /// refuses. A blank run with nothing installed after it is left to
    /// `discard_abandoned_preparations`.
    fn discard_blank_interior(&mut self, ids: &mut Vec<SegmentId>) -> Result<usize> {
        let is_blank =
            |view: &Self, id: SegmentId| -> Result<bool> { Ok(view.len(id)? < SEGMENT_HEADER_LEN) };
        let mut discarded = 0;
        let mut at = 1;
        while at + 1 < ids.len() {
            if !is_blank(self, ids[at])? {
                at += 1;
                continue;
            }
            let mut next = at + 1;
            while next < ids.len() && is_blank(self, ids[next])? {
                next += 1;
            }
            let Some(&following) = ids.get(next) else {
                break;
            };
            let previous = ids[at - 1];
            let previous_end = self
                .scan(previous, ScanStart::Full, self.tail_repair(previous))
                .map(|outcome| outcome.next_offset);
            let following_base = read_segment_header(&self.path(following), following, self.label)
                .map(|header| header.base_offset);
            match (previous_end, following_base) {
                (Ok(end), Ok(base)) if end == base => {}
                // Not provably harmless: leave it for the chain checks to report.
                (
                    Ok(_) | Err(StorageError::Corruption(_)),
                    Ok(_) | Err(StorageError::Corruption(_)),
                ) => {
                    at = next;
                    continue;
                }
                (Err(err), _) | (_, Err(err)) => return Err(err),
            }
            for id in ids.drain(at..next) {
                self.remove(id, Removal::LostRollRace);
                discarded += 1;
            }
        }
        if discarded > 0 {
            self.steps.push(Step::SyncDir);
        }
        Ok(discarded)
    }

    /// Remove trailing segments that a rollover created but never installed.
    ///
    /// A background rollover builds its replacement segment — file, header,
    /// directory entry, all fsynced — before taking the lock that swaps it in,
    /// so that the flushes never land on an append. The consequence is a window
    /// in which a replacement exists on disk but is not yet part of the log,
    /// and two things can end that window without installing it: a crash, or a
    /// truncation that rewinds the tail past the offset the replacement was
    /// built for.
    ///
    /// Such a segment is recognisable without any durable roll-intent record,
    /// because it is self-describing: it is the newest segment, it holds **zero
    /// records**, and its base offset is not where the log actually ends. A
    /// segment that holds no records has nothing to lose, so deleting it cannot
    /// discard an acknowledged write — which is what makes this rule safe to
    /// apply blindly.
    ///
    /// Both directions occur and both are handled:
    ///
    /// * base offset *below* the tail — appends kept landing in the old segment
    ///   after the replacement was built, which is the normal design of the
    ///   background roll;
    /// * base offset *above* the tail — a truncation rewound the log underneath
    ///   a replacement that had already been built.
    ///
    /// An empty newest segment whose base offset *does* match the tail is the
    /// ordinary state right after a successful roll, and is kept.
    fn discard_abandoned_preparations(&mut self, ids: &mut Vec<SegmentId>) -> Result<usize> {
        let mut discarded = 0;
        // More than one can accumulate: each crash during a roll can leave its own.
        while ids.len() > 1 {
            let id = *ids.last().expect("non-empty");

            // A file too short to hold a header is a rollover that was
            // interrupted between creating the segment and writing its header.
            // It has no base offset to compare and, having no header, cannot
            // hold a record either.
            let why = if header_never_written(&self.path(id))? {
                Removal::HeaderlessPreparation
            } else {
                let outcome = self.scan(id, ScanStart::Full, self.tail_repair(id))?;
                if outcome.record_count > 0 {
                    break;
                }

                // Empty. Compare its base against where the previous segment ends.
                let previous = *ids.get(ids.len() - 2).expect("len > 1");
                let previous_end = self
                    .scan(previous, ScanStart::Full, self.tail_repair(previous))?
                    .next_offset;
                if outcome.header.base_offset == previous_end {
                    break;
                }
                Removal::EmptyPreparation {
                    base_offset: outcome.header.base_offset,
                    log_tail: previous_end,
                }
            };
            self.remove(id, why);
            ids.pop();
            discarded += 1;
        }
        if discarded > 0 {
            self.steps.push(Step::SyncDir);
        }
        Ok(discarded)
    }

    /// Remove a log's first segment when it is the only one and is blank.
    ///
    /// Its creation failed, on a full disk say, or a crash came before the
    /// header. It never held a record, so the log starts again as if new. Only
    /// segment 0 qualifies: any later one follows records that were somewhere.
    ///
    /// Blank means no longer than a header, all zeros, and no durable mark
    /// past the header. Creation syncs the header before anything can append
    /// and preallocation leaves the size alone, so a file longer than a header
    /// had its header synced: zeros there are rot, even when the bytes after
    /// it are zeros too, and stay fatal.
    pub(crate) fn discard_blank_first(&mut self, ids: &mut Vec<SegmentId>) -> Result<()> {
        if ids.as_slice() != [0]
            || DurableMark::synced_through(self.mark, 0) > SEGMENT_HEADER_LEN
            || self.len(0)? > SEGMENT_HEADER_LEN
            || !header_never_written(&self.path(0))?
        {
            return Ok(());
        }
        self.remove(0, Removal::BlankFirst);
        self.steps.push(Step::SyncDir);
        ids.clear();
        Ok(())
    }

    /// The segment a background rollover retired but never finished sealing,
    /// when `detail` is damage a crash can leave there.
    ///
    /// The rollover installs the new active segment first and flushes the
    /// retired one afterwards. The two files reach the device independently, so
    /// a power loss in between can leave the retired segment short -- torn or
    /// zero-filled at the end, or cut cleanly back to its last sync -- with the
    /// newer segment after it intact. Every flush syncs the retired segment
    /// before the active one, so no record past the loss, in either segment,
    /// was ever reported durable. Two shapes qualify:
    ///
    /// * a torn tail in the retired segment (`scan_segment` with repair off);
    /// * an intact retired segment that ends before the active segment begins,
    ///   which is the same loss landing on a record boundary.
    ///
    /// Anything else is still corruption.
    fn unsealed_retired(
        &self,
        sealed_ids: &[SegmentId],
        active_id: SegmentId,
        detail: &Corruption,
    ) -> Result<Option<UnsealedRetired>> {
        let Some(&id) = sealed_ids.last() else {
            return Ok(None);
        };
        let cut_short = match detail.kind {
            CorruptionKind::OffsetOutOfOrder { expected, found } => {
                detail.site.segment == Some(active_id) && found > expected
            }
            _ => false,
        };
        if detail.site.segment != Some(id) && !cut_short {
            return Ok(None);
        }
        match self.scan(
            id,
            ScanStart::Full,
            TailRepair {
                checksum_tail: false,
                unsynced_from: DurableMark::unsynced_from(self.mark, id),
            },
        ) {
            Ok(outcome) if outcome.torn_tail.is_some() || cut_short => Ok(Some(UnsealedRetired {
                id,
                valid_bytes: outcome.valid_bytes,
                next_offset: outcome.next_offset,
            })),
            Ok(_) | Err(StorageError::Corruption(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Cut an unsealed retired segment back to its last intact record. The
    /// newer segment is kept only if it starts exactly there; otherwise records
    /// were lost in between and nothing after the tear can be kept in order.
    ///
    /// Returns `false`, planning nothing, when the mark says the tear is in
    /// bytes that were synced: those records may have been acknowledged, so the
    /// damage is corruption, not an unfinished seal. That also protects the
    /// active segment, because a mark can only reach it once the retired
    /// segment was synced whole.
    fn cut_unsealed_retired(
        &mut self,
        retired: &UnsealedRetired,
        active_id: SegmentId,
    ) -> Result<bool> {
        if retired.valid_bytes < DurableMark::synced_through(self.mark, retired.id) {
            return Ok(false);
        }
        let continues = read_segment_header(&self.path(active_id), active_id, self.label)
            .is_ok_and(|header| header.base_offset == retired.next_offset);
        if !continues {
            self.removed.insert(active_id);
        }
        self.cut.insert(retired.id, retired.valid_bytes);
        self.steps.push(Step::CutRetired {
            id: retired.id,
            valid_bytes: retired.valid_bytes,
            drop_next: (!continues).then_some(active_id),
        });
        Ok(true)
    }

    fn recover_existing(
        &mut self,
        offloaded: &Manifest,
        sealed_ids: &[SegmentId],
        active_id: SegmentId,
    ) -> Result<Resume> {
        let mut sealed: Vec<PlannedSealed> = Vec::with_capacity(sealed_ids.len());
        let mut sealed_index_rebuilds = 0usize;
        let mut expected_base: Option<Offset> = None;

        for id in sealed_ids {
            let opened = self.open_sealed(*id)?;
            if opened.rebuilt_index {
                sealed_index_rebuilds += 1;
            }
            let base_offset = opened.sealed.descriptor.base_offset;
            // Offsets must be contiguous across segment boundaries. A gap means
            // a segment file was deleted or replaced out from under us.
            if let Some(expected) = expected_base
                && expected != base_offset
                && !self.drop_offloaded_below(offloaded, &mut sealed, expected, base_offset)
            {
                return Err(gap_error(self.label, *id, expected, base_offset));
            }
            expected_base = Some(opened.next_offset);
            sealed.push(opened.sealed);
        }

        // The newest segment is the only one that can have been mid-write when
        // the process died, so it always gets a full scan.
        let active = self.scan(active_id, ScanStart::Full, self.tail_repair(active_id))?;
        if let Some(expected) = expected_base
            && expected != active.header.base_offset
            && !self.drop_offloaded_below(
                offloaded,
                &mut sealed,
                expected,
                active.header.base_offset,
            )
        {
            return Err(gap_error(
                self.label,
                active_id,
                expected,
                active.header.base_offset,
            ));
        }
        Ok(Resume {
            sealed,
            active_id,
            active,
            sealed_index_rebuilds,
        })
    }

    /// Accept a gap at `[expected, found)` that offload explains, by deleting
    /// the segments below it.
    ///
    /// Offload copies oldest first and records each copy before its local file
    /// goes, so a gap the manifest covers, above segments it also records, is
    /// an unlink that landed out of order. Every record below `found` has a
    /// verified copy, and the log resumes at `found`. Anything short of that is
    /// still the damage `gap_error` reports.
    fn drop_offloaded_below(
        &mut self,
        offloaded: &Manifest,
        sealed: &mut Vec<PlannedSealed>,
        expected: Offset,
        found: Offset,
    ) -> bool {
        if found < expected
            || !offloaded.covers(expected, found)
            || !sealed
                .iter()
                .all(|entry| offloaded.records(&entry.descriptor))
        {
            return false;
        }
        for entry in sealed.drain(..) {
            self.remove(entry.descriptor.id, Removal::BelowOffloadedGap);
            self.steps.push(Step::SyncDir);
        }
        true
    }

    /// Validate one sealed segment and prepare it for reads.
    fn open_sealed(&mut self, id: SegmentId) -> Result<OpenedSealed> {
        let file_len = self.len(id)?;

        // The header alone establishes the base offset every other check is
        // relative to, and proves the file is ours before anything else is
        // trusted.
        let header = read_segment_header(&self.path(id), id, self.label)?;
        let base_offset = header.base_offset;

        let last = self.index_last(id, base_offset);
        // A sealed segment was synced and trimmed when it was sealed, so
        // nothing in it can be an unfinished write.
        let sealed_repair = TailRepair::default();

        // `Err` says why the index cannot be used.
        let resumed = match (last, self.config.verify_all_on_open) {
            (Some(last), false) => {
                // Resume from the last index entry: only the records it does
                // not cover need checking, which is bounded by one index
                // interval.
                let resumed = self.scan(
                    id,
                    ScanStart::Resume {
                        position: last.position,
                        next_offset: last.offset,
                    },
                    sealed_repair,
                );
                match resumed {
                    Ok(outcome)
                        if outcome.torn_tail.is_none() && outcome.valid_bytes == file_len =>
                    {
                        Ok(outcome)
                    }
                    // The index sent the scan somewhere that is not a record
                    // boundary, or not the one it claims. That may be the index
                    // alone, so the full scan below decides: it fails only if
                    // the segment itself is damaged.
                    Ok(_) | Err(StorageError::Corruption(_)) => Err(IndexRebuild::Mismatch),
                    Err(err) => return Err(err),
                }
            }
            (Some(_), true) => Err(IndexRebuild::VerifyAll),
            (None, _) => Err(IndexRebuild::Missing),
        };
        let rebuilt_index = resumed.is_err();
        let outcome = match resumed {
            Ok(resumed) => resumed,
            Err(why) => {
                // Walk the whole segment and rebuild its index from it.
                let outcome = self.scan(id, ScanStart::Full, sealed_repair)?;
                self.indexes
                    .insert(id, outcome.index.entries().last().copied());
                self.steps.push(Step::RebuildIndex {
                    id,
                    index: outcome.index.clone(),
                    why,
                });
                outcome
            }
        };

        // A sealed segment was synced and trimmed when it was sealed, so damage
        // at its tail is not a torn write — it is data loss in committed bytes.
        if let Some(tail) = outcome.torn_tail {
            return Err(StorageError::Corruption(
                Corruption::new(tail.cause)
                    .in_segment(self.label, id)
                    .at_position(tail.position),
            ));
        }
        if outcome.valid_bytes != file_len {
            return Err(StorageError::Corruption(
                Corruption::new(CorruptionKind::Truncated {
                    needed: file_len,
                    available: outcome.valid_bytes,
                })
                .in_segment(self.label, id)
                .at_position(outcome.valid_bytes),
            ));
        }

        Ok(OpenedSealed {
            sealed: PlannedSealed {
                descriptor: SegmentDescriptor {
                    id,
                    base_offset,
                    last_offset: outcome.next_offset.saturating_sub(1).max(base_offset),
                    size_bytes: file_len,
                },
                holds_marks: header.holds_marks(),
            },
            next_offset: outcome.next_offset,
            rebuilt_index,
        })
    }
}

/// A segment whose header never reached the disk: shorter than a header, or
/// a header of zeros. A background rollover writes the header without a flush
/// of its own, and every flush that would cover it syncs the retired segment
/// first, so nothing in such a segment was ever reported durable.
fn header_never_written(path: &Path) -> Result<bool> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() < SEGMENT_HEADER_LEN {
        return Ok(true);
    }
    let mut header = [0u8; SEGMENT_HEADER_LEN as usize];
    let read = crate::io::read_at(&file, &mut header, 0)?;
    Ok(read == header.len() && header.iter().all(|byte| *byte == 0))
}

fn gap_error(label: &str, id: SegmentId, expected: Offset, found: Offset) -> StorageError {
    StorageError::Corruption(
        Corruption::new(CorruptionKind::OffsetOutOfOrder { expected, found })
            .in_segment(label, id)
            .at_position(0),
    )
}

#[cfg(test)]
mod tests;
