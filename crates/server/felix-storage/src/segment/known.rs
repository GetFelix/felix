//! Where a sealed v9 segment's first and last known generations sit, kept
//! beside it so an open need not walk the whole segment to find them.
//!
//! Derived, never trusted: an open checks the file against the segment's
//! length and reads each record it points at, and walks the segment (and
//! rewrites this file) when anything disagrees. See `docs/storage-format.md`.

use std::path::Path;

use crate::log::{Offset, SegmentId};
use crate::segment::format::crc32;
use crate::segment::scan::record_generation_at;

const MAGIC: [u8; 4] = *b"FLKG";
const VERSION: u16 = 1;
const LEN: usize = 4 + 2 + 2 + 8 * 6 + 4;

/// File name of the known-generations file beside segment `id`.
pub(crate) fn known_file_name(id: SegmentId) -> String {
    format!("{id:020}.gen")
}

/// The first and last records of a segment whose generation is known (not
/// [`crate::log::UNKNOWN_WRITER`]), as `(position, generation)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct KnownGenerations {
    pub(crate) first: Option<(u64, u64)>,
    pub(crate) last: Option<(u64, u64)>,
}

impl KnownGenerations {
    /// Take in a record at `position` stamped `generation`.
    pub(crate) fn observe(&mut self, position: u64, generation: Option<u64>) {
        if let Some(generation) =
            generation.filter(|generation| *generation != crate::log::UNKNOWN_WRITER)
        {
            self.first.get_or_insert((position, generation));
            self.last = Some((position, generation));
        }
    }

    pub(crate) fn first_generation(&self) -> Option<u64> {
        self.first.map(|(_, generation)| generation)
    }

    pub(crate) fn last_generation(&self) -> Option<u64> {
        self.last.map(|(_, generation)| generation)
    }

    /// Write the file for a segment at `base_offset` that is `segment_len`
    /// bytes long. Not synced: a torn or lost copy only costs a walk.
    pub(crate) fn persist(
        &self,
        path: &Path,
        base_offset: Offset,
        segment_len: u64,
    ) -> crate::Result<()> {
        let mut bytes = Vec::with_capacity(LEN);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        for value in [
            base_offset,
            segment_len,
            self.first.map_or(0, |(position, _)| position),
            self.first.map_or(0, |(_, generation)| generation),
            self.last.map_or(0, |(position, _)| position),
            self.last.map_or(0, |(_, generation)| generation),
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let crc = crc32(&[&bytes]);
        bytes.extend_from_slice(&crc.to_le_bytes());
        crate::fs::write(path, &bytes)?;
        Ok(())
    }

    /// Load the file for segment `id` and check it against the segment, or
    /// `None` when it is missing, damaged, stale, or points at a record that
    /// does not carry the generation it names.
    pub(crate) fn load_verified(
        dir: &Path,
        id: SegmentId,
        label: &str,
        version: u16,
        base_offset: Offset,
        segment_len: u64,
    ) -> Option<Self> {
        let bytes = crate::fs::read(dir.join(known_file_name(id))).ok()?;
        if bytes.len() != LEN
            || bytes[..4] != MAGIC
            || u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION
            || crc32(&[&bytes[..LEN - 4]]).to_le_bytes() != bytes[LEN - 4..]
        {
            return None;
        }
        let field = |at: usize| {
            let start = 8 + at * 8;
            u64::from_le_bytes(bytes[start..start + 8].try_into().expect("8 bytes"))
        };
        if field(0) != base_offset || field(1) != segment_len {
            return None;
        }
        // Position 0 is inside the segment header, so it means "none".
        let point =
            |position: u64, generation: u64| (position != 0).then_some((position, generation));
        let known = Self {
            first: point(field(2), field(3)),
            last: point(field(4), field(5)),
        };
        let consistent = match (known.first, known.last) {
            (None, None) => true,
            (Some((first_at, first)), Some((last_at, last))) => {
                first_at <= last_at && first <= last
            }
            _ => false,
        };
        if !consistent {
            return None;
        }
        let path = dir.join(crate::segment::segment_file_name(id));
        for (position, generation) in known.first.into_iter().chain(known.last) {
            let holds = position < segment_len
                && generation != crate::log::UNKNOWN_WRITER
                && record_generation_at(&path, id, label, version, position)
                    .is_ok_and(|found| found == Some(generation));
            if !holds {
                return None;
            }
        }
        Some(known)
    }
}

#[cfg(test)]
mod tests;
