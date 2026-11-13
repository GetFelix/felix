//! Which indexes a Raft log directory holds, read without opening the log.
//!
//! Opening a `DiskLog` can repair a torn tail, lower a commit offset and
//! write its marks, so a directory kept as evidence is read here instead:
//! nothing is created, truncated or synced.

use std::io;
use std::path::Path;

use super::purged;
use crate::disk_log::durable_mark;
use crate::log::Offset;
use crate::segment::parse_segment_file_name;
use crate::segment::scan::{ScanStart, TailRepair, scan_segment_with};

/// The records a directory holds, whether or not Raft still reads them as
/// entries: a purge drops whole segments, so the segment that holds the purged
/// id can keep records below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Held {
    /// Where the lowest segment that holds a record starts, which is the
    /// lowest record the directory physically has. None when no segment holds
    /// a record.
    pub(crate) lowest: Option<Offset>,
    /// The last record it holds, or the purged id if that is higher. None for
    /// a directory with neither.
    pub(crate) last: Option<Offset>,
}

/// What the directory `dir` holds, read off its segments and its purged id.
/// A segment is scanned up to the durable mark as an open would, so an
/// unsynced tail reads as the torn tail it is. Damage elsewhere, or a purged
/// id that does not decode, is an error. Nothing is written.
pub(crate) fn read(dir: &Path, label: &str, index_spacing_bytes: u64) -> io::Result<Held> {
    let purged = purged::load(dir)?;
    let mark = durable_mark::load(dir);
    let mut segments = Vec::new();
    for entry in crate::fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(id) = entry.file_name().to_str().and_then(parse_segment_file_name) {
            segments.push((id, entry.path()));
        }
    }
    segments.sort();
    let scan = |id: u64, path: &Path| {
        scan_segment_with(
            path,
            id,
            label,
            index_spacing_bytes,
            ScanStart::Full,
            TailRepair {
                checksum_tail: false,
                unsynced_from: durable_mark::DurableMark::unsynced_from(mark, id),
            },
        )
        .map_err(io::Error::other)
    };
    let mut lowest = None;
    for (id, path) in &segments {
        let scanned = scan(*id, path)?;
        if scanned.last_offset().is_some() {
            lowest = Some(scanned.header.base_offset);
            break;
        }
    }
    let mut last = None;
    for (id, path) in segments.iter().rev() {
        if let Some(offset) = scan(*id, path)?.last_offset() {
            last = Some(offset);
            break;
        }
    }
    let purged = purged.map(|purged| purged.index);
    Ok(Held {
        lowest,
        last: last.max(purged),
    })
}
