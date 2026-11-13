//! Which indexes a Raft log directory holds, read without opening the log.
//!
//! Opening a `DiskLog` can repair a torn tail, lower a commit offset and
//! write its marks, so a directory kept as evidence is read here instead:
//! nothing is created, truncated or synced.

use std::io;
use std::path::Path;

use super::purged;
use crate::DiskLog;
use crate::log::Offset;
use crate::segment::parse_segment_file_name;
use crate::segment::scan::{ScanStart, TailRepair, read_segment_header, scan_segment_with};

/// The entries a directory holds, as a Raft log reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Held {
    /// The index of the first entry the log holds or will hold: one past the
    /// purged id, or where the oldest segment starts.
    pub(crate) first: Offset,
    /// The last entry it holds, or the purged id when it holds none above.
    /// None for a directory with neither.
    pub(crate) last: Option<Offset>,
    /// Whether it holds any entry, as opposed to only a purged id.
    pub(crate) entries: bool,
}

/// The first index an open log holds or will hold.
pub(crate) fn first_index(log: &DiskLog) -> io::Result<Offset> {
    Ok(match purged::load(log.dir())? {
        Some(purged) => log.base_offset().max(purged.index + 1),
        None => log.base_offset(),
    })
}

/// What the directory `dir` holds, read off its segments and its purged id.
/// Damage inside a segment, or a purged id that does not decode, is an error.
pub(crate) fn read(dir: &Path, label: &str, index_spacing_bytes: u64) -> io::Result<Held> {
    let purged = purged::load(dir)?;
    let mut segments = Vec::new();
    for entry in crate::fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(id) = entry.file_name().to_str().and_then(parse_segment_file_name) {
            segments.push((id, entry.path()));
        }
    }
    segments.sort();
    let base = match segments.first() {
        Some((id, path)) => {
            read_segment_header(path, *id, label)
                .map_err(io::Error::other)?
                .base_offset
        }
        None => 0,
    };
    let first = purged.map_or(base, |purged| base.max(purged.index + 1));
    let mut last = None;
    for (id, path) in segments.iter().rev() {
        let scanned = scan_segment_with(
            path,
            *id,
            label,
            index_spacing_bytes,
            ScanStart::Full,
            TailRepair::default(),
        )
        .map_err(io::Error::other)?;
        if let Some(offset) = scanned.last_offset() {
            last = Some(offset);
            break;
        }
    }
    Ok(match last.filter(|last| *last >= first) {
        Some(last) => Held {
            first,
            last: Some(last),
            entries: true,
        },
        None => Held {
            first,
            last: purged.map(|purged| purged.index),
            entries: false,
        },
    })
}
