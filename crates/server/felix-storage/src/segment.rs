//! Durable segment files: the byte format, the sparse index beside each
//! segment, and the writer and reader that own a single segment.
//!
//! Nothing in here knows about shards, retention, or rollover policy — a
//! `SegmentWriter` writes one file until someone tells it to stop. Composing
//! segments into a log is `crate::disk_log`'s job.

pub mod format;

pub(crate) mod index;
pub(crate) mod reader;
pub(crate) mod scan;
pub(crate) mod writer;

mod cursor;
#[cfg(test)]
mod test_support;

pub use format::{
    DecodedRecord, FORMAT_VERSION, IndexEntry, IndexHeader, MAX_PAYLOAD_BYTES,
    OLDEST_READABLE_VERSION, PRODUCER_TAG_LEN, RECORD_HEADER_LEN, RecordHeader, SEGMENT_HEADER_LEN,
    SegmentHeader, record_len,
};
pub use index::{IndexWriter, SparseIndex};
pub use reader::{ReadBudget, SegmentReader};
pub use scan::{
    ScanOutcome, ScanStart, TailRepair, TornTail, read_segment_header, scan_segment,
    scan_segment_with,
};
pub use writer::{ResumeState, SegmentWriter};

use crate::log::SegmentId;

/// File name of the data file for `id`, e.g. `00000000000000000007.log`.
///
/// Zero padding keeps lexicographic and numeric order identical, which makes a
/// directory listing readable — but recovery still parses the number and sorts
/// on it rather than trusting the listing.
pub fn segment_file_name(id: SegmentId) -> String {
    format!("{id:020}.log")
}

/// File name of the sparse index that accompanies segment `id`.
pub fn index_file_name(id: SegmentId) -> String {
    format!("{id:020}.index")
}

/// Parse a segment id back out of a data file name, or `None` if the name is not
/// one of ours.
pub fn parse_segment_file_name(name: &str) -> Option<SegmentId> {
    name.strip_suffix(".log")?.parse().ok()
}

#[cfg(test)]
mod tests;
