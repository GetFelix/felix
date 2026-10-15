//! Segment files written by hand, for the scan and reader tests.

use std::path::Path;

use crate::Result;
use crate::log::{LogRecord, Offset};
use crate::segment::format::{SegmentHeader, encode_record};
use crate::segment::{ReadBudget, ScanOutcome, ScanStart, SegmentReader, scan_segment};

/// Build a segment file containing `count` records starting at `base`.
pub(super) fn write_segment(path: &Path, base: Offset, count: u64) -> Vec<u8> {
    let mut bytes = SegmentHeader::new(base, 1).encode().to_vec();
    for i in 0..count {
        encode_record(
            &mut bytes,
            base + i,
            100 + i,
            format!("payload-{i}").as_bytes(),
            None,
            &Default::default(),
        );
    }
    std::fs::write(path, &bytes).expect("write");
    bytes
}

pub(super) fn scan(path: &Path) -> Result<ScanOutcome> {
    scan_segment(path, 0, "t/ns/s/0", 4096, ScanStart::Full, true)
}

pub(super) fn read_all(path: &Path, outcome: &ScanOutcome, start: Offset) -> Vec<LogRecord> {
    let reader = SegmentReader::open(path, 0, outcome.header.base_offset).expect("open");
    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            start,
            outcome.valid_bytes,
            &mut ReadBudget::unbounded(),
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    out
}
