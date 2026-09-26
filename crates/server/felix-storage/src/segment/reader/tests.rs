use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::segment::format::SEGMENT_HEADER_LEN;
use crate::segment::test_support::{read_all, write_segment};
use crate::segment::{ScanStart, scan_segment};

#[test]
fn reader_returns_records_from_the_requested_offset() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 10);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");

    let out = read_all(&path, &outcome, 4);
    let offsets: Vec<Offset> = out.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, (4..10).collect::<Vec<_>>());
    assert_eq!(out[0].payload, Bytes::from_static(b"payload-4"));
}

/// Every record's position agrees with a full scan, and an offset past the
/// last record lands on the end of the valid bytes.
#[test]
fn position_of_finds_each_record_boundary() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let bytes = write_segment(&path, 5, 10);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    let reader = SegmentReader::open(&path, 0, 5).expect("open");

    let mut expected = SEGMENT_HEADER_LEN;
    for offset in 5..15 {
        let found = reader
            .position_of(&outcome.index, offset, outcome.valid_bytes, "t/ns/s/0")
            .expect("position");
        assert_eq!(found, expected, "offset {offset}");
        let header = RecordHeader::decode(&bytes[expected as usize..]).expect("header");
        expected += header.encoded_len();
    }
    let past = reader
        .position_of(&outcome.index, 99, outcome.valid_bytes, "t/ns/s/0")
        .expect("position");
    assert_eq!(past, outcome.valid_bytes);
}

#[test]
fn reader_respects_the_byte_budget_but_always_makes_progress() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 10);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    let reader = SegmentReader::open(&path, 0, 0).expect("open");

    let mut budget = ReadBudget::new(1, usize::MAX);
    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            0,
            outcome.valid_bytes,
            &mut budget,
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    assert_eq!(out.len(), 1);

    let mut budget = ReadBudget::new(b"payload-0".len() * 2, usize::MAX);
    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            0,
            outcome.valid_bytes,
            &mut budget,
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    assert_eq!(out.len(), 2);
}

#[test]
fn reader_respects_the_record_budget_and_reports_what_it_spent() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 10);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    let reader = SegmentReader::open(&path, 0, 0).expect("open");

    let mut budget = ReadBudget::new(usize::MAX, 3);
    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            0,
            outcome.valid_bytes,
            &mut budget,
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    assert_eq!(out.len(), 3);
    assert_eq!(budget.max_records, 0);

    // An exhausted budget produces nothing rather than looping.
    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            0,
            outcome.valid_bytes,
            &mut budget,
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    assert!(out.is_empty());
}

#[test]
fn reader_stops_at_the_valid_boundary() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 10);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    let reader = SegmentReader::open(&path, 0, 0).expect("open");

    // Pretend only the first three records were committed.
    let committed_bytes = SEGMENT_HEADER_LEN
        + read_all(&path, &outcome, 0)
            .iter()
            .take(3)
            .map(|r| RECORD_HEADER_LEN + r.payload.len() as u64)
            .sum::<u64>();

    let mut out = Vec::new();
    reader
        .read_from(
            &outcome.index,
            0,
            committed_bytes,
            &mut ReadBudget::unbounded(),
            "t/ns/s/0",
            &mut out,
        )
        .expect("read");
    assert_eq!(out.len(), 3);
}

#[test]
fn reader_past_the_tail_returns_nothing() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 4);
    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    assert!(read_all(&path, &outcome, 99).is_empty());
}
