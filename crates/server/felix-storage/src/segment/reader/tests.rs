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

/// A v7 segment: offsets 0 and 1 plain, 2 a trim with floor 1, 3 plain.
fn write_with_trim(path: &std::path::Path, version: u16) -> crate::segment::ScanOutcome {
    use crate::log::RecordMark;
    use crate::segment::format::{SegmentHeader, encode_record};
    let mut bytes = SegmentHeader::at_version(0, 1, version).encode().to_vec();
    encode_record(&mut bytes, 0, 100, b"a", None, &RecordMark::None);
    encode_record(&mut bytes, 1, 101, b"b", None, &RecordMark::None);
    encode_record(
        &mut bytes,
        2,
        102,
        b"headers",
        None,
        &RecordMark::Trim { floor: 1 },
    );
    encode_record(&mut bytes, 3, 103, b"d", None, &RecordMark::None);
    std::fs::write(path, &bytes).expect("write");
    // The scan would refuse a trim in an older segment, so take the
    // outcome from a v7 copy: the reader is what is under test.
    let checked = path.with_extension("v7");
    let mut v7 = bytes.clone();
    v7[..SEGMENT_HEADER_LEN as usize].copy_from_slice(&SegmentHeader::at_version(0, 1, 7).encode());
    std::fs::write(&checked, &v7).expect("write");
    scan_segment(&checked, 0, "t/ns/s/0", 4096, ScanStart::Full, false).expect("scan")
}

fn read(
    path: &std::path::Path,
    outcome: &crate::segment::ScanOutcome,
    start: u64,
    max_records: usize,
    trims: TrimRecords,
) -> crate::Result<Vec<crate::log::LogRecord>> {
    let reader = SegmentReader::open(path, 0, 0).expect("open");
    let mut out = Vec::new();
    reader.read_from(
        &outcome.index,
        start,
        outcome.valid_bytes,
        &mut ReadBudget::new(usize::MAX, max_records).with_trims(trims),
        "t/ns/s/0",
        &mut out,
    )?;
    Ok(out)
}

/// A normal read leaves the trim out, and a read starting at it, or one
/// with room for a single record, gets the next real record instead of
/// nothing. The internal read returns it with its floor and headers.
#[test]
fn a_trim_is_skipped_by_default_and_returned_on_request() {
    use crate::log::RecordMark;
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let outcome = write_with_trim(&path, 7);

    let offsets = |records: Vec<crate::log::LogRecord>| {
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>()
    };
    let skipped = read(&path, &outcome, 0, usize::MAX, TrimRecords::Skip).expect("read");
    assert_eq!(offsets(skipped), vec![0, 1, 3]);
    let from_trim = read(&path, &outcome, 2, 1, TrimRecords::Skip).expect("read");
    assert_eq!(offsets(from_trim), vec![3]);

    let all = read(&path, &outcome, 2, usize::MAX, TrimRecords::Include).expect("read");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].offset, 2);
    assert_eq!(all[0].mark, RecordMark::Trim { floor: 1 });
    assert_eq!(all[0].payload, Bytes::from_static(b"headers"));
    assert_eq!(all[1].offset, 3);
}

/// Recovery scans only the active segment, so the reader itself refuses a
/// trim found in a sealed segment older than v7, whether it would return
/// the record or skip it.
#[test]
fn a_trim_in_an_older_segment_is_refused_by_the_reader() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let outcome = write_with_trim(&path, 6);
    for trims in [TrimRecords::Skip, TrimRecords::Include] {
        let err = read(&path, &outcome, 0, usize::MAX, trims).expect_err("v6");
        let crate::StorageError::Corruption(corruption) = err else {
            panic!("expected corruption, got {err:?}");
        };
        assert!(matches!(
            corruption.kind,
            crate::CorruptionKind::RecordFlags { .. }
        ));
    }
    // Records before the trim still read.
    assert_eq!(
        read(&path, &outcome, 0, 2, TrimRecords::Skip)
            .expect("read")
            .len(),
        2
    );
}
