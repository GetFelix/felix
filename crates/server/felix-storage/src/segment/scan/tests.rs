use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::segment::cursor::READ_CHUNK_BYTES;
use crate::segment::format::encode_record;
use crate::segment::test_support::{read_all, scan, write_segment};

#[test]
fn scan_of_a_healthy_segment_reports_every_record() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 10, 5);

    let outcome = scan(&path).expect("scan");
    assert_eq!(outcome.record_count, 5);
    assert_eq!(outcome.next_offset, 15);
    assert_eq!(outcome.last_offset(), Some(14));
    assert!(outcome.torn_tail.is_none());
    assert_eq!(
        outcome.valid_bytes,
        std::fs::metadata(&path).expect("meta").len()
    );
}

#[test]
fn scan_of_an_empty_segment_is_valid() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 3, 0);

    let outcome = scan(&path).expect("scan");
    assert_eq!(outcome.record_count, 0);
    assert_eq!(outcome.next_offset, 3);
    assert_eq!(outcome.last_offset(), None);
    assert_eq!(outcome.valid_bytes, SEGMENT_HEADER_LEN);
}

#[test]
fn truncation_at_every_byte_of_the_tail_record_is_repairable() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let full = write_segment(&path, 0, 3);

    let mut prefix = SegmentHeader::new(0, 1).encode().to_vec();
    encode_record(&mut prefix, 0, 100, b"payload-0", &Default::default());
    encode_record(&mut prefix, 1, 101, b"payload-1", &Default::default());
    let third_start = prefix.len() as u64;

    // Start one byte in: cutting exactly at the boundary leaves a healthy
    // two-record segment, not a torn one.
    for cut in (third_start as usize + 1)..full.len() {
        std::fs::write(&path, &full[..cut]).expect("write");
        let outcome = scan(&path).expect("scan should repair");
        assert_eq!(outcome.record_count, 2, "cut at {cut}");
        assert_eq!(outcome.next_offset, 2);
        assert_eq!(outcome.valid_bytes, third_start);
        let tail = outcome.torn_tail.expect("torn tail");
        assert_eq!(tail.position, third_start);
        assert_eq!(tail.discarded_bytes, cut as u64 - third_start);
    }
}

#[test]
fn a_corrupt_final_record_is_treated_as_a_torn_tail() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 3);
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let outcome = scan(&path).expect("scan should repair");
    assert_eq!(outcome.record_count, 2);
    let tail = outcome.torn_tail.expect("torn tail");
    assert!(matches!(tail.cause, CorruptionKind::RecordChecksum { .. }));
}

#[test]
fn interior_payload_corruption_is_a_hard_error() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 4);
    let inside = SEGMENT_HEADER_LEN as usize + RECORD_HEADER_LEN as usize + 1;
    bytes[inside] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let err = scan(&path).expect_err("interior corruption");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert!(matches!(detail.kind, CorruptionKind::RecordChecksum { .. }));
    assert_eq!(detail.site.shard.as_deref(), Some("t/ns/s/0"));
    assert_eq!(detail.site.segment, Some(0));
    assert_eq!(detail.site.position, Some(SEGMENT_HEADER_LEN));
}

#[test]
fn an_offset_gap_in_committed_data_is_a_hard_error() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = SegmentHeader::new(0, 1).encode().to_vec();
    encode_record(&mut bytes, 0, 1, b"a", &Default::default());
    encode_record(&mut bytes, 5, 2, b"b", &Default::default());
    encode_record(&mut bytes, 6, 3, b"c", &Default::default());
    std::fs::write(&path, &bytes).expect("write");

    let err = scan(&path).expect_err("offset gap");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert!(matches!(
        detail.kind,
        CorruptionKind::OffsetOutOfOrder {
            expected: 1,
            found: 5
        }
    ));
}

#[test]
fn a_corrupt_segment_header_is_never_repaired() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 2);
    bytes[8] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    assert!(matches!(
        scan(&path).expect_err("bad header"),
        StorageError::Corruption(_)
    ));
}

#[test]
fn a_truncated_segment_header_is_reported_with_context() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    std::fs::write(&path, b"FLS").expect("write");

    let err = scan(&path).expect_err("short header");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert!(detail.is_truncation());
    assert_eq!(detail.site.segment, Some(0));
}

#[test]
fn a_garbage_header_at_the_tail_is_not_repaired_by_default() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 1);
    // Debris that happens to encode a huge length. Its header checksum does
    // not verify, so the length means nothing — and a record whose header
    // cannot be trusted might equally be a complete, acknowledged record
    // that rotted. Recovery refuses to decide.
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    bytes.extend_from_slice(&[0u8; (RECORD_HEADER_LEN - 4) as usize]);
    std::fs::write(&path, &bytes).expect("write");

    // Explicitly the default policy, not the permissive test helper.
    let err = scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, false)
        .expect_err("ambiguous tail must not be truncated silently");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert!(matches!(
        detail.kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn an_operator_can_opt_in_to_repairing_an_ambiguous_tail() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 1);
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    bytes.extend_from_slice(&[0u8; (RECORD_HEADER_LEN - 4) as usize]);
    std::fs::write(&path, &bytes).expect("write");

    let outcome =
        scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, true).expect("opt-in repair");
    assert_eq!(outcome.record_count, 1);
    assert!(matches!(
        outcome.torn_tail.expect("tail").cause,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn a_payload_cut_short_by_a_crash_is_still_repaired_automatically() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let full = write_segment(&path, 0, 3);
    // Cut inside the last record's payload: the header is intact and
    // verifies, so its length is trustworthy and the shortfall is provably
    // an unfinished write. This is the ordinary crash case and must not
    // require operator intervention.
    std::fs::write(&path, &full[..full.len() - 3]).expect("write");

    let outcome = scan_segment(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, false)
        .expect("a torn payload is provably incomplete");
    assert_eq!(outcome.record_count, 2);
    assert!(matches!(
        outcome.torn_tail.expect("tail").cause,
        CorruptionKind::Truncated { .. }
    ));
}

#[test]
fn scan_rebuilds_an_index_that_matches_the_records() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 20);

    let outcome = scan_segment(&path, 0, "t/ns/s/0", 64, ScanStart::Full, true).expect("scan");
    assert!(outcome.index.len() > 1);
    for entry in outcome.index.entries() {
        assert!(entry.position >= SEGMENT_HEADER_LEN);
        assert!(entry.position < outcome.valid_bytes);
    }
    // Every indexed position must actually start the record it claims.
    for entry in outcome.index.entries() {
        let found = read_all(&path, &outcome, entry.offset);
        assert_eq!(found[0].offset, entry.offset);
    }
}

#[test]
fn scan_reads_records_larger_than_the_read_ahead_window() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let big = vec![7u8; READ_CHUNK_BYTES * 2 + 11];
    let mut bytes = SegmentHeader::new(0, 1).encode().to_vec();
    encode_record(&mut bytes, 0, 1, &big, &Default::default());
    encode_record(&mut bytes, 1, 2, b"small", &Default::default());
    std::fs::write(&path, &bytes).expect("write");

    let outcome = scan(&path).expect("scan");
    assert_eq!(outcome.record_count, 2);
    let records = read_all(&path, &outcome, 0);
    assert_eq!(records[0].payload.len(), big.len());
    assert_eq!(records[1].payload, Bytes::from_static(b"small"));
}

#[test]
fn repairable_tail_rules() {
    // Provably incomplete: repaired regardless of policy.
    assert!(is_repairable_tail(
        &CorruptionKind::Truncated {
            needed: 10,
            available: 2
        },
        100,
        None,
        102,
        false
    ));
    // A complete-but-unverifiable tail: repaired only when opted in,
    // because it is indistinguishable from rot on acknowledged data.
    assert!(is_repairable_tail(
        &CorruptionKind::RecordChecksum {
            expected: 1,
            found: 2
        },
        100,
        Some(50),
        150,
        true
    ));
    assert!(!is_repairable_tail(
        &CorruptionKind::RecordChecksum {
            expected: 1,
            found: 2
        },
        100,
        Some(50),
        150,
        false
    ));
    // Committed records follow it, so it is interior damage either way.
    assert!(!is_repairable_tail(
        &CorruptionKind::RecordChecksum {
            expected: 1,
            found: 2
        },
        100,
        Some(50),
        300,
        true
    ));
    assert!(!is_repairable_tail(
        &CorruptionKind::SegmentMagic { found: 0 },
        0,
        None,
        100,
        true
    ));
}

/// The default policy, as recovery calls it for the active segment.
fn scan_strict(path: &std::path::Path) -> Result<ScanOutcome> {
    scan_segment(path, 0, "t/ns/s/0", 4096, ScanStart::Full, false)
}

/// Power loss can keep a file's new size while the blocks it covers come back
/// as zeros. That is a torn tail, and it must not need `repair_checksum_tail`.
#[test]
fn a_zero_filled_tail_is_torn_under_the_default_policy() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 3);
    let valid = bytes.len() as u64;
    bytes.resize(bytes.len() + 8192, 0);
    std::fs::write(&path, &bytes).expect("write");

    let outcome = scan_strict(&path).expect("a zeroed tail is repairable");
    assert_eq!(outcome.record_count, 3);
    assert_eq!(outcome.valid_bytes, valid);
    assert_eq!(outcome.torn_tail.expect("tail").discarded_bytes, 8192);
}

/// The header block of the last record reached the disk and the block after
/// it did not: the zeros start at a sector boundary inside the record.
#[test]
fn a_record_zeroed_from_a_sector_boundary_is_torn() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 2);
    let valid = bytes.len();
    encode_record(&mut bytes, 2, 102, &[0xAB; 1500], &Default::default());
    let lost_from = valid.next_multiple_of(512) + 512;
    assert!(lost_from < bytes.len());
    bytes[lost_from..].fill(0);
    bytes.resize(bytes.len() + 1024, 0);
    std::fs::write(&path, &bytes).expect("write");

    let outcome = scan_strict(&path).expect("a zeroed sector is a torn write");
    assert_eq!(outcome.record_count, 2);
    assert_eq!(outcome.valid_bytes, valid as u64);
    assert!(matches!(
        outcome.torn_tail.expect("tail").cause,
        CorruptionKind::RecordChecksum { .. }
    ));
}

/// Zeros that end before the file does are interior damage: a later record
/// made it to disk, so an fsync may have acknowledged what the zeros replaced.
#[test]
fn zeros_followed_by_a_valid_record_are_a_hard_error() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 2);
    let damaged = bytes.len();
    encode_record(&mut bytes, 2, 102, b"lost", &Default::default());
    encode_record(&mut bytes, 3, 103, b"kept", &Default::default());
    let second = bytes.len();
    encode_record(&mut bytes, 4, 104, b"last", &Default::default());
    bytes[damaged..second].fill(0);
    std::fs::write(&path, &bytes).expect("write");

    let err = scan_strict(&path).expect_err("interior zeros must not be truncated");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert_eq!(detail.site.position, Some(damaged as u64));
}

/// A final record whose last few bytes are zero, not from a sector boundary,
/// is a complete record that rotted: still refused by default.
#[test]
fn zeros_starting_mid_sector_in_the_last_record_are_not_a_torn_tail() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    let mut bytes = write_segment(&path, 0, 3);
    let len = bytes.len();
    bytes[len - 3..].fill(0);
    std::fs::write(&path, &bytes).expect("write");

    scan_strict(&path).expect_err("rot on a complete record is not a torn write");
}
