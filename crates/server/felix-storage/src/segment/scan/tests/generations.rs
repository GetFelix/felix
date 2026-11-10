//! v9 segments: records carry a generation, the header is 36 bytes, and the
//! scan holds generations to never fall.

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::io::writeback::{SplitMix64, Writeback, crash_contents};
use crate::segment::format::{GENERATION_HEADER_LEN, GENERATION_VERSION, encode_record_at};
use crate::segment::test_support::read_all;

const SECTOR: u64 = 512;

fn payload(offset: Offset) -> Vec<u8> {
    format!("record-{offset}").into_bytes()
}

/// A v9 segment holding one record per generation in `generations`, with the
/// byte position each record starts at.
fn v9_segment(generations: &[u64]) -> (Vec<u8>, Vec<u64>) {
    v9_segment_padded(generations, &|offset| payload(offset))
}

fn v9_segment_padded(
    generations: &[u64],
    payload: &dyn Fn(Offset) -> Vec<u8>,
) -> (Vec<u8>, Vec<u64>) {
    let mut bytes = SegmentHeader::at_version(0, 1, GENERATION_VERSION)
        .encode()
        .to_vec();
    let mut starts = Vec::new();
    for (offset, generation) in generations.iter().enumerate() {
        starts.push(bytes.len() as u64);
        encode_record_at(
            &mut bytes,
            Some(*generation),
            offset as u64,
            100 + offset as u64,
            &payload(offset as u64),
            None,
            None,
            &RecordMark::None,
        );
    }
    (bytes, starts)
}

fn write(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    std::fs::write(&path, bytes).expect("write");
    (dir, path)
}

#[test]
fn a_v9_segment_scans_and_reads_with_its_generations() {
    let (bytes, _) = v9_segment(&[1, 1, 2, 5, 5]);
    let (_dir, path) = write(&bytes);

    let outcome = scan_strict(&path).expect("scan");
    assert_eq!(outcome.header.version, GENERATION_VERSION);
    assert_eq!(outcome.record_count, 5);
    assert_eq!(outcome.valid_bytes, bytes.len() as u64);
    assert_eq!(outcome.last_generation, Some(5));
    assert!(outcome.torn_tail.is_none());

    let records = read_all(&path, &outcome, 2);
    let offsets: Vec<Offset> = records.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, [2, 3, 4]);
    for record in &records {
        assert_eq!(record.payload, Bytes::from(payload(record.offset)));
    }
}

#[test]
fn a_v8_segment_reports_no_generation() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("a.log");
    write_segment(&path, 0, 3);
    assert_eq!(scan_strict(&path).expect("scan").last_generation, None);
}

/// A falling generation is never a torn write: every checksum held. It stops
/// the open in the middle of a segment and at its end, under either policy.
#[test]
fn a_falling_generation_is_corruption() {
    for generations in [&[3, 2, 4][..], &[3, 3, 2]] {
        let (bytes, starts) = v9_segment(generations);
        let fell_at = generations
            .windows(2)
            .position(|pair| pair[1] < pair[0])
            .expect("a fall")
            + 1;
        let (_dir, path) = write(&bytes);
        for repair_checksum_tail in [false, true] {
            let err = scan_segment(
                &path,
                0,
                "t/ns/s/0",
                4096,
                ScanStart::Full,
                repair_checksum_tail,
            )
            .expect_err("a fallen generation");
            let StorageError::Corruption(corruption) = err else {
                panic!("expected corruption, got {err}");
            };
            assert!(
                matches!(
                    corruption.kind,
                    CorruptionKind::GenerationFell {
                        previous: 3,
                        found: 2
                    }
                ),
                "{corruption}"
            );
            assert_eq!(corruption.site.position, Some(starts[fell_at]));
        }
    }
}

/// A crash can stop the last record's write at any byte, including inside
/// its generation field. Every cut is a torn tail that leaves the records
/// before it.
#[test]
fn a_v9_header_cut_at_any_byte_is_a_torn_tail() {
    let (bytes, starts) = v9_segment(&[1, 2, 2]);
    let last = *starts.last().expect("records") as usize;
    for cut in last..bytes.len() {
        let (_dir, path) = write(&bytes[..cut]);
        let outcome = scan_strict(&path).unwrap_or_else(|err| panic!("cut at {cut}: {err}"));
        assert_eq!(outcome.record_count, 2, "cut at {cut}");
        assert_eq!(outcome.valid_bytes, last as u64, "cut at {cut}");
        assert_eq!(outcome.last_generation, Some(2), "cut at {cut}");
        if cut > last {
            assert_eq!(
                outcome.torn_tail.expect("torn").position,
                last as u64,
                "cut at {cut}"
            );
        }
    }
}

/// The file size reached the disk but the sector holding the end of the last
/// record's header did not, so it reads as zeros from that sector boundary.
/// Each placement of the boundary inside the 36-byte header, the generation
/// field and both checksums included, is a torn tail.
#[test]
fn a_v9_header_zeroed_from_a_sector_boundary_is_a_torn_tail() {
    for into_header in 1..GENERATION_HEADER_LEN {
        // Pad the first record so the second starts `into_header` bytes
        // before a sector boundary.
        let first_len = GENERATION_HEADER_LEN + payload(0).len() as u64;
        let fill = SECTOR - SEGMENT_HEADER_LEN - first_len - into_header;
        let (mut bytes, starts) = v9_segment_padded(&[4, 4], &|offset| {
            let mut out = payload(offset);
            if offset == 0 {
                out.extend(std::iter::repeat_n(b'.', fill as usize));
            }
            out
        });
        let last = starts[1];
        assert_eq!(last + into_header, SECTOR);
        bytes[SECTOR as usize..].fill(0);
        let (_dir, path) = write(&bytes);

        let outcome = scan_strict(&path)
            .unwrap_or_else(|err| panic!("zeros {into_header} bytes into the header: {err}"));
        assert_eq!(outcome.record_count, 1, "{into_header}");
        assert_eq!(outcome.valid_bytes, last, "{into_header}");
        assert_eq!(
            outcome.torn_tail.expect("torn").position,
            last,
            "{into_header}"
        );
    }
}

/// A header that does not verify, with the file ending at its last byte,
/// is a torn tail when the operator opted into repairing checksum tails.
#[test]
fn a_v9_header_failing_its_checksum_at_the_end_is_repairable() {
    let (mut bytes, starts) = v9_segment(&[1, 1]);
    let last = starts[1] as usize;
    bytes.truncate(last + GENERATION_HEADER_LEN as usize);
    bytes[last + 21] ^= 0xFF;
    let (_dir, path) = write(&bytes);
    let outcome = scan(&path).expect("repairable");
    assert_eq!(outcome.record_count, 1);
    assert_eq!(outcome.valid_bytes, last as u64);
    assert!(matches!(
        outcome.torn_tail.expect("torn").cause,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

/// Interior damage stays fatal in a v9 segment: a flipped generation bit in
/// a record with records after it.
#[test]
fn a_damaged_v9_header_in_the_middle_stops_the_scan() {
    let (mut bytes, starts) = v9_segment(&[1, 1, 1]);
    bytes[starts[1] as usize + 24] ^= 0x01;
    let (_dir, path) = write(&bytes);
    for repair_checksum_tail in [false, true] {
        let err = scan_segment(
            &path,
            0,
            "t/ns/s/0",
            4096,
            ScanStart::Full,
            repair_checksum_tail,
        )
        .expect_err("interior damage");
        assert!(matches!(
            err,
            StorageError::Corruption(Corruption {
                kind: CorruptionKind::RecordHeaderChecksum { .. },
                ..
            })
        ));
    }
}

/// The power-loss model over a v9 segment: the records up to the last flush
/// are on the device, the rest of the page cache is lost, written or torn by
/// sector. Every crash opens, keeps every flushed record, and keeps a prefix.
#[test]
fn every_power_loss_of_a_v9_segment_recovers_a_prefix() {
    // Short records of varying length, so headers straddle sectors and pages.
    let generations: Vec<u64> = (0..400).map(|i| 1 + i / 50).collect();
    let (bytes, starts) = v9_segment_padded(&generations, &|offset| {
        let mut out = payload(offset);
        out.extend(std::iter::repeat_n(b'x', (offset * 7 % 41) as usize));
        out
    });
    for flushed in [0usize, 37, 150, 399] {
        let synced = &bytes[..starts[flushed] as usize];
        for seed in 0..200u64 {
            for writeback in [Writeback::InOrder, Writeback::AnySubset] {
                let mut rng = SplitMix64::new(seed ^ ((flushed as u64) << 32));
                let image = crash_contents(synced, &bytes, &mut rng, writeback);
                let (_dir, path) = write(&image);
                let replay = format!("flushed {flushed}, seed {seed}, {writeback:?}");
                let repair = TailRepair {
                    checksum_tail: false,
                    unsynced_from: Some(synced.len() as u64),
                };
                let mut outcomes = vec![
                    scan_segment_with(&path, 0, "t/ns/s/0", 4096, ScanStart::Full, repair)
                        .unwrap_or_else(|err| panic!("{replay}: {err}")),
                ];
                // In order, the zeros run from the cut to the end of the
                // file, so even with no durable mark the tail is repaired.
                if writeback == Writeback::InOrder {
                    outcomes.push(
                        scan_strict(&path).unwrap_or_else(|err| panic!("{replay}, strict: {err}")),
                    );
                }
                for outcome in outcomes {
                    let kept = outcome.record_count as usize;
                    assert!(kept >= flushed, "{replay}: lost flushed records");
                    assert_eq!(
                        outcome.valid_bytes,
                        *starts.get(kept).unwrap_or(&(bytes.len() as u64)),
                        "{replay}"
                    );
                    assert_eq!(
                        outcome.last_generation,
                        kept.checked_sub(1).map(|last| generations[last]),
                        "{replay}"
                    );
                }
            }
        }
    }
}
