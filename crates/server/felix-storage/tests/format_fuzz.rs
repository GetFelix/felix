//! Deterministic fuzzing of the segment decoder and recovery path.
//!
//! These are the same properties the libFuzzer targets in `fuzz/` assert, driven
//! by a seeded generator so they run in the normal test suite and reproduce
//! exactly. The fuzz targets explore far more inputs; this file makes sure a
//! regression in the obvious ones cannot land without CI noticing.
//!
//! The properties, in order of importance:
//!
//! 1. **No panic, ever.** Untrusted bytes must produce a typed error, not an
//!    abort. A panic here is a remote crash once segments come from replication.
//! 2. **No unbounded allocation.** A corrupt length field must be rejected
//!    before it is used to size a buffer.
//! 3. **No silent data loss.** Recovery may truncate a torn *tail*; it must
//!    never quietly discard a record with committed data behind it.
//! 4. **Round-trip fidelity.** Anything the encoder writes, the decoder reads
//!    back byte-for-byte.
//!
//! Run the extended fuzzers with:
//!
//! ```text
//! cargo +nightly fuzz run segment_record   -- -max_total_time=300
//! cargo +nightly fuzz run segment_recovery -- -max_total_time=300
//! ```

use felix_storage::log::LogConfig;
use felix_storage::segment::format::{
    MAX_PAYLOAD_BYTES, RECORD_HEADER_LEN, SEGMENT_HEADER_LEN, SegmentHeader, decode_record,
    encode_record,
};
use felix_storage::segment::{ScanStart, scan_segment};
use felix_storage::{CorruptionKind, StorageError, segment};
use tempfile::tempdir;

/// CRC-32 (IEEE), matching what the format uses for its header checksum.
fn crc32_of(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

/// xorshift64*. Deterministic and dependency-free, so a failing seed printed in
/// an assertion reproduces the exact byte sequence that broke.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Zero is a fixed point of xorshift; nudge it.
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    /// `bytes(below(bound))` as one call, so the borrow checker does not force
    /// a temporary at every use site.
    fn bytes_below(&mut self, bound: usize) -> Vec<u8> {
        let len = self.below(bound);
        self.bytes(len)
    }
}

/// Build a well-formed segment of `count` records with pseudo-random payloads.
fn build_segment(rng: &mut Rng, count: usize, max_payload: usize) -> Vec<u8> {
    let mut bytes = SegmentHeader::new(0, 1).encode().to_vec();
    for offset in 0..count {
        let payload = rng.bytes_below(max_payload);
        encode_record(
            &mut bytes,
            offset as u64,
            offset as u64 * 7,
            &payload,
            None,
            &Default::default(),
        );
    }
    bytes
}

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 1024 * 1024,
        index_spacing_bytes: 128,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

#[test]
fn arbitrary_bytes_never_panic_the_record_decoder() {
    let mut rng = Rng::new(0xF0CA_CC1A);
    for iteration in 0..20_000 {
        let bytes = rng.bytes_below(96);
        // The contract is only "returns", not "returns Ok": random bytes are
        // overwhelmingly invalid and must produce typed errors.
        let outcome = decode_record(&bytes);
        if let Ok((decoded, consumed)) = outcome {
            assert_eq!(
                consumed,
                RECORD_HEADER_LEN + decoded.payload.len() as u64,
                "iteration {iteration}"
            );
        }
    }
}

#[test]
fn a_corrupt_length_field_is_rejected_before_allocation() {
    let mut rng = Rng::new(0xDEAD_BEEF);
    for _ in 0..5_000 {
        // A valid-looking header whose length field is enormous. If the decoder
        // sized a buffer from it before checking, this test would exhaust memory
        // rather than fail.
        let mut bytes = vec![0u8; RECORD_HEADER_LEN as usize];
        let claimed = (MAX_PAYLOAD_BYTES as u64 + 1 + rng.next_u64() % u32::MAX as u64) as u32;
        bytes[0..4].copy_from_slice(&claimed.to_be_bytes());
        // Give the header a valid checksum, or the length is rejected as
        // untrustworthy before its magnitude is ever considered — which is
        // itself correct, but not what this test is probing.
        let header_crc = crc32_of(&bytes[0..20]);
        bytes[20..24].copy_from_slice(&header_crc.to_be_bytes());

        // With both producer-mark bits set the word is refused as impossible
        // flags instead, which is just as early.
        let err = decode_record(&bytes).expect_err("oversized length");
        assert!(
            matches!(
                err.kind,
                CorruptionKind::RecordTooLarge { .. }
                    | CorruptionKind::Truncated { .. }
                    | CorruptionKind::RecordFlags { .. }
            ),
            "unexpected error for claimed length {claimed}: {err}"
        );
    }
}

#[test]
fn every_encoded_record_round_trips() {
    let mut rng = Rng::new(0x1234_5678);
    for iteration in 0..5_000 {
        let payload = rng.bytes_below(512);
        let offset = rng.next_u64();
        let timestamp = rng.next_u64();

        let mut bytes = Vec::new();
        let written = encode_record(
            &mut bytes,
            offset,
            timestamp,
            &payload,
            None,
            &Default::default(),
        );
        let (decoded, consumed) =
            decode_record(&bytes).unwrap_or_else(|err| panic!("iteration {iteration}: {err}"));

        assert_eq!(consumed, written);
        assert_eq!(decoded.header.offset, offset);
        assert_eq!(decoded.header.timestamp_micros, timestamp);
        assert_eq!(decoded.payload.as_ref(), payload.as_slice());
    }
}

#[test]
fn a_single_flipped_bit_is_always_detected() {
    let mut rng = Rng::new(0xABCD_1234);
    for iteration in 0..2_000 {
        let payload_len = 1 + rng.below(64);
        let payload = rng.bytes(payload_len);
        let mut bytes = Vec::new();
        encode_record(
            &mut bytes,
            rng.next_u64(),
            rng.next_u64(),
            &payload,
            None,
            &Default::default(),
        );

        let position = rng.below(bytes.len());
        let bit = 1u8 << rng.below(8);
        bytes[position] ^= bit;

        match decode_record(&bytes) {
            Err(_) => {}
            Ok((decoded, _)) => {
                // The only survivable flip is inside a field the checksum covers
                // but whose value we did not pin — there is none, so any Ok here
                // means the record decoded to something different than encoded.
                panic!(
                    "iteration {iteration}: flipping bit {bit:#04b} at byte {position} \
                     went undetected (offset {}, payload {} bytes)",
                    decoded.header.offset,
                    decoded.payload.len()
                );
            }
        }
    }
}

#[test]
fn truncating_a_segment_anywhere_never_panics_and_never_invents_records() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("00000000000000000000.log");
    let mut rng = Rng::new(0x5EED_5EED);
    let full = build_segment(&mut rng, 40, 48);

    let mut previous_count = None;
    for cut in (SEGMENT_HEADER_LEN as usize)..full.len() {
        std::fs::write(&path, &full[..cut]).expect("write");
        let outcome = scan_segment(&path, 0, "fuzz/shard/0", 128, ScanStart::Full, true)
            .unwrap_or_else(|err| panic!("cut at {cut}: {err}"));

        // Truncation can only ever remove records, never add them, and the
        // record count must move monotonically with the cut position.
        assert_eq!(outcome.record_count, outcome.next_offset);
        if let Some(previous) = previous_count {
            assert!(
                outcome.record_count >= previous,
                "cut at {cut} lost records relative to a shorter file"
            );
        }
        previous_count = Some(outcome.record_count);
        assert!(outcome.valid_bytes <= cut as u64);
    }
}

#[test]
fn corrupting_committed_bytes_is_reported_rather_than_dropped() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("00000000000000000000.log");
    let mut rng = Rng::new(0xC0FF_EE00);
    let clean = build_segment(&mut rng, 30, 32);

    let mut detected = 0usize;
    let mut repaired_tail = 0usize;
    for _ in 0..1_000 {
        let mut bytes = clean.clone();
        // Corrupt somewhere in the first half, so committed records always
        // follow the damage.
        let position = SEGMENT_HEADER_LEN as usize + rng.below(bytes.len() / 2);
        bytes[position] ^= 1u8 << rng.below(8);
        std::fs::write(&path, &bytes).expect("write");

        match scan_segment(&path, 0, "fuzz/shard/0", 128, ScanStart::Full, true) {
            // Loud failure is the correct outcome for interior damage.
            Err(StorageError::Corruption(detail)) => {
                assert!(detail.site.position.is_some());
                assert_eq!(detail.site.shard.as_deref(), Some("fuzz/shard/0"));
                detected += 1;
            }
            Err(other) => panic!("unexpected error kind: {other}"),
            Ok(outcome) => {
                // Recovery is allowed to treat damage as a torn tail only when
                // nothing decodable follows it. Anything it kept must still be
                // a contiguous prefix.
                assert!(
                    outcome.torn_tail.is_some(),
                    "corruption at byte {position} was neither reported nor truncated"
                );
                assert_eq!(outcome.record_count, outcome.next_offset);
                repaired_tail += 1;
            }
        }
    }

    // Corrupting the first half should overwhelmingly be interior damage; if
    // this ever inverts, the tail rule has become far too permissive.
    assert!(
        detected > repaired_tail,
        "only {detected} of {} corruptions were reported as interior damage",
        detected + repaired_tail
    );
}

#[test]
fn arbitrary_files_are_rejected_without_panicking() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("00000000000000000000.log");
    let mut rng = Rng::new(0x0BAD_F00D);

    for _ in 0..2_000 {
        let bytes = rng.bytes_below(256);
        std::fs::write(&path, &bytes).expect("write");
        // Garbage must be an error, never a panic and never a plausible log.
        match scan_segment(&path, 0, "fuzz/shard/0", 128, ScanStart::Full, true) {
            Ok(outcome) => assert_eq!(outcome.record_count, 0),
            Err(StorageError::Corruption(_)) | Err(StorageError::Io(_)) => {}
            Err(other) => panic!("unexpected error kind: {other}"),
        }
    }
}

#[tokio::test]
async fn recovery_of_arbitrary_directories_never_panics() {
    let mut rng = Rng::new(0xFEED_FACE);

    for iteration in 0..200 {
        let dir = tempdir().expect("dir");
        // A directory of plausible-looking but arbitrary segment files, plus
        // some files that are not ours at all.
        let segment_count = rng.below(4);
        for id in 0..segment_count {
            let bytes = if rng.below(2) == 0 {
                let records = rng.below(8);
                build_segment(&mut rng, records, 24)
            } else {
                rng.bytes_below(200)
            };
            std::fs::write(
                dir.path().join(segment::segment_file_name(id as u64)),
                bytes,
            )
            .expect("write");
        }
        std::fs::write(dir.path().join("README"), b"not a segment").expect("write");
        let index_bytes = rng.bytes_below(64);
        std::fs::write(dir.path().join(segment::index_file_name(0)), index_bytes).expect("write");

        // Either it opens or it reports corruption; both are fine, a panic or a
        // hang is not.
        match felix_storage::DiskLog::open(dir.path(), "fuzz/shard/0", config()) {
            Ok(_) | Err(StorageError::Corruption(_)) | Err(StorageError::Io(_)) => {}
            Err(other) => panic!("iteration {iteration}: unexpected error kind: {other}"),
        }
    }
}
