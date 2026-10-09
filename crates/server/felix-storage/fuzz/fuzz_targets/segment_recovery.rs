//! Whole-segment recovery against arbitrary file contents.
//!
//! The interesting property is not "does it parse" but "does it lie": recovery
//! is permitted to discard a torn tail and nothing else, so any log it does
//! return must be a contiguous, self-consistent prefix. And the plan offline
//! inspection reads must be the verdict startup then reaches.

#![no_main]

use libfuzzer_sys::fuzz_target;

use felix_storage::StorageError;
use felix_storage::segment::{ScanStart, scan_segment};

fuzz_target!(|data: &[u8]| {
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let path = dir.path().join("00000000000000000000.log");
    if std::fs::write(&path, data).is_err() {
        return;
    }

    // Both truncation policies, chosen by a byte of the input so the fuzzer can
    // steer it. With `repair_checksum_tail` off, only a provably incomplete
    // trailing record may be dropped — the stricter of the two, and the one
    // where wrongly discarding a record is silent data loss.
    let repair_checksum_tail = data.first().is_some_and(|byte| byte & 1 == 1);

    match scan_segment(
        &path,
        0,
        "fuzz/shard/0",
        128,
        ScanStart::Full,
        repair_checksum_tail,
    ) {
        // Property 1: whatever recovery accepts is internally consistent —
        // the record count matches the offsets, and the valid region fits
        // inside the file it came from.
        Ok(outcome) => {
            assert_eq!(
                outcome.record_count,
                outcome.next_offset - outcome.header.base_offset
            );
            assert!(outcome.valid_bytes <= data.len() as u64);
            if let Some(tail) = &outcome.torn_tail {
                assert_eq!(tail.position, outcome.valid_bytes);
                assert!(tail.discarded_bytes <= data.len() as u64);
            }
            // Property 2: every index entry points inside the valid region, so
            // a seek can never land past the end of committed data.
            for entry in outcome.index.entries() {
                assert!(entry.position < outcome.valid_bytes.max(1));
            }
        }
        // Property 3: failures are typed. A panic or an unexpected error kind
        // means untrusted bytes reached code that assumed they were valid.
        Err(StorageError::Corruption(_)) | Err(StorageError::Io(_)) => {}
        Err(other) => panic!("unexpected error kind: {other}"),
    }

    // Property 4: planning recovery writes nothing and predicts what
    // recovering the same directory then does.
    felix_storage::fuzzing::recovery_plan_agrees(dir.path(), repair_checksum_tail);
});
