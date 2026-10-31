use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::segment::format::crc32;
use crate::segment::{ScanOutcome, ScanStart, index_file_name, scan_segment};

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 42,
        mark: Default::default(),
        publisher: None,
    }
}

fn new_writer(dir: &TempDir, base: Offset) -> SegmentWriter {
    SegmentWriter::create(dir.path(), 0, base, 1, 0, 4096).expect("create")
}

fn scan(dir: &TempDir) -> ScanOutcome {
    scan_segment(
        &dir.path().join(segment_file_name(0)),
        0,
        "t/ns/s/0",
        4096,
        ScanStart::Full,
        true,
    )
    .expect("scan")
}

#[test]
fn a_fresh_segment_holds_only_its_header() {
    let dir = tempdir().expect("dir");
    let writer = new_writer(&dir, 0);
    assert_eq!(writer.size_bytes(), SEGMENT_HEADER_LEN);
    assert_eq!(writer.next_offset(), 0);
    assert_eq!(writer.record_count(), 0);
    assert!(writer.is_synced());
    assert_eq!(scan(&dir).record_count, 0);
}

#[test]
fn appends_assign_consecutive_offsets() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 100);

    let (first, last) = writer
        .append(&[record("a"), record("b"), record("c")])
        .expect("append");
    assert_eq!((first, last), (100, 102));

    let (first, last) = writer.append(&[record("d")]).expect("append");
    assert_eq!((first, last), (103, 103));
    assert_eq!(writer.next_offset(), 104);
    assert_eq!(writer.record_count(), 4);
}

#[test]
fn appended_records_read_back_in_order() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer
        .append(&[record("first"), record("second")])
        .expect("append");
    writer.sync().expect("sync");

    let outcome = scan(&dir);
    assert_eq!(outcome.record_count, 2);
    assert_eq!(outcome.next_offset, 2);
    assert!(outcome.torn_tail.is_none());
}

#[test]
fn sync_state_tracks_written_versus_durable_bytes() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    assert_eq!(writer.unsynced_bytes(), 0);

    writer.append(&[record("payload")]).expect("append");
    assert!(!writer.is_synced());
    assert_eq!(
        writer.unsynced_bytes(),
        crate::segment::format::RECORD_HEADER_LEN + 7
    );

    writer.sync().expect("sync");
    assert!(writer.is_synced());
    assert_eq!(writer.unsynced_bytes(), 0);

    // Syncing again with nothing pending is a no-op, not an error.
    writer.sync().expect("resync");
}

#[test]
fn projected_size_predicts_the_post_append_size() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    let batch = [record("aaaa"), record("bb")];
    let projected = writer.projected_size(&batch);
    writer.append(&batch).expect("append");
    assert_eq!(writer.size_bytes(), projected);
}

#[test]
fn empty_payloads_are_valid_records() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("")]).expect("append");
    writer.sync().expect("sync");
    assert_eq!(scan(&dir).record_count, 1);
}

#[test]
fn an_oversized_payload_is_rejected_without_writing() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    let before = writer.size_bytes();
    let huge = AppendRecord {
        payload: Bytes::from(vec![0u8; MAX_PAYLOAD_BYTES as usize + 1]),
        timestamp_micros: 0,
        mark: Default::default(),
        publisher: None,
    };
    assert!(matches!(
        writer.append(&[huge]).expect_err("oversized"),
        StorageError::Unsupported(_)
    ));
    assert_eq!(writer.size_bytes(), before);
    assert_eq!(writer.next_offset(), 0);
}

#[test]
fn a_rejected_batch_leaves_no_partial_records() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    let batch = vec![
        record("fine"),
        AppendRecord {
            payload: Bytes::from(vec![0u8; MAX_PAYLOAD_BYTES as usize + 1]),
            timestamp_micros: 0,
            mark: Default::default(),
            publisher: None,
        },
    ];
    assert!(writer.append(&batch).is_err());
    writer.sync().expect("sync");
    // The valid first record must not have been written either: the batch
    // is validated in full before any byte reaches the file.
    assert_eq!(scan(&dir).record_count, 0);
}

#[test]
fn descriptor_reports_the_offset_and_byte_range() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 50);
    assert_eq!(writer.descriptor().last_offset, 50);

    writer.append(&[record("a"), record("b")]).expect("append");
    let descriptor = writer.descriptor();
    assert_eq!(descriptor.id, 0);
    assert_eq!(descriptor.base_offset, 50);
    assert_eq!(descriptor.last_offset, 51);
    assert_eq!(descriptor.size_bytes, writer.size_bytes());
}

#[test]
fn reopen_resumes_appending_where_recovery_left_off() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("a"), record("b")]).expect("append");
    writer.sync().expect("sync");
    let outcome = scan(&dir);
    drop(writer);

    let mut writer = SegmentWriter::reopen(
        dir.path(),
        0,
        ResumeState {
            base_offset: 0,
            valid_bytes: outcome.valid_bytes,
            next_offset: outcome.next_offset,
            record_count: outcome.record_count,
            index: outcome.index,
            version: crate::segment::format::FORMAT_VERSION,
        },
        0,
        4096,
    )
    .expect("reopen");
    assert_eq!(writer.next_offset(), 2);
    writer.append(&[record("c")]).expect("append");
    writer.sync().expect("sync");

    let outcome = scan(&dir);
    assert_eq!(outcome.record_count, 3);
    assert_eq!(outcome.next_offset, 3);
    assert!(outcome.torn_tail.is_none());
}

#[test]
fn reopen_truncates_a_torn_tail_off_the_file() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("a"), record("b")]).expect("append");
    writer.sync().expect("sync");
    let valid_bytes = writer.size_bytes();
    drop(writer);

    // Simulate an interrupted third append.
    let path = dir.path().join(segment_file_name(0));
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0xAB; 9]);
    std::fs::write(&path, &bytes).expect("write");

    let outcome = scan(&dir);
    assert!(outcome.torn_tail.is_some());
    assert_eq!(outcome.valid_bytes, valid_bytes);

    SegmentWriter::reopen(
        dir.path(),
        0,
        ResumeState {
            base_offset: 0,
            valid_bytes: outcome.valid_bytes,
            next_offset: outcome.next_offset,
            record_count: outcome.record_count,
            index: outcome.index,
            version: crate::segment::format::FORMAT_VERSION,
        },
        0,
        4096,
    )
    .expect("reopen");

    // The file is now exactly the valid prefix, and a rescan is clean —
    // recovery is idempotent.
    assert_eq!(std::fs::metadata(&path).expect("meta").len(), valid_bytes);
    assert!(scan(&dir).torn_tail.is_none());
}

#[test]
fn mark_synced_is_monotonic_and_clamped() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("abc")]).expect("append");
    let size = writer.size_bytes();

    writer.mark_synced(size);
    assert!(writer.is_synced());
    // A stale, lower report cannot un-sync durable bytes.
    writer.mark_synced(0);
    assert!(writer.is_synced());
    // Nor can an over-report claim bytes that were never written.
    writer.mark_synced(u64::MAX);
    assert_eq!(writer.unsynced_bytes(), 0);

    writer.append(&[record("def")]).expect("append");
    assert!(!writer.is_synced());
}

#[test]
fn a_partially_written_batch_is_rewound_and_leaves_no_debris() {
    use std::io::{Seek as _, SeekFrom as _SeekFrom, Write as _};

    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("first")]).expect("append");
    let good_len = writer.size_bytes();

    // Stand in for a `write_all` that failed part-way: bytes on disk past
    // the last byte the writer accounts for. This is what the OS can leave
    // behind on ENOSPC or EIO mid-batch.
    {
        let path = dir.path().join(segment_file_name(0));
        let mut handle = OpenOptions::new().write(true).open(&path).expect("open");
        handle.seek(_SeekFrom::End(0)).expect("seek");
        handle.write_all(&[0xAB; 11]).expect("write debris");
        handle.sync_all().expect("sync");
    }
    assert_eq!(
        std::fs::metadata(dir.path().join(segment_file_name(0)))
            .expect("meta")
            .len(),
        good_len + 11
    );

    writer.rewind_after_failed_write().expect("rewind");

    // The debris is gone and the file matches the writer's own accounting.
    assert_eq!(
        std::fs::metadata(dir.path().join(segment_file_name(0)))
            .expect("meta")
            .len(),
        good_len
    );

    // And the segment keeps working: the next append lands contiguously
    // rather than after a hole, so a scan stays clean.
    writer
        .append(&[record("second")])
        .expect("append after rewind");
    writer.sync().expect("sync");
    let outcome = scan(&dir);
    assert_eq!(outcome.record_count, 2);
    assert!(
        outcome.torn_tail.is_none(),
        "rewound segment should scan clean, got {:?}",
        outcome.torn_tail
    );
}

#[test]
fn a_poisoned_writer_refuses_further_appends() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("a")]).expect("append");
    // A rewind that cannot be performed leaves the segment's real length
    // unknown; building on it would compound the damage.
    writer.poisoned = true;
    assert!(matches!(
        writer.append(&[record("b")]).expect_err("poisoned"),
        StorageError::Unsupported(_)
    ));
}

/// A failed sync poisons the writer, so nothing afterwards can claim
/// durability.
///
/// Linux may drop the dirty pages an fsync could not write. The next fsync
/// then returns success having flushed nothing, and the log would report
/// records durable that are gone — "fsyncgate". Refusing to carry on is the
/// same answer the log already gives to interior corruption.
#[test]
fn a_failed_sync_poisons_the_writer() {
    let dir = tempdir().expect("dir");
    let run = tempdir().expect("run dir");
    let boot = Some(crate::run_state::BootId::from_bytes([1; 16]));
    let begun =
        crate::run_state::begin(run.path(), true, None, boot, || false, || [1; 16]).expect("begin");
    crate::run_state::watch_poison(run.path());
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("a")]).expect("append");

    writer.fail_next_sync = true;
    assert!(matches!(
        writer.sync().expect_err("the sync should fail"),
        StorageError::SyncFailed(_)
    ));
    assert!(writer.poisoned, "a failed sync left the writer usable");
    // Before any flush poisons the log, a process crash must not keep the id.
    assert_eq!(
        crate::run_state::read(run.path(), begun.id).expect("read"),
        crate::run_state::LastRun::Crashed {
            relaxed: true,
            boot: None
        }
    );

    // The second sync would have succeeded on its own — that is the whole
    // danger — so it has to be refused rather than believed.
    assert!(matches!(
        writer.sync().expect_err("poisoned"),
        StorageError::Unsupported(_)
    ));
    assert!(matches!(
        writer.append(&[record("b")]).expect_err("poisoned"),
        StorageError::Unsupported(_)
    ));
}

/// The log-level syncer reports durability through `mark_synced`, which a
/// poisoned writer must not accept either.
#[test]
fn a_poisoned_writer_does_not_accept_a_durability_mark() {
    let dir = tempdir().expect("dir");
    let mut writer = new_writer(&dir, 0);
    writer.append(&[record("a")]).expect("append");
    let synced_before = writer.synced_bytes;

    writer.poisoned = true;
    writer.mark_synced(writer.size_bytes);

    assert_eq!(
        writer.synced_bytes, synced_before,
        "a poisoned writer accepted a durability mark from the log syncer",
    );
}

#[test]
fn seal_trims_preallocated_space() {
    let dir = tempdir().expect("dir");
    // Reserve far more than the records need.
    let mut writer = SegmentWriter::create(dir.path(), 0, 0, 1, 1024 * 1024, 4096).expect("create");
    writer.append(&[record("small")]).expect("append");
    let expected = writer.size_bytes();

    let descriptor = writer.seal().expect("seal");
    drop(writer);
    assert_eq!(descriptor.size_bytes, expected);
    assert_eq!(
        std::fs::metadata(dir.path().join(segment_file_name(0)))
            .expect("meta")
            .len(),
        expected
    );
    assert!(scan(&dir).torn_tail.is_none());
}

#[test]
fn the_index_is_populated_and_reloadable() {
    let dir = tempdir().expect("dir");
    let mut writer = SegmentWriter::create(dir.path(), 0, 0, 1, 0, 64).expect("create");
    for i in 0..20 {
        writer
            .append(&[record(&format!("record-{i}"))])
            .expect("append");
    }
    writer.sync().expect("sync");
    let in_memory = writer.index().clone();
    assert!(in_memory.len() > 1);

    let reloaded = SparseIndex::load(&dir.path().join(index_file_name(0)), 0).expect("load");
    assert_eq!(reloaded, in_memory);

    // And a rebuild from the segment must agree with both.
    let rebuilt = scan_segment(
        &dir.path().join(segment_file_name(0)),
        0,
        "t/ns/s/0",
        64,
        ScanStart::Full,
        true,
    )
    .expect("scan")
    .index;
    assert_eq!(rebuilt, in_memory);
}

#[test]
fn creating_over_an_existing_segment_fails() {
    let dir = tempdir().expect("dir");
    let _writer = new_writer(&dir, 0);
    assert!(SegmentWriter::create(dir.path(), 0, 0, 1, 0, 4096).is_err());
}

/// Each version's records and the batch that needs it.
fn records_for(version: u16) -> Vec<AppendRecord> {
    let mut records = vec![record("plain")];
    for mark in RecordMark::for_batch(4, 1, 2) {
        records.push(AppendRecord {
            mark,
            ..record("batch")
        });
    }
    if version >= 4 {
        records.push(AppendRecord {
            mark: RecordMark::GenerationStart,
            ..record("g")
        });
    }
    if version >= 5 {
        records.push(AppendRecord {
            mark: RecordMark::Commit,
            ..record("c")
        });
    }
    if version >= 6 {
        records.push(AppendRecord {
            publisher: Some(Bytes::from_static(b"alice")),
            ..record("p")
        });
    }
    records
}

/// A segment holding every kind its version allows is byte for byte what
/// the build before trim records wrote, index included. The checksums were
/// taken from that build.
#[test]
fn segments_without_trims_are_byte_for_byte_unchanged() {
    let dir = tempdir().expect("dir");
    let expected: [(u16, u64, u32, u64, u32); 4] = [
        (3, 151, 0xA12E_2EB3, 56, 0x7583_DF27),
        (4, 180, 0xD19A_A8CC, 56, 0x7583_DF27),
        (5, 209, 0x8E83_4724, 56, 0x7583_DF27),
        (6, 244, 0x548A_6A28, 72, 0xE44F_3774),
    ];
    for (version, seg_len, seg_crc, idx_len, idx_crc) in expected {
        let id = u64::from(version);
        let mut writer = SegmentWriter::create_at_version(dir.path(), id, 10, 1, 0, 64, version)
            .expect("create");
        let records = records_for(version);
        assert!(writer.successor_version(&records) <= version);
        writer.append(&records).expect("append");
        writer.sync().expect("sync");
        drop(writer);
        let segment = std::fs::read(dir.path().join(segment_file_name(id))).expect("segment");
        let index = std::fs::read(dir.path().join(index_file_name(id))).expect("index");
        assert_eq!(
            (segment.len() as u64, crc32(&[&segment])),
            (seg_len, seg_crc),
            "v{version} segment"
        );
        assert_eq!(
            (index.len() as u64, crc32(&[&index])),
            (idx_len, idx_crc),
            "v{version} index"
        );
    }
}

/// Only a trim record moves a log onto v7: no other record asks for it,
/// and a segment older than v7 refuses one.
#[test]
fn only_a_trim_needs_a_v7_segment() {
    let dir = tempdir().expect("dir");
    let trim = AppendRecord {
        mark: RecordMark::Trim { floor: 0 },
        ..record("headers")
    };
    let mut writer =
        SegmentWriter::create_at_version(dir.path(), 0, 0, 1, 0, 64, 6).expect("create");
    assert_eq!(writer.successor_version(&records_for(6)), 6);
    assert!(!writer.holds(std::slice::from_ref(&trim)));
    assert!(matches!(
        writer.append(std::slice::from_ref(&trim)),
        Err(StorageError::Unsupported(_))
    ));
    // A trim with a publisher still needs v7, not v6.
    let published = AppendRecord {
        publisher: Some(Bytes::from_static(b"alice")),
        ..trim.clone()
    };
    assert_eq!(writer.successor_version(&[published]), TRIM_VERSION);
    let baseline_dir = tempdir().expect("dir");
    let baseline = new_writer(&baseline_dir, 0);
    assert_eq!(
        baseline.successor_version(&records_for(3)),
        BASELINE_VERSION
    );
    assert_eq!(
        baseline.successor_version(std::slice::from_ref(&trim)),
        TRIM_VERSION
    );

    let dir = tempdir().expect("dir");
    let mut writer =
        SegmentWriter::create_at_version(dir.path(), 0, 0, 1, 0, 64, TRIM_VERSION).expect("create");
    assert!(writer.holds_trims());
    let mut batch = records_for(6);
    batch.push(trim);
    writer.append(&batch).expect("append");
    writer.sync().expect("sync");
    let outcome = scan(&dir);
    assert_eq!(outcome.record_count, batch.len() as u64);
    assert!(outcome.torn_tail.is_none());
    assert_eq!(
        outcome
            .marks
            .last()
            .map(|(offset, mark, _)| (*offset, *mark)),
        Some((batch.len() as u64 - 1, RecordMark::Trim { floor: 0 }))
    );
}
