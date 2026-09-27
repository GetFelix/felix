use std::path::PathBuf;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::log::{AppendRecord, FsyncMode};
use crate::segment::format::SEGMENT_HEADER_LEN;

fn segment_path(dir: &Path, id: SegmentId) -> PathBuf {
    dir.join(segment_file_name(id))
}

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: SEGMENT_HEADER_LEN + 80,
        index_spacing_bytes: 32,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 1,
        mark: Default::default(),
    }
}

/// Write `count` records through a real `SegmentSet`, rolling as configured.
fn populate(dir: &TempDir, count: usize) -> u64 {
    let recovered = recover_shard(dir.path(), "t/ns/s/0", &config()).expect("recover");
    let mut set = crate::disk_log::segments::SegmentSet::new(
        dir.path().to_path_buf(),
        "t/ns/s/0".into(),
        config(),
        recovered.sealed,
        recovered.active,
        crate::disk_log::sealed::SealedFiles::new(16),
    )
    .expect("set");
    for i in 0..count {
        set.append(&[record(&format!("value-{i:03}"))])
            .expect("append");
    }
    set.active_mut().sync().expect("sync");
    set.tail_offset()
}

fn reopen(dir: &TempDir) -> Result<Recovered> {
    recover_shard(dir.path(), "t/ns/s/0", &config())
}

/// Reproduce the on-disk state left by a crash while a rollover was
/// *preparing*: a replacement segment exists, fsynced, but was never
/// installed. `base_offset` is whatever the tail was when it was built.
fn plant_uninstalled_preparation(dir: &TempDir, base_offset: Offset) -> SegmentId {
    let id = discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .copied()
        .expect("ids")
        + 1;
    SegmentWriter::create(
        dir.path(),
        id,
        base_offset,
        now_micros(),
        0,
        config().index_spacing_bytes,
    )
    .expect("create");
    id
}

#[test]
fn a_crash_before_the_header_is_written_leaves_the_log_openable() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    // A rollover interrupted between creating the segment and activating
    // it: the file exists, preallocated and directory-synced, but carries
    // no header and so claims no base offset at all.
    let id = discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .copied()
        .expect("ids")
        + 1;
    let path = dir.path().join(segment_file_name(id));
    std::fs::write(&path, b"").expect("create");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(!path.exists());
}

#[test]
fn a_partially_written_header_is_also_an_uninstalled_rollover() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    let id = discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .copied()
        .expect("ids")
        + 1;
    let path = dir.path().join(segment_file_name(id));
    // Torn mid-header. Nothing can have been acknowledged here.
    std::fs::write(&path, vec![0u8; (SEGMENT_HEADER_LEN - 1) as usize]).expect("create");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(!path.exists());
}

#[test]
fn a_crash_while_preparing_a_rollover_leaves_the_log_openable() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    // Built when the tail was 3 records back; appends kept going after.
    // This is the ordinary shape of the background roll, interrupted.
    let planted = plant_uninstalled_preparation(&dir, tail - 3);

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail, "no record was lost");
    assert!(
        !dir.path().join(segment_file_name(planted)).exists(),
        "the uninstalled preparation should have been removed",
    );
}

#[test]
fn a_preparation_stranded_ahead_of_the_tail_is_also_discarded() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    // The mirror image: a truncation rewound the log underneath a
    // replacement that had already been built for a higher offset.
    let planted = plant_uninstalled_preparation(&dir, tail + 50);

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(!dir.path().join(segment_file_name(planted)).exists());
}

#[test]
fn an_empty_newest_segment_at_the_tail_is_a_completed_roll_and_is_kept() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    // Same shape as an abandoned preparation except for the one thing that
    // distinguishes them: its base offset *is* the tail. That is simply a
    // roll that finished with nothing appended yet.
    let planted = plant_uninstalled_preparation(&dir, tail);

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.id(), planted, "it is the active segment");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(dir.path().join(segment_file_name(planted)).exists());
}

#[test]
fn successive_crashes_can_strand_more_than_one_preparation() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    let first = plant_uninstalled_preparation(&dir, tail - 3);
    let second = plant_uninstalled_preparation(&dir, tail - 1);

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    for id in [first, second] {
        assert!(!dir.path().join(segment_file_name(id)).exists());
    }
}

#[test]
fn a_crash_while_sealing_a_retired_segment_loses_nothing() {
    let dir = tempdir().expect("dir");
    // A retired segment that was never sealed is byte-identical to one that
    // was: sealing only syncs and trims to `size_bytes`, and preallocation
    // never extends the logical length. So the crash window between
    // installing the replacement and flushing the retired segment leaves a
    // chain recovery reads normally -- which is what makes the background
    // roll safe without a durable roll-intent record.
    let tail = populate(&dir, 12);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    assert!(ids.len() > 1, "the test needs a retired segment");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert_eq!(recovered.sealed.len(), ids.len() - 1);
}

/// The segment before the active one, and the active one after it.
fn retired_and_active(dir: &TempDir) -> (PathBuf, PathBuf) {
    let ids = discover_segment_ids(dir.path()).expect("ids");
    assert!(ids.len() > 1, "the test needs a retired segment");
    (
        segment_path(dir.path(), ids[ids.len() - 2]),
        segment_path(dir.path(), ids[ids.len() - 1]),
    )
}

fn last_offset_of(path: &Path) -> Offset {
    scan_segment(path, 0, "t/ns/s/0", 32, ScanStart::Full, false)
        .expect("scan")
        .last_offset()
        .expect("records")
}

/// Power loss while a background roll was sealing: the retired segment's last
/// record came back as zeros, and the active segment starts after it. Nothing
/// past the tear was ever flushed, so recovery cuts there and drops the
/// segment that can no longer follow on.
#[test]
fn power_loss_while_sealing_recovers_to_the_torn_retired_segment() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, active) = retired_and_active(&dir);
    let lost = last_offset_of(&retired);
    let mut bytes = std::fs::read(&retired).expect("read");
    let len = bytes.len();
    let record = crate::segment::format::record_len(9, &Default::default()) as usize;
    bytes[len - record..].fill(0);
    std::fs::write(&retired, &bytes).expect("write");

    let recovered = reopen(&dir).expect("an unsealed retired segment is a torn tail");
    assert_eq!(recovered.active.next_offset(), lost);
    assert!(!active.exists(), "the segment past the tear was kept");
}

/// Only slack past the retired segment's last record was lost, so the active
/// segment still follows on exactly and is kept.
#[test]
fn a_zeroed_slack_on_the_retired_segment_keeps_the_next_segment() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    let (retired, active) = retired_and_active(&dir);
    let good_len = std::fs::metadata(&retired).expect("meta").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&retired)
        .expect("open")
        .set_len(good_len + 4096)
        .expect("extend");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(active.exists());
    assert_eq!(std::fs::metadata(&retired).expect("meta").len(), good_len);
}

/// Damage in the retired segment that a crash cannot leave is still fatal,
/// and the segment after it is left alone.
#[test]
fn a_rotted_record_in_the_retired_segment_is_still_fatal() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, active) = retired_and_active(&dir);
    let mut bytes = std::fs::read(&retired).expect("read");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&retired, &bytes).expect("write");

    reopen(&dir).expect_err("rot is not a torn tail");
    assert!(active.exists());
}

/// A background roll writes the new segment's header without a flush of its
/// own. Power loss can leave it as zeros; nothing in that segment was ever
/// flushed, so it is an unfinished roll rather than a corrupt segment.
#[test]
fn a_zeroed_header_on_the_newest_segment_is_an_unfinished_roll() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, active) = retired_and_active(&dir);
    let retired_end = last_offset_of(&retired) + 1;
    let mut bytes = std::fs::read(&active).expect("read");
    bytes[..SEGMENT_HEADER_LEN as usize].fill(0);
    std::fs::write(&active, &bytes).expect("write");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), retired_end);
    assert!(!active.exists());
}

#[test]
fn an_empty_directory_starts_a_fresh_log() {
    let dir = tempdir().expect("dir");
    let recovered = reopen(&dir).expect("recover");
    assert!(recovered.sealed.is_empty());
    assert_eq!(recovered.active.next_offset(), 0);
    assert_eq!(recovered.truncated_bytes, 0);
}

#[test]
fn discovery_orders_segments_numerically_not_lexically() {
    let dir = tempdir().expect("dir");
    for id in [0u64, 2, 10, 3] {
        std::fs::write(dir.path().join(segment_file_name(id)), b"").expect("write");
    }
    std::fs::write(dir.path().join("notes.txt"), b"ignored").expect("write");
    std::fs::write(dir.path().join(index_file_name(0)), b"ignored").expect("write");

    assert_eq!(
        discover_segment_ids(dir.path()).expect("discover"),
        vec![0, 2, 3, 10]
    );
}

#[test]
fn a_clean_log_reopens_with_every_record() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert_eq!(recovered.truncated_bytes, 0);
    assert!(!recovered.sealed.is_empty(), "expected rollovers");
}

#[test]
fn recovery_is_idempotent() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);

    let first = reopen(&dir).expect("first");
    let tail = first.active.next_offset();
    let sealed_count = first.sealed.len();
    drop(first);

    let bytes_before: Vec<u64> = discover_segment_ids(dir.path())
        .expect("ids")
        .iter()
        .map(|id| {
            std::fs::metadata(segment_path(dir.path(), *id))
                .expect("meta")
                .len()
        })
        .collect();

    let second = reopen(&dir).expect("second");
    assert_eq!(second.active.next_offset(), tail);
    assert_eq!(second.sealed.len(), sealed_count);
    assert_eq!(second.truncated_bytes, 0);
    drop(second);

    let bytes_after: Vec<u64> = discover_segment_ids(dir.path())
        .expect("ids")
        .iter()
        .map(|id| {
            std::fs::metadata(segment_path(dir.path(), *id))
                .expect("meta")
                .len()
        })
        .collect();
    assert_eq!(bytes_before, bytes_after);
}

#[test]
fn a_torn_tail_is_truncated_back_to_the_last_valid_record() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 5);
    let active_id = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("id");
    let path = segment_path(dir.path(), active_id);

    // Simulate a crash part-way through writing another record.
    let mut bytes = std::fs::read(&path).expect("read");
    let good_len = bytes.len() as u64;
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let recovered = reopen(&dir).expect("recover");
    assert_eq!(recovered.active.next_offset(), tail);
    assert_eq!(recovered.truncated_bytes, 13);
    assert_eq!(std::fs::metadata(&path).expect("meta").len(), good_len);
}

/// Power loss after the file grew but before its data blocks were written:
/// the size survives and the blocks read back as zeros. The default config has
/// to open this, not refuse it as interior corruption.
#[test]
fn a_zero_filled_tail_after_power_loss_recovers_with_the_default_config() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 5);
    let active_id = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("id");
    let path = segment_path(dir.path(), active_id);
    let good_len = std::fs::metadata(&path).expect("meta").len();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    file.set_len(good_len + 4096).expect("extend with zeros");
    drop(file);

    assert!(!config().repair_checksum_tail);
    let recovered = reopen(&dir).expect("a zeroed tail is a torn tail");
    assert_eq!(recovered.active.next_offset(), tail);
    assert_eq!(recovered.truncated_bytes, 4096);
    assert_eq!(std::fs::metadata(&path).expect("meta").len(), good_len);
}

#[test]
fn truncation_at_every_byte_of_a_trailing_record_recovers() {
    let dir = tempdir().expect("dir");
    populate(&dir, 5);
    let active_id = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("id");
    let path = segment_path(dir.path(), active_id);
    let full = std::fs::read(&path).expect("read");

    // Cut the file at every byte inside the last record and confirm the log
    // always comes back to a consistent prefix.
    for cut in (SEGMENT_HEADER_LEN as usize)..full.len() {
        std::fs::write(&path, &full[..cut]).expect("write");
        let recovered = reopen(&dir).expect("recover");
        let tail = recovered.active.next_offset();
        drop(recovered);
        // A second open must find nothing left to repair.
        let again = reopen(&dir).expect("recover again");
        assert_eq!(again.truncated_bytes, 0, "cut at {cut}");
        assert_eq!(again.active.next_offset(), tail, "cut at {cut}");
    }
}

#[test]
fn interior_corruption_in_the_active_segment_fails_loudly() {
    let dir = tempdir().expect("dir");
    // Six records fill the active segment with two, so the first of them has
    // a committed record after it and cannot be mistaken for a torn tail.
    populate(&dir, 6);
    let active_id = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("id");
    let path = segment_path(dir.path(), active_id);

    let mut bytes = std::fs::read(&path).expect("read");
    let first_record = SEGMENT_HEADER_LEN as usize + crate::segment::RECORD_HEADER_LEN as usize;
    assert!(
        bytes.len() as u64 > SEGMENT_HEADER_LEN + 2 * crate::segment::RECORD_HEADER_LEN,
        "the active segment needs more than one record"
    );
    bytes[first_record] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let err = reopen(&dir).expect_err("interior corruption");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert_eq!(detail.site.shard.as_deref(), Some("t/ns/s/0"));
    assert_eq!(detail.site.segment, Some(active_id));
    assert!(detail.site.position.is_some());
}

#[test]
fn corruption_in_a_sealed_segment_fails_loudly() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    assert!(ids.len() > 1, "expected a sealed segment");
    let sealed_id = ids[0];
    let path = segment_path(dir.path(), sealed_id);

    // Truncate a sealed segment: its bytes were already committed.
    let bytes = std::fs::read(&path).expect("read");
    std::fs::write(&path, &bytes[..bytes.len() - 3]).expect("write");

    let err = recover_shard(
        dir.path(),
        "t/ns/s/0",
        &LogConfig {
            verify_all_on_open: true,
            ..config()
        },
    )
    .expect_err("sealed corruption");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption");
    };
    assert_eq!(detail.site.segment, Some(sealed_id));
}

#[test]
fn a_missing_index_is_rebuilt() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    let sealed_id = ids[0];
    std::fs::remove_file(dir.path().join(index_file_name(sealed_id))).expect("remove");

    let recovered = reopen(&dir).expect("recover");
    assert!(recovered.index_rebuilds >= 2, "sealed plus active");
    assert!(dir.path().join(index_file_name(sealed_id)).exists());

    // The rebuilt index seeks to the same places as the original.
    let index = SparseIndex::load(&dir.path().join(index_file_name(sealed_id)), 0).expect("index");
    assert!(!index.is_empty());
    for indexed in index.entries() {
        assert_eq!(index.seek_position(indexed.offset), indexed.position);
    }
    drop(recovered);
}

#[test]
fn a_stale_index_is_replaced() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    let sealed_id = ids[0];
    // An index that claims a different segment generation is unusable.
    SparseIndex::new(9_999)
        .persist(&dir.path().join(index_file_name(sealed_id)))
        .expect("persist");

    reopen(&dir).expect("recover");
    let reloaded = SparseIndex::load(&dir.path().join(index_file_name(sealed_id)), 0);
    assert!(reloaded.is_some_and(|index| !index.is_empty()));
}

/// An index that loads but points somewhere wrong is still only an index: the
/// segment is rebuilt from, not declared corrupt because of it.
#[test]
fn a_sealed_index_pointing_at_the_wrong_place_is_rebuilt() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    let sealed_id = ids[0];
    let index_path = dir.path().join(index_file_name(sealed_id));
    let good = SparseIndex::load(&index_path, 0).expect("index");
    let last = *good.entries().last().expect("entry");

    // One byte into a record, and a claimed offset one past the real one:
    // each sends the resume scan to bytes that do not decode as claimed.
    for bad in [
        crate::segment::format::IndexEntry {
            offset: last.offset,
            position: last.position + 1,
        },
        crate::segment::format::IndexEntry {
            offset: last.offset + 1,
            position: last.position,
        },
    ] {
        let mut index = SparseIndex::new(0);
        for entry in &good.entries()[..good.len() - 1] {
            index.push(*entry);
        }
        index.push(bad);
        index.persist(&index_path).expect("persist");

        let recovered = reopen(&dir).expect("a bad index is rebuilt, not fatal");
        drop(recovered);
        assert_eq!(
            SparseIndex::load(&index_path, 0)
                .expect("rebuilt")
                .entries(),
            good.entries(),
        );
    }
}

#[test]
fn a_missing_segment_in_the_middle_is_an_error() {
    let dir = tempdir().expect("dir");
    populate(&dir, 20);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    assert!(ids.len() > 2, "need an interior segment");

    // Delete an interior segment, leaving an offset gap.
    std::fs::remove_file(segment_path(dir.path(), ids[1])).expect("remove");
    std::fs::remove_file(dir.path().join(index_file_name(ids[1]))).expect("remove");

    let err = reopen(&dir).expect_err("gap");
    let StorageError::Corruption(detail) = err else {
        panic!("expected corruption, got {err}");
    };
    assert!(matches!(
        detail.kind,
        CorruptionKind::OffsetOutOfOrder { .. }
    ));
}

#[test]
fn a_log_recovered_from_a_torn_tail_accepts_new_appends() {
    let dir = tempdir().expect("dir");
    populate(&dir, 5);
    let active_id = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("id");
    let path = segment_path(dir.path(), active_id);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x77; 7]);
    std::fs::write(&path, &bytes).expect("write");

    let recovered = reopen(&dir).expect("recover");
    let mut set = crate::disk_log::segments::SegmentSet::new(
        dir.path().to_path_buf(),
        "t/ns/s/0".into(),
        config(),
        recovered.sealed,
        recovered.active,
        crate::disk_log::sealed::SealedFiles::new(16),
    )
    .expect("set");
    let tail = set.tail_offset();
    set.append(&[record("after-recovery")]).expect("append");
    set.active_mut().sync().expect("sync");

    let reread = reopen(&dir).expect("recover again");
    assert_eq!(reread.active.next_offset(), tail + 1);
    assert_eq!(reread.truncated_bytes, 0);
}

/// Make room for a segment between the last two, so a test can plant one
/// mid-chain. Returns the freed id.
fn open_a_slot_before_the_active_segment(dir: &TempDir) -> SegmentId {
    let active = *discover_segment_ids(dir.path())
        .expect("ids")
        .last()
        .expect("ids");
    for name in [segment_file_name, index_file_name] {
        let from = dir.path().join(name(active));
        if from.exists() {
            std::fs::rename(&from, dir.path().join(name(active + 1))).expect("rename");
        }
    }
    active
}

/// A background roll that lost the race to an inline one deleted its blank
/// segment, and a power loss undid the unlink: an empty file now sits between
/// two installed segments. It never held a record, so recovery drops it.
#[test]
fn an_empty_segment_left_mid_chain_by_a_lost_roll_race_is_discarded() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 20);
    let blank = open_a_slot_before_the_active_segment(&dir);
    std::fs::File::create(segment_path(dir.path(), blank)).expect("plant");

    let recovered = reopen(&dir).expect("an empty mid-chain segment is not corruption");
    assert_eq!(recovered.active.next_offset(), tail);
    assert!(!segment_path(dir.path(), blank).exists());
}

/// A sealed segment that lost its bytes is not a blank preparation: dropping
/// it would leave a hole where its records were, so it is still fatal.
#[test]
fn a_sealed_segment_that_lost_its_bytes_is_still_fatal() {
    let dir = tempdir().expect("dir");
    populate(&dir, 20);
    let ids = discover_segment_ids(dir.path()).expect("ids");
    assert!(ids.len() > 2, "need an interior segment");
    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_path(dir.path(), ids[1]))
        .expect("open")
        .set_len(0)
        .expect("truncate");

    reopen(&dir).expect_err("records were lost");
    assert!(segment_path(dir.path(), ids[1]).exists());
}

/// Zero the retired segment's last record, as a power loss mid-seal would.
/// Returns the retired and active ids and the retired segment's length.
fn tear_the_retired_segment(dir: &TempDir) -> (SegmentId, SegmentId, u64) {
    let ids = discover_segment_ids(dir.path()).expect("ids");
    let (retired, active) = (ids[ids.len() - 2], ids[ids.len() - 1]);
    let path = segment_path(dir.path(), retired);
    let mut bytes = std::fs::read(&path).expect("read");
    let len = bytes.len();
    let record = crate::segment::format::record_len(9, &Default::default()) as usize;
    bytes[len - record..].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    (retired, active, len as u64)
}

fn record_mark(dir: &TempDir, segment: SegmentId, synced_bytes: u64) {
    crate::disk_log::durable_mark::MarkFile::open(dir.path())
        .expect("mark")
        .record(DurableMark {
            segment,
            synced_bytes,
        })
        .expect("record");
}

/// The mark says the active segment's records were synced, and every flush
/// syncs the retired segment first. A torn retired segment is then corruption,
/// and deleting the active segment to "repair" it would drop acked records.
#[test]
fn a_torn_retired_segment_does_not_delete_a_synced_active_segment() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, active, retired_len) = tear_the_retired_segment(&dir);
    let active_len = std::fs::metadata(segment_path(dir.path(), active))
        .expect("meta")
        .len();
    assert!(
        active_len > SEGMENT_HEADER_LEN,
        "the active segment holds records"
    );
    record_mark(&dir, active, active_len);

    reopen(&dir).expect_err("synced records would be discarded");
    let len_of = |id| {
        std::fs::metadata(segment_path(dir.path(), id))
            .expect("still there")
            .len()
    };
    assert_eq!(len_of(active), active_len);
    assert_eq!(len_of(retired), retired_len);
}

/// Nor may the retired segment itself be cut below what the mark vouches for.
#[test]
fn a_torn_retired_segment_is_not_cut_below_the_mark() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, _, retired_len) = tear_the_retired_segment(&dir);
    record_mark(&dir, retired, retired_len);

    reopen(&dir).expect_err("synced records would be discarded");
    let path = segment_path(dir.path(), retired);
    assert_eq!(std::fs::metadata(path).expect("meta").len(), retired_len);
}
