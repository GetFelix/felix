use std::collections::BTreeMap;
use std::path::PathBuf;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::super::super::durable_mark::{self, DurableMark};
use super::*;
use crate::DiskLog;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig};
use crate::segment::format::encode_record;
use crate::segment::{BASELINE_VERSION, IndexEntry, RECORD_HEADER_LEN};

/// Bytes a record with a nine-byte payload takes.
const RECORD_BYTES: u64 = RECORD_HEADER_LEN + 9;

/// One big segment with a tight index, so a seek has entries to land on.
fn one_segment() -> LogConfig {
    LogConfig {
        segment_size_bytes: 1 << 20,
        index_spacing_bytes: 64,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// About fifty records to a segment, so the sealed ones carry index entries.
/// The active segment's index is written lazily and is often empty.
fn indexed_segments() -> LogConfig {
    LogConfig {
        segment_size_bytes: 2000,
        ..one_segment()
    }
}

/// Two records to a segment.
fn small_segments() -> LogConfig {
    LogConfig {
        segment_size_bytes: SEGMENT_HEADER_LEN + 80,
        ..one_segment()
    }
}

fn record(payload: Bytes) -> AppendRecord {
    AppendRecord {
        payload,
        timestamp_micros: 1_700_000_000_000_000,
        mark: Default::default(),
        publisher: None,
    }
}

/// A shard of `count` records (`value-000`, `value-001`, ...), closed cleanly.
async fn write_log(config: LogConfig, count: usize) -> (TempDir, PathBuf) {
    let data = tempdir().expect("dir");
    let dir = data.path().join("shard");
    let log = DiskLog::open(dir.clone(), "shard".to_string(), config).expect("open");
    for i in 0..count {
        log.append(&[record(format!("value-{i:03}").into())])
            .await
            .expect("append");
    }
    log.shutdown().await.expect("shutdown");
    (data, dir)
}

/// Startup's rules, as the broker's defaults.
fn look(dir: &Path, query: &RecordsQuery) -> Result<RecordsReport> {
    read_records(dir, query, &LogConfig::default())
}

fn query(offset: Offset, around: u64) -> RecordsQuery {
    RecordsQuery {
        offset,
        around,
        payload: false,
    }
}

fn offsets(report: &RecordsReport) -> Vec<Offset> {
    report.records.iter().map(|r| r.offset).collect()
}

fn segment_ids(dir: &Path) -> Vec<SegmentId> {
    recovery::discover_segment_ids(dir).expect("ids")
}

fn active_path(dir: &Path) -> PathBuf {
    dir.join(segment_file_name(*segment_ids(dir).last().expect("id")))
}

/// Every file under `dir` with its bytes and modification time.
fn tree(dir: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("list") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("meta");
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                out.insert(
                    entry.path(),
                    (
                        std::fs::read(entry.path()).expect("read"),
                        meta.modified().expect("mtime"),
                    ),
                );
            }
        }
    }
    out
}

#[tokio::test]
async fn a_window_in_the_middle_is_found_through_the_index() {
    let (_data, dir) = write_log(indexed_segments(), 100).await;

    let report = look(&dir, &query(30, 3)).expect("read");

    assert_eq!(offsets(&report), (27..=33).collect::<Vec<_>>());
    assert_eq!(report.offset, OffsetStatus::Found);
    assert!(report.stop.is_none());
    assert!(report.seek.via_index, "{:?}", report.seek);
    let first = &report.records[0];
    assert!(report.seek.position > SEGMENT_HEADER_LEN);
    assert!(report.seek.position <= first.position);
    // Record 27 sits where the arithmetic of equal-sized records says.
    assert_eq!(first.position, SEGMENT_HEADER_LEN + 27 * RECORD_BYTES);
    assert_eq!(first.payload_len, 9);
    assert_eq!(first.timestamp_micros, 1_700_000_000_000_000);
    assert_eq!(first.payload, None);
}

#[tokio::test]
async fn the_window_runs_across_a_segment_boundary() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    assert!(segment_ids(&dir).len() > 4);

    let report = look(&dir, &query(5, 2)).expect("read");

    assert_eq!(offsets(&report), [3, 4, 5, 6, 7]);
    let segments: Vec<SegmentId> = report.records.iter().map(|r| r.segment).collect();
    assert!(
        segments.windows(2).any(|pair| pair[0] != pair[1]),
        "the window should span segments: {segments:?}"
    );
    assert_eq!(report.offset, OffsetStatus::Found);
    assert!(report.stop.is_none());
}

#[tokio::test]
async fn payloads_come_only_when_asked_and_are_cut() {
    let data = tempdir().expect("dir");
    let dir = data.path().join("shard");
    let log = DiskLog::open(dir.clone(), "shard".to_string(), one_segment()).expect("open");
    let big = vec![b'x'; PAYLOAD_PREVIEW_BYTES + 100];
    log.append(&[record("small".into()), record(big.clone().into())])
        .await
        .expect("append");
    log.shutdown().await.expect("shutdown");

    let without = look(&dir, &query(0, 5)).expect("read");
    assert!(without.records.iter().all(|r| r.payload.is_none()));

    let with = look(
        &dir,
        &RecordsQuery {
            payload: true,
            ..query(0, 5)
        },
    )
    .expect("read");
    assert_eq!(with.records[0].payload.as_deref(), Some(&b"small"[..]));
    assert_eq!(with.records[1].payload_len, big.len() as u64);
    assert_eq!(
        with.records[1].payload.as_deref(),
        Some(&big[..PAYLOAD_PREVIEW_BYTES])
    );
}

#[tokio::test]
async fn a_publisher_and_mark_are_reported() {
    let data = tempdir().expect("dir");
    let dir = data.path().join("shard");
    let log = DiskLog::open(dir.clone(), "shard".to_string(), one_segment()).expect("open");
    log.append(&[AppendRecord {
        publisher: Some("alice".into()),
        ..record("hello".into())
    }])
    .await
    .expect("append");
    log.shutdown().await.expect("shutdown");

    let report = look(&dir, &query(0, 0)).expect("read");
    assert_eq!(report.records[0].publisher.as_deref(), Some(&b"alice"[..]));
    assert_eq!(report.records[0].payload_len, 5);
    assert_eq!(report.records[0].mark, RecordMark::None);
}

#[tokio::test]
async fn around_is_capped() {
    let (_data, dir) = write_log(one_segment(), 1100).await;

    let report = look(&dir, &query(1050, u64::MAX)).expect("read");

    // 1000 either side of 1050, clamped to the 1100 records there are.
    assert_eq!(report.records.first().map(|r| r.offset), Some(50));
    assert_eq!(report.records.len(), 1050);
    assert_eq!(report.offset, OffsetStatus::Found);
}

#[tokio::test]
async fn a_torn_tail_ends_the_read_and_keeps_what_came_before() {
    let (_data, dir) = write_log(one_segment(), 5).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    let good = bytes.len() as u64;
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let report = look(&dir, &query(3, 10)).expect("read");

    assert_eq!(offsets(&report), [0, 1, 2, 3, 4]);
    let stop = report.stop.expect("stop");
    assert!(stop.tail, "{stop:?}");
    assert_eq!(stop.position, good);
    assert_eq!(stop.expected_offset, 5);
    assert!(matches!(stop.cause, CorruptionKind::Truncated { .. }));
    assert_eq!(report.offset, OffsetStatus::Found);
    assert_eq!(report.log_end, None);
}

#[tokio::test]
async fn an_offset_in_the_torn_tail_is_unreached() {
    let (_data, dir) = write_log(one_segment(), 5).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let report = look(&dir, &query(5, 0)).expect("read");

    assert!(report.records.is_empty());
    assert_eq!(report.offset, OffsetStatus::Unreached);
    assert!(report.stop.is_some_and(|stop| stop.tail));
}

#[tokio::test]
async fn damage_past_the_window_is_not_reported() {
    let (_data, dir) = write_log(one_segment(), 5).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let report = look(&dir, &query(4, 0)).expect("read");

    assert_eq!(offsets(&report), [4]);
    assert!(report.stop.is_none());
}

#[tokio::test]
async fn a_rotted_record_is_named_at_its_offset_and_position() {
    let (_data, dir) = write_log(one_segment(), 6).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    let record_3 = SEGMENT_HEADER_LEN + 3 * RECORD_BYTES;
    bytes[(record_3 + RECORD_HEADER_LEN) as usize + 2] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let report = look(&dir, &query(4, 3)).expect("read");

    assert_eq!(offsets(&report), [1, 2]);
    let stop = report.stop.expect("stop");
    assert_eq!(stop.position, record_3);
    assert_eq!(stop.expected_offset, 3);
    assert!(!stop.tail, "rot inside the file is not a torn tail");
    assert!(matches!(stop.cause, CorruptionKind::RecordChecksum { .. }));
    assert_eq!(report.offset, OffsetStatus::Unreached);

    // A window that ends before the rot does not see it.
    let before = look(&dir, &query(1, 1)).expect("read");
    assert_eq!(offsets(&before), [0, 1, 2]);
    assert!(before.stop.is_none());
}

#[tokio::test]
async fn a_rotted_header_is_named_too() {
    let (_data, dir) = write_log(one_segment(), 6).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    let record_2 = SEGMENT_HEADER_LEN + 2 * RECORD_BYTES;
    // The offset field, covered by the header checksum.
    bytes[record_2 as usize + 7] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let report = look(&dir, &query(2, 1)).expect("read");

    assert_eq!(offsets(&report), [1]);
    let stop = report.stop.expect("stop");
    assert_eq!(stop.position, record_2);
    assert!(!stop.tail);
    assert!(matches!(
        stop.cause,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[tokio::test]
async fn a_missing_index_costs_a_longer_walk_not_a_wrong_answer() {
    let (_data, dir) = write_log(indexed_segments(), 100).await;
    let with_index = look(&dir, &query(30, 3)).expect("read");
    assert!(with_index.seek.via_index);

    let id = segment_ids(&dir)[0];
    std::fs::remove_file(dir.join(index_file_name(id))).expect("remove");
    let without = look(&dir, &query(30, 3)).expect("read");

    assert!(!without.seek.via_index);
    assert_eq!(without.seek.position, SEGMENT_HEADER_LEN);
    assert_eq!(without.records, with_index.records);
    assert!(
        !dir.join(index_file_name(id)).exists(),
        "the index must not be rebuilt on disk"
    );
}

#[tokio::test]
async fn a_stale_index_is_checked_and_not_trusted() {
    let (_data, dir) = write_log(indexed_segments(), 100).await;
    let expected = look(&dir, &query(30, 3)).expect("read");
    let id = segment_ids(&dir)[0];
    let index_path = dir.join(index_file_name(id));

    // An entry into the middle of a record.
    let mut inside = SparseIndex::new(0);
    inside.push(IndexEntry {
        offset: 20,
        position: SEGMENT_HEADER_LEN + 20 * RECORD_BYTES + 5,
    });
    inside.persist(&index_path).expect("persist");
    let report = look(&dir, &query(30, 3)).expect("read");
    assert!(!report.seek.via_index, "{:?}", report.seek);
    assert_eq!(report.records, expected.records);

    // An entry on a real record, claiming an offset it does not carry. The
    // record is a valid place to start from, so the walk continues from the
    // record's own offset.
    let mut lying = SparseIndex::new(0);
    lying.push(IndexEntry {
        offset: 25,
        position: SEGMENT_HEADER_LEN + 10 * RECORD_BYTES,
    });
    lying.persist(&index_path).expect("persist");
    let report = look(&dir, &query(30, 3)).expect("read");
    assert_eq!(report.records, expected.records);

    // An entry past the end of the file.
    let mut beyond = SparseIndex::new(0);
    beyond.push(IndexEntry {
        offset: 25,
        position: 1 << 30,
    });
    beyond.persist(&index_path).expect("persist");
    let report = look(&dir, &query(30, 3)).expect("read");
    assert!(!report.seek.via_index);
    assert_eq!(report.records, expected.records);
}

#[tokio::test]
async fn an_offset_below_the_oldest_segment_says_where_the_log_starts() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let first = segment_ids(&dir)[0];
    std::fs::remove_file(dir.join(segment_file_name(first))).expect("remove");
    std::fs::remove_file(dir.join(index_file_name(first))).expect("remove");

    let report = look(&dir, &query(0, 3)).expect("read");

    assert_eq!(report.log_start, Some(2));
    assert_eq!(report.offset, OffsetStatus::BelowStart { log_start: 2 });
    assert_eq!(offsets(&report), [2, 3]);
    assert!(report.stop.is_none());
}

#[tokio::test]
async fn an_offset_past_the_tail_says_where_the_log_ends() {
    let (_data, dir) = write_log(small_segments(), 12).await;

    for offset in [12, 50] {
        let report = look(&dir, &query(offset, 2)).expect("read");
        assert_eq!(report.offset, OffsetStatus::PastTail { log_end: 12 });
        assert_eq!(report.log_end, Some(12));
        assert!(report.stop.is_none());
    }
    // The last offset itself is still there.
    let last = look(&dir, &query(11, 0)).expect("read");
    assert_eq!(last.offset, OffsetStatus::Found);
    assert_eq!(offsets(&last), [11]);
}

#[tokio::test]
async fn a_missing_segment_between_two_others_is_damage_at_the_join() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let ids = segment_ids(&dir);
    std::fs::remove_file(dir.join(segment_file_name(ids[1]))).expect("remove");

    let report = look(&dir, &query(3, 4)).expect("read");

    assert_eq!(offsets(&report), [0, 1]);
    let stop = report.stop.expect("stop");
    assert_eq!(stop.segment, ids[2]);
    assert_eq!(stop.position, SEGMENT_HEADER_LEN);
    assert_eq!(stop.expected_offset, 2);
    assert!(!stop.tail);
    assert!(matches!(
        stop.cause,
        CorruptionKind::OffsetOutOfOrder {
            expected: 2,
            found: 4
        }
    ));
}

#[test]
fn a_directory_with_no_segments_is_empty() {
    let data = tempdir().expect("dir");
    let report = look(data.path(), &query(0, 1)).expect("read");
    assert_eq!(report.offset, OffsetStatus::Empty);
    assert!(report.records.is_empty());
    assert_eq!(report.log_start, None);
}

#[test]
fn an_absent_directory_is_an_error_and_is_not_created() {
    let data = tempdir().expect("dir");
    let dir = data.path().join("nothing-here");
    assert!(look(&dir, &query(0, 1)).is_err());
    assert!(!dir.exists());
}

#[tokio::test]
async fn reading_writes_nothing() {
    // Every kind of trouble at once: a torn tail on the active segment, and a
    // sealed segment without its index.
    let (data, dir) = write_log(small_segments(), 12).await;
    let ids = segment_ids(&dir);
    std::fs::remove_file(dir.join(index_file_name(ids[0]))).expect("remove");
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let before = tree(data.path());
    for offset in [0, 5, 11, 12, 100] {
        let query = RecordsQuery {
            payload: true,
            ..query(offset, 1000)
        };
        look(&dir, &query).expect("read");
    }
    assert_eq!(tree(data.path()), before, "reading changed the data dir");
}

/// A segment file after the newest one, as a rollover leaves it.
fn add_segment(dir: &Path, bytes: &[u8]) -> SegmentId {
    let id = segment_ids(dir).last().expect("id") + 1;
    std::fs::write(dir.join(segment_file_name(id)), bytes).expect("write");
    id
}

fn flip(path: &Path, at: u64) {
    let mut bytes = std::fs::read(path).expect("read");
    bytes[at as usize] ^= 0xFF;
    std::fs::write(path, &bytes).expect("write");
}

fn mark(dir: &Path, segment: SegmentId, synced_bytes: u64) {
    let mark = DurableMark {
        segment,
        synced_bytes,
    };
    std::fs::write(dir.join(durable_mark::mark_file_name()), mark.encode()).expect("write");
}

#[tokio::test]
async fn a_headerless_trailing_segment_is_a_leftover_not_data() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let leftover = add_segment(&dir, &[0u8; 10]);

    let report = look(&dir, &query(50, 2)).expect("read");

    assert!(report.stop.is_none(), "{:?}", report.stop);
    assert_eq!(report.offset, OffsetStatus::PastTail { log_end: 12 });
    assert_eq!(report.startup, Startup::Repair);
    assert!(matches!(
        report.actions.as_slice(),
        [Action::RemoveSegment { segment, .. }, ..] if *segment == leftover
    ));
}

#[tokio::test]
async fn a_stale_header_only_segment_does_not_move_the_log_end() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    // Prepared for offset 15 by a rollover that the log then rewound below.
    let header = SegmentHeader::at_version(15, 1, BASELINE_VERSION).encode();
    add_segment(&dir, &header);

    let report = look(&dir, &query(60, 2)).expect("read");

    assert_eq!(report.offset, OffsetStatus::PastTail { log_end: 12 });
    assert_eq!(report.log_end, Some(12));
    assert!(report.stop.is_none());
}

#[tokio::test]
async fn a_cut_short_record_in_an_older_segment_is_not_a_torn_tail() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let oldest = dir.join(segment_file_name(segment_ids(&dir)[0]));
    let len = std::fs::metadata(&oldest).expect("meta").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&oldest)
        .expect("open")
        .set_len(len - 5)
        .expect("cut");

    let report = look(&dir, &query(1, 5)).expect("read");

    assert_eq!(offsets(&report), [0]);
    let stop = report.stop.expect("stop");
    assert_eq!(stop.expected_offset, 1);
    assert!(matches!(stop.cause, CorruptionKind::Truncated { .. }));
    assert!(!stop.tail, "startup refuses this, it does not cut it");
    assert!(matches!(report.startup, Startup::Refuse(_)));
}

#[tokio::test]
async fn the_durable_mark_decides_whether_a_bad_last_record_is_a_torn_tail() {
    let (_data, dir) = write_log(one_segment(), 5).await;
    let path = active_path(&dir);
    let id = *segment_ids(&dir).last().expect("id");
    let len = std::fs::metadata(&path).expect("meta").len();
    let record_4 = SEGMENT_HEADER_LEN + 4 * RECORD_BYTES;
    flip(&path, record_4 + RECORD_HEADER_LEN + 2);

    // Synced through the end of the file: the record may have been
    // acknowledged, so this is rot.
    mark(&dir, id, len);
    let synced = look(&dir, &query(4, 2)).expect("read");
    let stop = synced.stop.expect("stop");
    assert_eq!(stop.position, record_4);
    assert!(!stop.tail);
    assert!(matches!(synced.startup, Startup::Refuse(_)));

    // Synced only up to the record: it was never reported durable.
    mark(&dir, id, record_4);
    let unsynced = look(&dir, &query(4, 2)).expect("read");
    assert!(unsynced.stop.is_some_and(|stop| stop.tail));
    assert_eq!(unsynced.startup, Startup::Repair);
}

#[tokio::test]
async fn repair_checksum_tail_is_passed_to_startups_plan() {
    let (_data, dir) = write_log(one_segment(), 5).await;
    let path = active_path(&dir);
    let id = *segment_ids(&dir).last().expect("id");
    let len = std::fs::metadata(&path).expect("meta").len();
    mark(&dir, id, len);
    flip(
        &path,
        SEGMENT_HEADER_LEN + 4 * RECORD_BYTES + RECORD_HEADER_LEN + 2,
    );

    let strict = look(&dir, &query(4, 0)).expect("read");
    assert!(strict.stop.is_some_and(|stop| !stop.tail));

    let lenient = LogConfig {
        repair_checksum_tail: true,
        ..LogConfig::default()
    };
    let report = read_records(&dir, &query(4, 0), &lenient).expect("read");
    assert!(report.stop.is_some_and(|stop| stop.tail));
}

#[tokio::test]
async fn a_retired_segment_cut_short_ends_the_log_where_startup_cuts_it() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let ids = segment_ids(&dir);
    // Without the mark nothing says the lost record was synced.
    std::fs::remove_file(dir.join(durable_mark::mark_file_name())).expect("remove");
    let retired = dir.join(segment_file_name(ids[ids.len() - 2]));
    let len = std::fs::metadata(&retired).expect("meta").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&retired)
        .expect("open")
        .set_len(len - RECORD_BYTES)
        .expect("cut");

    let report = look(&dir, &query(10, 4)).expect("read");

    assert!(report.stop.is_none(), "{:?}", report.stop);
    assert_eq!(report.startup, Startup::Repair);
    assert_eq!(report.offset, OffsetStatus::PastTail { log_end: 9 });
    assert_eq!(offsets(&report), [6, 7, 8]);
}

#[tokio::test]
async fn a_damaged_oldest_header_is_named_not_taken_for_the_start() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let oldest = segment_ids(&dir)[0];
    flip(&dir.join(segment_file_name(oldest)), 1);

    // Far enough in that the window alone would not touch the oldest segment.
    let report = look(&dir, &query(9, 0)).expect("read");

    assert_eq!(report.log_start, None);
    assert_eq!(report.offset, OffsetStatus::Unreached);
    let stop = report.stop.expect("stop");
    assert_eq!((stop.segment, stop.position), (oldest, 0));
    assert!(!stop.tail);
}

#[tokio::test]
async fn the_first_damage_is_found_without_knowing_its_offset() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    assert_eq!(
        first_damage(&dir, &LogConfig::default()).expect("scan"),
        None
    );

    let ids = segment_ids(&dir);
    // The first record of the fourth segment, offset 6.
    flip(
        &dir.join(segment_file_name(ids[3])),
        SEGMENT_HEADER_LEN + RECORD_HEADER_LEN + 2,
    );
    let stop = first_damage(&dir, &LogConfig::default())
        .expect("scan")
        .expect("damage");
    assert_eq!((stop.segment, stop.expected_offset), (ids[3], 6));
}

#[tokio::test]
async fn payloads_shrink_the_window_cap() {
    let (_data, dir) = write_log(one_segment(), 400).await;
    let asked = RecordsQuery {
        payload: true,
        ..query(200, u64::MAX)
    };

    let report = look(&dir, &asked).expect("read");

    assert_eq!(report.records.len() as u64, 2 * MAX_AROUND_WITH_PAYLOAD + 1);
    assert_eq!(report.records[0].offset, 200 - MAX_AROUND_WITH_PAYLOAD);
}

#[tokio::test]
async fn an_empty_segment_in_the_middle_with_the_wrong_base_is_a_gap() {
    let (_data, dir) = write_log(small_segments(), 10).await;
    // An empty segment based at 15, then a real one at 15: the log skips 10..15.
    let header = SegmentHeader::at_version(15, 1, BASELINE_VERSION).encode();
    let empty = add_segment(&dir, &header);
    let mut next = header.to_vec();
    encode_record(&mut next, 15, 1, b"after-gap", None, &RecordMark::None);
    add_segment(&dir, &next);

    let stop = first_damage(&dir, &LogConfig::default())
        .expect("scan")
        .expect("a gap is damage");
    assert_eq!((stop.segment, stop.expected_offset), (empty, 10));
    assert!(!stop.tail);
    assert!(matches!(
        stop.cause,
        CorruptionKind::OffsetOutOfOrder {
            expected: 10,
            found: 15
        }
    ));
    let report = look(&dir, &query(9, 20)).expect("read");
    assert!(report.stop.is_some());
    assert!(matches!(report.startup, Startup::Refuse(_)));
}

#[tokio::test]
async fn a_torn_retired_segment_is_cut_and_the_read_goes_on_into_the_next() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let ids = segment_ids(&dir);
    std::fs::remove_file(dir.join(durable_mark::mark_file_name())).expect("remove");
    // A partial record after the retired segment's last whole one, which
    // loses nothing: the segment after it still starts where it ends.
    let retired = dir.join(segment_file_name(ids[ids.len() - 2]));
    let mut bytes = std::fs::read(&retired).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&retired, &bytes).expect("write");

    let report = look(&dir, &query(11, 3)).expect("read");

    assert!(report.stop.is_none(), "{:?}", report.stop);
    assert_eq!(offsets(&report), [8, 9, 10, 11]);
    assert_eq!(report.offset, OffsetStatus::Found);
    assert_eq!(report.startup, Startup::Repair);
    assert!(report.actions.iter().any(|action| matches!(
        action,
        Action::CutRetired {
            drops_next: None,
            ..
        }
    )));
}

#[tokio::test]
async fn damage_reports_a_damaged_header() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let ids = segment_ids(&dir);
    flip(&dir.join(segment_file_name(ids[2])), 1);

    let stop = first_damage(&dir, &LogConfig::default())
        .expect("scan")
        .expect("damage");

    assert_eq!((stop.segment, stop.position), (ids[2], 0));
    assert!(!stop.tail);
}

#[tokio::test]
async fn damage_reports_a_torn_tail_as_the_first_damage() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    let path = active_path(&dir);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let stop = first_damage(&dir, &LogConfig::default())
        .expect("scan")
        .expect("damage");

    assert_eq!(stop.expected_offset, 12);
    assert!(stop.tail);
}

#[tokio::test]
async fn damage_does_not_count_a_rollover_leftover() {
    let (_data, dir) = write_log(small_segments(), 12).await;
    add_segment(&dir, &[0u8; 10]);
    assert_eq!(
        first_damage(&dir, &LogConfig::default()).expect("scan"),
        None
    );
}

#[test]
fn a_vanished_file_is_read_again_once_and_only_once() {
    let dir = tempdir().expect("dir");
    let gone = || -> Result<u8> {
        Err(StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::NotFound,
        )))
    };

    let attempts = std::cell::Cell::new(0);
    let read = retrying(dir.path(), || {
        attempts.set(attempts.get() + 1);
        if attempts.get() == 1 { gone() } else { Ok(7) }
    });
    assert_eq!(read.expect("second attempt"), 7);
    assert_eq!(attempts.get(), 2);

    let attempts = std::cell::Cell::new(0);
    let read = retrying(dir.path(), || {
        attempts.set(attempts.get() + 1);
        gone()
    });
    assert!(read.is_err());
    assert_eq!(attempts.get(), 2, "one retry, not a loop");

    // Without the directory nothing can reappear, so there is no second read.
    let attempts = std::cell::Cell::new(0);
    let missing = dir.path().join("nothing-here");
    let read = retrying(&missing, || {
        attempts.set(attempts.get() + 1);
        gone()
    });
    assert!(read.is_err());
    assert_eq!(attempts.get(), 1);
}
