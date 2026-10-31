//! The head floor trim records set: where reads start once a trim is durable
//! (and committed, on a held log), which sealed segments below it are
//! deleted, and how it is rebuilt at open, from the `head` file or from the
//! trim records when the file cannot be used.

use std::path::Path;

use super::*;
use crate::log::RecordMark;

fn trim(floor: Offset, headers: &'static str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(headers.as_bytes()),
        timestamp_micros: 1,
        mark: RecordMark::Trim { floor },
        publisher: None,
    }
}

fn payload(offset: Offset) -> String {
    format!("record-{offset:02}")
}

/// Retention off and its timer out of the way: the tests run deletion with
/// `enforce_retention_now`.
fn quiet(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        retention_check_interval: Duration::from_secs(3600),
        ..config(fsync_mode)
    }
}

/// Append one record per offset in `offsets`, each carrying `payload`.
async fn fill(log: &DiskLog, offsets: std::ops::Range<Offset>) {
    for offset in offsets {
        let appended = log
            .append(&records(&[&payload(offset)]))
            .await
            .expect("append");
        assert_eq!(appended.first_offset, offset);
    }
}

async fn page(log: &DiskLog, start: Offset) -> crate::Result<ReadPage> {
    log.read_range(ReadRange {
        start,
        max_bytes: usize::MAX,
    })
    .await
}

fn trimmed(result: crate::Result<ReadPage>) -> (Offset, Offset, Option<Bytes>) {
    match result {
        Err(StorageError::Trimmed {
            requested,
            oldest,
            trim_headers,
        }) => (requested, oldest, trim_headers),
        other => panic!("expected Trimmed, got {other:?}"),
    }
}

/// Records from `start`, by payload, skipping trims.
async fn payloads_from(log: &DiskLog, start: Offset) -> Vec<String> {
    read_all(log, start).await
}

fn expected(offsets: std::ops::Range<Offset>) -> Vec<String> {
    offsets.map(payload).collect()
}

fn head_path(dir: &Path) -> std::path::PathBuf {
    dir.join(head::head_file_name())
}

/// A trim's floor applies once its record is durable, which a commit makes
/// it under every fsync policy. Below it a read is refused with the trim's
/// headers; from it, the records are there.
#[tokio::test]
async fn a_trim_applies_once_it_is_durable() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("open");
    fill(&log, 0..6).await;
    let pending = log
        .append_pending(&[trim(3, "first")])
        .await
        .expect("append");
    assert_eq!(pending.first_offset(), 6);
    assert_eq!(log.readable_base(), 0, "not durable yet");
    assert_eq!(log.head_trim(), None);

    log.commit(&pending).await.expect("commit");
    assert_eq!(log.readable_base(), 3);
    assert_eq!(log.base_offset(), 0, "nothing deleted by the trim itself");
    assert_eq!(
        log.head_trim(),
        Some(HeadTrim {
            offset: 6,
            floor: 3,
            headers: Bytes::from_static(b"first"),
        })
    );
    assert_eq!(
        trimmed(page(&log, 2).await),
        (2, 3, Some(Bytes::from_static(b"first")))
    );
    assert_eq!(payloads_from(&log, 3).await, expected(3..6));
    // Replication still reads from the physical base.
    let shipped = log
        .read_range_with_trims(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read with trims");
    assert_eq!(shipped.len(), 7);
}

/// On a log held at its commit offset a trim applies only once the commit
/// offset passes its record.
#[tokio::test]
async fn a_held_log_applies_a_trim_below_its_commit_offset() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::OnCommit)).expect("open");
    log.hold_retention_at_commit(true).await.expect("hold");
    fill(&log, 0..4).await;
    log.append(&[trim(2, "held")]).await.expect("trim");
    assert_eq!(log.readable_base(), 0, "durable but not committed");
    log.advance_commit_offset(4).await.expect("advance");
    assert_eq!(log.readable_base(), 0, "the trim is at 4");
    log.advance_commit_offset(5).await.expect("advance");
    assert_eq!(log.readable_base(), 2);
    assert_eq!(trimmed(page(&log, 1).await).1, 2);
}

/// The largest floor wins, a later trim asking for less changes nothing,
/// and the headers are those of the first trim to set the floor.
#[tokio::test]
async fn the_first_trim_to_set_the_floor_tags_it() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    fill(&log, 0..4).await;
    log.append(&[trim(3, "a")]).await.expect("trim");
    log.append(&[trim(2, "b")]).await.expect("trim");
    log.append(&[trim(3, "c")]).await.expect("trim");
    assert_eq!(log.readable_base(), 3);
    assert_eq!(
        log.head_trim().map(|trim| (trim.offset, trim.headers)),
        Some((4, Bytes::from_static(b"a")))
    );
}

/// A read past the last record but below the tail, where only trims are
/// left, returns no records and names the tail to go on from, so a caller
/// paging to the tail never spins.
#[tokio::test]
async fn a_page_of_only_trims_names_where_to_go_on() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    fill(&log, 0..2).await;
    log.append(&[trim(1, "a"), trim(2, "b")])
        .await
        .expect("trims");
    let tail = log.tail_offset().await.expect("tail");
    assert_eq!(tail, 4);

    let empty = page(&log, 2).await.expect("read");
    assert!(empty.records.is_empty());
    assert_eq!(empty.next, tail);
    assert_eq!(page(&log, tail).await.expect("read").next, tail);

    fill(&log, 4..6).await;
    let one = log
        .read_range(ReadRange {
            start: 2,
            max_bytes: 1,
        })
        .await
        .expect("read");
    assert_eq!(one.records.len(), 1);
    assert_eq!(one.records[0].offset, 4);
    assert_eq!(one.next, 5);

    // Paging from the floor to the tail ends.
    let tail = log.tail_offset().await.expect("tail");
    let mut cursor = log.readable_base();
    let mut seen = Vec::new();
    for _ in 0..10 {
        if cursor >= tail {
            break;
        }
        let page = page(&log, cursor).await.expect("read");
        assert!(page.next > cursor, "stuck at {cursor}");
        seen.extend(page.records.iter().map(|record| record.offset));
        cursor = page.next;
    }
    assert_eq!(cursor, tail);
    assert_eq!(seen, vec![4, 5]);
}

/// Deletion removes the sealed segments wholly below the floor and nothing
/// else: every record at or above the floor stays, and so does the trim.
#[tokio::test]
async fn deletion_stops_below_the_floor() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("open");
    fill(&log, 0..24).await;
    log.append(&[trim(12, "t")]).await.expect("trim");
    fill(&log, 25..28).await;
    log.sync().await.expect("sync");

    let outcome = log.enforce_retention_now().await.expect("delete");
    assert!(outcome.segments_deleted > 0, "{outcome:?}");
    let base = log.base_offset();
    assert!(base > 0 && base <= 12, "base {base}");
    assert!(
        log.segments()[0].last_offset >= 12,
        "a segment wholly below the floor was kept: {:?}",
        log.segments()[0]
    );
    assert_eq!(log.readable_base(), 12);
    let mut kept = expected(12..24);
    kept.extend(expected(25..28));
    assert_eq!(payloads_from(&log, 12).await, kept);

    // Nothing more to do, and the floor survives a restart.
    assert_eq!(
        log.enforce_retention_now()
            .await
            .expect("delete")
            .segments_deleted,
        0
    );
    log.shutdown().await.expect("shutdown");
    drop(log);
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("reopen");
    assert_eq!(log.readable_base(), 12);
    assert_eq!(log.head_trim().map(|trim| trim.offset), Some(24));
}

/// Storage does not check that a floor is at or below its record. When it
/// is not, deletion stops at the record, so the floor is still there to read
/// back after a restart.
#[tokio::test]
async fn deletion_never_reaches_the_record_that_set_the_floor() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("open");
    fill(&log, 0..10).await;
    log.append(&[trim(20, "past")]).await.expect("trim");
    fill(&log, 11..24).await;
    log.sync().await.expect("sync");
    assert_eq!(log.readable_base(), 20);

    log.enforce_retention_now().await.expect("delete");
    assert!(log.base_offset() <= 10);
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("reopen");
    assert_eq!(log.head_trim().map(|trim| trim.offset), Some(10));
    assert_eq!(log.readable_base(), 20);
}

/// On a held log deletion stops at the commit offset on disk, not the one in
/// memory, so a restart never reads back a commit offset below the base.
#[tokio::test]
async fn a_held_log_deletes_only_below_the_saved_commit_offset() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::None)).expect("open");
    log.hold_retention_at_commit(true).await.expect("hold");
    fill(&log, 0..24).await;
    log.append(&[trim(12, "t")]).await.expect("trim");
    log.advance_commit_offset(6).await.expect("advance");
    log.persist_replica_state().await.expect("persist");
    // Within the persist interval, so only memory moves.
    log.advance_commit_offset(25).await.expect("advance");
    assert_eq!(
        log.inner.commit_persisted.load(Ordering::Acquire),
        6,
        "the second advance must stay in memory for this test"
    );
    assert_eq!(log.readable_base(), 12, "the floor follows memory");

    log.enforce_retention_now().await.expect("delete");
    let base = log.base_offset();
    assert!(base > 0 && base <= 6, "base {base} passed the saved commit");
}

/// A truncation below a trim takes its floor away, and the `head` file with
/// it, so a restart cannot bring the floor back.
#[tokio::test]
async fn a_truncation_below_a_trim_drops_its_floor() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    fill(&log, 0..6).await;
    log.append(&[trim(3, "t")]).await.expect("trim");
    fill(&log, 7..16).await;
    assert_eq!(log.readable_base(), 3);
    assert!(head_path(dir.path()).exists(), "saved at a roll");

    log.truncate(6).await.expect("truncate");
    assert_eq!(log.readable_base(), 0);
    assert_eq!(log.head_trim(), None);
    assert!(!head_path(dir.path()).exists());
    assert_eq!(payloads_from(&log, 0).await, expected(0..6));
    log.shutdown().await.expect("shutdown");
    drop(log);

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.readable_base(), 0);
}

/// A reset discards every trim along with the records.
#[tokio::test]
async fn a_reset_forgets_every_trim() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    fill(&log, 0..4).await;
    log.append(&[trim(3, "t")]).await.expect("trim");
    log.reset_to(100).await.expect("reset");
    assert_eq!(log.readable_base(), 100);
    assert_eq!(log.head_trim(), None);
}

/// A log of 20 records with a trim to 5 at offset 10, in small segments,
/// shut down cleanly.
async fn trimmed_log(dir: &TempDir) {
    let log = open(dir, FsyncMode::OnCommit);
    fill(&log, 0..10).await;
    log.append(&[trim(5, "t")]).await.expect("trim");
    fill(&log, 11..20).await;
    log.shutdown().await.expect("shutdown");
}

async fn assert_floor(dir: &TempDir, when: &str) {
    let log = open(dir, FsyncMode::OnCommit);
    assert_eq!(log.readable_base(), 5, "{when}");
    assert_eq!(
        log.head_trim().map(|trim| (trim.offset, trim.headers)),
        Some((10, Bytes::from_static(b"t"))),
        "{when}"
    );
    let mut kept = expected(5..10);
    kept.extend(expected(11..20));
    assert_eq!(payloads_from(&log, 5).await, kept, "{when}");
    log.shutdown().await.expect("shutdown");
}

/// The floor is derived from the trim records, so it comes back whatever
/// happened to the `head` file: kept, lost, damaged, or naming a trim that
/// is not there.
#[tokio::test]
async fn the_floor_is_rebuilt_whatever_the_head_file_holds() {
    let dir = tempdir().expect("dir");
    trimmed_log(&dir).await;
    let path = head_path(dir.path());
    assert!(path.exists());
    assert_floor(&dir, "with the file").await;

    std::fs::remove_file(&path).expect("remove");
    assert_floor(&dir, "without the file").await;
    assert!(path.exists(), "saved again once the segments were read");

    std::fs::write(&path, b"not a head file").expect("write");
    assert_floor(&dir, "with a damaged file").await;

    // Checksummed and in range, but naming a record that is not a trim.
    std::fs::write(
        &path,
        head::encode(
            11,
            &[head::HeadEntry {
                offset: 3,
                floor: 9,
                headers: Bytes::from_static(b"forged"),
            }],
        ),
    )
    .expect("write");
    assert_floor(&dir, "with a file naming a trim that is not there").await;

    // Naming the real trim, but from beyond where the file stops, so the
    // read after it would find the trim again.
    std::fs::write(
        &path,
        head::encode(
            10,
            &[head::HeadEntry {
                offset: 10,
                floor: 5,
                headers: Bytes::from_static(b"t"),
            }],
        ),
    )
    .expect("write");
    assert_floor(&dir, "with an entry past the file's offset").await;

    // From past the tail, as one left by a truncation a crash interrupted.
    std::fs::write(&path, head::encode(1_000, &[])).expect("write");
    assert_floor(&dir, "with a file from past the tail").await;
}

/// A roll saves the file only after the segment it covers is sealed, so a
/// crash during one leaves the file from an earlier roll. The open reads
/// on from where that file stops.
#[tokio::test]
async fn a_file_from_an_earlier_roll_is_read_on_from() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);
    fill(&log, 0..4).await;
    log.append(&[trim(2, "early")]).await.expect("trim");
    fill(&log, 5..10).await;
    let path = head_path(dir.path());
    let early = std::fs::read(&path).expect("saved at a roll");
    log.append(&[trim(5, "t")]).await.expect("trim");
    fill(&log, 11..20).await;
    log.shutdown().await.expect("shutdown");
    drop(log);

    std::fs::write(&path, early).expect("put the earlier file back");
    // The floor from the later trim, which the earlier file does not hold.
    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.readable_base(), 5);
    assert_eq!(log.head_trim().map(|trim| trim.offset), Some(10));
}

/// Copy a shard directory's files.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        if entry.file_type().expect("type").is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
        }
    }
}

/// Deletion unlinks oldest first, so a crash part way through leaves the
/// log missing some prefix of the segments it chose. Every such log opens
/// with the same floor and every record above it.
#[tokio::test]
async fn a_crash_part_way_through_deletion_keeps_the_floor() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", quiet(FsyncMode::OnCommit)).expect("open");
    fill(&log, 0..24).await;
    log.append(&[trim(12, "t")]).await.expect("trim");
    fill(&log, 25..28).await;
    log.shutdown().await.expect("shutdown");
    let before = log.segments();
    drop(log);

    let probe = tempdir().expect("probe");
    copy_dir(dir.path(), probe.path());
    let deleting = DiskLog::open(probe.path(), "t/ns/s/0", quiet(FsyncMode::OnCommit))
        .expect("open the probe");
    deleting.enforce_retention_now().await.expect("delete");
    let chosen = before.len() - deleting.segments().len();
    assert!(chosen >= 2, "only {chosen} segments deleted");
    drop(deleting);

    for unlinked in 0..=chosen {
        let image = tempdir().expect("image");
        copy_dir(dir.path(), image.path());
        for segment in &before[..unlinked] {
            segments::remove_segment_files(image.path(), segment.id).expect("unlink");
        }
        let log = DiskLog::open(image.path(), "t/ns/s/0", quiet(FsyncMode::OnCommit))
            .unwrap_or_else(|err| panic!("after {unlinked} unlinks: {err}"));
        assert_eq!(log.readable_base(), 12, "after {unlinked} unlinks");
        let mut kept = expected(12..24);
        kept.extend(expected(25..28));
        assert_eq!(
            payloads_from(&log, 12).await,
            kept,
            "after {unlinked} unlinks"
        );
    }
}

/// A trim written but not yet synced can be torn by a crash. Cut anywhere
/// inside it, the log opens with no floor, or, once the record is whole,
/// with its floor; every record before it stays.
#[tokio::test]
async fn a_crash_before_a_trim_is_synced_leaves_no_floor_or_its_floor() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        segment_size_bytes: 64 * 1024,
        ..quiet(FsyncMode::None)
    };
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config.clone()).expect("open");
    // A trim asking for nothing puts the log on v7 first, so the one under
    // test lands in the same segment as the records before it.
    log.append(&[trim(0, "v7")]).await.expect("trim");
    fill(&log, 1..6).await;
    log.sync().await.expect("sync");
    let active = log.segments().last().cloned().expect("active");
    let synced = active.size_bytes;
    // Written, not committed, so not synced.
    log.append_pending(&[trim(3, "t")]).await.expect("trim");
    let written = log.segments().last().expect("active").size_bytes;
    assert!(written > synced);
    let image_of = |len: u64| {
        let image = tempdir().expect("image");
        copy_dir(dir.path(), image.path());
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(
                image
                    .path()
                    .join(crate::segment::segment_file_name(active.id)),
            )
            .expect("segment");
        file.set_len(len).expect("cut");
        image
    };
    for len in synced..=written {
        let image = image_of(len);
        let log = DiskLog::open(image.path(), "t/ns/s/0", config.clone())
            .unwrap_or_else(|err| panic!("cut at {len}: {err}"));
        let floor = if len == written { 3 } else { 0 };
        assert_eq!(log.readable_base(), floor, "cut at {len}");
        assert_eq!(
            payloads_from(&log, floor).await,
            expected(floor.max(1)..6),
            "cut at {len}"
        );
    }
    drop(log);
}
