//! Offloading sealed segments to an object store. The property every test
//! comes back to: each record is either in the local log or in a copy the
//! manifest records and that verifies, never neither.

use std::path::{Path, PathBuf};

use super::*;
use crate::disk_log::offload::manifest::{self, Manifest};
use crate::disk_log::offload::test_hooks::Stop;
use crate::log::{OffloadTarget, SegmentId};
use crate::segment::{ReadBudget, SegmentReader};

const LABEL: &str = "t/ns/s/0";
const RECORDS: usize = 24;

/// A log under `root/log` copying to `root/store`.
fn offload_config(root: &Path, retention_bytes: Option<u64>) -> LogConfig {
    LogConfig {
        retention_bytes,
        // The tests drive passes themselves through `enforce_retention_now`.
        retention_check_interval: Duration::from_secs(3600),
        offload: Some(OffloadTarget::LocalDir(root.join("store"))),
        ..config(FsyncMode::None)
    }
}

/// Small enough that retention wants every sealed segment gone.
fn tight_retention() -> Option<u64> {
    Some(crate::segment::SEGMENT_HEADER_LEN + 120)
}

fn payloads() -> Vec<String> {
    (0..RECORDS).map(|i| format!("record-{i:02}")).collect()
}

async fn filled_log(root: &Path, config: &LogConfig) -> DiskLog {
    let log = DiskLog::open(root.join("log"), LABEL, config.clone()).expect("open");
    for payload in payloads() {
        log.append(&records(&[&payload])).await.expect("append");
    }
    log.sync().await.expect("sync");
    log
}

fn offloader(log: &DiskLog) -> &crate::disk_log::offload::Offloader {
    log.inner.offloader.as_ref().expect("offload is on")
}

/// The records a recorded copy holds, after checking it against its entry.
fn read_copy(store: &Path, entry: &manifest::ManifestEntry) -> Vec<String> {
    let path = store.join(&entry.key);
    let bytes = std::fs::read(&path)
        .unwrap_or_else(|err| panic!("recorded copy {} is missing: {err}", entry.key));
    assert_eq!(
        bytes.len() as u64,
        entry.size_bytes,
        "size of {}",
        entry.key
    );
    assert_eq!(
        crate::segment::format::crc32(&[&bytes]),
        entry.checksum,
        "checksum of {}",
        entry.key
    );
    let reader =
        SegmentReader::open(&path, entry.segment_id, entry.base_offset).expect("open copy");
    let mut out = Vec::new();
    reader
        .read_from_position(
            crate::segment::SEGMENT_HEADER_LEN,
            entry.base_offset,
            entry.size_bytes,
            &mut ReadBudget::new(usize::MAX, usize::MAX),
            LABEL,
            &mut out,
        )
        .expect("read copy");
    out.into_iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}

/// Open the log under `root` and check that every record written is in it
/// or in a recorded copy. Returns the log, open.
async fn assert_nothing_lost(root: &Path, config: &LogConfig, context: &str) -> DiskLog {
    let log = DiskLog::open(root.join("log"), LABEL, config.clone())
        .unwrap_or_else(|err| panic!("{context}: recovery failed: {err}"));
    let base = log.base_offset();
    let expected = payloads();
    assert_eq!(
        read_all(&log, base).await,
        expected[base as usize..],
        "{context}: local records"
    );
    let manifest = manifest::load(&root.join("log")).expect("manifest");
    assert!(
        manifest.covers(0, base),
        "{context}: offsets below the local base {base} have no recorded copy: {manifest:?}"
    );
    for entry in manifest.entries() {
        let held = read_copy(&root.join("store"), entry);
        assert_eq!(
            held,
            expected[entry.base_offset as usize..=entry.last_offset as usize],
            "{context}: records in {}",
            entry.key
        );
    }
    log
}

/// Copy a stopped log's whole tree, as a process crash would leave it.
fn crash_image(root: &Path) -> TempDir {
    let image = tempdir().expect("image");
    copy_tree(root, image.path());
    image
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target: PathBuf = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy");
        }
    }
}

#[tokio::test]
async fn offload_is_off_by_default() {
    assert_eq!(LogConfig::default().offload, None);
    let dir = tempdir().expect("dir");
    let log = filled_log(dir.path(), &config(FsyncMode::None)).await;
    log.enforce_retention_now().await.expect("pass");
    assert!(log.inner.offloader.is_none());
    assert!(!dir.path().join("log").join(manifest::FILE_NAME).exists());
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_pass_copies_every_sealed_segment_and_keeps_them_without_a_bound() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    let log = filled_log(dir.path(), &config).await;
    let sealed = log.inner.segments.read().descriptors().len() - 1;
    assert!(sealed >= 4, "too few segments to say much: {sealed}");

    log.enforce_retention_now().await.expect("pass");

    let recorded = log.inner.manifest.lock().clone();
    assert_eq!(recorded.entries().len(), sealed);
    assert_eq!(log.base_offset(), 0, "no bound, so nothing local goes");
    // A second pass finds nothing new to copy.
    log.enforce_retention_now().await.expect("pass");
    assert_eq!(*log.inner.manifest.lock(), recorded);
    log.shutdown().await.expect("shutdown");
    assert_nothing_lost(dir.path(), &config, "after a pass")
        .await
        .shutdown()
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn retention_with_offload_deletes_only_recorded_segments() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), tight_retention());
    let log = filled_log(dir.path(), &config).await;

    let outcome = log.enforce_retention_now().await.expect("pass");
    assert!(outcome.segments_deleted >= 4, "{outcome:?}");
    let base = log.base_offset();
    assert!(base > 0);
    assert!(log.inner.manifest.lock().covers(0, base));
    // Below the local head a read is still `Trimmed`: cold reads are not
    // wired up.
    let err = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect_err("trimmed");
    assert!(matches!(err, StorageError::Trimmed { .. }), "{err}");
    log.shutdown().await.expect("shutdown");
    assert_nothing_lost(dir.path(), &config, "after retention")
        .await
        .shutdown()
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn a_copy_that_fails_verification_is_not_recorded_and_its_segment_stays() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), tight_retention());
    let log = filled_log(dir.path(), &config).await;
    offloader(&log).faults.corrupt_next_upload();

    log.enforce_retention_now()
        .await
        .expect_err("the damaged copy fails verification");
    assert_eq!(
        log.base_offset(),
        0,
        "retention deleted a segment with no verified copy"
    );
    assert!(log.inner.manifest.lock().entries().is_empty());

    // The next pass copies it again, and only then does it go.
    log.enforce_retention_now().await.expect("pass");
    assert!(log.base_offset() > 0);
    log.shutdown().await.expect("shutdown");
    assert_nothing_lost(dir.path(), &config, "after a retried copy")
        .await
        .shutdown()
        .await
        .expect("shutdown");
}

/// Stop a pass at `stop`, as a crash there would, and check what a restart
/// finds; then that a pass on the restarted log finishes the job.
async fn crash_at(stop: Stop) {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), tight_retention());
    let log = filled_log(dir.path(), &config).await;
    offloader(&log).faults.stop_at(stop);
    log.enforce_retention_now()
        .await
        .expect_err("the armed stop fires");
    let image = crash_image(dir.path());
    log.shutdown().await.expect("shutdown");

    let config = offload_config(image.path(), tight_retention());
    let recovered = assert_nothing_lost(image.path(), &config, &format!("{stop:?}")).await;
    let manifest = recovered.inner.manifest.lock().clone();
    match stop {
        Stop::Uploaded => {
            assert!(manifest.entries().is_empty(), "{manifest:?}");
            assert_eq!(recovered.base_offset(), 0);
        }
        Stop::Recorded => {
            assert_eq!(manifest.entries().len(), 1, "{manifest:?}");
            assert_eq!(recovered.base_offset(), 0);
        }
        Stop::Unlinked => {
            assert!(recovered.base_offset() > 0);
        }
    }

    recovered.enforce_retention_now().await.expect("pass");
    assert!(recovered.base_offset() > 0);
    recovered.shutdown().await.expect("shutdown");
    assert_nothing_lost(image.path(), &config, &format!("{stop:?}, then a pass"))
        .await
        .shutdown()
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn a_crash_after_the_upload_and_before_the_manifest_keeps_the_segment() {
    crash_at(Stop::Uploaded).await;
}

#[tokio::test]
async fn a_crash_after_the_manifest_and_before_the_unlink_keeps_both() {
    crash_at(Stop::Recorded).await;
}

#[tokio::test]
async fn a_crash_after_the_unlink_leaves_a_recorded_copy() {
    crash_at(Stop::Unlinked).await;
}

/// Offload every sealed segment, then delete the second's files by hand,
/// leaving the first: an unlink that landed out of order.
async fn log_with_a_hole(root: &Path, config: &LogConfig) -> (SegmentId, Offset) {
    let log = filled_log(root, config).await;
    log.enforce_retention_now().await.expect("pass");
    let descriptors = log.inner.segments.read().descriptors();
    log.shutdown().await.expect("shutdown");
    drop(log);
    let hole = &descriptors[1];
    remove_segment_files_for_test(&root.join("log"), hole.id);
    (descriptors[0].id, hole.last_offset + 1)
}

fn remove_segment_files_for_test(dir: &Path, id: SegmentId) {
    super::segments::remove_segment_files(dir, id).expect("remove");
}

#[tokio::test]
async fn recovery_accepts_a_head_gap_the_manifest_covers() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    let (first, resumes_at) = log_with_a_hole(dir.path(), &config).await;

    let recovered = assert_nothing_lost(dir.path(), &config, "a covered gap").await;
    assert_eq!(recovered.base_offset(), resumes_at);
    assert!(
        !dir.path()
            .join("log")
            .join(crate::segment::segment_file_name(first))
            .exists(),
        "the segment below the gap is dropped; its copy is the record"
    );
    recovered.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn recovery_still_refuses_a_gap_the_manifest_does_not_cover() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    log_with_a_hole(dir.path(), &config).await;
    let log_dir = dir.path().join("log");
    let mut manifest = manifest::load(&log_dir).expect("manifest");
    // Forget the copy of the missing segment and of everything after it.
    let second = manifest.entries()[1].base_offset;
    manifest.forget_from(second);
    manifest::store(&log_dir, &manifest).expect("store");

    let err = DiskLog::open(&log_dir, LABEL, config).expect_err("an uncovered gap");
    assert!(
        matches!(err, StorageError::Corruption(_)),
        "expected the gap error, got {err}"
    );
}

#[tokio::test]
async fn truncation_forgets_the_copies_it_cuts_into() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    let log = filled_log(dir.path(), &config).await;
    log.enforce_retention_now().await.expect("pass");
    let cut = log.inner.manifest.lock().entries()[2].base_offset + 1;

    log.truncate(cut).await.expect("truncate");

    let in_memory = log.inner.manifest.lock().clone();
    assert!(in_memory.entries().iter().all(|e| e.last_offset < cut));
    assert_eq!(in_memory.entries().len(), 2);
    assert_eq!(
        manifest::load(&dir.path().join("log")).expect("load"),
        in_memory
    );
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_reset_forgets_every_copy() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    let log = filled_log(dir.path(), &config).await;
    log.enforce_retention_now().await.expect("pass");
    assert!(!log.inner.manifest.lock().entries().is_empty());

    log.reset_to(100).await.expect("reset");

    assert!(log.inner.manifest.lock().entries().is_empty());
    assert!(
        manifest::load(&dir.path().join("log"))
            .expect("load")
            .entries()
            .is_empty()
    );
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_manifest_that_does_not_decode_fails_the_open() {
    let dir = tempdir().expect("dir");
    let config = offload_config(dir.path(), None);
    let log = filled_log(dir.path(), &config).await;
    log.enforce_retention_now().await.expect("pass");
    log.shutdown().await.expect("shutdown");
    drop(log);
    let path = dir.path().join("log").join(manifest::FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[0] ^= 0xff;
    std::fs::write(&path, bytes).expect("damage");

    DiskLog::open(dir.path().join("log"), LABEL, config).expect_err("damaged manifest");
}

/// An archive under `blocker`, which starts as a file, so the archive
/// directory cannot be created until the test removes it.
fn blocked_archive_config(root: &Path) -> (PathBuf, LogConfig) {
    let blocker = root.join("blocker");
    std::fs::write(&blocker, b"not a directory").expect("blocker");
    let config = LogConfig {
        offload: Some(OffloadTarget::LocalDir(blocker.join("store"))),
        ..offload_config(root, tight_retention())
    };
    (blocker, config)
}

#[tokio::test]
async fn an_unreachable_archive_does_not_fail_the_open_or_publishes() {
    let dir = tempdir().expect("dir");
    let (_blocker, config) = blocked_archive_config(dir.path());
    let log = filled_log(dir.path(), &config).await;
    assert_eq!(read_all(&log, 0).await, payloads());
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retention_keeps_and_reports_segments_while_the_archive_is_down_then_resumes() {
    let dir = tempdir().expect("dir");
    let (blocker, config) = blocked_archive_config(dir.path());
    let log = filled_log(dir.path(), &config).await;

    for pass in 1..=2 {
        log.enforce_retention_now()
            .await
            .expect_err("the archive cannot be opened");
        assert_eq!(log.base_offset(), 0, "deleted a segment with no copy");
        let health = offloader(&log).health.lock();
        assert_eq!(health.consecutive_failures, pass);
        assert!(health.held_bytes > 0, "held bytes not reported");
    }
    // Publishes keep working through the outage.
    log.append(&records(&["during-outage"]))
        .await
        .expect("append");

    std::fs::remove_file(&blocker).expect("the archive comes back");
    log.enforce_retention_now().await.expect("pass");
    assert!(log.base_offset() > 0, "deletion did not resume");
    assert!(log.inner.manifest.lock().covers(0, log.base_offset()));
    {
        let health = offloader(&log).health.lock();
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.held_bytes, 0);
    }
    log.shutdown().await.expect("shutdown");
}

#[cfg(unix)]
#[tokio::test]
async fn a_read_only_archive_does_not_fail_the_open_and_nothing_is_deleted() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().expect("dir");
    let archive = dir.path().join("store");
    std::fs::create_dir(&archive).expect("archive");
    std::fs::set_permissions(&archive, std::fs::Permissions::from_mode(0o555)).expect("chmod");
    if std::fs::write(archive.join("probe"), b"").is_ok() {
        // Root ignores the mode bits; there is nothing to test.
        return;
    }
    let config = offload_config(dir.path(), tight_retention());
    let log = filled_log(dir.path(), &config).await;
    log.enforce_retention_now()
        .await
        .expect_err("nothing can be written to the archive");
    assert_eq!(log.base_offset(), 0);
    assert_eq!(offloader(&log).health.lock().consecutive_failures, 1);

    std::fs::set_permissions(&archive, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    log.enforce_retention_now().await.expect("pass");
    assert!(log.base_offset() > 0);
    log.shutdown().await.expect("shutdown");
}

#[test]
fn offload_needs_a_check_interval() {
    let config = LogConfig {
        retention_check_interval: Duration::ZERO,
        offload: Some(OffloadTarget::LocalDir(PathBuf::from("store"))),
        ..LogConfig::default()
    };
    assert!(config.validate().is_err());
    assert!(Manifest::default().entries().is_empty());
}

/// The same three stops under a power loss rather than a process crash:
/// unsynced pages and directory changes may be lost, in the log and in the
/// store alike. Every image must still hold each record somewhere.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_power_loss_at_any_offload_step_loses_nothing() {
    use crate::io::power_loss::{PowerLoss, Writeback};

    let dir = tempdir().expect("dir");
    let root = dir.path().join("tree");
    std::fs::create_dir_all(root.join("log")).expect("log dir");
    std::fs::create_dir_all(root.join("store")).expect("store dir");
    let observer = PowerLoss::install(&root).expect("install the power-loss observer");
    let config = offload_config(&root, tight_retention());
    let log = filled_log(&root, &config).await;

    for stop in [Stop::Uploaded, Stop::Recorded, Stop::Unlinked] {
        offloader(&log).faults.stop_at(stop);
        log.enforce_retention_now()
            .await
            .expect_err("the armed stop fires");
        for seed in 0x0ff1_0000..0x0ff1_0010u64 {
            for writeback in [Writeback::AnySubset, Writeback::InOrder] {
                let image = tempdir().expect("image dir");
                observer
                    .crash(seed, writeback, image.path())
                    .expect("build the crash image");
                let image_config = offload_config(image.path(), tight_retention());
                assert_nothing_lost(
                    image.path(),
                    &image_config,
                    &format!("{stop:?}, seed {seed:#x} ({writeback:?})"),
                )
                .await
                .shutdown()
                .await
                .expect("shutdown");
            }
        }
    }
    log.shutdown().await.expect("shutdown");
}
