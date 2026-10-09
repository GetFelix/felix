//! The plan says exactly what recovery then does, and planning writes nothing.

use std::collections::BTreeMap;
use std::path::PathBuf;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::disk_log::now_micros;
use crate::disk_log::recovery::recover_shard;
use crate::log::{AppendRecord, FsyncMode};
use crate::segment::SegmentWriter;

const LABEL: &str = "t/ns/s/0";

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: SEGMENT_HEADER_LEN + 80,
        index_spacing_bytes: 32,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn segment_path(dir: &TempDir, id: SegmentId) -> PathBuf {
    dir.path().join(segment_file_name(id))
}

/// Write `count` records through a real `SegmentSet`, rolling as configured.
fn populate(dir: &TempDir, count: usize) -> Offset {
    let recovered =
        recover_shard(dir.path(), LABEL, &config(), &Manifest::default()).expect("recover");
    let mut set = crate::disk_log::segments::SegmentSet::new(
        dir.path().to_path_buf(),
        LABEL.into(),
        config(),
        recovered.sealed,
        recovered.active,
        crate::disk_log::sealed::SealedFiles::new(16),
    )
    .expect("set");
    for i in 0..count {
        set.append(&[AppendRecord {
            payload: Bytes::from(format!("value-{i:03}")),
            timestamp_micros: 1,
            mark: Default::default(),
            publisher: None,
        }])
        .expect("append");
    }
    set.active_mut().sync().expect("sync");
    set.tail_offset()
}

fn ids(dir: &TempDir) -> Vec<SegmentId> {
    discover_segment_ids(dir.path()).expect("ids")
}

fn len_of(dir: &TempDir, id: SegmentId) -> u64 {
    std::fs::metadata(segment_path(dir, id))
        .expect("meta")
        .len()
}

/// Every file in the directory with its bytes and modification time.
fn snapshot(dir: &Path) -> BTreeMap<String, (Vec<u8>, std::time::SystemTime)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return BTreeMap::new();
    };
    entries
        .map(|entry| {
            let entry = entry.expect("entry");
            let modified = entry.metadata().expect("meta").modified().expect("mtime");
            (
                entry.file_name().to_string_lossy().into_owned(),
                (std::fs::read(entry.path()).expect("read"), modified),
            )
        })
        .collect()
}

/// Plan, check nothing changed, recover, and check recovery did what the plan
/// said. Returns the plan for the fixture's own assertions.
fn plan_then_recover(dir: &TempDir, config: &LogConfig) -> RecoveryPlan {
    let before = snapshot(dir.path());
    let plan = plan_recovery(dir.path(), LABEL, config, &Manifest::default()).expect("plan");
    assert_eq!(
        snapshot(dir.path()),
        before,
        "planning wrote to the directory"
    );

    let recovered = recover_shard(dir.path(), LABEL, config, &Manifest::default());
    let after = ids(dir);
    for step in &plan.steps {
        match step {
            Step::Remove { id, .. } => assert!(!after.contains(id), "segment {id} was kept"),
            Step::RebuildIndex { id, index, .. } => {
                let written =
                    SparseIndex::load(&dir.path().join(index_file_name(*id)), index.base_offset())
                        .expect("index written");
                assert_eq!(written.entries(), index.entries(), "index {id}");
            }
            Step::CutRetired { drop_next, .. } => {
                if let Some(next) = drop_next {
                    assert!(!after.contains(next), "segment {next} was kept");
                }
            }
            Step::SyncDir => {}
        }
    }
    match (&plan.end, &recovered) {
        (End::Refuse(planned), Err(StorageError::Corruption(found))) => {
            assert_eq!(planned, found);
        }
        (End::Fresh, Ok(recovered)) => {
            assert_eq!(recovered.active.id(), 0);
            assert_eq!(recovered.active.next_offset(), 0);
            assert!(recovered.sealed.is_empty());
        }
        (End::Resume(resume), Ok(recovered)) => {
            assert_eq!(recovered.active.id(), resume.active_id);
            assert_eq!(recovered.active.next_offset(), resume.active.next_offset);
            assert_eq!(
                recovered.truncated_bytes,
                resume
                    .active
                    .torn_tail
                    .as_ref()
                    .map_or(0, |tail| tail.discarded_bytes)
            );
            assert_eq!(len_of(dir, resume.active_id), resume.active.valid_bytes);
            let sealed: Vec<_> = recovered
                .sealed
                .iter()
                .map(|e| e.descriptor.clone())
                .collect();
            let planned: Vec<_> = resume.sealed.iter().map(|e| e.descriptor.clone()).collect();
            assert_eq!(sealed, planned);
            for entry in &resume.sealed {
                assert_eq!(
                    len_of(dir, entry.descriptor.id),
                    entry.descriptor.size_bytes
                );
            }
        }
        (end, recovered) => panic!("planned {end:?}, recovery gave {recovered:?}"),
    }
    plan
}

fn removals(plan: &RecoveryPlan) -> Vec<(SegmentId, Removal)> {
    plan.steps
        .iter()
        .filter_map(|step| match step {
            Step::Remove { id, why } => Some((*id, *why)),
            _ => None,
        })
        .collect()
}

fn rebuilds(plan: &RecoveryPlan) -> Vec<(SegmentId, IndexRebuild)> {
    plan.steps
        .iter()
        .filter_map(|step| match step {
            Step::RebuildIndex { id, why, .. } => Some((*id, *why)),
            _ => None,
        })
        .collect()
}

/// Zero the retired segment's last record, as a power loss during its seal
/// can. Returns the retired and active ids and the retired segment's length.
fn tear_the_retired_segment(dir: &TempDir) -> (SegmentId, SegmentId, u64) {
    let all = ids(dir);
    let (retired, active) = (all[all.len() - 2], all[all.len() - 1]);
    let path = segment_path(dir, retired);
    let mut bytes = std::fs::read(&path).expect("read");
    let len = bytes.len();
    let record = crate::segment::format::record_len(9, &Default::default()) as usize;
    bytes[len - record..].fill(0);
    std::fs::write(&path, &bytes).expect("write");
    (retired, active, (len - record) as u64)
}

#[test]
fn a_missing_directory_plans_a_fresh_log_without_creating_it() {
    let parent = tempdir().expect("dir");
    let dir = parent.path().join("shard");
    let plan = plan_recovery(&dir, LABEL, &config(), &Manifest::default()).expect("plan");
    assert!(matches!(plan.end, End::Fresh));
    assert!(plan.steps.is_empty());
    assert!(!dir.exists());
}

#[test]
fn a_clean_log_plans_nothing() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 12);
    let plan = plan_then_recover(&dir, &config());
    assert!(plan.steps.is_empty(), "{:?}", plan.steps);
    let End::Resume(resume) = plan.end else {
        panic!("expected a resume");
    };
    assert_eq!(resume.active.next_offset, tail);
    assert!(resume.active.torn_tail.is_none());
}

#[test]
fn a_torn_tail_is_planned_as_a_cut_of_the_active_segment() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 5);
    let path = segment_path(&dir, *ids(&dir).last().expect("id"));
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let plan = plan_then_recover(&dir, &config());
    let End::Resume(resume) = plan.end else {
        panic!("expected a resume");
    };
    assert_eq!(resume.active.next_offset, tail);
    assert_eq!(resume.active.torn_tail.expect("torn").discarded_bytes, 13);
}

#[test]
fn a_zero_filled_tail_is_planned_as_a_cut() {
    let dir = tempdir().expect("dir");
    populate(&dir, 5);
    let path = segment_path(&dir, *ids(&dir).last().expect("id"));
    let mut bytes = std::fs::read(&path).expect("read");
    bytes.extend_from_slice(&[0; 4096]);
    std::fs::write(&path, &bytes).expect("write");

    let plan = plan_then_recover(&dir, &config());
    let End::Resume(resume) = plan.end else {
        panic!("expected a resume");
    };
    assert_eq!(resume.active.torn_tail.expect("torn").discarded_bytes, 4096);
}

#[test]
fn interior_corruption_is_planned_as_a_refusal_naming_the_place() {
    let dir = tempdir().expect("dir");
    populate(&dir, 6);
    let active = *ids(&dir).last().expect("id");
    let path = segment_path(&dir, active);
    let mut bytes = std::fs::read(&path).expect("read");
    let first_record = SEGMENT_HEADER_LEN as usize + crate::segment::RECORD_HEADER_LEN as usize;
    bytes[first_record] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let plan = plan_then_recover(&dir, &config());
    let End::Refuse(detail) = plan.end else {
        panic!("expected a refusal");
    };
    assert_eq!(detail.site.segment, Some(active));
    assert_eq!(detail.site.position, Some(SEGMENT_HEADER_LEN));
}

#[test]
fn a_cut_sealed_segment_is_refused_after_its_index_is_rebuilt() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let sealed = ids(&dir)[0];
    let path = segment_path(&dir, sealed);
    let bytes = std::fs::read(&path).expect("read");
    std::fs::write(&path, &bytes[..bytes.len() - 3]).expect("write");

    // Startup rewrites the index from the full scan before it finds the
    // damage, and the plan says so.
    let plan = plan_then_recover(&dir, &config());
    assert_eq!(rebuilds(&plan), [(sealed, IndexRebuild::Mismatch)]);
    assert!(matches!(plan.end, End::Refuse(ref detail) if detail.site.segment == Some(sealed)));
}

#[test]
fn a_missing_index_is_planned_as_a_rebuild() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let sealed = ids(&dir)[0];
    std::fs::remove_file(dir.path().join(index_file_name(sealed))).expect("remove");

    let plan = plan_then_recover(&dir, &config());
    assert_eq!(rebuilds(&plan), [(sealed, IndexRebuild::Missing)]);
    assert!(matches!(plan.end, End::Resume(_)));
}

#[test]
fn a_stale_index_is_planned_as_a_rebuild() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let sealed = ids(&dir)[0];
    let index_path = dir.path().join(index_file_name(sealed));
    let mut stale = SparseIndex::load(&index_path, 0).expect("index");
    let last = *stale.entries().last().expect("entry");
    stale.push(IndexEntry {
        offset: last.offset + 1,
        position: last.position + 1,
    });
    stale.persist(&index_path).expect("persist");

    let plan = plan_then_recover(&dir, &config());
    assert_eq!(rebuilds(&plan), [(sealed, IndexRebuild::Mismatch)]);
}

#[test]
fn verify_all_on_open_plans_a_rebuild_of_every_sealed_index() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let sealed = ids(&dir).len() - 1;
    let plan = plan_then_recover(
        &dir,
        &LogConfig {
            verify_all_on_open: true,
            ..config()
        },
    );
    let found = rebuilds(&plan);
    assert_eq!(found.len(), sealed);
    assert!(found.iter().all(|(_, why)| *why == IndexRebuild::VerifyAll));
}

#[test]
fn an_uninstalled_rollover_is_planned_as_a_removal() {
    let dir = tempdir().expect("dir");
    let tail = populate(&dir, 5);
    let id = ids(&dir).last().expect("id") + 1;
    SegmentWriter::create(dir.path(), id, 1, now_micros(), 0, 32).expect("create");

    let plan = plan_then_recover(&dir, &config());
    assert_eq!(
        removals(&plan),
        [(
            id,
            Removal::EmptyPreparation {
                base_offset: 1,
                log_tail: tail
            }
        )]
    );
    assert_eq!(plan.abandoned, 1);
}

/// The plan follows recovery through its own repair: it cuts the retired
/// segment, drops the active one that no longer follows on, and resumes from
/// the cut segment.
#[test]
fn a_torn_retired_segment_is_planned_through_its_repair() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (retired, active, kept) = tear_the_retired_segment(&dir);

    let plan = plan_then_recover(&dir, &config());
    assert!(plan.steps.iter().any(|step| matches!(
        step,
        Step::CutRetired { id, valid_bytes, drop_next: Some(next) }
            if *id == retired && *valid_bytes == kept && *next == active
    )));
    let End::Resume(resume) = plan.end else {
        panic!("expected a resume");
    };
    assert_eq!(resume.active_id, retired);
    assert_eq!(resume.active.valid_bytes, kept);
}

#[test]
fn a_torn_retired_segment_under_a_synced_active_one_is_refused_untouched() {
    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    let (_, active, _) = tear_the_retired_segment(&dir);
    crate::disk_log::durable_mark::MarkFile::open(dir.path())
        .expect("mark")
        .record(DurableMark {
            segment: active,
            synced_bytes: len_of(&dir, active),
        })
        .expect("record");

    let plan = plan_then_recover(&dir, &config());
    assert!(
        !plan
            .steps
            .iter()
            .any(|step| matches!(step, Step::CutRetired { .. }))
    );
    assert!(matches!(plan.end, End::Refuse(_)));
}

/// Without write permission anywhere in the directory, planning still works.
#[cfg(unix)]
#[test]
fn planning_needs_no_write_permission() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().expect("dir");
    populate(&dir, 12);
    std::fs::remove_file(dir.path().join(index_file_name(ids(&dir)[0]))).expect("remove");
    let set_mode = |mode: u32| {
        for entry in std::fs::read_dir(dir.path()).expect("list") {
            let path = entry.expect("entry").path();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode & 0o666))
                .expect("chmod");
        }
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(mode)).expect("chmod");
    };
    set_mode(0o555);
    let plan = plan_recovery(dir.path(), LABEL, &config(), &Manifest::default());
    set_mode(0o755);
    assert_eq!(rebuilds(&plan.expect("plan")).len(), 1);
}
