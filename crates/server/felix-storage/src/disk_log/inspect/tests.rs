use std::collections::BTreeMap;

use tempfile::{TempDir, tempdir};

use super::super::layout;
use super::*;
use crate::DiskLog;
use crate::log::{AppendOnlyLog, AppendRecord, FsyncMode};
use crate::segment::SEGMENT_HEADER_LEN;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: SEGMENT_HEADER_LEN + 80,
        index_spacing_bytes: 32,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: stream.into(),
        shard: 0,
    }
}

/// A shard of `store` under `data` holding `count` records, closed cleanly.
async fn write_shard(data: &Path, store: Store, stream: &str, count: usize) -> PathBuf {
    let dir = shard_dir(data, store, &key(stream));
    let log = DiskLog::open(dir.clone(), stream.to_string(), config()).expect("open");
    for i in 0..count {
        log.append(&[AppendRecord {
            payload: format!("value-{i:03}").into(),
            timestamp_micros: 1,
            mark: Default::default(),
            publisher: None,
        }])
        .await
        .expect("append");
    }
    log.shutdown().await.expect("shutdown");
    dir
}

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

fn segment_ids(dir: &Path) -> Vec<SegmentId> {
    recovery::discover_segment_ids(dir).expect("ids")
}

#[tokio::test]
async fn shards_are_found_in_every_store() {
    let data = tempdir().expect("dir");
    write_shard(data.path(), Store::Stream, "orders", 1).await;
    write_shard(data.path(), Store::Cache, "sessions", 1).await;
    write_shard(data.path(), Store::Counters, "hits", 1).await;
    std::fs::create_dir(data.path().join("leftover.retired")).expect("mkdir");

    let found: Vec<(Store, String)> = find_shards(data.path())
        .expect("find")
        .into_iter()
        .map(|shard| (shard.store, shard.name))
        .collect();
    assert_eq!(
        found,
        [
            (Store::Stream, layout::shard_dir_name(&key("orders"))),
            (Store::Cache, layout::shard_dir_name(&key("sessions"))),
            (Store::Counters, layout::shard_dir_name(&key("hits"))),
        ]
    );
}

#[test]
fn a_missing_data_dir_is_an_error() {
    let parent = tempdir().expect("dir");
    assert!(find_shards(&parent.path().join("nope")).is_err());
}

#[tokio::test]
async fn a_clean_shard_reports_its_segments_and_starts_clean() {
    let data = tempdir().expect("dir");
    let dir = write_shard(data.path(), Store::Stream, "orders", 12).await;

    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(report.startup, Startup::Clean);
    assert!(report.actions.is_empty(), "{:?}", report.actions);
    assert!(report.segments.len() > 1);
    assert_eq!(
        report
            .segments
            .iter()
            .filter_map(|s| s.records)
            .sum::<u64>(),
        12
    );
    assert!(report.segments.iter().all(|s| s.damage.is_none()));
    assert!(
        report
            .segments
            .iter()
            .all(|s| matches!(s.index, IndexState::Matches | IndexState::Behind)),
        "{:?}",
        report.segments
    );
    assert!(report.durable_mark.is_some());
}

#[tokio::test]
async fn a_torn_tail_is_a_repair_and_nothing_is_written() {
    let data = tempdir().expect("dir");
    let dir = write_shard(data.path(), Store::Stream, "orders", 5).await;
    let active = *segment_ids(&dir).last().expect("id");
    let path = dir.join(segment_file_name(active));
    let mut bytes = std::fs::read(&path).expect("read");
    let good = bytes.len() as u64;
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&path, &bytes).expect("write");

    let before = tree(data.path());
    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(tree(data.path()), before, "inspect wrote to the data dir");

    assert_eq!(report.startup, Startup::Repair);
    assert!(matches!(
        report.actions.as_slice(),
        [Action::TruncateTail { segment, position, discarded_bytes: 13, .. }]
            if *segment == active && *position == good
    ));
    let damaged = report.segments.last().expect("segment");
    assert!(damaged.damage.as_ref().is_some_and(|damage| damage.tail));
}

#[tokio::test]
async fn interior_damage_is_a_refusal_naming_the_segment() {
    let data = tempdir().expect("dir");
    let dir = write_shard(data.path(), Store::Stream, "orders", 6).await;
    let active = *segment_ids(&dir).last().expect("id");
    let path = dir.join(segment_file_name(active));
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[SEGMENT_HEADER_LEN as usize + crate::segment::RECORD_HEADER_LEN as usize] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let before = tree(data.path());
    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(tree(data.path()), before, "inspect wrote to the data dir");

    let Startup::Refuse(detail) = &report.startup else {
        panic!("expected a refusal, got {:?}", report.startup);
    };
    assert_eq!(detail.site.segment, Some(active));
    let damage = report
        .segments
        .last()
        .and_then(|s| s.damage.clone())
        .expect("damage");
    assert_eq!(damage.position, SEGMENT_HEADER_LEN);
    assert!(!damage.tail);
}

/// Startup only checks a sealed segment past its last index entry, so rot
/// before that starts fine and fails on read. The full check still finds it.
#[tokio::test]
async fn rot_startup_does_not_look_at_is_still_reported() {
    let data = tempdir().expect("dir");
    let dir = write_shard(data.path(), Store::Stream, "orders", 12).await;
    let sealed = segment_ids(&dir)[0];
    let path = dir.join(segment_file_name(sealed));
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[SEGMENT_HEADER_LEN as usize + crate::segment::RECORD_HEADER_LEN as usize] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("write");

    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(report.startup, Startup::Clean);
    assert!(report.segments[0].damage.is_some());

    let strict = inspect_shard(
        &dir,
        &LogConfig {
            verify_all_on_open: true,
            ..config()
        },
    )
    .expect("inspect");
    assert!(matches!(strict.startup, Startup::Refuse(_)));
}

#[tokio::test]
async fn a_missing_index_is_reported_and_its_rebuild_is_not_a_repair() {
    let data = tempdir().expect("dir");
    let dir = write_shard(data.path(), Store::Stream, "orders", 12).await;
    let sealed = segment_ids(&dir)[0];
    std::fs::remove_file(dir.join(index_file_name(sealed))).expect("remove");

    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(report.segments[0].index, IndexState::Missing);
    assert_eq!(report.startup, Startup::Clean);
    assert_eq!(
        report.actions,
        [Action::RebuildIndex {
            segment: sealed,
            reason: "missing"
        }]
    );
    assert!(!dir.join(index_file_name(sealed)).exists());
}

#[test]
fn an_absent_shard_is_a_fresh_log_and_is_not_created() {
    let data: TempDir = tempdir().expect("dir");
    let dir = data.path().join("nothing-here");
    let report = inspect_shard(&dir, &config()).expect("inspect");
    assert_eq!(report.startup, Startup::Clean);
    assert_eq!(report.actions, [Action::CreateFirstSegment]);
    assert!(!dir.exists());
}
