use std::path::{Path, PathBuf};

use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode};

use super::*;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 200,
        index_spacing_bytes: 32,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key() -> ShardKey {
    parse_shard("acme/default/orders/0").expect("key")
}

async fn write_shard(data: &Path, count: usize) -> PathBuf {
    let dir = shard_dir(data, Store::Stream, &key());
    let log = DiskLog::open(dir.clone(), "orders", config()).expect("open");
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

fn report_of(dir: &Path) -> Value {
    let shard = ShardDir {
        store: Store::Stream,
        name: dir.file_name().unwrap().to_string_lossy().into_owned(),
        path: dir.to_path_buf(),
    };
    shard_json(&shard, &inspect_shard(dir, &config()).expect("inspect"))
}

fn exit_of(reports: &[Value]) -> Option<Exit> {
    verdict(reports)
        .err()
        .map(|err| crate::error::exit_for(&err))
}

#[test]
fn a_shard_is_tenant_namespace_name_and_number() {
    let key = parse_shard("acme/default/orders/3").expect("parse");
    assert_eq!(
        (
            key.tenant.as_str(),
            key.namespace.as_str(),
            key.stream.as_str(),
            key.shard
        ),
        ("acme", "default", "orders", 3)
    );
    for bad in [
        "orders",
        "acme/default/orders",
        "acme/default/orders/x",
        "//orders/0",
    ] {
        let err = parse_shard(bad).expect_err(bad);
        assert_eq!(crate::error::exit_for(&err), Exit::Usage, "{bad}");
    }
}

#[tokio::test]
async fn a_clean_shard_prints_one_line_and_exits_zero() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 12).await;
    let report = report_of(&dir);
    assert_eq!(report["startup"]["verdict"], "clean");
    assert_eq!(report["store"], "stream");

    let text = render(std::slice::from_ref(&report), false);
    assert!(text.contains("STARTUP"), "{text}");
    assert!(
        text.lines()
            .nth(1)
            .is_some_and(|line| line.ends_with("clean")),
        "{text}"
    );
    assert!(
        !text.contains("SEGMENT "),
        "no details for a clean shard: {text}"
    );
    assert!(render(std::slice::from_ref(&report), true).contains("RECORDS CHECK"));
    assert_eq!(exit_of(&[report]), None);
}

#[tokio::test]
async fn a_torn_tail_is_a_repair_with_status_6() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 5).await;
    let active = std::fs::read_dir(&dir)
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
        .expect("segment");
    let mut bytes = std::fs::read(&active).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&active, &bytes).expect("write");

    let report = report_of(&dir);
    assert_eq!(report["startup"]["verdict"], "repair");
    let text = render(std::slice::from_ref(&report), false);
    assert!(text.contains("cut the torn tail of segment"), "{text}");
    assert!(text.contains("append in flight"), "{text}");
    assert_eq!(exit_of(&[report]), Some(Exit::WouldRepair));
}

#[tokio::test]
async fn rot_in_a_sealed_segment_is_damage_with_status_7() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 30).await;
    let first = dir.join(format!("{:020}.log", 0));
    let mut bytes = std::fs::read(&first).expect("read");
    let header = felix_storage::segment::SEGMENT_HEADER_LEN as usize;
    bytes[header + felix_storage::segment::RECORD_HEADER_LEN as usize] ^= 0xFF;
    std::fs::write(&first, &bytes).expect("write");

    let report = report_of(&dir);
    let text = render(std::slice::from_ref(&report), false);
    assert!(text.contains("records fail their checksum"), "{text}");
    assert!(text.contains("damaged at byte"), "{text}");
    assert_eq!(exit_of(&[report]), Some(Exit::Damaged));
}
