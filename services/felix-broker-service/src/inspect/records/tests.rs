use std::collections::BTreeMap;
use std::path::Path;

use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ShardKey};
use felix_storage::segment::{RECORD_HEADER_LEN, SEGMENT_HEADER_LEN};

use super::*;
use crate::inspect::parse_shard;

/// 2023-11-14T22:13:20Z.
const WHEN: u64 = 1_700_000_000_000_000;

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
    let dir = felix_storage::inspect::shard_dir(data, Store::Stream, &key());
    let log = DiskLog::open(dir.clone(), "orders", config()).expect("open");
    for i in 0..count {
        log.append(&[AppendRecord {
            payload: format!("value-{i:03}").into(),
            timestamp_micros: WHEN + i as u64,
            mark: Default::default(),
            publisher: None,
            headers: None,
        }])
        .await
        .expect("append");
    }
    log.shutdown().await.expect("shutdown");
    dir
}

fn args(data: &Path, offset: u64, around: u64) -> Args {
    Args {
        data_dir: data.to_path_buf(),
        shard: "acme/default/orders/0".to_string(),
        offset: Some(offset),
        damage: false,
        around,
        payload: false,
        kind: None,
        incarnation: None,
        json: false,
        repair_checksum_tail: false,
        index_spacing: None,
        verify_all_on_open: false,
    }
}

fn report(data: &Path, offset: u64, around: u64) -> RecordsReport {
    read(&args(data, offset, around)).expect("read").0
}

fn parsed(list: &[&str]) -> Result<Parsed, String> {
    parse(
        list.iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<_>>()
            .into_iter(),
    )
}

fn active_segment(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
        .expect("segment")
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

#[test]
fn arguments_parse_like_the_usage_says() {
    let Ok(Parsed::Records(got)) = parsed(&[
        "/data",
        "acme/default/orders/0",
        "--offset",
        "42",
        "--around",
        "3",
        "--payload",
        "--kind",
        "cache",
        "--incarnation",
        "7",
        "--json",
    ]) else {
        panic!("expected records");
    };
    assert_eq!(
        got,
        Args {
            data_dir: "/data".into(),
            shard: "acme/default/orders/0".into(),
            offset: Some(42),
            damage: false,
            around: 3,
            payload: true,
            kind: Some(Store::Cache),
            incarnation: Some(7),
            json: true,
            repair_checksum_tail: false,
            index_spacing: None,
            verify_all_on_open: false,
        }
    );

    let Ok(Parsed::Records(defaults)) = parsed(&["/data", "a/b/c/0", "--offset", "0"]) else {
        panic!("expected records");
    };
    assert_eq!(defaults.around, DEFAULT_AROUND);
    assert!(!defaults.payload && !defaults.json);

    let Ok(Parsed::Records(found)) = parsed(&[
        "/data",
        "a/b/c/0",
        "--damage",
        "--repair-checksum-tail",
        "--verify-all-on-open",
        "--index-spacing",
        "512",
    ]) else {
        panic!("expected records");
    };
    assert!(found.damage && found.offset.is_none());
    assert!(found.repair_checksum_tail && found.verify_all_on_open);
    assert_eq!(found.index_spacing, Some(512));

    assert!(matches!(parsed(&["--help"]), Ok(Parsed::Help)));
    for bad in [
        &["/data", "a/b/c/0"][..],
        &["/data", "--offset", "1"],
        &["--offset", "1"],
        &["/data", "a/b/c/0", "--offset"],
        &["/data", "a/b/c/0", "--offset", "x"],
        &["/data", "a/b/c/0", "--offset", "-1"],
        &["/data", "a/b/c/0", "--offset", "1", "--around", "many"],
        &["/data", "a/b/c/0", "--offset", "1", "--kind", "topics"],
        &["/data", "a/b/c/0", "--offset", "1", "--damage"],
        &["/data", "a/b/c/0", "--index-spacing", "wide", "--damage"],
        &["/data", "a/b/c/0", "--offset", "1", "extra"],
        &["/data", "a/b/c/0", "--offset", "1", "--frobnicate"],
    ] {
        assert!(parsed(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn times_are_utc_with_microseconds() {
    assert_eq!(time(WHEN), "2023-11-14T22:13:20.000000Z");
    assert_eq!(time(WHEN + 42), "2023-11-14T22:13:20.000042Z");
    assert_eq!(time(951_782_400_000_000), "2000-02-29T00:00:00.000000Z");
    assert_eq!(time(1_709_251_199_999_999), "2024-02-29T23:59:59.999999Z");
    assert_eq!(time(0), "-");
}

#[tokio::test]
async fn a_clean_window_prints_a_table_and_exits_zero() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 12).await;

    let report = report(data.path(), 6, 2);
    let text = render(&report, 6, false);

    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 6, "{text}");
    for column in [
        "OFFSET",
        "TIME",
        "KIND",
        "PAYLOAD BYTES",
        "SEGMENT",
        "BYTE",
        "CHECKSUM",
    ] {
        assert!(lines[0].contains(column), "{text}");
    }
    let offsets: Vec<&str> = lines[1..]
        .iter()
        .map(|line| {
            line.split_whitespace()
                .find(|cell| cell.parse::<u64>().is_ok())
                .unwrap()
        })
        .collect();
    assert_eq!(offsets, ["4", "5", "6", "7", "8"]);
    // Only the offset asked for is starred, and every row verified.
    assert_eq!(lines.iter().filter(|line| line.starts_with('*')).count(), 1);
    assert!(
        lines[3].starts_with('*') && lines[3].contains("6"),
        "{text}"
    );
    assert!(lines[3].contains("2023-11-14T22:13:20.000006Z"), "{text}");
    assert!(lines[1..].iter().all(|line| line.ends_with("ok")), "{text}");
    assert!(
        lines[1..].iter().all(|line| line.contains("plain")),
        "{text}"
    );
    assert!(
        !text.contains("value-"),
        "payloads only with --payload: {text}"
    );
    assert_eq!(outcome(&report, 6), (0, vec![]));
}

#[tokio::test]
async fn payloads_are_dumped_as_hex_and_text_with_the_flag() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 4).await;
    let mut asked = args(data.path(), 1, 0);
    asked.payload = true;
    let report = read(&asked).expect("read").0;

    let text = render(&report, 1, true);
    assert!(text.contains("offset 1 payload, 9 bytes:"), "{text}");
    assert!(
        text.contains("00000000  76 61 6c 75 65 2d 30 30 31"),
        "{text}"
    );
    assert!(text.contains("|value-001|"), "{text}");

    let line = &json_lines(&report)[0];
    let value: Value = serde_json::from_str(line).expect("json");
    assert_eq!(value["payload_hex"], "76616c75652d303031");
    assert_eq!(value["payload_utf8"], "value-001");
    assert_eq!(value["payload_truncated"], false);
}

#[tokio::test]
async fn json_is_one_line_per_record() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 8).await;
    let report = report(data.path(), 3, 1);

    let lines = json_lines(&report);
    assert_eq!(lines.len(), 3);
    let values: Vec<Value> = lines
        .iter()
        .map(|line| serde_json::from_str(line).expect("json"))
        .collect();
    assert_eq!(
        values
            .iter()
            .map(|v| v["offset"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [2, 3, 4]
    );
    assert_eq!(values[1]["time"], "2023-11-14T22:13:20.000003Z");
    assert_eq!(values[1]["payload_bytes"], 9);
    assert_eq!(values[1]["checksum_ok"], true);
    assert_eq!(values[1]["kind"], "plain");
    assert!(values[1]["headers_bytes"].is_null());
    assert!(values[1].get("payload_hex").is_none());
}

#[tokio::test]
async fn offsets_outside_the_log_exit_five_and_say_where_it_is() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 12).await;

    let past = report(data.path(), 40, 2);
    let (code, notes) = outcome(&past, 40);
    assert_eq!(code, EXIT_NOT_FOUND);
    assert!(
        notes[0].contains("offset 40 is past the end of the log, which holds offsets 0 to 11"),
        "{notes:?}"
    );
    assert!(notes[0].contains("next record gets offset 12"), "{notes:?}");
    assert_eq!(render(&past, 40, false), "no records in range");

    let first = std::fs::read_dir(&dir)
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|ext| ext == "log" || ext == "index")
        })
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("00000000000000000000")
        })
        .collect::<Vec<_>>();
    for path in first {
        std::fs::remove_file(path).expect("remove");
    }
    let below = report(data.path(), 0, 0);
    let (code, notes) = outcome(&below, 0);
    assert_eq!(code, EXIT_NOT_FOUND);
    assert!(
        notes[0].contains("before the start of the log"),
        "{notes:?}"
    );
    assert!(notes[0].contains("starts at offset 4"), "{notes:?}");
}

#[tokio::test]
async fn a_torn_tail_exits_six_and_keeps_the_rows_before_it() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 6).await;
    let active = active_segment(&dir);
    let mut bytes = std::fs::read(&active).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&active, &bytes).expect("write");

    let report = report(data.path(), 4, 5);
    let text = render(&report, 4, false);

    let last = text.lines().last().expect("row");
    assert!(last.contains("TORN: truncated"), "{text}");
    assert!(
        last.split_whitespace().any(|cell| cell == "6"),
        "the offset the record should have: {text}"
    );
    assert_eq!(text.lines().filter(|line| line.ends_with("ok")).count(), 6);
    let (code, notes) = outcome(&report, 4);
    assert_eq!(code, EXIT_WOULD_REPAIR);
    assert!(
        notes.iter().any(|note| note.contains("append in flight")),
        "{notes:?}"
    );

    let lines = json_lines(&report);
    let stop: Value = serde_json::from_str(lines.last().expect("line")).expect("json");
    assert_eq!(stop["checksum_ok"], false);
    assert_eq!(stop["torn_tail"], true);
}

#[tokio::test]
async fn a_rotted_record_exits_seven_at_the_right_offset() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 6).await;
    let active = active_segment(&dir);
    let mut bytes = std::fs::read(&active).expect("read");
    // Segments hold four 37-byte records, so offset 5 is the second record of
    // the second segment.
    let position = SEGMENT_HEADER_LEN as usize + 37;
    bytes[position + RECORD_HEADER_LEN as usize + 3] ^= 0xFF;
    std::fs::write(&active, &bytes).expect("write");

    let report = report(data.path(), 5, 3);
    let text = render(&report, 5, false);

    let last = text.lines().last().expect("row");
    assert!(last.starts_with('*'), "the row asked for: {text}");
    assert!(last.contains("FAIL: record checksum mismatch"), "{text}");
    assert!(last.contains(&position.to_string()), "{text}");
    let (code, notes) = outcome(&report, 5);
    assert_eq!(code, EXIT_DAMAGED);
    assert!(
        notes.iter().any(|note| note.contains("offset 5 should be")),
        "{notes:?}"
    );

    // The records before it are still shown, and a window short of it is clean.
    assert_eq!(text.lines().filter(|line| line.ends_with("ok")).count(), 3);
    let short = self::report(data.path(), 2, 1);
    assert_eq!(outcome(&short, 2).0, 0);
}

#[tokio::test]
async fn a_missing_shard_or_directory_exits_five() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 1).await;

    let mut other = args(data.path(), 0, 0);
    other.shard = "acme/default/other/0".to_string();
    assert_eq!(read(&other).expect_err("missing").0, EXIT_NOT_FOUND);

    let nowhere = args(&data.path().join("nope"), 0, 0);
    assert_eq!(read(&nowhere).expect_err("missing").0, EXIT_NOT_FOUND);

    let mut bad = args(data.path(), 0, 0);
    bad.shard = "orders".to_string();
    assert_eq!(read(&bad).expect_err("usage").0, super::super::EXIT_USAGE);
}

#[tokio::test]
async fn the_shard_kind_and_incarnation_pick_the_directory() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 3).await;

    let mut cache = args(data.path(), 0, 0);
    cache.kind = Some(Store::Cache);
    assert_eq!(read(&cache).expect_err("none").0, EXIT_NOT_FOUND);
    let mut later = args(data.path(), 0, 0);
    later.incarnation = Some(3);
    assert_eq!(read(&later).expect_err("none").0, EXIT_NOT_FOUND);
}

#[tokio::test]
async fn reading_changes_nothing_on_disk() {
    let data = tempfile::tempdir().expect("dir");
    let dir = write_shard(data.path(), 12).await;
    let active = active_segment(&dir);
    let mut bytes = std::fs::read(&active).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&active, &bytes).expect("write");
    std::fs::remove_file(dir.join("00000000000000000000.index")).expect("remove");

    let before = tree(data.path());
    for offset in [0, 6, 11, 99] {
        let mut asked = args(data.path(), offset, 1000);
        asked.payload = true;
        let report = read(&asked).expect("read").0;
        render(&report, offset, true);
    }
    assert_eq!(
        tree(data.path()),
        before,
        "inspect records wrote to the data dir"
    );
}

async fn rot_record_5(data: &Path) -> PathBuf {
    let dir = write_shard(data, 6).await;
    let active = active_segment(&dir);
    let mut bytes = std::fs::read(&active).expect("read");
    let position = SEGMENT_HEADER_LEN as usize + 37;
    bytes[position + RECORD_HEADER_LEN as usize + 3] ^= 0xFF;
    std::fs::write(&active, &bytes).expect("write");
    dir
}

#[tokio::test]
async fn damage_finds_the_offset_for_you() {
    let data = tempfile::tempdir().expect("dir");
    rot_record_5(data.path()).await;

    let mut asked = args(data.path(), 0, 1);
    asked.offset = None;
    asked.damage = true;
    let (report, offset) = read(&asked).expect("read");

    assert_eq!(offset, 5);
    assert_eq!(report.stop.expect("stop").expected_offset, 5);
    assert_eq!(report.records.last().map(|r| r.offset), Some(4));

    let clean = tempfile::tempdir().expect("dir");
    write_shard(clean.path(), 6).await;
    let mut asked = args(clean.path(), 0, 1);
    asked.offset = None;
    asked.damage = true;
    let (code, message) = read(&asked).expect_err("nothing to find");
    assert_eq!(
        (code, message.as_str()),
        (0, "every record in the shard verifies")
    );
}

#[tokio::test]
async fn a_shard_that_startup_would_refuse_says_so() {
    let data = tempfile::tempdir().expect("dir");
    rot_record_5(data.path()).await;

    let (report, offset) = read(&args(data.path(), 2, 1)).expect("read");
    let (code, notes) = outcome(&report, offset);

    assert_eq!(code, 0, "the range itself reads clean");
    assert!(
        notes
            .iter()
            .any(|note| note.contains("startup would refuse")),
        "{notes:?}"
    );
}

#[tokio::test]
async fn an_incarnation_is_looked_up_in_its_own_directory() {
    let data = tempfile::tempdir().expect("dir");
    let dir = felix_storage::disk_log::layout::incarnation_dir(
        &Store::Stream.root(data.path()),
        &key(),
        3,
    );
    let log = DiskLog::open(dir, "orders", config()).expect("open");
    log.append(&[AppendRecord {
        payload: "third".into(),
        timestamp_micros: WHEN,
        mark: Default::default(),
        publisher: None,
        headers: None,
    }])
    .await
    .expect("append");
    log.shutdown().await.expect("shutdown");

    let mut asked = args(data.path(), 0, 0);
    asked.incarnation = Some(3);
    let (report, _) = read(&asked).expect("read");
    assert_eq!(report.records[0].payload_len, 5);
    // Incarnation 0 is a different directory, and is not there.
    assert_eq!(
        read(&args(data.path(), 0, 0)).expect_err("none").0,
        EXIT_NOT_FOUND
    );
}

fn words(list: &[&str]) -> Vec<String> {
    list.iter().map(|word| word.to_string()).collect()
}

#[tokio::test]
async fn run_returns_the_exit_status_in_both_formats() {
    let data = tempfile::tempdir().expect("dir");
    rot_record_5(data.path()).await;
    let data = data.path().to_str().expect("utf8");
    let shard = "acme/default/orders/0";

    for format in [&[][..], &["--json"]] {
        let mut list = vec!["records", data, shard, "--offset", "5", "--around", "2"];
        list.extend_from_slice(format);
        assert_eq!(super::super::run(words(&list)), EXIT_DAMAGED, "{list:?}");
    }
    assert_eq!(
        super::super::run(words(&[
            "records", data, shard, "--offset", "1", "--around", "1"
        ])),
        0
    );
    assert_eq!(
        super::super::run(words(&["records", data, shard, "--offset", "500"])),
        EXIT_NOT_FOUND
    );
    assert_eq!(super::super::run(words(&["records", "--help"])), 0);
    assert_eq!(
        super::super::run(words(&["records", data, shard])),
        super::super::EXIT_USAGE
    );
}

#[test]
fn json_for_a_stop_is_a_line_with_the_error() {
    let report = {
        let data = tempfile::tempdir().expect("dir");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(rot_record_5(data.path()));
        report(data.path(), 5, 1)
    };
    let lines = json_lines(&report);
    let stop: Value = serde_json::from_str(lines.last().expect("line")).expect("json");
    assert_eq!(stop["checksum_ok"], false);
    assert_eq!(stop["torn_tail"], false);
    assert_eq!(stop["offset"], 5);
    assert!(
        stop["error"]
            .as_str()
            .is_some_and(|e| e.contains("checksum"))
    );
}
