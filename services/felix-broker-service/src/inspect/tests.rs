use std::path::Path;

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
    let dir = felix_storage::inspect::shard_dir(data, Store::Stream, &key());
    let log = DiskLog::open(dir.clone(), "orders", config()).expect("open");
    for i in 0..count {
        log.append(&[AppendRecord {
            payload: format!("value-{i:03}").into(),
            timestamp_micros: 1,
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

fn report_of(dir: &Path) -> Value {
    let shard = ShardDir {
        store: Store::Stream,
        name: dir.file_name().unwrap().to_string_lossy().into_owned(),
        path: dir.to_path_buf(),
    };
    shard_json(&shard, &inspect_shard(dir, &config()).expect("inspect"))
}

fn exit_of(reports: &[Value]) -> u8 {
    verdict(reports).0
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
        parse_shard(bad).expect_err(bad);
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
    assert_eq!(exit_of(&[report]), 0);
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
    assert_eq!(exit_of(&[report]), EXIT_WOULD_REPAIR);
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
    assert_eq!(exit_of(&[report]), EXIT_DAMAGED);
}

fn args(list: &[&str]) -> Result<Parsed, String> {
    parse(list.iter().map(|arg| arg.to_string()).collect())
}

#[test]
fn arguments_parse_like_the_usage_says() {
    let Ok(Parsed::Run(parsed)) = args(&[
        "segments",
        "/data",
        "acme/default/orders/0",
        "--kind",
        "cache",
        "--incarnation",
        "42",
        "--json",
        "--index-spacing",
        "8192",
        "--repair-checksum-tail",
        "--verify-all-on-open",
        "--segments",
    ]) else {
        panic!("expected a run");
    };
    assert_eq!(
        parsed,
        Args {
            data_dir: "/data".into(),
            shard: Some("acme/default/orders/0".into()),
            kind: Some(Store::Cache),
            incarnation: Some(42),
            segments: true,
            json: true,
            repair_checksum_tail: true,
            index_spacing: Some(8192),
            verify_all_on_open: true,
        }
    );
    assert!(matches!(args(&["segments", "--help"]), Ok(Parsed::Help)));
    assert!(matches!(args(&[]), Ok(Parsed::Help)));
    for bad in [
        &["records", "/data"][..],
        &["segments"],
        &["segments", "/data", "--kind", "topics"],
        &["segments", "/data", "--index-spacing", "lots"],
        &["segments", "/data", "--incarnation", "new"],
        &["segments", "/data", "--frobnicate"],
        &["segments", "/data", "a/b/c/0", "extra"],
    ] {
        assert!(args(bad).is_err(), "{bad:?}");
    }
}

fn run_args(list: &[&str]) -> u8 {
    run(list.iter().map(|arg| arg.to_string()).collect())
}

fn segments_args(data: &Path, shard: Option<&str>, kind: Option<Store>) -> Args {
    Args {
        data_dir: data.to_path_buf(),
        shard: shard.map(str::to_string),
        kind,
        index_spacing: Some(32),
        ..Args::default()
    }
}

/// **The exit status is the contract scripts read**: 0 for help, 2 for bad
/// arguments, 5 for a missing directory, and 0 for a clean shard, in text or
/// JSON.
#[tokio::test]
async fn run_exits_with_the_documented_status() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 4).await;
    let dir = data.path().to_str().expect("utf-8");
    assert_eq!(run_args(&["--help"]), 0);
    assert_eq!(run_args(&["segments", "--help"]), 0);
    assert_eq!(run_args(&["segments", dir, "--bogus"]), EXIT_USAGE);
    assert_eq!(run_args(&["frobnicate"]), EXIT_USAGE);
    let missing = data.path().join("nope");
    assert_eq!(
        run_args(&["segments", missing.to_str().expect("utf-8")]),
        EXIT_NOT_FOUND
    );
    assert_eq!(
        run_args(&[
            "segments",
            dir,
            "acme/default/orders/0",
            "--index-spacing",
            "32"
        ]),
        0
    );
    assert_eq!(
        run_args(&["segments", dir, "--json", "--index-spacing", "32"]),
        0
    );
    assert_eq!(
        run_args(&["segments", dir, "acme/default/orders/9"]),
        EXIT_NOT_FOUND
    );
}

/// **A named shard is reported alone and marked as named**, so its segments
/// are listed even when it is clean.
#[tokio::test]
async fn a_named_shard_is_the_only_report() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 4).await;
    let (reports, single) = inspect(&segments_args(
        data.path(),
        Some("acme/default/orders/0"),
        None,
    ))
    .expect("inspect");
    assert!(single);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["startup"]["verdict"], "clean");
    assert!(
        reports[0]["segments"]
            .as_array()
            .is_some_and(|s| !s.is_empty())
    );
}

/// **Without a shard every shard is listed, and `--kind` keeps only that
/// store's**: a stream shard is not reported when caches are asked for.
#[tokio::test]
async fn kind_filters_the_listed_shards() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 4).await;
    let (all, single) = inspect(&segments_args(data.path(), None, None)).expect("inspect");
    assert!(!single);
    assert_eq!(all.len(), 1);
    let (streams, _) =
        inspect(&segments_args(data.path(), None, Some(Store::Stream))).expect("inspect");
    assert_eq!(streams.len(), 1);
    let (caches, _) =
        inspect(&segments_args(data.path(), None, Some(Store::Cache))).expect("inspect");
    assert!(caches.is_empty(), "{caches:?}");
}

/// **A shard that is not there is status 5 and says where it looked**; one
/// named badly is a usage error, status 2.
#[tokio::test]
async fn a_missing_or_malformed_shard_is_refused_with_its_status() {
    let data = tempfile::tempdir().expect("dir");
    write_shard(data.path(), 1).await;
    let (code, message) = inspect(&segments_args(
        data.path(),
        Some("acme/default/orders/0"),
        Some(Store::Cache),
    ))
    .expect_err("no cache shard");
    assert_eq!(code, EXIT_NOT_FOUND);
    assert!(
        message.contains("no cache shard acme/default/orders/0"),
        "{message}"
    );
    assert!(message.contains("looked for"), "{message}");

    let (code, _) = inspect(&segments_args(data.path(), Some("orders"), None)).expect_err("bad");
    assert_eq!(code, EXIT_USAGE);

    let file = data.path().join("plain-file");
    std::fs::write(&file, b"x").expect("write");
    let (code, message) = inspect(&segments_args(&file, None, None)).expect_err("not a dir");
    assert_eq!(code, EXIT_NOT_FOUND);
    assert!(message.contains("is not a directory"), "{message}");
}

/// **The broker settings the verdict depends on are the ones given**, and
/// everything else keeps the broker's defaults.
#[test]
fn log_config_takes_the_three_settings_from_the_arguments() {
    let config = log_config(&Args {
        index_spacing: Some(4096),
        repair_checksum_tail: true,
        verify_all_on_open: true,
        ..Args::default()
    });
    assert_eq!(config.index_spacing_bytes, 4096);
    assert!(config.repair_checksum_tail);
    assert!(config.verify_all_on_open);
    let defaults = log_config(&Args::default());
    assert_eq!(
        defaults.index_spacing_bytes,
        LogConfig::default().index_spacing_bytes
    );
    assert!(!defaults.repair_checksum_tail);
}

/// **Every planned write reads as a sentence**, including a retired segment's
/// cut with and without the segment it removes, and an action this build does
/// not know, which is shown by name rather than dropped.
#[test]
fn every_startup_action_is_described() {
    let cases = [
        (
            json!({"action": "remove_segment", "segment": 3, "reason": "leftover"}),
            "remove segment 3: leftover",
        ),
        (
            json!({"action": "rebuild_index", "segment": 4, "reason": "stale"}),
            "rebuild the index of segment 4: stale",
        ),
        (
            json!({"action": "cut_retired_segment", "segment": 5, "to_bytes": 100,
                   "removes_segment": null}),
            "cut segment 5 to 100 bytes, a seal that never finished",
        ),
        (
            json!({"action": "cut_retired_segment", "segment": 5, "to_bytes": 100,
                   "removes_segment": 6}),
            "cut segment 5 to 100 bytes, a seal that never finished, and remove segment 6, \
             which no longer follows on",
        ),
        (
            json!({"action": "create_first_segment"}),
            "start an empty log",
        ),
        (json!({"action": "something_new"}), "something_new"),
    ];
    for (action, expected) in cases {
        assert_eq!(describe(&action), expected);
    }
}

fn refused_report() -> Value {
    json!({
        "store": "stream",
        "dir": "orders-0",
        "path": "/data/orders-0",
        "startup": {"verdict": "refuse", "segment": 2, "position": 77, "detail": "bad crc"},
        "actions": [{"action": "rebuild_index", "segment": 1, "reason": "missing"}],
        "durable_mark": null,
        "segments": [],
    })
}

/// **A refused shard names the segment and byte where startup stops**, and
/// the writes it would make first are introduced as coming before the
/// refusal.
#[test]
fn a_refused_shard_says_where_and_what_comes_first() {
    let report = refused_report();
    assert_eq!(summary(&report), "refuse: segment 2 at byte 77: bad crc");
    let text = details(&report);
    assert!(
        text.starts_with("orders-0 (stream)  /data/orders-0"),
        "{text}"
    );
    assert!(text.contains("  no segments"), "{text}");
    assert!(
        text.contains("startup refuses: segment 2 at byte 77: bad crc"),
        "{text}"
    );
    assert!(text.contains("before refusing, startup would:"), "{text}");
    assert!(
        text.contains("    rebuild the index of segment 1: missing"),
        "{text}"
    );
    assert!(has_findings(&report));
}

/// **Damage in a segment startup removes is explained by the removal**, and
/// damage behind a refusal is the refusal's; only damage startup reads past
/// counts as unexplained.
#[test]
fn damage_is_unexplained_only_where_startup_reads_past_it() {
    let damaged = |actions: Value| {
        json!({
            "startup": {"verdict": "clean"},
            "actions": actions,
            "segments": [{"id": 1, "index": "matches",
                          "damage": {"position": 9, "cause": "crc", "tail": false}}],
        })
    };
    assert!(unexplained_damage(&damaged(json!([]))));
    assert!(!unexplained_damage(&damaged(json!([
        {"action": "remove_segment", "segment": 1, "reason": "leftover"}
    ]))));
    assert!(!unexplained_damage(&damaged(json!([
        {"action": "cut_retired_segment", "segment": 0, "to_bytes": 5, "removes_segment": 1}
    ]))));
    assert!(!unexplained_damage(&refused_report()));
    let torn = json!({
        "startup": {"verdict": "clean"},
        "actions": [],
        "segments": [{"id": 1, "index": "matches",
                      "damage": {"position": 9, "cause": "short", "tail": true}}],
    });
    assert!(!unexplained_damage(&torn));
}

/// **A missing or stale index is a finding even on a clean shard**, so the
/// shard's segments are listed; a matching index is not.
#[test]
fn a_bad_index_is_a_finding() {
    for (index, finding) in [("missing", true), ("stale", true), ("matches", false)] {
        let report = json!({
            "startup": {"verdict": "clean"},
            "actions": [],
            "segments": [{"id": 0, "index": index, "damage": null}],
        });
        assert_eq!(has_findings(&report), finding, "{index}");
    }
}

/// **Damage outranks repair in the exit status**, and the reason counts the
/// shards of each kind.
#[test]
fn the_verdict_counts_refused_damaged_and_repaired_shards() {
    let rotted = json!({
        "startup": {"verdict": "clean"},
        "actions": [],
        "segments": [{"id": 1, "damage": {"position": 1, "cause": "crc", "tail": false}}],
    });
    let repaired = json!({"startup": {"verdict": "repair"}, "actions": [], "segments": []});
    assert_eq!(
        verdict(&[
            refused_report(),
            refused_report(),
            rotted.clone(),
            repaired.clone()
        ]),
        (
            EXIT_DAMAGED,
            Some(
                "startup would refuse 2 shards; 1 shard hold records that fail their checksum"
                    .to_string()
            )
        )
    );
    assert_eq!(
        verdict(&[repaired.clone(), repaired]),
        (
            EXIT_WOULD_REPAIR,
            Some("startup would repair 2 shards".to_string())
        )
    );
    assert_eq!(verdict(&[]), (0, None));
}

/// **An unknown offset prints as a dash, not as an empty cell or `null`.**
#[test]
fn a_null_cell_prints_as_a_dash() {
    assert_eq!(dash(&Value::Null), "-");
    assert_eq!(dash(&json!(7)), "7");
    assert_eq!(text(&Value::Null), "");
    assert_eq!(text(&json!("x")), "x");
}
