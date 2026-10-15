//! `felix-broker inspect segments` as a process: it needs no configuration,
//! exits by verdict, and leaves the data directory exactly as it found it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ShardKey};

fn inspect(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_felix-broker"))
        .arg("inspect")
        .args(args)
        .env_clear()
        .output()
        .expect("run felix-broker")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// A data directory with one stream shard of `count` records, closed cleanly.
async fn data_dir_with_a_shard(count: usize) -> (tempfile::TempDir, PathBuf) {
    let data = tempfile::tempdir().expect("dir");
    let key = ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 0,
    };
    let dir =
        felix_storage::inspect::shard_dir(data.path(), felix_storage::inspect::Store::Stream, &key);
    let config = LogConfig {
        segment_size_bytes: 200,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let log = felix_storage::DiskLog::open(dir.clone(), "orders", config).expect("open");
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
    (data, dir)
}

/// Every file under `dir` with its bytes and modification time.
fn files_under(dir: &Path) -> Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> {
    let mut out = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("list") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("meta");
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                let bytes = std::fs::read(entry.path()).expect("read");
                out.push((entry.path(), bytes, meta.modified().expect("mtime")));
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn inspect_segments_reads_a_data_dir_without_starting_a_broker() {
    let (data, dir) = data_dir_with_a_shard(20).await;
    let data_arg = data.path().to_str().expect("utf-8");

    let clean = inspect(&["segments", data_arg]);
    assert_eq!(clean.status.code(), Some(0), "{clean:?}");
    assert!(stdout(&clean).contains("clean"), "{}", stdout(&clean));

    // A torn tail on the newest segment: startup would cut it.
    let newest = std::fs::read_dir(&dir)
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
        .expect("segment");
    let mut bytes = std::fs::read(&newest).expect("read");
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&newest, &bytes).expect("write");

    let before = files_under(data.path());
    let torn = inspect(&["segments", data_arg, "acme/default/orders/0"]);
    assert_eq!(
        files_under(data.path()),
        before,
        "inspect wrote to the data dir"
    );
    assert_eq!(torn.status.code(), Some(6), "{torn:?}");
    assert!(stdout(&torn).contains("RECORDS CHECK"), "{}", stdout(&torn));
    assert!(
        stdout(&torn).contains("cut the torn tail"),
        "{}",
        stdout(&torn)
    );

    let json = inspect(&["segments", data_arg, "--json"]);
    assert_eq!(json.status.code(), Some(6), "{json:?}");
    let report: serde_json::Value =
        serde_json::from_str(stdout(&json).trim()).expect("one JSON line");
    assert_eq!(report["startup"]["verdict"], "repair");
    assert_eq!(report["actions"][0]["action"], "truncate_tail");
    assert_eq!(report["actions"][0]["discarded_bytes"], 13);

    let missing = inspect(&["segments", data_arg, "acme/default/nope/0"]);
    assert_eq!(missing.status.code(), Some(5), "{missing:?}");
    let usage = inspect(&["segments"]);
    assert_eq!(usage.status.code(), Some(2), "{usage:?}");
}
