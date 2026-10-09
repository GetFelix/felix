//! The binary without a cluster: help, exit statuses for bad input and for
//! endpoints that do not answer, and contexts.

use std::path::Path;
use std::process::{Command, Output};

fn felixctl(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_felixctl"))
        .args(args)
        .env_clear()
        .env("HOME", home)
        .env("FELIX_CLI_CONFIG", home.join("config.toml"))
        .output()
        .expect("run felixctl")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn no_arguments_prints_the_overview() {
    let home = tempfile::tempdir().unwrap();
    let output = felixctl(home.path(), &[]);
    assert!(output.status.success());
    assert!(
        stdout(&output).contains("Get started:"),
        "{}",
        stdout(&output)
    );
}

#[test]
fn help_works_both_ways_for_every_command() {
    let home = tempfile::tempdir().unwrap();
    for command in [
        "context",
        "pub",
        "sub",
        "cache",
        "topology",
        "group",
        "counter",
        "tenant",
        "namespace",
        "stream",
        "node",
        "shard",
        "placement",
        "inspect",
        "bench",
        "completions",
    ] {
        let by_flag = felixctl(home.path(), &[command, "--help"]);
        let by_help = felixctl(home.path(), &["help", command]);
        assert!(by_flag.status.success(), "{command} --help failed");
        assert!(by_help.status.success(), "help {command} failed");
        assert_eq!(stdout(&by_flag), stdout(&by_help), "{command}");
        assert!(stdout(&by_flag).contains("Examples:"), "{command}");
    }
}

#[test]
fn a_destructive_group_command_off_a_terminal_needs_yes() {
    let home = tempfile::tempdir().unwrap();
    // stdin is not a terminal here, so nothing can be asked. The refusal
    // comes before any broker is contacted; port 9 would not answer anyway.
    for args in [
        &["group", "rm", "orders", "billing"][..],
        &[
            "group",
            "dead-letters",
            "discard",
            "orders",
            "billing",
            "0:3",
        ],
    ] {
        let mut args = args.to_vec();
        args.extend(["--brokers", "127.0.0.1:9", "--tenant", "t1", "--token", "x"]);
        let output = felixctl(home.path(), &args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--yes"), "{stderr}");
    }
}

#[test]
fn a_usage_error_exits_2() {
    let home = tempfile::tempdir().unwrap();
    assert_eq!(felixctl(home.path(), &["pub"]).status.code(), Some(2));
    assert_eq!(
        felixctl(home.path(), &["sub", "s", "--from", "x"])
            .status
            .code(),
        Some(2)
    );
    // Nothing says where the brokers are.
    assert_eq!(
        felixctl(home.path(), &["pub", "s", "x"]).status.code(),
        Some(2)
    );
}

#[test]
fn an_unreachable_control_plane_exits_3() {
    let home = tempfile::tempdir().unwrap();
    // Port 9 (discard) on loopback: nothing listens there in CI.
    let output = felixctl(
        home.path(),
        &[
            "--json",
            "stream",
            "ls",
            "--tenant",
            "t",
            "--controlplane-url",
            "http://127.0.0.1:9",
        ],
    );
    assert_eq!(output.status.code(), Some(3));
    let error: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("a JSON error on stderr");
    assert_eq!(error["exit"], 3);
}

#[test]
fn a_destructive_command_off_a_terminal_needs_yes() {
    let home = tempfile::tempdir().unwrap();
    // The control plane is unreachable, so exit 3 would mean a request was
    // sent; 2 means it stopped first.
    let reach = ["--tenant", "t", "--controlplane-url", "http://127.0.0.1:9"];
    for command in [
        &["stream", "rm", "orders"][..],
        &["cache", "rm", "sessions"],
        &["namespace", "rm", "payments"],
        &["tenant", "rm", "acme"],
        &["node", "drain", "broker-2"],
        &["node", "deregister", "broker-2"],
        &["placement", "abandon", "orders", "0"],
    ] {
        let args: Vec<&str> = command.iter().chain(&reach).copied().collect();
        let output = felixctl(home.path(), &args);
        assert_eq!(output.status.code(), Some(2), "{command:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--yes"), "{command:?}: {stderr}");
    }
    let mut args = vec!["stream", "rm", "orders", "--yes"];
    args.extend(reach);
    assert_eq!(felixctl(home.path(), &args).status.code(), Some(3));
}

#[test]
fn a_missing_context_exits_5() {
    let home = tempfile::tempdir().unwrap();
    let output = felixctl(home.path(), &["context", "use", "nope"]);
    assert_eq!(output.status.code(), Some(5));
}

#[test]
fn contexts_are_saved_listed_and_removed() {
    let home = tempfile::tempdir().unwrap();
    let add = felixctl(
        home.path(),
        &[
            "context",
            "add",
            "dev",
            "--brokers",
            "127.0.0.1:5000",
            "--tenant",
            "t1",
        ],
    );
    assert!(add.status.success());
    let ls = felixctl(home.path(), &["context", "ls"]);
    assert!(stdout(&ls).contains("*  dev"), "{}", stdout(&ls));
    assert!(
        felixctl(home.path(), &["context", "rm", "dev"])
            .status
            .success()
    );
    let ls = felixctl(home.path(), &["context", "ls", "--json"]);
    let value: serde_json::Value = serde_json::from_slice(&ls.stdout).unwrap();
    assert_eq!(value["contexts"], serde_json::json!([]));
}

#[test]
fn completions_and_man_pages_are_generated() {
    let home = tempfile::tempdir().unwrap();
    let zsh = felixctl(home.path(), &["completions", "zsh"]);
    assert!(stdout(&zsh).starts_with("#compdef felixctl"));
    let dir = home.path().join("man");
    let man = felixctl(home.path(), &["man", "--out-dir", dir.to_str().unwrap()]);
    assert!(man.status.success());
    assert!(dir.join("felixctl.1").exists());
    assert!(dir.join("felixctl-cache-watch.1").exists());
}

/// A target that is neither NAME nor TENANT/NAMESPACE/NAME is a usage error,
/// found before anything is dialled.
#[test]
fn inspect_refuses_a_malformed_target() {
    let home = tempfile::tempdir().unwrap();
    let output = felixctl(
        home.path(),
        &[
            "inspect",
            "shard",
            "acme/orders",
            "--brokers",
            "127.0.0.1:1",
        ],
    );
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("TENANT/NAMESPACE/NAME"),
        "{output:?}"
    );
}

/// A cursor felixctl did not print, or one without the broker it belongs to,
/// is a usage error found before anything is dialled.
#[test]
fn inspect_subs_refuses_a_cursor_it_cannot_use() {
    let home = tempfile::tempdir().unwrap();
    for args in [
        &[
            "inspect",
            "subs",
            "--cursor",
            "abc",
            "--brokers",
            "127.0.0.1:1",
        ][..],
        &[
            "inspect",
            "subs",
            "--node",
            "broker-a",
            "--cursor",
            "abc",
            "--brokers",
            "127.0.0.1:1",
        ][..],
        &[
            "inspect",
            "subs",
            "--shard",
            "0",
            "--brokers",
            "127.0.0.1:1",
        ][..],
    ] {
        let output = felixctl(home.path(), args);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
    }
}

/// A data directory with one stream shard of `count` records, closed cleanly.
fn data_dir_with_a_shard(count: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, ShardKey};

    let data = tempfile::tempdir().unwrap();
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let log = felix_storage::DiskLog::open(dir.clone(), "orders", config).unwrap();
        for i in 0..count {
            log.append(&[AppendRecord {
                payload: format!("value-{i:03}").into(),
                timestamp_micros: 1,
                mark: Default::default(),
                publisher: None,
            }])
            .await
            .unwrap();
        }
        log.shutdown().await.unwrap();
    });
    (data, dir)
}

/// Every file under `dir` with its bytes and modification time.
fn files_under(dir: &Path) -> Vec<(std::path::PathBuf, Vec<u8>, std::time::SystemTime)> {
    let mut out = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                let bytes = std::fs::read(entry.path()).unwrap();
                out.push((entry.path(), bytes, meta.modified().unwrap()));
            }
        }
    }
    out.sort();
    out
}

/// `inspect segments` needs no context or broker, exits by verdict, and
/// leaves the data directory exactly as it found it.
#[test]
fn inspect_segments_reads_a_data_dir_offline() {
    let home = tempfile::tempdir().unwrap();
    let (data, dir) = data_dir_with_a_shard(20);
    let data_arg = data.path().to_str().unwrap();

    let clean = felixctl(home.path(), &["inspect", "segments", data_arg]);
    assert_eq!(clean.status.code(), Some(0), "{clean:?}");
    assert!(stdout(&clean).contains("clean"), "{}", stdout(&clean));

    // A torn tail on the newest segment: startup would cut it.
    let newest = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
        .unwrap();
    let mut bytes = std::fs::read(&newest).unwrap();
    bytes.extend_from_slice(&[0x11; 13]);
    std::fs::write(&newest, &bytes).unwrap();

    let before = files_under(data.path());
    let torn = felixctl(
        home.path(),
        &["inspect", "segments", data_arg, "acme/default/orders/0"],
    );
    assert_eq!(
        files_under(data.path()),
        before,
        "felixctl wrote to the data dir"
    );
    assert_eq!(torn.status.code(), Some(6), "{torn:?}");
    assert!(stdout(&torn).contains("RECORDS CHECK"), "{}", stdout(&torn));
    assert!(
        stdout(&torn).contains("cut the torn tail"),
        "{}",
        stdout(&torn)
    );

    let json = felixctl(home.path(), &["--json", "inspect", "segments", data_arg]);
    assert_eq!(json.status.code(), Some(6), "{json:?}");
    let report: serde_json::Value = serde_json::from_str(stdout(&json).trim()).unwrap();
    assert_eq!(report["startup"]["verdict"], "repair");
    assert_eq!(report["actions"][0]["action"], "truncate_tail");
    assert_eq!(report["actions"][0]["discarded_bytes"], 13);

    let missing = felixctl(
        home.path(),
        &["inspect", "segments", data_arg, "acme/default/nope/0"],
    );
    assert_eq!(missing.status.code(), Some(5), "{missing:?}");
}
