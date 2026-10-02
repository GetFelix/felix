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
        "tenant",
        "namespace",
        "stream",
        "node",
        "shard",
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
