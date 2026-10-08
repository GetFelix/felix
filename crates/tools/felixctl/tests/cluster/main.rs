//! The `felixctl` binary against a real cluster: broker processes and a control
//! plane started by the `felix-cluster` harness.
//!
//! Each test starts its own cluster, so they run one at a time. The broker
//! binary must be built first; `cargo test --workspace` does that.
//!
//! The binary runs as a child process under `tokio::process`, so the test's
//! runtime keeps serving the in-process control plane while it runs.

mod bench;
mod contexts;
mod control_plane;
mod data_plane;
mod rbac;

use std::path::{Path, PathBuf};
use std::process::Stdio;

use felix_cluster::Cluster;
use tokio::io::AsyncWriteExt;

/// What one run of `felixctl` printed and how it ended.
pub(crate) struct Run {
    pub(crate) code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

impl Run {
    /// Fail the test unless the run succeeded.
    pub(crate) fn ok(self) -> Self {
        assert_eq!(
            self.code, 0,
            "felixctl failed:\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_str(self.stdout.trim())
            .unwrap_or_else(|err| panic!("not JSON ({err}):\n{}", self.stdout))
    }

    pub(crate) fn json_lines(&self) -> Vec<serde_json::Value> {
        self.stdout
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON line"))
            .collect()
    }
}

/// A scratch directory with an empty config file and the brokers' exported
/// certificates, so the CLI checks them as a real client would.
pub(crate) struct Env {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) ca: PathBuf,
}

impl Env {
    pub(crate) fn new(cluster: &Cluster) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca = dir.path().join("brokers.pem");
        let mut bundle = String::new();
        for node in &cluster.nodes {
            let pem = std::fs::read_to_string(node.data_dir.join("broker-cert.pem"))
                .expect("the broker exported its certificate");
            bundle.push_str(&pem);
            bundle.push('\n');
        }
        std::fs::write(&ca, bundle).expect("write the CA bundle");
        Self { dir, ca }
    }

    pub(crate) fn config(&self) -> PathBuf {
        self.dir.path().join("config.toml")
    }

    /// Every connection flag, so a test needs no context.
    pub(crate) fn flags(&self, cluster: &Cluster) -> Vec<String> {
        let brokers: Vec<String> = cluster
            .broker_addrs()
            .iter()
            .map(|addr| addr.to_string())
            .collect();
        vec![
            "--brokers".into(),
            brokers.join(","),
            "--tenant".into(),
            cluster.tenant_id.clone(),
            "--namespace".into(),
            cluster.namespace.clone(),
            "--token".into(),
            cluster.client_token(),
            "--ca-file".into(),
            self.ca.display().to_string(),
            "--controlplane-url".into(),
            cluster.control_plane_url().to_string(),
            "--controlplane-token".into(),
            cluster.admin_token(),
        ]
    }

    /// Run `felixctl` with `args`, the connection flags `args` does not
    /// already set, and this config file.
    pub(crate) async fn felixctl(&self, cluster: &Cluster, args: &[&str]) -> Run {
        self.run(&with_flags(args, self.flags(cluster)), &[], None)
            .await
    }

    /// The offset of the first record in shard 0 of `stream` that is not one
    /// of the harness's readiness probes. Their number is not fixed, and a
    /// publish ack carries no offset when the broker acks on enqueue, so a
    /// test finds where its own records start by reading.
    pub(crate) async fn first_after_probes(&self, cluster: &Cluster, stream: &str) -> u64 {
        let mut offset = 0;
        loop {
            let from = offset.to_string();
            let run = self
                .felixctl(
                    cluster,
                    &[
                        "sub", stream, "--shard", "0", "--from", &from, "--count", "1", "--json",
                    ],
                )
                .await
                .ok();
            let event = run.json();
            let at = event["offset"]
                .as_u64()
                .unwrap_or_else(|| panic!("no offset: {}", run.stdout));
            if event["payload"] != "harness-probe" {
                return at;
            }
            offset = at + 1;
        }
    }

    /// Run `felixctl` with only `args`, the given environment, and `stdin`.
    pub(crate) async fn run(
        &self,
        args: &[String],
        env: &[(&str, &str)],
        stdin: Option<&[u8]>,
    ) -> Run {
        run_felixctl(self.dir.path(), &self.config(), args, env, stdin).await
    }
}

/// `args` followed by each `--flag value` pair in `flags` that `args` does not
/// set itself. clap refuses a flag given twice, so a test overriding one
/// connection flag must not also get the default.
fn with_flags(args: &[&str], flags: Vec<String>) -> Vec<String> {
    let mut all: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
    let mut flags = flags.into_iter();
    while let (Some(name), Some(value)) = (flags.next(), flags.next()) {
        if !args.contains(&name.as_str()) {
            all.push(name);
            all.push(value);
        }
    }
    all
}

#[test]
fn an_explicit_flag_replaces_the_default() {
    let defaults = ["--tenant", "t1", "--controlplane-token", "default"]
        .map(String::from)
        .to_vec();
    assert_eq!(
        with_flags(
            &["tenant", "ls", "--controlplane-token", "admin"],
            defaults.clone()
        ),
        [
            "tenant",
            "ls",
            "--controlplane-token",
            "admin",
            "--tenant",
            "t1"
        ]
    );
    assert_eq!(
        with_flags(&["tenant", "ls"], defaults),
        [
            "tenant",
            "ls",
            "--tenant",
            "t1",
            "--controlplane-token",
            "default"
        ]
    );
}

/// Run the binary with a clean environment: only `HOME`, `PATH`, the config
/// file and `env`, so nothing from the machine running the tests leaks in.
pub(crate) async fn run_felixctl(
    home: &Path,
    config: &Path,
    args: &[String],
    env: &[(&str, &str)],
    stdin: Option<&[u8]>,
) -> Run {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_felixctl"));
    command
        .args(args)
        .env_clear()
        .env("HOME", home)
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("FELIX_CLI_CONFIG", config)
        .envs(env.iter().copied())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn felixctl");
    if let Some(input) = stdin {
        let mut pipe = child.stdin.take().expect("stdin");
        pipe.write_all(input).await.expect("write stdin");
        drop(pipe);
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        child.wait_with_output(),
    )
    .await
    .unwrap_or_else(|_| panic!("felixctl {args:?} did not finish in 120 s"))
    .expect("wait for felixctl");
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}
