//! Backup points taken while publishers run, and restores from a copy taken
//! the same way.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test backup`, after
//! `cargo build -p felix-broker-service --bin felix-broker`.

mod point;
mod restore;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_cluster::Cluster;
use serde::{Deserialize, Serialize};

/// A record some publisher was told is committed, and when it was told.
#[derive(Debug, Clone)]
struct Acked {
    stream: &'static str,
    payload: String,
    /// Wall-clock milliseconds, read after the acknowledgement arrived.
    at_millis: u64,
}

/// Publishers running against the cluster until stopped.
struct Load {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<Vec<Acked>>>,
}

impl Load {
    /// One publisher per broker per stream, each on its own connection,
    /// publishing keyed records as fast as they are acknowledged.
    async fn start(cluster: &Cluster, streams: &[&'static str]) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for node in cluster.node_ids() {
            for &stream in streams {
                let client = cluster.client_on(&node).await.expect("connect");
                let publisher = client.publisher().await.expect("publisher");
                let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
                let stop = Arc::clone(&stop);
                let node = node.clone();
                tasks.push(tokio::spawn(async move {
                    let _client = client;
                    let mut acked = Vec::new();
                    let mut seq = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        seq += 1;
                        let payload = format!("{stream}-{node}-{seq}");
                        // A failed publish is not acknowledged and so is not
                        // counted; only what a client was told matters here.
                        if publisher
                            .publish_keyed(
                                &tenant,
                                &namespace,
                                stream,
                                bytes::Bytes::from(seq.to_be_bytes().to_vec()),
                                payload.clone().into_bytes(),
                                felix_wire::AckMode::PerMessage,
                            )
                            .await
                            .is_ok()
                        {
                            acked.push(Acked {
                                stream,
                                payload,
                                at_millis: now_millis(),
                            });
                        }
                    }
                    acked
                }));
            }
        }
        Self { stop, tasks }
    }

    async fn stop(self) -> Vec<Acked> {
        self.stop.store(true, Ordering::Relaxed);
        let mut acked = Vec::new();
        for task in self.tasks {
            acked.extend(task.await.expect("publisher task"));
        }
        acked
    }
}

/// A backup point as `felixctl backup-point` writes it; the fields the tests
/// read, and enough of the rest for `felix-broker restore-point` to take the
/// manifest back.
#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    format_version: u32,
    name: String,
    taken_at_millis: u64,
    metadata_version: u64,
    shards: Vec<PointShard>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PointShard {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: String,
    leader: String,
    generation: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    incarnation: u64,
    logs: PointLogs,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Serialize, Deserialize)]
struct PointLogs {
    records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    group_cursors: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    group_dead_letters: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    counters: Option<u64>,
}

/// A backup point of the cluster, taken by running `felixctl backup-point`
/// (built alongside the tests by `cargo test --workspace`) against it.
async fn take_point(cluster: &Cluster, name: &str) -> Manifest {
    let mut exe = std::env::current_exe().expect("the running executable");
    exe.pop();
    // Integration test binaries live in `target/<profile>/deps`.
    if exe.ends_with("deps") {
        exe.pop();
    }
    let exe = exe.join("felixctl");
    assert!(
        exe.exists(),
        "felixctl not found at {}; build it with `cargo build -p felixctl`",
        exe.display()
    );
    let scratch = tempfile::tempdir().expect("tempdir");
    let out = scratch.path().join("point.json");
    let mut command = std::process::Command::new(exe);
    command
        .arg("backup-point")
        .arg(name)
        .arg("--out")
        .arg(&out)
        .arg("--controlplane-url")
        .arg(cluster.control_plane_url())
        .arg("--controlplane-token")
        .arg(cluster.admin_token())
        .env_clear()
        .env("HOME", scratch.path())
        .env("FELIX_CLI_CONFIG", scratch.path().join("config.toml"));
    // Every harness broker has its own metrics port on one host.
    for node in &cluster.nodes {
        command
            .arg("--broker")
            .arg(format!("{}=http://{}", node.node_id, node.metrics_addr));
    }
    let output = tokio::task::spawn_blocking(move || command.output())
        .await
        .expect("join")
        .expect("run felixctl");
    assert!(
        output.status.success(),
        "felixctl backup-point failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&std::fs::read(&out).expect("the manifest")).expect("parse the manifest")
}

/// `GET /backup/offsets` on `node`, as `(stream, shard) -> records offset`.
async fn committed_on(cluster: &Cluster, node: &str) -> HashMap<(String, u32), u64> {
    let metrics = cluster.node(node).expect("node").metrics_addr;
    let body: serde_json::Value = reqwest::get(format!("http://{metrics}/backup/offsets"))
        .await
        .expect("reach the metrics listener")
        .json()
        .await
        .expect("json");
    body["shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|shard| {
            (
                (
                    shard["name"].as_str().expect("name").to_string(),
                    shard["shard"].as_u64().expect("shard") as u32,
                ),
                shard["logs"]["records"].as_u64().expect("records"),
            )
        })
        .collect()
}

fn point_shard<'a>(manifest: &'a Manifest, stream: &str, shard: u32) -> &'a PointShard {
    manifest
        .shards
        .iter()
        .find(|s| s.name == stream && s.shard == shard)
        .unwrap_or_else(|| panic!("the point has no entry for {stream}/{shard}"))
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

/// Long enough for every publisher to have been acknowledged many times.
const LOAD_WARMUP: Duration = Duration::from_millis(1500);
