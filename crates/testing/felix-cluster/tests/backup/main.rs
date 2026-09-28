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
use felix_controlplane_service::admin::backup_point::{BackupPoint, Manifest, PointShard};

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

/// A backup point of the cluster, reading each broker's offsets from its
/// metrics listener.
async fn take_point(cluster: &Cluster, name: &str) -> Manifest {
    backup_point(cluster, name)
        .take()
        .await
        .expect("take a backup point")
}

fn backup_point(cluster: &Cluster, name: &str) -> BackupPoint {
    BackupPoint {
        control_plane_url: cluster.control_plane_url().to_string(),
        token: Some(cluster.admin_token.clone()),
        name: name.to_string(),
        // Every harness broker has its own metrics port on one host.
        brokers: cluster
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node_id.clone(),
                    format!("http://{}", node.metrics_addr),
                )
            })
            .collect(),
        metrics_port: 0,
    }
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
