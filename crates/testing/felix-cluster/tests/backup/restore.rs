//! Restoring a live copy of a leader's shards to a backup point.
//!
//! Run with `cargo test -p felix-cluster --test backup restore::`.
use std::collections::HashSet;
use std::path::Path;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec, broker_binary};
use felix_storage::DiskLog;
use felix_storage::disk_log::layout::shard_dir;
use felix_storage::log::{AppendOnlyLog, LogConfig, ReadRange, ShardKey};
use serial_test::serial;

use super::{LOAD_WARMUP, Load, point_shard, take_point};

const STREAM: &str = "orders";
const SHARDS: u32 = 2;

/// **A restore keeps everything acknowledged before the barrier and nothing
/// past the point.** Each leader's shard directories are copied while the
/// publishers keep going, so the copy runs past the point; `restore-point`
/// cuts it back, below the copy's own commit offset.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_live_copy_restored_to_the_point_keeps_every_ack_before_it_and_nothing_after() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, SHARDS, 3)],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start");

    let load = Load::start(&cluster, &[STREAM]).await;
    tokio::time::sleep(LOAD_WARMUP).await;
    let point = take_point(&cluster, "live-copy").await;
    // Let the copy run well past the point.
    tokio::time::sleep(LOAD_WARMUP / 2).await;

    let backups = tempfile::tempdir().expect("tempdir");
    let manifest_path = backups.path().join("live-copy.backup-point.json");
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&point).expect("manifest"),
    )
    .expect("write the manifest");
    for shard in 0..SHARDS {
        let entry = point_shard(&point, STREAM, shard);
        let live = cluster
            .node(&entry.leader)
            .expect("leader")
            .data_dir
            .clone();
        let key = shard_key(&cluster, shard);
        copy_live_shard(
            &shard_dir(&live, &key),
            &shard_dir(&backups.path().join(&entry.leader), &key),
        );
    }
    let acked = load.stop().await;

    // Opening the copy is what the restore does first too, torn tail and all.
    let mut copied_past_the_point = false;
    for shard in 0..SHARDS {
        let entry = point_shard(&point, STREAM, shard);
        let dir = shard_dir(
            &backups.path().join(&entry.leader),
            &shard_key(&cluster, shard),
        );
        let log = DiskLog::open(&dir, format!("{STREAM}/{shard}"), LogConfig::default())
            .expect("open the copy");
        copied_past_the_point |= log.tail_offset().await.expect("tail") > entry.logs.records;
        log.shutdown().await.expect("close the copy");
    }
    assert!(
        copied_past_the_point,
        "the copy never ran past the point, so the restore had nothing to cut"
    );

    let leaders: HashSet<&str> = point.shards.iter().map(|s| s.leader.as_str()).collect();
    for leader in leaders {
        let status = std::process::Command::new(broker_binary().expect("felix-broker"))
            .arg("restore-point")
            .arg("--point")
            .arg(&manifest_path)
            .arg("--node")
            .arg(leader)
            .env("FELIX_DURABLE_STORAGE_DIR", backups.path().join(leader))
            .status()
            .expect("run restore-point");
        assert!(status.success(), "restore-point for {leader}: {status}");
    }

    let mut restored = HashSet::new();
    for shard in 0..SHARDS {
        let entry = point_shard(&point, STREAM, shard);
        let dir = shard_dir(
            &backups.path().join(&entry.leader),
            &shard_key(&cluster, shard),
        );
        let log = DiskLog::open(&dir, format!("{STREAM}/{shard}"), LogConfig::default())
            .expect("open the restored copy");
        let tail = log.tail_offset().await.expect("tail");
        assert_eq!(
            tail, entry.logs.records,
            "{STREAM}/{shard} restored to {tail}, not to the point"
        );
        assert!(
            log.commit_offset() <= entry.logs.records,
            "{STREAM}/{shard} still claims {} committed, past the point at {}",
            log.commit_offset(),
            entry.logs.records
        );
        let records = log
            .read_range(ReadRange {
                start: log.base_offset(),
                max_bytes: usize::MAX,
            })
            .await
            .expect("read the restored copy");
        assert!(records.iter().all(|r| r.offset < entry.logs.records));
        restored.extend(
            records
                .into_iter()
                .map(|r| String::from_utf8(r.payload.to_vec()).expect("utf8")),
        );
    }
    let before: Vec<_> = acked
        .iter()
        .filter(|ack| ack.at_millis < point.taken_at_millis)
        .collect();
    assert!(
        before.len() > 50,
        "too few acknowledgements: {}",
        before.len()
    );
    for ack in before {
        assert!(
            restored.contains(&ack.payload),
            "{} was acknowledged before the barrier and is not in the restored copy",
            ack.payload
        );
    }
    cluster.shutdown().await;
}

fn shard_key(cluster: &Cluster, shard: u32) -> ShardKey {
    ShardKey {
        tenant: cluster.tenant_id.clone(),
        namespace: cluster.namespace.clone(),
        stream: STREAM.to_string(),
        shard,
    }
}

/// Copy a shard directory a broker is writing to, the way the runbook says:
/// the small files first, then the segments oldest first, so no file claims
/// more than the segments copied after it hold.
fn copy_live_shard(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the copy");
    let mut names: Vec<String> = std::fs::read_dir(from)
        .expect("list the shard")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .into_string()
                .expect("name")
        })
        .collect();
    let is_segment = |name: &str| name.ends_with(".log") || name.ends_with(".index");
    names.sort_by_key(|name| (is_segment(name), name.clone()));
    for name in names {
        // A temporary a rename is about to replace can vanish mid-listing.
        let _ = std::fs::copy(from.join(&name), to.join(&name));
    }
}
