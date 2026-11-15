//! A broker that runs out of file descriptors keeps serving the shards it
//! holds, refuses the one it cannot open, and opens it once files close.
//!
//! The open-file limit is process-wide, so this is its own test binary with
//! a single test: lowering the limit cannot starve any other test.
#![cfg(unix)]

use std::fs::File;
use std::sync::Arc;

use bytes::Bytes;
use felix_broker::BrokerError;
use felix_broker_service::shards::lifecycle::{
    DurableShardStore, Phase, ShardLifecycle, reconcile,
};
use felix_broker_service::shards::watch::ShardAssignment;
use felix_broker_service::shards::{ShardKey, ShardKind};
use felix_common::random::HashMap;
use felix_storage::log::{FsyncMode, LogConfig};

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

fn assignments(shards: &[u32]) -> HashMap<ShardKey, ShardAssignment> {
    shards
        .iter()
        .map(|&shard| {
            let assignment = ShardAssignment {
                key: key(shard),
                leader: "broker-a".to_string(),
                replicas: Vec::new(),
                generation: 1,
                state: "active".to_string(),
                successor: None,
                routing: Default::default(),
                incarnation: 0,
                fence_storage_ids: Default::default(),
                member_storage_ids: Default::default(),
                pending_set: None,
                previous_generation: None,
                readmitted: Default::default(),
            };
            (key(shard), assignment)
        })
        .collect()
}

fn soft_limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid `rlimit` for the call to fill.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit
}

fn set_soft_limit(limit: &libc::rlimit, soft: u64) {
    let wanted = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: `wanted` is a valid `rlimit` with the hard limit unchanged.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) }, 0);
}

/// The descriptors open in this process, counted below `ceiling`.
fn open_descriptors(ceiling: u64) -> usize {
    (0..ceiling as libc::c_int)
        // SAFETY: F_GETFD only reads the descriptor's flags.
        .filter(|&fd| unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1)
        .count()
}

/// Open `/dev/null` until the process is out of descriptors.
fn exhaust() -> Vec<File> {
    let mut held = Vec::new();
    loop {
        match File::open("/dev/null") {
            Ok(file) => held.push(file),
            Err(err) => {
                assert!(felix_storage::open_files::is_exhausted(&err), "{err}");
                return held;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_shard_that_runs_out_of_descriptors_opens_once_they_free_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage =
        Arc::new(felix_broker::DurableStorage::open(dir.path(), config).expect("storage"));
    let store = DurableShardStore::new(Arc::clone(&storage));
    let lifecycle = tokio::sync::Mutex::new(ShardLifecycle::new("broker-a"));

    reconcile(&lifecycle, &store, &assignments(&[0])).await;
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
    let serving = storage
        .open_stream("t1", "ns", "orders", 0)
        .expect("held shard");

    let original = soft_limit();
    let before = open_descriptors(original.rlim_cur);
    set_soft_limit(&original, before as u64 + 32);
    let filler = exhaust();

    // The open fails with an error that names the limit, and nothing it
    // opened on the way stays open.
    let Err(refused) = storage.open_stream("t1", "ns", "orders", 2) else {
        panic!("an open with no descriptors left succeeded");
    };
    assert!(
        matches!(refused, BrokerError::OpenFileLimit(_)),
        "{refused}"
    );
    assert!(refused.to_string().contains("open-file limit"), "{refused}");

    // The new shard does not serve and is not given up; the held one serves.
    reconcile(&lifecycle, &store, &assignments(&[0, 1])).await;
    assert_eq!(lifecycle.lock().await.phase(&key(1)), Phase::Opening);
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
    serving
        .append(&[Bytes::from_static(b"still serving")])
        .await
        .expect("append to the held shard");

    drop(filler);
    assert_eq!(
        open_descriptors(original.rlim_cur),
        before,
        "the failed opens left descriptors open"
    );
    set_soft_limit(&original, original.rlim_cur);

    reconcile(&lifecycle, &store, &assignments(&[0, 1])).await;
    assert_eq!(lifecycle.lock().await.phase(&key(1)), Phase::Active);
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
}
