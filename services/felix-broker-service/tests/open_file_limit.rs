//! A broker that runs out of file descriptors keeps serving the shards it
//! holds, refuses the one it cannot open, and opens it once files close.
//!
//! The open-file limit is process-wide, so this is its own test binary, and
//! its tests take turns: lowering the limit must not starve another one.
#![cfg(unix)]

use std::fs::File;
use std::sync::Arc;

use bytes::Bytes;
use felix_broker::BrokerError;
use felix_broker_service::shards::lifecycle::{
    DurableShardStore, GenerationStarts, Phase, ShardLifecycle, reconcile,
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

/// The descriptors open in this process. Listed rather than probed up to
/// the soft limit, which may be unlimited; the listing holds one itself.
fn open_descriptors() -> usize {
    std::fs::read_dir("/dev/fd").expect("list /dev/fd").count() - 1
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

/// Held for a whole test: the limit and the descriptors are the process's.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn broker(dir: &std::path::Path) -> Arc<felix_broker::Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir, config).expect("storage");
    let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
        .with_durable_storage(storage);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                shards: 256,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    Arc::new(broker)
}

/// Every step of a shard open, from the stream log through the group logs,
/// the ballot and the generation-start record, runs out of descriptors in
/// turn: one more is free on each round. Each time the shard stays opening
/// (never failed), the shard already held keeps serving, and the open goes
/// through on a later pass once descriptors free up.
#[tokio::test(flavor = "current_thread")]
async fn every_open_step_that_runs_out_of_descriptors_is_retried() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(dir.path()).await;
    let storage = Arc::new(broker.durable_storage().expect("storage").clone());
    let store = DurableShardStore::new(Arc::clone(&storage))
        .with_ballots("broker-a")
        .with_generation_starts(GenerationStarts {
            broker: Arc::clone(&broker),
            fleet: Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        });
    let lifecycle = tokio::sync::Mutex::new(ShardLifecycle::new("broker-a"));

    reconcile(&lifecycle, &store, &assignments(&[0])).await;
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
    let serving = storage
        .open_stream("t1", "ns", "orders", 0)
        .expect("held shard");

    let original = soft_limit();
    for shard in 1..200 {
        let free = (shard - 1) as usize;
        let before = open_descriptors();
        set_soft_limit(&original, (before + free + 64) as u64);
        let mut filler = exhaust();
        filler.truncate(filler.len() - free);

        reconcile(&lifecycle, &store, &assignments(&[0, shard])).await;
        let phase = lifecycle.lock().await.phase(&key(shard));
        assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
        serving
            .append(&[Bytes::from_static(b"still serving")])
            .await
            .expect("append to the held shard");

        drop(filler);
        set_soft_limit(&original, original.rlim_cur);
        if phase == Phase::Active {
            assert!(free > 0, "the first round had descriptors to spare");
            return;
        }
        assert_eq!(
            phase,
            Phase::Opening,
            "{free} free descriptors failed the shard"
        );
        reconcile(&lifecycle, &store, &assignments(&[0, shard])).await;
        assert_eq!(
            lifecycle.lock().await.phase(&key(shard)),
            Phase::Active,
            "{free} free descriptors: the retry did not open the shard"
        );
    }
    panic!("the shard never opened with descriptors to spare");
}

#[tokio::test(flavor = "current_thread")]
async fn a_shard_that_runs_out_of_descriptors_opens_once_they_free_up() {
    let _turn = TURN.lock().await;
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
    let before = open_descriptors();
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
        open_descriptors(),
        before,
        "the failed opens left descriptors open"
    );
    set_soft_limit(&original, original.rlim_cur);

    reconcile(&lifecycle, &store, &assignments(&[0, 1])).await;
    assert_eq!(lifecycle.lock().await.phase(&key(1)), Phase::Active);
    assert_eq!(lifecycle.lock().await.phase(&key(0)), Phase::Active);
}
