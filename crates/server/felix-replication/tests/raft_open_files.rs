//! The file descriptors a Raft member keeps open, against the budget the
//! broker's open-file check assumes (`docs/per-shard-raft.md`, "Open
//! files"). Its own test binary: it counts every descriptor in the process.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use felix_replication::raft::{
    GroupId, GroupRuntime, MemberId, RaftReply, RaftRequest, RaftTransport, Refused, Role,
    RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::log::{
    AppendOnlyLog, AppendRecord, FDS_PER_SHARD_LOG, FsyncMode, LogConfig, RecordMark,
    ShardKey as LogShard,
};
use felix_storage::{DiskLogProvider, log::ReadRange};

const BROKERS: [&str; 3] = ["a", "b", "c"];
const GROUPS: u32 = 40;
/// Sealed segments the provider may keep open, for the whole root.
const SEALED_OPEN: usize = 16;
/// Open files beyond the per-shard budget that a few files in flight may add.
const IN_FLIGHT: usize = 8;

#[derive(Default)]
struct Net {
    brokers: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let runtime = self.brokers.lock().get(&*request.to().broker).cloned();
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable("no such broker".into())),
        }
    }
}

fn shard(n: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: n,
        kind: ShardKind::Stream,
    }
}

/// Descriptors open in this process.
fn open_files() -> usize {
    let dir = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    // The listing holds one itself.
    std::fs::read_dir(dir).expect("fd dir").count() - 1
}

fn log_shard(root: &str, n: u32) -> LogShard {
    LogShard {
        tenant: "t".into(),
        namespace: root.into(),
        stream: "orders".into(),
        shard: n,
    }
}

fn record() -> AppendRecord {
    AppendRecord {
        payload: Bytes::from_static(b"record"),
        timestamp_micros: 1_700_000_000,
        mark: RecordMark::None,
        publisher: None,
        headers: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_stays_within_the_open_file_budget() {
    let root = tempfile::tempdir().expect("dir");
    let net = Arc::new(Net::default());
    for broker in BROKERS {
        let transport: Arc<dyn RaftTransport> = net.clone();
        let runtime =
            GroupRuntime::start(broker, RuntimeConfig::default(), transport).expect("timers");
        net.brokers.lock().insert(broker.into(), runtime);
    }
    let runtime = |broker: &str| net.brokers.lock()[broker].clone();
    let mut providers = Vec::new();
    // Segments too big to roll: an idle member has nothing sealed, so any
    // descriptor past the per-shard budget is a member's own.
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        max_open_sealed_segments: SEALED_OPEN,
        ..LogConfig::default()
    };

    // A shard's log without Raft, written to and read back across many
    // sealed segments, for comparison. One provider, as the broker has.
    let start = open_files();
    // Small segments, so a few records roll several of them.
    let plain = LogConfig {
        raft_log: false,
        segment_size_bytes: 1024,
        ..config.clone()
    };
    let plain_provider =
        DiskLogProvider::new(root.path().join("plain"), plain).expect("plain provider");
    let mut plain_logs = Vec::new();
    for n in 0..GROUPS {
        let log = plain_provider
            .open_shard(&log_shard("plain", n))
            .expect("log");
        for _ in 0..200 {
            log.append(&[record()]).await.expect("append");
        }
        // Rollover runs in the background.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while log.segments().len() <= 3 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the log should have rolled"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        log.read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read back");
        plain_logs.push(log);
    }
    let plain_files = open_files() - start;
    // The shared sealed handles are a fixed amount, not a cost per log.
    let a_plain_log = plain_files.saturating_sub(SEALED_OPEN) as f64 / f64::from(GROUPS);
    assert!(
        plain_files <= FDS_PER_SHARD_LOG * GROUPS as usize + SEALED_OPEN + IN_FLIGHT,
        "{GROUPS} written logs hold {plain_files} files open, over {FDS_PER_SHARD_LOG} each \
         plus the {SEALED_OPEN} sealed segments one root may keep open"
    );

    let before = open_files();
    let mut logs = Vec::new();
    for broker in BROKERS {
        let provider =
            DiskLogProvider::new(root.path().join(broker), config.clone()).expect("provider");
        for n in 0..GROUPS {
            let log = provider.open_shard(&log_shard(broker, n)).expect("log");
            logs.push((broker, n, log));
        }
        providers.push(provider);
    }
    let members: BTreeSet<_> = BROKERS.iter().map(|b| MemberId::new(*b, 1)).collect();
    for (broker, n, log) in &logs {
        let group = GroupId::new(shard(*n), 0);
        runtime(broker)
            .open(group, 1, log.clone())
            .await
            .expect("open");
    }
    let settled = |n: u32| {
        let statuses: Vec<_> = BROKERS
            .iter()
            .filter_map(|broker| runtime(broker).status(&shard(n)))
            .collect();
        let leader = statuses.iter().find(|status| status.role == Role::Leader);
        leader.is_some_and(|leader| {
            statuses.iter().all(|status| {
                status.leader.as_ref() == Some(&leader.member)
                    && status.committed == leader.last_index
            })
        })
    };
    // One group at a time: see `raft_memory`.
    for n in 0..GROUPS {
        let first = BROKERS[n as usize % BROKERS.len()];
        runtime(first)
            .initialize(&shard(n), members.clone())
            .await
            .expect("initialize");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !settled(n) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "group {n} did not settle"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // Vote and state files are written and closed; give stragglers a moment.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let count = BROKERS.len() * GROUPS as usize;
    let held = open_files() - before;
    let each = held as f64 / count as f64;
    println!(
        "{count} idle members: {each:.2} open files each, against {a_plain_log:.2} for a \
         written log without Raft (not counting its {SEALED_OPEN} shared sealed handles)"
    );
    // Nothing is sealed or read back here, so the slack is only a few files
    // in flight: one extra descriptor per member would blow far past it.
    assert!(
        held <= FDS_PER_SHARD_LOG * count + IN_FLIGHT,
        "a member holds {each:.2} files open, over the {FDS_PER_SHARD_LOG} the broker's \
         open-file check assumes"
    );

    for broker in BROKERS {
        runtime(broker).shutdown().await;
    }
}
