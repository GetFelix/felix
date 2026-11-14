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
use felix_storage::DiskLog;
use felix_storage::log::{
    AppendOnlyLog, AppendRecord, FDS_PER_SHARD_LOG, FsyncMode, LogConfig, RecordMark,
};

const BROKERS: [&str; 3] = ["a", "b", "c"];
const GROUPS: u32 = 40;

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
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };

    // A shard's log without Raft, written to, for comparison.
    let start = open_files();
    let plain = LogConfig {
        raft_log: false,
        ..config.clone()
    };
    let mut plain_logs = Vec::new();
    for n in 0..GROUPS {
        let dir = root.path().join("plain").join(n.to_string());
        std::fs::create_dir_all(&dir).expect("dir");
        let log = DiskLog::open(dir, format!("t/ns/plain/{n}"), plain.clone()).expect("log");
        log.append(&[record()]).await.expect("append");
        plain_logs.push(log);
    }
    let a_plain_log = (open_files() - start) as f64 / f64::from(GROUPS);

    let before = open_files();
    let mut logs = Vec::new();
    for broker in BROKERS {
        for n in 0..GROUPS {
            let dir = root.path().join(broker).join(n.to_string());
            std::fs::create_dir_all(&dir).expect("dir");
            let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
            logs.push((broker, n, log));
        }
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
    let each = (open_files() - before) as f64 / count as f64;
    println!(
        "{count} idle members: {each:.2} open files each, against {a_plain_log:.2} for a \
         written log without Raft"
    );
    assert!(
        each <= FDS_PER_SHARD_LOG as f64,
        "a member holds {each:.2} files open, over the {FDS_PER_SHARD_LOG} the broker's \
         open-file check assumes"
    );

    for broker in BROKERS {
        runtime(broker).shutdown().await;
    }
}
