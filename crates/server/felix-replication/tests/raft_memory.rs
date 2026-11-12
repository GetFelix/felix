//! What a Raft member adds to a shard's memory, measured against the
//! design's estimate (`docs/per-shard-raft.md`, "The groups-per-broker
//! gate"). Its own test binary: it counts every allocation in the process.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use felix_replication::raft::{
    GroupId, GroupRuntime, MemberId, RaftReply, RaftRequest, RaftTransport, Refused, Role,
    RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, RecordMark};

/// Bytes allocated and not yet freed, in allocations under [`RESERVATION`].
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// The same in allocations of [`RESERVATION`] or more.
static RESERVED: AtomicUsize = AtomicUsize::new(0);
/// An allocation this large is a buffer set aside ahead of use: openraft
/// reserves room for 4,096 commands per member when it starts. The pages
/// of one are not resident until they are written, and an idle member does
/// not write them, so they are counted apart from what the gate measures.
const RESERVATION: usize = 128 * 1024;

fn counter(size: usize) -> &'static AtomicUsize {
    if size >= RESERVATION {
        &RESERVED
    } else {
        &LIVE
    }
}

struct Counting;

// SAFETY: every call goes to `System` unchanged; this only counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        counter(layout.size()).fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        counter(layout.size()).fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        counter(new_size).fetch_add(new_size, Ordering::Relaxed);
        counter(layout.size()).fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const BROKERS: [&str; 3] = ["a", "b", "c"];
const GROUPS: u32 = 40;
/// The design measured 180 KB for a leader and 145 KB for a follower, so
/// about 157 KB a member with a third of them leading. The bound leaves room
/// for a debug build. A member that costs more than this breaks the
/// 5,000-member gate, and the design has to hear about it.
const MEMBER_BUDGET: usize = 256 * 1024;

#[derive(Default)]
struct Net {
    brokers: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let runtime = self.brokers.lock().get(&request.to().broker).cloned();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_member_stays_within_the_designs_memory() {
    let root = tempfile::tempdir().expect("dir");
    let net = Arc::new(Net::default());
    for broker in BROKERS {
        let transport: Arc<dyn RaftTransport> = net.clone();
        let runtime =
            GroupRuntime::start(broker, RuntimeConfig::default(), transport).expect("timers");
        net.brokers.lock().insert(broker.into(), runtime);
    }
    let runtime = |broker: &str| net.brokers.lock()[broker].clone();

    // The logs first: a shard has one with or without Raft, so it is not
    // part of what Raft adds.
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let mut logs = Vec::new();
    for broker in BROKERS {
        for n in 0..GROUPS {
            let dir = root.path().join(broker).join(n.to_string());
            std::fs::create_dir_all(&dir).expect("dir");
            let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
            logs.push((broker, n, log));
        }
    }
    // What a log allocates once it is written to, with or without Raft:
    // measured on plain logs that take two records each, as a new member's
    // log does, and taken off below.
    let plain = LogConfig {
        raft_log: false,
        ..config.clone()
    };
    let mut written = Vec::new();
    for n in 0..GROUPS {
        let dir = root.path().join("plain").join(n.to_string());
        std::fs::create_dir_all(&dir).expect("dir");
        written.push(DiskLog::open(dir, format!("t/ns/plain/{n}"), plain.clone()).expect("log"));
    }
    let unwritten = LIVE.load(Ordering::Relaxed);
    for log in &written {
        for _ in 0..2 {
            let record = AppendRecord {
                payload: Bytes::from_static(b"record"),
                timestamp_micros: 1_700_000_000,
                mark: RecordMark::None,
                publisher: None,
                headers: None,
            };
            log.append(&[record]).await.expect("append");
        }
    }
    let a_written_log = (LIVE.load(Ordering::Relaxed) - unwritten) / written.len();

    let before = LIVE.load(Ordering::Relaxed);
    let reserved_before = RESERVED.load(Ordering::Relaxed);

    let members: BTreeSet<_> = BROKERS.iter().map(|b| MemberId::new(*b, 1)).collect();
    for (broker, n, log) in &logs {
        let group = GroupId {
            shard: shard(*n),
            epoch: 0,
        };
        runtime(broker)
            .open(group, 1, log.clone())
            .await
            .expect("open");
    }

    // Led, with every member following that leader and holding its first
    // entry.
    let statuses = |n: u32| -> Vec<_> {
        BROKERS
            .iter()
            .filter_map(|broker| runtime(broker).status(&shard(n)))
            .collect()
    };
    let settled = |n: u32| {
        let statuses = statuses(n);
        let leader = statuses.iter().find(|status| status.role == Role::Leader);
        leader.is_some_and(|leader| {
            statuses.iter().all(|status| {
                status.leader.as_ref() == Some(&leader.member)
                    && status.committed == leader.last_index
            })
        })
    };
    // One group at a time, each led by the member told first, so leaders
    // spread over the brokers and no group waits out a split vote. Forming
    // them all at once is a storm of vote fsyncs, which is not what this
    // measures.
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
                "group {n} did not settle: {:#?}",
                statuses(n)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let leading = |broker: &str| {
        (0..GROUPS)
            .filter(|n| {
                runtime(broker).status(&shard(*n)).map(|status| status.role) == Some(Role::Leader)
            })
            .count()
    };
    let leaders: Vec<_> = BROKERS.iter().map(|broker| leading(broker)).collect();
    // A few heartbeats of doing nothing.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = LIVE.load(Ordering::Relaxed);

    let members = BROKERS.len() * GROUPS as usize;
    let with_log = after.saturating_sub(before) / members;
    let each = with_log.saturating_sub(a_written_log);
    let reserved = RESERVED
        .load(Ordering::Relaxed)
        .saturating_sub(reserved_before)
        / members;
    println!(
        "{members} idle members, leaders per broker {leaders:?}: {} KB each, after {} KB for a \
         written log, and {} KB each reserved in large buffers that are not counted",
        each / 1024,
        a_written_log / 1024,
        reserved / 1024
    );
    assert!(
        each <= MEMBER_BUDGET,
        "a member costs {each} bytes, over the {MEMBER_BUDGET} budget"
    );

    for broker in BROKERS {
        runtime(broker).shutdown().await;
    }
}
