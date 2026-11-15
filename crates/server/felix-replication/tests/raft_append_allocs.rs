//! A measurement, not a test: what a write costs in allocations per record
//! through a Raft group's propose path and its log, on one member that leads
//! alone. Ignored by default. Its own test binary: it counts every
//! allocation in the process.
//!
//! ```text
//! cargo test -p felix-replication --release --test raft_append_allocs -- --ignored --nocapture
//! ```
//!
//! The counts move a little from run to run, because how many writes one
//! drain takes depends on timing. The bound on the log's share is in
//! `felix-storage`'s `raft_log_cost`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_replication::raft::{
    AckAt, AgreeingPeers, Batch, GroupId, GroupRuntime, MemberId, RaftEngine, Record, Role,
    RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every call goes to the system allocator unchanged. A realloc goes
// through `alloc`, so a growing Vec counts once per growth.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const PER_WRITE: usize = 64;

fn shard() -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn batch(payload: &Bytes) -> Batch {
    Batch {
        records: (0..PER_WRITE)
            .map(|_| Record {
                timestamp_micros: 1_700_000_000,
                payload: payload.clone(),
                headers: None,
            })
            .collect(),
        ack: AckAt::Written,
        term: None,
        producer: None,
        expected: None,
        trim: None,
        publisher: None,
    }
}

/// Write `writes` batches, `in_flight` at a time.
async fn write(engine: &RaftEngine, writes: usize, in_flight: usize, payload: &Bytes) {
    let mut flying: FuturesUnordered<_> = (0..in_flight.min(writes))
        .map(|_| engine.propose(&shard(), batch(payload)))
        .collect();
    let mut started = flying.len();
    while let Some(written) = flying.next().await {
        written.expect("written");
        if started < writes {
            flying.push(engine.propose(&shard(), batch(payload)));
            started += 1;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement"]
async fn a_pipelined_write_through_propose() {
    const WARM: usize = 2_000;
    const WRITES: usize = 10_000;
    let dir = tempfile::tempdir().expect("dir");
    let runtime =
        GroupRuntime::start("a", RuntimeConfig::default(), Arc::new(AgreeingPeers)).expect("start");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        raft_log: true,
        ..LogConfig::default()
    };
    let log = DiskLog::open(dir.path(), "t/ns/orders/0", config).expect("log");
    runtime
        .open(GroupId::new(shard(), 0), 1, log)
        .await
        .expect("open");
    let members: BTreeSet<_> = [MemberId::new("a", 1)].into();
    runtime
        .initialize(&shard(), members)
        .await
        .expect("initialize");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while runtime.status(&shard()).map(|status| status.role) != Some(Role::Leader) {
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let engine = RaftEngine::new(runtime.clone());
    let payload = Bytes::from(vec![7u8; 100]);
    write(&engine, WARM, 8, &payload).await;
    // One write at a time is one drain and one append of each write's
    // records, so its count is steady. Writes in flight share drains.
    for in_flight in [1, 8] {
        let (allocations, bytes) = (
            ALLOCATIONS.load(Ordering::Relaxed),
            ALLOCATED_BYTES.load(Ordering::Relaxed),
        );
        write(&engine, WRITES, in_flight, &payload).await;
        let records = (WRITES * PER_WRITE) as f64;
        println!(
            "propose, {PER_WRITE} records a write, {in_flight} in flight: \
             {:.2} allocations and {:.0} bytes a record",
            (ALLOCATIONS.load(Ordering::Relaxed) - allocations) as f64 / records,
            (ALLOCATED_BYTES.load(Ordering::Relaxed) - bytes) as f64 / records,
        );
    }
    runtime.shutdown().await;
}
