//! What the Raft log adapter adds to an append and to a replication read.
//! The timed measurements are ignored by default; the allocation bound on a
//! batched append is not.
//!
//! ```text
//! cargo test -p felix-storage --release --test raft_log_cost -- --ignored --nocapture
//! ```
//!
//! Allocation counts do not depend on the machine. Times do.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{
    AppendRecord, Epoch, FsyncMode, LogConfig, LogRecord, RecordMark, WrittenBy,
};
use felix_storage::raft_log::{EntryCodec, RaftLog};
use openraft::entry::{Entry, EntryPayload, RaftEntry};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

/// A command that is its payload, as a published record is.
#[derive(Debug, Clone, PartialEq)]
struct Blob(Bytes);

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

impl Serialize for Blob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u8>::deserialize(deserializer).map(|bytes| Self(Bytes::from(bytes)))
    }
}

openraft::declare_raft_types!(
    Types:
        D = Blob,
        R = (),
        NodeId = u64,
        Node = openraft::impls::EmptyNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);

type Membership = openraft::impls::Membership<u64, openraft::impls::EmptyNode>;
type BlobEntry = Entry<CommittedLeaderIdOf<Types>, EntryPayload<Blob, Membership>>;

#[derive(Clone, Copy)]
struct Codec;

impl EntryCodec<Types> for Codec {
    fn encode_data(&self, data: &Blob) -> io::Result<AppendRecord> {
        Ok(AppendRecord {
            payload: data.0.clone(),
            timestamp_micros: 0,
            mark: RecordMark::None,
            publisher: None,
            headers: None,
        })
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<Blob> {
        Ok(Blob(record.payload))
    }

    fn encode_membership(&self, _: &Membership) -> io::Result<Bytes> {
        Err(io::Error::other("no membership here"))
    }

    fn decode_membership(&self, _: &[u8]) -> io::Result<Membership> {
        Err(io::Error::other("no membership here"))
    }

    fn encode_node_id(&self, node_id: &u64) -> String {
        node_id.to_string()
    }

    fn decode_node_id(&self, leader: &str) -> io::Result<u64> {
        leader.parse().map_err(io::Error::other)
    }
}

const PAYLOAD_BYTES: usize = 1024;

fn open(dir: &tempfile::TempDir, fsync_mode: FsyncMode) -> RaftLog<Types, Codec> {
    let config = LogConfig {
        fsync_mode,
        raft_log: true,
        ..LogConfig::default()
    };
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config).expect("open");
    RaftLog::new(log, Codec).expect("a Raft log")
}

fn entry(index: u64, payload: &Bytes) -> BlobEntry {
    let id = LogIdOf::<Types>::new(1.into(), index);
    BlobEntry::new_normal(id, Blob(payload.clone()))
}

fn report(what: &str, count: u64, started: Instant, allocations: u64) {
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
    println!(
        "{what}: {:.2} us and {:.1} allocations each, over {count}",
        started.elapsed().as_secs_f64() * 1e6 / count as f64,
        allocations as f64 / count as f64,
    );
}

/// One entry per append, each callback awaited before the next append, so
/// each append pays a flush of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement"]
async fn append_one_at_a_time() {
    const APPENDS: u64 = 5_000;
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir, FsyncMode::OnCommit);
    let payload = Bytes::from(vec![7u8; PAYLOAD_BYTES]);

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    for index in 0..APPENDS {
        let (tx, rx) = Types::oneshot();
        raft.append([entry(index, &payload)], IOFlushed::signal(tx))
            .await
            .expect("append");
        rx.await.expect("callback").expect("flush");
    }
    report("append, one at a time", APPENDS, started, allocations);
    println!("  {} flushes", raft.disk_log().flushes());
}

/// Appends issued back to back on a log that fsyncs on commit, as openraft
/// issues them, with the callbacks awaited at the end. Group commit shares
/// the flushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement"]
async fn append_back_to_back_with_fsync() {
    const APPENDS: u64 = 20_000;
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir, FsyncMode::OnCommit);
    let payload = Bytes::from(vec![7u8; PAYLOAD_BYTES]);

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    let mut waiting = Vec::with_capacity(APPENDS as usize);
    for index in 0..APPENDS {
        let (tx, rx) = Types::oneshot();
        raft.append([entry(index, &payload)], IOFlushed::signal(tx))
            .await
            .expect("append");
        waiting.push(rx);
    }
    for flushed in waiting {
        flushed.await.expect("callback").expect("flush");
    }
    report("append, fsync on commit", APPENDS, started, allocations);
    println!("  {} flushes", raft.disk_log().flushes());
}

/// What replication does behind an append: read the newest 64 entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement"]
async fn read_the_newest_entries() {
    const HELD: u64 = 2_000;
    const READS: u64 = 20_000;
    const PER_READ: u64 = 64;
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir, FsyncMode::OnCommit);
    let payload = Bytes::from(vec![7u8; PAYLOAD_BYTES]);
    for first in (0..HELD).step_by(100) {
        let (tx, rx) = Types::oneshot();
        let entries: Vec<_> = (first..first + 100).map(|i| entry(i, &payload)).collect();
        raft.append(entries, IOFlushed::signal(tx))
            .await
            .expect("append");
        rx.await.expect("callback").expect("flush");
    }

    let mut reader = raft.reader();
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    for read in 0..READS {
        let start = HELD - PER_READ - read % 512;
        let entries = reader
            .limited_get_log_entries(start, start + PER_READ)
            .await
            .expect("read");
        assert_eq!(entries.len() as u64, PER_READ);
    }
    report("read of 64 entries", READS, started, allocations);
}

/// Allocations and bytes allocated per record over `records` records since
/// `(allocations, bytes)` was read.
fn per_record(since: (u64, u64), records: u64) -> (f64, f64) {
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - since.0;
    let bytes = ALLOCATED_BYTES.load(Ordering::Relaxed) - since.1;
    (
        allocations as f64 / records as f64,
        bytes as f64 / records as f64,
    )
}

fn counters() -> (u64, u64) {
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    )
}

/// What a batched append costs in allocations per record, through a Raft
/// log and through a plain log storing a leader's batch at its offsets, as
/// a hybrid follower does. Both run in this one test so no other test's
/// allocations land in the counts.
///
/// The bounds hold the per-record work to what the record itself needs: the
/// log copies no record list per append, and a batch's vectors are sized
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batched_append_allocates_little_per_record() {
    const PER_APPEND: u64 = 64;
    const WARM: u64 = 8;
    const APPENDS: u64 = 200;
    let payload = Bytes::from(vec![7u8; PAYLOAD_BYTES]);
    let records = APPENDS * PER_APPEND;

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir, FsyncMode::OnCommit);
    let mut since = counters();
    for append in 0..WARM + APPENDS {
        if append == WARM {
            since = counters();
        }
        let first = append * PER_APPEND;
        let entries: Vec<_> = (first..first + PER_APPEND)
            .map(|i| entry(i, &payload))
            .collect();
        let (tx, rx) = Types::oneshot();
        raft.append(entries, IOFlushed::signal(tx))
            .await
            .expect("append");
        rx.await.expect("callback").expect("flush");
    }
    let (raft_allocations, raft_bytes) = per_record(since, records);

    let dir = tempfile::tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        ..LogConfig::default()
    };
    let log = DiskLog::open(dir.path(), "t/ns/plain/0", config).expect("open");
    let mut since = counters();
    for append in 0..WARM + APPENDS {
        if append == WARM {
            since = counters();
        }
        let first = append * PER_APPEND;
        let batch: Vec<_> = (0..PER_APPEND)
            .map(|_| AppendRecord {
                payload: payload.clone(),
                timestamp_micros: 0,
                mark: RecordMark::None,
                publisher: None,
                headers: None,
            })
            .collect();
        let writers = WrittenBy::Writers(vec![Epoch {
            generation: 1,
            start_offset: first,
        }]);
        log.append_pending_at(first, batch, writers)
            .await
            .expect("append")
            .expect("at the tail");
    }
    let (copy_allocations, copy_bytes) = per_record(since, records);

    println!(
        "raft append: {raft_allocations:.2} allocations, {raft_bytes:.0} bytes per record; \
         copy append: {copy_allocations:.2} allocations, {copy_bytes:.0} bytes per record"
    );
    // Measured at 0.21 allocations and 313 bytes for the Raft log, 0.09 and
    // 160 for the copy. The allocation count is judged against the copy
    // baseline from this same run, since the flush path differs by platform.
    // A copy of the record list per append adds about 110 bytes a record and
    // 0.04 allocations; bytes are what tells it apart.
    assert!(
        raft_allocations < copy_allocations + 0.25 && raft_bytes < 400.0,
        "a Raft append allocates {raft_allocations:.2} times and {raft_bytes:.0} bytes a record"
    );
    assert!(
        copy_allocations < 0.12 && copy_bytes < 220.0,
        "a copy append allocates {copy_allocations:.2} times and {copy_bytes:.0} bytes a record"
    );
}
