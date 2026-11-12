//! A measurement, not a test: what the Raft log adapter adds to an append
//! and to a replication read. Ignored by default.
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
use felix_storage::log::{AppendRecord, FsyncMode, LogConfig, LogRecord, RecordMark};
use felix_storage::raft_log::{EntryCodec, RaftLog};
use openraft::entry::{Entry, EntryPayload, RaftEntry};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every call goes to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
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

/// One entry per append, each callback awaited before the next append. The
/// log does not fsync, so this is the adapter and the write, not the disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement"]
async fn append_one_at_a_time_without_fsync() {
    const APPENDS: u64 = 50_000;
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir, FsyncMode::None);
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
    report("append, no fsync", APPENDS, started, allocations);
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
    let mut raft = open(&dir, FsyncMode::None);
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
