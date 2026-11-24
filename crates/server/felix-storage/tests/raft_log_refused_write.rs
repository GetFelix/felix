//! A write the disk refuses does not stop a Raft log; a failed flush does.
//!
//! openraft stops a member for good on any append error. A refused write
//! leaves nothing on disk, so the log holds the append and tries again; a
//! failed flush leaves the disk in an unknown state, so its error goes to
//! openraft.
//!
//! Its own test binary because the faults go through the process-wide fault
//! file, which no other test may share.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{AppendRecord, FsyncMode, LogConfig, LogRecord, RecordMark};
use felix_storage::raft_log::{EntryCodec, RaftLog};
use openraft::entry::{Entry, EntryPayload, RaftEntry};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Blob(Vec<u8>);

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0.len())
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
            payload: Bytes::from(data.0.clone()),
            timestamp_micros: 0,
            mark: RecordMark::None,
            publisher: None,
            headers: None,
        })
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<Blob> {
        Ok(Blob(record.payload.to_vec()))
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

/// One test at a time: the fault file is the whole process's.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn fault_file() -> &'static Path {
    static FILE: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &FILE
        .get_or_init(|| {
            let dir = tempfile::tempdir().expect("dir");
            let file = dir.path().join("faults");
            std::fs::write(&file, "").expect("fault file");
            // SAFETY: set before any log is opened, under `SERIAL`, and the
            // tests in this binary read no other environment.
            unsafe { std::env::set_var("FELIX_STORAGE_FAULT_FILE", &file) };
            (dir, file)
        })
        .1
}

/// Set the faults and wait out the interval the storage layer reuses its
/// last reading for.
fn set_faults(faults: &str) {
    std::fs::write(fault_file(), faults).expect("fault file");
    std::thread::sleep(Duration::from_millis(150));
}

fn open(dir: &tempfile::TempDir) -> RaftLog<Types, Codec> {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        raft_log: true,
        ..LogConfig::default()
    };
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config).expect("open");
    RaftLog::new(log, Codec).expect("a Raft log")
}

fn entry(index: u64) -> BlobEntry {
    let id = LogIdOf::<Types>::new(1.into(), index);
    BlobEntry::new_normal(id, Blob(format!("entry-{index}").into_bytes()))
}

/// Append and wait for the callback.
async fn append(raft: &mut RaftLog<Types, Codec>, index: u64) -> io::Result<()> {
    let (tx, rx) = Types::oneshot();
    raft.append([entry(index)], IOFlushed::signal(tx)).await?;
    rx.await.expect("the callback was dropped")
}

async fn indexes(raft: &mut RaftLog<Types, Codec>) -> Vec<u64> {
    let mut reader = raft.get_log_reader().await;
    reader
        .try_get_log_entries(0..100)
        .await
        .expect("read")
        .iter()
        .map(|entry| entry.index())
        .collect()
}

/// **A write refused by a full disk waits for the disk, then lands.** The
/// append does not return while the disk is full, so openraft neither hears
/// the entry written nor sees an error that would stop the member.
#[tokio::test]
async fn a_refused_write_is_held_and_lands_once_the_disk_takes_it() {
    let _serial = SERIAL.lock().await;
    set_faults("");
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append(&mut raft, 1)
        .await
        .expect("append on a healthy disk");

    set_faults("write=enospc\n");
    let held = tokio::spawn(async move {
        let appended = append(&mut raft, 2).await;
        (raft, appended)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !held.is_finished(),
        "an append the disk refused returned instead of waiting for the disk"
    );

    set_faults("");
    let (mut raft, appended) = tokio::time::timeout(Duration::from_secs(10), held)
        .await
        .expect("the held append finished once the disk took it")
        .expect("task");
    appended.expect("the held append is written once the disk takes it");
    append(&mut raft, 3)
        .await
        .expect("the log goes on taking appends");
    assert_eq!(indexes(&mut raft).await, [1, 2, 3]);
}

/// **A failed flush is not retried.** The disk's state is unknown after it,
/// so the append's callback fails, the log is stopped, and the next append
/// fails at once instead of waiting.
#[tokio::test]
async fn a_failed_flush_stops_the_log_and_is_not_retried() {
    let _serial = SERIAL.lock().await;
    set_faults("");
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append(&mut raft, 1)
        .await
        .expect("append on a healthy disk");

    set_faults("fsync=fail\n");
    let flushed = tokio::time::timeout(Duration::from_secs(10), append(&mut raft, 2))
        .await
        .expect("a failed flush answers");
    set_faults("");
    assert!(flushed.is_err(), "a failed flush was reported flushed");
    assert!(raft.disk_log().is_poisoned());
    let next = tokio::time::timeout(Duration::from_secs(5), append(&mut raft, 3))
        .await
        .expect("an append to a stopped log fails at once");
    assert!(next.is_err(), "a stopped log took an append");
}
