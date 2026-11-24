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
use felix_storage::log::{AppendRecord, FsyncMode, LogConfig, LogRecord, RecordMark};
use felix_storage::raft_log::{EntryCodec, RaftLog};
use felix_storage::{DiskLog, StorageError};
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
    append_all(raft, vec![index]).await
}

/// Append `indexes` as one batch and wait for the callback.
async fn append_all(raft: &mut RaftLog<Types, Codec>, indexes: Vec<u64>) -> io::Result<()> {
    let entries: Vec<BlobEntry> = indexes.into_iter().map(entry).collect();
    let (tx, rx) = Types::oneshot();
    raft.append(entries, IOFlushed::signal(tx)).await?;
    rx.await.expect("the callback was dropped")
}

/// Close the log and open it again from what is on disk, checking the open
/// repaired nothing: a retried write left no torn bytes behind.
async fn reopen(dir: &tempfile::TempDir, raft: RaftLog<Types, Codec>) -> RaftLog<Types, Codec> {
    let before = segment_sizes(dir.path());
    raft.disk_log().close().await.expect("close");
    drop(raft);
    let reopened = open(dir);
    assert_eq!(
        segment_sizes(dir.path()),
        before,
        "the reopened log cut bytes off its segments"
    );
    reopened
}

fn segment_sizes(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut sizes = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            sizes.extend(segment_sizes(&path));
        } else if path.extension().is_some_and(|ext| ext == "log") {
            let len = std::fs::metadata(&path).expect("metadata").len();
            sizes.push((path, len));
        }
    }
    sizes.sort();
    sizes
}

/// Every entry's index and payload, as the log reads them.
async fn contents(raft: &mut RaftLog<Types, Codec>) -> Vec<(u64, Vec<u8>)> {
    let mut reader = raft.get_log_reader().await;
    reader
        .try_get_log_entries(0..100)
        .await
        .expect("read")
        .into_iter()
        .map(|entry| {
            let index = entry.index();
            match entry.payload {
                EntryPayload::Normal(Blob(bytes)) => (index, bytes),
                other => panic!("entry {index} is not a normal entry: {other:?}"),
            }
        })
        .collect()
}

fn expected(indexes: impl IntoIterator<Item = u64>) -> Vec<(u64, Vec<u8>)> {
    indexes
        .into_iter()
        .map(|index| (index, format!("entry-{index}").into_bytes()))
        .collect()
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

    let mut raft = reopen(&dir, raft).await;
    assert_eq!(contents(&mut raft).await, expected(1..=3));
}

/// **A batch refused part way through is written whole, once.** The fault
/// lands half the batch, so the first record is complete on disk when the
/// write fails; the rewind must take it off again or the retry duplicates
/// it.
#[tokio::test]
async fn a_batch_refused_after_its_first_record_lands_once() {
    let _serial = SERIAL.lock().await;
    set_faults("");
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append(&mut raft, 1)
        .await
        .expect("append on a healthy disk");

    set_faults("write=enospc\n");
    let held = tokio::spawn(async move {
        let appended = append_all(&mut raft, vec![2, 3, 4]).await;
        (raft, appended)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!held.is_finished(), "a refused batch was not held");

    set_faults("");
    let (mut raft, appended) = tokio::time::timeout(Duration::from_secs(10), held)
        .await
        .expect("the held batch finished once the disk took it")
        .expect("task");
    appended.expect("the held batch is written once the disk takes it");
    append(&mut raft, 5)
        .await
        .expect("the log goes on taking appends");
    assert_eq!(contents(&mut raft).await, expected(1..=5));

    let mut raft = reopen(&dir, raft).await;
    assert_eq!(contents(&mut raft).await, expected(1..=5));
}

/// **A member being stopped does not wait out a full disk.** Its stop waits
/// for the held append, so the append gives up with the disk's error.
#[tokio::test]
async fn a_stop_ends_a_held_write_at_once() {
    let _serial = SERIAL.lock().await;
    set_faults("");
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append(&mut raft, 1)
        .await
        .expect("append on a healthy disk");

    set_faults("write=enospc\n");
    let stopper = raft.clone();
    let held = tokio::spawn(async move { append(&mut raft, 2).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!held.is_finished(), "a refused write was not held");

    stopper.stop_retrying();
    let appended = tokio::time::timeout(Duration::from_secs(5), held)
        .await
        .expect("a stop ends the held write promptly")
        .expect("task");
    set_faults("");
    let err = appended.expect_err("a write the disk refused was reported written");
    let full = err
        .get_ref()
        .and_then(|err| err.downcast_ref::<StorageError>())
        .is_some_and(|err| matches!(err, StorageError::Full(_)));
    assert!(
        full,
        "the held write failed with something other than the disk's error: {err}"
    );
}

/// **A write whose rewind fails is not retried.** The segment's shape on
/// disk is unknown then, so the log is poisoned and the append fails with
/// the rewind's error.
#[tokio::test]
async fn a_write_whose_rewind_fails_stops_the_log() {
    let _serial = SERIAL.lock().await;
    set_faults("");
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append(&mut raft, 1)
        .await
        .expect("append on a healthy disk");

    set_faults("write=enospc\nrewind=fail\n");
    let appended = tokio::time::timeout(Duration::from_secs(5), append(&mut raft, 2))
        .await
        .expect("a write whose rewind failed answers at once");
    set_faults("");
    let err = appended.expect_err("a write whose rewind failed was reported written");
    assert!(
        err.to_string().contains("could not be rewound"),
        "the append failed with something other than the rewind: {err}"
    );
    assert!(
        raft.disk_log().is_poisoned(),
        "a failed rewind left the log sound"
    );
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
