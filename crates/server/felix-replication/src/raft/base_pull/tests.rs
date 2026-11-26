use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::raft_copy::{EpochBase, EpochLogId};
use felix_storage::raft_log::RaftLog;
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf};

use super::*;
use crate::raft::types::Command;

fn log_id(term: u64, index: u64) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes> {
        leader_id: CommittedLeaderIdOf::<ShardTypes> { term },
        index,
    }
}

fn record(index: u64, payload: &'static [u8]) -> EntryOf<ShardTypes> {
    EntryOf::<ShardTypes>::new_normal(
        log_id(2, index),
        Command {
            timestamp_micros: 7,
            payload: Bytes::from_static(payload),
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
            cond: None,
        },
    )
}

/// A log holding a blank at 0 and records 1..=9, all in term 2.
async fn log(dir: &std::path::Path) -> RaftLog<ShardTypes, ShardCodec> {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let disk = DiskLog::open(dir, "t/ns/orders/0".to_string(), config).expect("open");
    let mut raft = RaftLog::new(disk, ShardCodec).expect("raft log");
    let mut entries = vec![EntryOf::<ShardTypes>::new_blank(log_id(2, 0))];
    entries.extend((1..10).map(|index| record(index, b"a record of some bytes")));
    let (tx, rx) = ShardTypes::oneshot();
    raft.append(entries, IOFlushed::signal(tx))
        .await
        .expect("append");
    rx.await.expect("callback").expect("flush");
    raft
}

/// A base ending at 7 in term 2, with the epoch starting at 20.
fn base() -> Option<EpochBase> {
    Some(EpochBase {
        base_last: Some(EpochLogId {
            index: 7,
            epoch: 0,
            term: 2,
        }),
        first_index: 20,
    })
}

fn indexes(chunk: &BaseChunk) -> Vec<u64> {
    chunk
        .entries
        .iter()
        .map(|entry| entry.log_id.index)
        .collect()
}

#[tokio::test]
async fn a_chunk_stops_at_the_base_and_at_its_count() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = log(dir.path()).await;
    let mut reader = raft.reader();
    let chunk = serve(&mut reader, |_| 0, base(), 2, u32::MAX, u32::MAX)
        .await
        .expect("served");
    // Entries 8 and 9 are in the log but past the base: never served.
    assert_eq!(indexes(&chunk), (2..=7).collect::<Vec<_>>());
    assert_eq!(chunk.floor, 0);
    let chunk = serve(&mut reader, |_| 0, base(), 1, u32::MAX, 3)
        .await
        .expect("served");
    assert_eq!(indexes(&chunk), vec![1, 2, 3]);
    // A count of 0 still moves.
    let chunk = serve(&mut reader, |_| 0, base(), 4, u32::MAX, 0)
        .await
        .expect("served");
    assert_eq!(indexes(&chunk), vec![4]);
}

#[tokio::test]
async fn a_chunk_stops_at_its_bytes_but_carries_its_first_entry() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = log(dir.path()).await;
    let mut reader = raft.reader();
    let one = entry_bytes(&record(1, b"a record of some bytes")) as u32;
    let chunk = serve(&mut reader, |_| 0, base(), 1, 1, u32::MAX)
        .await
        .expect("served");
    assert_eq!(indexes(&chunk), vec![1]);
    let chunk = serve(&mut reader, |_| 0, base(), 1, 2 * one + 1, u32::MAX)
        .await
        .expect("served");
    assert_eq!(indexes(&chunk), vec![1, 2]);
}

#[tokio::test]
async fn a_pull_past_the_base_or_without_one_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = log(dir.path()).await;
    let mut reader = raft.reader();
    let past = serve(&mut reader, |_| 0, base(), 8, u32::MAX, u32::MAX).await;
    assert!(past.expect_err("refused").contains("past the base"));
    let none = serve(&mut reader, |_| 0, None, 1, u32::MAX, u32::MAX).await;
    assert!(none.expect_err("refused").contains("no re-form base"));
    let empty = Some(EpochBase {
        base_last: None,
        first_index: 20,
    });
    let empty = serve(&mut reader, |_| 0, empty, 0, u32::MAX, u32::MAX).await;
    assert!(empty.expect_err("refused").contains("no re-form base"));
}

/// Below the voter's first index the chunk is empty and names that index,
/// so the learner restarts there instead of taking a hole.
#[tokio::test]
async fn a_pull_below_the_first_index_names_it() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = log(dir.path()).await;
    let mut reader = raft.reader();
    let chunk = serve(&mut reader, |_| 4, base(), 2, u32::MAX, u32::MAX)
        .await
        .expect("served");
    assert_eq!(chunk.floor, 4);
    assert!(chunk.entries.is_empty());
}

/// A purge or trim that raises the floor under the read answers an empty
/// chunk at the new floor, not entries the voter may have cut.
#[tokio::test]
async fn a_floor_that_moves_under_the_read_restarts_the_pull() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = log(dir.path()).await;
    let mut reader = raft.reader();
    let reads = std::cell::Cell::new(0);
    let floor = |at: u64| {
        // The floor is the one in force at the base.
        assert_eq!(at, 7);
        reads.set(reads.get() + 1);
        if reads.get() == 1 { 0 } else { 5 }
    };
    let chunk = serve(&mut reader, floor, base(), 2, u32::MAX, u32::MAX)
        .await
        .expect("served");
    assert_eq!(chunk.floor, 5);
    assert!(chunk.entries.is_empty());
    // A floor at or below the start does not stop the chunk.
    let chunk = serve(&mut reader, |_| 2, base(), 2, u32::MAX, u32::MAX)
        .await
        .expect("served");
    assert_eq!(chunk.floor, 2);
    assert_eq!(indexes(&chunk), (2..=7).collect::<Vec<_>>());
}
