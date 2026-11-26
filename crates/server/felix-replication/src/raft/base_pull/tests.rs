use std::sync::Arc;

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::raft_copy::{EpochBase, EpochLogId};
use felix_storage::raft_log::RaftLog;
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
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

/// A voter as the fake source serves it: its entries, a floor that is
/// `raised` from the `after`th chunk on, and a cap of two entries a chunk.
struct FakeVoter {
    entries: Vec<EntryOf<ShardTypes>>,
    floor: u64,
    raised: Option<(usize, u64)>,
    refuses: bool,
}

struct FakeSource {
    voters: std::collections::HashMap<Arc<str>, FakeVoter>,
    asked: parking_lot::Mutex<Vec<(Arc<str>, u64)>>,
}

#[async_trait]
impl ChunkSource for FakeSource {
    async fn chunk(
        &self,
        voter: &MemberId,
        start: u64,
        _max_bytes: u32,
        _max_count: u32,
    ) -> Result<BaseChunk, String> {
        let asked = {
            let mut asked = self.asked.lock();
            asked.push((Arc::clone(&voter.broker), start));
            asked.iter().filter(|(b, _)| *b == voter.broker).count()
        };
        let fake = self.voters.get(&voter.broker).ok_or("unknown voter")?;
        if fake.refuses {
            return Err("it has not offered base pulls".into());
        }
        let floor = match fake.raised {
            Some((after, floor)) if asked > after => floor,
            _ => fake.floor,
        };
        if floor > start {
            return Ok(BaseChunk {
                floor,
                entries: Vec::new(),
            });
        }
        let entries = fake
            .entries
            .iter()
            .filter(|entry| entry.log_id.index >= start && entry.log_id.index <= 7)
            .take(2)
            .cloned()
            .collect();
        Ok(BaseChunk { floor, entries })
    }
}

fn history(term_at: impl Fn(u64) -> u64) -> Vec<EntryOf<ShardTypes>> {
    (0..10)
        .map(|index| {
            let mut entry = record(index, b"a record of some bytes");
            entry.log_id = log_id(term_at(index), index);
            entry
        })
        .collect()
}

fn voter(name: &str) -> MemberId {
    MemberId::new(name, 1)
}

fn source(voters: Vec<(&str, FakeVoter)>) -> FakeSource {
    FakeSource {
        voters: voters
            .into_iter()
            .map(|(name, fake)| (Arc::from(name), fake))
            .collect(),
        asked: parking_lot::Mutex::new(Vec::new()),
    }
}

fn good() -> FakeVoter {
    FakeVoter {
        entries: history(|_| 2),
        floor: 0,
        raised: None,
        refuses: false,
    }
}

fn quick() -> PullPace {
    PullPace {
        rounds: 2,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    }
}

fn staging_config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

/// The indexes and terms the finished staging log holds.
async fn staged(copy: &std::path::Path) -> Vec<(u64, u64)> {
    let dir = copy.join("base-staging");
    assert!(
        dir.join("base-staged").exists(),
        "the staging log is marked"
    );
    let disk = DiskLog::open(
        &dir,
        "staged".to_string(),
        LogConfig {
            raft_log: true,
            ..staging_config()
        },
    )
    .expect("open staged");
    let raft = RaftLog::new(disk, ShardCodec).expect("raft log");
    let mut reader = raft.reader();
    reader
        .limited_get_log_entries(0, 20)
        .await
        .expect("read")
        .iter()
        .map(|entry| (entry.log_id.index, entry.log_id.leader_id.term))
        .collect()
}

fn base_last() -> EpochLogId {
    base().and_then(|base| base.base_last).expect("base")
}

#[tokio::test]
async fn a_pull_stages_the_whole_base_from_its_floor() {
    let dir = tempfile::tempdir().expect("dir");
    let source = source(vec![("a", good())]);
    let base = base().expect("base");
    let pulled = pull(
        &source,
        &[voter("a")],
        &base,
        1,
        dir.path(),
        &staging_config(),
        quick(),
    )
    .await
    .expect("pull");
    assert_eq!(pulled, Pulled::Staged(base_last()));
    assert_eq!(
        staged(dir.path()).await,
        (1..=7).map(|i| (i, 2)).collect::<Vec<_>>()
    );
}

/// A voter whose entries are of another history fails the check partway;
/// what it served is dropped and the next voter serves the whole base.
#[tokio::test]
async fn a_voter_of_another_history_is_abandoned_for_the_next() {
    let dir = tempfile::tempdir().expect("dir");
    let other = FakeVoter {
        entries: history(|index| if index < 4 { 2 } else { 3 }),
        ..good()
    };
    let source = source(vec![("other", other), ("a", good())]);
    let base = base().expect("base");
    let voters = [voter("other"), voter("a")];
    let pulled = pull(
        &source,
        &voters,
        &base,
        0,
        dir.path(),
        &staging_config(),
        quick(),
    )
    .await
    .expect("pull");
    assert_eq!(pulled, Pulled::Staged(base_last()));
    assert_eq!(
        staged(dir.path()).await,
        (0..=7).map(|i| (i, 2)).collect::<Vec<_>>()
    );
    let asked = source.asked.lock();
    assert!(asked.iter().any(|(b, at)| &**b == "other" && *at == 4));
    assert_eq!(asked.iter().filter(|(b, _)| &**b == "a").count(), 4);
}

/// A floor that rises under the pull restarts it there, so the staged log
/// starts at the new floor with no hole.
#[tokio::test]
async fn a_raised_floor_restarts_the_pull_there() {
    let dir = tempfile::tempdir().expect("dir");
    let raising = FakeVoter {
        raised: Some((1, 5)),
        ..good()
    };
    let source = source(vec![("a", raising)]);
    let base = base().expect("base");
    let pulled = pull(
        &source,
        &[voter("a")],
        &base,
        0,
        dir.path(),
        &staging_config(),
        quick(),
    )
    .await
    .expect("pull");
    assert_eq!(pulled, Pulled::Staged(base_last()));
    assert_eq!(
        staged(dir.path()).await,
        (5..=7).map(|i| (i, 2)).collect::<Vec<_>>()
    );
}

/// No voter that serves base pulls: nothing is staged, and the pull gives
/// up after its rounds.
#[tokio::test]
async fn no_capable_voter_means_no_pull() {
    let dir = tempfile::tempdir().expect("dir");
    let refusing = FakeVoter {
        refuses: true,
        ..good()
    };
    let source = source(vec![("a", refusing)]);
    let base = base().expect("base");
    let pulled = pull(
        &source,
        &[voter("a")],
        &base,
        0,
        dir.path(),
        &staging_config(),
        quick(),
    )
    .await
    .expect("pull");
    assert_eq!(pulled, Pulled::GaveUp);
    assert!(!dir.path().join("base-staging").exists());
    assert_eq!(source.asked.lock().len(), 2, "one ask a round");
    let none = pull(
        &source,
        &[],
        &base,
        0,
        dir.path(),
        &staging_config(),
        quick(),
    )
    .await
    .expect("pull");
    assert_eq!(none, Pulled::GaveUp);
    assert!(!dir.path().join("base-staging").exists());
}
