//! The tail cache: the newest entries are read from memory, and a read
//! never returns an entry the log no longer holds.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use openraft::storage::RaftLogStorage;

use super::writes::append_flushed;
use super::*;
use crate::raft_log::TailBudget;

fn segment_reads(raft: &RaftLog<Types, Codec>) -> u64 {
    raft.shared.segment_reads.load(Ordering::Relaxed)
}

fn commands(term: u64, range: std::ops::Range<u64>, what: &str) -> Vec<TestEntry> {
    range
        .map(|index| normal(term, index, &format!("{what}-{index}")))
        .collect()
}

/// Entries just appended are read without a read of the segments, and read
/// the same as the segments give them after a reopen.
#[tokio::test]
async fn fresh_entries_are_read_from_memory() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let mut entries = vec![membership(0, 0), blank(1, 1)];
    entries.extend(commands(1, 2..7, "command"));
    entries.extend([blank(3, 7), membership(3, 8), normal(3, 9, "last")]);
    append_flushed(&mut raft, &entries).await;

    let mut reader = raft.reader();
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(
        reader.try_get_log_entries(3..=8).await.expect("read"),
        entries[3..=8]
    );
    assert_eq!(
        reader.limited_get_log_entries(4, 10).await.expect("read"),
        entries[4..]
    );
    assert_eq!(segment_reads(&raft), 0);

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert!(
        segment_reads(&raft) > 0,
        "a reopened log holds none in memory"
    );
}

/// A range that starts below what memory holds comes from the segments,
/// whole, and one that starts inside it from memory.
#[tokio::test]
async fn older_entries_are_read_from_the_segments() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = commands(1, 0..60, "command");
    append_flushed(&mut raft, &entries).await;

    let mut reader = raft.reader();
    assert_eq!(
        reader.try_get_log_entries(58..60).await.expect("read"),
        entries[58..]
    );
    assert_eq!(segment_reads(&raft), 0);
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert!(
        segment_reads(&raft) > 0,
        "sixty entries fit in the test cache"
    );
    assert_eq!(
        reader.try_get_log_entries(1..59).await.expect("read"),
        entries[1..59]
    );
}

/// After a truncation and an append at the same indexes, a read returns
/// the new entries and never one that was cut.
#[tokio::test]
async fn a_cut_entry_is_never_read_again() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let old = commands(1, 0..6, "old");
    append_flushed(&mut raft, &old).await;

    raft.truncate_after(Some(log_id(1, 2)))
        .await
        .expect("truncate");
    assert_eq!(all(&raft).await.expect("read"), old[..3]);
    assert_eq!(
        raft.reader().try_get_log_entries(3..6).await.expect("read"),
        []
    );

    let new = commands(2, 3..5, "new");
    append_flushed(&mut raft, &new).await;
    assert_eq!(
        raft.reader().try_get_log_entries(3..6).await.expect("read"),
        new
    );
    assert_eq!(
        all(&raft).await.expect("read"),
        [&old[..3], &new[..]].concat()
    );
}

/// A cut the adapter did not make is seen too: the log counts its cuts,
/// and memory is not read once the count has moved.
#[tokio::test]
async fn a_cut_made_under_the_adapter_is_seen() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let old = commands(1, 0..6, "old");
    append_flushed(&mut raft, &old).await;
    assert_eq!(all(&raft).await.expect("read"), old);
    assert_eq!(segment_reads(&raft), 0, "the rest proves nothing otherwise");

    raft.disk_log().truncate(3).await.expect("truncate");
    assert_eq!(all(&raft).await.expect("read"), old[..3]);
    assert_eq!(
        raft.reader().try_get_log_entries(4..6).await.expect("read"),
        []
    );

    let new = commands(2, 3..5, "new");
    append_flushed(&mut raft, &new).await;
    assert_eq!(
        all(&raft).await.expect("read"),
        [&old[..3], &new[..]].concat()
    );
}

/// A purged entry is not read from memory, whether the purge deleted a
/// segment or only wrote its id, and a log restarted past its end reads
/// only what was appended after.
#[tokio::test]
async fn a_purged_entry_is_never_read_again() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = commands(1, 0..9, "command");
    append_flushed(&mut raft, &entries).await;
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(segment_reads(&raft), 0, "the rest proves nothing otherwise");

    // Inside the newest segment, so nothing is deleted.
    raft.purge(log_id(1, 7)).await.expect("purge");
    assert_eq!(all(&raft).await.expect("read"), entries[8..]);
    assert_eq!(
        raft.reader().try_get_log_entries(5..9).await.expect("read"),
        entries[8..]
    );

    raft.purge(log_id(4, 40)).await.expect("purge past the end");
    assert_eq!(all(&raft).await.expect("read"), []);
    let after = commands(5, 41..44, "after");
    append_flushed(&mut raft, &after).await;
    assert_eq!(all(&raft).await.expect("read"), after);
}

/// A closed log refuses a read. Entries still in memory do not answer it.
#[tokio::test]
async fn a_closed_log_is_not_read_from_memory() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &commands(1, 0..3, "command")).await;
    assert_eq!(all(&raft).await.expect("read").len(), 3);
    assert_eq!(segment_reads(&raft), 0, "the rest proves nothing otherwise");

    raft.disk_log().close().await.expect("close");
    all(&raft).await.expect_err("the log is closed");
}

/// A cut that lands while a read holds records from memory restarts the
/// read: it returns the log as the cut left it.
#[tokio::test]
async fn a_read_from_memory_restarts_when_the_log_is_cut_under_it() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let old = commands(1, 0..6, "old");
    append_flushed(&mut raft, &old).await;

    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *raft.shared.pause_after_page.lock() = Some((reached_tx, resume_rx));
    let mut reader = raft.reader();
    let read = tokio::spawn(async move { reader.try_get_log_entries(..).await });
    reached.await.expect("the read took its records");
    assert_eq!(segment_reads(&raft), 0, "the read was not from memory");

    raft.disk_log().truncate(2).await.expect("truncate");
    resume.send(()).expect("resume");
    assert_eq!(read.await.expect("join").expect("read"), old[..2]);
}

/// A log whose cache another log's append emptied still reads every entry,
/// from the segments, and the one that appended keeps its own in memory.
#[tokio::test]
async fn an_evicted_cache_is_read_from_the_segments() {
    let budget = TailBudget::new(2048);
    let open_on = |dir: &TempDir, label: &str| {
        let log = DiskLog::open(dir.path(), label, config()).expect("open");
        RaftLog::<Types, Codec>::with_tail_budget(log, Codec, Arc::clone(&budget)).expect("log")
    };
    let (dir_a, dir_b) = (tempdir().expect("dir"), tempdir().expect("dir"));
    let (mut a, mut b) = (
        open_on(&dir_a, "t/ns/raft/0"),
        open_on(&dir_b, "t/ns/raft/1"),
    );
    let entries = commands(1, 0..3, &"x".repeat(400));
    append_flushed(&mut a, &entries).await;
    assert_eq!(all(&a).await.expect("read"), entries);
    assert_eq!(segment_reads(&a), 0, "held in memory before the eviction");

    append_flushed(&mut b, &entries).await;
    assert_eq!(all(&a).await.expect("read"), entries);
    assert!(segment_reads(&a) > 0, "a's cache was given up");
    assert_eq!(all(&b).await.expect("read"), entries);
    assert_eq!(segment_reads(&b), 0, "b keeps what it just appended");
}
