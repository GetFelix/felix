//! Purge and the purged id.

use openraft::storage::RaftLogStorage;

use super::machine::Machine;
use super::writes::{append, append_flushed};
use super::*;
use crate::raft_log::{SnapshotId, StoredSnapshot};

/// Thirty entries: a blank at 0, commands of term 1 up to 19, then term 2.
fn thirty() -> Vec<TestEntry> {
    let mut entries = vec![blank(1, 0)];
    entries.extend((1..20).map(|index| normal(1, index, &format!("command-{index}"))));
    entries.push(blank(2, 20));
    entries.extend((21..30).map(|index| normal(2, index, &format!("command-{index}"))));
    entries
}

async fn state(raft: &RaftLog<Types, Codec>) -> (Option<LogIdOf<Types>>, Option<LogIdOf<Types>>) {
    let state = raft.log_state().await.expect("state");
    (state.last_purged_log_id, state.last_log_id)
}

/// A purge drops entries up to the id it is given and no further, whole
/// segments at a time, and the id outlives a reopen. Records left below it
/// in a segment that also holds later entries are not entries any more.
#[tokio::test]
async fn a_purge_drops_entries_through_its_id() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = thirty();
    append_flushed(&mut raft, &entries).await;
    let segments = raft.disk_log().segments().len();

    raft.purge(log_id(1, 10)).await.expect("purge");
    assert!(raft.disk_log().segments().len() < segments, "nothing went");
    assert!(
        raft.disk_log().base_offset() < 11,
        "the purge should end inside a segment for this to prove much"
    );
    assert_eq!(all(&raft).await.expect("read"), entries[11..]);
    assert_eq!(
        raft.reader()
            .try_get_log_entries(5..15)
            .await
            .expect("read"),
        entries[11..15]
    );
    assert_eq!(
        state(&raft).await,
        (Some(log_id(1, 10)), Some(log_id(2, 29)))
    );

    // At or below the purged id: nothing to do, whatever term is named.
    raft.purge(log_id(1, 4)).await.expect("purge");
    raft.purge(log_id(1, 10)).await.expect("purge");
    assert_eq!(state(&raft).await.0, Some(log_id(1, 10)));

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let mut raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries[11..]);
    assert_eq!(
        state(&raft).await,
        (Some(log_id(1, 10)), Some(log_id(2, 29)))
    );
    let next = normal(2, 30, "next");
    append_flushed(&mut raft, std::slice::from_ref(&next)).await;
    assert_eq!(all(&raft).await.expect("read").last(), Some(&next));
}

/// A purge names an entry. Another term's entry at that index is refused,
/// with nothing purged.
#[tokio::test]
async fn a_purge_of_another_terms_entry_is_refused() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = thirty();
    append_flushed(&mut raft, &entries).await;
    let err = raft
        .purge(log_id(2, 10))
        .await
        .expect_err("term 1 is there");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    assert_eq!(state(&raft).await, (None, Some(log_id(2, 29))));
    assert_eq!(all(&raft).await.expect("read"), entries);
}

/// openraft purges past the log's end at startup, when the state machine
/// is ahead of the log. The log is then empty and starts right after the
/// id: its last id is the purged id, the next append must follow it, and a
/// reopen before that append changes nothing.
#[tokio::test]
async fn a_purge_past_the_end_restarts_the_log_after_its_id() {
    for reopen in [false, true] {
        let dir = tempdir().expect("dir");
        let mut raft = open(&dir);
        append_flushed(&mut raft, &thirty()[..12]).await;
        raft.purge(log_id(3, 40)).await.expect("purge");
        if reopen {
            raft.disk_log().close().await.expect("close");
            drop(raft);
            raft = open(&dir);
        }
        assert_eq!(
            state(&raft).await,
            (Some(log_id(3, 40)), Some(log_id(3, 40))),
            "reopen {reopen}"
        );
        assert_eq!(all(&raft).await.expect("read"), []);
        assert_eq!(
            raft.read_committed().await.expect("committed"),
            Some(log_id(3, 40))
        );

        append(&mut raft, &[normal(3, 45, "a hole")])
            .await
            .map(drop)
            .expect_err("not right after the purged id");
        let next = [normal(3, 41, "next"), normal(4, 42, "after")];
        append_flushed(&mut raft, &next).await;
        assert_eq!(all(&raft).await.expect("read"), next);
        assert_eq!(
            state(&raft).await,
            (Some(log_id(3, 40)), Some(log_id(4, 42)))
        );
    }
}

/// The purge of exactly the last entry also empties the log.
#[tokio::test]
async fn a_purge_of_the_last_entry_empties_the_log() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &thirty()).await;
    raft.purge(log_id(2, 29)).await.expect("purge");
    assert_eq!(
        state(&raft).await,
        (Some(log_id(2, 29)), Some(log_id(2, 29)))
    );
    assert_eq!(all(&raft).await.expect("read"), []);
    let next = normal(2, 30, "next");
    append_flushed(&mut raft, std::slice::from_ref(&next)).await;
    assert_eq!(all(&raft).await.expect("read"), [next]);
}

/// Only committed entries are purged, so the committed id never reads back
/// below the purged id, whether none was saved or an older one.
#[tokio::test]
async fn the_committed_id_is_never_below_the_purged_id() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &thirty()).await;
    raft.purge(log_id(1, 6)).await.expect("purge");
    assert_eq!(
        raft.read_committed().await.expect("committed"),
        Some(log_id(1, 6))
    );
    raft.save_committed(Some(log_id(1, 8))).await.expect("save");
    raft.purge(log_id(1, 12)).await.expect("purge");
    assert_eq!(
        raft.read_committed().await.expect("committed"),
        Some(log_id(1, 12))
    );
    raft.save_committed(Some(log_id(2, 25)))
        .await
        .expect("save");
    assert_eq!(
        raft.read_committed().await.expect("committed"),
        Some(log_id(2, 25))
    );
}

/// A truncation never reaches below the purged id. Naming the purged id
/// itself cuts every entry; an id below it, or below the base of a log
/// that purged nothing, is refused instead of moving the log's start down.
#[tokio::test]
async fn a_truncation_stops_at_the_purged_id() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = thirty();
    append_flushed(&mut raft, &entries).await;
    raft.purge(log_id(1, 10)).await.expect("purge");

    raft.truncate_after(Some(log_id(1, 4)))
        .await
        .expect_err("below the purged id");
    raft.truncate_after(Some(log_id(2, 10)))
        .await
        .expect_err("the purged id is of term 1");
    assert_eq!(all(&raft).await.expect("read"), entries[11..]);

    raft.truncate_after(Some(log_id(1, 10))).await.expect("cut");
    assert_eq!(
        state(&raft).await,
        (Some(log_id(1, 10)), Some(log_id(1, 10)))
    );
    raft.truncate_after(None).await.expect("nothing to cut");
    let next = normal(5, 11, "next");
    append_flushed(&mut raft, std::slice::from_ref(&next)).await;
    assert_eq!(all(&raft).await.expect("read"), [next]);

    // A log that starts past zero and purged nothing.
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let late = [normal(1, 7, "a"), normal(1, 8, "b")];
    append_flushed(&mut raft, &late).await;
    raft.truncate_after(Some(log_id(1, 3)))
        .await
        .expect_err("below the log");
    assert_eq!(raft.disk_log().base_offset(), 7);
    assert_eq!(all(&raft).await.expect("read"), late);
}

/// A purge more than one below the base of a log that purged nothing would
/// record a purged id with missing entries between it and the first one.
#[tokio::test]
async fn a_purge_below_the_base_is_refused() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let late = [normal(1, 7, "a"), normal(1, 8, "b")];
    append_flushed(&mut raft, &late).await;
    let err = raft
        .purge(log_id(1, 3))
        .await
        .expect_err("entries 4 to 6 are missing");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    assert_eq!(state(&raft).await.0, None);
    assert_eq!(all(&raft).await.expect("read"), late);
    // Right below the base nothing is missing.
    raft.purge(log_id(1, 6)).await.expect("purge");
    assert_eq!(state(&raft).await.0, Some(log_id(1, 6)));
}

/// Under `purge_within`, a purge stops at the stored snapshot, and a log
/// already purged past its snapshot is refused.
#[tokio::test]
async fn a_purge_stays_within_the_stored_snapshot() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &thirty()).await;
    let machine = Machine::open(dir.path());
    raft.purge_within(&machine.snapshots).expect("no purge yet");
    raft.purge(log_id(1, 5))
        .await
        .expect_err("no snapshot covers it");
    assert_eq!(state(&raft).await.0, None);

    let snapshot = |index| StoredSnapshot {
        last: Some(SnapshotId { term: 1, index }),
        meta: Bytes::new(),
        data: Bytes::from_static(b"state"),
    };
    assert!(
        machine
            .snapshots
            .install(snapshot(8))
            .await
            .expect("install")
    );
    raft.purge(log_id(1, 9)).await.expect_err("one past it");
    raft.purge(log_id(1, 8)).await.expect("covered");
    assert_eq!(state(&raft).await.0, Some(log_id(1, 8)));
    raft.disk_log().close().await.expect("close");
    drop(raft);

    // The same log beside a snapshot that covers less than was purged.
    let behind = tempdir().expect("dir");
    let machine = Machine::open(behind.path());
    assert!(
        machine
            .snapshots
            .install(snapshot(3))
            .await
            .expect("install")
    );
    let raft = open(&dir);
    let err = raft
        .purge_within(&machine.snapshots)
        .expect_err("purged past it");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

/// A purge between two pages of a read restarts the read above the purged
/// id instead of failing it or returning a range with a hole.
#[tokio::test]
async fn a_paged_read_restarts_when_entries_are_purged_under_it() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = thirty();
    append_flushed(&mut raft, &entries).await;

    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    *raft.shared.pause_after_page.lock() = Some((reached_tx, resume_rx));
    let mut reader = raft.reader();
    let read = tokio::spawn(async move { reader.try_get_log_entries(3..25).await });
    reached_rx.await.expect("the first page");
    raft.purge(log_id(1, 15)).await.expect("purge");
    resume_tx.send(()).expect("resume");
    assert_eq!(read.await.expect("join").expect("read"), entries[16..25]);
}

/// A trim record sets a floor the log deletes below on its own timer. Only
/// a purge may delete a Raft log's entries, so a command stored as one is
/// refused.
#[tokio::test]
async fn a_command_stored_as_a_trim_is_refused() {
    #[derive(Clone, Copy)]
    struct Trims;

    impl EntryCodec<Types> for Trims {
        fn encode_data(&self, _data: &String) -> io::Result<AppendRecord> {
            Ok(record(RecordMark::Trim { floor: 9 }, b""))
        }
        fn decode_data(&self, record: LogRecord) -> io::Result<String> {
            Codec.decode_data(record)
        }
        fn encode_membership(&self, membership: &Membership) -> io::Result<Bytes> {
            Codec.encode_membership(membership)
        }
        fn decode_membership(&self, payload: &[u8]) -> io::Result<Membership> {
            Codec.decode_membership(payload)
        }
        fn encode_node_id(&self, node_id: &u64) -> String {
            Codec.encode_node_id(node_id)
        }
        fn decode_node_id(&self, leader: &str) -> io::Result<u64> {
            Codec.decode_node_id(leader)
        }
    }

    let dir = tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let mut raft = RaftLog::<Types, Trims>::new(log, Trims).expect("a Raft log");
    let (tx, _rx) = <Types as openraft::type_config::TypeConfigExt>::oneshot();
    let err = raft
        .append(
            [normal(1, 0, "trim")],
            openraft::storage::IOFlushed::signal(tx),
        )
        .await
        .expect_err("a trim is no command");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    assert_eq!(raft.disk_log().tail_offset().await.expect("tail"), 0);
}

/// One append larger than a segment is written a segment's worth at a
/// time, not as one oversized segment, and its callback still covers all
/// of it.
#[tokio::test]
async fn a_large_append_is_spread_over_segments() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = thirty();
    let flushed = append(&mut raft, &entries).await.expect("append");
    flushed.await.expect("flush");
    let segments = raft.disk_log().segments();
    assert!(segments.len() > 4, "{} segments", segments.len());
    assert_eq!(all(&raft).await.expect("read"), entries);
    raft.disk_log().close().await.expect("close");
    drop(raft);
    assert_eq!(all(&open(&dir)).await.expect("read"), entries);
}

/// The purged id is on disk before a purge deletes anything. A purge that
/// got no further reads as done at the next open: the log starts after the
/// id, whether the id is inside the log or past its end.
#[tokio::test]
async fn a_purge_that_only_wrote_its_id_reads_as_done() {
    use crate::raft_log::purged::{self, Purged};

    for (purged, kept) in [(log_id(1, 5), 6..12), (log_id(3, 40), 12..12)] {
        let dir = tempdir().expect("dir");
        let mut raft = open(&dir);
        let entries = &thirty()[..12];
        append_flushed(&mut raft, entries).await;
        raft.disk_log().close().await.expect("close");
        drop(raft);
        let id = Purged {
            index: purged.index,
            generation: **purged.committed_leader_id() + 1,
        };
        purged::store(dir.path(), &id).expect("store");

        let mut raft = open(&dir);
        assert_eq!(raft.disk_log().base_offset(), 0, "nothing was deleted");
        assert_eq!(all(&raft).await.expect("read"), entries[kept.clone()]);
        assert_eq!(
            raft.reader()
                .try_get_log_entries(3..100)
                .await
                .expect("read"),
            entries[kept.clone()]
        );
        let last = entries[kept].last().map_or(purged, |entry| entry.log_id);
        assert_eq!(state(&raft).await, (Some(purged), Some(last)));

        let next = normal(4, last.index + 1, "next");
        append_flushed(&mut raft, std::slice::from_ref(&next)).await;
        assert_eq!(all(&raft).await.expect("read").last(), Some(&next));
        assert_eq!(state(&raft).await.0, Some(purged));
    }
}
