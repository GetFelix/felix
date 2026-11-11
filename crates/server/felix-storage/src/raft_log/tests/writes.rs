//! The write side: append and its callback, truncation, the vote, and what
//! a Raft log refuses.

use futures_util::FutureExt;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::VoteOf;
use openraft::vote::{RaftLeaderId, RaftVote};

use super::*;
use crate::StorageError;
use crate::segment::GENERATION_VERSION;

/// Resolves to what an append's callback was given.
type Flushed = std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// Append, returning the callback's receiver once the append has returned.
pub(super) async fn append(
    raft: &mut RaftLog<Types, Codec>,
    entries: &[TestEntry],
) -> io::Result<Flushed> {
    let (tx, rx) = Types::oneshot();
    raft.append(entries.to_vec(), IOFlushed::signal(tx)).await?;
    Ok(Box::pin(async move {
        rx.await.expect("the callback was dropped")
    }))
}

/// Append a few entries at a time, waiting for each callback. A batch goes
/// into one segment whole, so several are what spread a log over segments.
pub(super) async fn append_flushed(raft: &mut RaftLog<Types, Codec>, entries: &[TestEntry]) {
    for batch in entries.chunks(3) {
        let flushed = append(raft, batch).await.expect("append");
        flushed.await.expect("flush");
    }
}

fn vote(term: u64, node: u64, committed: bool) -> VoteOf<Types> {
    let leader = <Types as openraft::RaftTypeConfig>::LeaderId::new(term, node);
    VoteOf::<Types>::from_leader_id(leader, committed)
}

/// The mixed run every kind of entry and two terms go through.
fn mixed() -> Vec<TestEntry> {
    let mut entries = vec![membership(0, 0), blank(1, 1)];
    entries.extend((2..9).map(|index| normal(1, index, &format!("command-{index}"))));
    entries.extend([blank(3, 9), membership(3, 10), normal(3, 11, "last")]);
    entries
}

/// One append of every kind of entry across two terms reads back as given,
/// from a log that is v9 from its first segment, and a reopen changes
/// nothing.
#[tokio::test]
async fn an_append_keeps_each_entrys_term_and_kind() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    assert_eq!(
        raft.disk_log().segments()[0].version,
        GENERATION_VERSION,
        "the first segment, before any entry"
    );
    let entries = mixed();
    append_flushed(&mut raft, &entries).await;

    let segments = raft.disk_log().segments();
    assert!(segments.len() > 2, "one segment proves little");
    assert!(
        segments.iter().all(|s| s.version == GENERATION_VERSION),
        "{segments:?}"
    );
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(3, 11))
    );
    raft.disk_log().close().await.expect("close");
    drop(raft);
    let raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries);
}

/// The append returns with its entries readable while the flush is still
/// held, and the callback fires only once the flush has covered them.
#[tokio::test]
async fn the_callback_waits_for_the_flush_and_readers_do_not() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = mixed();

    let log = raft.disk_log().clone();
    let held = log.hold_flushes().await;
    let mut flushed = append(&mut raft, &entries).await.expect("append");
    assert_eq!(all(&raft).await.expect("read"), entries);
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        flushed.as_mut().now_or_never().is_none(),
        "the callback fired before a flush"
    );
    assert_eq!(raft.disk_log().durable_offset(), 0);

    drop(held);
    flushed.await.expect("flush");
    assert_eq!(raft.disk_log().durable_offset(), 12);
}

/// A flush that fails reaches the callback as an error, once.
#[tokio::test]
async fn a_failed_flush_fails_the_callback() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    raft.disk_log().fail_next_flush();
    let flushed = append(&mut raft, &[blank(1, 0)]).await.expect("append");
    flushed.await.expect_err("the flush failed");
}

/// A truncation waits for the callbacks of the appends before it. Here the
/// flush is held and the truncation would otherwise reach the flush lock
/// first, cut, and leave the append's flush waiting on entries that are
/// gone: it would report the append failed, which stops the node.
#[tokio::test]
async fn a_truncation_waits_for_the_callbacks_of_what_it_cuts() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = mixed();
    let log = raft.disk_log().clone();
    let held = log.hold_flushes().await;
    let flushed = append(&mut raft, &entries).await.expect("append");

    // Polled without yielding, so the append's flush task has not run when
    // the truncation gets as far as it can.
    let mut cutter = raft.clone();
    let mut cut = std::pin::pin!(cutter.truncate_after(Some(log_id(1, 1))));
    for _ in 0..40 {
        assert!(
            cut.as_mut().now_or_never().is_none(),
            "cut under a held flush"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    drop(held);
    cut.await.expect("truncate");
    flushed
        .now_or_never()
        .expect("the callback had not fired when the truncation returned")
        .expect("flush");
    assert_eq!(all(&raft).await.expect("read"), entries[..2]);

    // The cut is where the next term's entries go.
    let next = [blank(2, 2), normal(2, 3, "after the cut")];
    append_flushed(&mut raft, &next).await;
    assert_eq!(
        all(&raft).await.expect("read"),
        [&entries[..2], &next[..]].concat()
    );
}

/// An append that does not start at the tail, or skips inside itself, is
/// refused with nothing written.
#[tokio::test]
async fn an_append_must_continue_the_log() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = [blank(1, 0), normal(1, 1, "a"), normal(1, 2, "b")];
    append_flushed(&mut raft, &entries).await;

    for refused in [
        vec![normal(1, 5, "past the tail")],
        vec![normal(1, 2, "over an entry")],
        vec![normal(1, 3, "c"), normal(1, 5, "skips")],
    ] {
        let err = append(&mut raft, &refused).await.err().expect("refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    }
    assert_eq!(all(&raft).await.expect("read"), entries);
}

/// An empty log starts at whatever index its first entry has, and holds
/// nothing below it after a reopen.
#[tokio::test]
async fn an_empty_log_starts_at_any_index() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = [blank(2, 7), normal(2, 8, "a")];
    append_flushed(&mut raft, &entries).await;
    assert_eq!(raft.disk_log().base_offset(), 7);
    assert_eq!(all(&raft).await.expect("read"), entries);

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let mut raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(
        raft.disk_log().segments()[0].version,
        GENERATION_VERSION,
        "a log restarted at a base is still v9"
    );

    // Emptied, it is still not a log that goes back.
    raft.truncate_after(None).await.expect("truncate");
    assert_eq!(raft.log_state().await.expect("state").last_log_id, None);
    assert_eq!(
        raft.disk_log().segments()[0].version,
        GENERATION_VERSION,
        "an emptied log starts again on v9"
    );
    assert!(
        append(&mut raft, &[blank(3, 4)]).await.is_err(),
        "below where the log starts"
    );
    append_flushed(&mut raft, &[blank(3, 7)]).await;
    assert_eq!(all(&raft).await.expect("read"), [blank(3, 7)]);
}

/// `truncate_after` keeps through the id it is given, checks that id against
/// the log, and cuts nothing for an id past the tail.
#[tokio::test]
async fn truncate_after_keeps_through_the_entry_it_names() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries = mixed();
    append_flushed(&mut raft, &entries).await;

    let err = raft
        .truncate_after(Some(log_id(2, 5)))
        .await
        .expect_err("the entry at 5 is of term 1");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    raft.truncate_after(Some(log_id(3, 40)))
        .await
        .expect("past the tail");
    assert_eq!(all(&raft).await.expect("read"), entries);

    // Across segments, into the middle of one.
    raft.truncate_after(Some(log_id(1, 5)))
        .await
        .expect("truncate");
    assert_eq!(all(&raft).await.expect("read"), entries[..=5]);
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(1, 5))
    );
    raft.disk_log().close().await.expect("close");
    drop(raft);
    let mut raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries[..=5]);

    raft.truncate_after(None).await.expect("truncate");
    assert!(all(&raft).await.expect("read").is_empty());
}

/// A saved vote is read back, survives a reopen, and only moves forward.
#[tokio::test]
async fn a_vote_only_moves_forward() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    let mut reader = raft.reader();
    assert_eq!(reader.read_vote().await.expect("read"), None);

    for step in [vote(0, 1, false), vote(0, 1, true), vote(0, 1, true)] {
        raft.save_vote(&step).await.expect("save");
        assert_eq!(reader.read_vote().await.expect("read"), Some(step));
    }
    for back in [vote(0, 1, false), vote(0, 2, true), vote(0, 2, false)] {
        let err = raft.save_vote(&back).await.expect_err("goes back");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{back}: {err}");
        assert_eq!(
            reader.read_vote().await.expect("read"),
            Some(vote(0, 1, true))
        );
    }
    raft.save_vote(&vote(3, 2, false)).await.expect("save");
    raft.save_vote(&vote(2, 1, true))
        .await
        .expect_err("a lower term");

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let raft = open(&dir);
    let found = raft.reader().read_vote().await.expect("read");
    assert_eq!(found, Some(vote(3, 2, false)));
    assert!(!found.expect("a vote").is_committed());
}

/// The committed id is saved, read back and kept across a reopen, and a
/// truncation below it is refused.
#[tokio::test]
async fn the_committed_id_is_read_back() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    assert_eq!(raft.read_committed().await.expect("read"), None);
    let entries = mixed();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(None).await.expect("save");
    assert_eq!(raft.read_committed().await.expect("read"), None);
    raft.save_committed(Some(log_id(1, 5))).await.expect("save");
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 5))
    );
    raft.truncate_after(Some(log_id(1, 3)))
        .await
        .expect_err("below the committed id");

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let mut raft = open(&dir);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 5))
    );
    assert_eq!(all(&raft).await.expect("read"), entries);
}

/// Retention would delete entries nothing purged, so a Raft log refuses
/// bounds at open and later, and a log not opened as one is no Raft log.
#[tokio::test]
async fn a_raft_log_takes_no_retention_bounds() {
    let dir = tempdir().expect("dir");
    for bounded in [
        LogConfig {
            retention_bytes: Some(1 << 20),
            ..config()
        },
        LogConfig {
            retention_age: Some(std::time::Duration::from_secs(60)),
            ..config()
        },
    ] {
        let err = DiskLog::open(dir.path(), "t/ns/raft/0", bounded).expect_err("bounded");
        assert!(matches!(err, StorageError::InvalidConfig(_)), "{err}");
    }

    let raft = open(&dir);
    let err = raft
        .disk_log()
        .set_retention(crate::log::Retention {
            bytes: Some(1 << 20),
            age: None,
        })
        .expect_err("bounded later");
    assert!(matches!(err, StorageError::InvalidConfig(_)), "{err}");
    assert!(!raft.disk_log().retention().is_set());

    let plain = tempdir().expect("dir");
    let log = DiskLog::open(
        plain.path(),
        "t/ns/s/0",
        LogConfig {
            raft_log: false,
            ..config()
        },
    )
    .expect("open");
    assert!(RaftLog::<Types, Codec>::new(log, Codec).is_err());
}

/// A directory that held records before it became a Raft log's moves onto
/// v9 at the first entry, so that entry has a term.
#[tokio::test]
async fn an_older_log_moves_onto_v9_at_its_first_entry() {
    let dir = tempdir().expect("dir");
    let plain = LogConfig {
        raft_log: false,
        ..config()
    };
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", plain).expect("open");
    log.append(&[record(RecordMark::None, b"old")])
        .await
        .expect("append");
    log.close().await.expect("close");
    drop(log);

    let mut raft = open(&dir);
    // A command, which the old segment's format could hold.
    let entries = [normal(4, 1, "a"), normal(4, 2, "b")];
    append_flushed(&mut raft, &entries).await;
    let versions: Vec<u16> = raft
        .disk_log()
        .segments()
        .iter()
        .map(|s| s.version)
        .collect();
    assert!(versions[0] < GENERATION_VERSION, "{versions:?}");
    assert_eq!(versions.last(), Some(&GENERATION_VERSION), "{versions:?}");
    assert_eq!(
        raft.reader().try_get_log_entries(1..).await.expect("read"),
        entries
    );
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(4, 2))
    );
}

/// A committed id names an entry, not an index. Here the disk comes back
/// with the tail a truncation cut, as if the cut had never reached it, under
/// a committed id saved for the entry that replaced it. The index is inside
/// the log, so lowering the id to the log's end does nothing: only the term
/// tells the two entries apart. The cut tail was never committed and must
/// not read back as committed.
#[tokio::test]
async fn a_committed_id_is_not_another_terms_entry_at_its_index() {
    let dir = tempdir().expect("dir");
    let stale = tempdir().expect("dir");
    let mut raft = open(&dir);
    let old: Vec<_> = (0..6).map(|index| normal(1, index, "old")).collect();
    append_flushed(&mut raft, &old).await;
    raft.save_committed(Some(log_id(1, 2))).await.expect("save");
    raft.disk_log().shutdown().await.expect("shutdown");
    let segments = |dir: &TempDir| {
        std::fs::read_dir(dir.path())
            .expect("dir")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.file_name().is_some_and(|name| name != "replica"))
            .collect::<Vec<_>>()
    };
    for path in segments(&dir) {
        std::fs::copy(&path, stale.path().join(path.file_name().expect("name"))).expect("copy");
    }

    raft.truncate_after(Some(log_id(1, 3))).await.expect("cut");
    let new = [normal(2, 4, "new"), normal(2, 5, "new")];
    append_flushed(&mut raft, &new).await;
    raft.save_committed(Some(log_id(2, 5))).await.expect("save");
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(2, 5))
    );
    raft.disk_log().close().await.expect("close");
    drop(raft);

    // The reopen in between shows the id itself survives one.
    let mut raft = open(&dir);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(2, 5))
    );
    raft.disk_log().close().await.expect("close");
    drop(raft);

    for path in segments(&dir) {
        std::fs::remove_file(path).expect("remove");
    }
    for path in segments(&stale) {
        std::fs::copy(&path, dir.path().join(path.file_name().expect("name"))).expect("copy");
    }
    let mut raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), old, "the cut tail is back");
    assert_eq!(raft.read_committed().await.expect("read"), None);
    // The leader can cut what is not committed, and the id it then sends
    // is saved over the dropped one.
    raft.truncate_after(Some(log_id(1, 3))).await.expect("cut");
    append_flushed(&mut raft, &new).await;
    raft.save_committed(Some(log_id(2, 5))).await.expect("save");
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(2, 5))
    );
}

/// Copy one file of `from` over `to`'s.
fn copy_file(from: &TempDir, to: &TempDir, name: &str) {
    std::fs::copy(from.path().join(name), to.path().join(name)).expect("copy");
}

/// A log holding entries 0..=9 of term 1 under a `replica` that says
/// (term 5, index 10) is committed: the entry at 10 was lost with the power.
async fn a_committed_id_past_the_log() -> TempDir {
    let ahead = tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..10).map(|index| normal(1, index, "kept")).collect();
    entries.push(normal(5, 10, "lost"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(5, 10)))
        .await
        .expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &entries[..10]).await;
    raft.disk_log().close().await.expect("close");
    copy_file(&ahead, &dir, "replica");
    dir
}

/// The open that lowers a committed id to the log's end writes the lower
/// index and its entry's term in one write. Stopped at every step of that
/// open, the next one still reads the lowered id. Two writes would leave
/// the lower index beside the lost entry's term in between, which reads as
/// another entry's id and drops the commit.
#[tokio::test]
async fn a_lowered_committed_id_is_written_with_its_own_term() {
    use crate::disk_log::crash_point;

    let dir = a_committed_id_past_the_log().await;
    crash_point::watch(dir.path(), None);
    let mut raft = open(&dir);
    let steps = crash_point::unwatch(dir.path());
    assert!(steps >= 4, "the open wrote nothing: {steps} steps");
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 9))
    );
    raft.disk_log().close().await.expect("close");

    for stop in 0..steps {
        let dir = a_committed_id_past_the_log().await;
        crash_point::watch(dir.path(), Some(stop));
        let stopped = DiskLog::open(dir.path(), "t/ns/raft/0", config());
        crash_point::unwatch(dir.path());
        assert!(stopped.is_err(), "stop {stop} of {steps}");
        drop(stopped);
        let mut raft = open(&dir);
        assert_eq!(
            raft.read_committed().await.expect("read"),
            Some(log_id(1, 9)),
            "stopped at step {stop} of {steps}"
        );
    }
}

/// A committed id lowered to the log's end names an earlier entry of the
/// same log, so that entry's term is at most the saved one. A higher term
/// there is some other log's entry and is not taken as committed.
#[tokio::test]
async fn a_lowered_committed_id_is_not_a_later_terms_entry() {
    let ahead = tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..10).map(|index| normal(1, index, "a")).collect();
    entries.push(normal(5, 10, "a"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(5, 10)))
        .await
        .expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    entries.truncate(9);
    entries.push(normal(7, 9, "b"));
    append_flushed(&mut raft, &entries).await;
    raft.disk_log().close().await.expect("close");
    copy_file(&ahead, &dir, "replica");

    let mut raft = open(&dir);
    assert_eq!(raft.read_committed().await.expect("read"), None);
    assert_eq!(raft.disk_log().commit_offset(), 0);
    raft.truncate_after(Some(log_id(1, 8))).await.expect("cut");
}

/// `read_committed` compares the saved term with the entry on every call.
/// The commit offset of a Raft log moves only with a term, so this takes a
/// test hook to get wrong.
#[tokio::test]
async fn read_committed_refuses_an_offset_moved_without_its_term() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &mixed()).await;
    raft.save_committed(Some(log_id(1, 5))).await.expect("save");
    raft.disk_log()
        .advance_commit_offset(11)
        .await
        .expect_err("a Raft log's commit offset takes a term");
    raft.disk_log().note_commit_offset(11);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 5))
    );

    raft.disk_log().move_commit_offset_for_tests(11);
    let err = raft.read_committed().await.expect_err("index 10 is term 3");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

/// An offline restore lowers the commit offset to an entry the log holds,
/// and saves that entry's term with it, so the next open keeps the id.
#[tokio::test]
async fn a_restore_keeps_a_committed_id_the_next_open_accepts() {
    let dir = tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &mixed()).await;
    raft.save_committed(Some(log_id(3, 10)))
        .await
        .expect("save");
    raft.disk_log().restore_to(6).await.expect("restore");
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 5))
    );
    raft.disk_log().close().await.expect("close");
    drop(raft);
    let mut raft = open(&dir);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 5))
    );
}
