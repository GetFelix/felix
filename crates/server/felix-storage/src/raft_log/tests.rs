//! The read side against logs written the way the write side will write
//! them: one v9 record per entry, stamped with the entry's term plus one.

#[cfg(feature = "sim")]
mod crash_points;
mod flush_queue;
#[cfg(feature = "sim")]
mod install_crash_points;
mod machine;
mod purge;
mod snapshots;
mod suite;
mod tail;
mod writes;

use std::io;

use bytes::Bytes;
use openraft::entry::{Entry, EntryPayload, RaftEntry};
use openraft::storage::RaftLogReader;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};
use tempfile::{TempDir, tempdir};

use super::codec::{entry_from_record, record_from_entry};
use super::{EntryCodec, RaftLog};
use crate::DiskLog;
use crate::disk_log::WriterAt;
use crate::log::{
    AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, LogRecord, ReadRange, RecordMark,
    UNKNOWN_WRITER,
};

openraft::declare_raft_types!(
    pub(super) Types:
        D = String,
        R = String,
        NodeId = u64,
        Node = openraft::impls::EmptyNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);

type Membership = openraft::impls::Membership<u64, openraft::impls::EmptyNode>;
type TestEntry = Entry<CommittedLeaderIdOf<Types>, EntryPayload<String, Membership>>;

#[derive(Clone, Copy)]
pub(super) struct Codec;

impl EntryCodec<Types> for Codec {
    fn encode_data(&self, data: &String) -> io::Result<AppendRecord> {
        Ok(record(RecordMark::None, data.as_bytes()))
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<String> {
        String::from_utf8(record.payload.to_vec()).map_err(io::Error::other)
    }

    fn encode_membership(&self, membership: &Membership) -> io::Result<Bytes> {
        serde_json::to_vec(membership)
            .map(Bytes::from)
            .map_err(io::Error::other)
    }

    fn decode_membership(&self, payload: &[u8]) -> io::Result<Membership> {
        serde_json::from_slice(payload).map_err(io::Error::other)
    }

    fn encode_node_id(&self, node_id: &u64) -> String {
        node_id.to_string()
    }

    fn decode_node_id(&self, leader: &str) -> io::Result<u64> {
        leader.parse().map_err(io::Error::other)
    }
}

fn record(mark: RecordMark, payload: &[u8]) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload),
        timestamp_micros: 1_700_000_000,
        mark,
        publisher: None,
        headers: None,
    }
}

/// Small segments and pages, so a read crosses both.
fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 200,
        index_spacing_bytes: 48,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        max_records_per_read: 3,
        raft_log: true,
        ..LogConfig::default()
    }
}

pub(super) type PagePause = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

pub(super) fn open(dir: &TempDir) -> RaftLog<Types, Codec> {
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    RaftLog::new(log, Codec).expect("a Raft log")
}

fn log_id(term: u64, index: u64) -> LogIdOf<Types> {
    LogIdOf::<Types>::new(term.into(), index)
}

fn blank(term: u64, index: u64) -> TestEntry {
    TestEntry::new_blank(log_id(term, index))
}

fn normal(term: u64, index: u64, data: &str) -> TestEntry {
    TestEntry::new_normal(log_id(term, index), data.to_string())
}

fn membership(term: u64, index: u64) -> TestEntry {
    let voters = [(1, Default::default()), (2, Default::default())];
    TestEntry::new_membership(
        log_id(term, index),
        Membership::from(std::collections::BTreeMap::from(voters)),
    )
}

/// Store `entries` at the tail, each run of one term in one segment. Stands
/// in for the write side until it lands.
pub(super) async fn store(raft: &RaftLog<Types, Codec>, entries: &[TestEntry]) -> io::Result<()> {
    let log = raft.disk_log();
    for run in
        entries.chunk_by(|a, b| a.log_id.committed_leader_id() == b.log_id.committed_leader_id())
    {
        let mut tail = log.tail_offset().await.map_err(io::Error::other)?;
        // The suite starts some logs past index 0. An empty log can start
        // anywhere.
        if tail == log.base_offset() && run[0].log_id.index() > tail {
            tail = run[0].log_id.index();
            log.reset_to(tail).await.map_err(io::Error::other)?;
        }
        assert_eq!(
            run[0].log_id.index(),
            tail,
            "an entry's index is its offset"
        );
        let mut records = Vec::new();
        let mut generation = 0;
        for entry in run {
            let (record, stamped) =
                record_from_entry::<Types, Codec>(&Codec, entry, 1_700_000_000)?;
            generation = stamped;
            records.push(record);
        }
        log.append_v9_for_tests(&records, generation)
            .await
            .map_err(io::Error::other)?;
        // The hook never rolls on size, so each run gets its own segment.
        log.roll_now().await.map_err(io::Error::other)?;
    }
    log.sync().await.map_err(io::Error::other)
}

async fn all(raft: &RaftLog<Types, Codec>) -> io::Result<Vec<TestEntry>> {
    raft.reader().try_get_log_entries(..).await
}

/// Each kind of entry reads back as it was stored, at its index and term,
/// across segments and read pages, and a reopen changes nothing.
#[tokio::test]
async fn entries_read_back_with_their_term_and_kind() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let mut entries = vec![membership(0, 0), blank(1, 1)];
    entries.extend((2..12).map(|index| normal(1, index, &format!("command-{index}"))));
    entries.extend([blank(4, 12), membership(4, 13), normal(4, 14, "last")]);
    store(&raft, &entries).await.expect("store");
    assert!(
        raft.disk_log().segments().len() > 2,
        "one segment proves little"
    );

    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(
        raft.reader()
            .try_get_log_entries(3..=12)
            .await
            .expect("read"),
        entries[3..=12]
    );
    let state = raft.log_state().await.expect("state");
    assert_eq!(state.last_log_id, Some(log_id(4, 14)));
    assert_eq!(state.last_purged_log_id, None);

    raft.disk_log().close().await.expect("close");
    drop(raft);
    let raft = open(&dir);
    assert_eq!(all(&raft).await.expect("read"), entries);
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(4, 14))
    );
}

/// The safety call: a blank or membership entry takes an offset, since the
/// offset is the Raft index, but no client read returns it. Read the way
/// clients read (`read_range`, keeping `is_data` records, as the broker's
/// `StreamLog::read_from` does), the events come back at their own offsets,
/// and every offset between two of them is a skipped entry, which
/// `skipped_before` reports. A consumer sees dense offsets once skips are
/// counted, so any other jump is still a drop.
#[tokio::test]
async fn blank_and_membership_entries_are_records_clients_skip() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let entries = [
        membership(0, 0),
        blank(1, 1),
        normal(1, 2, "a"),
        membership(1, 3),
        normal(1, 4, "b"),
        normal(1, 5, "c"),
        blank(2, 6),
        membership(2, 7),
        membership(2, 8),
        normal(2, 9, "d"),
        blank(3, 10),
    ];
    store(&raft, &entries).await.expect("store");
    let tail = raft.disk_log().tail_offset().await.expect("tail");
    assert_eq!(tail, 11);

    // (offset, payload, skipped_before) for each event a consumer receives.
    let mut delivered = Vec::new();
    let mut next = 0;
    while next < tail {
        let page = raft
            .disk_log()
            .read_range(ReadRange {
                start: next,
                // Paged by record count only: a byte bound can skip records
                // across a segment end (#2218).
                max_bytes: usize::MAX,
            })
            .await
            .expect("read");
        assert!(page.next > next, "the read moves forward");
        for record in page.records.into_iter().filter(|r| r.mark.is_data()) {
            let skipped_before = record.offset - delivered.last().map_or(0, |(at, _, _)| at + 1);
            let payload = String::from_utf8(record.payload.to_vec()).expect("utf-8");
            delivered.push((record.offset, payload, skipped_before));
        }
        next = page.next;
    }

    let events: Vec<_> = entries
        .iter()
        .filter_map(|entry| match &entry.payload {
            EntryPayload::Normal(data) => Some((entry.log_id.index(), data.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        delivered
            .iter()
            .map(|(at, data, _)| (*at, data.clone()))
            .collect::<Vec<_>>(),
        events,
        "only the commands reach a consumer, at their own indexes"
    );
    assert_eq!(
        delivered
            .iter()
            .map(|(_, _, skipped)| *skipped)
            .collect::<Vec<_>>(),
        [2, 1, 0, 3],
        "each skip counts exactly the blank and membership entries before it"
    );
}

/// A limited read may stop at a page, but returns the start of the range.
#[tokio::test]
async fn a_limited_read_returns_a_prefix() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let entries: Vec<_> = (0..10).map(|index| normal(2, index, "x")).collect();
    store(&raft, &entries).await.expect("store");
    let read = raft
        .reader()
        .limited_get_log_entries(1, 9)
        .await
        .expect("read");
    assert!(!read.is_empty() && read.len() < 8, "{}", read.len());
    assert_eq!(read, entries[1..1 + read.len()]);
}

/// Generation 0 in a record header means the writer is unknown. openraft
/// uses term 0 for its first entry, so reading such a record as a term-0
/// entry would hand a voter a term nobody wrote. It is an error instead.
#[tokio::test]
async fn an_unknown_writer_is_never_a_term_zero_entry() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    store(&raft, &[blank(0, 0)]).await.expect("store");
    raft.disk_log()
        .append_v9_for_tests(&[record(RecordMark::None, b"copied")], UNKNOWN_WRITER)
        .await
        .expect("append");
    assert_eq!(
        raft.disk_log().writer_at(1).await.expect("writer"),
        WriterAt::Generation(UNKNOWN_WRITER)
    );

    for err in [
        raft.log_state().await.expect_err("state"),
        all(&raft).await.expect_err("read"),
        raft.reader()
            .try_get_log_entries(1..2)
            .await
            .expect_err("read"),
    ] {
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("unknown"), "{err}");
    }
    // The entry below it is still an entry.
    assert_eq!(
        raft.reader().try_get_log_entries(0..1).await.expect("read"),
        [blank(0, 0)]
    );
}

/// A record in a segment older than v9 has no generation at all, and is no
/// entry either.
#[tokio::test]
async fn a_record_without_a_generation_is_no_entry() {
    let dir = tempdir().expect("dir");
    // Written before the directory became a Raft log's.
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
    let raft = open(&dir);
    assert_eq!(
        raft.disk_log().writer_at(0).await.expect("writer"),
        WriterAt::Unstamped
    );
    assert_eq!(
        raft.log_state().await.expect_err("state").kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        all(&raft).await.expect_err("read").kind(),
        io::ErrorKind::InvalidData
    );
}

/// A record below the log's first index is never an entry.
#[tokio::test]
async fn a_record_below_the_first_index_is_no_entry() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    store(&raft, &[blank(1, 0), normal(1, 1, "a")])
        .await
        .expect("store");
    let records = raft
        .disk_log()
        .read_range_with_trims(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read");
    let err = entry_from_record::<Types, Codec>(&Codec, records[0].clone(), 1).expect_err("below");
    assert!(
        err.to_string().contains("below the log's first index"),
        "{err}"
    );
    entry_from_record::<Types, Codec>(&Codec, records[1].clone(), 1).expect("at the first index");

    // A log placed at a base holds nothing below it, and reads that way.
    let placed = tempdir().expect("dir");
    let log = DiskLog::open_at(placed.path(), "t/ns/raft/1", config(), 5).expect("open");
    let raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");
    assert_eq!(raft.log_state().await.expect("state").last_log_id, None);
    store(&raft, &[blank(2, 5), normal(2, 6, "a")])
        .await
        .expect("store");
    assert_eq!(
        all(&raft).await.expect("read"),
        [blank(2, 5), normal(2, 6, "a")]
    );
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(2, 6))
    );
}

/// A generation-start record whose payload names another generation than
/// its header was not written as that term's blank entry.
#[tokio::test]
async fn a_blank_entry_must_name_its_own_generation() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    raft.disk_log()
        .append_v9_for_tests(
            &[record(RecordMark::GenerationStart, &7u64.to_be_bytes())],
            3,
        )
        .await
        .expect("append");
    let err = all(&raft).await.expect_err("read");
    assert!(err.to_string().contains("another generation"), "{err}");
}

/// The vote is the ballot: generation minus one is the term, the leader is
/// the node, and a generation given up is not a vote.
#[tokio::test]
async fn the_vote_is_read_from_the_ballot() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let mut reader = raft.reader();
    assert_eq!(reader.read_vote().await.expect("vote"), None);

    raft.disk_log()
        .accept_generation(1, Some("7"))
        .await
        .expect("accept");
    let vote = reader.read_vote().await.expect("vote").expect("some");
    assert_eq!(
        (
            vote.leader_id.term,
            vote.leader_id.voted_for,
            vote.committed
        ),
        (0, 7, false)
    );

    raft.disk_log()
        .commit_vote_for_tests(4, "2")
        .await
        .expect("commit");
    let vote = reader.read_vote().await.expect("vote").expect("some");
    assert_eq!(
        (
            vote.leader_id.term,
            vote.leader_id.voted_for,
            vote.committed
        ),
        (3, 2, true)
    );

    raft.disk_log().give_up_generation().await.expect("give up");
    assert_eq!(
        reader.read_vote().await.expect_err("given up").kind(),
        io::ErrorKind::InvalidData
    );
}

/// `writer_at` answers from the record header, in sealed and active
/// segments, and says when there is no record to ask about.
#[tokio::test]
async fn the_writer_of_a_record_comes_from_its_header() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let log = raft.disk_log();
    assert_eq!(log.last_writer().await.expect("last"), None);
    let mut entries: Vec<_> = (0..6).map(|index| normal(1, index, "0123456789")).collect();
    entries.extend((6..12).map(|index| normal(5, index, "0123456789")));
    store(&raft, &entries).await.expect("store");
    assert!(log.segments().len() > 2);

    for offset in 0..12 {
        let generation = if offset < 6 { 2 } else { 6 };
        assert_eq!(
            log.writer_at(offset).await.expect("writer"),
            WriterAt::Generation(generation),
            "offset {offset}"
        );
    }
    assert_eq!(log.writer_at(12).await.expect("writer"), WriterAt::Absent);
    assert_eq!(
        log.last_writer().await.expect("last"),
        Some((11, WriterAt::Generation(6)))
    );

    log.truncate(6).await.expect("truncate");
    assert_eq!(log.writer_at(6).await.expect("writer"), WriterAt::Absent);
    assert_eq!(
        log.last_writer().await.expect("last"),
        Some((5, WriterAt::Generation(2)))
    );
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        Some(log_id(1, 5))
    );
}

/// A read that spans pages must not join records from before a truncation
/// to ones written after it: the entries it returns are one log's.
#[tokio::test]
async fn a_paged_read_restarts_when_the_log_is_cut_under_it() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let old: Vec<_> = (0..8).map(|index| normal(1, index, "old")).collect();
    store(&raft, &old).await.expect("store");

    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *raft.shared.pause_after_page.lock() = Some((reached_tx, resume_rx));
    let mut reader = raft.reader();
    let read = tokio::spawn(async move { reader.try_get_log_entries(..).await });
    reached.await.expect("the read took its first page");

    // Entries 1.. are replaced by another leader's while the read is held.
    raft.disk_log().truncate(1).await.expect("truncate");
    let new: Vec<_> = (1..6).map(|index| normal(2, index, "new")).collect();
    store(&raft, &new).await.expect("store");
    resume.send(()).expect("resume");

    let mut expected = vec![old[0].clone()];
    expected.extend(new);
    assert_eq!(read.await.expect("join").expect("read"), expected);
}

/// Removing the head under a paged read leaves the read's next page below
/// the oldest segment. openraft takes a range missing its start, so the read
/// starts again from the new head instead of failing.
#[tokio::test]
async fn a_paged_read_restarts_when_the_head_is_removed_under_it() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let mut entries: Vec<_> = (0..10).map(|index| normal(1, index, "old")).collect();
    entries.extend((10..14).map(|index| normal(2, index, "new")));
    store(&raft, &entries).await.expect("store");

    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *raft.shared.pause_after_page.lock() = Some((reached_tx, resume_rx));
    let mut reader = raft.reader();
    let read = tokio::spawn(async move { reader.try_get_log_entries(3..).await });
    reached.await.expect("the read took its first page");

    raft.disk_log().trim_before(10).await.expect("trim");
    assert_eq!(raft.disk_log().base_offset(), 10);
    resume.send(()).expect("resume");

    assert_eq!(read.await.expect("join").expect("read"), entries[10..]);
}

/// A record the byte budget refuses ends the read. The next segment's first
/// record fits what is left of the budget, and returning it would leave a
/// hole where the refused record is.
#[tokio::test]
async fn a_read_stops_at_a_record_its_byte_budget_refuses() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let wide = "w".repeat(40);
    let entries = [
        blank(1, 0),
        normal(1, 1, &wide),
        normal(1, 2, &wide),
        normal(1, 3, "d"),
        blank(2, 4),
    ];
    store(&raft, &entries).await.expect("store");

    let read = |start| {
        let log = raft.disk_log().clone();
        async move {
            let range = ReadRange {
                start,
                max_bytes: 64,
            };
            let page = log.read_range_with_trims(range).await.expect("read");
            page.iter().map(|record| record.offset).collect::<Vec<_>>()
        }
    };
    assert_eq!(read(0).await, [0, 1], "offset 2 does not fit");
    assert_eq!(read(2).await, [2, 3, 4]);
}

/// The same through the reader, whose pages are bounded in bytes: a page
/// that ends on a record too large for it is followed by one that starts
/// there.
#[tokio::test]
async fn a_paged_read_crosses_a_record_too_large_for_its_page() {
    let dir = tempdir().expect("dir");
    let raft = open(&dir);
    let entries = [
        normal(1, 0, &"a".repeat(3 << 20)),
        normal(1, 1, &"b".repeat(2 << 20)),
        blank(2, 2),
    ];
    store(&raft, &entries).await.expect("store");
    assert_eq!(all(&raft).await.expect("read"), entries);
}
