//! The written-at-index signal: a token on a command resolves with the
//! entry's index and term once the entry is written, before the fsync.

use std::fmt;

use futures_util::FutureExt;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use serde::{Deserialize, Serialize};

use super::*;
use crate::raft_log::{LogPoint, LogPositions, WrittenAt, WrittenToken};

/// A command shaped like a shard's: what is stored, and a token that is not.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Proposed {
    body: String,
    #[serde(skip)]
    written: Option<WrittenToken>,
}

impl fmt::Display for Proposed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.body)
    }
}

openraft::declare_raft_types!(
    pub(crate) TokenTypes:
        D = Proposed,
        R = String,
        NodeId = u64,
        Node = openraft::impls::EmptyNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);

type TokenEntry = Entry<CommittedLeaderIdOf<TokenTypes>, EntryPayload<Proposed, Membership>>;
type TokenLog = RaftLog<TokenTypes, TokenCodec>;

#[derive(Clone, Copy)]
struct TokenCodec;

impl EntryCodec<TokenTypes> for TokenCodec {
    fn encode_data(&self, data: &Proposed) -> io::Result<AppendRecord> {
        Ok(record(RecordMark::None, data.body.as_bytes()))
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<Proposed> {
        Ok(Proposed {
            body: Codec.decode_data(record)?,
            written: None,
        })
    }

    fn written_token(&self, data: &Proposed) -> Option<WrittenToken> {
        data.written.clone()
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

fn token_log(log: DiskLog) -> TokenLog {
    RaftLog::new(log, TokenCodec).expect("a Raft log")
}

fn id(term: u64, index: u64) -> LogIdOf<TokenTypes> {
    LogIdOf::<TokenTypes>::new(term.into(), index)
}

fn plain(term: u64, index: u64, body: &str) -> TokenEntry {
    let command = Proposed {
        body: body.to_string(),
        written: None,
    };
    TokenEntry::new_normal(id(term, index), command)
}

/// An entry whose proposer waits to hear where it was written.
fn proposed(term: u64, index: u64, body: &str) -> (TokenEntry, WrittenAt) {
    let (token, written) = WrittenToken::channel();
    let command = Proposed {
        body: body.to_string(),
        written: Some(token),
    };
    (TokenEntry::new_normal(id(term, index), command), written)
}

/// Resolves to what an append's callback was given.
type Flushed = std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// Append, returning the callback's receiver once the append has returned.
async fn append(raft: &mut TokenLog, entries: Vec<TokenEntry>) -> io::Result<Flushed> {
    let (tx, rx) = TokenTypes::oneshot();
    raft.append(entries, IOFlushed::signal(tx)).await?;
    Ok(Box::pin(async move {
        rx.await.expect("the callback was dropped")
    }))
}

async fn append_flushed(raft: &mut TokenLog, entries: Vec<TokenEntry>) {
    let flushed = append(raft, entries).await.expect("append");
    flushed.await.expect("flush");
}

fn at(term: u64, index: u64) -> Option<LogPoint> {
    Some(LogPoint { term, index })
}

/// With the flush held, each token already has its own entry's index and
/// term and the log's written position has moved, while the callback and
/// the flushed position wait for the fsync.
#[tokio::test]
async fn a_token_resolves_once_written_and_before_the_flush() {
    let dir = tempdir().expect("dir");
    let mut raft = token_log(DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open"));
    let log = raft.disk_log().clone();
    let positions = raft.positions();
    append_flushed(&mut raft, vec![TokenEntry::new_blank(id(1, 0))]).await;

    let held = log.hold_flushes().await;
    let (first, first_at) = proposed(1, 1, "a");
    let (last, last_at) = proposed(1, 3, "c");
    let mut flushed = append(&mut raft, vec![first, plain(1, 2, "b"), last])
        .await
        .expect("append");

    assert_eq!(first_at.now_or_never(), Some(at(1, 1)));
    assert_eq!(last_at.now_or_never(), Some(at(1, 3)));
    let waiting = LogPositions {
        written: at(1, 3),
        flushed: at(1, 0),
    };
    assert_eq!(*positions.borrow(), waiting);
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        flushed.as_mut().now_or_never().is_none(),
        "the callback fired before the flush"
    );
    assert_eq!(*positions.borrow(), waiting);

    drop(held);
    flushed.await.expect("flush");
    assert_eq!(positions.borrow().flushed, at(1, 3));
}

/// An append larger than a segment goes down as several batches. A token
/// on its last entry resolves once, with that entry's index, and a second
/// append of other entries does not disturb it.
#[tokio::test]
async fn a_token_on_an_append_of_several_batches_resolves_with_its_own_index() {
    let dir = tempdir().expect("dir");
    let mut raft = token_log(DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open"));
    let body = "x".repeat(60);
    let mut entries: Vec<_> = (0..8).map(|index| plain(2, index, &body)).collect();
    let (early, early_at) = proposed(2, 8, &body);
    let (last, last_at) = proposed(2, 9, &body);
    entries.extend([early, last]);

    append_flushed(&mut raft, entries).await;
    assert!(
        raft.disk_log().segments().len() > 2,
        "the append must span batches"
    );
    append_flushed(&mut raft, vec![plain(2, 10, "after")]).await;
    assert_eq!(early_at.await, at(2, 8));
    assert_eq!(last_at.await, at(2, 9));
}

/// A token whose append failed, or whose command never reached the log,
/// resolves as not written.
#[tokio::test]
async fn a_failed_append_resolves_no_token_as_written() {
    let dir = tempdir().expect("dir");
    let mut raft = token_log(DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open"));
    append_flushed(&mut raft, vec![TokenEntry::new_blank(id(1, 0))]).await;

    // Does not continue the log.
    let (skips, skips_at) = proposed(1, 5, "skips");
    assert!(append(&mut raft, vec![skips]).await.is_err(), "a gap");
    assert_eq!(skips_at.await, None);

    let (dropped, dropped_at) = proposed(1, 1, "dropped");
    drop(dropped);
    assert_eq!(dropped_at.await, None);

    // A failed flush poisons the log, so the next write fails.
    raft.disk_log().fail_next_flush();
    let (unflushed, unflushed_at) = proposed(1, 1, "unflushed");
    let flushed = append(&mut raft, vec![unflushed]).await.expect("append");
    flushed.await.expect_err("the flush failed");
    assert_eq!(
        unflushed_at.await,
        at(1, 1),
        "written, though never flushed"
    );
    let (refused, refused_at) = proposed(1, 2, "refused");
    assert!(
        append(&mut raft, vec![refused]).await.is_err(),
        "an append on a poisoned log"
    );
    assert_eq!(refused_at.await, None);
}

/// The token is the proposer's alone: it is not in the record, and an entry
/// read back has none.
#[tokio::test]
async fn a_token_is_never_stored() {
    let dir = tempdir().expect("dir");
    let mut raft = token_log(DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open"));
    let (entry, written) = proposed(1, 0, "a");
    let EntryPayload::Normal(command) = &entry.payload else {
        unreachable!()
    };
    assert_eq!(
        serde_json::to_string(command).expect("json"),
        r#"{"body":"a"}"#
    );
    append_flushed(&mut raft, vec![entry]).await;
    assert_eq!(written.await, at(1, 0));

    let read = raft.reader().try_get_log_entries(..).await.expect("read");
    let [EntryPayload::Normal(command)] = [&read[0].payload] else {
        panic!("not a command");
    };
    assert_eq!(command.body, "a");
    assert!(command.written.is_none());
}

/// Power is cut after the token resolved and before the flush. Written was
/// signalled every time, the callback never fired, and what reopens is a
/// prefix that keeps everything flushed. On some seed the written entry is
/// gone, which is why nothing but an acknowledgement may rest on the token.
#[cfg(feature = "sim")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_after_written_and_before_the_flush_can_take_the_entry() {
    use super::crash_points::{Sim, config};
    use crate::sim_fs;

    let open = |sim: &Sim| {
        token_log(DiskLog::open(sim.0.join("log"), "t/ns/raft/0", config()).expect("open"))
    };
    let mut lost = 0;
    for seed in 0..8 {
        let sim = Sim::new("written", seed);
        let mut raft = open(&sim);
        append_flushed(
            &mut raft,
            (0..4).map(|index| plain(1, index, "flushed")).collect(),
        )
        .await;

        let log = raft.disk_log().clone();
        let held = log.hold_flushes().await;
        let (entry, written) = proposed(1, 4, &"w".repeat(600));
        let mut flushed = append(&mut raft, vec![entry]).await.expect("append");
        assert_eq!(written.now_or_never(), Some(at(1, 4)), "seed {seed}");
        assert!(flushed.as_mut().now_or_never().is_none(), "seed {seed}");
        sim_fs::crash(&sim.0, seed).expect("crash");
        // The process died with the cut: nothing after it counts.
        drop(held);
        drop(flushed);
        drop(raft);

        let raft = open(&sim);
        let found = raft.reader().try_get_log_entries(..).await.expect("read");
        assert!(
            (4..=5).contains(&found.len()),
            "seed {seed}: {}",
            found.len()
        );
        for (index, entry) in found.iter().enumerate() {
            assert_eq!(entry.log_id, id(1, index as u64), "seed {seed}");
        }
        lost += usize::from(found.len() == 4);
        raft.disk_log().shutdown().await.expect("shutdown");
    }
    assert!(lost > 0, "no seed lost the written entry");
}
