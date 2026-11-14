//! Starting a voter on a directory the hybrid wrote: what is checked, what
//! is cut, what is written, and that a crash before the engine marker is
//! started again from the beginning with the same result.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::disk_log::{Member, Membership, ReplicaSet};
use crate::log::{AppendRecord, FsyncMode, ProducerBatch};
use crate::raft_log::RaftLog;
use crate::raft_log::tests::{Codec, Types, all, blank, log_id, store};

const LABEL: &str = "t/ns/s/0";
/// The seal's generation.
const SEAL: u64 = 5;
const VOTE_FOR: &str = "b#7";

fn hybrid(gate_open: &Arc<AtomicBool>) -> LogConfig {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let open = Arc::clone(gate_open);
    config
        .generations
        .open_when(move || open.load(Ordering::Acquire));
    config
}

fn open_config() -> LogConfig {
    hybrid(&Arc::new(AtomicBool::new(true)))
}

fn raft() -> LogConfig {
    LogConfig {
        raft_log: true,
        ..open_config()
    }
}

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 7,
        mark: RecordMark::None,
        publisher: None,
        headers: None,
    }
}

async fn lead(log: &DiskLog, generation: u64) {
    log.accept_generation(generation, Some("a"))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
    log.lead_at(generation);
}

/// How a test copy was written below the seal.
#[derive(Clone, Copy, PartialEq)]
enum Below {
    /// Every record carries the generation that wrote it.
    Stamped,
    /// The first records sit in a segment from before generations were
    /// kept, so this copy has no generation where another has one.
    Unstamped,
}

/// A hybrid copy holding `payloads` at offsets 0.., the seal's
/// generation-start record of generation [`SEAL`] after them, and `tail`
/// above it: records a leader wrote that were never acknowledged.
async fn copy(dir: &Path, below: Below, payloads: &[&str], tail: &[&str]) -> Offset {
    let gate = Arc::new(AtomicBool::new(below == Below::Stamped));
    let log = DiskLog::open(dir, LABEL, hybrid(&gate)).expect("open");
    let (first, rest) = payloads.split_at(payloads.len() / 2);
    if below == Below::Stamped {
        lead(&log, 1).await;
    }
    log.append(&first.iter().map(|p| record(p)).collect::<Vec<_>>())
        .await
        .expect("append");
    if below == Below::Unstamped {
        gate.store(true, Ordering::Release);
        log.roll_now().await.expect("roll");
        lead(&log, 1).await;
    }
    log.append(&rest.iter().map(|p| record(p)).collect::<Vec<_>>())
        .await
        .expect("append");
    log.accept_set(ReplicaSet {
        current: Membership {
            generation: 1,
            members: ["a", "b", "c"]
                .into_iter()
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .collect(),
        },
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("set");
    lead(&log, SEAL).await;
    let n = log.tail_offset().await.expect("tail");
    let base = SwitchBase {
        generation: SEAL,
        offset: n,
        start: 0,
        digest: digest(&log, 0, n).await.expect("digest"),
        dead_letters: empty_side(),
        cursors: empty_side(),
    };
    let seal = log
        .append(&[base.record(7)])
        .await
        .expect("seal")
        .first_offset;
    if !tail.is_empty() {
        log.append(&tail.iter().map(|p| record(p)).collect::<Vec<_>>())
            .await
            .expect("tail");
    }
    log.shutdown().await.expect("shutdown");
    seal
}

async fn base_of(dir: &Path, offset: Offset) -> SwitchBase {
    let log = DiskLog::open(dir, LABEL, open_config()).expect("open");
    let at = log
        .read_range_with_trims(ReadRange {
            start: offset,
            max_bytes: 1,
        })
        .await
        .expect("read");
    log.shutdown().await.expect("shutdown");
    SwitchBase::from_record(&at[0]).expect("a seal record")
}

/// A side log with nothing in it.
pub(super) fn empty_side() -> SideRange {
    SideRange {
        start: 0,
        end: 0,
        digest: RangeDigest(Sha256::new().finalize().into()),
    }
}

/// Empty side logs beside `dir`.
pub(super) fn side_dirs(dir: &Path) -> [(std::path::PathBuf, LogConfig); 2] {
    ["dead-letters", "cursors"].map(|kind| (dir.with_extension(kind), open_config()))
}

pub(super) fn sides(dirs: &[(std::path::PathBuf, LogConfig); 2]) -> [SideDir<'_>; 2] {
    [0, 1].map(|i| SideDir {
        dir: &dirs[i].0,
        label: LABEL,
        config: dirs[i].1.clone(),
    })
}

fn snapshot(base: &SwitchBase) -> StoredSnapshot {
    StoredSnapshot {
        last: Some(SnapshotId {
            term: base.term().expect("term"),
            index: base.offset,
        }),
        meta: Bytes::from_static(b"{}"),
        data: Bytes::new(),
    }
}

async fn start(dir: &Path, base: &SwitchBase) -> std::result::Result<(), NotAVoter> {
    let side = side_dirs(dir);
    start_voter(
        dir,
        LABEL,
        open_config(),
        base,
        sides(&side),
        VOTE_FOR,
        snapshot(base),
    )
    .await
    .expect("start")
}

/// The client records from `from` on, without the generation-start ones.
async fn payloads(log: &DiskLog, from: Offset) -> Vec<String> {
    log.read_range(ReadRange {
        start: from,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .records
    .into_iter()
    .filter(|record| record.mark == RecordMark::None)
    .map(|record| String::from_utf8_lossy(&record.payload).into_owned())
    .collect()
}

fn files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("dir")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn the_digest_leaves_out_the_generation_and_nothing_else() {
    let root = tempdir().expect("tempdir");
    let stamped = root.path().join("stamped");
    let unstamped = root.path().join("unstamped");
    let n = copy(&stamped, Below::Stamped, &["a", "b", "c", "d"], &[]).await;
    copy(&unstamped, Below::Unstamped, &["a", "b", "c", "d"], &[]).await;
    assert_eq!(base_of(&stamped, n).await, base_of(&unstamped, n).await);

    let other = root.path().join("other");
    copy(&other, Below::Stamped, &["a", "b", "x", "d"], &[]).await;
    assert_ne!(base_of(&stamped, n).await, base_of(&other, n).await);

    // A copy that differs only in a record's producer mark, publisher or
    // headers holds a different record.
    let digest_of = |change: fn(&mut AppendRecord)| async move {
        let dir = tempdir().expect("tempdir");
        let log = DiskLog::open(dir.path(), LABEL, open_config()).expect("open");
        lead(&log, 1).await;
        let mut changed = record("a");
        change(&mut changed);
        log.append(&[changed]).await.expect("append");
        let digest = digest(&log, 0, 1).await.expect("digest");
        log.shutdown().await.expect("shutdown");
        digest
    };
    let plain = digest_of(|_| {}).await;
    let marked = digest_of(|r| {
        r.mark = RecordMark::Opens(ProducerBatch {
            producer_id: 3,
            sequence: 0,
            len: 1,
        })
    })
    .await;
    let published = digest_of(|r| r.publisher = Some(Bytes::from_static(b"alice"))).await;
    let headed = digest_of(|r| r.headers = Some(Box::new(Bytes::from_static(b"k")))).await;
    let empty_headers = digest_of(|r| r.headers = Some(Box::new(Bytes::new()))).await;
    let timed = digest_of(|r| r.timestamp_micros = 8).await;
    let all = [plain, marked, published, headed, empty_headers, timed];
    for (i, a) in all.iter().enumerate() {
        for b in &all[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[tokio::test]
async fn a_digest_refuses_a_range_the_log_does_not_hold() {
    let root = tempdir().expect("tempdir");
    let n = copy(root.path(), Below::Stamped, &["a", "b"], &[]).await;
    let log = DiskLog::open(root.path(), LABEL, open_config()).expect("open");
    assert!(digest(&log, 0, n + 5).await.is_err());
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_voter_keeps_the_records_below_the_base_and_starts_after_it() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let n = copy(&dir, Below::Unstamped, &["a", "b", "c", "d"], &["unacked"]).await;
    let base = base_of(&dir, n).await;
    assert!(files(&dir).contains(&"replica-set".to_string()));

    assert_eq!(start(&dir, &base).await, Ok(()));
    assert_eq!(
        engine_marker::switched_base(&dir, LABEL).expect("marker"),
        Some((SEAL, n))
    );
    assert!(!files(&dir).contains(&"replica-set".to_string()));

    let log = DiskLog::open(&dir, LABEL, raft()).expect("open as a Raft log");
    // The record above the base was never acknowledged, and is gone.
    assert_eq!(log.tail_offset().await.expect("tail"), n + 1);
    // Records are not rewritten: the ones below the base read as before.
    assert_eq!(payloads(&log, 0).await, ["a", "b", "c", "d"]);
    // The member starts after the base: the purged id is the base's entry,
    // and the vote is at the base's term.
    assert_eq!(
        crate::raft_log::last_point(&log).await.expect("last"),
        Some(crate::raft_log::LogPoint {
            term: SEAL - 1,
            index: n,
        })
    );
    assert_eq!(
        log.accepted_vote(),
        (SEAL, Some(Arc::from(VOTE_FOR)), false)
    );
    assert_eq!(log.committed(), (n + 1, SEAL));

    // As a Raft log it holds no entry: the records up to the base, the
    // unstamped ones included, are below its first index and are never
    // read as entries, and the next entry goes at the base plus one.
    let raft = RaftLog::<Types, Codec>::new(log.clone(), Codec).expect("a Raft log");
    let state = raft.log_state().await.expect("state");
    assert_eq!(state.last_purged_log_id, Some(log_id(SEAL - 1, n)));
    assert_eq!(state.last_log_id, Some(log_id(SEAL - 1, n)));
    assert_eq!(all(&raft).await.expect("read"), []);
    store(&raft, &[blank(SEAL, n + 1)])
        .await
        .expect("append after the base");
    assert_eq!(all(&raft).await.expect("read"), [blank(SEAL, n + 1)]);
    assert_eq!(payloads(&log, 0).await, ["a", "b", "c", "d"]);
    log.shutdown().await.expect("shutdown");

    // A marked directory is a started member: starting it again changes
    // nothing, and the hybrid refuses it.
    let before = files(&dir);
    assert_eq!(start(&dir, &base).await, Ok(()));
    assert_eq!(files(&dir), before);
    assert!(DiskLog::open(&dir, LABEL, open_config()).is_err());
}

#[tokio::test]
async fn a_crash_before_the_marker_starts_the_voter_again_with_the_same_result() {
    let root = tempdir().expect("tempdir");
    let reference = root.path().join("reference");
    let n = copy(&reference, Below::Stamped, &["a", "b", "c"], &["unacked"]).await;
    let base = base_of(&reference, n).await;
    start(&reference, &base).await.expect("voter");
    let expected = files(&reference);

    // A crash after each step of the start leaves no marker, so whatever
    // reads the directory next takes it as the hybrid's and starts the
    // voter again, which ends where an uninterrupted start does.
    for crash_after in 1..=6 {
        let dir = root.path().join(format!("crash-{crash_after}"));
        copy(&dir, Below::Stamped, &["a", "b", "c"], &["unacked"]).await;
        CRASH_AFTER.with(|at| at.set(crash_after));
        let crashed = start_voter(
            &dir,
            LABEL,
            open_config(),
            &base,
            sides(&side_dirs(&dir)),
            VOTE_FOR,
            snapshot(&base),
        )
        .await;
        CRASH_AFTER.with(|at| at.set(0));
        assert!(crashed.is_err(), "step {crash_after}");
        assert_eq!(
            engine_marker::switched_base(&dir, LABEL).expect("marker"),
            None,
            "after step {crash_after}"
        );
        assert_eq!(start(&dir, &base).await, Ok(()), "after step {crash_after}");
        assert_eq!(files(&dir), expected, "after step {crash_after}");
        let log = DiskLog::open(&dir, LABEL, raft()).expect("open as a Raft log");
        assert_eq!(log.tail_offset().await.expect("tail"), n + 1);
        assert_eq!(payloads(&log, 0).await, ["a", "b", "c"]);
        assert_eq!(
            crate::raft_log::last_point(&log).await.expect("last"),
            Some(crate::raft_log::LogPoint {
                term: SEAL - 1,
                index: n
            })
        );
        assert_eq!(
            log.accepted_vote(),
            (SEAL, Some(Arc::from(VOTE_FOR)), false)
        );
        log.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn a_copy_that_does_not_hold_the_base_is_not_a_voter_and_is_left_alone() {
    let root = tempdir().expect("tempdir");
    let leader = root.path().join("leader");
    let n = copy(&leader, Below::Stamped, &["a", "b", "c", "d"], &[]).await;
    let base = base_of(&leader, n).await;

    // A different record below the base.
    let differs = root.path().join("differs");
    copy(&differs, Below::Stamped, &["a", "x", "c", "d"], &[]).await;
    // A copy whose log ends before the base.
    let lags = root.path().join("lags");
    copy(&lags, Below::Stamped, &["a", "b"], &[]).await;
    // A copy whose record at the base is another generation's.
    let other_seal = root.path().join("other-seal");
    copy(&other_seal, Below::Stamped, &["a", "b", "c", "d"], &[]).await;
    let other_base = SwitchBase {
        generation: SEAL + 1,
        ..base
    };

    for (dir, base, expected) in [
        (
            &differs,
            base,
            NotAVoter::DigestDiffers {
                start: 0,
                offset: n,
            },
        ),
        (&lags, base, NotAVoter::EndsBelow { tail: 3, offset: n }),
        (
            &other_seal,
            other_base,
            NotAVoter::NotTheSeal {
                offset: n,
                generation: SEAL + 1,
            },
        ),
    ] {
        let before = files(dir);
        assert_eq!(start(dir, &base).await, Err(expected));
        assert_eq!(files(dir), before);
        assert_eq!(
            engine_marker::switched_base(dir, LABEL).expect("marker"),
            None
        );
    }

    // A log that starts above the seal's start.
    let starts_late = root.path().join("starts-late");
    {
        let log = DiskLog::open_at(&starts_late, LABEL, open_config(), 2).expect("open");
        log.shutdown().await.expect("shutdown");
    }
    let before = files(&starts_late);
    assert_eq!(
        start(&starts_late, &base).await,
        Err(NotAVoter::StartsAbove { first: 2, start: 0 })
    );
    assert_eq!(files(&starts_late), before);
}

#[tokio::test]
async fn a_member_never_starts_from_a_second_base() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let n = copy(&dir, Below::Stamped, &["a", "b"], &[]).await;
    let base = base_of(&dir, n).await;
    start(&dir, &base).await.expect("voter");
    let second = SwitchBase {
        offset: n - 1,
        ..base
    };
    assert_eq!(
        start(&dir, &second).await,
        Err(NotAVoter::AnotherBase {
            found: Some((SEAL, n)),
            base: (SEAL, n - 1),
        })
    );

    // A directory claimed as an empty Raft log is not the hybrid's.
    let claimed = root.path().join("claimed");
    engine_marker::claim_for_raft(&claimed, LABEL).expect("claim");
    assert_eq!(
        start(&claimed, &base).await,
        Err(NotAVoter::AnotherBase {
            found: None,
            base: (SEAL, n),
        })
    );
}

#[tokio::test]
async fn a_record_at_the_base_from_another_claim_is_not_the_seal() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let log = DiskLog::open(&dir, LABEL, open_config()).expect("open");
    lead(&log, 1).await;
    log.append(&[record("a"), record("b")])
        .await
        .expect("append");
    lead(&log, SEAL).await;
    let base = SwitchBase {
        generation: SEAL,
        offset: 2,
        start: 0,
        digest: digest(&log, 0, 2).await.expect("digest"),
        dead_letters: empty_side(),
        cursors: empty_side(),
    };
    // Same generation, offset and records, but the record names other
    // cursors than the ones recorded.
    let other = SwitchBase {
        cursors: SideRange {
            end: 1,
            ..empty_side()
        },
        ..base
    };
    log.append(&[other.record(7)]).await.expect("seal");
    log.shutdown().await.expect("shutdown");
    assert_eq!(
        start(&dir, &base).await,
        Err(NotAVoter::NotTheSeal {
            offset: 2,
            generation: SEAL
        })
    );
}

#[tokio::test]
async fn a_copy_whose_side_log_differs_from_the_seals_is_not_a_voter() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let n = copy(&dir, Below::Stamped, &["a", "b"], &[]).await;
    let base = base_of(&dir, n).await;
    // A cursor the seal leader's log did not hold.
    let cursors = DiskLog::open(&side_dirs(&dir)[1].0, LABEL, open_config()).expect("open");
    lead(&cursors, 1).await;
    cursors.append(&[record("cursor")]).await.expect("append");
    cursors.shutdown().await.expect("shutdown");
    let before = files(&dir);
    assert_eq!(
        start(&dir, &base).await,
        Err(NotAVoter::SideDiffers {
            log: SideLog::Cursors
        })
    );
    assert_eq!(files(&dir), before);
}

#[tokio::test]
async fn a_copy_that_accepted_a_later_generation_is_refused_before_anything_is_cut() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let n = copy(&dir, Below::Stamped, &["a", "b", "c"], &[]).await;
    let base = base_of(&dir, n).await;
    // The copy holds the seal's record and the matching range, then took a
    // later generation and its records above the base.
    {
        let log = DiskLog::open(&dir, LABEL, open_config()).expect("open");
        lead(&log, SEAL + 1).await;
        log.append(&[record("later")]).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }
    let before = files(&dir);

    assert_eq!(
        start(&dir, &base).await,
        Err(NotAVoter::AcceptedAbove {
            accepted: SEAL + 1,
            generation: SEAL,
        })
    );
    // Not one byte changed: the later records, no purged id, no snapshot.
    assert_eq!(files(&dir), before);
    let log = DiskLog::open(&dir, LABEL, open_config()).expect("open");
    assert_eq!(payloads(&log, 0).await, ["a", "b", "c", "later"]);
    log.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retention_deletes_nothing_between_the_check_and_the_cut() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    // Two segments: retention at one byte would delete the first.
    let n = copy(&dir, Below::Unstamped, &["a", "b", "c", "d"], &["unacked"]).await;
    let base = base_of(&dir, n).await;
    let retained = LogConfig {
        retention_bytes: Some(1),
        retention_check_interval: std::time::Duration::from_secs(3600),
        ..open_config()
    };

    RETAIN_BETWEEN.with(|at| at.set(true));
    let side = side_dirs(&dir);
    let started = start_voter(
        &dir,
        LABEL,
        retained,
        &base,
        sides(&side),
        VOTE_FOR,
        snapshot(&base),
    )
    .await;
    RETAIN_BETWEEN.with(|at| at.set(false));
    assert_eq!(started.expect("start"), Ok(()));

    let log = DiskLog::open(&dir, LABEL, raft()).expect("open as a Raft log");
    assert_eq!(log.base_offset(), 0);
    assert_eq!(payloads(&log, 0).await, ["a", "b", "c", "d"]);
    log.shutdown().await.expect("shutdown");
}
