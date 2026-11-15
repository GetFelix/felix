//! What a re-form reads: kept copies' commits and the pad above every
//! reservation.

use std::collections::{BTreeMap, BTreeSet};

use openraft::storage::RaftLogStorage;
use tempfile::TempDir;

use super::reclaim::{kept_dir, rotated};
use super::*;
use crate::DiskLog;
use crate::raft_log::tests::writes::append_flushed;
use crate::raft_log::tests::{Codec, Types, log_id, normal};
use crate::raft_log::{LogPoint, RaftLog, reserved};

type Log = RaftLog<Types, Codec>;

const SPACING: u64 = 4096;

fn open(dir: &TempDir) -> Log {
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    Log::new(log, Codec).expect("a Raft log")
}

/// A log holding entries 0..=9 of term 1 whose saved commit names (term 5,
/// index 10), an entry its last sync never covered: the state a power cut
/// leaves on a relaxed copy.
async fn saved_commit_past_the_log() -> TempDir {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..10).map(|index| normal(1, index, "kept")).collect();
    entries.push(normal(5, 10, "lost"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(5, 10)))
        .await
        .expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    append_flushed(&mut raft, &entries[..10]).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");
    dir
}

/// A kept copy is read without an open, so its saved commit is cut at its
/// last entry that decodes here, to what an open of the same bytes reads.
#[tokio::test]
async fn a_kept_saved_commit_past_its_last_entry_is_cut_at_the_entry() {
    let dir = saved_commit_past_the_log().await;
    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!(kept.last, Some(9));
    assert_eq!(kept.committed, Some(9));

    // Nothing was written, and an open agrees.
    let mut raft = open(&dir);
    assert_eq!(
        raft.read_committed().await.expect("read"),
        Some(log_id(1, 9))
    );
    raft.disk_log().close().await.expect("close");
}

/// The cut lands on an earlier entry of the committed one's log, so a
/// later term there is another log's entry and vouches for nothing.
#[tokio::test]
async fn a_kept_commit_cut_onto_a_later_terms_entry_vouches_for_nothing() {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let entries: Vec<_> = (0..4).map(|index| normal(2, index, "x")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(2, 3))).await.expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let other: Vec<_> = (0..2).map(|index| normal(3, index, "y")).collect();
    append_flushed(&mut raft, &other).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!(kept.last, Some(1));
    assert_eq!(kept.committed, None);
}

/// A saved commit inside the log whose entry is another term's names some
/// other log's entry. An open does not take it, so neither does the read.
#[tokio::test]
async fn a_kept_commit_on_another_terms_entry_inside_the_log_vouches_for_nothing() {
    let ahead = tempfile::tempdir().expect("dir");
    let mut raft = open(&ahead);
    let mut entries: Vec<_> = (0..5).map(|index| normal(1, index, "x")).collect();
    entries.push(normal(2, 5, "other"));
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(2, 5))).await.expect("save");
    raft.disk_log().close().await.expect("close");

    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let ours: Vec<_> = (0..6).map(|index| normal(1, index, "x")).collect();
    append_flushed(&mut raft, &ours).await;
    raft.disk_log().close().await.expect("close");
    std::fs::copy(ahead.path().join("replica"), dir.path().join("replica")).expect("copy");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(5), None));
}

/// A saved commit the log holds stands as saved.
#[tokio::test]
async fn a_kept_commit_inside_the_log_stands() {
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let entries: Vec<_> = (0..6).map(|index| normal(1, index, "x")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.save_committed(Some(log_id(1, 3))).await.expect("save");
    raft.disk_log().commit_offset_persisted().await;
    raft.disk_log().close().await.expect("close");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(5), Some(3)));
}

/// The pad reads the kept copies too: a kept reservation above the current
/// copy's still moves the first free index past it.
#[tokio::test]
async fn the_first_free_index_is_above_every_reservation_current_or_kept() {
    let root = Root::new();
    let dir = root.shard_dir();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    reserved::store(&kept, 100).expect("reserve");
    reserved::store(&dir, 40).expect("reserve");

    assert_eq!(first_free_index(&dir, Some(2), None).expect("pad"), 101);
    assert_eq!(
        first_free_index(&dir, Some(2), Some(500)).expect("pad"),
        501
    );
    assert_eq!(
        first_free_index(&dir, Some(200), Some(5)).expect("pad"),
        201
    );

    let logs = kept_logs(&dir, SPACING).expect("kept");
    assert_eq!(
        logs,
        vec![KeptLog {
            path: kept,
            copy: Some(old),
            last: Some(2),
            committed: None,
        }]
    );
    new.log.close().await.expect("close");
}

/// A copy that never reserved and an empty base pad from the floor alone.
#[tokio::test]
async fn with_no_reservation_the_floor_and_base_decide() {
    let root = Root::new();
    let dir = root.shard_dir();
    std::fs::create_dir_all(&dir).expect("dir");
    assert_eq!(first_free_index(&dir, None, None).expect("pad"), 0);
    assert_eq!(first_free_index(&dir, None, Some(7)).expect("pad"), 8);
}

/// A purge only reaches committed entries, so a kept copy that purged
/// through 20 vouches for 20 whether its saved commit is behind that or
/// was never saved, as an open reads it.
#[tokio::test]
async fn a_kept_copy_vouches_for_its_purged_id_when_its_saved_commit_is_behind() {
    for saved in [Some(5), None] {
        let dir = tempfile::tempdir().expect("dir");
        let mut raft = open(&dir);
        let entries: Vec<_> = (0..=30).map(|index| normal(1, index, "x")).collect();
        append_flushed(&mut raft, &entries).await;
        if let Some(index) = saved {
            raft.save_committed(Some(log_id(1, index)))
                .await
                .expect("save");
        }
        raft.purge(log_id(1, 20)).await.expect("purge");
        raft.disk_log().close().await.expect("close");

        let kept = read_kept(dir.path(), SPACING).expect("read");
        assert_eq!(
            (kept.last, kept.committed),
            (Some(30), Some(20)),
            "{saved:?}"
        );
    }
}

/// Generation 0 is no term, so a saved commit stamped with it names no
/// entry, even where the record there is unstamped too.
#[tokio::test]
async fn a_kept_commit_saved_with_no_generation_vouches_for_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let records: Vec<_> = (0..4u8)
        .map(|n| crate::log::AppendRecord {
            payload: bytes::Bytes::from(vec![n]),
            timestamp_micros: 1,
            mark: crate::log::RecordMark::None,
            publisher: None,
            headers: None,
        })
        .collect();
    log.append_v9_for_tests(&records, 0).await.expect("append");
    log.save_committed(2, 0).await.expect("save");
    log.close().await.expect("close");

    let kept = read_kept(dir.path(), SPACING).expect("read");
    assert_eq!((kept.last, kept.committed), (Some(3), None));
}

/// A kept directory that cannot be read fails the whole read.
#[tokio::test]
async fn an_unreadable_kept_directory_fails_kept_logs() {
    let root = Root::new();
    let dir = root.shard_dir();
    let (_copies, old, new) = rotated(&root, 2).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    std::fs::write(kept.join("raft-purged"), b"garbage").expect("damage");

    assert!(kept_logs(&dir, SPACING).is_err());
    new.log.close().await.expect("close");
}

/// A reservation that does not decode cannot be padded around, so the
/// first free index is an error, never a guess.
#[tokio::test]
async fn a_damaged_reservation_fails_the_first_free_index() {
    let root = Root::new();
    let dir = root.shard_dir();
    std::fs::create_dir_all(&dir).expect("dir");
    reserved::store(&dir, 10).expect("reserve");
    std::fs::write(dir.join("raft-reserved"), b"garbage").expect("damage");

    assert!(first_free_index(&dir, Some(2), None).is_err());
}

/// A rotation writes why into the kept directory, and the survey reads the
/// current copy and the kept one with their terms, votes and reservations.
#[tokio::test]
async fn a_survey_reads_every_copy_with_its_reason_and_terms() {
    let root = Root::new();
    let dir = root.shard_dir();
    let (_copies, old, new) = rotated(&root, 3).await;
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    assert_eq!(rotated_by::load(&kept).expect("by"), RotatedBy::Restore);
    reserved::store(&kept, 9).expect("reserve");
    new.log.close().await.expect("close");

    let probes = BTreeSet::from([1, 3, 8]);
    let read = read_copies(&dir, SPACING, &probes).expect("read");
    assert_eq!(read.current.copy, Some(new.copy));
    assert_eq!((read.current.last, read.current.rotated_by), (None, None));
    let [kept_read] = &read.kept[..] else {
        panic!("one kept copy: {:?}", read.kept);
    };
    assert_eq!(kept_read.copy, Some(old));
    assert_eq!(kept_read.last, Some(LogPoint { term: 1, index: 3 }));
    assert_eq!(kept_read.rotated_by, Some(RotatedBy::Restore));
    assert_eq!(kept_read.reserved, Some(9));
    assert_eq!(kept_read.epoch, None);
    assert_eq!(
        kept_read.terms,
        BTreeMap::from([(1, Some(1)), (3, Some(1)), (8, None)])
    );

    let file = EpochFile {
        incarnation: 3,
        bases: vec![EpochBase {
            base_last: Some(EpochLogId {
                index: 2,
                epoch: 0,
                term: 1,
            }),
            first_index: 6,
        }],
    };
    epoch_file::store(&kept, &file).expect("epoch");
    let read = read_copies(&dir, SPACING, &probes).expect("read");
    assert_eq!(read.kept[0].epoch, Some(file));
}

/// Fail closed: a kept `rotated-by` or `raft-epoch` that does not decode
/// fails the survey, never reads as a default.
#[tokio::test]
async fn a_survey_fails_on_any_file_that_does_not_decode() {
    for name in ["rotated-by", "raft-epoch"] {
        let root = Root::new();
        let dir = root.shard_dir();
        let (_copies, old, new) = rotated(&root, 1).await;
        new.log.close().await.expect("close");
        let kept = kept_dir(&root).join(format!("0.{old:016x}"));
        std::fs::write(kept.join(name), b"garbage").expect("damage");
        let err = read_copies(&dir, SPACING, &BTreeSet::new()).expect_err(name);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{name}: {err}");
    }
}

/// Every reason but a log below its mark is a restore.
#[test]
fn only_a_log_below_its_mark_is_a_power_cut() {
    use RotateReason::*;
    for reason in [
        Empty,
        NoCopyFile,
        StorageId,
        NoMarks,
        OtherCopy,
        VoteBelowMark,
        Poisoned,
    ] {
        assert_eq!(RotatedBy::of(reason, 1), RotatedBy::Restore, "{reason:?}");
    }
    assert_eq!(
        RotatedBy::of(LogBelowMark, 1),
        RotatedBy::PowerCut { to: 1 }
    );
}

/// `raft-epoch` refuses what no start writes: nonzero reserved bytes, a
/// first index below its base's end, a base ending in a later epoch.
#[test]
fn an_epoch_file_that_breaks_its_rules_does_not_decode() {
    let dir = tempfile::tempdir().expect("dir");
    let base = |index, epoch, first_index| EpochBase {
        base_last: Some(EpochLogId {
            index,
            epoch,
            term: 1,
        }),
        first_index,
    };
    let store = |bases: Vec<EpochBase>| {
        epoch_file::store(
            dir.path(),
            &EpochFile {
                incarnation: 1,
                bases,
            },
        )
        .expect("store")
    };
    let fails = || {
        let err = epoch_file::load(dir.path()).expect_err("decodes");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    };

    store(vec![base(3, 0, 5), base(6, 1, 9)]);
    assert_eq!(
        epoch_file::load(dir.path())
            .expect("load")
            .expect("file")
            .epoch(),
        2
    );

    store(vec![base(3, 0, 3)]);
    fails();
    store(vec![base(3, 1, 5)]);
    fails();

    store(vec![base(3, 0, 5)]);
    let path = dir.path().join("raft-epoch");
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[17] = 1;
    let body = bytes.len() - 4;
    let crc = crate::segment::format::crc32(&[&bytes[..body]]);
    bytes[body..].copy_from_slice(&crc.to_be_bytes());
    std::fs::write(&path, &bytes).expect("write");
    fails();
}

/// A copy as a start leaves it: entries 0..=4 of term 1 as its base, the
/// epoch's first index at 8, and a purged id at 7 so its first entry lands
/// there. `extra` is appended above the base first.
async fn started(extra: &[(u64, u64)]) -> TempDir {
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = open(&dir);
    let mut entries: Vec<_> = (0..5).map(|index| normal(1, index, "base")).collect();
    entries.extend(extra.iter().map(|&(term, index)| normal(term, index, "x")));
    append_flushed(&mut raft, &entries).await;
    raft.disk_log().close().await.expect("close");
    crate::raft_log::store_purged(dir.path(), 7, crate::raft_log::generation_of(1).unwrap())
        .expect("purged");
    let file = EpochFile {
        incarnation: 1,
        bases: vec![EpochBase {
            base_last: Some(EpochLogId {
                index: 4,
                epoch: 0,
                term: 1,
            }),
            first_index: 8,
        }],
    };
    epoch_file::store(dir.path(), &file).expect("epoch");
    dir
}

/// The purged id a start writes ends its gap and names no entry: the copy
/// reads as its base, with every base index's term from its records.
#[tokio::test]
async fn a_started_copy_reads_as_its_base() {
    let dir = started(&[]).await;
    let probes = BTreeSet::from([2, 4, 6, 7]);
    let read = read_copy(dir.path(), SPACING, &probes).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 4 }));
    assert_eq!(read.purged, None);
    assert_eq!(
        read.terms,
        BTreeMap::from([(2, Some(1)), (4, Some(1)), (6, None), (7, None)])
    );
}

/// A record in the gap is not the base: the purged id stays, so the plan
/// sees an id in the gap and refuses.
#[tokio::test]
async fn a_started_copy_with_a_record_in_its_gap_keeps_its_purged_id() {
    let dir = started(&[(1, 5)]).await;
    let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 7 }));
    assert_eq!(read.purged, Some(7));
}

/// Plan a re-form of epoch 1 from three copies read off disk, each as
/// `started` left it with `extra` above its base.
async fn replan(extra: &[(u64, u64)]) -> Result<ReformPlan<&'static str>, Refused<&'static str>> {
    let dirs = [
        started(extra).await,
        started(extra).await,
        started(extra).await,
    ];
    let probes: BTreeSet<u64> = (0..10).collect();
    let mut reports = BTreeMap::new();
    for ((name, dir), copy) in ["a", "b", "c"].into_iter().zip(&dirs).zip(1..) {
        let mut current = read_copy(dir.path(), SPACING, &probes).expect("read");
        current.copy = Some(copy);
        current.vote = Some(1);
        reports.insert(
            name,
            BrokerReport {
                leads: false,
                names: BTreeSet::new(),
                copies: BrokerCopies {
                    current,
                    kept: vec![],
                },
            },
        );
    }
    let record = GroupRecord {
        incarnation: 1,
        bases: epoch_file::load(dirs[0].path())
            .expect("epoch")
            .expect("a file")
            .bases,
        floor: None,
        placed: Some(BTreeSet::from(["a", "b", "c"])),
        dead: BTreeSet::new(),
    };
    plan_reform(&record, &reports)
}

/// #2640: a voter that started its epoch and is re-formed again vouches its
/// whole base, though its purged id sits at the end of the gap.
#[tokio::test]
async fn a_started_voter_re_formed_again_vouches_its_whole_base() {
    let plan = replan(&[]).await.expect("a plan");
    assert!(!plan.marked);
    assert_eq!(plan.base_end(), 5);
}

/// A real record in the gap still refuses the plan.
#[tokio::test]
async fn a_started_copy_with_a_record_in_its_gap_refuses_the_plan() {
    let refused = replan(&[(1, 5)]).await.expect_err("refused");
    assert!(
        matches!(refused, Refused::RecordInGap { index: 7, .. }),
        "{refused:?}"
    );
}
